import asyncio
import json
import random
import hashlib
import re
import time
from contextlib import asynccontextmanager
from typing import Dict, Optional
from urllib.parse import urlencode, quote

import httpx
import websockets
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

# ============================================================
#  IN-MEMORY SESSION STORE
#  (one session per RC code — good enough for personal use)
# ============================================================
SESSIONS: Dict[str, dict] = {}
# session = {
#   "ws": websocket or None,
#   "id": "", "password": "", "username": "",
#   "haaapsi": "", "platform": "352",
#   "planets": [],
#   "auto_pop": {planet_id: asyncio.Task}
# }

GALAXY_BASE = "https://galaxy.mobstudio.ru/services/"

DEFAULT_HEADERS = {
    "accept": "*/*",
    "x-galaxy-client-ver": "9.5",
    "x-galaxy-lng": "en",
    "x-galaxy-model": "chrome 139.0.0.0",
    "x-galaxy-orientation": "portrait",
    "x-galaxy-os-ver": "1",
    "x-galaxy-platform": "web",
    "x-galaxy-scr-dpi": "3",
    "x-galaxy-scr-h": "704",
    "x-galaxy-scr-w": "360",
}

PHONE_MESSAGES = [
    "PHONE 1080 2400 0 2 :Dalvik/2.1.0 (Linux; U; Android 12; DN2101 Build/SP1A.210812.016)",
    "PHONE 1080 2340 0 2 :Dalvik/2.1.0 (Linux; U; Android 11; M2007J20CG Build/RKQ1.200710.003)",
    "PHONE 1440 3200 0 2 :Dalvik/2.1.0 (Linux; U; Android 13; SM-G996B Build/TP1A.220624.014)",
    "PHONE 1080 2412 0 2 :Dalvik/2.1.0 (Linux; U; Android 13; CPH2409 Build/TKQ1.230127.002)",
    "PHONE 720 1600 0 2 :Dalvik/2.1.0 (Linux; U; Android 10; Redmi 9A Build/QP1A.190711.020)",
    "PHONE 1080 2400 0 2 :Dalvik/2.1.0 (Linux; U; Android 12; SM-A525F Build/SP1A.210812.016)",
    "PHONE 1440 3120 0 2 :Dalvik/2.1.0 (Linux; U; Android 9; LM-G820 Build/PKQ1.190522.001)",
    "PHONE 1080 2460 0 2 :Dalvik/2.1.0 (Linux; U; Android 14; V2141 Build/UP1A.231005.007)",
    "PHONE 1080 2340 0 2 :Dalvik/2.1.0 (Linux; U; Android 11; RMX3085 Build/RP1A.200720.011)",
    "PHONE 1080 2408 0 2 :Dalvik/2.1.0 (Linux; U; Android 13; SM-M336B Build/TP1A.220624.014)",
]


# ============================================================
#  HELPERS
# ============================================================
def parse_haaapsi(value: str) -> str:
    if not value:
        return ""
    temp = hashlib.md5(value.encode()).hexdigest()
    # reverse then fill with '0' (mirrors JS: temp.split('').reverse().join('0'))
    temp = "0".join(temp[::-1])
    return temp[5:15]


def extract_planets_from_html(html: str) -> list:
    """Parse vue-data divs from Galaxy channel list response."""
    planets = []
    pattern = re.compile(r'<div class="vue-data"[^>]*data-json="([^"]*)"')
    for m in pattern.finditer(html):
        raw = m.group(1)
        try:
            from urllib.parse import unquote
            data = json.loads(unquote(raw))
        except Exception:
            try:
                import base64
                data = json.loads(base64.b64decode(raw))
            except Exception:
                continue
        channels = data.get("channels") if isinstance(data, dict) else None
        if isinstance(channels, list):
            for ch in channels:
                if ch.get("id") is not None and ch.get("name") is not None:
                    planets.append({
                        "id": str(ch["id"]),
                        "name": str(ch["name"]),
                        "online": int(ch.get("online") or 0),
                    })
    # dedupe by id
    seen = set()
    unique = []
    for p in planets:
        if p["id"] in seen:
            continue
        seen.add(p["id"])
        unique.append(p)
    return unique


async def galaxy_get(params: dict, platform: str = "352", cookies: dict = None) -> str:
    headers = dict(DEFAULT_HEADERS)
    headers["x-galaxy-kbv"] = platform
    async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
        r = await client.get(GALAXY_BASE, params=params, headers=headers)
        r.raise_for_status()
        return r.text


async def galaxy_post_multipart(fields: dict, params: dict, platform: str = "352") -> str:
    headers = dict(DEFAULT_HEADERS)
    headers["x-galaxy-kbv"] = platform
    # httpx builds multipart automatically when using `files` with bytes
    files = {k: (None, str(v)) for k, v in fields.items()}
    async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
        r = await client.post(GALAXY_BASE, params=params, headers=headers, files=files)
        r.raise_for_status()
        return r.text


# ============================================================
#  GALAXY WEBSOCKET HANDSHAKE
# ============================================================
async def galaxy_handshake(session: dict) -> bool:
    """Connect to Galaxy WS, do handshake, store id/password/username."""
    try:
        ws = await websockets.connect("wss://cs.mobstudio.ru:6672", ping_interval=None)
        session["ws"] = ws

        platform = session.get("platform", "352")

        async def reader():
            async for msg in ws:
                text = msg if isinstance(msg, str) else msg.decode(errors="ignore")
                parts = text.replace(";", " ").split(" ")
                head = parts[0] if parts else ""

                if head == "REGISTER" and len(parts) >= 4:
                    session["id"] = parts[1]
                    session["password"] = parts[2]
                    session["username"] = parts[3].split("\r\n")[0]
                    haa = parse_haaapsi(session.get("haaapsi", ""))
                    await ws.send(
                        f"USER {session['id']} {session['password']} "
                        f"{session['username']} {haa}\r\n"
                    )

                elif head == "HAAAPSI":
                    session["haaapsi"] = parts[1] if len(parts) > 1 else ""
                    rc = session.get("rc", "")
                    if rc:
                        await ws.send(f"RECOVER {rc}\r\n")

                elif head == "999":
                    phone = random.choice(PHONE_MESSAGES)
                    await ws.send("FWLISTVER 336\r\n")
                    await ws.send("ADDONS 252213 2\r\n")
                    await ws.send("MYADDONS 252213 2\r\n")
                    await ws.send(phone + "\r\n")
                    await ws.send("JOIN\r\n")

                elif text.startswith("PING"):
                    await ws.send("PONG\r\n")

                # keep handshake messages for debugging
                session.setdefault("log", []).append(text[:200])

        reader_task = asyncio.create_task(reader())
        session["reader_task"] = reader_task

        # kick off
        await ws.send(f":en IDENT {platform} -2 4030 1 2 :GALA\r\n")

        # wait up to 15s for credentials
        for _ in range(150):
            if session.get("id") and session.get("password") and session.get("username"):
                return True
            await asyncio.sleep(0.1)

        return False
    except Exception as e:
        session["error"] = str(e)
        return False


# ============================================================
#  FASTAPI APP
# ============================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    # cleanup on shutdown
    for s in SESSIONS.values():
        if s.get("ws"):
            try:
                await s["ws"].close()
            except Exception:
                pass
        for t in s.get("auto_pop", {}).values():
            t.cancel()


app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],          # tighten in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/", response_class=HTMLResponse)
async def root():
    with open("static/index.html", "r", encoding="utf-8") as f:
        return f.read()


# ---------- REQUEST MODELS ----------
class ConnectReq(BaseModel):
    rc: str
    platform: str = "352"


class PopReq(BaseModel):
    rc: str
    planet_id: str
    planet_name: str


# ---------- ENDPOINTS ----------
@app.post("/api/connect")
async def api_connect(req: ConnectReq):
    rc = req.rc.strip()[:10]
    if not rc:
        raise HTTPException(400, "Missing RC")

    # reuse existing session if already connected
    if rc in SESSIONS and SESSIONS[rc].get("ws"):
        return {"ok": True, "reused": True,
                "username": SESSIONS[rc].get("username", "")}

    session = {
        "rc": rc,
        "platform": req.platform,
        "ws": None,
        "id": "",
        "password": "",
        "username": "",
        "haaapsi": "",
        "planets": [],
        "auto_pop": {},
        "log": [],
    }
    SESSIONS[rc] = session

    ok = await galaxy_handshake(session)
    if not ok:
        SESSIONS.pop(rc, None)
        raise HTTPException(502, f"Handshake failed: {session.get('error', 'timeout')}")

    # auto-load planets after 5s (fire-and-forget)
    async def delayed_load():
        await asyncio.sleep(5)
        try:
            await load_planets_internal(rc)
        except Exception as e:
            print("delayed load error:", e)
    asyncio.create_task(delayed_load())

    return {"ok": True, "username": session["username"],
            "id": session["id"]}


@app.post("/api/disconnect")
async def api_disconnect(req: ConnectReq):
    rc = req.rc.strip()[:10]
    s = SESSIONS.get(rc)
    if not s:
        return {"ok": True, "note": "no session"}
    for t in s.get("auto_pop", {}).values():
        t.cancel()
    if s.get("ws"):
        try:
            await s["ws"].send("QUIT :ds\r\n")
        except Exception:
            pass
        try:
            await s["ws"].close()
        except Exception:
            pass
    # keep planets & credentials in memory (persistent auto-pop like original)
    s["ws"] = None
    return {"ok": True}


async def load_planets_internal(rc: str):
    s = SESSIONS.get(rc)
    if not s or not s.get("id"):
        raise HTTPException(400, "Not authenticated")
    params = {
        "a": "channel_list_index",
        "type": "user",
        "force_my": "1",
        "userID": s["id"],
        "password": s["password"],
        "usercur": "87629269",
        "random": str(random.random()),
    }
    html = await galaxy_get(params, platform=s["platform"])
    planets = extract_planets_from_html(html)
    s["planets"] = planets
    return planets


@app.get("/api/planets/{rc}")
async def api_planets(rc: str):
    s = SESSIONS.get(rc.strip()[:10])
    if not s:
        raise HTTPException(404, "Session not found")
    if not s.get("planets"):
        await load_planets_internal(rc.strip()[:10])
    return {"ok": True, "planets": s["planets"],
            "username": s.get("username", "")}


@app.get("/api/status/{rc}")
async def api_status(rc: str):
    s = SESSIONS.get(rc.strip()[:10])
    if not s:
        return {"connected": False}
    return {
        "connected": bool(s.get("ws")),
        "username": s.get("username", ""),
        "id": s.get("id", ""),
        "planets": len(s.get("planets", [])),
        "auto_pop": list(s.get("auto_pop", {}).keys()),
    }


# ---------- AUTO POP ----------
async def fetch_top10(rc: str) -> list:
    s = SESSIONS[rc]
    params = {
        "a": "channel_list_index",
        "type": "popular",
        "userID": s["id"],
        "password": s["password"],
        "usercur": s["id"],
        "random": str(random.random()),
    }
    html = await galaxy_get(params, platform=s["platform"])
    planets = extract_planets_from_html(html)
    return planets[:10]


async def send_pop(rc: str, planet_id: str):
    s = SESSIONS[rc]
    fields = {
        "a": "buy_privilege",
        "channel_id": planet_id,
        "use_fund": "1",
        "ajax": "1",
    }
    params = {
        "userID": s["id"],
        "password": s["password"],
        "query_rand": str(random.random()),
    }
    await galaxy_post_multipart(fields, params, platform=s["platform"])


async def auto_pop_loop(rc: str, planet_id: str, planet_name: str):
    try:
        while True:
            try:
                top = await fetch_top10(rc)
                names = [p["name"].strip().lower() for p in top]
                if planet_name.strip().lower() not in names:
                    await send_pop(rc, planet_id)
                    print(f"[pop] sent for {planet_name}")
                else:
                    print(f"[pop] {planet_name} in top10, skip")
            except Exception as e:
                print("[pop] error:", e)
            await asyncio.sleep(310)  # 5min 10s
    except asyncio.CancelledError:
        print(f"[pop] stopped for {planet_name}")


@app.post("/api/autopop/start")
async def api_autopop_start(req: PopReq):
    rc = req.rc.strip()[:10]
    s = SESSIONS.get(rc)
    if not s or not s.get("id"):
        raise HTTPException(400, "Not authenticated")
    if req.planet_id in s["auto_pop"]:
        return {"ok": True, "note": "already running"}
    task = asyncio.create_task(
        auto_pop_loop(rc, req.planet_id, req.planet_name)
    )
    s["auto_pop"][req.planet_id] = task
    return {"ok": True}


@app.post("/api/autopop/stop")
async def api_autopop_stop(req: PopReq):
    rc = req.rc.strip()[:10]
    s = SESSIONS.get(rc)
    if not s:
        raise HTTPException(404, "Session not found")
    t = s["auto_pop"].pop(req.planet_id, None)
    if t:
        t.cancel()
    return {"ok": True}


@app.get("/api/health")
async def health():
    return {"ok": True, "sessions": len(SESSIONS)}
