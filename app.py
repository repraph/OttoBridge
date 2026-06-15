"""
OttoBridge v2 — Multi-Printer Orchestrator
Supports: Bambu Lab (X1C, P1S, P1P, A1, P2S), Prusa (MK3/MK4/Core One),
          Creality (K1C), Anycubic (Kobra S1), Elegoo (Centauri Carbon),
          FlashForge (AD5X, Adventurer 5M Pro), Klipper/Moonraker (generic)
Pi Zero 2 W — runs alongside Klipper + Moonraker
"""

import asyncio, ftplib, json, logging, os, ssl, time, uuid
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

import aiomqtt
import aiofiles, aiofiles.os
import httpx
from fastapi import FastAPI, HTTPException, UploadFile, File, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("ottobridge")

BASE_DIR    = Path(__file__).parent
UPLOAD_DIR  = BASE_DIR / "uploads"
CONFIG_FILE = BASE_DIR / "config.json"
UPLOAD_DIR.mkdir(exist_ok=True)

MOONRAKER_URL = os.getenv("MOONRAKER_URL", "http://localhost:7125")

# ── Brand registry ─────────────────────────────────────────────────────────────
# Macro names match the actual cfg files exactly (corrected from original OTTOengine)
BRANDS = {
    "bambu_lab": {
        "label": "Bambu Lab",
        "protocol": "mqtt_ftp",
        "models": ["X1C", "P1S", "P1P", "A1", "A1 Mini", "P2S"],
        "auth_fields": ["ip", "access_code", "serial"],
        "start_grace_s": 180,
    },
    "prusa": {
        "label": "Prusa",
        "protocol": "prusalink",
        "models": ["MK3S", "MK3", "MK4S", "MK4", "Core One"],
        "auth_fields": ["ip", "api_key"],
        "start_grace_s": 480,
    },
    "creality": {
        "label": "Creality",
        "protocol": "websocket",
        "models": ["K1C", "K1", "K1 Max"],
        "auth_fields": ["ip"],
        "start_grace_s": 120,
    },
    "anycubic": {
        "label": "Anycubic",
        "protocol": "moonraker",
        "models": ["Kobra S1"],
        "auth_fields": ["ip"],
        "start_grace_s": 720,
    },
    "elegoo": {
        "label": "Elegoo",
        "protocol": "websocket_elegoo",
        "models": ["Centauri Carbon", "Centauri"],
        "auth_fields": ["ip"],
        "start_grace_s": 480,
    },
    "flashforge": {
        "label": "FlashForge",
        "protocol": "http_tcp",
        "models": ["AD5X", "Adventurer 5M Pro", "Adventurer 5M"],
        "auth_fields": ["ip", "serial_code", "check_code"],
        "start_grace_s": 480,
    },
    "klipper": {
        "label": "Klipper / Moonraker",
        "protocol": "moonraker",
        "models": ["Generic Klipper"],
        "auth_fields": ["ip"],
        "start_grace_s": 360,
    },
}

# ── Macro resolver — names match actual .cfg files exactly ────────────────────
import re as _re

def _norm(s):
    return _re.sub(r'[^a-z0-9]', '', str(s or '').lower())

# (eject_macro, load_macro, has_door)
_MACRO_MAP = {
    # Bambu Lab — X1C uses X_ONE_C from _printer_x1c.cfg
    ("bambu_lab", "x1c"):          ("EJECT_FROM_BAMBULAB_X_ONE_C",              "LOAD_ONTO_BAMBULAB_X_ONE_C",              True),
    ("bambu_lab", "p1s"):          ("EJECT_FROM_BAMBULAB_P_ONE_S",              "LOAD_ONTO_BAMBULAB_P_ONE_S",              True),
    ("bambu_lab", "p1p"):          ("EJECT_FROM_BAMBULAB_P_ONE_P",              "LOAD_ONTO_BAMBULAB_P_ONE_P",              False),
    ("bambu_lab", "a1"):           ("EJECT_FROM_BAMBULAB_A_ONE",                "LOAD_ONTO_BAMBULAB_A_ONE",                False),
    ("bambu_lab", "a1mini"):       ("EJECT_FROM_BAMBULAB_A_ONE",                "LOAD_ONTO_BAMBULAB_A_ONE",                False),
    ("bambu_lab", "p2s"):          ("EJECT_FROM_BAMBULAB_P_ONE_S",              "LOAD_ONTO_BAMBULAB_P_ONE_S",              True),
    # Prusa
    ("prusa", "mk3s"):             ("EJECT_FROM_PRUSA_MK_THREE_S",              "LOAD_ONTO_PRUSA_MK_THREE_S",              False),
    ("prusa", "mk3"):              ("EJECT_FROM_PRUSA_MK_THREE",                "LOAD_ONTO_PRUSA_MK_THREE",                False),
    ("prusa", "mk4s"):             ("EJECT_FROM_PRUSA_MK_FOUR_S",               "LOAD_ONTO_PRUSA_MK_FOUR_S",               False),
    ("prusa", "mk4"):              ("EJECT_FROM_PRUSA_MK_FOUR",                 "LOAD_ONTO_PRUSA_MK_FOUR",                 False),
    ("prusa", "coreone"):          ("EJECT_FROM_PRUSA_CORE_ONE",                "LOAD_ONTO_PRUSA_CORE_ONE",                False),
    # Anycubic
    ("anycubic", "kobras1"):       ("EJECT_FROM_ANYCUBIC_KOBRA_S_ONE",          "LOAD_ONTO_ANYCUBIC_KOBRA_S_ONE",          True),
    # Elegoo
    ("elegoo", "centauricarbon"):  ("EJECT_FROM_ELEGOO_CC",                     "LOAD_ONTO_ELEGOO_CC",                     True),
    ("elegoo", "centauri"):        ("EJECT_FROM_ELEGOO_CC",                     "LOAD_ONTO_ELEGOO_CC",                     True),
    # Creality
    ("creality", "k1c"):           ("EJECT_FROM_CREALITY_K_ONE_C",              "LOAD_ONTO_CREALITY_K_ONE_C",              True),
    ("creality", "k1"):            ("EJECT_FROM_CREALITY_K_ONE_C",              "LOAD_ONTO_CREALITY_K_ONE_C",              False),
    ("creality", "k1max"):         ("EJECT_FROM_CREALITY_K_ONE_C",              "LOAD_ONTO_CREALITY_K_ONE_C",              False),
    # FlashForge
    ("flashforge", "ad5x"):        ("EJECT_FROM_FLASHFORGE_AD_FIVE_X",          "LOAD_ONTO_FLASHFORGE_AD_FIVE_X",          False),
    ("flashforge", "adventurer5mpro"): ("EJECT_FROM_FLASHFORGE_ADVENTURE_FIVEM_PRO", "LOAD_ONTO_FLASHFORGE_ADVENTURE_FIVEM_PRO", False),
    ("flashforge", "adventurer5m"):    ("EJECT_FROM_FLASHFORGE_ADVENTURE_FIVEM_PRO", "LOAD_ONTO_FLASHFORGE_ADVENTURE_FIVEM_PRO", False),
}

def _lookup(brand: str, model: str):
    key = (_norm(brand).replace("bambu", "bambu_lab") if "bambu" in _norm(brand) else _norm(brand), _norm(model))
    # Try exact key first
    for (b, m), v in _MACRO_MAP.items():
        if _norm(brand) in _norm(b) or _norm(b) in _norm(brand):
            if _norm(m) in _norm(model) or _norm(model) in _norm(m):
                return v
    return (None, None, False)

def get_eject_macro(brand, model):  return _lookup(brand, model)[0]
def get_load_macro(brand, model):   return _lookup(brand, model)[1]
def has_door(brand, model):         return _lookup(brand, model)[2]

def get_close_door_macro(brand, model):
    if not has_door(brand, model):
        return None
    m = _norm(model)
    b = _norm(brand)
    if "x1c" in m or "xonec" in m:       return "CLOSE_DOOR_BAMBULAB_X_ONE_C"
    if "p1s" in m or "p2s" in m:         return "CLOSE_DOOR_BAMBULAB_P_ONE_S"
    if "kobra" in m:                      return "CLOSE_DOOR_ANYCUBIC_KOBRA_S_ONE"
    if "centauri" in m or "carbon" in m:  return "CLOSE_DOOR_ELEGOO_CC"
    if "k1c" in m:                        return "CLOSE_DOOR_CREALITY_K_ONE_C"
    return None

# ── Printer state ──────────────────────────────────────────────────────────────
class PrinterState:
    def __init__(self):
        self.id = None; self.name = ""; self.brand = ""; self.model = ""
        self.ip = ""; self.access_code = ""; self.serial = ""
        self.api_key = ""; self.serial_code = ""; self.check_code = ""
        self.connected = False; self.raw = {}; self.last_seen = 0.0
        self.status = "UNKNOWN"; self.nozzle_temp = None; self.nozzle_target = None
        self.bed_temp = None; self.bed_target = None; self.progress = None
        self.remaining_min = None; self.filename = ""; self.layer_num = None
        self.total_layer_num = None; self.ams = {}; self.print_error = "0"
        self.subtask_id = "0"; self._mqtt = None

    @property
    def protocol(self): return BRANDS.get(self.brand, {}).get("protocol", "unknown")
    @property
    def stale(self): return (time.time() - self.last_seen) > 120 if self.last_seen else True

    def to_dict(self):
        e = get_eject_macro(self.brand, self.model)
        l = get_load_macro(self.brand, self.model)
        d = get_close_door_macro(self.brand, self.model)
        return {
            "id": self.id, "name": self.name, "brand": self.brand, "model": self.model,
            "protocol": self.protocol, "ip": self.ip, "serial": self.serial,
            "connected": self.connected and not self.stale,
            "status": "OFFLINE" if self.stale else self.status,
            "nozzle_temp": self.nozzle_temp, "nozzle_target": self.nozzle_target,
            "bed_temp": self.bed_temp, "bed_target": self.bed_target,
            "progress": self.progress, "remaining_min": self.remaining_min,
            "filename": self.filename, "layer_num": self.layer_num,
            "total_layer_num": self.total_layer_num, "ams": self.ams,
            "print_error": self.print_error, "subtask_id": self.subtask_id,
            "last_seen": self.last_seen,
            "macros": {"eject": e, "load": l, "close_door": d},
            "has_door": d is not None,
        }

# ── Rack state ─────────────────────────────────────────────────────────────────
class RackState:
    def __init__(self):
        self.num_slots = 6
        # slot: {state: "empty"|"ready", label: str, note: str}
        self.slots: list[dict] = [{"state": "empty", "label": "", "note": ""} for _ in range(self.num_slots)]

    def resize(self, n: int):
        n = max(1, min(30, n))
        if n > len(self.slots):
            for _ in range(n - len(self.slots)):
                self.slots.append({"state": "empty", "label": "", "note": ""})
        elif n < len(self.slots):
            self.slots = self.slots[:n]
        self.num_slots = n

    def to_dict(self):
        return {"num_slots": self.num_slots, "slots": self.slots}

rack = RackState()

# ── Global state ───────────────────────────────────────────────────────────────
printers: dict[str, PrinterState] = {}
mqtt_tasks: dict[str, asyncio.Task] = {}
jobs: list[dict] = []
ws_clients: list[WebSocket] = []

# ── Helpers ────────────────────────────────────────────────────────────────────
def _flt(v):
    try: return float(v)
    except: return None

def _seq(): return str(int(time.time() * 1000))[-8:]

def _merge(target, source):
    for k, v in source.items():
        if isinstance(v, dict) and isinstance(target.get(k), dict):
            _merge(target[k], v)
        else:
            target[k] = v

async def broadcast(event, data):
    msg = json.dumps({"event": event, "data": data})
    dead = []
    for ws in ws_clients:
        try: await ws.send_text(msg)
        except: dead.append(ws)
    for ws in dead:
        try: ws_clients.remove(ws)
        except ValueError: pass

# ── Config persistence ─────────────────────────────────────────────────────────
def load_config() -> dict:
    if CONFIG_FILE.exists():
        try: return json.loads(CONFIG_FILE.read_text())
        except: pass
    return {"printers": [], "rack": {"num_slots": 6, "slots": []}}

def save_config():
    cfg = load_config()
    cfg["printers"] = []
    for p in printers.values():
        cfg["printers"].append({
            "id": p.id, "name": p.name, "brand": p.brand, "model": p.model,
            "ip": p.ip, "access_code": p.access_code, "serial": p.serial,
            "api_key": p.api_key, "serial_code": p.serial_code, "check_code": p.check_code,
        })
    cfg["rack"] = rack.to_dict()
    CONFIG_FILE.write_text(json.dumps(cfg, indent=2))

# ── MQTT (Bambu) ───────────────────────────────────────────────────────────────
async def bambu_mqtt_loop(pid: str):
    p = printers.get(pid)
    if not p: return
    tls = ssl.create_default_context()
    tls.check_hostname = False; tls.verify_mode = ssl.CERT_NONE
    report  = f"device/{p.serial}/report"
    request = f"device/{p.serial}/request"
    log.info(f"[{p.name}] MQTT → {p.ip}:8883")
    try:
        async with aiomqtt.Client(
            hostname=p.ip, port=8883, username="bblp", password=p.access_code,
            tls_context=tls, identifier=f"ob-{pid[:6]}-{uuid.uuid4().hex[:6]}", timeout=30,
        ) as client:
            p.connected = True; p._mqtt = client
            await client.subscribe(report, qos=0)
            await client.publish(request,
                json.dumps({"pushing": {"command": "pushall"}, "info": {"command": "get_version"}}), qos=0)
            await broadcast("state", p.to_dict())
            async for message in client.messages:
                if not printers.get(pid): break
                try:
                    payload = json.loads(message.payload.decode())
                    _merge(p.raw, payload)
                    p.last_seen = time.time()
                    pr = p.raw.get("print", {})
                    p.status          = pr.get("gcode_state", "UNKNOWN")
                    p.nozzle_temp     = _flt(pr.get("nozzle_temper"))
                    p.nozzle_target   = _flt(pr.get("nozzle_target_temper"))
                    p.bed_temp        = _flt(pr.get("bed_temper"))
                    p.bed_target      = _flt(pr.get("bed_target_temper"))
                    p.progress        = pr.get("mc_percent")
                    p.remaining_min   = pr.get("mc_remaining_time")
                    p.filename        = pr.get("gcode_file", "")
                    p.layer_num       = pr.get("layer_num")
                    p.total_layer_num = pr.get("total_layer_num")
                    p.print_error     = str(pr.get("print_error", "0"))
                    p.subtask_id      = str(pr.get("subtask_id", "0"))
                    p.ams             = pr.get("ams", {})
                    await broadcast("state", p.to_dict())
                except Exception as e: log.debug(f"[{p.name}] parse: {e}")
    except Exception as e: log.error(f"[{p.name}] MQTT: {e}")
    finally:
        if printers.get(pid): printers[pid].connected = False; printers[pid]._mqtt = None
        await broadcast("state", p.to_dict())

async def moonraker_poll_loop(pid: str):
    p = printers.get(pid)
    if not p: return
    base = f"http://{p.ip}:7125"
    log.info(f"[{p.name}] Moonraker poll {base}")
    _SM = {"printing":"RUNNING","paused":"PAUSED","complete":"FINISH","error":"FAILED",
           "standby":"IDLE","cancelled":"CANCELLED","stopped":"STOPPED"}
    while printers.get(pid):
        try:
            async with httpx.AsyncClient(timeout=8) as c:
                r = await c.get(f"{base}/printer/objects/query?print_stats&virtual_sdcard&extruder&heater_bed")
            if r.status_code == 200:
                s  = r.json().get("result", {}).get("status", {})
                ps = s.get("print_stats", {}); vs = s.get("virtual_sdcard", {})
                ex = s.get("extruder", {});    hb = s.get("heater_bed", {})
                p.status        = _SM.get(ps.get("state", ""), "UNKNOWN")
                p.nozzle_temp   = _flt(ex.get("temperature")); p.nozzle_target = _flt(ex.get("target"))
                p.bed_temp      = _flt(hb.get("temperature")); p.bed_target    = _flt(hb.get("target"))
                p.progress      = round(vs.get("progress", 0) * 100, 1)
                p.filename      = ps.get("filename", "")
                p.layer_num     = ps.get("current_layer"); p.total_layer_num = ps.get("total_layer")
                p.connected     = True; p.last_seen = time.time()
                await broadcast("state", p.to_dict())
        except Exception as e:
            log.debug(f"[{p.name}] poll: {e}")
            if p.connected: p.connected = False; await broadcast("state", p.to_dict())
        await asyncio.sleep(3)

async def prusa_poll_loop(pid: str):
    p = printers.get(pid)
    if not p: return
    base = f"http://{p.ip}"; headers = {"X-Api-Key": p.api_key}
    log.info(f"[{p.name}] PrusaLink poll {base}")
    _SM = {"IDLE":"IDLE","PRINTING":"RUNNING","PAUSED":"PAUSED","FINISHED":"FINISH","ERROR":"FAILED","ATTENTION":"PAUSED"}
    while printers.get(pid):
        try:
            async with httpx.AsyncClient(timeout=8) as c:
                r = await c.get(f"{base}/api/v1/status", headers=headers)
            if r.status_code == 200:
                d = r.json(); job = d.get("job", {}); prn = d.get("printer", {})
                p.status        = _SM.get(prn.get("state", ""), "UNKNOWN")
                p.nozzle_temp   = _flt(prn.get("temp_nozzle")); p.nozzle_target = _flt(prn.get("target_nozzle"))
                p.bed_temp      = _flt(prn.get("temp_bed"));    p.bed_target    = _flt(prn.get("target_bed"))
                p.progress      = _flt(job.get("progress")); p.filename = job.get("file", {}).get("display_name", "")
                p.connected     = True; p.last_seen = time.time()
                await broadcast("state", p.to_dict())
        except Exception as e:
            log.debug(f"[{p.name}] prusa: {e}")
            if p.connected: p.connected = False; await broadcast("state", p.to_dict())
        await asyncio.sleep(3)

async def creality_poll_loop(pid: str):
    p = printers.get(pid)
    if not p: return
    log.info(f"[{p.name}] Creality poll {p.ip}")
    _SM = {0:"IDLE", 1:"RUNNING", 2:"PAUSED", 3:"FINISH", 4:"FAILED"}
    while printers.get(pid):
        try:
            async with httpx.AsyncClient(timeout=8) as c:
                r = await c.get(f"http://{p.ip}/status")
            if r.status_code == 200:
                d = r.json()
                p.status        = _SM.get(d.get("printStatus"), "UNKNOWN")
                p.nozzle_temp   = _flt(d.get("nozzleTemp")); p.nozzle_target = _flt(d.get("nozzleTempTarget"))
                p.bed_temp      = _flt(d.get("bedTemp"));    p.bed_target    = _flt(d.get("bedTempTarget"))
                p.progress      = _flt(d.get("printProgress")); p.filename = d.get("filename", "")
                p.connected     = True; p.last_seen = time.time()
                await broadcast("state", p.to_dict())
        except Exception as e:
            log.debug(f"[{p.name}] creality: {e}")
            if p.connected: p.connected = False; await broadcast("state", p.to_dict())
        await asyncio.sleep(4)

async def flashforge_poll_loop(pid: str):
    p = printers.get(pid)
    if not p: return
    base = f"http://{p.ip}:8898"; auth = {"serialNumber": p.serial_code, "checkCode": p.check_code}
    log.info(f"[{p.name}] FlashForge poll {base}")
    _SM = {"IDLE":"IDLE","PRINTING":"RUNNING","PAUSED":"PAUSED","COMPLETED":"FINISH","ERROR":"FAILED","STOPPED":"STOPPED"}
    while printers.get(pid):
        try:
            async with httpx.AsyncClient(timeout=10) as c:
                r = await c.post(f"{base}/detail", json=auth)
            if r.status_code == 200:
                d = r.json()
                p.status        = _SM.get(d.get("printStatus",""), "UNKNOWN")
                p.nozzle_temp   = _flt(d.get("currentTemp")); p.nozzle_target = _flt(d.get("targetTemp"))
                p.bed_temp      = _flt(d.get("bedCurrentTemp")); p.bed_target  = _flt(d.get("bedTargetTemp"))
                p.progress      = _flt(d.get("printProgress")); p.filename = d.get("printFileName", "")
                p.connected     = True; p.last_seen = time.time()
                await broadcast("state", p.to_dict())
        except Exception as e:
            log.debug(f"[{p.name}] flashforge: {e}")
            if p.connected: p.connected = False; await broadcast("state", p.to_dict())
        await asyncio.sleep(4)

async def start_printer_task(pid: str):
    if pid in mqtt_tasks and not mqtt_tasks[pid].done():
        mqtt_tasks[pid].cancel()
        try: await mqtt_tasks[pid]
        except asyncio.CancelledError: pass
    p = printers.get(pid)
    if not p: return
    loop_map = {
        "mqtt_ftp":        bambu_mqtt_loop,
        "prusalink":       prusa_poll_loop,
        "websocket":       creality_poll_loop,
        "websocket_elegoo":moonraker_poll_loop,
        "http_tcp":        flashforge_poll_loop,
        "moonraker":       moonraker_poll_loop,
    }
    fn = loop_map.get(p.protocol)
    if fn: mqtt_tasks[pid] = asyncio.create_task(fn(pid))
    else: log.warning(f"Unknown protocol {p.protocol}")

# ── FTP (Bambu) ────────────────────────────────────────────────────────────────
def ftp_upload_sync(pid, local_path, remote_name):
    p = printers.get(pid)
    if not p: return False
    ctx = ssl.create_default_context(); ctx.check_hostname = False; ctx.verify_mode = ssl.CERT_NONE
    try:
        with ftplib.FTP_TLS(context=ctx) as ftp:
            ftp.connect(p.ip, 990, timeout=30); ftp.login("bblp", p.access_code); ftp.prot_p()
            with open(local_path, "rb") as f: ftp.storbinary(f"STOR {remote_name}", f)
        return True
    except Exception as e: log.error(f"[{p.name}] FTP: {e}"); return False

async def prusa_upload(pid, local_path, remote_name):
    p = printers.get(pid)
    if not p: return False
    try:
        async with httpx.AsyncClient(timeout=60) as c:
            with open(local_path, "rb") as f:
                r = await c.put(f"http://{p.ip}/api/v1/files/usb/{remote_name}",
                    content=f.read(), headers={"X-Api-Key": p.api_key, "Content-Type":"application/octet-stream"})
        return r.status_code in (200, 201, 204, 409)
    except Exception as e: log.error(f"FTP prusa: {e}"); return False

async def moonraker_upload(pid, local_path, remote_name):
    p = printers.get(pid)
    if not p: return False
    try:
        async with httpx.AsyncClient(timeout=60) as c:
            with open(local_path, "rb") as f:
                r = await c.post(f"http://{p.ip}:7125/server/files/upload",
                    files={"file": (remote_name, f, "application/octet-stream")})
        return r.status_code == 201
    except Exception as e: log.error(f"moonraker upload: {e}"); return False

async def bambu_publish(pid, payload):
    p = printers.get(pid)
    if not p or not p.connected or not p._mqtt: return False
    try:
        await p._mqtt.publish(f"device/{p.serial}/request", json.dumps(payload), qos=0)
        return True
    except Exception as e: log.error(f"publish: {e}"); return False

async def moonraker_gcode(pid, script):
    p = printers.get(pid)
    if not p: return False
    try:
        async with httpx.AsyncClient(timeout=10) as c:
            r = await c.post(f"http://{p.ip}:7125/printer/gcode/script", json={"script": script})
        return r.status_code == 200
    except Exception as e: log.error(f"gcode: {e}"); return False

# ── Lifespan ───────────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    cfg = load_config()
    for pr in cfg.get("printers", []):
        pid = pr.get("id") or uuid.uuid4().hex[:8]
        p = PrinterState(); p.id = pid; p._mqtt = None
        p.name = pr.get("name",""); p.brand = pr.get("brand",""); p.model = pr.get("model","")
        p.ip = pr.get("ip",""); p.access_code = pr.get("access_code",""); p.serial = pr.get("serial","")
        p.api_key = pr.get("api_key",""); p.serial_code = pr.get("serial_code",""); p.check_code = pr.get("check_code","")
        printers[pid] = p
        if p.ip: asyncio.create_task(start_printer_task(pid))
    rc = cfg.get("rack", {})
    if rc.get("num_slots"): rack.resize(rc["num_slots"])
    for i, s in enumerate(rc.get("slots", [])):
        if i < len(rack.slots): rack.slots[i] = s
    yield
    for t in mqtt_tasks.values():
        if not t.done(): t.cancel()

app = FastAPI(title="OttoBridge v2", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")

# ── Pydantic models ────────────────────────────────────────────────────────────
class PrinterCfg(BaseModel):
    id: Optional[str] = None
    name: str; brand: str; model: str; ip: str
    access_code: str = ""; serial: str = ""; api_key: str = ""
    serial_code: str = ""; check_code: str = ""

class StartPrint(BaseModel):
    printer_id: str; filename: str; use_ams: bool = False
    ams_map: list[int] = Field(default_factory=lambda: [0])
    bed_type: str = "textured_plate"; bed_level: bool = True
    flow_cali: bool = True; vibr_cali: bool = True
    layer_inspect: bool = True; timelapse: bool = False
    grab_slot: Optional[int] = None   # rack slot to grab plate from before print
    park_slot: Optional[int] = None   # rack slot to park plate after print

class PidOnly(BaseModel):
    printer_id: str

class GcodeReq(BaseModel):
    printer_id: str; command: str

class MacroReq(BaseModel):
    printer_id: str = ""; macro: str

class SlotUpdate(BaseModel):
    slot_index: int; state: str; label: str = ""; note: str = ""

class RackConfig(BaseModel):
    num_slots: int

# ── Printer routes ─────────────────────────────────────────────────────────────
@app.get("/api/brands")
async def get_brands(): return BRANDS

@app.get("/api/printers")
async def list_printers(): return [p.to_dict() for p in printers.values()]

@app.post("/api/printers")
async def add_printer(cfg: PrinterCfg):
    pid = cfg.id or uuid.uuid4().hex[:8]
    p = PrinterState(); p.id = pid; p._mqtt = None
    p.name = cfg.name; p.brand = cfg.brand; p.model = cfg.model; p.ip = cfg.ip
    p.access_code = cfg.access_code; p.serial = cfg.serial; p.api_key = cfg.api_key
    p.serial_code = cfg.serial_code; p.check_code = cfg.check_code
    printers[pid] = p; save_config()
    await start_printer_task(pid)
    return p.to_dict()

@app.put("/api/printers/{pid}")
async def update_printer(pid: str, cfg: PrinterCfg):
    if pid not in printers: raise HTTPException(404)
    p = printers[pid]
    p.name = cfg.name; p.brand = cfg.brand; p.model = cfg.model; p.ip = cfg.ip
    p.access_code = cfg.access_code; p.serial = cfg.serial; p.api_key = cfg.api_key
    p.serial_code = cfg.serial_code; p.check_code = cfg.check_code
    save_config(); await start_printer_task(pid); return p.to_dict()

@app.delete("/api/printers/{pid}")
async def delete_printer(pid: str):
    if pid in mqtt_tasks and not mqtt_tasks[pid].done(): mqtt_tasks[pid].cancel()
    printers.pop(pid, None); save_config(); return {"ok": True}

# ── Print control ─────────────────────────────────────────────────────────────
@app.post("/api/print/start")
async def start_print(req: StartPrint):
    p = printers.get(req.printer_id)
    if not p: raise HTTPException(404)
    if p.protocol == "mqtt_ftp":
        ok = await bambu_publish(req.printer_id, {"print": {
            "command":"project_file","sequence_id":_seq(),
            "file":req.filename,"url":f"ftp:///{req.filename}","param":"",
            "bed_type":req.bed_type,"bed_leveling":req.bed_level,
            "flow_cali":req.flow_cali,"vibration_cali":req.vibr_cali,
            "layer_inspect":req.layer_inspect,"use_ams":req.use_ams,
            "ams_mapping":req.ams_map,"timelapse":req.timelapse,
            "task_id":_seq(),"subtask_id":"0","project_id":"0","profile_id":"0",
            "subtask_name":Path(req.filename).name,"project_name":Path(req.filename).name,
        }})
    elif p.protocol == "prusalink":
        try:
            async with httpx.AsyncClient(timeout=10) as c:
                r = await c.post(f"http://{p.ip}/api/v1/print",
                    headers={"X-Api-Key":p.api_key}, json={"path":f"/usb/{req.filename}"})
            ok = r.status_code in (200,201,204)
        except Exception as e: raise HTTPException(503, str(e))
    elif p.protocol in ("moonraker","websocket","websocket_elegoo"):
        ok = await moonraker_gcode(req.printer_id, f"SDCARD_PRINT_FILE FILENAME={req.filename}")
    elif p.protocol == "http_tcp":
        try:
            async with httpx.AsyncClient(timeout=10) as c:
                r = await c.post(f"http://{p.ip}:8898/print",
                    json={"serialNumber":p.serial_code,"checkCode":p.check_code,"filename":req.filename})
            ok = r.status_code == 200
        except Exception as e: raise HTTPException(503, str(e))
    else: raise HTTPException(501, f"Not implemented: {p.protocol}")
    if not ok: raise HTTPException(503, "Failed")
    return {"ok": True}

async def _ctrl(pid, cmd):
    p = printers.get(pid)
    if not p: raise HTTPException(404)
    if p.protocol == "mqtt_ftp":
        return await bambu_publish(pid, {"print":{"command":cmd,"param":"","sequence_id":_seq()}})
    elif p.protocol == "prusalink":
        cm = {"pause":"PAUSE","resume":"RESUME","stop":"CANCEL"}.get(cmd, cmd.upper())
        async with httpx.AsyncClient(timeout=10) as c:
            r = await c.put(f"http://{p.ip}/api/v1/job",
                headers={"X-Api-Key":p.api_key}, json={"command":cm})
        return r.status_code in (200,204)
    elif p.protocol in ("moonraker","websocket","websocket_elegoo"):
        gc = {"pause":"PAUSE","resume":"RESUME","stop":"CANCEL_PRINT"}.get(cmd, cmd.upper())
        return await moonraker_gcode(pid, gc)
    return False

@app.post("/api/print/pause")
async def pause(req: PidOnly): return {"ok": await _ctrl(req.printer_id,"pause")}
@app.post("/api/print/resume")
async def resume(req: PidOnly): return {"ok": await _ctrl(req.printer_id,"resume")}
@app.post("/api/print/stop")
async def stop(req: PidOnly): return {"ok": await _ctrl(req.printer_id,"stop")}
@app.post("/api/print/clear_error")
async def clear_err(req: PidOnly):
    p = printers.get(req.printer_id)
    if not p: raise HTTPException(404)
    if p.protocol == "mqtt_ftp":
        ok = await bambu_publish(req.printer_id, {"print":{
            "command":"clean_print_error","sequence_id":_seq(),
            "subtask_id":p.subtask_id,"print_error":int(p.print_error or 0)}})
        return {"ok": ok}
    return {"ok": False}

@app.post("/api/gcode")
async def gcode(req: GcodeReq):
    p = printers.get(req.printer_id)
    if not p: raise HTTPException(404)
    if p.protocol == "mqtt_ftp":
        ok = await bambu_publish(req.printer_id,
            {"print":{"command":"gcode_line","sequence_id":_seq(),"param":req.command}})
    else:
        ok = await moonraker_gcode(req.printer_id, req.command)
    return {"ok": ok}

# ── File routes ────────────────────────────────────────────────────────────────
@app.post("/api/upload/{pid}")
async def upload(pid: str, file: UploadFile = File(...)):
    p = printers.get(pid)
    if not p: raise HTTPException(404)
    if not file.filename.endswith((".3mf",".gcode")): raise HTTPException(400,"Only .3mf/.gcode")
    dest = UPLOAD_DIR / file.filename
    async with aiofiles.open(dest, "wb") as f:
        while chunk := await file.read(65536): await f.write(chunk)
    loop = asyncio.get_event_loop()
    if p.protocol == "mqtt_ftp":
        ok = await loop.run_in_executor(None, ftp_upload_sync, pid, str(dest), file.filename)
    elif p.protocol == "prusalink":
        ok = await prusa_upload(pid, str(dest), file.filename)
    elif p.protocol in ("moonraker","websocket","websocket_elegoo"):
        ok = await moonraker_upload(pid, str(dest), file.filename)
    else: ok = True
    if not ok: raise HTTPException(502, "Upload to printer failed")
    return {"ok": True, "filename": file.filename}

@app.get("/api/files")
async def list_files():
    return [{"name":fp.name,"size":fp.stat().st_size,"mtime":fp.stat().st_mtime}
            for fp in sorted(UPLOAD_DIR.iterdir()) if fp.suffix in (".3mf",".gcode")]

@app.delete("/api/files/{filename}")
async def del_file(filename: str):
    p = UPLOAD_DIR / filename
    if p.exists(): await aiofiles.os.remove(p)
    return {"ok": True}

# ── Job routes ─────────────────────────────────────────────────────────────────
@app.get("/api/jobs")
async def get_jobs(): return jobs

@app.post("/api/jobs")
async def add_job(req: StartPrint):
    job = {"id": uuid.uuid4().hex[:8], "status":"queued", **req.model_dump()}
    jobs.append(job); await broadcast("job_added", job); return job

@app.delete("/api/jobs/{jid}")
async def del_job(jid: str):
    global jobs; jobs = [j for j in jobs if j["id"] != jid]; return {"ok": True}

# ── Rack routes ────────────────────────────────────────────────────────────────
@app.get("/api/rack")
async def get_rack(): return rack.to_dict()

@app.put("/api/rack/config")
async def set_rack_config(cfg: RackConfig):
    rack.resize(cfg.num_slots); save_config(); return rack.to_dict()

@app.put("/api/rack/slot")
async def update_slot(req: SlotUpdate):
    if req.slot_index < 0 or req.slot_index >= rack.num_slots:
        raise HTTPException(400, "Invalid slot index")
    rack.slots[req.slot_index] = {"state": req.state, "label": req.label, "note": req.note}
    save_config()
    await broadcast("rack", rack.to_dict())
    return rack.to_dict()

# ── OttoEject routes ──────────────────────────────────────────────────────────
async def _run_klipper(macro: str):
    try:
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.post(f"{MOONRAKER_URL}/printer/gcode/script", json={"script": macro})
        return {"ok": r.status_code == 200, "macro": macro}
    except Exception as e: raise HTTPException(503, f"Moonraker: {e}")

@app.post("/api/ottoeject/eject/{pid}")
async def ej_eject(pid: str):
    p = printers.get(pid)
    if not p: raise HTTPException(404)
    m = get_eject_macro(p.brand, p.model)
    if not m: raise HTTPException(422, f"No eject macro for {p.brand} {p.model}")
    return await _run_klipper(m)

@app.post("/api/ottoeject/load/{pid}")
async def ej_load(pid: str):
    p = printers.get(pid)
    if not p: raise HTTPException(404)
    m = get_load_macro(p.brand, p.model)
    if not m: raise HTTPException(422, f"No load macro for {p.brand} {p.model}")
    return await _run_klipper(m)

@app.post("/api/ottoeject/close_door/{pid}")
async def ej_door(pid: str):
    p = printers.get(pid)
    if not p: raise HTTPException(404)
    m = get_close_door_macro(p.brand, p.model)
    if not m: raise HTTPException(422, f"No door macro for {p.brand} {p.model}")
    return await _run_klipper(m)

@app.post("/api/ottoeject/grab_slot/{slot}")
async def grab_slot(slot: int):
    """GRAB_FROM_SLOT_N → marks slot as empty"""
    if slot < 1 or slot > rack.num_slots: raise HTTPException(400, "Invalid slot")
    result = await _run_klipper(f"GRAB_FROM_SLOT_{slot}")
    if result.get("ok"):
        idx = slot - 1
        rack.slots[idx] = {"state": "empty", "label": "", "note": ""}
        save_config(); await broadcast("rack", rack.to_dict())
    return result

@app.post("/api/ottoeject/store_slot/{slot}")
async def store_slot(slot: int):
    """STORE_TO_SLOT_N → marks slot as ready"""
    if slot < 1 or slot > rack.num_slots: raise HTTPException(400, "Invalid slot")
    result = await _run_klipper(f"STORE_TO_SLOT_{slot}")
    if result.get("ok"):
        idx = slot - 1
        rack.slots[idx] = {"state": "ready", "label": "Platte drin", "note": ""}
        save_config(); await broadcast("rack", rack.to_dict())
    return result

@app.post("/api/ottoeject/macro")
async def run_macro(req: MacroReq):
    return await _run_klipper(req.macro)

@app.get("/api/ottoeject/status")
async def klipper_status():
    try:
        async with httpx.AsyncClient(timeout=5) as c:
            r = await c.get(f"{MOONRAKER_URL}/printer/info")
        return r.json()
    except Exception as e: return {"error": str(e)}

# ── WebSocket ──────────────────────────────────────────────────────────────────
@app.websocket("/ws")
async def ws_ep(ws: WebSocket):
    await ws.accept(); ws_clients.append(ws)
    await ws.send_text(json.dumps({"event":"init","data":{
        "printers": [p.to_dict() for p in printers.values()],
        "rack": rack.to_dict(),
    }}))
    try:
        while True: await ws.receive_text()
    except WebSocketDisconnect:
        try: ws_clients.remove(ws)
        except ValueError: pass

@app.get("/", response_class=HTMLResponse)
async def root(): return (BASE_DIR / "static" / "index.html").read_text()
