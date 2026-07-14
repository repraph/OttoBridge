"""
OttoBridge v2 — Multi-Printer Orchestrator
Supports: Bambu Lab (X1C, P1S, P1P, A1, P2S), Prusa (MK3/MK4/Core One),
          Creality (K1C), Anycubic (Kobra S1), Elegoo (Centauri Carbon),
          FlashForge (AD5X, Adventurer 5M Pro), Klipper/Moonraker (generic)
Pi Zero 2 W — runs alongside Klipper + Moonraker
"""

import asyncio, ftplib, json, logging, os, re, ssl, time, uuid, zipfile, io
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

import aiomqtt
import aiofiles, aiofiles.os
import httpx
import websockets
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
        # NOTE: requires the Rinkhals custom firmware overlay (jbatonnet/Rinkhals).
        # Rinkhals installs a real Moonraker instance on top of stock Anycubic
        # firmware (non-destructive), which is what this protocol talks to.
        # Native/stock Anycubic firmware has NO usable local API for this.
        "protocol": "moonraker",
        "models": ["Kobra S1"],
        "auth_fields": ["ip"],
        "start_grace_s": 720,
        "requires_custom_firmware": "Rinkhals (jbatonnet/Rinkhals)",
    },
    "elegoo": {
        "label": "Elegoo",
        # Native stock firmware — talks SDCP v3 directly over WebSocket (port 3030).
        # No custom firmware needed, no auth handshake required by the printer.
        "protocol": "sdcp_ws",
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

def get_open_door_macro(brand, model):
    """NOTE: the community configs only define a single generic _OPEN_DOOR
    macro (internal, prefixed with underscore) — there is no per-printer
    OPEN_DOOR_<PRINTER> macro like there is for CLOSE_DOOR_<PRINTER>.
    _OPEN_DOOR reads its door_x_start/door_y_start/door_z_engage/door_d_to_pin_dist
    variables via SET_GCODE_VARIABLE calls that live inside each printer's
    EJECT_FROM_<PRINTER> macro — so calling _OPEN_DOOR standalone before any
    EJECT_FROM_<PRINTER> call has run will use stale/unset variables.
    Until the configs expose a proper per-printer OPEN_DOOR_<PRINTER> macro,
    this returns None and the queue runner falls back to skipping the
    explicit pre-open-door step (the printer's own EJECT_FROM_<PRINTER>
    macro already opens the door as part of every later cycle)."""
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
        self.subtask_id = "0"; self._mqtt = None; self._ws = None

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
SLOT_GAP_MM = 55  # matches storage_calibration_variables.cfg global_slot_gap + offset

class RackState:
    """
    Slot states:
      empty          - no plate, nothing in this slot
      ready          - plate present, available to grab
      grab_reserved  - plate is being picked up for an active job
      park           - this is the bottom slot of an active/printed job (has plate + print)
      printed        - job finished, plate+print sit here, awaiting removal

    Print overlays describe a print that spans 1+ slots above its park (bottom) slot.
    Slots covered by an overlay (other than the bottom park slot) have NO plate and are
    blocked for new "ready" plates until the user clears the overlay (clear_print).
    """
    def __init__(self):
        self.num_slots = 6
        self.slots: list[dict] = [{"state": "empty", "label": "", "note": "", "job_id": None}
                                   for _ in range(self.num_slots)]
        # overlays: [{job_id, bottom_slot(0-idx), slots_needed, height_mm, file, done}]
        self.overlays: list[dict] = []

    def resize(self, n: int):
        n = max(1, min(30, n))
        if n > len(self.slots):
            for _ in range(n - len(self.slots)):
                self.slots.append({"state": "empty", "label": "", "note": "", "job_id": None})
        elif n < len(self.slots):
            self.slots = self.slots[:n]
        self.num_slots = n

    def blocked_indices(self) -> set[int]:
        """Slot indices blocked by active print overlays (cannot insert a plate)."""
        blocked = set()
        for ov in self.overlays:
            for k in range(1, ov["slots_needed"]):
                idx = ov["bottom_slot"] + k
                if idx < self.num_slots: blocked.add(idx)
            # If the print overflows its allocated slots, block one extra slot above
            if ov["height_mm"] > ov["slots_needed"] * SLOT_GAP_MM:
                idx = ov["bottom_slot"] + ov["slots_needed"]
                if idx < self.num_slots: blocked.add(idx)
        return blocked

    def check_park(self, bottom_slot: int, slots_needed: int, grab_slot: Optional[int] = None) -> Optional[str]:
        """Validate a park assignment. Returns an error string, or None if OK.
        Slot 6 (last slot) is open-topped: prints can extend above it without limit.
        A 'ready' slot (existing plate) is only a valid park target if it's the
        same slot being grabbed from (pick up, print, store back in place)."""
        if bottom_slot < 0 or bottom_slot >= self.num_slots:
            return "Invalid slot"
        blocked = self.blocked_indices()
        last = self.num_slots - 1
        for k in range(slots_needed):
            idx = bottom_slot + k
            if idx > last:
                # Above the top slot: open rack, always OK
                continue
            if idx in blocked:
                return f"Slot {idx+1} is blocked by another print"
            s = self.slots[idx]
            if k == 0:
                if s["state"] == "ready" and idx != grab_slot:
                    return f"Slot {idx+1} already has a plate"
                if s["state"] not in ("empty", "ready", "grab_reserved"):
                    return f"Slot {idx+1} is not available"
            else:
                if s["state"] != "empty":
                    return f"Slot {idx+1} is occupied"
        return None

    def reserve_for_job(self, job_id: str, grab_slot: Optional[int], bottom_slot: int,
                         slots_needed: int, height_mm: float, filename: str):
        if grab_slot is not None:
            self.slots[grab_slot] = {"state": "grab_reserved", "label": filename,
                                      "note": "", "job_id": job_id}
        self.slots[bottom_slot] = {"state": "park", "label": filename,
                                    "note": "", "job_id": job_id}
        self.overlays.append({"job_id": job_id, "bottom_slot": bottom_slot,
                               "slots_needed": slots_needed, "height_mm": height_mm,
                               "file": filename, "done": False})

    def free_job_slots(self, job_id: str):
        """Free grab + park slots for a job (used on abort/skip/stop).
        Removes the print overlay entirely so blocked slots above become available."""
        for i, s in enumerate(self.slots):
            if s.get("job_id") == job_id:
                self.slots[i] = {"state": "empty", "label": "", "note": "", "job_id": None}
        self.overlays = [o for o in self.overlays if o["job_id"] != job_id]

    def mark_printed(self, job_id: str):
        """Mark the bottom (park) slot as printed; overlay stays (still blocks upper slots)
        until the user clears the print via clear_print()."""
        for i, s in enumerate(self.slots):
            if s.get("job_id") == job_id and s["state"] == "park":
                self.slots[i]["state"] = "printed"
        for ov in self.overlays:
            if ov["job_id"] == job_id: ov["done"] = True

    def clear_print(self, slot_index: int):
        """User confirms the finished print + plate were physically removed.
        Frees the printed slot AND any slots blocked by its overlay."""
        s = self.slots[slot_index]
        job_id = s.get("job_id")
        self.slots[slot_index] = {"state": "empty", "label": "", "note": "", "job_id": None}
        if job_id:
            self.overlays = [o for o in self.overlays if o["job_id"] != job_id]
            # also clear any other slots tagged with this job_id (shouldn't normally happen)
            for i, s2 in enumerate(self.slots):
                if s2.get("job_id") == job_id:
                    self.slots[i] = {"state": "empty", "label": "", "note": "", "job_id": None}

    def free_grab_slot(self, slot_index: int):
        """Manually free a grab_reserved slot (plate was already picked, slot is empty)."""
        s = self.slots[slot_index]
        if s["state"] == "grab_reserved":
            self.slots[slot_index] = {"state": "empty", "label": "", "note": "", "job_id": None}

    def to_dict(self):
        return {"num_slots": self.num_slots, "slots": self.slots,
                "overlays": self.overlays, "blocked": sorted(self.blocked_indices())}

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
    cfg["jobs"] = jobs
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
                    if p.status == "IDLE":
                        # p.raw is a cumulative merge of every partial MQTT update
                        # we've ever received (_merge never deletes stale keys),
                        # and Bambu doesn't reliably re-send layer_num/progress/etc.
                        # as 0 once a print ends — so without this, the dashboard
                        # keeps showing the previous print's layer count and
                        # progress indefinitely even though nothing is printing.
                        p.layer_num = p.total_layer_num = p.progress = p.remaining_min = None
                        p.filename = ""
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

def _sdcp_msg(cmd: int, data: dict, mainboard_id: str = "") -> str:
    """Build an SDCP request envelope per cbd-tech spec (Cmd/RequestID/MainboardID/TimeStamp/From)."""
    return json.dumps({
        "Id": uuid.uuid4().hex, "Data": {
            "Cmd": cmd, "Data": data, "RequestID": uuid.uuid4().hex,
            "MainboardID": mainboard_id, "TimeStamp": int(time.time()), "From": 0,
        },
    })

async def elegoo_sdcp_loop(pid: str):
    """Native Elegoo Centauri Carbon — SDCP v3 over WebSocket, port 3030.
    No auth/handshake required. Official protocol spec:
    github.com/cbd-tech/SDCP-Smart-Device-Control-Protocol-V3.0.0
    FDM-specific PrintInfo.Status codes cross-checked against a real Centauri
    Carbon capture: github.com/WalkerFrederick/sdcp-centauri-carbon (the official
    spec's enum is written for CBD's resin printers — HOMING/DROPPING/EXPOSURING/
    LIFTING don't apply to an FDM machine, so we use the FDM-observed codes below).
    Keeps the websocket on p._ws so control commands (start/pause/stop) can reuse
    the same connection via elegoo_sdcp_send() instead of reconnecting each time.
    """
    p = printers.get(pid)
    if not p: return
    uri = f"ws://{p.ip}:3030/websocket"
    log.info(f"[{p.name}] SDCP → {uri}")
    _SM = {0: "IDLE", 5: "PAUSING", 8: "PREPARE", 9: "RUNNING",
           10: "PAUSED", 13: "RUNNING", 20: "RESUMING"}
    while printers.get(pid):
        try:
            async with websockets.connect(uri, open_timeout=10, ping_interval=20, ping_timeout=20) as ws:
                p.connected = True; p.last_seen = time.time(); p._ws = ws
                await broadcast("state", p.to_dict())
                await ws.send(_sdcp_msg(0, {}))  # Cmd 0: request status refresh
                async for raw in ws:
                    if not printers.get(pid): break
                    try:
                        msg = json.loads(raw)
                        status = msg.get("Status")
                        if not status:
                            continue  # response/attributes/error/notice frames — ignore here
                        pi = status.get("PrintInfo", {})
                        p.status          = _SM.get(pi.get("Status"), "UNKNOWN")
                        p.nozzle_temp     = _flt(status.get("TempOfNozzle"))
                        p.nozzle_target   = _flt(status.get("TempTargetNozzle"))
                        p.bed_temp        = _flt(status.get("TempOfHotbed"))
                        p.bed_target      = _flt(status.get("TempTargetHotbed"))
                        p.layer_num       = pi.get("CurrentLayer")
                        p.total_layer_num = pi.get("TotalLayer")
                        if pi.get("TotalTicks"):
                            p.progress = round(pi.get("CurrentTicks", 0) / pi["TotalTicks"] * 100, 1)
                        p.filename        = pi.get("Filename", p.filename)
                        p.print_error     = str(pi.get("ErrorNumber", "0"))
                        p.last_seen       = time.time()
                        await broadcast("state", p.to_dict())
                    except Exception as e:
                        log.debug(f"[{p.name}] sdcp parse: {e}")
        except Exception as e:
            log.debug(f"[{p.name}] sdcp: {e}")
        finally:
            if printers.get(pid):
                printers[pid].connected = False; printers[pid]._ws = None
                await broadcast("state", p.to_dict())
        await asyncio.sleep(5)

async def elegoo_sdcp_send(pid: str, cmd: int, data: dict) -> bool:
    """Send an SDCP control command (128 start / 129 pause / 130 stop / 131 resume)
    over the persistent connection kept open by elegoo_sdcp_loop."""
    p = printers.get(pid)
    if not p or not p._ws: return False
    try:
        await p._ws.send(_sdcp_msg(cmd, data)); return True
    except Exception as e:
        log.error(f"[{p.name}] sdcp send: {e}"); return False

async def elegoo_sdcp_upload(pid: str, local_path: str, remote_name: str) -> bool:
    """Native Elegoo file upload — dedicated HTTP endpoint on port 3030, NOT the
    websocket. Sent as 1MB multipart/form-data chunks per the official spec:
    POST http://<ip>:3030/uploadFile/upload
      S-File-MD5, Check, Offset, Uuid (same for every chunk), TotalSize, File
    """
    import hashlib
    p = printers.get(pid)
    if not p: return False
    path = Path(local_path)
    data = path.read_bytes()
    md5 = hashlib.md5(data).hexdigest()
    file_uuid = uuid.uuid4().hex
    total = len(data)
    chunk_size = 1024 * 1024
    try:
        async with httpx.AsyncClient(timeout=30) as c:
            for offset in range(0, total, chunk_size):
                chunk = data[offset:offset + chunk_size]
                r = await c.post(
                    f"http://{p.ip}:3030/uploadFile/upload",
                    data={"S-File-MD5": md5, "Check": "1", "Offset": str(offset),
                          "Uuid": file_uuid, "TotalSize": str(total)},
                    files={"File": (remote_name, chunk, "application/octet-stream")},
                )
                if r.status_code != 200 or not r.json().get("success", False):
                    log.error(f"[{p.name}] sdcp upload chunk @{offset}: {r.text[:200]}")
                    return False
        return True
    except Exception as e:
        log.error(f"[{p.name}] sdcp upload: {e}"); return False

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
        "sdcp_ws":         elegoo_sdcp_loop,
        "http_tcp":        flashforge_poll_loop,
        "moonraker":       moonraker_poll_loop,   # Anycubic KS1 requires Rinkhals installed
    }
    fn = loop_map.get(p.protocol)
    if fn: mqtt_tasks[pid] = asyncio.create_task(fn(pid))
    else: log.warning(f"Unknown protocol {p.protocol}")

# ── FTP (Bambu) ────────────────────────────────────────────────────────────────
class ImplicitFTP_TLS(ftplib.FTP_TLS):
    """Bambu Lab printers speak IMPLICIT FTPS on port 990 (TLS from the very
    first byte). Stock ftplib.FTP_TLS only supports EXPLICIT FTPS (plaintext
    connect, then an AUTH TLS command on port 21) — calling .connect() on
    port 990 with the plain class makes it wait for a plaintext welcome
    banner that never arrives, which is exactly the connect-time timeout we
    hit initially. This subclass wraps the socket in TLS the moment it's
    set, before ftplib tries to read anything from it.

    This implementation (sock property/setter, ntransfercmd, and critically
    the storbinary override below) is taken directly from the proven-working
    `bambulabs_api` package (BambuTools/bambulabs_api, ftp_client.py) after
    confirming empirically that OUR previous version — which relied on the
    *inherited* stock ftplib.FTP_TLS.storbinary() — hung indefinitely
    waiting for the final "226 Transfer complete" reply after a fully
    successful data transfer (confirmed via wire-level debug logging: login,
    PASV, STOR, and the data-channel TLS handshake all completed fine; only
    the post-transfer control-channel read hung). The stock storbinary()
    closes the data socket via a `with conn:` context manager; overriding it
    to explicitly `conn.close()` in a `finally` block before calling
    self.voidresp() is what actually fixes it — Bambu's embedded FTP server
    apparently doesn't send its final response until the data connection is
    closed in a way the `with`-based teardown timing doesn't reliably
    trigger. This is a known, documented Bambu-firmware-specific quirk
    (several independent Bambu automation projects hit and fixed the exact
    same thing)."""
    def __init__(self, *args, unwrap: bool = False, **kwargs):
        super().__init__(*args, **kwargs)
        self._sock = None
        self.unwrap = unwrap

    @property
    def sock(self):
        return self._sock

    @sock.setter
    def sock(self, value):
        if value is not None and not isinstance(value, ssl.SSLSocket):
            value = self.context.wrap_socket(value)
        self._sock = value

    def ntransfercmd(self, cmd, rest=None):
        conn, size = ftplib.FTP.ntransfercmd(self, cmd, rest)
        if self._prot_p:
            conn = self.context.wrap_socket(conn, server_hostname=self.host, session=self.sock.session)
        return conn, size

    def storbinary(self, cmd, fp, blocksize=8192, callback=None, rest=None):
        self.voidcmd("TYPE I")
        conn = self.transfercmd(cmd, rest)
        try:
            while True:
                buf = fp.read(blocksize)
                if not buf:
                    break
                conn.sendall(buf)
                if callback:
                    callback(buf)
            if isinstance(conn, ssl.SSLSocket) and self.unwrap:
                conn.unwrap()
        finally:
            conn.close()
        return self.voidresp()

def _bambu_ftp_ctx() -> ssl.SSLContext:
    """SSL context for talking to a Bambu printer's self-signed FTPS cert.
    OP_IGNORE_UNEXPECTED_EOF works around Python 3.11+'s stricter TLS
    shutdown handling — some Bambu firmware versions close the TLS
    connection without a proper close_notify, which newer Python otherwise
    surfaces as ssl.SSLEOFError."""
    ctx = ssl.create_default_context()
    ctx.check_hostname = False; ctx.verify_mode = ssl.CERT_NONE
    if hasattr(ssl, "OP_IGNORE_UNEXPECTED_EOF"):
        ctx.options |= ssl.OP_IGNORE_UNEXPECTED_EOF
    return ctx

def ftp_upload_sync(pid, local_path, remote_name):
    p = printers.get(pid)
    if not p: return False
    ctx = _bambu_ftp_ctx()
    try:
        with ImplicitFTP_TLS(context=ctx) as ftp:
            ftp.connect(p.ip, 990, timeout=30)
            ftp.login("bblp", p.access_code)
            ftp.prot_p()
            with open(local_path, "rb") as f: ftp.storbinary(f"STOR {remote_name}", f)
            # NOOP confirms the server has fully acknowledged the transfer before
            # we close the TCP session. Without this the X1C firmware may still be
            # writing to its internal storage when the connection drops, causing
            # a 0500-4003 "cannot process file" error on the next project_file command.
            ftp.voidcmd("NOOP")
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

# ── System stats (footer) ───────────────────────────────────────────────────
# Pulls host metrics from the Moonraker instance running alongside the
# OTTOeject's Klipper board — exactly the same data source Mainsail's own
# "System" panel uses, so no extra agent/psutil setup needed on the Pi.
_last_net = {}          # {iface: (bytes, ts)} for throughput deltas
_disk_cache = {"total": None, "used": None, "ts": 0}

async def _fetch_disk_usage():
    """Moonraker exposes filesystem usage on /server/files/directory. Cached
    and refreshed only every 30s — it's slow-changing and no need to hammer it
    on every 2s tick."""
    if time.time() - _disk_cache["ts"] < 30:
        return _disk_cache["total"], _disk_cache["used"]
    try:
        async with httpx.AsyncClient(timeout=5) as c:
            r = await c.get(f"{MOONRAKER_URL}/server/files/directory", params={"path": "gcodes"})
            du = r.json().get("result", {}).get("disk_usage", {})
            _disk_cache.update(total=du.get("total"), used=du.get("used"), ts=time.time())
    except Exception as e:
        log.debug(f"disk_usage: {e}")
    return _disk_cache["total"], _disk_cache["used"]

async def system_stats_loop():
    while True:
        try:
            async with httpx.AsyncClient(timeout=5) as c:
                r = await c.get(f"{MOONRAKER_URL}/machine/proc_stats")
                res = r.json().get("result", {})
            cpu = res.get("system_cpu_usage", {}).get("cpu")
            mem = res.get("system_memory", {})
            mem_total = mem.get("total"); mem_used = (mem_total or 0) - mem.get("available", 0)
            temp = res.get("cpu_temp")
            uptime_s = res.get("system_uptime")

            now = time.time(); rx_rate = tx_rate = 0.0
            for iface, n in (res.get("network") or {}).items():
                rx, tx = n.get("rx_bytes", 0), n.get("tx_bytes", 0)
                prev = _last_net.get(iface)
                if prev:
                    dt = now - prev[2]
                    if dt > 0:
                        rx_rate += max(0, rx - prev[0]) / dt
                        tx_rate += max(0, tx - prev[1]) / dt
                _last_net[iface] = (rx, tx, now)

            disk_total, disk_used = await _fetch_disk_usage()

            await broadcast("system_stats", {
                "cpu_pct":     round(cpu, 1) if cpu is not None else None,
                "mem_pct":     round(mem_used / mem_total * 100, 1) if mem_total else None,
                "mem_used_mb": round(mem_used / 1024, 0) if mem_used else None,
                "disk_pct":    round(disk_used / disk_total * 100, 1) if disk_total else None,
                "temp_c":      round(temp, 1) if temp is not None else None,
                "uptime_s":    uptime_s,
                "rx_kbps":     round(rx_rate / 1024, 1),
                "tx_kbps":     round(tx_rate / 1024, 1),
                "connected":   True,
            })
        except Exception as e:
            log.debug(f"system_stats: {e}")
            await broadcast("system_stats", {"connected": False})
        await asyncio.sleep(2)

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
    asyncio.create_task(system_stats_loop())
    rc = cfg.get("rack", {})
    if rc.get("num_slots"): rack.resize(rc["num_slots"])
    for i, s in enumerate(rc.get("slots", [])):
        if i < len(rack.slots):
            s.setdefault("job_id", None)
            rack.slots[i] = s
    rack.overlays = rc.get("overlays", [])

    global jobs
    jobs = cfg.get("jobs", [])
    # A job that was 'running' when the server stopped is now stale —
    # the queue automation task no longer exists, so put it back to 'queued'
    # so it can be safely retried (its rack reservation is untouched).
    for j in jobs:
        if j.get("status") == "running":
            j["status"] = "queued"

    # Auto-heal orphaned rack reservations: any overlay or slot referencing a
    # job_id that no longer exists in `jobs` (e.g. from a server restart that
    # happened before job persistence was added) is cleared automatically,
    # instead of leaving a permanently stuck slot the user has to fix by hand.
    valid_job_ids = {j["id"] for j in jobs}
    orphaned_overlays = [o for o in rack.overlays if o.get("job_id") not in valid_job_ids]
    if orphaned_overlays:
        orphaned_ids = {o["job_id"] for o in orphaned_overlays}
        rack.overlays = [o for o in rack.overlays if o.get("job_id") in valid_job_ids]
        for s in rack.slots:
            if s.get("job_id") in orphaned_ids:
                s["state"] = "empty"; s["label"] = ""; s["note"] = ""; s["job_id"] = None
        log.warning(f"Cleared {len(orphaned_overlays)} orphaned rack reservation(s) on startup: {orphaned_ids}")
        save_config()
    yield
    for t in mqtt_tasks.values():
        if not t.done(): t.cancel()

app = FastAPI(title="OttoBridge v2", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")

@app.get("/api/system/stats")
async def get_system_stats():
    """One-shot fetch for initial page load, before the websocket delivers the
    first periodic update from system_stats_loop()."""
    try:
        async with httpx.AsyncClient(timeout=5) as c:
            r = await c.get(f"{MOONRAKER_URL}/machine/proc_stats")
            res = r.json().get("result", {})
        mem = res.get("system_memory", {})
        mem_total = mem.get("total"); mem_used = (mem_total or 0) - mem.get("available", 0)
        disk_total, disk_used = await _fetch_disk_usage()
        return {
            "cpu_pct": res.get("system_cpu_usage", {}).get("cpu"),
            "mem_pct": round(mem_used / mem_total * 100, 1) if mem_total else None,
            "disk_pct": round(disk_used / disk_total * 100, 1) if disk_total else None,
            "temp_c": res.get("cpu_temp"),
            "uptime_s": res.get("system_uptime"),
            "connected": True,
        }
    except Exception as e:
        return {"connected": False, "error": str(e)}

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
    grab_slot: Optional[int] = None   # rack slot (1-indexed) to grab plate from before print
    park_slot: Optional[int] = None   # rack slot (1-indexed) to park plate after print
    slots_needed: int = 1             # how many rack slots the print occupies
    height_mm: float = 0.0            # detected print height, for the overlay display

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
    """Update printer settings. Credential fields (access_code, api_key, check_code)
    are only overwritten if a new non-empty value is provided — the edit form
    intentionally leaves these blank to avoid re-displaying secrets, so an empty
    value here means 'keep the existing stored credential', not 'clear it'."""
    if pid not in printers: raise HTTPException(404)
    p = printers[pid]
    p.name = cfg.name; p.brand = cfg.brand; p.model = cfg.model; p.ip = cfg.ip
    if cfg.access_code: p.access_code = cfg.access_code
    if cfg.serial: p.serial = cfg.serial
    if cfg.api_key: p.api_key = cfg.api_key
    if cfg.serial_code: p.serial_code = cfg.serial_code
    if cfg.check_code: p.check_code = cfg.check_code
    save_config(); await start_printer_task(pid); return p.to_dict()

@app.delete("/api/printers/{pid}")
async def delete_printer(pid: str):
    if pid in mqtt_tasks and not mqtt_tasks[pid].done(): mqtt_tasks[pid].cancel()
    printers.pop(pid, None); save_config(); return {"ok": True}

class TestConnCfg(BaseModel):
    """Same shape as PrinterCfg's credential fields, minus name/model — the
    Test Connection button only needs enough to actually reach the printer."""
    brand: str; ip: str; printer_id: str = ""
    access_code: str = ""; serial: str = ""; api_key: str = ""
    serial_code: str = ""; check_code: str = ""

@app.post("/api/printers/test")
async def test_printer_connection(cfg: TestConnCfg):
    """Stateless connectivity check using whatever credentials are currently
    typed into the Add/Edit Printer form — lets the user verify a LAN code
    or API key BEFORE saving. If a credential field is left blank (which
    happens whenever editing an existing printer, since the UI never
    re-displays stored secrets) and printer_id matches a saved printer, we
    fall back to the already-stored value instead of testing an empty
    string — otherwise every edit-without-retyping would falsely "fail"."""
    stored = printers.get(cfg.printer_id)
    access_code = cfg.access_code or (stored.access_code if stored else "")
    api_key     = cfg.api_key     or (stored.api_key     if stored else "")
    serial_code = cfg.serial_code or (stored.serial_code if stored else "")
    check_code  = cfg.check_code  or (stored.check_code  if stored else "")

    binfo = BRANDS.get(cfg.brand, {})
    proto = binfo.get("protocol", "")
    if not cfg.ip:
        return {"ok": False, "message": "IP address required"}
    try:
        if proto == "mqtt_ftp":
            ctx = _bambu_ftp_ctx()
            def _try():
                with ImplicitFTP_TLS(context=ctx) as ftp:
                    ftp.connect(cfg.ip, 990, timeout=8); ftp.login("bblp", access_code); ftp.prot_p()
            await asyncio.get_event_loop().run_in_executor(None, _try)
            return {"ok": True, "message": "FTP login successful"}
        elif proto == "prusalink":
            async with httpx.AsyncClient(timeout=8) as c:
                r = await c.get(f"http://{cfg.ip}/api/v1/status", headers={"X-Api-Key": api_key})
            return {"ok": r.status_code == 200, "message": f"HTTP {r.status_code}" if r.status_code != 200 else "PrusaLink reachable"}
        elif proto == "moonraker":
            async with httpx.AsyncClient(timeout=8) as c:
                r = await c.get(f"http://{cfg.ip}:7125/printer/info")
            return {"ok": r.status_code == 200, "message": f"HTTP {r.status_code}" if r.status_code != 200 else "Moonraker reachable"}
        elif proto == "websocket":
            async with httpx.AsyncClient(timeout=8) as c:
                r = await c.get(f"http://{cfg.ip}/status")
            return {"ok": r.status_code == 200, "message": f"HTTP {r.status_code}" if r.status_code != 200 else "Reachable"}
        elif proto == "http_tcp":
            async with httpx.AsyncClient(timeout=8) as c:
                r = await c.post(f"http://{cfg.ip}:8898/detail", json={"serialNumber": serial_code, "checkCode": check_code})
            return {"ok": r.status_code == 200, "message": f"HTTP {r.status_code}" if r.status_code != 200 else "Reachable"}
        elif proto == "sdcp_ws":
            async with websockets.connect(f"ws://{cfg.ip}:3030/websocket", open_timeout=8):
                pass
            return {"ok": True, "message": "WebSocket connected"}
        else:
            return {"ok": False, "message": f"Unknown protocol for brand '{cfg.brand}'"}
    except Exception as e:
        return {"ok": False, "message": str(e)}

# ── Print control ─────────────────────────────────────────────────────────────
SLOT_GAP_MM = 55  # global_slot_gap (25mm) + 30mm offset, see storage_calibration_variables.cfg

def _normalize_ams_mapping(ams_map: list[int], use_ams: bool) -> list[int]:
    """Bambu AMS mapping must always have exactly 4 elements (one per AMS slot).
    If use_ams=False or list is empty, return [254,254,254,254] (external spool).
    Otherwise pad/truncate to exactly 4 elements.
    Reference: https://cinder.works/blog/bambu-ams-filament-mapping-guide
    """
    if not use_ams or not ams_map:
        return [254, 254, 254, 254]  # 254 = external spool for all slots
    # Pad with last element or truncate to exactly 4
    mapping = list(ams_map)
    while len(mapping) < 4:
        mapping.append(mapping[-1])
    return mapping[:4]

_HEIGHT_PATTERNS = (
    re.compile(rb";MAX_LAYER_Z:([\d.]+)", re.I),
    re.compile(rb";\s*total height\s*[=:]\s*([\d.]+)", re.I),
    re.compile(rb";LAYER_HEIGHT:([\d.]+)", re.I),
)

def _scan_height(data: bytes) -> Optional[float]:
    for pat in _HEIGHT_PATTERNS:
        m = pat.search(data)
        if m: return float(m.group(1))
    # Fallback: scan Z-moves in first 500KB
    mx = 0.0
    for m in re.finditer(rb"^G[01] [^;\n]*Z([\d.]+)", data[:500_000], re.M):
        z = float(m.group(1))
        if z > mx: mx = z
    return mx or None

_COLOUR_RE = re.compile(rb";\s*filament_colour\s*=\s*(.+)", re.I)
_TYPE_RE   = re.compile(rb";\s*filament_type\s*=\s*(.+)", re.I)
_USED_RE   = re.compile(rb";\s*filament_used \[g\]\s*=\s*(.+)", re.I)
_TOOL_RE   = re.compile(rb"(?:^|\n)T(\d+)")

def _scan_ams(data: bytes) -> dict:
    """Parse slicer header comments for multi-material/AMS info.
    Works for plain .gcode and the gcode extracted from .3mf/.gcode.3mf."""
    cm = _COLOUR_RE.search(data)
    if not cm:
        return {"multi_material": False}
    colours = [c.strip().decode() for c in re.split(rb"[;,]", cm.group(1).strip()) if c.strip()]
    tm = _TYPE_RE.search(data)
    types = [t.strip().decode() for t in re.split(rb"[;,]", tm.group(1).strip())] if tm else []
    um = _USED_RE.search(data)
    used = [float(u) for u in re.split(rb"[;,]", um.group(1).strip())] if um else []

    if used:
        used_slots = [u > 0 for u in used[:len(colours)]]
    else:
        used_slots = [True] * len(colours)

    tool_changes = sorted({int(t) for t in _TOOL_RE.findall(data[:200_000])})
    active = sum(used_slots)
    multi = active > 1 or len(tool_changes) > 1

    return {
        "multi_material": multi,
        "colours": colours,
        "types": types,
        "used_slots": used_slots,
        "active_count": active,
        "tool_changes": tool_changes,
    }

def _ams_mapping_from_scan(ams: dict) -> list[int]:
    """Build a 4-element ams_mapping from scanned slot usage.
    Maps used slots to AMS tray indices 0..3 in order; unused -> 254."""
    if not ams.get("multi_material"):
        return [254, 254, 254, 254]
    mapping, tray = [], 0
    for used in ams.get("used_slots", []):
        if used:
            mapping.append(tray); tray += 1
        else:
            mapping.append(254)
    while len(mapping) < 4: mapping.append(254)
    return mapping[:4]

def analyze_print_file(filename: str, data: bytes) -> dict:
    """Analyze a .gcode, .3mf or .gcode.3mf file: print height + multi-material info."""
    lower = filename.lower()
    gcode_data = data
    if lower.endswith(".3mf") or lower.endswith(".gcode.3mf"):
        # .3mf (and .gcode.3mf) is a ZIP archive — find the plate gcode inside
        try:
            with zipfile.ZipFile(io.BytesIO(data)) as zf:
                gcode_names = [n for n in zf.namelist()
                                if n.lower().endswith(".gcode") and "metadata" in n.lower()]
                if not gcode_names:
                    gcode_names = [n for n in zf.namelist() if n.lower().endswith(".gcode")]
                if gcode_names:
                    gcode_data = zf.read(gcode_names[0])
        except zipfile.BadZipFile:
            pass  # not a valid zip — fall back to scanning raw bytes

    height = _scan_height(gcode_data)
    ams = _scan_ams(gcode_data)
    slots_needed = None
    if height:
        slots_needed = max(1, -(-int(height) // SLOT_GAP_MM))  # ceil division

    return {
        "filename": filename,
        "height_mm": height,
        "slots_needed": slots_needed,
        "multi_material": ams.get("multi_material", False),
        "colours": ams.get("colours", []),
        "types": ams.get("types", []),
        "used_slots": ams.get("used_slots", []),
        "active_count": ams.get("active_count", 0),
        "use_ams": ams.get("multi_material", False),
        "ams_mapping": _ams_mapping_from_scan(ams),
    }

@app.post("/api/analyze")
async def analyze_file(file: UploadFile = File(...)):
    """Upload a .gcode/.3mf/.gcode.3mf, persist it to UPLOAD_DIR, and get
    height + AMS info. Used by the Jobs tab for slot calculation and
    automatic use_ams/ams_mapping detection. The file MUST land in
    UPLOAD_DIR here — the queue automation later reads it from there when
    the job actually starts (potentially much later), it does not re-upload
    from the browser."""
    name = file.filename or "upload.gcode"
    if not (name.lower().endswith((".gcode", ".3mf")) or name.lower().endswith(".gcode.3mf")):
        raise HTTPException(400, "Only .gcode, .3mf or .gcode.3mf supported")
    data = await file.read()
    if len(data) > 200 * 1024 * 1024:
        raise HTTPException(413, "File too large")
    result = analyze_print_file(name, data)
    if result["height_mm"] is None:
        raise HTTPException(422, f'Could not detect print height in "{name}"')
    dest = UPLOAD_DIR / name
    await asyncio.get_event_loop().run_in_executor(None, dest.write_bytes, data)
    return result

@app.get("/api/analyze/{filename}")
async def analyze_stored_file(filename: str):
    """Analyze a file already in UPLOAD_DIR (selected from the Jobs file list)."""
    path = UPLOAD_DIR / filename
    if not path.exists(): raise HTTPException(404, "File not found")
    data = await asyncio.get_event_loop().run_in_executor(None, path.read_bytes)
    if len(data) > 200 * 1024 * 1024:
        raise HTTPException(413, "File too large")
    result = analyze_print_file(filename, data)
    if result["height_mm"] is None:
        raise HTTPException(422, f'Could not detect print height in "{filename}"')
    return result

@app.post("/api/print/start")
async def start_print(req: StartPrint):
    p = printers.get(req.printer_id)
    if not p: raise HTTPException(404)
    if p.protocol == "mqtt_ftp":
        # Bambu Lab: AMS mapping must be exactly 4 elements
        # Multi-material (AMS/AMS Lite): use_ams=True + correct ams_mapping
        # Single-material (external spool): use_ams=False, ams_mapping=[254,254,254,254]
        ams_mapping = _normalize_ams_mapping(req.ams_map, req.use_ams)
        # For .3mf project files, Bambu firmware needs "param" to point at the
        # specific plate's gcode inside the archive (Printloom's independent
        # implementation documents this explicitly and always sets it for
        # .3mf; we previously always sent "" here). Raw .gcode files have no
        # such internal structure, so param stays empty for those.
        param = "Metadata/plate_1.gcode" if req.filename.lower().endswith(".3mf") else ""
        ok = await bambu_publish(req.printer_id, {"print": {
            "command":"project_file","sequence_id":_seq(),
            "file":req.filename,"url":f"ftp:///{req.filename}","param":param,
            "bed_type":req.bed_type,"bed_leveling":req.bed_level,
            "flow_cali":req.flow_cali,"vibration_cali":req.vibr_cali,
            "layer_inspect":req.layer_inspect,"use_ams":req.use_ams,
            "ams_mapping":ams_mapping,"timelapse":req.timelapse,
            "task_id":_seq(),"subtask_id":"0","project_id":"0","profile_id":"0",
            "subtask_name":Path(req.filename).name,"project_name":Path(req.filename).name,
        }})
    elif p.protocol == "prusalink":
        # Prusa: MMU3 tool changes are embedded in the gcode by the slicer.
        # PrusaLink does not accept filament mapping parameters — the gcode handles it.
        try:
            async with httpx.AsyncClient(timeout=10) as c:
                r = await c.post(f"http://{p.ip}/api/v1/print",
                    headers={"X-Api-Key":p.api_key}, json={"path":f"/usb/{req.filename}"})
            ok = r.status_code in (200,201,204)
        except Exception as e: raise HTTPException(503, str(e))
    elif p.protocol in ("moonraker","websocket"):
        # Anycubic (ACE Pro, via Rinkhals+Moonraker), Creality (CFS):
        # Multi-material tool changes (T0, T1, ACE_CHANGE_TOOL etc.) are embedded
        # in the gcode by the slicer. Klipper/Moonraker handles them automatically
        # via the respective Klipper modules (ACEPRO driver, etc.).
        # OttoBridge only needs to start the file — no extra parameters needed.
        ok = await moonraker_gcode(req.printer_id, f"SDCARD_PRINT_FILE FILENAME={req.filename}")
    elif p.protocol == "sdcp_ws":
        # Elegoo native SDCP: Cmd 128 starts the file already sitting on the printer
        # (uploaded beforehand via elegoo_sdcp_upload). CANVAS multi-material tool
        # changes are embedded in the gcode itself — no extra params needed here.
        ok = await elegoo_sdcp_send(req.printer_id, 128, {"Filename": req.filename, "StartLayer": 0})
    elif p.protocol == "http_tcp":
        # FlashForge: no multi-material system supported
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
    elif p.protocol in ("moonraker","websocket"):
        gc = {"pause":"PAUSE","resume":"RESUME","stop":"CANCEL_PRINT"}.get(cmd, cmd.upper())
        return await moonraker_gcode(pid, gc)
    elif p.protocol == "sdcp_ws":
        sdcp_cmd = {"pause":129,"stop":130,"resume":131}.get(cmd)
        return await elegoo_sdcp_send(pid, sdcp_cmd, {}) if sdcp_cmd else False
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
        ok1 = await bambu_publish(req.printer_id, {"print":{
            "command":"clean_print_error","sequence_id":_seq(),
            "subtask_id":p.subtask_id,"print_error":int(p.print_error or 0)}})
        # clean_print_error alone often doesn't actually flip gcode_state back to
        # IDLE (confirmed: our own "Clear error" button had no visible effect).
        # An independent Bambu integration (Printloom) resets a stuck FAILED
        # state by sending "stop" instead — do both for the best chance of
        # actually clearing it.
        ok2 = await bambu_publish(req.printer_id, {"print":{
            "command":"stop","sequence_id":_seq()}})
        return {"ok": ok1 or ok2}
    return {"ok": False}

@app.post("/api/print/sync")
async def sync_printer(req: PidOnly):
    """Force a fresh full status refresh instead of relying on the printer to
    push one unprompted. Bambu (mqtt_ftp) only sends its full report once at
    MQTT connect time (pushall) and afterwards only on state changes it
    decides to announce — if a print genuinely fails and the person clears
    the error on the printer's own touchscreen, that isn't guaranteed to
    trigger a fresh push, so our cached gcode_state can go stale (stuck on
    e.g. FAILED) even though the printer itself is idle again. Re-requesting
    pushall fixes that immediately. Poll-based protocols already refresh
    every few seconds on their own, so this is a no-op there."""
    p = printers.get(req.printer_id)
    if not p: raise HTTPException(404)
    if p.protocol == "mqtt_ftp":
        ok = await bambu_publish(req.printer_id, {"pushing": {"command": "pushall"}})
        return {"ok": ok}
    return {"ok": True}

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
    if not (file.filename.endswith((".3mf",".gcode")) or file.filename.lower().endswith(".gcode.3mf")):
        raise HTTPException(400,"Only .3mf/.gcode/.gcode.3mf")
    dest = UPLOAD_DIR / file.filename
    async with aiofiles.open(dest, "wb") as f:
        while chunk := await file.read(65536): await f.write(chunk)
    loop = asyncio.get_event_loop()
    if p.protocol == "mqtt_ftp":
        ok = await loop.run_in_executor(None, ftp_upload_sync, pid, str(dest), file.filename)
    elif p.protocol == "prusalink":
        ok = await prusa_upload(pid, str(dest), file.filename)
    elif p.protocol in ("moonraker","websocket"):
        ok = await moonraker_upload(pid, str(dest), file.filename)
    elif p.protocol == "sdcp_ws":
        ok = await elegoo_sdcp_upload(pid, str(dest), file.filename)
    else: ok = True
    if not ok: raise HTTPException(502, "Upload to printer failed")
    return {"ok": True, "filename": file.filename}

@app.get("/api/files")
async def list_files():
    return [{"name":fp.name,"size":fp.stat().st_size,"mtime":fp.stat().st_mtime}
            for fp in sorted(UPLOAD_DIR.iterdir())
            if fp.suffix in (".3mf",".gcode") or fp.name.lower().endswith(".gcode.3mf")]

@app.delete("/api/files/{filename}")
async def del_file(filename: str):
    p = UPLOAD_DIR / filename
    if p.exists(): await aiofiles.os.remove(p)
    return {"ok": True}

# ── Job routes ─────────────────────────────────────────────────────────────────
# ── Queue automation ─────────────────────────────────────────────────────────
# Job lifecycle: queued -> running -> done | error | aborted | skipped
# All jobs remain in `jobs` for history; only `queued` jobs can be removed while idle.
#
# Once per queue start (before the first job runs):
#   0a. OTTOEJECT_HOME              (Moonraker/Klipper)
#   0b. printer home (G28)          (printer protocol)
#
# Execution sequence per job (with grab_slot set):
#   1. parallel:
#        - OPEN_DOOR (only for the very first job of the queue, door-equipped printers)
#        - move printer axis to safe position (printer protocol)
#        - GRAB_FROM_SLOT_<grab>     (Moonraker/Klipper)
#   2. wait until printer reports IDLE, then:
#      LOAD_ONTO_<PRINTER>           (Moonraker/Klipper — also closes the door)
#   3. PARK_OTTOEJECT                (Moonraker/Klipper)
#   4. start_print (file)            (printer protocol)
#   5. wait for status FINISH        (poll)
#   6. move axis to safe pos + M400  (printer protocol, sent to printer)
#   7. wait until printer reports IDLE
#   8. EJECT_FROM_<PRINTER>          (Moonraker/Klipper — also opens the door)
#   9. STORE_TO_SLOT_<park>          (Moonraker/Klipper)
#  10. PARK_OTTOEJECT                (Moonraker/Klipper)
#
# After the last job in the queue finishes (no more queued jobs left):
#   CLOSE_DOOR is sent so the printer doesn't sit with an open door indefinitely.
#   If more jobs remain queued, the door is left open for the next cycle.
#
# If grab_slot is None (plate already in printer), steps 1-3 are skipped.
# On retry after an error during/after printing, steps 1-3 are also skipped
# (the plate is already on the bed) — retry restarts from step 4.

STEPS_FULL    = ["grab_parallel", "load", "park1_upload", "print", "wait_finish", "move", "wait_idle", "eject", "store"]
STEPS_NOPLATE = ["upload", "print", "wait_finish", "move", "wait_idle", "eject", "store"]

class QueueManager:
    def __init__(self):
        self.state = "idle"        # idle | running | paused | error
        self.cur_job_id: Optional[str] = None
        self.cur_step_idx = 0
        self.retry_from_print = False
        self.error_msg = ""
        self.task: Optional[asyncio.Task] = None
        self.home_done = False      # OTTOEJECT_HOME + printer home — once per queue start
        self.door_opened_job_id: Optional[str] = None  # which job's OPEN_DOOR already ran

    def _steps(self, job: dict) -> list[str]:
        if job.get("grab_slot") and not (self.retry_from_print and job["id"] == self.cur_job_id):
            return STEPS_FULL
        return STEPS_NOPLATE

    def to_dict(self):
        cur = next((j for j in jobs if j["id"] == self.cur_job_id), None)
        return {
            "state": self.state,
            "current_job_id": self.cur_job_id,
            "current_step": self.cur_step_idx,
            "current_steps": self._steps(cur) if cur else [],
            "retry_from_print": self.retry_from_print,
            "error_msg": self.error_msg,
        }

    def start(self):
        if self.state != "idle": return False
        nxt = next((j for j in jobs if j["status"] == "queued"), None)
        if not nxt: return False
        self.state = "running"
        self.cur_job_id = nxt["id"]
        self.cur_step_idx = 0
        self.retry_from_print = False
        self.error_msg = ""
        self.home_done = False
        self.door_opened_job_id = None
        self.task = asyncio.create_task(self._run())
        return True

    def pause(self):
        if self.state != "running": return False
        self.state = "paused"
        if self.task and not self.task.done(): self.task.cancel()
        return True

    def resume(self):
        if self.state != "paused": return False
        self.state = "running"
        self.task = asyncio.create_task(self._run())
        return True

    def stop(self):
        """Abort the current job and free its rack slots. Queue goes idle.
        All job history (done/aborted/skipped/queued) remains visible."""
        if self.task and not self.task.done(): self.task.cancel()
        job = next((j for j in jobs if j["id"] == self.cur_job_id), None)
        if job and job["status"] == "running":
            job["status"] = "aborted"
            rack.free_job_slots(job["id"])
        self.state = "idle"
        self.cur_job_id = None
        self.cur_step_idx = 0
        self.retry_from_print = False
        self.error_msg = ""
        return job

    def retry(self):
        """Retry the failed job. Skips grab/load — plate is already on the printer."""
        if self.state != "error": return False
        job = next((j for j in jobs if j["id"] == self.cur_job_id), None)
        if not job: return False
        job["status"] = "queued"
        self.retry_from_print = True
        self.cur_step_idx = 0
        self.state = "running"
        self.error_msg = ""
        self.task = asyncio.create_task(self._run())
        return True

    def skip(self):
        """Mark the failed job as skipped, free its slots, advance to next job."""
        if self.state != "error": return False
        job = next((j for j in jobs if j["id"] == self.cur_job_id), None)
        if job:
            job["status"] = "skipped"
            rack.free_job_slots(job["id"])
        self.state = "idle"
        self.cur_job_id = None
        self.cur_step_idx = 0
        self.retry_from_print = False
        self.error_msg = ""
        self._advance_after_idle()
        return True

    def abort(self):
        """Abort the failed job, free its slots. Same as skip but explicit
        'abort & free slots' wording for the UI error banner."""
        if self.state != "error": return False
        job = next((j for j in jobs if j["id"] == self.cur_job_id), None)
        if job:
            job["status"] = "aborted"
            rack.free_job_slots(job["id"])
        self.state = "idle"
        self.cur_job_id = None
        self.cur_step_idx = 0
        self.retry_from_print = False
        self.error_msg = ""
        self._advance_after_idle()
        return True

    def _advance_after_idle(self):
        """After skip/abort, optionally auto-continue is NOT done — queue stays idle.
        User must press Start again. (Manual-start, pause-on-error per spec.)"""
        pass

    async def _run(self):
        try:
            if not self.home_done:
                job0 = next((j for j in jobs if j["id"] == self.cur_job_id), None)
                if job0:
                    ok = await self._home_for_queue_start(job0)
                    if not ok:
                        self.state = "error"
                        self.error_msg = "Homing failed at queue start"
                        await broadcast("queue", self.to_dict())
                        return
                self.home_done = True

            while self.state == "running":
                job = next((j for j in jobs if j["id"] == self.cur_job_id), None)
                if not job:
                    self.state = "idle"; return
                job["status"] = "running"
                await broadcast("queue", self.to_dict())
                steps = self._steps(job)

                if self.cur_step_idx >= len(steps):
                    # Job finished successfully
                    job["status"] = "done"
                    rack.mark_printed(job["id"])
                    save_config()
                    await broadcast("rack", rack.to_dict())
                    await broadcast("job_done", job)
                    nxt = next((j for j in jobs if j["status"] == "queued"), None)
                    if nxt:
                        self.cur_job_id = nxt["id"]
                        self.cur_step_idx = 0
                        self.retry_from_print = False
                        await asyncio.sleep(1)
                        continue
                    else:
                        # Last job done — close door then park the arm.
                        p = printers.get(job["printer_id"])
                        if p:
                            m = get_close_door_macro(p.brand, p.model)
                            if m:
                                try: await _run_klipper(m)
                                except Exception as e:
                                    log.warning(f"Failed to close door at queue end: {e}")
                        try: await _run_klipper("PARK_OTTOEJECT")
                        except Exception as e:
                            log.warning(f"Failed to park OttoEject at queue end: {e}")
                        self.state = "idle"
                        self.cur_job_id = None
                        await broadcast("queue", self.to_dict())
                        return

                step = steps[self.cur_step_idx]
                ok = await self._exec_step(job, step)
                if not ok:
                    self.state = "error"
                    self.error_msg = f'Step "{step}" failed for {job["filename"]}'
                    await broadcast("queue", self.to_dict())
                    return
                self.cur_step_idx += 1
                await broadcast("queue", self.to_dict())
        except asyncio.CancelledError:
            raise

    async def _home_for_queue_start(self, job: dict) -> bool:
        """Runs once per queue start, before the first job's steps:
        1. OTTOEJECT_HOME (Moonraker/Klipper)
        2. Printer home (G28, sent to the printer itself)"""
        try:
            r = await _run_klipper("OTTOEJECT_HOME")
            if not r.get("ok"): return False
            pid = job["printer_id"]
            ok = await _send_printer_gcode(pid, "G28")
            return ok
        except Exception as e:
            log.error(f"Queue-start homing failed: {e}")
            return False

    async def _exec_step(self, job: dict, step: str) -> bool:
        pid = job["printer_id"]
        p = printers.get(pid)
        if not p: return False
        try:
            if step == "grab_parallel":
                # Phase 1 (parallel): open door + move to safe pos.
                # These two can run simultaneously — neither moves toward the plate.
                # Door open is only needed for the very first job of the queue;
                # later jobs already have the door open from the previous EJECT_FROM_<PRINTER>.
                is_first_job = (self.door_opened_job_id is None)
                phase1 = [self._move_to_safe_pos(pid, p)]
                if is_first_job and has_door(p.brand, p.model):
                    phase1.append(self._open_door(p))
                    self.door_opened_job_id = job["id"]
                results1 = await asyncio.gather(*phase1, return_exceptions=True)
                if not all((r is True) for r in results1):
                    return False
                # Phase 2 (sequential): grab only after door is confirmed open
                return await self._grab_from_slot(job)
            if step == "load":
                if not await self._wait_for_idle(pid): return False
                m = get_load_macro(p.brand, p.model)
                if not m: return True  # no load macro defined -> skip
                r = await _run_klipper(m)
                return bool(r.get("ok"))
            if step == "park1_upload":
                # PARK_OTTOEJECT and file upload run in parallel —
                # both are independent and neither blocks the other.
                park_task   = _run_klipper("PARK_OTTOEJECT")
                upload_task = self._upload_file(job, p)
                results = await asyncio.gather(park_task, upload_task, return_exceptions=True)
                park_ok   = isinstance(results[0], dict) and results[0].get("ok")
                upload_ok = results[1] is True
                return park_ok and upload_ok
            if step == "upload":
                return await self._upload_file(job, p)
            if step == "print":
                req = StartPrint(**{k: v for k, v in job.items()
                                     if k in StartPrint.model_fields})
                # Bambu X1C/P1S: give the printer filesystem a moment to fully
                # settle after FTP before sending project_file via MQTT.
                # Avoids 0500-4003 "cannot process file" errors seen when the
                # command arrives while the firmware is still flushing the write.
                if p and p.protocol == "mqtt_ftp":
                    await asyncio.sleep(5)
                await start_print(req)
                # Nudge Bambu for a fresh status ASAP so _wait_for_finish's
                # seen_active guard clears the stale-status window quickly
                # instead of waiting for the next unprompted push.
                if p and p.protocol == "mqtt_ftp":
                    await bambu_publish(pid, {"pushing": {"command": "pushall"}})
                return True
            if step == "wait_finish":
                return await self._wait_for_finish(pid)
            if step == "move":
                await self._move_to_safe_pos(pid, p)
                return True
            if step == "wait_idle":
                return await self._wait_for_idle(pid)
            if step == "eject":
                m = get_eject_macro(p.brand, p.model)
                if not m: return False
                r = await _run_klipper(m)
                return bool(r.get("ok"))
            if step == "store":
                r = await _run_klipper(f"STORE_TO_SLOT_{job['park_slot']}")
                return bool(r.get("ok"))
        except Exception as e:
            log.error(f"Queue step '{step}' failed for job {job['id']}: {e}")
            return False
        return False

    async def _move_to_safe_pos(self, pid: str, p) -> bool:
        if p.model in COREXY_MODELS:
            move_cmd = "G1 Z200 F3000"
        else:
            move_cmd = f"G1 Y{CARTESIAN_YMAX.get(p.model, 210)} F6000"
        await _send_printer_gcode(pid, move_cmd)
        await _send_printer_gcode(pid, "M400")
        return True

    async def _grab_from_slot(self, job: dict) -> bool:
        r = await _run_klipper(f"GRAB_FROM_SLOT_{job['grab_slot']}")
        return bool(r.get("ok"))

    async def _upload_file(self, job: dict, p) -> bool:
        """Transfer the job file from OttoBridge's uploads/ to the printer."""
        fname = job["filename"]
        local = UPLOAD_DIR / fname
        if not local.exists():
            log.error(f"Upload failed: {local} not found in uploads/")
            return False
        loop = asyncio.get_event_loop()
        if p.protocol == "mqtt_ftp":
            ok = await loop.run_in_executor(None, ftp_upload_sync, p.id, str(local), fname)
        elif p.protocol == "prusalink":
            ok = await prusa_upload(p.id, str(local), fname)
        elif p.protocol in ("moonraker", "websocket"):
            ok = await moonraker_upload(p.id, str(local), fname)
        elif p.protocol == "sdcp_ws":
            ok = await elegoo_sdcp_upload(p.id, str(local), fname)
        elif p.protocol == "http_tcp":
            try:
                async with httpx.AsyncClient(timeout=60) as c:
                    with open(local, "rb") as f:
                        r = await c.post(
                            f"http://{p.ip}:8898/upload",
                            files={"file": (fname, f, "application/octet-stream")},
                            data={"serialNumber": p.serial_code, "checkCode": p.check_code},
                        )
                ok = r.status_code == 200
            except Exception as e:
                log.error(f"FlashForge upload failed: {e}")
                ok = False
        else:
            log.warning(f"No upload method for protocol {p.protocol!r}, skipping")
            ok = True
        if not ok:
            log.error(f"Upload of {fname!r} to {p.name} failed")
        return ok

    async def _open_door(self, p) -> bool:
        """Open the printer door standalone, before any EJECT_FROM_<PRINTER>
        has run in this Klipper session. The door coordinates are fixed
        values already stored in _PRINTER_VARS (printer_calibration_variables.cfg).
        Jinja2 templating ({% %} / { }) only evaluates inside a gcode_macro
        definition in the config file — it is NOT evaluated in a raw script
        sent via Moonraker's gcode/script endpoint. So we first read the
        literal values via Moonraker's objects/query, then send plain
        SET_GCODE_VARIABLE commands with those literal numbers, exactly
        mirroring what each EJECT_FROM_<PRINTER> macro does internally."""
        try:
            async with httpx.AsyncClient(timeout=10) as c:
                r = await c.get(f"{MOONRAKER_URL}/printer/objects/query",
                                 params={"gcode_macro _PRINTER_VARS": ""})
            data = r.json().get("result", {}).get("status", {}).get("gcode_macro _PRINTER_VARS", {})
            x_start = data.get("x_start"); y_start = data.get("y_start")
            z_engage = data.get("z_engage"); d_pin_dist = data.get("d_pin_dist")
            if None in (x_start, y_start, z_engage, d_pin_dist):
                log.error("_PRINTER_VARS missing door coordinates (x_start/y_start/z_engage/d_pin_dist)")
                return False
        except Exception as e:
            log.error(f"Failed to query _PRINTER_VARS for door open: {e}")
            return False

        gcode = (
            f"SET_GCODE_VARIABLE MACRO=_OPEN_DOOR VARIABLE=door_x_start VALUE={x_start}\n"
            f"SET_GCODE_VARIABLE MACRO=_OPEN_DOOR VARIABLE=door_y_start VALUE={y_start}\n"
            f"SET_GCODE_VARIABLE MACRO=_OPEN_DOOR VARIABLE=door_z_engage VALUE={z_engage}\n"
            f"SET_GCODE_VARIABLE MACRO=_OPEN_DOOR VARIABLE=door_d_to_pin_dist VALUE={d_pin_dist}\n"
            "_OPEN_DOOR"
        )
        r = await _run_klipper(gcode)
        return bool(r.get("ok"))

    async def _wait_for_idle(self, pid: str) -> bool:
        """Poll printer status until IDLE."""
        while True:
            if self.state != "running": return False
            p = printers.get(pid)
            if not p: return False
            if p.status in ("IDLE", "FINISH"): return True
            if p.status in ("FAILED", "CANCELLED", "STOPPED"): return False
            await asyncio.sleep(2)

    async def _wait_for_finish(self, pid: str) -> bool:
        """Poll printer status until FINISH (success) or FAILED/CANCELLED (error).
        For Bambu printers, also watches print_error — the X1C sets a non-zero
        error code (e.g. 0500-4003) before gcode_state flips to FAILED, so we
        catch it early to avoid the queue hanging on a stalled RUNNING status.

        We only start honoring FAILED/CANCELLED/STOPPED as a real failure once
        we've either (a) seen the printer actually go active (RUNNING/PREPARE)
        at least once, or (b) a 60s grace period has passed. (a) exists
        because right after start_print() there's a window where p.status can
        still hold a STALE value left over from a previous print attempt —
        MQTT pushes aren't instant — and without this guard that gets
        misread as this print having already failed. (b) exists so a print
        that genuinely fails immediately and never reaches RUNNING/PREPARE at
        all doesn't hang the queue forever waiting for a state that's never
        coming — 60s is generous enough for any push delay while still
        catching a real fast failure promptly."""
        seen_active = False
        grace_deadline = time.time() + 60
        while True:
            if self.state != "running": return False
            p = printers.get(pid)
            if not p: return False
            if p.status in ("RUNNING", "PREPARE"): seen_active = True
            if p.status == "FINISH": return True
            trust_terminal_status = seen_active or time.time() > grace_deadline
            if trust_terminal_status and p.status in ("FAILED", "CANCELLED", "STOPPED"):
                log.error(f"[{p.name}] status={p.status} while waiting for finish — treating as failed")
                return False
            # Bambu-specific: catch print errors before status flips
            if trust_terminal_status and p.protocol == "mqtt_ftp" and p.print_error not in ("0", "", "0x0", None):
                log.error(f"[{p.name}] print_error={p.print_error} detected — treating as FAILED")
                return False
            await asyncio.sleep(3)

queue_mgr = QueueManager()

@app.get("/api/jobs")
async def get_jobs(): return jobs

@app.get("/api/queue")
async def get_queue(): return queue_mgr.to_dict()

@app.post("/api/jobs")
async def add_job(req: StartPrint):
    """Queue a new job and reserve its rack slots (lock grab + park + overlay)."""
    if req.park_slot is None:
        raise HTTPException(400, "park_slot is required")
    bottom = req.park_slot - 1
    grab_idx = (req.grab_slot - 1) if req.grab_slot is not None else None
    err = rack.check_park(bottom, max(1, req.slots_needed), grab_idx)
    if err: raise HTTPException(409, err)
    if req.grab_slot is not None:
        gi = req.grab_slot - 1
        if not (0 <= gi < rack.num_slots) or rack.slots[gi]["state"] != "ready":
            raise HTTPException(409, f"Slot {req.grab_slot} has no plate")

    job = {"id": uuid.uuid4().hex[:8], "status": "queued", **req.model_dump()}
    jobs.append(job)
    rack.reserve_for_job(job["id"], (req.grab_slot - 1) if req.grab_slot else None,
                          bottom, max(1, req.slots_needed), req.height_mm, req.filename)
    save_config()
    await broadcast("job_added", job)
    await broadcast("rack", rack.to_dict())
    return job

@app.delete("/api/jobs/{jid}")
async def del_job(jid: str):
    """Remove a queued job and free its reserved slots.
    Only allowed while the queue is idle and the job hasn't started."""
    global jobs
    job = next((j for j in jobs if j["id"] == jid), None)
    if not job: raise HTTPException(404)
    if job["status"] != "queued":
        raise HTTPException(409, "Only queued jobs can be removed")
    rack.free_job_slots(jid)
    jobs = [j for j in jobs if j["id"] != jid]
    save_config()
    await broadcast("rack", rack.to_dict())
    return {"ok": True}

@app.post("/api/queue/start")
async def queue_start():
    if not queue_mgr.start(): raise HTTPException(409, "Nothing to start")
    return queue_mgr.to_dict()

@app.post("/api/queue/pause")
async def queue_pause():
    if not queue_mgr.pause(): raise HTTPException(409, "Queue is not running")
    return queue_mgr.to_dict()

@app.post("/api/queue/resume")
async def queue_resume():
    if not queue_mgr.resume(): raise HTTPException(409, "Queue is not paused")
    return queue_mgr.to_dict()

@app.post("/api/queue/stop")
async def queue_stop():
    job = queue_mgr.stop()
    save_config()
    await broadcast("rack", rack.to_dict())
    await broadcast("queue", queue_mgr.to_dict())
    return queue_mgr.to_dict()

@app.post("/api/queue/retry")
async def queue_retry():
    if not queue_mgr.retry(): raise HTTPException(409, "Queue is not in error state")
    return queue_mgr.to_dict()

@app.post("/api/queue/skip")
async def queue_skip():
    if not queue_mgr.skip(): raise HTTPException(409, "Queue is not in error state")
    save_config()
    await broadcast("rack", rack.to_dict())
    return queue_mgr.to_dict()

@app.post("/api/queue/abort")
async def queue_abort():
    if not queue_mgr.abort(): raise HTTPException(409, "Queue is not in error state")
    save_config()
    await broadcast("rack", rack.to_dict())
    return queue_mgr.to_dict()

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
    rack.slots[req.slot_index] = {"state": req.state, "label": req.label,
                                   "note": req.note, "job_id": None}
    save_config()
    await broadcast("rack", rack.to_dict())
    return rack.to_dict()

@app.post("/api/rack/slot/{slot}/clear_print")
async def clear_print_slot(slot: int):
    """User confirms the finished print + plate were physically removed.
    Frees this slot and any slots blocked by its print overlay."""
    if slot < 1 or slot > rack.num_slots: raise HTTPException(400, "Invalid slot")
    idx = slot - 1
    if rack.slots[idx]["state"] != "printed":
        raise HTTPException(409, "Slot has no finished print")
    rack.clear_print(idx)
    save_config()
    await broadcast("rack", rack.to_dict())
    return rack.to_dict()

@app.post("/api/rack/slot/{slot}/free_grab")
async def free_grab_slot(slot: int):
    """Manually free a grab_reserved slot — the plate was already picked up."""
    if slot < 1 or slot > rack.num_slots: raise HTTPException(400, "Invalid slot")
    idx = slot - 1
    if rack.slots[idx]["state"] != "grab_reserved":
        raise HTTPException(409, "Slot is not reserved for grabbing")
    rack.free_grab_slot(idx)
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

COREXY_MODELS = {
    "X1C","P1S","P1P","A1","A1 Mini","P2S",   # Bambu Lab
    "Core One",                                  # Prusa Core One
    "K1C","K1","K1 Max",                         # Creality
    "Kobra S1",                                  # Anycubic
    "Centauri Carbon","Centauri",                # Elegoo
    "AD5X","Adventurer 5M Pro","Adventurer 5M",  # FlashForge
    "Generic Klipper",
}
CARTESIAN_YMAX = {"MK3S":210,"MK3":210,"MK4S":250,"MK4":250}

async def _send_printer_gcode(pid: str, cmd: str) -> bool:
    """Send a gcode command to the printer itself (not Klipper/Moonraker)."""
    p = printers.get(pid)
    if not p: return False
    if p.protocol == "mqtt_ftp":
        return await bambu_publish(pid,
            {"print":{"command":"gcode_line","sequence_id":_seq(),"param":cmd}})
    else:
        return await moonraker_gcode(pid, cmd)

@app.post("/api/ottoeject/eject/{pid}")
async def ej_eject(pid: str):
    """Full eject sequence:
    1. Move printer axis to safe position (sent to printer directly)
    2. M400 — wait for move (sent to printer)
    3. EJECT_FROM_<PRINTER> (sent to Klipper via Moonraker)
    """
    p = printers.get(pid)
    if not p: raise HTTPException(404)
    m = get_eject_macro(p.brand, p.model)
    if not m: raise HTTPException(422, f"No eject macro for {p.brand} {p.model}")

    # Step 1+2: move printer to safe position
    if p.model in COREXY_MODELS:
        move_cmd = "G1 Z200 F3000"
    else:
        ymax = CARTESIAN_YMAX.get(p.model, 210)
        move_cmd = f"G1 Y{ymax} F6000"

    await _send_printer_gcode(pid, move_cmd)
    await _send_printer_gcode(pid, "M400")

    # Step 3: eject via Klipper macro
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
        rack.slots[idx] = {"state": "empty", "label": "", "note": "", "job_id": None}
        save_config(); await broadcast("rack", rack.to_dict())
    return result

@app.post("/api/ottoeject/store_slot/{slot}")
async def store_slot(slot: int):
    """STORE_TO_SLOT_N → marks slot as ready"""
    if slot < 1 or slot > rack.num_slots: raise HTTPException(400, "Invalid slot")
    result = await _run_klipper(f"STORE_TO_SLOT_{slot}")
    if result.get("ok"):
        idx = slot - 1
        rack.slots[idx] = {"state": "ready", "label": "Plate loaded", "note": "", "job_id": None}
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
        "queue": queue_mgr.to_dict(),
        "jobs": jobs,
    }}))
    try:
        while True: await ws.receive_text()
    except WebSocketDisconnect:
        try: ws_clients.remove(ws)
        except ValueError: pass

@app.get("/", response_class=HTMLResponse)
async def root(): return (BASE_DIR / "static" / "index.html").read_text()
