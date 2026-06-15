# OttoBridge

**Lightweight web-based orchestrator for 3D print farms with OttoEject rack automation.**

Runs on a Raspberry Pi Zero 2 W alongside Klipper + Moonraker. Replaces OTTOengine with a lean Python/FastAPI backend (~30 MB RAM) and a vanilla JS frontend — no Electron, no Node.js, no build step.

![OttoBridge Dashboard](https://raw.githubusercontent.com/repraph/OttoBridge/main/docs/screenshot.png)

---

## Supported Printers

| Brand | Models | Protocol |
|---|---|---|
| Bambu Lab | X1C, P1S, P1P, A1, A1 Mini, P2S | MQTT + FTPS |
| Prusa | MK3S, MK3, MK4S, MK4, Core One | PrusaLink HTTP |
| Creality | K1C, K1, K1 Max | HTTP |
| Anycubic | Kobra S1, Kobra S1 Max | Moonraker |
| Elegoo | Centauri Carbon, Centauri | WebSocket |
| FlashForge | AD5X, Adventurer 5M Pro, 5M | HTTP |
| Generic | Any Klipper/Moonraker printer | Moonraker |

---

## Features

- **Dashboard** — live printer status, temperatures, progress, AMS tray info
- **OttoEject** — one-click eject, load and door-close macros for all supported printers
- **Rack management** — up to 6 slots, SVG visualisation, slot states (ready / grab-reserved / park-reserved / printed)
- **Gcode analysis** — drag-and-drop `.gcode` or `.3mf`, auto-detects print height, calculates required slots
- **Job queue** — assign grab slot + park slot per job; slots locked only when queued, not on upload
- **Smart slot reuse** — if a plate is grabbed from Slot N before printing, Slot N is immediately free to park the finished print
- **Mainsail integration** — appears as external link in Mainsail sidebar via `moonraker.conf`
- **WebSocket live updates** — all tabs update in real time
- **Multi-printer** — manage multiple printers simultaneously

---

## Hardware Requirements

- Raspberry Pi Zero 2 W (512 MB RAM)
- Klipper + Moonraker already running
- OttoEject rack hardware (aluminum profile + bracket assembly)

### RAM footprint

| Service | RAM |
|---|---|
| Klipper | ~40 MB |
| Moonraker | ~60 MB |
| OttoBridge | ~30 MB |
| **Total** | **~130 MB** ✓ |

---

## Installation

```bash
# Copy files to Pi
scp -r OttoBridge/ pi@<pi-ip>:~/

# On the Pi
cd ~/OttoBridge
bash install.sh
```

OttoBridge is now available at `http://<pi-ip>:8080`

### Manual install

```bash
python3 -m venv venv
venv/bin/pip install -r requirements.txt
venv/bin/uvicorn app:app --host 0.0.0.0 --port 8080 --workers 1
```

---

## Klipper Macros Setup

Copy the macro files to your Klipper config directory:

```bash
cp klipper_macros/rack_slots.cfg ~/printer_data/config/macros/
```

Add to your `printer.cfg`:

```ini
[include macros/rack_slots.cfg]
[include macros/ottoeject_macros.cfg]
[include macros/printer_calibration_variables.cfg]
[include macros/storage_calibration_variables.cfg]

# Activate your printer — uncomment one:
;[include macros/_printer_x1c.cfg]
;[include macros/_printer_p1s.cfg]
;[include macros/_printer_p1p.cfg]
;[include macros/_printer_a1.cfg]
;[include macros/_printer_k1c.cfg]
;[include macros/_printer_kobra_s1.cfg]
;[include macros/_printer_elegoo_cc.cfg]
;[include macros/_printer_flashforge_ad5x.cfg]
```

---

## Mainsail Integration

Add to `moonraker.conf` to show OttoBridge as a sidebar link in Mainsail:

```ini
[application OttoBridge]
type: adhoc
website: http://localhost:8080
```

> Mainsail has no native plugin system. This is the recommended integration method — OttoBridge runs independently on port 8080.

---

## Gcode Height Detection

OttoBridge reads print height from slicer comments (fastest):

```
;MAX_LAYER_Z:62.4        ← OrcaSlicer, BambuStudio
; total height: 62.4     ← BambuStudio
;LAYER_HEIGHT:62.4       ← PrusaSlicer
```

Falls back to scanning all Z-moves if no comment is found. Supports `.gcode` and `.3mf`.

**Slot calculation:** `slots_needed = ceil(print_height_mm / 55)`

The 55 mm slot gap matches the default `global_slot_gap` (25 mm) + 30 mm offset from `_DO_SLOT_OPERATION` in `storage_calibration_variables.cfg`.

---

## API

```
GET  /api/printers              → list all printers
POST /api/printers              → add printer
PUT  /api/printers/{id}         → update printer
DELETE /api/printers/{id}       → remove printer

GET  /api/rack                  → rack state
PUT  /api/rack/config           → set slot count
PUT  /api/rack/slot             → update slot state

POST /api/print/start           → start print
POST /api/print/pause           → pause
POST /api/print/resume          → resume
POST /api/print/stop            → stop
POST /api/print/clear_error     → clear error

POST /api/upload/{printer_id}   → upload + FTP transfer
GET  /api/files                 → list uploaded files

GET  /api/jobs                  → job queue
POST /api/jobs                  → add job
DELETE /api/jobs/{id}           → remove job

POST /api/ottoeject/eject/{id}       → run eject macro
POST /api/ottoeject/load/{id}        → run load macro
POST /api/ottoeject/close_door/{id}  → run door macro
POST /api/ottoeject/grab_slot/{n}    → GRAB_FROM_SLOT_N
POST /api/ottoeject/store_slot/{n}   → STORE_TO_SLOT_N
POST /api/ottoeject/macro            → run any macro

WS   /ws                        → live state updates
```

---

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `MOONRAKER_URL` | `http://localhost:7125` | Moonraker address |
| `LOG_LEVEL` | `INFO` | Set to `DEBUG` for MQTT details |

---

## Project Structure

```
OttoBridge/
├── app.py                  ← FastAPI backend
├── requirements.txt
├── install.sh
├── ottobridge.service      ← systemd unit
├── static/
│   └── index.html          ← complete frontend (single file)
├── klipper_macros/
│   └── rack_slots.cfg      ← GRAB/STORE slot macros
├── test_gcodes/            ← sample files for testing
│   ├── phone_stand_15mm.gcode
│   ├── cable_clip_38mm.gcode
│   ├── vase_62mm.gcode
│   ├── lamp_shade_118mm.gcode
│   ├── voron_toolhead_165mm.gcode
│   └── tall_column_340mm.gcode
└── uploads/                ← auto-created, gitignored
```

---

## Correct Macro Names

OTTOengine used incorrect macro names internally. OttoBridge uses the exact names from the `.cfg` files:

| Printer | Eject | Load |
|---|---|---|
| Bambu X1C | `EJECT_FROM_BAMBULAB_X_ONE_C` | `LOAD_ONTO_BAMBULAB_X_ONE_C` |
| Bambu P1S | `EJECT_FROM_BAMBULAB_P_ONE_S` | `LOAD_ONTO_BAMBULAB_P_ONE_S` |
| Bambu P1P | `EJECT_FROM_BAMBULAB_P_ONE_P` | `LOAD_ONTO_BAMBULAB_P_ONE_P` |
| Bambu A1 | `EJECT_FROM_BAMBULAB_A_ONE` | `LOAD_ONTO_BAMBULAB_A_ONE` |
| Prusa MK4S | `EJECT_FROM_PRUSA_MK_FOUR_S` | `LOAD_ONTO_PRUSA_MK_FOUR_S` |
| Prusa Core One | `EJECT_FROM_PRUSA_CORE_ONE` | `LOAD_ONTO_PRUSA_CORE_ONE` |
| Anycubic Kobra S1 | `EJECT_FROM_ANYCUBIC_KOBRA_S_ONE` | `LOAD_ONTO_ANYCUBIC_KOBRA_S_ONE` |
| Elegoo Centauri | `EJECT_FROM_ELEGOO_CC` | `LOAD_ONTO_ELEGOO_CC` |
| Creality K1C | `EJECT_FROM_CREALITY_K_ONE_C` | `LOAD_ONTO_CREALITY_K_ONE_C` |
| FlashForge AD5X | `EJECT_FROM_FLASHFORGE_AD_FIVE_X` | `LOAD_ONTO_FLASHFORGE_AD_FIVE_X` |

---

## License

MIT License — Non-Commercial. Free for personal, educational, and community use.
Commercial use requires written permission. See [LICENSE](LICENSE).

Not affiliated with Bambu Lab, Prusa, Creality, Anycubic, Elegoo, or FlashForge.
