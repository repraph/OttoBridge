# OttoBridge

Lightweight web orchestrator for 3D print farms with OttoEject rack automation. Runs on a Raspberry Pi Zero 2 W alongside Klipper + Moonraker (~30 MB RAM). No Electron, no Node.js, no build step.

📖 **First time? → [INSTALL.md](INSTALL.md)**

---

## Supported Printers

| Brand | Models | Protocol |
|---|---|---|
| Bambu Lab | X1C, P1S, P1P, A1, A1 Mini, P2S | MQTT + FTPS |
| Prusa | MK3S, MK3, MK4S, MK4, Core One | PrusaLink HTTP |
| Creality | K1C, K1, K1 Max | HTTP |
| Anycubic | Kobra S1 | Moonraker |
| Elegoo | Centauri Carbon, Centauri | WebSocket |
| FlashForge | AD5X, Adventurer 5M Pro, 5M | HTTP |
| Generic | Any Klipper/Moonraker printer | Moonraker |

## Multi-Material Systems

| Printer | System | Notes |
|---|---|---|
| Bambu X1C / P1S / P2S / A1 | AMS / AMS Lite | Full support via MQTT `ams_mapping` (always 4 elements) |
| Anycubic Kobra S1 | ACE Pro | Tool changes in gcode, handled by Klipper [ACEPRO driver](https://github.com/Kobra-S1/ACEPRO) |
| Elegoo Centauri | CANVAS | Tool changes in gcode, Klipper-based |
| Creality K1C | CFS | Tool changes in gcode, Klipper-based |
| Prusa MK4S | MMU3 | Tool changes in gcode, PrusaLink starts file |

---

## Features

- Dashboard — live status, temperatures, progress
- Printer tab — connect any supported printer, OttoEject calibration shortcuts
- OttoEject tab — eject, load, door-close macros; full eject sequence with Z/Y move sent to printer first
- Rack tab — up to 6 slots, SVG visualisation, collision detection, print overlay with filename + height
- Jobs tab — drag-and-drop gcode analysis, grab/park slot assignment, slots locked only when queued
- Queue — automated sequence: grab → load → print → wait → Z200 → eject → store; pause on error, retry from print start
- Mainsail integration via `moonraker.conf`
- WebSocket live updates

---

## Installation

```bash
scp -r OttoBridge/ pi@<pi-ip>:~/
ssh pi@<pi-ip>
cd ~/OttoBridge && bash install.sh
```

Open `http://<pi-ip>:8080` in your browser.

See [INSTALL.md](INSTALL.md) for detailed steps including Windows instructions and troubleshooting.

---

## Klipper Setup

No additional macros needed. `GRAB_FROM_SLOT_N` and `STORE_TO_SLOT_N` are already defined in `storage_calibration_variables.cfg`. Place all OttoEject cfg files in your Klipper config directory and add to `printer.cfg`:

```ini
[include ottoeject_macros.cfg]
[include printer_calibration_variables.cfg]
[include storage_calibration_variables.cfg]
[include _printer_x1c.cfg]   # uncomment your printer
```

## Mainsail Sidebar

```ini
# moonraker.conf
[application OttoBridge]
type: adhoc
website: http://localhost:8080
```

---

## Eject Sequence

OttoBridge sends the park move **to the printer** (via MQTT/PrusaLink/HTTP), then the eject macro to Klipper:

```
1. G1 Z200 F3000   → printer  (CoreXY)
   G1 Y[Ymax] F6000 → printer  (Cartesian: Prusa MK3/MK4)
2. M400            → printer  (wait for move)
3. EJECT_FROM_...  → Klipper  (macro)
4. STORE_TO_SLOT_N → Klipper  (macro)
```

## Macro Names

| Printer | Eject | Load |
|---|---|---|
| Bambu X1C | `EJECT_FROM_BAMBULAB_X_ONE_C` | `LOAD_ONTO_BAMBULAB_X_ONE_C` |
| Bambu P1S / P2S | `EJECT_FROM_BAMBULAB_P_ONE_S` | `LOAD_ONTO_BAMBULAB_P_ONE_S` |
| Bambu P1P | `EJECT_FROM_BAMBULAB_P_ONE_P` | `LOAD_ONTO_BAMBULAB_P_ONE_P` |
| Bambu A1 / A1 Mini | `EJECT_FROM_BAMBULAB_A_ONE` | `LOAD_ONTO_BAMBULAB_A_ONE` |
| Prusa MK3S / MK3 | `EJECT_FROM_PRUSA_MK_THREE_S` | `LOAD_ONTO_PRUSA_MK_THREE_S` |
| Prusa MK4S / MK4 | `EJECT_FROM_PRUSA_MK_FOUR_S` | `LOAD_ONTO_PRUSA_MK_FOUR_S` |
| Prusa Core One | `EJECT_FROM_PRUSA_CORE_ONE` | `LOAD_ONTO_PRUSA_CORE_ONE` |
| Anycubic Kobra S1 | `EJECT_FROM_ANYCUBIC_KOBRA_S_ONE` | `LOAD_ONTO_ANYCUBIC_KOBRA_S_ONE` |
| Elegoo Centauri | `EJECT_FROM_ELEGOO_CC` | `LOAD_ONTO_ELEGOO_CC` |
| Creality K1C | `EJECT_FROM_CREALITY_K_ONE_C` | `LOAD_ONTO_CREALITY_K_ONE_C` |
| FlashForge AD5X | `EJECT_FROM_FLASHFORGE_AD_FIVE_X` | `LOAD_ONTO_FLASHFORGE_AD_FIVE_X` |

---

## License

MIT — Non-Commercial. Free for personal, educational, and community use. Commercial use requires written permission. See [LICENSE](LICENSE).

Not affiliated with Bambu Lab, Prusa, Creality, Anycubic, Elegoo, or FlashForge.
