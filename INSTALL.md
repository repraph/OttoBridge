# OttoBridge — Installation Guide

This guide walks you through installing OttoBridge on a Raspberry Pi Zero 2 W that already has Klipper and Moonraker running (e.g. via MainsailOS).

---

## What you need

- Raspberry Pi Zero 2 W running **MainsailOS** (or any Klipper + Moonraker setup)
- A computer on the same Wi-Fi network
- Your printer's IP address and credentials (Access Code for Bambu Lab, API Key for Prusa, etc.)

---

## Step 1 — Get OttoBridge onto the Pi

SSH into your Pi:

```bash
ssh <user>@<your-pi-ip>
```

Replace `<user>` and `<your-pi-ip>` with your login (e.g. `pi@192.168.1.50`). You can find the IP in your router or in Mainsail under the hostname.

Then clone the repository:

```bash
cd ~
git clone https://github.com/repraph/OttoBridge.git
```

This creates a folder `~/OttoBridge` with everything needed.

> **No internet access on the Pi / prefer manual copy?** Download `OttoBridge_repo.zip` from the [Releases page](https://github.com/repraph/OttoBridge/releases), unzip it on your computer, then copy it over:
> ```bash
> scp -r OttoBridge/ <user>@<your-pi-ip>:~/
> ```
> **Windows users:** Use [WinSCP](https://winscp.net) to drag and drop the OttoBridge folder onto the Pi via SFTP.

---

## Step 2 — Run the installer

You should still be connected via SSH. Run the installer:

```bash
cd ~/OttoBridge
bash install.sh
```

The installer will:
1. Install Python dependencies
2. Create a Python virtual environment
3. Install OttoBridge as a systemd service that starts automatically on boot

When it's done, you'll see something like:

```
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  → http://192.168.1.50:8080
  Logs: sudo journalctl -u ottobridge -f
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
```

---

## Step 3 — Add OttoBridge to Mainsail (do this now)

Before going further, add OttoBridge as a link in Mainsail's sidebar so you can always find it.

Create the theme folder if it doesn't exist yet:

```bash
mkdir -p ~/printer_data/config/.theme
nano ~/printer_data/config/.theme/navi.json
```

Add the following content:

```json
[
  {
    "title": "OttoBridge",
    "href": "http://ottoeject.local:8080",
    "target": "_blank",
    "position": 95
  }
]
```

Save and exit (`Ctrl+O`, `Enter`, `Ctrl+X`). Reload Mainsail in your browser — OttoBridge now appears as a link near the bottom of the sidebar and opens in a new tab.

> Replace `ottoeject.local` with your Pi's actual hostname or IP address. `localhost` does not work here, since the link is opened by your browser, not by the Pi itself.

> If you already have other entries in `navi.json`, add the OttoBridge object to the existing array instead of replacing the file.

---

## Step 4 — Open OttoBridge

Open a browser and go to:

```
http://<your-pi-ip>:8080
```

You should see the OttoBridge dashboard.

---

## Step 5 — Connect your printer

Go to the **Printer** tab and fill in your printer details:

**Bambu Lab (X1C, P1S, P1P, A1, P2S):**
- Brand: `Bambu Lab`
- Model: select your model
- IP Address: your printer's IP (shown in printer Settings → Network)
- Access Code: shown in printer Settings → LAN (enable LAN Mode first)
- Serial Number: shown in printer Settings → Device

**Prusa (MK3S, MK4S, Core One):**
- Brand: `Prusa`
- IP Address: your printer's IP
- API Key: shown in Mainsail/Fluidd → PrusaLink settings

**Anycubic / Elegoo / Creality:**
- Brand: select your brand
- IP Address: your printer's IP
- No additional credentials needed (Klipper/Moonraker handles auth)

Click **Connect**. The status dot turns green when the connection is successful.

---

## Step 6 — Connect OttoEject (Moonraker)

Go to the **OttoEject** tab and click **Connect**. OttoBridge connects to Moonraker on `http://localhost:7125` by default — this is already correct if OttoBridge runs on the same Pi as Klipper.

If Klipper runs on a different machine, change the Moonraker URL to `http://<klipper-pi-ip>:7125`.

---

## Step 7 — Calibrate OttoEject

Before using the rack for the first time:

1. Go to the **Printer** tab → **OttoEject Calibration**
2. Click **OttoEject Home** — this runs `OTTOEJECT_HOME` which homes all axes and moves to the starting position
3. Click **Z → 200mm** (CoreXY) or **Y → Ymax** (Cartesian) to move to the calibration position
4. Follow the standard OttoEject calibration procedure in Mainsail

---

## Step 8 — Set up your rack slots

Go to the **Rack** tab. You'll see 6 slots (Slot 1 at the bottom). For each slot that has a print plate loaded:

- Click **+ Insert plate**

This tells OttoBridge which slots have plates ready for printing.

---

## Step 9 — Add your first print job

Go to the **Jobs** tab:

1. Drag and drop a `.gcode` or `.3mf` file onto the drop zone — OttoBridge reads the print height automatically
2. Choose where to get the plate from:
   - Toggle **"Plate already in printer"** if a plate is already on the bed
   - Or turn it off and select which rack slot to grab the plate from
3. Choose a **Park slot** — where to store the plate after printing
4. Click **+ Queue — lock slots**

Repeat for each job. Then click **▶ Start queue**.

---

## Troubleshooting

**OttoBridge doesn't start:**
```bash
sudo systemctl status ottobridge
sudo journalctl -u ottobridge -f
```

**Can't connect to Bambu printer:**
- Make sure **LAN Mode** is enabled on the printer (Settings → Network → LAN Mode)
- Double-check the Access Code and Serial Number
- Bambu printers only allow a few simultaneous connections — close Bambu Studio/Handy first

**Gcode height not detected:**
- Make sure your slicer adds height comments. In OrcaSlicer/BambuStudio this is on by default (`;MAX_LAYER_Z:`)
- PrusaSlicer: enable "Verbose G-code" in Print Settings

**Slots not freeing after print:**
- In the Rack tab, click **Print removed ✓** on the slot with the finished print
- This removes the print overlay and frees any blocked slots above

**Updating OttoBridge:**
```bash
cd ~/OttoBridge
git pull   # or re-copy files manually
sudo systemctl restart ottobridge
```

---

## File locations on the Pi

| Path | What |
|---|---|
| `/home/pi/ottobridge/` | OttoBridge installation |
| `/home/pi/ottobridge/uploads/` | Uploaded gcode files |
| `/home/pi/ottobridge/config.json` | Saved printer and rack configuration |
| `/etc/systemd/system/ottobridge.service` | Systemd service |

---

## Uninstalling

```bash
sudo systemctl stop ottobridge
sudo systemctl disable ottobridge
sudo rm /etc/systemd/system/ottobridge.service
sudo systemctl daemon-reload
rm -rf ~/ottobridge
```
