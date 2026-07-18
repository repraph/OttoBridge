#!/bin/bash
set -e

INSTALL_DIR="/home/pi/ottobridge"
SOURCE_DIR="$(pwd)"

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  OttoBridge v2 Installer"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

echo "[1/7] System packages…"
sudo apt-get update -qq
sudo apt-get install -y -qq python3 python3-pip python3-venv rsync

echo "[2/7] Stopping running service (if any)…"
sudo systemctl stop ottobridge 2>/dev/null || true

echo "[3/7] Syncing files → $INSTALL_DIR"
sudo mkdir -p "$INSTALL_DIR"
sudo rsync -a --delete \
  --exclude '.git' \
  --exclude 'venv' \
  --exclude 'uploads' \
  --exclude 'gcode_profiles' \
  "$SOURCE_DIR"/ "$INSTALL_DIR"/
sudo chown -R pi:pi "$INSTALL_DIR"

echo "[4/7] Python venv…"
cd "$INSTALL_DIR"
if [ ! -d "venv" ]; then
  python3 -m venv venv
fi
venv/bin/pip install --upgrade pip -q
venv/bin/pip install -r requirements.txt -q
mkdir -p uploads gcode_profiles

echo "[5/7] Systemd service…"
sudo cp ottobridge.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable ottobridge
sudo systemctl restart ottobridge

echo "[6/7] Mainsail sidebar link (navi.json)…"
NAVI_DIR="/home/pi/printer_data/config/.theme"
NAVI_FILE="$NAVI_DIR/navi.json"
# .local (mDNS) instead of a raw IP, since the link is opened by the browser
# on the person's own machine, not by the Pi itself — a DHCP-assigned IP can
# change, but the mDNS hostname stays stable. Requires avahi-daemon, which
# ships by default on Raspberry Pi OS.
PI_HOST="$(hostname).local"
if [ -d "/home/pi/printer_data/config" ]; then
  mkdir -p "$NAVI_DIR"
  python3 - "$NAVI_FILE" "$PI_HOST" << 'PYEOF'
import json, sys, os

navi_file, host = sys.argv[1], sys.argv[2]
entry = {"title": "OttoBridge", "href": f"http://{host}:8080", "target": "_blank", "position": 95}

data = []
if os.path.exists(navi_file):
    try:
        with open(navi_file) as f:
            loaded = json.load(f)
        if isinstance(loaded, list):
            data = loaded
    except (json.JSONDecodeError, ValueError):
        # Existing file wasn't valid JSON — back it up instead of silently
        # discarding whatever the person had in there.
        backup = navi_file + ".bak"
        os.rename(navi_file, backup)
        print(f"  (navi.json wasn't valid JSON — backed up to {backup})")

# Drop any previous OttoBridge entry (e.g. from an earlier install with a
# different IP/hostname) before adding the current one fresh, so re-running
# this script never creates duplicates.
data = [e for e in data if not (isinstance(e, dict) and e.get("title") == "OttoBridge")]
data.append(entry)

with open(navi_file, "w") as f:
    json.dump(data, f, indent=2)
print(f"  → {navi_file} updated ({entry['href']})")
PYEOF
else
  echo "  ~/printer_data/config not found — skipping (Mainsail/Moonraker not detected here)"
fi

echo "[7/7] Done!"
sleep 1
sudo systemctl status ottobridge --no-pager
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  → http://$(hostname -I | awk '{print $1}'):8080"
echo "  Logs: sudo journalctl -u ottobridge -f"
echo "  Mainsail: reload the sidebar to see the OttoBridge link"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
