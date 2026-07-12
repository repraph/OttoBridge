#!/bin/bash
set -e

INSTALL_DIR="/home/pi/ottobridge"
SOURCE_DIR="$(pwd)"

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  OttoBridge v2 Installer"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

echo "[1/6] System-Pakete…"
sudo apt-get update -qq
sudo apt-get install -y -qq python3 python3-pip python3-venv rsync

echo "[2/6] Stoppe laufenden Service (falls vorhanden)…"
sudo systemctl stop ottobridge 2>/dev/null || true

echo "[3/6] Synchronisiere Dateien → $INSTALL_DIR"
sudo mkdir -p "$INSTALL_DIR"
sudo rsync -a --delete \
  --exclude '.git' \
  --exclude 'venv' \
  --exclude 'uploads' \
  --exclude 'gcode_profiles' \
  "$SOURCE_DIR"/ "$INSTALL_DIR"/
sudo chown -R pi:pi "$INSTALL_DIR"

echo "[4/6] Python venv…"
cd "$INSTALL_DIR"
if [ ! -d "venv" ]; then
  python3 -m venv venv
fi
venv/bin/pip install --upgrade pip -q
venv/bin/pip install -r requirements.txt -q
mkdir -p uploads gcode_profiles

echo "[5/6] Systemd Service…"
sudo cp ottobridge.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable ottobridge
sudo systemctl restart ottobridge

echo "[6/6] Fertig!"
sleep 1
sudo systemctl status ottobridge --no-pager
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  → http://$(hostname -I | awk '{print $1}'):8080"
echo "  Logs: sudo journalctl -u ottobridge -f"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
