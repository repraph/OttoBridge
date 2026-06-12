#!/bin/bash
set -e
INSTALL_DIR="/home/pi/ottobridge"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  OttoBridge v2 Installer"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "[1/5] System-Pakete…"
sudo apt-get update -qq
sudo apt-get install -y -qq python3 python3-pip python3-venv
echo "[2/5] Kopiere Dateien → $INSTALL_DIR"
sudo mkdir -p "$INSTALL_DIR"
sudo cp -r ./* "$INSTALL_DIR/"
sudo chown -R pi:pi "$INSTALL_DIR"
echo "[3/5] Python venv…"
cd "$INSTALL_DIR"
python3 -m venv venv
venv/bin/pip install --upgrade pip -q
venv/bin/pip install -r requirements.txt -q
mkdir -p uploads gcode_profiles
echo "[4/5] Systemd Service…"
sudo cp ottobridge.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable ottobridge
sudo systemctl restart ottobridge
echo "[5/5] Fertig!"
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  → http://$(hostname -I | awk '{print $1}'):8080"
echo "  Logs: sudo journalctl -u ottobridge -f"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
