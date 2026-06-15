#!/usr/bin/env python3
"""
OttoBridge — GitHub Push via REST API
Kein git, kein Xcode nötig. Läuft mit Python 3 (vorinstalliert auf macOS).

Usage:
    python3 push_to_github.py
"""

import base64
import getpass
import json
import os
import urllib.request
import urllib.error
from pathlib import Path

REPO  = "repraph/OttoBridge"
BRANCH = "main"
API   = "https://api.github.com"

# Files to push — relative to script location
FILES = [
    "app.py",
    "requirements.txt",
    "install.sh",
    "ottobridge.service",
    "README.md",
    "INSTALL.md",
    "LICENSE",
    ".gitignore",
    "static/index.html",
    "uploads/.gitkeep",
    "docs/logo.png",
]

COMMIT_MSG = "OttoBridge v2 - multi-printer orchestrator with rack management"

# ── Helpers ────────────────────────────────────────────────────────────────────

def gh(method, path, token, data=None):
    url = f"{API}{path}"
    body = json.dumps(data).encode() if data else None
    req = urllib.request.Request(
        url, data=body, method=method,
        headers={
            "Authorization": f"token {token}",
            "Accept": "application/vnd.github.v3+json",
            "Content-Type": "application/json",
            "User-Agent": "OttoBridge-pusher/1.0",
        }
    )
    try:
        with urllib.request.urlopen(req) as r:
            return json.loads(r.read()), r.status
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        return json.loads(body) if body else {}, e.code

def encode(path):
    with open(path, "rb") as f:
        return base64.b64encode(f.read()).decode()

def get_sha(token, repo_path):
    """Get existing file SHA (needed for updates)."""
    data, status = gh("GET", f"/repos/{REPO}/contents/{repo_path}", token)
    return data.get("sha") if status == 200 else None

def ensure_gitkeep(local_path):
    """Create .gitkeep if it doesn't exist."""
    p = Path(local_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    if not p.exists():
        p.write_text("")

# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    base = Path(__file__).parent

    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    print("  OttoBridge → GitHub Push (REST API)")
    print(f"  Repo: {REPO}")
    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    print()

    token = getpass.getpass("GitHub Personal Access Token (repo scope): ").strip()
    if not token:
        print("Abbruch: kein Token eingegeben.")
        return

    # Verify token
    print("\nToken wird geprüft…")
    data, status = gh("GET", f"/repos/{REPO}", token)
    if status == 404:
        print(f"✗ Repository '{REPO}' nicht gefunden.")
        print("  → Erstelle es auf github.com/new (leer, ohne README)")
        return
    elif status == 401:
        print("✗ Token ungültig oder abgelaufen.")
        return
    elif status != 200:
        print(f"✗ GitHub Fehler {status}: {data.get('message','')}")
        return

    print(f"✓ Verbunden mit {REPO}")
    print()

    # Ensure .gitkeep files exist
    ensure_gitkeep(base / "uploads" / ".gitkeep")
    ensure_gitkeep(base / "gcode_profiles" / ".gitkeep")

    # Push each file
    ok = 0
    skip = 0
    errors = []

    for rel in FILES:
        local = base / rel
        if not local.exists():
            print(f"  — Übersprungen (nicht gefunden): {rel}")
            skip += 1
            continue

        try:
            content = encode(local)
            sha = get_sha(token, rel)

            payload = {
                "message": COMMIT_MSG if ok == 0 else f"Add {rel}",
                "content": content,
                "branch":  BRANCH,
            }
            if sha:
                payload["sha"] = sha

            _, status = gh("PUT", f"/repos/{REPO}/contents/{rel}", token, payload)

            if status in (200, 201):
                action = "aktualisiert" if sha else "erstellt"
                print(f"  ✓ {rel} ({action})")
                ok += 1
            else:
                print(f"  ✗ {rel} — Status {status}")
                errors.append(rel)
        except Exception as e:
            print(f"  ✗ {rel} — {e}")
            errors.append(rel)

    print()
    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    print(f"  ✓ {ok} Dateien gepusht")
    if skip:  print(f"  — {skip} übersprungen")
    if errors: print(f"  ✗ {len(errors)} Fehler: {', '.join(errors)}")
    print(f"\n  → https://github.com/{REPO}")
    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

if __name__ == "__main__":
    main()
