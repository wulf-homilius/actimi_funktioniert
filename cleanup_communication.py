"""
Löscht alle Communications ohne 'category' für den Patienten.
Nur Communications MIT category (vitalsign) bleiben erhalten.

Aufruf:
    python cleanup_communications.py --dry-run   # nur anzeigen
    python cleanup_communications.py              # wirklich löschen
"""
import json
import os
import sys
from pathlib import Path

import requests

def load_env(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value

root = Path(__file__).resolve().parent
load_env(root / ".env")

user = os.getenv("FHIR_USER_post", "").strip()
pw   = os.getenv("FHIR_PASS_post", "").strip()

if not user or not pw:
    print("ERROR: FHIR_USER_post oder FHIR_PASS_post nicht gesetzt!")
    sys.exit(1)

dry_run = "--dry-run" in sys.argv
auth    = requests.auth.HTTPBasicAuth(user, pw)
base    = "http://194.163.128.215:4010"
headers = {"Accept": "application/fhir+json", "Content-Type": "application/fhir+json"}
patient_id = "d7fa4c18-5adf-46fa-9534-49f9f79b92ff"

print(f"{'[DRY-RUN] ' if dry_run else ''}Lade alle Communications für Patient {patient_id}...")

# Alle Communications laden (paginiert)
all_comms = []
url = f"{base}/Communication"
params = {"subject": f"Patient/{patient_id}", "_count": "200"}

while url:
    r = requests.get(url, auth=auth, headers=headers, params=params, timeout=60)
    if r.status_code != 200:
        print(f"ERROR: {r.status_code} {r.text}")
        sys.exit(1)
    data = r.json()
    for entry in data.get("entry", []) or []:
        resource = entry.get("resource", {})
        if resource.get("resourceType") == "Communication":
            all_comms.append(resource)
    # Nächste Seite
    url = None
    params = {}
    for link in data.get("link", []) or []:
        if link.get("relation") == "next":
            url = link.get("url")
            break

print(f"Gefunden: {len(all_comms)} Communications gesamt")

# Aufteilen: mit und ohne category
with_cat    = [c for c in all_comms if c.get("category")]
without_cat = [c for c in all_comms if not c.get("category")]

print(f"  Mit category (behalten):  {len(with_cat)}")
print(f"  Ohne category (löschen):  {len(without_cat)}")

if not without_cat:
    print("Nichts zu löschen!")
    sys.exit(0)

print()
deleted = 0
errors  = 0
for comm in without_cat:
    cid = comm.get("id", "")
    enc = (comm.get("encounter") or {}).get("reference", "")
    sent = comm.get("sent", "")
    payload = ", ".join(p.get("contentString", "") for p in comm.get("payload", []))
    print(f"  {'[DRY] ' if dry_run else ''}DELETE Communication/{cid}  enc={enc}  sent={sent}  payload={payload}")
    if not dry_run:
        r = requests.delete(f"{base}/Communication/{cid}", auth=auth, headers=headers, timeout=30)
        if r.status_code in (200, 204, 404):
            deleted += 1
        else:
            print(f"    ERROR {r.status_code}: {r.text[:200]}")
            errors += 1

print()
if dry_run:
    print(f"[DRY-RUN] Würde {len(without_cat)} Communications löschen.")
    print("Starten Sie ohne --dry-run um wirklich zu löschen.")
else:
    print(f"Gelöscht: {deleted}, Fehler: {errors}")
    print("Fertig! Starten Sie jetzt main_actimi_to_sensdoc.py für einen sauberen Lauf.")