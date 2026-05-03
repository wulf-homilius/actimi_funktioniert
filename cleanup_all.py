"""
Löscht ALLE Communications und Encounters für den Patienten.
Danach kann ein sauberer Sync-Lauf starten.

Aufruf:
    python cleanup_all.py --dry-run   # nur anzeigen
    python cleanup_all.py              # wirklich löschen
"""
import json
import os
import sys
from pathlib import Path

import requests

def load_env(path):
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

load_env(Path(".env"))
user = os.getenv("FHIR_USER_post", "").strip()
pw   = os.getenv("FHIR_PASS_post", "").strip()
if not user or not pw:
    print("ERROR: FHIR_USER_post oder FHIR_PASS_post fehlt!")
    sys.exit(1)

dry_run    = "--dry-run" in sys.argv
auth       = requests.auth.HTTPBasicAuth(user, pw)
base       = "http://194.163.128.215:4010"
headers    = {"Accept": "application/fhir+json", "Content-Type": "application/fhir+json"}
patient_id = "991fe909-0b20-44d0-976b-ecac5841e3bc"

def fetch_all(resource_type, params):
    items = []
    url = f"{base}/{resource_type}"
    p = dict(params)
    while url:
        r = requests.get(url, auth=auth, headers=headers, params=p, timeout=60)
        if r.status_code != 200:
            print(f"ERROR {r.status_code}: {r.text[:200]}")
            break
        data = r.json()
        for entry in data.get("entry", []) or []:
            res = entry.get("resource", {})
            if res.get("resourceType") == resource_type:
                items.append(res)
        url = None
        p = {}
        for link in data.get("link", []) or []:
            if link.get("relation") == "next":
                url = link.get("url")
                break
    return items

def delete_all(resource_type, items):
    deleted = errors = 0
    for res in items:
        rid = res.get("id", "")
        print(f"  {'[DRY] ' if dry_run else ''}DELETE {resource_type}/{rid}")
        if not dry_run:
            r = requests.delete(f"{base}/{resource_type}/{rid}", auth=auth, headers=headers, timeout=30)
            if r.status_code in (200, 204, 404):
                deleted += 1
            else:
                print(f"    ERROR {r.status_code}: {r.text[:100]}")
                errors += 1
    return deleted, errors

# Communications
print(f"\n{'[DRY-RUN] ' if dry_run else ''}Lade Communications...")
comms = fetch_all("Communication", {"subject": f"Patient/{patient_id}", "_count": "200"})
print(f"Gefunden: {len(comms)} Communications")
d, e = delete_all("Communication", comms)
if not dry_run:
    print(f"Communications gelöscht: {d}, Fehler: {e}")

# Encounters
print(f"\n{'[DRY-RUN] ' if dry_run else ''}Lade Encounters...")
encs = fetch_all("Encounter", {"subject": f"Patient/{patient_id}", "_count": "200"})
print(f"Gefunden: {len(encs)} Encounters")
d, e = delete_all("Encounter", encs)
if not dry_run:
    print(f"Encounters gelöscht: {d}, Fehler: {e}")

print("\nFertig!" if not dry_run else "\n[DRY-RUN] Fertig - nichts wurde gelöscht.")
