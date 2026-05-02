"""
Kleines Diagnoseskript – liest Encounter und Communication aus Sensdoc
und zeigt die rohe JSON-Antwort.

Aufruf:
    python check_sensdoc.py

Credentials werden aus der .env-Datei geladen (FHIR_USER_post / FHIR_PASS_post).
"""
import json
import os
import sys
from pathlib import Path

import requests

# .env laden (dieselbe Logik wie config_loader)
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

auth    = requests.auth.HTTPBasicAuth(user, pw)
base    = "http://194.163.128.215:4010"
headers = {"Accept": "application/fhir+json"}

ENCOUNTER_ID = "3edacfd7-d83a-448e-9f15-f10838ec97b8"

print("=" * 60)
print(f"GET /Encounter/{ENCOUNTER_ID}")
print("=" * 60)
r = requests.get(f"{base}/Encounter/{ENCOUNTER_ID}", auth=auth, headers=headers, timeout=30)
print(f"Status: {r.status_code}")
try:
    print(json.dumps(r.json(), indent=2, ensure_ascii=False))
except Exception:
    print(r.text)

print()
print("=" * 60)
print(f"GET /Communication?encounter=Encounter/{ENCOUNTER_ID}")
print("=" * 60)
r2 = requests.get(
    f"{base}/Communication",
    auth=auth,
    headers=headers,
    params={"encounter": f"Encounter/{ENCOUNTER_ID}"},
    timeout=30,
)
print(f"Status: {r2.status_code}")
try:
    print(json.dumps(r2.json(), indent=2, ensure_ascii=False))
except Exception:
    print(r2.text)

print()
print("=" * 60)
print("GET /Communication?category=http://www.nursiti.com/notificationType|vitalsign&subject=Patient/d7fa4c18-5adf-46fa-9534-49f9f79b92ff")
print("=" * 60)
r3 = requests.get(
    f"{base}/Communication",
    auth=auth,
    headers=headers,
    params={
        "subject": "Patient/d7fa4c18-5adf-46fa-9534-49f9f79b92ff",
        "category": "http://www.nursiti.com/notificationType|vitalsign",
        "_count": "3",
        "_sort": "-sent",
    },
    timeout=30,
)
print(f"Status: {r3.status_code}")
try:
    data = r3.json()
    print(f"Total: {data.get('total', '?')}")
    for entry in (data.get("entry") or [])[:2]:
        print(json.dumps(entry.get("resource", {}), indent=2, ensure_ascii=False))
except Exception:
    print(r3.text)
