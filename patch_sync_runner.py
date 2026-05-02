"""Patcht create_communication in sync_runner.py um category und reasonCode hinzuzufügen."""
from pathlib import Path

path = Path("sync_runner.py")
content = path.read_text(encoding="utf-8")

old = (
    '    communication: Dict[str, Any] = {\n'
    '        "resourceType": "Communication",\n'
    '        "status": "in-progress",\n'
    '        "subject": {"reference": f"Patient/{patient_id}"},\n'
    '        "sent": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),\n'
    '        "about": [{"reference": f"Observation/{oid}"} for oid in observation_ids],\n'
    '        "payload": [{"contentString": settings.communication_payloads.get(code, "Vitalzeichen")} for code in observation_codes],\n'
    '    }\n'
    '    if encounter_ref:\n'
    '        communication["encounter"] = {"reference": encounter_ref}'
)

new = (
    '    category_coding: Dict[str, Any] = {\n'
    '        "system": settings.communication_category_system,\n'
    '        "code": settings.communication_category_code or "vitalsign",\n'
    '    }\n'
    '    if settings.communication_category_display:\n'
    '        category_coding["display"] = settings.communication_category_display\n'
    '    communication: Dict[str, Any] = {\n'
    '        "resourceType": "Communication",\n'
    '        "status": settings.communication_status,\n'
    '        "category": [{"coding": [category_coding], "text": "Vital"}],\n'
    '        "subject": {"reference": f"Patient/{patient_id}"},\n'
    '        "sent": datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"),\n'
    '        "about": [{"reference": f"Observation/{oid}"} for oid in observation_ids],\n'
    '        "reasonCode": [{"coding": [{"system": "http://www.nursiti.com/notificationReason", "code": "add"}], "text": "Ein neues Vitalzeichen wurde hinzugef\u00fcgt"}],\n'
    '        "payload": [{"contentString": settings.communication_payloads.get(code, "Vitalzeichen")} for code in observation_codes],\n'
    '    }\n'
    '    if encounter_ref:\n'
    '        communication["encounter"] = {"reference": encounter_ref}'
)

if old in content:
    path.write_text(content.replace(old, new), encoding="utf-8")
    print("Erfolgreich gepatcht!")
else:
    print("FEHLER: Text nicht gefunden!")
    idx = content.find("def create_communication")
    if idx >= 0:
        print("Gefundener Text um create_communication:")
        print(repr(content[idx:idx+600]))
    else:
        print("create_communication nicht gefunden!")
