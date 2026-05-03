from __future__ import annotations

import os
import re
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import import_requests as ir

PATIENT_URL = "https://ovok.api.actimi.health/v2/partner/Patient"
OBS_URL = "https://ovok.api.actimi.health/v2/partner/Observation"


def iter_resources(payload: Any) -> Iterable[Dict[str, Any]]:
    if isinstance(payload, list):
        for item in payload:
            if isinstance(item, dict):
                yield item
        return
    if isinstance(payload, dict):
        entry = payload.get("entry")
        if isinstance(entry, list):
            for item in entry:
                if not isinstance(item, dict):
                    continue
                resource = item.get("resource")
                if isinstance(resource, dict):
                    yield resource
            return
        yield payload


def fetch_json(url: str, token: str, params: Optional[Dict[str, Any]] = None) -> Any:
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {token}",
    }
    response = ir.SESSION.get(url, headers=headers, params=params, timeout=30)
    response.raise_for_status()
    return response.json()


def fetch_patients(token: str) -> Dict[str, str]:
    payload = fetch_json(PATIENT_URL, token)
    patient_map: Dict[str, str] = {}
    for patient in iter_resources(payload):
        if patient.get("resourceType") not in (None, "Patient"):
            continue
        patient_id = str(patient.get("id") or "").strip()
        if not patient_id:
            continue

        given_name = ""
        for name in patient.get("name", []) or []:
            if not isinstance(name, dict):
                continue
            for given in name.get("given", []) or []:
                if isinstance(given, str) and given.strip():
                    given_name = given.strip()
                    break
            if given_name:
                break

        patient_map[patient_id] = given_name or "<kein given-name>"
    return patient_map


def normalize_dt(value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        return "unbekannte Zeit"
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return value
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")


def parse_patient_ref(value: str) -> Optional[str]:
    text = value.strip()
    if not text:
        return None
    if text.startswith("Patient/"):
        return text.split("/", 1)[1].strip() or None
    match = re.search(r"(?:^|/)Patient/([^/?#]+)", text)
    if match:
        return match.group(1).strip() or None
    return None


def subject_patient_id(observation: Dict[str, Any]) -> Optional[str]:
    subject = observation.get("subject") or {}
    if not isinstance(subject, dict):
        return None

    subject_id = subject.get("id")
    if isinstance(subject_id, str) and subject_id.strip():
        return subject_id.strip()

    reference = subject.get("reference")
    if isinstance(reference, str):
        parsed = parse_patient_ref(reference)
        if parsed:
            return parsed

    identifier = subject.get("identifier")
    if isinstance(identifier, dict):
        for key in ("value", "id", "reference"):
            val = identifier.get(key)
            if isinstance(val, str) and val.strip():
                parsed = parse_patient_ref(val)
                if parsed:
                    return parsed
                return val.strip()
    return None


def code_values(observation: Dict[str, Any]) -> set[str]:
    values: set[str] = set()
    code = observation.get("code") or {}
    if isinstance(code, dict):
        text = code.get("text")
        if isinstance(text, str) and text.strip():
            values.add(text.strip().casefold())
        for coding in code.get("coding", []) or []:
            if isinstance(coding, dict):
                val = coding.get("code")
                if isinstance(val, str) and val.strip():
                    values.add(val.strip())
    return values


def _format_value_unit(value: Any, unit: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(unit, str) and unit.strip():
        return f"{value} {unit.strip()}"
    return str(value)


def quantity_text(node: Dict[str, Any]) -> Optional[str]:
    value_quantity = node.get("valueQuantity")
    if isinstance(value_quantity, dict):
        out = _format_value_unit(
            value_quantity.get("value"),
            value_quantity.get("unit") or value_quantity.get("code"),
        )
        if out:
            return out

    # Actimi variant from your sample: value.Quantity
    value_obj = node.get("value")
    if isinstance(value_obj, dict):
        quantity = value_obj.get("Quantity") or value_obj.get("quantity")
        if isinstance(quantity, dict):
            out = _format_value_unit(
                quantity.get("value"),
                quantity.get("unit") or quantity.get("code"),
            )
            if out:
                return out

    return None


def blood_pressure_text(observation: Dict[str, Any]) -> Optional[str]:
    systolic: Optional[str] = None
    diastolic: Optional[str] = None

    for component in observation.get("component", []) or []:
        if not isinstance(component, dict):
            continue
        comp_codes = set()
        code = component.get("code") or {}
        if isinstance(code, dict):
            for coding in code.get("coding", []) or []:
                if isinstance(coding, dict):
                    c = coding.get("code")
                    if isinstance(c, str) and c.strip():
                        comp_codes.add(c.strip())
        q = quantity_text(component)
        if not q:
            continue
        if "8480-6" in comp_codes:
            systolic = q
        if "8462-4" in comp_codes:
            diastolic = q

    if systolic or diastolic:
        return f"{systolic or '?'} / {diastolic or '?'}"

    return quantity_text(observation)


def classify_observation(observation: Dict[str, Any]) -> Optional[tuple[str, str]]:
    codes = code_values(observation)

    if {
        "8867-4",
        "8885-6",
        "heart-rate",
        "heartrate",
        "heart rate",
        "blood pressure heart rate",
    }.intersection(codes):
        value = quantity_text(observation)
        if value:
            return ("Heart Rate", value)

    if {"85354-9", "55284-4", "blood-pressure", "blood pressure", "8480-6", "8462-4"}.intersection(codes):
        value = blood_pressure_text(observation)
        if value:
            return ("Blood Pressure", value)

    if isinstance(observation.get("component"), list):
        bp = blood_pressure_text(observation)
        if bp:
            return ("Blood Pressure", bp)

    return None


def fetch_observations(token: str) -> List[Dict[str, Any]]:
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    queries = [
        {"date": ["ge2024-01-01T00:00:00Z", f"le{now_iso}"], "code": "8867-4", "_sort": "date"},
        {"date": ["ge2024-01-01T00:00:00Z", f"le{now_iso}"], "code": "8885-6", "_sort": "date"},
        {"date": ["ge2024-01-01T00:00:00Z", f"le{now_iso}"], "code": "85354-9", "_sort": "date"},
        {"date": ["ge2024-01-01T00:00:00Z", f"le{now_iso}"], "code": "55284-4", "_sort": "date"},
        {"code": "heart-rate", "sort": "asc"},
        {"code": "blood-pressure", "sort": "asc"},
        {"sort": "asc"},
        {},
    ]

    collected: Dict[str, Dict[str, Any]] = {}
    for params in queries:
        try:
            payload = fetch_json(OBS_URL, token, params=params or None)
        except Exception:
            continue
        for obs in iter_resources(payload):
            if obs.get("resourceType") not in (None, "Observation"):
                continue
            obs_id = str(obs.get("id") or "").strip()
            if obs_id:
                collected[obs_id] = obs
            else:
                collected[f"anon-{len(collected)+1}"] = obs

    return list(collected.values())


def render_readable_output(patient_map: Dict[str, str], observations: List[Dict[str, Any]]) -> str:
    grouped: Dict[str, List[str]] = defaultdict(list)

    for obs in observations:
        patient_id = subject_patient_id(obs)
        if not patient_id:
            continue
        metric = classify_observation(obs)
        if not metric:
            continue
        metric_name, metric_value = metric
        ts_raw = (
            obs.get("effectiveDateTime")
            or ((obs.get("effective") or {}).get("dateTime"))
            or ((obs.get("meta") or {}).get("lastUpdated"))
        )
        ts = normalize_dt(ts_raw)
        grouped[patient_id].append(f"- {ts}: {metric_name} = {metric_value}")

    lines: List[str] = []
    for patient_id in sorted(grouped.keys()):
        given = patient_map.get(patient_id, "<given-name unbekannt>")
        lines.append(f"Patient ID: {patient_id}")
        lines.append(f"Given Name: {given}")
        lines.extend(grouped[patient_id])
        lines.append("")

    if not grouped:
        return "Keine passenden Heart-Rate/Blood-Pressure Observations gefunden."
    return "\n".join(lines).rstrip() + "\n"


def main() -> int:
    root = Path(__file__).resolve().parent
    ir.load_env_file(root / "config" / ".env")
    api_key = os.getenv("ACTIMI_API_KEY", "O2SUM53XRT24E6XD1LWZDEHT")

    token = ir.get_token(api_key)
    patients = fetch_patients(token)
    observations = fetch_observations(token)

    text = render_readable_output(patients, observations)
    print(text)

    out_path = root / "patient_observations_readable2.txt"
    out_path.write_text(text, encoding="utf-8")
    print(f"Ausgabe gespeichert in: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
