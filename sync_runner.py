from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from threading import Lock
from typing import Any, Dict, List, Optional, Tuple

import requests

from config_loader import Settings
from fhir_client import FHIR_HEADERS, SESSION, http_get_json, iter_resources
from transformations import (
    build_target_observation,
    expand_observation_for_transfer,
    extract_effective_datetime,
    extract_observation_value,
    has_observation_value,
    observation_transfer_code,
)

_STATS_LOCK = Lock()


def parse_dt(value: Any) -> Optional[datetime]:
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def observation_in_window(observation: Dict[str, Any], *, from_time: datetime) -> bool:
    candidate = parse_dt(extract_effective_datetime(observation))
    return bool(candidate and candidate >= from_time)


def second_given_name(patient: Dict[str, Any]) -> Optional[str]:
    for name in patient.get("name", []) or []:
        if not isinstance(name, dict):
            continue
        givens = name.get("given", []) or []
        if len(givens) > 1 and isinstance(givens[1], str):
            value = givens[1].strip()
            if value:
                return value
    return None


def all_given_names(patient: Dict[str, Any]) -> List[str]:
    out: List[str] = []
    for name in patient.get("name", []) or []:
        if not isinstance(name, dict):
            continue
        for value in name.get("given", []) or []:
            if isinstance(value, str):
                v = value.strip()
                if v and v not in out:
                    out.append(v)
    return out


def fetch_patients(base_url: str, auth: Optional[requests.auth.HTTPBasicAuth], page_count: int, headers: Optional[Dict[str, str]] = None) -> List[Dict[str, Any]]:
    last_error: Optional[Exception] = None
    for params in ([("_count", str(page_count))], None):
        try:
            payload = http_get_json(f"{base_url}/Patient", auth, params=params, headers=headers)
            return [item for item in iter_resources(payload) if item.get("resourceType") == "Patient"]
        except RuntimeError as exc:
            last_error = exc
            continue
    if last_error:
        raise last_error
    return []


def fetch_patient_map_by_given(
    base_url: str,
    auth: Optional[requests.auth.HTTPBasicAuth],
    page_count: int,
    headers: Optional[Dict[str, str]] = None,
    second_given_only: bool = False,
) -> Tuple[Dict[str, Dict[str, Any]], List[Dict[str, Any]]]:
    all_patients = fetch_patients(base_url, auth, page_count, headers=headers)
    out: Dict[str, Dict[str, Any]] = {}
    for patient in all_patients:
        if second_given_only:
            given = second_given_name(patient)
            if given and given not in out:
                out[given] = patient
        else:
            for given in all_given_names(patient):
                if given and given not in out:
                    out[given] = patient
    return out, all_patients


def find_sensdoc_patient_by_given_alias(patients: List[Dict[str, Any]], given_value: str) -> Optional[Dict[str, Any]]:
    matches: List[Dict[str, Any]] = []
    needle = given_value.strip().lower()
    for patient in patients:
        given = second_given_name(patient)
        if isinstance(given, str) and given.strip().lower() == needle:
            matches.append(patient)
    return matches[0] if len(matches) == 1 else None


def ensure_second_given_name(
    patient: Dict[str, Any],
    second_given: str,
    base_url: str,
    auth: requests.auth.HTTPBasicAuth,
    dry_run: bool,
) -> bool:
    if not second_given or not isinstance(second_given, str):
        return False
    second_given = second_given.strip()
    if not second_given:
        return False

    current = second_given_name(patient)
    if current and current.strip().lower() == second_given.lower():
        return False

    updated = json.loads(json.dumps(patient))
    names = updated.get("name")
    if not isinstance(names, list) or not names:
        updated["name"] = [{"given": ["", second_given]}]
    else:
        first_name = names[0]
        if not isinstance(first_name, dict):
            names[0] = {"given": ["", second_given]}
        else:
            givens = first_name.get("given")
            if not isinstance(givens, list):
                first_name["given"] = ["", second_given]
            elif len(givens) == 0:
                first_name["given"] = ["", second_given]
            elif len(givens) == 1:
                if str(givens[0]).strip().lower() == second_given.lower():
                    first_name["given"] = [str(givens[0]).strip(), second_given]
                else:
                    first_name["given"] = [str(givens[0]).strip(), second_given]
            else:
                first_name["given"][1] = second_given

    patient_id = str(updated.get("id") or "").strip()
    if not patient_id:
        return False
    if dry_run:
        return True

    resp = SESSION.put(
        f"{base_url}/Patient/{patient_id}",
        auth=auth,
        headers=FHIR_HEADERS,
        data=json.dumps(updated),
        timeout=45,
    )
    if resp.status_code not in (200, 201):
        raise RuntimeError(f"PUT Patient failed {resp.status_code}: {resp.text}")

    patient.clear()
    patient.update(updated)
    return True


def extract_subject_patient_id(observation: Dict[str, Any]) -> Optional[str]:
    subject = observation.get("subject") or {}
    if not isinstance(subject, dict):
        return None
    sid = subject.get("id")
    if isinstance(sid, str) and sid.strip():
        return sid.strip()
    reference = subject.get("reference")
    if isinstance(reference, str) and reference.startswith("Patient/"):
        return reference.split("/", 1)[1].strip() or None
    return None


def fetch_all_actimi_observations(base_url: str, auth: Optional[requests.auth.HTTPBasicAuth], page_count: int, actimi_id_to_given: Dict[str, str], headers: Optional[Dict[str, str]] = None) -> Dict[str, List[Dict[str, Any]]]:
    payload: Any = None
    for params in ([("_count", str(page_count))], None):
        try:
            payload = http_get_json(f"{base_url}/Observation", auth, params=params, headers=headers)
            break
        except RuntimeError:
            continue
    if payload is None:
        return {}
    by_given: Dict[str, List[Dict[str, Any]]] = {}
    for item in iter_resources(payload):
        if item.get("resourceType") != "Observation":
            continue
        pid = extract_subject_patient_id(item)
        if not pid:
            continue
        given = actimi_id_to_given.get(pid)
        if given:
            by_given.setdefault(given, []).append(item)
    return by_given


def find_observation_by_identifier(base_url: str, auth: requests.auth.HTTPBasicAuth, identifier_system: str, identifier_value: str) -> Optional[str]:
    payload = http_get_json(f"{base_url}/Observation", auth, params=[("identifier", f"{identifier_system}|{identifier_value}"), ("_count", "1")])
    for resource in iter_resources(payload):
        if resource.get("resourceType") == "Observation" and isinstance(resource.get("id"), str):
            return resource["id"].strip()
    return None


def post_or_put_observation(observation: Dict[str, Any], base_url: str, auth: requests.auth.HTTPBasicAuth, source_identifier_system: str) -> Tuple[str, str]:
    source_id: Optional[str] = None
    for identifier in observation.get("identifier", []) or []:
        if isinstance(identifier, dict) and identifier.get("system") == source_identifier_system:
            value = identifier.get("value")
            if isinstance(value, str) and value:
                source_id = value
                break

    observation.pop("id", None)
    if source_id:
        existing_id = find_observation_by_identifier(base_url, auth, source_identifier_system, source_id)
        if existing_id:
            observation["id"] = existing_id
            resp = SESSION.put(f"{base_url}/Observation/{existing_id}", auth=auth, headers=FHIR_HEADERS, data=json.dumps(observation), timeout=45)
            if resp.status_code not in (200, 201):
                raise RuntimeError(f"PUT Observation failed {resp.status_code}: {resp.text}")
            return "updated", existing_id

    resp = SESSION.post(f"{base_url}/Observation", auth=auth, headers=FHIR_HEADERS, data=json.dumps(observation), timeout=45)
    if resp.status_code not in (200, 201):
        raise RuntimeError(f"POST Observation failed {resp.status_code}: {resp.text}")
    created_id = str((resp.json() or {}).get("id") or "")
    return ("created" if resp.status_code == 201 else "updated"), created_id


# ---------------------------------------------------------------------------
# Encounter suchen oder anlegen – ein Encounter pro Messzeitpunkt
# ---------------------------------------------------------------------------

def find_or_create_encounter(
    base_url: str,
    auth: requests.auth.HTTPBasicAuth,
    patient_id: str,
    effective_dt: datetime,
    dry_run: bool,
) -> Optional[str]:
    """
    Sucht einen Encounter für den Patienten mit einem Zeitfenster von ±1 Minute
    um die exakte Messzeit. Falls keiner existiert, wird ein neuer angelegt.
    Gibt 'Encounter/<id>' zurück, oder None bei dry_run.
    """
    timestamp_str = effective_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    try:
        payload = http_get_json(
            f"{base_url}/Encounter",
            auth,
            params=[
                ("subject", f"Patient/{patient_id}"),
                ("date", f"eq{timestamp_str}"),   # exakter Sekundenmatch
                ("_count", "10"),
            ],
        )
        for resource in iter_resources(payload):
            if resource.get("resourceType") != "Encounter":
                continue
            enc_id = str(resource.get("id") or "").strip()
            if not enc_id:
                continue
            # period.start exakt prüfen (Sekunden-Präfix reicht)
            period_start = str((resource.get("period") or {}).get("start") or "")
            if period_start[:19] == timestamp_str[:19]:
                print(f"[INFO] Encounter gefunden: {enc_id} für Patient {patient_id} um {timestamp_str}")
                return f"Encounter/{enc_id}"
    except RuntimeError as exc:
        print(f"[WARN] Encounter-Suche fehlgeschlagen: {exc}")

    # Keinen gefunden → neu anlegen mit exakter Messzeit
    encounter: Dict[str, Any] = {
        "resourceType": "Encounter",
        "status": "finished",
        "class": {
            "system": "http://terminology.hl7.org/CodeSystem/v3-ActCode",
            "code": "AMB",
            "display": "ambulatory",
        },
        "subject": {"reference": f"Patient/{patient_id}"},
        "period": {
            "start": timestamp_str,
            "end":   timestamp_str,
        },
    }

    if dry_run:
        print(f"[DRY-RUN] Würde Encounter anlegen für Patient {patient_id} um {timestamp_str}")
        return None

    resp = SESSION.post(
        f"{base_url}/Encounter",
        auth=auth,
        headers=FHIR_HEADERS,
        data=json.dumps(encounter),
        timeout=45,
    )
    if resp.status_code not in (200, 201):
        raise RuntimeError(f"POST Encounter failed {resp.status_code}: {resp.text}")

    enc_id = str((resp.json() or {}).get("id") or "").strip()
    if not enc_id:
        raise RuntimeError("POST Encounter: keine ID in der Antwort")
    print(f"[INFO] Encounter erstellt: {enc_id} für Patient {patient_id} um {timestamp_str}")
    return f"Encounter/{enc_id}"


# ---------------------------------------------------------------------------

def create_communication(base_url: str, auth: requests.auth.HTTPBasicAuth, *, patient_id: str, observation_ids: List[str], observation_codes: List[str], encounter_ref: Optional[str], settings: Settings, dry_run: bool) -> bool:
    if not observation_ids:
        return False
    category_coding: Dict[str, Any] = {
        "system": settings.communication_category_system,
        "code": settings.communication_category_code or "vitalsign",
    }
    if settings.communication_category_display:
        category_coding["display"] = settings.communication_category_display
    communication: Dict[str, Any] = {
        "resourceType": "Communication",
        "status": settings.communication_status,
        "category": [{"coding": [category_coding], "text": "Vital"}],
        "subject": {"reference": f"Patient/{patient_id}"},
        "sent": datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        "about": [{"reference": f"Observation/{oid}"} for oid in observation_ids],
        "reasonCode": [{"coding": [{"system": "http://www.nursiti.com/notificationReason", "code": "add"}], "text": "Ein neues Vitalzeichen wurde hinzugefügt"}],
        "payload": [{"contentString": settings.communication_payloads.get(code, "Vitalzeichen")} for code in observation_codes],
    }
    if encounter_ref:
        communication["encounter"] = {"reference": encounter_ref}
    if dry_run:
        return True
    resp = SESSION.post(f"{base_url}/Communication", auth=auth, headers=FHIR_HEADERS, data=json.dumps(communication), timeout=45)
    if resp.status_code not in (200, 201):
        raise RuntimeError(f"POST Communication failed {resp.status_code}: {resp.text}")
    return True


def _sync_patient(
    given_key: str,
    actimi_patients: Dict[str, Dict[str, Any]],
    sensdoc_patients: Dict[str, Dict[str, Any]],
    sensdoc_patients_all: List[Dict[str, Any]],
    actimi_observations_by_patient: Dict[str, List[Dict[str, Any]]],
    from_time: datetime,
    settings: Settings,
) -> Dict[str, int]:
    stats = {
        "matched": 0, "missing_in_actimi": 0, "missing_in_sensdoc": 0,
        "created": 0, "updated": 0, "patient_alias_updated": 0,
        "communication_created": 0, "encounter_created": 0,
        "skipped_by_filter": 0, "skipped_no_timestamp": 0, "scanned": 0,
    }

    actimi_patient = actimi_patients.get(given_key)
    if not actimi_patient:
        stats["missing_in_actimi"] += 1
        return stats

    sensdoc_patient = sensdoc_patients.get(given_key) or find_sensdoc_patient_by_given_alias(sensdoc_patients_all, given_key)
    if not sensdoc_patient:
        stats["missing_in_sensdoc"] += 1
        return stats

    sensdoc_id = str(sensdoc_patient["id"])
    stats["matched"] += 1
    observations = actimi_observations_by_patient.get(given_key, [])

    if ensure_second_given_name(
        sensdoc_patient,
        given_key,
        settings.sensdoc_base,
        settings.sensdoc_auth,
        settings.dry_run,
    ):
        stats["patient_alias_updated"] += 1

    # Cache: exakter Timestamp → Encounter-Ref
    # Blutdruck-Komponenten teilen denselben effectiveDateTime → selber Encounter
    encounter_cache: Dict[str, Optional[str]] = {}

    for observation in observations:
        stats["scanned"] += 1
        if not observation_in_window(observation, from_time=from_time):
            stats["skipped_by_filter"] += 1
            continue

        expanded = expand_observation_for_transfer(observation)

        # Alle gültigen Items mit ihrem geparsten Timestamp sammeln
        valid_items: List[Tuple[Dict[str, Any], datetime]] = []
        for item in expanded:
            if not has_observation_value(item):
                continue
            code = observation_transfer_code(item)
            if settings.only_codes and (not code or code not in set(settings.only_codes)):
                continue
            effective_dt = extract_effective_datetime(item)
            if not effective_dt:
                stats["skipped_no_timestamp"] += 1
                continue
            parsed = parse_dt(effective_dt)
            if not parsed:
                stats["skipped_no_timestamp"] += 1
                continue
            valid_items.append((item, parsed))

        if not valid_items:
            stats["skipped_by_filter"] += 1
            continue

        # Encounter einmal pro exaktem Messzeitpunkt anlegen (gecacht)
        ref_dt    = valid_items[0][1]
        cache_key = ref_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

        if cache_key not in encounter_cache:
            try:
                enc_ref = find_or_create_encounter(
                    settings.sensdoc_base,
                    settings.sensdoc_auth,
                    sensdoc_id,
                    ref_dt,
                    settings.dry_run,
                )
                encounter_cache[cache_key] = enc_ref
                if enc_ref:
                    stats["encounter_created"] += 1
            except RuntimeError as exc:
                print(f"[WARN] Encounter für {cache_key} fehlgeschlagen: {exc}")
                encounter_cache[cache_key] = None

        encounter_ref = encounter_cache[cache_key]

        observation_ids:   List[str] = []
        observation_codes: List[str] = []

        for item, _ in valid_items:
            target_obs = build_target_observation(
                item, sensdoc_id, settings.source_identifier_system, settings
            )
            # Encounter-Referenz in Observation eintragen
            if encounter_ref:
                target_obs["encounter"] = {"reference": encounter_ref}

            if settings.dry_run:
                continue

            action, observation_id = post_or_put_observation(
                target_obs,
                settings.sensdoc_base,
                settings.sensdoc_auth,
                settings.source_identifier_system,
            )
            stats["created" if action == "created" else "updated"] += 1
            observation_ids.append(observation_id)
            c = observation_transfer_code(item)
            if c:
                observation_codes.append(c)

        if settings.create_communication and observation_ids:
            if create_communication(
                settings.sensdoc_base,
                settings.sensdoc_auth,
                patient_id=sensdoc_id,
                observation_ids=observation_ids,
                observation_codes=observation_codes,
                encounter_ref=encounter_ref,
                settings=settings,
                dry_run=settings.dry_run,
            ):
                stats["communication_created"] += 1

    return stats


def sync(settings: Settings, workers: int = 4) -> None:
    actimi_patients, _ = fetch_patient_map_by_given(
        settings.actimi_base,
        settings.actimi_auth,
        settings.page_count,
        headers=settings.actimi_headers,
        second_given_only=False,
    )
    sensdoc_patients, sensdoc_patients_all = fetch_patient_map_by_given(
        settings.sensdoc_base,
        settings.sensdoc_auth,
        settings.page_count,
        second_given_only=True,
    )
    actimi_id_to_given = {
        str(p.get("id")): given
        for given, p in actimi_patients.items()
        if p.get("id")
    }

    print("[INFO] Fetching all Actimi observations (bulk)...")
    actimi_observations_by_patient = fetch_all_actimi_observations(
        settings.actimi_base,
        settings.actimi_auth,
        settings.page_count,
        actimi_id_to_given,
        headers=settings.actimi_headers,
    )
    print(f"[INFO] Loaded observations for {len(actimi_observations_by_patient)} Actimi patients.")

    wanted_keys = settings.given_keys or sorted(set(actimi_patients.keys()))
    from_time = datetime.now(timezone.utc) - timedelta(minutes=settings.window_minutes)

    totals: Dict[str, int] = {
        "matched": 0, "missing_in_actimi": 0, "missing_in_sensdoc": 0,
        "created": 0, "updated": 0, "patient_alias_updated": 0,
        "communication_created": 0, "encounter_created": 0,
        "skipped_by_filter": 0, "skipped_no_timestamp": 0, "scanned": 0,
    }

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(
                _sync_patient, key,
                actimi_patients, sensdoc_patients, sensdoc_patients_all,
                actimi_observations_by_patient, from_time, settings,
            ): key
            for key in wanted_keys
        }
        for future in as_completed(futures):
            try:
                delta = future.result()
            except Exception as exc:
                print(f"[ERROR] Patient {futures[future]} failed: {exc}")
                continue
            with _STATS_LOCK:
                for k, v in delta.items():
                    totals[k] += v

    print(
        f"Done. matched_patients={totals['matched']}, "
        f"missing_in_actimi={totals['missing_in_actimi']}, "
        f"missing_in_sensdoc={totals['missing_in_sensdoc']}, "
        f"observations_scanned={totals['scanned']}, "
        f"filtered_out={totals['skipped_by_filter']}, "
        f"no_timestamp={totals['skipped_no_timestamp']}, "
        f"created={totals['created']}, updated={totals['updated']}, "
        f"patient_alias_updated={totals['patient_alias_updated']}, "
        f"communication_created={totals['communication_created']}, "
        f"encounter_created={totals['encounter_created']}, "
        f"dry_run={settings.dry_run}"
    )
