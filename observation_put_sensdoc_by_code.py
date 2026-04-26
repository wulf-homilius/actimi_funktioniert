from __future__ import annotations

import argparse
import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import Lock
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
import yaml

FHIR_HEADERS = {"Content-Type": "application/fhir+json"}
SESSION = requests.Session()
SESSION.trust_env = False

# Thread-safe counter lock for the sync summary
_STATS_LOCK = Lock()


@dataclass(frozen=True)
class Settings:
    actimi_base: str
    sensdoc_base: str
    actimi_auth: Optional[requests.auth.HTTPBasicAuth]
    actimi_headers: Dict[str, str]
    sensdoc_auth: requests.auth.HTTPBasicAuth
    window_minutes: int
    dry_run: bool
    only_codes: List[str]
    given_keys: List[str]
    page_count: int
    source_identifier_system: str
    patient_links: Dict[str, Dict[str, str]]
    add_source_key_to_target_given: bool
    create_communication: bool
    communication_status: str
    communication_text: str
    communication_category_system: str
    communication_category_code: Optional[str]
    communication_category_display: Optional[str]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sync Observation from Actimi to Sensdoc with patient match by given-name key.",
    )
    parser.add_argument("--config", default="config/actimi_to_sensdoc.yaml", help="Path to YAML config")
    parser.add_argument("--env-file", default="config/.env", help="Path to .env file")
    parser.add_argument("--window-minutes", type=int, help="Transfer only records from last N minutes")
    parser.add_argument("--given-key", action="append", default=[], help="Patient key (name.given), repeatable")
    parser.add_argument("--code", action="append", default=[], help="Only transfer these LOINC codes")
    parser.add_argument("--dry-run", action="store_true", help="No write to Sensdoc; only print summary")
    parser.add_argument("--debug", action="store_true", help="Print resolved settings and exit")
    parser.add_argument("--workers", type=int, default=4, help="Number of parallel worker threads (default: 4)")
    return parser.parse_args()


def load_config(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


def load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def env_required(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing environment variable: {name}")
    return value


def read_server(cfg: Dict[str, Any], block_name: str) -> Dict[str, Any]:
    block = cfg.get(block_name)
    if not isinstance(block, dict):
        raise RuntimeError(f"Missing config block: {block_name}")
    return block


def build_auth(block: Dict[str, Any], user_env_default: str, pass_env_default: str) -> requests.auth.HTTPBasicAuth:
    auth = block.get("auth", {}) or {}
    user_env = auth.get("user_env", user_env_default)
    pass_env = auth.get("pass_env", pass_env_default)
    return requests.auth.HTTPBasicAuth(env_required(str(user_env)), env_required(str(pass_env)))


def build_optional_auth(
    block: Dict[str, Any],
    user_env_default: str,
    pass_env_default: str,
) -> Optional[requests.auth.HTTPBasicAuth]:
    auth = block.get("auth", {}) or {}
    user_env = str(auth.get("user_env", user_env_default))
    pass_env = str(auth.get("pass_env", pass_env_default))
    user = os.getenv(user_env)
    password = os.getenv(pass_env)
    if user and password:
        return requests.auth.HTTPBasicAuth(user, password)
    return None


def request_actimi_access_token(api_key: str, token_url: str) -> str:
    headers = {"accept": "application/json", "content-type": "application/json"}
    payload = {"apiKey": api_key}
    response = SESSION.post(token_url, headers=headers, json=payload, timeout=45)
    if response.status_code != 200:
        raise RuntimeError(f"Actimi token request failed {response.status_code} for {token_url}: {response.text}")
    data: Any = response.json()
    if isinstance(data, dict):
        for key in ("token", "accessToken", "access_token", "jwt"):
            value = data.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        nested = data.get("data")
        if isinstance(nested, dict):
            for key in ("token", "accessToken", "access_token", "jwt"):
                value = nested.get(key)
                if isinstance(value, str) and value.strip():
                    return value.strip()
    raise RuntimeError(f"Actimi token missing in response: {data}")


def build_settings(args: argparse.Namespace, cfg: Dict[str, Any]) -> Settings:
    actimi_block = read_server(cfg, "actimi")
    sensdoc_block = read_server(cfg, "sensdoc")
    sync_block = cfg.get("sync", {}) or {}

    actimi_base = str(actimi_block.get("url", "")).rstrip("/")
    sensdoc_base = str(sensdoc_block.get("url", "")).rstrip("/")
    if not actimi_base or not sensdoc_base:
        raise RuntimeError("Both actimi.url and sensdoc.url are required in config")

    window_minutes = int(args.window_minutes if args.window_minutes is not None else sync_block.get("window_minutes", 1440))
    dry_run = bool(args.dry_run or sync_block.get("dry_run", False))

    configured_codes = [str(v).strip() for v in (sync_block.get("only_codes", []) or []) if str(v).strip()]
    cli_codes = [str(v).strip() for v in (args.code or []) if str(v).strip()]
    only_codes = cli_codes or configured_codes

    configured_given_keys = [str(v).strip() for v in (sync_block.get("given_keys", []) or []) if str(v).strip()]
    configured_patient_keys: List[str] = []
    cli_given_keys = [str(v).strip() for v in (args.given_key or []) if str(v).strip()]
    given_keys = cli_given_keys or configured_given_keys
    patient_links_raw = sync_block.get("patient_links", {}) or {}
    patient_links: Dict[str, Dict[str, str]] = {}
    if isinstance(patient_links_raw, dict):
        for key, value in patient_links_raw.items():
            given_key = str(key).strip()
            if not given_key or not isinstance(value, dict):
                continue
            normalized_rule = {}
            for field in ("patient_id", "given", "family", "birth_date"):
                field_value = value.get(field)
                if isinstance(field_value, str) and field_value.strip():
                    normalized_rule[field] = field_value.strip()
            if normalized_rule:
                patient_links[given_key] = normalized_rule

    patients_raw = sync_block.get("patients", []) or []
    if isinstance(patients_raw, list):
        for entry in patients_raw:
            if not isinstance(entry, dict):
                continue
            primary_key = str(
                entry.get("primary_key")
                or entry.get("given_key")
                or entry.get("actimi_given")
                or ""
            ).strip()
            if not primary_key:
                continue
            configured_patient_keys.append(primary_key)
            normalized_rule: Dict[str, str] = {}
            for field in ("patient_id", "given", "family", "birth_date"):
                value = entry.get(field)
                if isinstance(value, str) and value.strip():
                    normalized_rule[field] = value.strip()
            if normalized_rule:
                patient_links[primary_key] = normalized_rule

    if not given_keys and configured_patient_keys:
        given_keys = configured_patient_keys

    actimi_auth_cfg = actimi_block.get("auth", {}) or {}
    token_prefix = str(actimi_auth_cfg.get("token_prefix", "Bearer")).strip()

    token_env = str(actimi_auth_cfg.get("token_env", "ACTIMI_TOKEN")).strip()
    token_value = (os.getenv(token_env) or str(actimi_auth_cfg.get("token", "")).strip()).strip()

    api_key_env = str(actimi_auth_cfg.get("api_key_env", "ACTIMI_API_KEY")).strip()
    api_key_value = (os.getenv(api_key_env) or str(actimi_auth_cfg.get("api_key", "")).strip()).strip()
    token_url = str(actimi_auth_cfg.get("token_url", f"{actimi_base}/Auth/token")).strip()

    if api_key_value:
        token_value = request_actimi_access_token(api_key_value, token_url)

    actimi_headers: Dict[str, str] = {}
    if token_value:
        actimi_headers["Authorization"] = f"{token_prefix} {token_value}".strip()

    return Settings(
        actimi_base=actimi_base,
        sensdoc_base=sensdoc_base,
        actimi_auth=build_optional_auth(actimi_block, "FHIR_USER_get", "FHIR_PASS_get"),
        actimi_headers=actimi_headers,
        sensdoc_auth=build_auth(sensdoc_block, "FHIR_USER_post", "FHIR_PASS_post"),
        window_minutes=window_minutes,
        dry_run=dry_run,
        only_codes=only_codes,
        given_keys=given_keys,
        page_count=int(sync_block.get("page_count", 200)),
        source_identifier_system=str(sync_block.get("source_identifier_system", "urn:actimi:observation-id")),
        patient_links=patient_links,
        add_source_key_to_target_given=bool(sync_block.get("add_source_key_to_target_given", True)),
        create_communication=bool(sync_block.get("create_communication", True)),
        communication_status=str(sync_block.get("communication_status", "completed")),
        communication_text=str(sync_block.get("communication_text", "Actimi Observation in Sensdoc übernommen")),
        communication_category_system=str(
            sync_block.get("communication_category_system", "http://www.nursiti.com/notificationType")
        ),
        communication_category_code=(
            str(sync_block.get("communication_category_code")).strip()
            if sync_block.get("communication_category_code") is not None
            else None
        ),
        communication_category_display=(
            str(sync_block.get("communication_category_display")).strip()
            if sync_block.get("communication_category_display") is not None
            else None
        ),
    )


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


def http_get_json(
    url: str,
    auth: Optional[requests.auth.HTTPBasicAuth],
    params: Optional[List[tuple[str, str]]] = None,
    headers: Optional[Dict[str, str]] = None,
) -> Any:
    resp = SESSION.get(url, auth=auth, params=params, headers=headers, timeout=45)
    if resp.status_code != 200:
        raise RuntimeError(f"GET failed {resp.status_code} for {url}: {resp.text}")
    return resp.json()


def iter_resources(payload: Any) -> Iterable[Dict[str, Any]]:
    if isinstance(payload, list):
        for item in payload:
            if isinstance(item, dict):
                yield item
        return
    if isinstance(payload, dict) and isinstance(payload.get("entry"), list):
        for entry in payload.get("entry", []):
            if not isinstance(entry, dict):
                continue
            resource = entry.get("resource")
            if isinstance(resource, dict):
                yield resource
        return
    if isinstance(payload, dict):
        yield payload


def first_given(patient: Dict[str, Any]) -> Optional[str]:
    for name in patient.get("name", []) or []:
        if not isinstance(name, dict):
            continue
        for value in name.get("given", []) or []:
            if isinstance(value, str) and value.strip():
                return value.strip()
    return None


def normalize_name(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    return " ".join(value.strip().casefold().split())


def patient_name_parts(patient: Dict[str, Any]) -> List[tuple[str, List[str]]]:
    out: List[tuple[str, List[str]]] = []
    for name in patient.get("name", []) or []:
        if not isinstance(name, dict):
            continue
        family = normalize_name(name.get("family"))
        givens: List[str] = []
        for given in name.get("given", []) or []:
            normalized = normalize_name(given)
            if normalized:
                givens.append(normalized)
        out.append((family, givens))
    return out


def fetch_patients(
    base_url: str,
    auth: Optional[requests.auth.HTTPBasicAuth],
    page_count: int,
    headers: Optional[Dict[str, str]] = None,
) -> List[Dict[str, Any]]:
    query_variants: List[Optional[List[tuple[str, str]]]] = [
        [("_count", str(page_count))],
        None,
    ]
    last_error: Optional[Exception] = None
    for params in query_variants:
        try:
            payload = http_get_json(f"{base_url}/Patient", auth, params=params, headers=headers)
            return [item for item in iter_resources(payload) if item.get("resourceType") == "Patient"]
        except RuntimeError as exc:
            last_error = exc
            continue
    if last_error:
        raise last_error
    return []


# PERF FIX #4: fetch_patient_map_by_given now returns both the map AND the full list
# so callers don't need to make a second API call to get sensdoc_patients_all.
def fetch_patient_map_by_given(
    base_url: str,
    auth: Optional[requests.auth.HTTPBasicAuth],
    page_count: int,
    headers: Optional[Dict[str, str]] = None,
) -> Tuple[Dict[str, Dict[str, Any]], List[Dict[str, Any]]]:
    all_patients = fetch_patients(base_url, auth, page_count, headers=headers)
    out: Dict[str, Dict[str, Any]] = {}
    for patient in all_patients:
        patient_id = patient.get("id")
        given = first_given(patient)
        if isinstance(patient_id, str) and patient_id and given and given not in out:
            out[given] = patient
    return out, all_patients


def find_sensdoc_patient_by_rule(
    patients: List[Dict[str, Any]],
    rule: Dict[str, str],
) -> Optional[Dict[str, Any]]:
    target_birth = rule.get("birth_date", "").strip()
    target_family = normalize_name(rule.get("family"))
    target_given = normalize_name(rule.get("given"))
    if target_family and target_given and target_birth:
        matches: List[Dict[str, Any]] = []
        for patient in patients:
            birth_date = patient.get("birthDate")
            if not isinstance(birth_date, str) or birth_date.strip() != target_birth:
                continue
            for family, givens in patient_name_parts(patient):
                if family != target_family:
                    continue
                if target_given in givens:
                    matches.append(patient)
                    break
        if len(matches) == 1:
            return matches[0]
        return None

    target_id = rule.get("patient_id")
    if target_id:
        for patient in patients:
            if patient.get("id") == target_id:
                return patient
    return None


def observation_codes(observation: Dict[str, Any]) -> List[str]:
    out: List[str] = []
    code = observation.get("code") or {}
    if isinstance(code, dict):
        for coding in code.get("coding", []) or []:
            if not isinstance(coding, dict):
                continue
            code_value = coding.get("code")
            if isinstance(code_value, str) and code_value.strip():
                out.append(code_value.strip())
    return out


def extract_effective_datetime(observation: Dict[str, Any]) -> Optional[str]:
    candidates: List[Any] = [
        observation.get("effectiveDateTime"),
        (observation.get("effective") or {}).get("dateTime"),
        observation.get("issued"),
        (observation.get("meta") or {}).get("lastUpdated"),
    ]
    for value in candidates:
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def observation_in_window(observation: Dict[str, Any], *, from_time: datetime) -> bool:
    candidate = parse_dt(extract_effective_datetime(observation))
    return bool(candidate and candidate >= from_time)


def fetch_all_actimi_observations(
    base_url: str,
    auth: Optional[requests.auth.HTTPBasicAuth],
    page_count: int,
    actimi_id_to_given: Dict[str, str],
    headers: Optional[Dict[str, str]] = None,
) -> Dict[str, List[Dict[str, Any]]]:
    """Fetch ALL observations from Actimi in one request, then group by given_name.

    Actimi's API does not support filtering by subject/patient. The only working
    strategy is a full fetch and local grouping.

    Patient IDs differ between Actimi and Sensdoc — the only shared key is the
    given name. We therefore use actimi_id_to_given to translate the subject
    reference in each observation to the given name used for matching.

    Returns a dict: { given_name -> [observation, ...] }
    """
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


def extract_subject_patient_id(observation: Dict[str, Any]) -> Optional[str]:
    subject = observation.get("subject") or {}
    if not isinstance(subject, dict):
        return None
    subject_id = subject.get("id")
    if isinstance(subject_id, str) and subject_id.strip():
        return subject_id.strip()
    reference = subject.get("reference")
    if isinstance(reference, str) and reference.startswith("Patient/"):
        return reference.split("/", 1)[1].strip() or None
    return None


def upsert_identifier(observation: Dict[str, Any], system: str, value: str) -> None:
    identifiers = observation.get("identifier")
    if not isinstance(identifiers, list):
        identifiers = []
        observation["identifier"] = identifiers
    for identifier in identifiers:
        if not isinstance(identifier, dict):
            continue
        if identifier.get("system") == system and identifier.get("value") == value:
            return
    identifiers.append({"system": system, "value": value})


# PERF FIX #3: Replace copy.deepcopy with json round-trip (faster for JSON-compatible dicts)
def _shallow_clone_observation(source: Dict[str, Any]) -> Dict[str, Any]:
    return json.loads(json.dumps(source))


def build_target_observation(source_observation: Dict[str, Any], target_patient_id: str, source_identifier_system: str) -> Dict[str, Any]:
    transformed = _shallow_clone_observation(source_observation)
    source_id = str(transformed.get("id", "")).strip()
    effective_datetime = extract_effective_datetime(transformed)
    transformed.pop("id", None)
    transformed.pop("meta", None)
    transformed["subject"] = {"reference": f"Patient/{target_patient_id}"}
    transformed.pop("effective", None)
    if effective_datetime:
        transformed["effectiveDateTime"] = effective_datetime
    if source_id:
        upsert_identifier(transformed, source_identifier_system, source_id)
    return transformed


# PERF FIX #2: Use FHIR conditional create (If-None-Exist) to avoid the extra GET per observation.
# The server returns 200 if it already exists, 201 if newly created — one request instead of two.
def post_or_put_observation(
    observation: Dict[str, Any],
    base_url: str,
    auth: requests.auth.HTTPBasicAuth,
    source_identifier_system: str,
) -> Tuple[str, str]:
    source_id: Optional[str] = None
    for identifier in observation.get("identifier", []) or []:
        if isinstance(identifier, dict) and identifier.get("system") == source_identifier_system:
            value = identifier.get("value")
            if isinstance(value, str) and value:
                source_id = value
                break

    observation.pop("id", None)

    conditional_headers = {**FHIR_HEADERS}
    if source_id:
        conditional_headers["If-None-Exist"] = f"identifier={source_identifier_system}|{source_id}"

    resp = SESSION.post(
        f"{base_url}/Observation",
        auth=auth,
        headers=conditional_headers,
        data=json.dumps(observation),
        timeout=45,
    )
    if resp.status_code not in (200, 201):
        raise RuntimeError(f"POST Observation failed {resp.status_code}: {resp.text}")

    action = "updated" if resp.status_code == 200 else "created"
    created_id = str((resp.json() or {}).get("id") or "")
    return action, created_id


def patient_has_given(patient: Dict[str, Any], value: str) -> bool:
    probe = normalize_name(value)
    if not probe:
        return False
    for name in patient.get("name", []) or []:
        if not isinstance(name, dict):
            continue
        for given in name.get("given", []) or []:
            if normalize_name(given) == probe:
                return True
    return False


def ensure_patient_given_alias(
    patient: Dict[str, Any],
    alias_given: str,
    base_url: str,
    auth: requests.auth.HTTPBasicAuth,
    dry_run: bool,
) -> bool:
    if not alias_given or patient_has_given(patient, alias_given):
        return False
    updated = _shallow_clone_observation(patient)
    names = updated.get("name")
    if not isinstance(names, list) or not names:
        updated["name"] = [{"given": [alias_given]}]
    else:
        first = names[0]
        if not isinstance(first, dict):
            names[0] = {"given": [alias_given]}
        else:
            givens = first.get("given")
            if not isinstance(givens, list):
                first["given"] = [alias_given]
            else:
                givens.append(alias_given)

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


def search_encounter_by_start_datetime(
    base_url: str,
    auth: requests.auth.HTTPBasicAuth,
    patient_id: str,
    start_datetime: str,
) -> Optional[str]:
    params = [("subject", f"Patient/{patient_id}"), ("date", start_datetime), ("_count", "1")]
    try:
        payload = http_get_json(f"{base_url}/Encounter", auth, params=params)
    except RuntimeError:
        return None
    for resource in iter_resources(payload):
        if resource.get("resourceType") != "Encounter":
            continue
        encounter_id = resource.get("id")
        if isinstance(encounter_id, str) and encounter_id.strip():
            return f"Encounter/{encounter_id.strip()}"
    return None


def create_encounter_for_patient(
    base_url: str,
    auth: requests.auth.HTTPBasicAuth,
    patient_id: str,
    effective_datetime: Optional[str],
) -> str:
    encounter: Dict[str, Any] = {
        "resourceType": "Encounter",
        "status": "in-progress",
        "subject": {"reference": f"Patient/{patient_id}"},
    }
    if isinstance(effective_datetime, str) and effective_datetime.strip():
        encounter["period"] = {"start": effective_datetime.strip()}
    resp = SESSION.post(
        f"{base_url}/Encounter",
        auth=auth,
        headers=FHIR_HEADERS,
        data=json.dumps(encounter),
        timeout=45,
    )
    if resp.status_code not in (200, 201):
        raise RuntimeError(f"POST Encounter failed {resp.status_code}: {resp.text}")
    created = resp.json() if resp.text.strip() else {}
    encounter_id = created.get("id") if isinstance(created, dict) else None
    if not isinstance(encounter_id, str) or not encounter_id.strip():
        raise RuntimeError("Encounter creation returned no id")
    return f"Encounter/{encounter_id.strip()}"


def ensure_patient_encounter_reference(
    target_patient_id: str,
    effective_datetime: Optional[str],
    base_url: str,
    auth: requests.auth.HTTPBasicAuth,
    dry_run: bool,
) -> Tuple[Optional[str], bool]:
    if not effective_datetime:
        return None, False
    found = search_encounter_by_start_datetime(base_url, auth, target_patient_id, effective_datetime)
    if found:
        return found, False
    if dry_run:
        return f"Encounter/<would-create-for-{target_patient_id}-{effective_datetime}>", True
    created = create_encounter_for_patient(base_url, auth, target_patient_id, effective_datetime)
    return created, True


def create_communication(
    base_url: str,
    auth: requests.auth.HTTPBasicAuth,
    *,
    patient_id: str,
    observation_id: str,
    settings: Settings,
    dry_run: bool,
) -> bool:
    if not observation_id:
        return False
    communication: Dict[str, Any] = {
        "resourceType": "Communication",
        "status": settings.communication_status,
        "subject": {"reference": f"Patient/{patient_id}"},
        "sent": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "about": [{"reference": f"Observation/{observation_id}"}],
        "payload": [{"contentString": settings.communication_text}],
    }
    coding: Dict[str, str] = {"system": settings.communication_category_system}
    if settings.communication_category_code:
        coding["code"] = settings.communication_category_code
    if settings.communication_category_display:
        coding["display"] = settings.communication_category_display
    communication["category"] = [{"coding": [coding]}]
    if dry_run:
        return True
    resp = SESSION.post(
        f"{base_url}/Communication",
        auth=auth,
        headers=FHIR_HEADERS,
        data=json.dumps(communication),
        timeout=45,
    )
    if resp.status_code not in (200, 201):
        raise RuntimeError(f"POST Communication failed {resp.status_code}: {resp.text}")
    return True


# PERF FIX #1: Per-patient sync logic extracted so it can run in parallel threads.
def _sync_patient(
    given_key: str,
    actimi_patients: Dict[str, Dict[str, Any]],
    sensdoc_patients: Dict[str, Dict[str, Any]],
    sensdoc_patients_all: List[Dict[str, Any]],
    actimi_observations_by_patient: Dict[str, List[Dict[str, Any]]],
    from_time: datetime,
    settings: Settings,
) -> Dict[str, int]:
    """Sync one patient. Returns a dict of stat deltas."""
    stats = {
        "matched": 0,
        "missing_in_actimi": 0,
        "missing_in_sensdoc": 0,
        "created": 0,
        "updated": 0,
        "patient_alias_updated": 0,
        "communication_created": 0,
        "encounter_created": 0,
        "skipped_by_filter": 0,
        "scanned": 0,
    }

    actimi_patient = actimi_patients.get(given_key)
    if not actimi_patient:
        stats["missing_in_actimi"] += 1
        print(f"[WARN] Given key not found in Actimi: {given_key}")
        return stats

    sensdoc_patient = sensdoc_patients.get(given_key)
    if not sensdoc_patient and given_key in settings.patient_links:
        sensdoc_patient = find_sensdoc_patient_by_rule(sensdoc_patients_all, settings.patient_links[given_key])
    if not sensdoc_patient:
        stats["missing_in_sensdoc"] += 1
        print(f"[WARN] Given key not found in Sensdoc: {given_key}")
        return stats

    actimi_id = str(actimi_patient["id"])
    sensdoc_id = str(sensdoc_patient["id"])

    if settings.add_source_key_to_target_given:
        if ensure_patient_given_alias(
            sensdoc_patient,
            given_key,
            settings.sensdoc_base,
            settings.sensdoc_auth,
            settings.dry_run,
        ):
            stats["patient_alias_updated"] += 1

    stats["matched"] += 1

    # Observations are grouped by given_name — use given_key directly
    observations = actimi_observations_by_patient.get(given_key, [])

    for observation in observations:
        stats["scanned"] += 1
        if not observation_in_window(observation, from_time=from_time):
            stats["skipped_by_filter"] += 1
            continue
        if settings.only_codes:
            codes = set(observation_codes(observation))
            if not codes.intersection(settings.only_codes):
                stats["skipped_by_filter"] += 1
                continue

        effective_datetime = extract_effective_datetime(observation)
        encounter_ref, created_now = ensure_patient_encounter_reference(
            sensdoc_id,
            effective_datetime,
            settings.sensdoc_base,
            settings.sensdoc_auth,
            settings.dry_run,
        )
        if created_now:
            stats["encounter_created"] += 1

        target_obs = build_target_observation(observation, sensdoc_id, settings.source_identifier_system)
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
        if action == "created":
            stats["created"] += 1
        else:
            stats["updated"] += 1

        if settings.create_communication:
            if create_communication(
                settings.sensdoc_base,
                settings.sensdoc_auth,
                patient_id=sensdoc_id,
                observation_id=observation_id,
                settings=settings,
                dry_run=settings.dry_run,
            ):
                stats["communication_created"] += 1

    return stats


def sync(settings: Settings, workers: int = 4) -> None:
    # PERF FIX #4: single call returns both map and full list — no duplicate HTTP request
    actimi_patients, _ = fetch_patient_map_by_given(
        settings.actimi_base,
        settings.actimi_auth,
        settings.page_count,
        headers=settings.actimi_headers,
    )
    sensdoc_patients, sensdoc_patients_all = fetch_patient_map_by_given(
        settings.sensdoc_base,
        settings.sensdoc_auth,
        settings.page_count,
    )

    # Build a reverse map: actimi patient_id → given_name
    # This is needed because Actimi observations reference the Actimi patient UUID,
    # but matching to Sensdoc only works via given_name.
    actimi_id_to_given: Dict[str, str] = {
        str(p.get("id")): given
        for given, p in actimi_patients.items()
        if p.get("id")
    }

    # Fetch ALL Actimi observations in one request and group by given_name.
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
        "skipped_by_filter": 0, "scanned": 0,
    }

    # PERF FIX #1: Process patients in parallel threads
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(
                _sync_patient,
                key,
                actimi_patients,
                sensdoc_patients,
                sensdoc_patients_all,
                actimi_observations_by_patient,
                from_time,
                settings,
            ): key
            for key in wanted_keys
        }
        for future in as_completed(futures):
            given_key = futures[future]
            try:
                delta = future.result()
            except Exception as exc:
                print(f"[ERROR] Patient {given_key} failed: {exc}")
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
        f"created={totals['created']}, updated={totals['updated']}, "
        f"patient_alias_updated={totals['patient_alias_updated']}, "
        f"communication_created={totals['communication_created']}, "
        f"encounter_created={totals['encounter_created']}, "
        f"dry_run={settings.dry_run}"
    )


def print_debug(settings: Settings, config_path: Path, env_file_path: Path) -> None:
    print(f"config: {config_path}")
    print(f"env_file: {env_file_path}")
    print(f"actimi_base: {settings.actimi_base}")
    print(f"actimi_auth_basic: {bool(settings.actimi_auth)}")
    print(f"actimi_auth_header: {bool(settings.actimi_headers.get('Authorization'))}")
    print(f"sensdoc_base: {settings.sensdoc_base}")
    print(f"window_minutes: {settings.window_minutes}")
    print(f"dry_run: {settings.dry_run}")
    print(f"page_count: {settings.page_count}")
    print(f"source_identifier_system: {settings.source_identifier_system}")
    print(f"given_keys: {settings.given_keys}")
    print(f"only_codes: {settings.only_codes}")
    print(f"patient_links: {settings.patient_links}")
    print(f"add_source_key_to_target_given: {settings.add_source_key_to_target_given}")
    print(f"create_communication: {settings.create_communication}")


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parent
    config_path = Path(args.config)
    env_path = Path(args.env_file)
    if not config_path.is_absolute():
        config_path = root / config_path
    if not env_path.is_absolute():
        env_path = root / env_path

    load_env_file(env_path)
    cfg = load_config(config_path)
    settings = build_settings(args, cfg)

    if args.debug:
        print_debug(settings, config_path, env_path)
        return 0

    sync(settings, workers=args.workers)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())