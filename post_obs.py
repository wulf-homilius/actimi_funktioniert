from __future__ import annotations

import copy
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
import yaml

import import_requests as ir
from Patient_main_obs import classify_observation, fetch_observations

FHIR_HEADERS = {"Content-Type": "application/fhir+json", "Accept": "application/fhir+json"}


def load_config(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


def load_env(root: Path) -> None:
    ir.load_env_file(root / "config" / ".env")


def build_sensdoc_auth(config: Dict[str, Any]) -> requests.auth.HTTPBasicAuth:
    sensdoc_block = config.get("sensdoc", {}) or {}
    auth_block = sensdoc_block.get("auth", {}) or {}
    user_env = auth_block.get("user_env")
    pass_env = auth_block.get("pass_env")
    if not user_env or not pass_env:
        raise RuntimeError("sensdoc.auth.user_env und sensdoc.auth.pass_env müssen in der Konfiguration stehen")
    user = os.getenv(user_env)
    password = os.getenv(pass_env)
    if not user or not password:
        raise RuntimeError(f"Umgebungsvariablen {user_env} oder {pass_env} fehlen")
    return requests.auth.HTTPBasicAuth(user, password)


def extract_resources(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, dict):
        if payload.get("resourceType") in ("Bundle",):
            entries = payload.get("entry") or []
            resources: List[Dict[str, Any]] = []
            for entry in entries:
                if isinstance(entry, dict):
                    resource = entry.get("resource")
                    if isinstance(resource, dict):
                        resources.append(resource)
            return resources
        if payload.get("resourceType") in ("Patient", "Observation", "Encounter"):
            return [payload]
        return []
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    return []


def patient_has_given(patient: Dict[str, Any], value: str) -> bool:
    probe = str(value or "").strip().casefold()
    if not probe:
        return False
    for name in patient.get("name", []) or []:
        if not isinstance(name, dict):
            continue
        for given in name.get("given", []) or []:
            if isinstance(given, str) and given.strip().casefold() == probe:
                return True
    return False


def observation_effective_datetime(observation: Dict[str, Any]) -> Optional[str]:
    if not isinstance(observation, dict):
        return None
    effective = observation.get("effectiveDateTime")
    if isinstance(effective, str) and effective.strip():
        return effective.strip()
    effective_block = observation.get("effective") or {}
    if isinstance(effective_block, dict):
        date_time = effective_block.get("dateTime")
        if isinstance(date_time, str) and date_time.strip():
            return date_time.strip()
    return None


def find_sensdoc_patient(base_url: str, auth: requests.auth.HTTPBasicAuth, family: str, birthdate: str, fixed_code: str) -> Dict[str, Any]:
    params: Dict[str, str] = {}
    if fixed_code:
        params["given"] = fixed_code
    if family:
        params["family"] = family
    if birthdate:
        params["birthdate"] = birthdate

    response = requests.get(
        f"{base_url}/Patient",
        auth=auth,
        headers={"Accept": "application/fhir+json"},
        params=params,
        timeout=45,
    )
    response.raise_for_status()
    patients = extract_resources(response.json())
    if not patients:
        raise RuntimeError(
            f"Kein Patient in Sensdoc gefunden mit given={fixed_code}, family={family}, birthdate={birthdate}"
        )
    exact_patients = [patient for patient in patients if patient_has_given(patient, fixed_code)]
    if exact_patients:
        patients = exact_patients
    if len(patients) > 1:
        print(f"⚠️ Es wurden {len(patients)} Patienten gefunden, verwende den ersten Treffer.")
    return patients[0]


def sanitize_observation(observation: Dict[str, Any], patient_id: str, fixed_code: str, encounter_reference: Optional[str] = None) -> Dict[str, Any]:
    output = copy.deepcopy(observation)
    for key in ("id", "meta", "subject", "encounter", "performer", "basedOn", "partOf", "text", "contained"):
        output.pop(key, None)
    output["resourceType"] = "Observation"
    output["subject"] = {"reference": f"Patient/{patient_id}"}
    if encounter_reference:
        output["encounter"] = {"reference": encounter_reference}
    if "status" not in output:
        output["status"] = "final"

    source_id = None
    if isinstance(observation.get("id"), str) and observation["id"].strip():
        source_id = observation["id"].strip()
    if not output.get("identifier") and source_id:
        output["identifier"] = [
            {
                "system": "urn:actimi:observation-id",
                "value": source_id,
            }
        ]
    return output


def post_observation(base_url: str, auth: requests.auth.HTTPBasicAuth, observation: Dict[str, Any]) -> str:
    headers = {**FHIR_HEADERS}
    identifier = None
    for candidate in observation.get("identifier", []) or []:
        if isinstance(candidate, dict) and candidate.get("system") and candidate.get("value"):
            identifier = candidate
            break
    if identifier:
        headers["If-None-Exist"] = f"identifier={identifier['system']}|{identifier['value']}"

    response = requests.post(
        f"{base_url}/Observation",
        auth=auth,
        headers=headers,
        data=json.dumps(observation),
        timeout=45,
    )
    if response.status_code not in (200, 201):
        raise RuntimeError(f"POST Observation fehlgeschlagen: {response.status_code} - {response.text}")
    payload = response.json()
    return str(payload.get("id") or "")


def search_encounter(base_url: str, auth: requests.auth.HTTPBasicAuth, patient_id: str, effective_datetime: str) -> Optional[str]:
    if not effective_datetime:
        return None
    response = requests.get(
        f"{base_url}/Encounter",
        auth=auth,
        headers={"Accept": "application/fhir+json"},
        params={"subject": f"Patient/{patient_id}"},
        timeout=45,
    )
    response.raise_for_status()
    for encounter in extract_resources(response.json()):
        if encounter.get("resourceType") != "Encounter":
            continue
        period = encounter.get("period") or {}
        if period.get("start") == effective_datetime or period.get("end") == effective_datetime:
            encounter_id = encounter.get("id")
            if isinstance(encounter_id, str) and encounter_id.strip():
                return f"Encounter/{encounter_id.strip()}"
    return None


def create_encounter(base_url: str, auth: requests.auth.HTTPBasicAuth, patient_id: str, effective_datetime: Optional[str]) -> str:
    encounter: Dict[str, Any] = {
        "resourceType": "Encounter",
        "status": "in-progress",
        "subject": {"reference": f"Patient/{patient_id}"},
    }
    if effective_datetime:
        encounter["period"] = {"start": effective_datetime}
    response = requests.post(
        f"{base_url}/Encounter",
        auth=auth,
        headers=FHIR_HEADERS,
        data=json.dumps(encounter),
        timeout=45,
    )
    if response.status_code not in (200, 201):
        raise RuntimeError(f"POST Encounter fehlgeschlagen: {response.status_code} - {response.text}")
    payload = response.json()
    encounter_id = str(payload.get("id") or "")
    if not encounter_id:
        raise RuntimeError("Encounter-Erstellung lieferte keine id")
    return f"Encounter/{encounter_id}"


def ensure_encounter(base_url: str, auth: requests.auth.HTTPBasicAuth, patient_id: str, effective_datetime: Optional[str]) -> Optional[str]:
    if effective_datetime:
        found = search_encounter(base_url, auth, patient_id, effective_datetime)
        if found:
            return found
    return create_encounter(base_url, auth, patient_id, effective_datetime)


def main() -> int:
    root = Path(__file__).resolve().parent
    load_env(root)

    config_path = root / "config" / "config_put_code_to_sensd.yaml"
    config = load_config(config_path)
    sensdoc_base = str(config.get("sensdoc", {}).get("database", {}).get("url", "")).rstrip("/")
    if not sensdoc_base:
        raise RuntimeError("sensdoc.database.url muss in der Konfiguration stehen")

    fixed_code = str(config.get("fixed_code") or "").strip()
    if not fixed_code:
        raise RuntimeError("fixed_code muss in der Konfiguration stehen")

    filter_config = config.get("filter", {}) or {}
    family = str(filter_config.get("family") or "").strip()
    birthdate = str(filter_config.get("birthDate") or "").strip()
    if not family or not birthdate:
        raise RuntimeError("filter.family und filter.birthDate müssen in der Konfiguration stehen")

    auth = build_sensdoc_auth(config)
    patient = find_sensdoc_patient(sensdoc_base, auth, family, birthdate, fixed_code)
    patient_id = str(patient.get("id") or "").strip()
    if not patient_id:
        raise RuntimeError("Gefundener Sensdoc-Patient hat keine id")

    print(f"🔎 Sensdoc-Patient gefunden: id={patient_id}, family={family}, birthdate={birthdate}, given={fixed_code}")

    api_key = os.getenv("ACTIMI_API_KEY") or ""
    if not api_key:
        raise RuntimeError("Umgebungsvariable ACTIMI_API_KEY fehlt, um Actimi zu erreichen")
    token = ir.get_token(api_key)

    print("🔄 Hole Actimi-Observations...")
    actimi_observations = fetch_observations(token)
    observations = [obs for obs in actimi_observations if classify_observation(obs) is not None]
    print(f"ℹ️ {len(observations)} lesbare Observations aus Actimi gefunden")

    if not observations:
        print("❌ Keine passenden Observations zum Posten gefunden.")
        return 1

    posted = 0
    for obs in observations:
        effective_datetime = observation_effective_datetime(obs)
        encounter_ref = None
        if effective_datetime:
            encounter_ref = ensure_encounter(sensdoc_base, auth, patient_id, effective_datetime)
            print(f"🔁 Encounter für Observation bereitgestellt: {encounter_ref}")
        else:
            print("⚠️ Keine effektive Zeit in Observation gefunden; erstelle Encounter ohne Startzeit.")
            encounter_ref = ensure_encounter(sensdoc_base, auth, patient_id, None)

        sensdoc_obs = sanitize_observation(obs, patient_id, fixed_code, encounter_ref)
        obs_id = post_observation(sensdoc_base, auth, sensdoc_obs)
        print(f"✅ Observation erstellt/gefunden in Sensdoc: {obs_id}")
        posted += 1

    print(f"✅ {posted} Observations dem Sensdoc-Patienten {patient_id} zugewiesen")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
