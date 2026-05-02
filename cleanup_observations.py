import argparse
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import requests
import yaml

FHIR_HEADERS = {"Content-Type": "application/fhir+json", "Accept": "application/fhir+json"}
SESSION = requests.Session()
SESSION.trust_env = False


def load_config(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


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


def iter_resources(payload: Any) -> Iterable[Dict[str, Any]]:
    if isinstance(payload, list):
        for item in payload:
            if isinstance(item, dict):
                yield item
        return
    if isinstance(payload, dict) and isinstance(payload.get("entry"), list):
        for entry in payload["entry"]:
            if isinstance(entry, dict) and isinstance(entry.get("resource"), dict):
                yield entry["resource"]
        return
    if isinstance(payload, dict):
        yield payload


def parse_dt(value: Optional[str]) -> Optional[datetime]:
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def extract_effective_datetime(observation: Dict[str, Any]) -> Optional[str]:
    value = observation.get("effectiveDateTime")
    if isinstance(value, str) and value.strip():
        return value.strip()
    effective = observation.get("effective") or {}
    if isinstance(effective, dict):
        dt = effective.get("dateTime")
        if isinstance(dt, str) and dt.strip():
            return dt.strip()
    return None


def observation_code(observation: Dict[str, Any]) -> str:
    code = observation.get("code") or {}
    if isinstance(code, dict):
        coding = code.get("coding") or []
        if isinstance(coding, list):
            for item in coding:
                if isinstance(item, dict):
                    code_value = item.get("code")
                    if isinstance(code_value, str) and code_value.strip():
                        return code_value.strip()
    return ""


def http_get_json(url: str, auth: requests.auth.HTTPBasicAuth, params: List[tuple[str, str]]) -> Any:
    resp = SESSION.get(url, auth=auth, params=params, headers={"Accept": "application/fhir+json"}, timeout=60)
    if resp.status_code != 200:
        raise RuntimeError(f"GET failed {resp.status_code} for {url}: {resp.text}")
    return resp.json()


def fetch_patient(base_url: str, auth: requests.auth.HTTPBasicAuth, patient_id: str) -> Dict[str, Any]:
    payload = http_get_json(f"{base_url}/Patient/{patient_id}", auth, [])
    if payload.get("resourceType") != "Patient":
        raise RuntimeError(f"Patient/{patient_id} not found")
    return payload


def fetch_patient_by_given(base_url: str, auth: requests.auth.HTTPBasicAuth, given_key: str, page_count: int) -> Dict[str, Any]:
    payload = http_get_json(f"{base_url}/Patient", auth, [("_count", str(page_count))])
    matches: List[Dict[str, Any]] = []
    for patient in iter_resources(payload):
        if patient.get("resourceType") != "Patient":
            continue
        for name in patient.get("name", []) or []:
            if not isinstance(name, dict):
                continue
            for given in name.get("given", []) or []:
                if isinstance(given, str) and given.strip() == given_key:
                    matches.append(patient)
                    break
    if len(matches) != 1:
        raise RuntimeError(f"Expected exactly one patient for given '{given_key}', found {len(matches)}")
    return matches[0]


def fetch_patient_observations(base_url: str, auth: requests.auth.HTTPBasicAuth, patient_id: str, page_count: int) -> List[Dict[str, Any]]:
    payload = http_get_json(
        f"{base_url}/Observation",
        auth,
        [("subject", f"Patient/{patient_id}"), ("_count", str(page_count))],
    )
    out: List[Dict[str, Any]] = []
    for resource in iter_resources(payload):
        if resource.get("resourceType") == "Observation":
            out.append(resource)
    return out


def should_delete(observation: Dict[str, Any], cutoff_date: Optional[datetime], only_codes: List[str]) -> bool:
    eff = extract_effective_datetime(observation)
    dt = parse_dt(eff)
    code = observation_code(observation)

    if only_codes and code not in only_codes:
        return False
    if cutoff_date and dt is not None and dt.date() <= cutoff_date.date():
        return False
    if cutoff_date and dt is None:
        return False
    return True


def delete_observation(base_url: str, auth: requests.auth.HTTPBasicAuth, obs_id: str) -> None:
    resp = SESSION.delete(f"{base_url}/Observation/{obs_id}", auth=auth, timeout=60)
    if resp.status_code not in (200, 202, 204):
        raise RuntimeError(f"DELETE Observation/{obs_id} failed {resp.status_code}: {resp.text}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Delete wrong Sensdoc observations for one patient.")
    parser.add_argument("--config", default="config/actimi_to_sensdoc.yaml")
    parser.add_argument("--env-file", default="config/.env")
    parser.add_argument("--patient-id", default="")
    parser.add_argument("--given-key", default="HGR2HKQ9")
    parser.add_argument("--cutoff-date", default="2026-04-25", help="Keep observations on/before this date (YYYY-MM-DD)")
    parser.add_argument("--code", action="append", default=[], help="Delete only these codes; repeatable")
    parser.add_argument("--page-count", type=int, default=400)
    parser.add_argument("--apply", action="store_true", help="Actually delete. Without this flag, dry-run only.")
    args = parser.parse_args()

    root = Path(__file__).resolve().parent
    config_path = Path(args.config)
    env_path = Path(args.env_file)
    if not config_path.is_absolute():
        config_path = root / config_path
    if not env_path.is_absolute():
        env_path = root / env_path

    load_env_file(env_path)
    cfg = load_config(config_path)

    sensdoc = cfg.get("sensdoc", {}) or {}
    sensdoc_url = str(sensdoc.get("url", "")).rstrip("/")
    auth_cfg = sensdoc.get("auth", {}) or {}
    user_env = str(auth_cfg.get("user_env", "FHIR_USER_post"))
    pass_env = str(auth_cfg.get("pass_env", "FHIR_PASS_post"))
    auth = requests.auth.HTTPBasicAuth(env_required(user_env), env_required(pass_env))

    if not sensdoc_url:
        raise RuntimeError("Missing sensdoc.url in config")

    patient_id = args.patient_id.strip()
    if not patient_id:
        patient = fetch_patient_by_given(sensdoc_url, auth, args.given_key.strip(), args.page_count)
        patient_id = str(patient.get("id") or "").strip()
    else:
        _ = fetch_patient(sensdoc_url, auth, patient_id)

    if not patient_id:
        raise RuntimeError("Could not resolve patient id")

    cutoff = datetime.fromisoformat(args.cutoff_date)
    only_codes = [c.strip() for c in args.code if c.strip()]

    observations = fetch_patient_observations(sensdoc_url, auth, patient_id, args.page_count)
    candidates: List[Dict[str, Any]] = []
    for obs in observations:
        if should_delete(obs, cutoff, only_codes):
            candidates.append(obs)

    print(f"patient_id={patient_id}")
    print(f"total_observations={len(observations)}")
    print(f"delete_candidates={len(candidates)}")
    print(f"dry_run={not args.apply}")

    for obs in sorted(candidates, key=lambda o: extract_effective_datetime(o) or ""):
        obs_id = str(obs.get("id") or "")
        code = observation_code(obs)
        eff = extract_effective_datetime(obs)
        print(f"- Observation/{obs_id} code={code or '?'} effective={eff or '?'}")

    if args.apply:
        for obs in candidates:
            obs_id = str(obs.get("id") or "").strip()
            if not obs_id:
                continue
            delete_observation(sensdoc_url, auth, obs_id)
        print(f"deleted={len(candidates)}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
