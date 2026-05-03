from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
import yaml

FHIR_HEADERS = {"Content-Type": "application/fhir+json", "Accept": "application/fhir+json"}
SESSION = requests.Session()
SESSION.trust_env = False


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Remove/stop a running vital-sign category attribute for a single patient in SensDoc.",
    )
    parser.add_argument("--config", default="config/stop_running1.yaml", help="Path to YAML config")
    parser.add_argument("--env-file", default="config/.env", help="Path to .env file")
    parser.add_argument("--dry-run", action="store_true", help="Only print planned updates")
    parser.add_argument("--limit", type=int, default=200, help="FHIR page size")
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


def build_auth(block: Dict[str, Any]) -> requests.auth.HTTPBasicAuth:
    auth = block.get("auth", {}) or {}
    user_env = str(auth.get("user_env", "FHIR_USER_post"))
    pass_env = str(auth.get("pass_env", "FHIR_PASS_post"))
    return requests.auth.HTTPBasicAuth(env_required(user_env), env_required(pass_env))


def http_get_json(
    url: str,
    auth: requests.auth.HTTPBasicAuth,
    params: Optional[List[Tuple[str, str]]] = None,
) -> Any:
    resp = SESSION.get(url, auth=auth, params=params, headers=FHIR_HEADERS, timeout=90)
    if resp.status_code != 200:
        raise RuntimeError(f"GET failed {resp.status_code} for {url}: {resp.text}")
    return resp.json()


def iter_resources(payload: Any) -> Iterable[Dict[str, Any]]:
    if isinstance(payload, dict) and isinstance(payload.get("entry"), list):
        for entry in payload.get("entry", []):
            if isinstance(entry, dict) and isinstance(entry.get("resource"), dict):
                yield entry["resource"]
        return
    if isinstance(payload, list):
        for item in payload:
            if isinstance(item, dict):
                yield item
        return
    if isinstance(payload, dict):
        yield payload


def fetch_observations_for_patient(
    base_url: str,
    auth: requests.auth.HTTPBasicAuth,
    patient_id: str,
    page_size: int,
) -> List[Dict[str, Any]]:
    variants = [
        [("subject", f"Patient/{patient_id}"), ("_count", str(page_size))],
        [("patient", patient_id), ("_count", str(page_size))],
    ]
    for params in variants:
        payload = http_get_json(f"{base_url}/Observation", auth, params=params)
        resources = [r for r in iter_resources(payload) if r.get("resourceType") == "Observation"]
        if resources:
            return resources
    return []


def _normalize_coding(coding: Dict[str, Any]) -> Tuple[str, str]:
    system = str(coding.get("system") or "").strip()
    code = str(coding.get("code") or "").strip()
    return system, code


def remove_category_codings(
    observation: Dict[str, Any],
    targets: List[Tuple[str, str]],
) -> bool:
    categories = observation.get("category")
    if not isinstance(categories, list) or not categories:
        return False

    changed = False
    new_categories: List[Dict[str, Any]] = []
    for category in categories:
        if not isinstance(category, dict):
            continue
        codings = category.get("coding")
        if not isinstance(codings, list):
            new_categories.append(category)
            continue

        kept: List[Dict[str, Any]] = []
        for coding in codings:
            if not isinstance(coding, dict):
                continue
            system, code = _normalize_coding(coding)
            if any((system == ts and code == tc) for ts, tc in targets):
                changed = True
                continue
            kept.append(coding)

        if kept:
            category["coding"] = kept
            new_categories.append(category)
        else:
            changed = True

    if changed:
        if new_categories:
            observation["category"] = new_categories
        else:
            observation.pop("category", None)
    return changed


def put_observation(base_url: str, auth: requests.auth.HTTPBasicAuth, observation: Dict[str, Any]) -> None:
    obs_id = str(observation.get("id") or "").strip()
    if not obs_id:
        raise RuntimeError("Observation without id cannot be updated")
    resp = SESSION.put(
        f"{base_url}/Observation/{obs_id}",
        auth=auth,
        headers=FHIR_HEADERS,
        data=json.dumps(observation),
        timeout=90,
    )
    if resp.status_code not in (200, 201):
        raise RuntimeError(f"PUT failed {resp.status_code} for Observation/{obs_id}: {resp.text}")


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parent
    cfg_path = Path(args.config)
    env_path = Path(args.env_file)
    if not cfg_path.is_absolute():
        cfg_path = root / cfg_path
    if not env_path.is_absolute():
        env_path = root / env_path

    load_env_file(env_path)
    cfg = load_config(cfg_path)

    sensdoc = cfg.get("sensdoc", {}) or {}
    base_url = str((sensdoc.get("database", {}) or {}).get("url", "")).rstrip("/")
    if not base_url:
        raise RuntimeError("Missing sensdoc.database.url in config")

    filter_cfg = cfg.get("filter", {}) or {}
    patient_id = str(filter_cfg.get("patient_id") or "").strip()
    if not patient_id:
        raise RuntimeError("Missing filter.patient_id in config")

    target_codings_cfg = cfg.get("target_category_codings", []) or [
        {"system": "http://nursit-institute.com/fhir/observation-category", "code": "vital-signs-ranges"}
    ]
    target_codings: List[Tuple[str, str]] = []
    for item in target_codings_cfg:
        if not isinstance(item, dict):
            continue
        system = str(item.get("system") or "").strip()
        code = str(item.get("code") or "").strip()
        if system and code:
            target_codings.append((system, code))
    if not target_codings:
        raise RuntimeError("No valid target_category_codings configured")

    dry_run = bool(args.dry_run or cfg.get("dry_run", False))
    auth = build_auth(sensdoc)

    observations = fetch_observations_for_patient(base_url, auth, patient_id, args.limit)
    scanned = len(observations)
    changed = 0
    updated = 0

    for observation in observations:
        cloned = json.loads(json.dumps(observation))
        if not remove_category_codings(cloned, target_codings):
            continue
        changed += 1
        obs_id = str(cloned.get("id") or "")
        if dry_run:
            print(f"[DRY-RUN] would update Observation/{obs_id}")
            continue
        put_observation(base_url, auth, cloned)
        updated += 1
        print(f"[OK] updated Observation/{obs_id}")

    print(
        f"Done stop_running. patient_id={patient_id}, scanned={scanned}, "
        f"changed={changed}, updated={updated}, dry_run={dry_run}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
