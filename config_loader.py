from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
import yaml


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
    communication_payloads: Dict[str, str]
    observation_category_codings: List[Dict[str, str]]


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
    from fhir_client import SESSION

    headers = {"accept": "application/json", "content-type": "application/json"}
    payload = {"apiKey": api_key}
    response = SESSION.post(token_url, headers=headers, json=payload, timeout=45)
    if response.status_code != 200:
        raise RuntimeError(f"Actimi token request failed {response.status_code} for {token_url}: {response.text}")
    data: Any = response.json()
    if isinstance(data, dict):
        for key in ("token", "accessToken", "accessTOKEN", "access_token", "jwt"):
            value = data.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        nested = data.get("data")
        if isinstance(nested, dict):
            for key in ("token", "accessToken", "accessTOKEN", "access_token", "jwt"):
                value = nested.get(key)
                if isinstance(value, str) and value.strip():
                    return value.strip()
    raise RuntimeError(f"Actimi token missing in response: {data}")


def build_settings(args: Any, cfg: Dict[str, Any]) -> Settings:
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
            primary_key = str(entry.get("primary_key") or entry.get("given_key") or entry.get("actimi_given") or "").strip()
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

    if api_key_value and not dry_run:
        token_value = request_actimi_access_token(api_key_value, token_url)

    print(f"[DEBUG build_settings] api_key_env={api_key_env!r}, api_key_found={bool(api_key_value)}, token_found={bool(token_value)}, token_prefix={token_prefix!r}")

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
        communication_category_system=str(sync_block.get("communication_category_system", "http://www.nursiti.com/notificationType")),
        communication_category_code=(str(sync_block.get("communication_category_code")).strip() if sync_block.get("communication_category_code") is not None else None),
        communication_category_display=(str(sync_block.get("communication_category_display")).strip() if sync_block.get("communication_category_display") is not None else None),
        communication_payloads=dict(sync_block.get("communication_payloads", {}) or {}),
        observation_category_codings=[
            {
                "system": str(item.get("system", "")).strip(),
                "code": str(item.get("code", "")).strip(),
                **({"display": str(item.get("display", "")).strip()} if item.get("display") is not None and str(item.get("display", "")).strip() else {}),
            }
            for item in (sync_block.get("observation_categories", []) or [])
            if isinstance(item, dict)
            and str(item.get("system", "")).strip()
            and str(item.get("code", "")).strip()
        ] or [
            {"system": "http://hl7.org/fhir/observation-category", "code": "vital-signs", "display": "Vital Signs"},
            {"system": "http://nursit-institute.com/fhir/observation-category", "code": "vital-signs-ranges", "display": "Vital Signs Ranges"},
        ],
    )
