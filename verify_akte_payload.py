from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from config_loader import build_settings, load_config, load_env_file
from fhir_client import http_get_json, iter_resources
from sync_runner import fetch_patient_map_by_given


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify final FHIR payload in Sensdoc for one patient")
    parser.add_argument("--config", default="config/actimi_to_sensdoc.yaml", help="Path to YAML config")
    parser.add_argument("--env-file", default=".env", help="Path to .env file")
    parser.add_argument("--given-key", default="HGR2HKQ9", help="Given key to match (expected given[1] in Sensdoc)")
    parser.add_argument("--limit", type=int, default=200, help="FHIR _count limit")
    parser.add_argument("--show", type=int, default=100, help="How many rows to print")
    return parser.parse_args()


def _first_code(resource: Dict[str, Any]) -> str:
    code = resource.get("code") or {}
    if not isinstance(code, dict):
        return ""
    coding = code.get("coding") or []
    if not isinstance(coding, list):
        return ""
    for item in coding:
        if isinstance(item, dict):
            value = item.get("code")
            if isinstance(value, str) and value.strip():
                return value.strip()
    return ""


def _categories(resource: Dict[str, Any]) -> List[Tuple[str, str]]:
    out: List[Tuple[str, str]] = []
    for cat in resource.get("category", []) or []:
        if not isinstance(cat, dict):
            continue
        for coding in cat.get("coding", []) or []:
            if not isinstance(coding, dict):
                continue
            system = str(coding.get("system") or "").strip()
            code = str(coding.get("code") or "").strip()
            if system or code:
                out.append((system, code))
    return out


def _effective(resource: Dict[str, Any]) -> str:
    value = resource.get("effectiveDateTime")
    if isinstance(value, str) and value.strip():
        return value.strip()
    effective = resource.get("effective") or {}
    if isinstance(effective, dict):
        dt = effective.get("dateTime")
        if isinstance(dt, str) and dt.strip():
            return dt.strip()
    return ""


def _value(resource: Dict[str, Any]) -> str:
    vq = resource.get("valueQuantity")
    if isinstance(vq, dict):
        return f"{vq.get('value')} {vq.get('unit', '')}".strip()
    vs = resource.get("valueString")
    if isinstance(vs, str):
        return vs
    return ""


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

    ns = argparse.Namespace(
        config=str(config_path),
        env_file=str(env_path),
        window_minutes=None,
        given_key=[],
        code=[],
        dry_run=False,
        debug=False,
        workers=4,
    )
    settings = build_settings(ns, cfg)

    sens_map, _ = fetch_patient_map_by_given(
        settings.sensdoc_base,
        settings.sensdoc_auth,
        settings.page_count,
        second_given_only=True,
    )
    patient = sens_map.get(args.given_key)
    if not patient:
        print(f"[ERROR] No Sensdoc patient found for given[1]={args.given_key}")
        return 1

    patient_id = str(patient.get("id") or "").strip()
    print(f"patient_id={patient_id}")
    print(f"patient_name={patient.get('name')}")

    obs_payload = http_get_json(
        f"{settings.sensdoc_base}/Observation",
        settings.sensdoc_auth,
        params=[("subject", f"Patient/{patient_id}"), ("_count", str(args.limit))],
    )
    observations = [r for r in iter_resources(obs_payload) if r.get("resourceType") == "Observation"]
    print(f"observations_total={len(observations)}")

    code_counts: Dict[str, int] = {}
    rows: List[Tuple[str, str, str, List[Tuple[str, str]], str]] = []
    for obs in observations:
        code = _first_code(obs) or "?"
        code_counts[code] = code_counts.get(code, 0) + 1
        rows.append((_effective(obs), code, _value(obs), _categories(obs), str(obs.get("id") or "")))

    print("code_counts=", code_counts)

    rows.sort(key=lambda x: x[0])
    print("--- observations (effective | code | value | categories | id) ---")
    for row in rows[: args.show]:
        print(f"{row[0]} | {row[1]} | {row[2]} | {row[3]} | {row[4]}")

    com_payload = http_get_json(
        f"{settings.sensdoc_base}/Communication",
        settings.sensdoc_auth,
        params=[("subject", f"Patient/{patient_id}"), ("_count", str(args.limit))],
    )
    communications = [r for r in iter_resources(com_payload) if r.get("resourceType") == "Communication"]
    print(f"communications_total={len(communications)}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
