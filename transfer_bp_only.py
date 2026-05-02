from __future__ import annotations

import argparse
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional

from config_loader import build_settings, load_config, load_env_file
from sync_runner import (
    fetch_all_actimi_observations,
    fetch_patient_map_by_given,
    post_or_put_observation,
)
from transformations import (
    build_target_observation,
    expand_observation_for_transfer,
    extract_effective_datetime,
    has_observation_value,
    observation_transfer_code,
)

BP_CODES = {"8480-6", "8462-4"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Transfer only blood pressure observations to Sensdoc")
    parser.add_argument("--config", default="config/actimi_to_sensdoc.yaml", help="Path to YAML config")
    parser.add_argument("--env-file", default=".env", help="Path to .env file")
    parser.add_argument("--window-minutes", type=int, help="Transfer only records from last N minutes")
    parser.add_argument("--given-key", action="append", default=[], help="Patient key (name.given), repeatable")
    parser.add_argument("--code", action="append", default=[], help="Unused here; kept for shared settings parser compatibility")
    parser.add_argument("--dry-run", action="store_true", help="No write to Sensdoc; only print summary")
    return parser.parse_args()


def _parse_dt(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


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
    print("access_TOKEN =", os.getenv("access_TOKEN"))
    print("ACTIMI_TOKEN =", os.getenv("ACTIMI_TOKEN"))
    print("ACTIMI_API_KEY =", os.getenv("ACTIMI_API_KEY"))

    cfg = load_config(config_path)

    # Reuse global settings, but force BP-only behavior in this script
    settings = build_settings(args, cfg)
    settings = settings.__class__(
        **{**settings.__dict__, "only_codes": ["8480-6", "8462-4"], "dry_run": bool(args.dry_run or settings.dry_run)}
    )

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

    wanted_keys = settings.given_keys or sorted(set(actimi_patients.keys()))
    from_time = datetime.now(timezone.utc) - timedelta(minutes=settings.window_minutes)

    scanned = 0
    eligible = 0
    created = 0
    updated = 0
    missing_in_actimi = 0
    missing_in_sensdoc = 0

    for given_key in wanted_keys:
        actimi_patient = actimi_patients.get(given_key)
        if not actimi_patient:
            missing_in_actimi += 1
            print(f"[WARN] Given key not found in Actimi: {given_key}")
            continue

        sensdoc_patient = sensdoc_patients.get(given_key)
        if not sensdoc_patient:
            for patient in sensdoc_patients_all:
                for name in patient.get("name", []) or []:
                    if not isinstance(name, dict):
                        continue
                    for g in name.get("given", []) or []:
                        if isinstance(g, str) and g.strip().lower() == given_key.strip().lower():
                            sensdoc_patient = patient
                            break
                    if sensdoc_patient:
                        break
                if sensdoc_patient:
                    break
        if not sensdoc_patient:
            missing_in_sensdoc += 1
            print(f"[WARN] Given key not found in Sensdoc: {given_key}")
            continue

        sensdoc_id = str(sensdoc_patient["id"])
        observations = actimi_observations_by_patient.get(given_key, [])

        for observation in observations:
            scanned += 1
            dt = _parse_dt(extract_effective_datetime(observation))
            if not dt or dt < from_time:
                continue

            expanded = expand_observation_for_transfer(observation)
            for item in expanded:
                if not has_observation_value(item):
                    continue
                code = observation_transfer_code(item)
                if code not in BP_CODES:
                    continue
                eligible += 1

                target_obs = build_target_observation(
                    item,
                    sensdoc_id,
                    settings.source_identifier_system,
                    settings,
                )

                if settings.dry_run:
                    print(f"[DRY] would upsert code={code} effective={target_obs.get('effectiveDateTime')} patient={sensdoc_id}")
                    continue

                action, _obs_id = post_or_put_observation(
                    target_obs,
                    settings.sensdoc_base,
                    settings.sensdoc_auth,
                    settings.source_identifier_system,
                )
                if action == "created":
                    created += 1
                else:
                    updated += 1

    print(
        f"Done BP sync. scanned={scanned}, eligible_bp={eligible}, created={created}, updated={updated}, "
        f"missing_in_actimi={missing_in_actimi}, missing_in_sensdoc={missing_in_sensdoc}, dry_run={settings.dry_run}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
