from __future__ import annotations

import argparse
import os
from pathlib import Path

from config_loader import build_settings, load_config, load_env_file
from sync_runner import sync


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sync Observation from Actimi to Sensdoc with patient match by given-name key.",
    )
    parser.add_argument("--config", default="config/actimi_to_sensdoc.yaml", help="Path to YAML config")
    parser.add_argument("--env-file", default=".env", help="Path to .env file")
    parser.add_argument("--window-minutes", type=int, help="Transfer only records from last N minutes")
    parser.add_argument("--given-key", action="append", default=[], help="Patient key (name.given), repeatable")
    parser.add_argument("--code", action="append", default=[], help="Only transfer these LOINC codes")
    parser.add_argument("--dry-run", action="store_true", help="No write to Sensdoc; only print summary")
    parser.add_argument("--debug", action="store_true", help="Print resolved settings and exit")
    parser.add_argument("--workers", type=int, default=4, help="Number of parallel worker threads (default: 4)")
    return parser.parse_args()


def print_debug(settings, config_path: Path, env_file_path: Path) -> None:
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

    try:
        cfg = load_config(config_path)
        print("config OK")
        settings = build_settings(args, cfg)
        print("settings OK:", settings.actimi_headers)
    except Exception:
        import traceback
        traceback.print_exc()
        return 1

    if args.debug:
        print_debug(settings, config_path, env_path)
        return 0

    sync(settings, workers=args.workers)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
