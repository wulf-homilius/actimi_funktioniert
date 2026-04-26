from __future__ import annotations

import argparse
import importlib.util
import inspect
import json
import os
from pathlib import Path
from typing import Any, Iterable, Optional

import requests

DEFAULT_API_BASE = "https://ovok.api.actimi.health/v2/partner"
DEFAULT_PATIENT_URL = f"{DEFAULT_API_BASE}/Patient"
DEFAULT_TOKEN_URLS = (
    f"{DEFAULT_API_BASE}/access-token",
    f"{DEFAULT_API_BASE}/token",
    f"{DEFAULT_API_BASE}/auth/token",
)
DEFAULT_API_KEY = "O2SUM53XRT24E6XD1LWZDEHT"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download patient data from Actimi partner API.")
    parser.add_argument("--env-file", default="config/.env", help="Optional .env file path")
    parser.add_argument("--token-url", help="Optional explicit token endpoint URL")
    parser.add_argument("--patients-url", default=DEFAULT_PATIENT_URL, help="Patient endpoint URL")
    parser.add_argument("--output", default="patients.json", help="Output JSON file path")
    parser.add_argument("--timeout", type=int, default=45, help="Request timeout in seconds")
    return parser.parse_args()


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


def iter_token_urls(explicit_url: Optional[str]) -> Iterable[str]:
    if explicit_url:
        yield explicit_url
        return
    env_url = os.getenv("ACTIMI_TOKEN_URL", "").strip()
    if env_url:
        yield env_url
        return
    for url in DEFAULT_TOKEN_URLS:
        yield url


def parse_token(payload: Any) -> Optional[str]:
    if isinstance(payload, dict):
        for key in ("access_token", "accessToken", "token", "jwt"):
            value = payload.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        data = payload.get("data")
        if isinstance(data, dict):
            return parse_token(data)
    return None


def request_access_token(api_key: str, *, explicit_url: Optional[str], timeout: int) -> str:
    session = requests.Session()
    for token_url in iter_token_urls(explicit_url):
        try:
            response = session.post(
                token_url,
                json={"apiKey": api_key},
                headers={
                    "Content-Type": "application/json",
                    "x-api-key": api_key,
                    "Authorization": f"Bearer {api_key}",
                },
                timeout=timeout,
            )
        except requests.RequestException:
            continue
        if response.status_code >= 400:
            continue
        token = parse_token(response.json())
        if token:
            return token
    raise RuntimeError(
        "Could not retrieve access token. Set ACTIMI_TOKEN_URL or pass --token-url "
        "if your token endpoint differs from defaults."
    )


def try_token_from_import_requests(api_key: str) -> Optional[str]:
    module_path = Path.cwd() / "import_requests.py"
    if not module_path.exists():
        return None

    spec = importlib.util.spec_from_file_location("import_requests", module_path)
    if spec is None or spec.loader is None:
        return None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    for func_name in (
        "get_access_token",
        "fetch_access_token",
        "request_access_token",
        "obtain_access_token",
    ):
        func = getattr(module, func_name, None)
        if not callable(func):
            continue
        try:
            signature = inspect.signature(func)
            if len(signature.parameters) == 0:
                value = func()
            else:
                value = func(api_key)
        except Exception:
            continue
        if isinstance(value, str) and value.strip():
            return value.strip()
        if isinstance(value, dict):
            parsed = parse_token(value)
            if parsed:
                return parsed
    return None


def extract_patients(payload: Any) -> Any:
    if isinstance(payload, dict) and isinstance(payload.get("entry"), list):
        out = []
        for entry in payload["entry"]:
            if isinstance(entry, dict):
                resource = entry.get("resource")
                if isinstance(resource, dict):
                    out.append(resource)
        if out:
            return out
    return payload


def download_patients(
    patients_url: str,
    *,
    token: str,
    api_key: str,
    timeout: int,
) -> Any:
    response = requests.get(
        patients_url,
        headers={
            "Authorization": f"Bearer {token}",
            "x-api-key": api_key,
            "Accept": "application/json",
        },
        timeout=timeout,
    )
    response.raise_for_status()
    return extract_patients(response.json())


def main() -> int:
    args = parse_args()
    env_file = Path(args.env_file)
    if not env_file.is_absolute():
        env_file = Path.cwd() / env_file
    load_env_file(env_file)

    api_key = os.getenv("ACTIMI_API_KEY", DEFAULT_API_KEY).strip()
    if not api_key:
        raise RuntimeError("Missing ACTIMI_API_KEY")

    token = request_access_token(
        api_key,
        explicit_url=args.token_url,
        timeout=args.timeout,
    )
    patients = download_patients(args.patients_url, token=token, api_key=api_key, timeout=args.timeout)

    out_path = Path(args.output)
    if not out_path.is_absolute():
        out_path = Path.cwd() / out_path
    out_path.write_text(json.dumps(patients, ensure_ascii=False, indent=2), encoding="utf-8")

    count = len(patients) if isinstance(patients, list) else 1
    print(f"Downloaded {count} patient record(s) to: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
