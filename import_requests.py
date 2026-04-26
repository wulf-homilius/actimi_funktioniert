import os
from pathlib import Path
from typing import Any

import requests
#observation_get enthält Funktionen zum Abrufen von Beobachtungsdaten von der Actimi API
TOKEN_URL = "https://ovok.api.actimi.health/v2/partner/Auth/token"
OBS_URL = "https://ovok.api.actimi.health/v2/partner/Observation"
SESSION = requests.Session()
SESSION.trust_env = False


def load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def get_token(api_key: str) -> str:
    headers = {"accept": "application/json", "content-type": "application/json"}
    payload = {"apiKey": api_key}
    response = SESSION.post(TOKEN_URL, headers=headers, json=payload, timeout=30)
    response.raise_for_status()
    data: Any = response.json()
    if isinstance(data, dict):
        for key in ("token", "accessToken", "access_token", "jwt"):
            value = data.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
    raise RuntimeError(f"Token not found in response: {data}")


def get_observations(token: str) -> None:
    headers = {"accept": "application/json", "Authorization": f"Bearer {token}"}
    attempts = [
        {
            "afterDateTime": "2024-01-01T00:00:00Z",
            "beforeDateTime": "2026-04-16T23:59:59Z",
            "code": "heart-rate",
            "sort": "asc",
        },
        {
            # Some implementations expect prefixes directly in these params.
            "afterDateTime": "ge2024-01-01T00:00:00Z",
            "beforeDateTime": "le2026-12-31T23:59:59Z",
            "code": "heart-rate",
            "sort": "asc",
        },
        {
            # Single-parameter range fallback.
            "afterDateTime": "ge2024-01-01T00:00:00Z,le2026-12-31T23:59:59Z",
            "code": "heart-rate",
            "sort": "asc",
        },
        {
            # Lower bound only.
            "afterDateTime": "ge2024-01-01T00:00:00Z",
            "code": "heart-rate",
            "sort": "asc",
        },
        {
            # FHIR-style date range fallback (many servers expect this).
            "date": ["ge2024-01-01T00:00:00Z", "le2026-12-31T23:59:59Z"],
            "code": "heart-rate",
            "_sort": "date",
        },
        {
            # LOINC fallback for heart-rate
            "date": ["ge2024-01-01T00:00:00Z", "le2026-12-31T23:59:59Z"],
            "code": "8867-4",
            "_sort": "date",
        },
        {
            # No date filter: checks if code itself is accepted.
            "code": "heart-rate",
            "sort": "asc",
        },
        {
            # No date filter + LOINC code fallback.
            "code": "8867-4",
            "sort": "asc",
        },
        {
            # Fully relaxed: no date, no code.
            "sort": "asc",
        },
    ]

    for idx, params in enumerate(attempts, start=1):
        response = SESSION.get(OBS_URL, headers=headers, params=params, timeout=30)
        print(f"ATTEMPT {idx} URL:", response.url)
        print(f"ATTEMPT {idx} STATUS:", response.status_code)
        if response.status_code == 200:
            body = response.text
            print(body)
            try:
                parsed = response.json()
            except Exception:
                return
            if isinstance(parsed, list) and len(parsed) == 0:
                print(f"ATTEMPT {idx}: empty list, trying next variant...")
                continue
            if isinstance(parsed, dict):
                entries = parsed.get("entry")
                if isinstance(entries, list) and len(entries) == 0:
                    print(f"ATTEMPT {idx}: empty bundle, trying next variant...")
                    continue
            return
        print(response.text)

    print("No query variant worked. Please send this output to the API provider.")


def main() -> int:
    root = Path(__file__).resolve().parent
    load_env_file(root / "config" / ".env")
    api_key = os.getenv("ACTIMI_API_KEY", "O2SUM53XRT24E6XD1LWZDEHT")
    token = get_token(api_key)
    print("TOKEN_OK")
    get_observations(token)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
