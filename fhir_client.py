from __future__ import annotations

from time import sleep
from typing import Any, Dict, Iterable, List, Optional

import requests

FHIR_HEADERS = {"Content-Type": "application/fhir+json", "Accept": "application/fhir+json"}
SESSION = requests.Session()
SESSION.trust_env = False


def http_get_json(
    url: str,
    auth: Optional[requests.auth.HTTPBasicAuth],
    params: Optional[List[tuple[str, str]]] = None,
    headers: Optional[Dict[str, str]] = None,
    timeout: int = 180,
    max_retries: int = 5,
) -> Any:
    last_error: Optional[Exception] = None
    for attempt in range(max_retries):
        try:
            resp = SESSION.get(url, auth=auth, params=params, headers=headers, timeout=timeout)
            if resp.status_code in (502, 503, 504):
                last_error = RuntimeError(f"GET returned {resp.status_code} for {url}")
                if attempt < max_retries - 1:
                    sleep(2 ** attempt)
                continue
            if resp.status_code != 200:
                raise RuntimeError(f"GET failed {resp.status_code} for {url}: {resp.text}")
            return resp.json()
        except (requests.exceptions.Timeout, requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError) as e:
            last_error = e
            if attempt < max_retries - 1:
                sleep(2 ** attempt)
    if last_error:
        raise last_error
    raise RuntimeError(f"GET failed for {url}: Unknown error")


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
