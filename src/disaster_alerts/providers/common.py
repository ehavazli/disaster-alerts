from __future__ import annotations

import json
import logging
import os
import time
from typing import Any, Dict, Optional

import requests

log = logging.getLogger(__name__)

# Defaults
DEFAULT_TIMEOUT: float = 15.0  # seconds
DEFAULT_RETRIES: int = 2
DEFAULT_BACKOFF: float = 1.5  # exponential backoff base
DEFAULT_UA: str = "disaster-alerts (+contact: emre.havazli@jpl.nasa.gov)"

# Reuse a single session for connection pooling
_SESSION: requests.Session = requests.Session()

__all__ = ["get_json", "user_agent"]


def user_agent() -> str:
    """
    Compose a User-Agent string. Allows override via env:
    DISASTER_ALERTS_UA="my-agent/1.0".
    """
    return os.environ.get("DISASTER_ALERTS_UA", DEFAULT_UA)


def _sleep_for_retry(attempt: int, backoff: float, retry_after: Optional[str]) -> None:
    if retry_after:
        try:
            # Honor simple Retry-After (seconds); ignore HTTP-date formats.
            secs = float(retry_after)
            time.sleep(max(secs, 0.0))
            return
        except Exception:
            pass
    time.sleep(backoff**attempt)


def get_json(
    url: str,
    params: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    timeout: float = DEFAULT_TIMEOUT,
    retries: int = DEFAULT_RETRIES,
    backoff: float = DEFAULT_BACKOFF,
) -> Dict[str, Any]:
    """
    GET a JSON (or GeoJSON) endpoint with small retry/backoff.
    Returns {} on failure.

    Retries on non-2xx/3xx responses and network errors.
    Warns if Content-Type is not JSON but still attempts JSON decode.
    Honors simple numeric Retry-After headers when present.
    """
    hdrs = {
        "User-Agent": user_agent(),
        "Accept": "application/geo+json, application/json;q=0.9, */*;q=0.1",
    }
    if headers:
        hdrs.update(headers)

    attempt = 0
    while True:
        attempt += 1
        try:
            resp = _SESSION.get(url, params=params, headers=hdrs, timeout=timeout)
        except requests.RequestException as e:
            log.warning(
                "GET %s failed on attempt %d/%d: %s", url, attempt, retries + 1, e
            )
            if attempt > retries:
                return {}
            _sleep_for_retry(attempt, backoff, None)
            continue

        # Fast-path 304
        if resp.status_code == 304:
            log.debug("GET %s -> 304 Not Modified", url)
            return {}

        if 200 <= resp.status_code < 300:
            ctype = (resp.headers.get("Content-Type") or "").lower()
            if "json" not in ctype:
                log.warning("Expected JSON from %s but got Content-Type=%s", url, ctype)
            try:
                return resp.json()
            except json.JSONDecodeError:
                log.error("Failed to decode JSON from %s", url)
                return {}
        else:
            # Non-success status
            retry_after = resp.headers.get("Retry-After")
            log.warning(
                "GET %s -> HTTP %s (attempt %d/%d)",
                url,
                resp.status_code,
                attempt,
                retries + 1,
            )
            if attempt > retries:
                return {}
            _sleep_for_retry(attempt, backoff, retry_after)
