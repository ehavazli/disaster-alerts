from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from ..settings import Settings
from .common import get_json

log = logging.getLogger(__name__)

Event = Dict[str, Any]
NWS_ACTIVE_URL = "https://api.weather.gov/alerts/active"


def _pick_str(d: Dict[str, Any], *keys: str) -> Optional[str]:
    """Return the first non-empty string value for any of keys in dict d."""
    for k in keys:
        v = d.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return None


def _preferred_link(feature: Dict[str, Any], props: Dict[str, Any]) -> Optional[str]:
    """
    Choose the most useful link for an alert.

    Preference:
    1) feature["id"] (NWS Feature URL)
    2) props["id"] / props["@id"] (also often a URL)
    3) props["url"] (sometimes present)
    4) fall back to first "references" URL if present
    """
    # 1–3
    link = (
        _pick_str(feature, "id")
        or _pick_str(props, "id", "@id")
        or _pick_str(props, "url")
    )
    if link:
        return link

    # 4) references can be a list of dicts or strings
    refs = props.get("references")
    if isinstance(refs, list) and refs:
        first = refs[0]
        if isinstance(first, str):
            return first.strip() or None
        if isinstance(first, dict):
            # Some shapes: {"identifier": "...", "sender": "...", "url": "..."}
            return _pick_str(first, "url", "identifier")
    return None


def _severity(props: Dict[str, Any]) -> Optional[str]:
    s = props.get("severity")
    return s if isinstance(s, str) and s.strip() else None


def _updated(props: Dict[str, Any]) -> Optional[str]:
    # Prefer effective/onset/sent. 'updated' and 'ends' are last-resort
    return _pick_str(props, "effective", "onset", "sent", "updated", "ends")


def fetch_events(settings: Settings) -> List[Event]:
    """
    Fetch all active NWS alerts and normalize them into Event dicts.
    Returns an empty list on failure.
    """
    data = get_json(NWS_ACTIVE_URL)
    feats = data.get("features") or []
    if not isinstance(feats, list):
        log.warning("NWS response missing 'features' list")
        return []

    out: List[Event] = []
    for f in feats:
        try:
            if not isinstance(f, dict):
                continue
            props = f.get("properties") or {}
            if not isinstance(props, dict):
                props = {}

            fid = (
                _pick_str(f, "id")
                or _pick_str(props, "id", "@id")
                or _pick_str(props, "event", "headline")
                or "nws-unknown"
            )

            title = _pick_str(props, "headline", "event") or "(NWS Alert)"
            sev = _severity(props)
            link = _preferred_link(f, props)

            ev: Event = {
                "id": fid,
                "provider": "nws",
                "updated": _updated(props),
                "title": title,
                "severity": sev,
                "link": link,
                "geometry": f.get("geometry"),
                "properties": props,  # keep full properties for downstream formatting
                # Pipeline may override routing later; still set a sensible default.
                "routing_key": (
                    "severe"
                    if (sev or "").lower() in {"severe", "extreme"}
                    else "default"
                ),
            }
            out.append(ev)
        except Exception as e:
            # Keep this quiet (debug) so a single malformed feature doesn't spam logs.
            log.debug("Skipping malformed NWS feature: %s", e, exc_info=False)
            continue

    log.info("NWS: normalized %d active alert(s)", len(out))
    return out
