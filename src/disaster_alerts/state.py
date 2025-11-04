from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

log = logging.getLogger(__name__)

# --------------------------- helpers ---------------------------


def _parse_iso8601(ts: Optional[str]) -> Optional[datetime]:
    """
    Parse a common subset of ISO8601 into an aware UTC datetime.
    Returns None if invalid/empty.

    Accepts:
      2025-10-29T21:36:47Z
      2025-10-29T21:36:47.776Z
      2025-10-29T21:36:47
      2025-10-29T21:36:47+00:00
      2025-10-29T21:36:47-07:00
    """
    if not ts or not isinstance(ts, str):
        return None
    s = ts.strip()
    try:
        # Normalize trailing 'Z' to +00:00 for fromisoformat
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
    except Exception:
        return None
    # Make naive datetimes UTC-aware
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt


def _is_newer(a: Optional[str], b: Optional[str]) -> bool:
    """
    True if ISO timestamp `a` is strictly newer than `b`. None is lowest.
    """
    da, db = _parse_iso8601(a), _parse_iso8601(b)
    if da is None:
        return False
    if db is None:
        return True
    return da > db


# --------------------------- core types ---------------------------

DEFAULT_LRU_LIMIT = 5000


def _env_lru_limit() -> int:
    """Read current env at runtime for testability (monkeypatch-friendly)."""
    try:
        return int(os.environ.get("DISASTER_ALERTS_STATE_LRU", str(DEFAULT_LRU_LIMIT)))
    except Exception:
        return DEFAULT_LRU_LIMIT


@dataclass
class _ProviderState:
    ids: List[str] = field(default_factory=list)  # most recent first
    last_updated: Optional[str] = None  # ISO8601 string (UTC preferred)

    def add_id(self, eid: str, lru_limit: int) -> None:
        """Move eid to front; cap by lru_limit."""
        if not eid:
            return
        if self.ids and self.ids[0] == eid:
            return
        try:
            self.ids.remove(eid)
        except ValueError:
            pass
        self.ids.insert(0, eid)
        if len(self.ids) > lru_limit:
            del self.ids[lru_limit:]

    def consider_updated(self, updated: Optional[str]) -> None:
        """Advance watermark if `updated` is newer."""
        if updated and _is_newer(updated, self.last_updated):
            self.last_updated = updated


@dataclass
class State:
    path: Path
    version: int = 1
    providers: Dict[str, _ProviderState] = field(default_factory=dict)
    lru_limit: int = field(default_factory=_env_lru_limit)

    # ---------------------- construction ----------------------

    @classmethod
    def load(cls, path: Path) -> "State":
        """
        Load state from JSON file. If missing or corrupt, return an empty state.
        Ensures parent directory exists.
        """
        path.parent.mkdir(parents=True, exist_ok=True)
        if not path.exists():
            log.debug("State not found at %s; starting fresh", path)
            return cls(path=path)

        try:
            raw = path.read_text(encoding="utf-8")
            data = json.loads(raw)
        except Exception:
            # Corrupt file; back it up once and start fresh
            try:
                backup = path.with_suffix(".json.bak")
                if not backup.exists():
                    path.replace(backup)
                else:
                    path.unlink(missing_ok=True)
            except Exception:
                pass
            log.warning("State was corrupt at %s; reset to empty", path)
            return cls(path=path)

        version = int(data.get("version", 1))
        prov_raw = data.get("providers", {}) or {}
        providers: Dict[str, _ProviderState] = {}

        if isinstance(prov_raw, dict):
            for name, obj in prov_raw.items():
                if not isinstance(obj, dict):
                    continue
                ids = obj.get("ids") or []
                if not isinstance(ids, list):
                    ids = []
                ids = [s for s in ids if isinstance(s, str)]
                last_updated = obj.get("last_updated")
                if not isinstance(last_updated, str):
                    last_updated = None
                providers[name] = _ProviderState(ids=ids, last_updated=last_updated)

        lru_limit = int(data.get("lru_limit", _env_lru_limit()))
        return cls(path=path, version=version, providers=providers, lru_limit=lru_limit)

    # ---------------------- query / update ----------------------

    def _prov(self, name: str) -> _ProviderState:
        ps = self.providers.get(name)
        if ps is None:
            ps = _ProviderState()
            self.providers[name] = ps
        return ps

    def is_new(self, event: Dict[str, Any]) -> bool:
        """
        Return True if event has not been seen before.

        Criteria:
          1) Event id not in provider LRU list
          2) Watermark is advisory only; we still check ids to allow late arrivals
        """
        provider = str(event.get("provider") or "").strip() or "unknown"
        eid = str(event.get("id") or "").strip()
        if not eid:
            # If an event has no id, treat as notifiable (cannot dedup safely)
            return True
        return eid not in self._prov(provider).ids

    def update_with(self, events: List[Dict[str, Any]]) -> None:
        """
        Update internal state with a batch of events that were notified:
        - Adds each event id to provider LRU
        - Advances provider last_updated to the max 'updated' in the batch
        """
        per_provider_max_updated: Dict[str, Optional[str]] = {}
        for e in events:
            provider = str(e.get("provider") or "").strip() or "unknown"
            eid = str(e.get("id") or "").strip()
            updated = e.get("updated") if isinstance(e.get("updated"), str) else None

            self._prov(provider).add_id(eid, self.lru_limit)

            prev = per_provider_max_updated.get(provider)
            per_provider_max_updated[provider] = (
                updated if _is_newer(updated, prev) else prev
            )

        for provider, new_max in per_provider_max_updated.items():
            if new_max:
                self._prov(provider).consider_updated(new_max)

    # ---------------------- persistence ----------------------

    def to_dict(self) -> Dict[str, Any]:
        return {
            "version": self.version,
            "lru_limit": self.lru_limit,
            "providers": {
                name: {
                    "ids": ps.ids,
                    "last_updated": ps.last_updated,
                }
                for name, ps in self.providers.items()
            },
        }

    def save(self) -> None:
        """
        Atomically write state JSON:
          1) write to temp file in same dir
          2) flush to disk
          3) replace target with os.replace
        """
        tmp = self.path.with_suffix(".json.tmp")
        data = json.dumps(self.to_dict(), ensure_ascii=False, indent=2)

        # Write & flush
        with open(tmp, "w", encoding="utf-8") as fh:
            fh.write(data)
            fh.flush()
            os.fsync(fh.fileno())
        # Atomic replace
        os.replace(tmp, self.path)
