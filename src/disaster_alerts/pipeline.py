from __future__ import annotations

import logging
from collections import defaultdict
from typing import Any, Dict, Iterable, List, Tuple

from . import email as _email
from . import rules as _rules
from .providers import fetch_from_enabled as _fetch_from_enabled
from .settings import Settings, Thresholds
from .state import State as _State

log = logging.getLogger(__name__)
Event = Dict[str, Any]


# ------------------------ logging setup ------------------------


def _setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


# ------------------------ fetch ------------------------


def _collect_events(settings: Settings) -> List[Event]:
    events = _fetch_from_enabled(settings)
    # Normalize required/expected keys defensively
    out: List[Event] = []
    for i, e in enumerate(events):
        if not isinstance(e, dict):
            log.debug("Skipping non-dict event at %d: %r", i, e)
            continue
        if "id" not in e or "provider" not in e:
            raise RuntimeError(f"Event #{i} missing required keys 'id'/'provider'")
        e.setdefault("routing_key", "default")
        e.setdefault("updated", None)
        e.setdefault("title", "")
        e.setdefault("severity", None)
        e.setdefault("link", None)
        e.setdefault("properties", {})
        out.append(e)
    log.info(
        "Fetched total %d event(s) from %d provider(s)",
        len(out),
        len(settings.enabled_providers),
    )
    return out


# ------------------------ filtering & dedup ------------------------


def _apply_rules(
    events: List[Event],
    thresholds: Thresholds,
    aoi: Dict[str, Any] | None,
) -> List[Event]:
    return _rules.filter_events(events, thresholds, aoi)


def _only_new(events: List[Event], state: _State) -> List[Event]:
    """Return events not yet seen according to the current state (no mutation)."""
    return [e for e in events if state.is_new(e)]


# ------------------------ routing & email ------------------------


def _group_by_routing_key(
    events: Iterable[Event], settings: Settings
) -> Dict[str, List[Event]]:
    """
    Apply routing:
      - force_group: send everything to this group
      - drop_groups: remove listed groups entirely
      - merge: remap source->target groups
    """
    cfg = settings.app.routing
    drop = set(cfg.drop_groups or [])
    groups: Dict[str, List[Event]] = defaultdict(list)

    for e in events:
        key = (
            cfg.force_group or str(e.get("routing_key", "default")).strip() or "default"
        )
        if key in drop:
            continue
        if not cfg.force_group and key in (cfg.merge or {}):
            key = cfg.merge[key] or key
        groups[key].append(e)
    return groups


def _recipients_for_key(settings: Settings, key: str) -> List[str]:
    """Resolve the recipient list for a routing key, honoring fallback_to_default."""
    recips = settings.recipients.get(key, [])  # type: ignore[attr-defined]
    if recips:
        return recips
    if key != "default" and settings.app.routing.fallback_to_default:
        return settings.recipients.get("default", [])  # type: ignore[attr-defined]
    return []


def _dispatch_emails(
    settings: Settings, grouped: Dict[str, List[Event]]
) -> Tuple[int, int, List[Event]]:
    """
    Send one email per group (routing key).
    Returns (groups_sent, events_notified, sent_events_flat_list).
    """
    settings.require_email()

    groups_sent = 0
    events_notified = 0
    sent_events: List[Event] = []

    for key, evs in grouped.items():
        if not evs:
            continue
        recipients = _recipients_for_key(settings, key)
        if not recipients:
            log.warning("No recipients configured for routing key '%s'. Skipping.", key)
            continue

        subject, html_body, text_body = _email.build_message(settings, evs, key)
        _email.send(settings, recipients, subject, html_body, text_body)

        groups_sent += 1
        events_notified += len(evs)
        sent_events.extend(evs)
        log.info(
            "Sent %d event(s) to %d recipient(s) for group '%s'",
            len(evs),
            len(recipients),
            key,
        )

    return groups_sent, events_notified, sent_events


# ------------------------ public entrypoint ------------------------


def run(settings: Settings) -> int:
    """
    Execute one full pipeline run. Returns number of events notified.

    Order:
      1) fetch from enabled providers
      2) filter by rules (global severity, provider thresholds, AOI)
      3) dedup against state (non-mutating)
      4) group by routing key (force/merge/drop)
      5) email each group
      6) persist state with events that were actually emailed
    """
    _setup_logging(settings.app.log_level)

    # 1) fetch
    events = _collect_events(settings)
    if not events:
        log.info("No events fetched.")
        return 0

    # 2) filter
    events = _apply_rules(events, settings.thresholds, settings.app.aoi)
    if not events:
        log.info("All events filtered out by rules.")
        return 0

    # 3) dedup (do not update state yet—only after successful sends)
    state = _State.load(settings.paths.state_file)
    events = _only_new(events, state)
    if not events:
        log.info("No new events after deduplication.")
        return 0

    # 4) route
    grouped = _group_by_routing_key(events, settings)
    if not grouped:
        log.info("No routable groups after routing rules.")
        return 0

    # 5) email
    try:
        groups_sent, events_notified, sent_events = _dispatch_emails(settings, grouped)
    except RuntimeError as e:
        # Likely missing email credentials; surface clearly
        log.error("Notification failed: %s", e)
        raise

    # 6) persist state (only for events we actually attempted to send)
    if sent_events:
        state.update_with(sent_events)
        state.save()

    log.info(
        "Pipeline completed: %d group(s) emailed, %d event(s) notified.",
        groups_sent,
        events_notified,
    )
    return events_notified
