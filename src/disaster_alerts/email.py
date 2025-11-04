# src/disaster_alerts/email.py
"""
Email notifications via yagmail.

- One message per routing group with concise subject lines.
- Renders both plaintext and HTML (from templates) with a tiny, safe templater.
- Includes WKT derived from event detail JSON (NWS/USGS) when available.
- Handles USGS eventpage URLs by converting to their *.geojson detail endpoint.
"""

from __future__ import annotations

import html
import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple
from zoneinfo import ZoneInfo

import yagmail

from .providers.common import get_json
from .settings import Settings

Event = Dict[str, Any]
log = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# tiny template loader
# -----------------------------------------------------------------------------


def _templates_dir() -> Path:
    return Path(__file__).resolve().parent / "templates"


def _read_template(name: str) -> str:
    path = _templates_dir() / name
    if not path.exists():
        if name.endswith(".html"):
            return (
                "<html><body><h3>{{ subject }}</h3><pre>{{ body }}</pre></body></html>"
            )
        return "{{ subject }}\n\n{{ body }}\n"
    return path.read_text(encoding="utf-8")


def _render(template: str, context: Dict[str, str]) -> str:
    out = template
    for key, val in context.items():
        out = out.replace(f"{{{{ {key} }}}}", val)
        out = out.replace(f"{{{{{key}}}}}", val)  # tolerate missing space
    return out


# -----------------------------------------------------------------------------
# WKT helpers (with per-run JSON cache)
# -----------------------------------------------------------------------------

_JSON_CACHE: Dict[str, Dict[str, Any]] = {}

_USGS_EVENTPAGE_RE = re.compile(
    r"^https?://earthquake\.usgs\.gov/earthquakes/eventpage/([^/?#]+)"
)


def _normalize_detail_url(url: str) -> str:
    """
    Convert human-facing pages to machine JSON endpoints when known.
    - USGS: /eventpage/<id>  -> /feed/v1.0/detail/<id>.geojson
    """
    m = _USGS_EVENTPAGE_RE.match(url)
    if m:
        eid = m.group(1)
        return f"https://earthquake.usgs.gov/earthquakes/feed/v1.0/detail/{eid}.geojson"
    return url


def _fetch_detail_json(url: str) -> Dict[str, Any]:
    if url in _JSON_CACHE:
        return _JSON_CACHE[url]
    data = get_json(url)
    _JSON_CACHE[url] = data or {}
    return _JSON_CACHE[url]


def _to_wkt(geom: dict | None) -> str | None:
    """Convert minimal GeoJSON (Point/Polygon/MultiPolygon) to WKT."""
    if not isinstance(geom, dict):
        return None
    gtype = geom.get("type")
    coords = geom.get("coordinates")
    try:
        if gtype == "Point" and isinstance(coords, (list, tuple)) and len(coords) >= 2:
            x, y = float(coords[0]), float(coords[1])
            return f"POINT ({x} {y})"
        if gtype == "Polygon" and isinstance(coords, list) and coords:
            ring = coords[0]
            pairs = ", ".join(f"{float(x)} {float(y)}" for x, y, *_ in ring)
            return f"POLYGON (({pairs}))"
        if gtype == "MultiPolygon" and isinstance(coords, list) and coords:
            # keep compact: first polygon
            poly = coords[0]
            ring = poly[0]
            pairs = ", ".join(f"{float(x)} {float(y)}" for x, y, *_ in ring)
            return f"MULTIPOLYGON ((({pairs})))"
    except Exception:
        return None
    return None


def _wkt_for_event(event: dict) -> str | None:
    """
    Prefer WKT from the event's detail JSON (link/id), fall back to in-memory geometry.
    """
    link = event.get("link") or event.get("id")
    if isinstance(link, str) and link.startswith("http"):
        detail = _normalize_detail_url(link)
        try:
            data = _fetch_detail_json(detail)
        except Exception as exc:
            log.debug("Detail fetch failed for %s: %s", detail, exc)
            data = {}

        # Plain or Feature
        geom = data.get("geometry")
        if isinstance(geom, dict):
            w = _to_wkt(geom)
            if w:
                return w

        # FeatureCollection → first feature geometry
        if data.get("type") == "FeatureCollection":
            feats = data.get("features") or []
            if feats and isinstance(feats[0], dict):
                w = _to_wkt(feats[0].get("geometry"))
                if w:
                    return w

    return _to_wkt(event.get("geometry"))


# -----------------------------------------------------------------------------
# formatting helpers (NWS + USGS aware)
# -----------------------------------------------------------------------------

_SEV_RANK = {"extreme": 4, "severe": 3, "moderate": 2, "minor": 1, "none": 0}


def _sev_rank(severity: str | None) -> int:
    return _SEV_RANK.get((severity or "").strip().lower(), 0)


def _tz(settings: Settings) -> ZoneInfo:
    tzname = getattr(settings.app, "display_timezone", None) or "UTC"
    try:
        return ZoneInfo(tzname)
    except Exception:
        return ZoneInfo("UTC")


def _pick_time(props: dict, *keys: str) -> str | None:
    for key in keys:
        val = props.get(key)
        if isinstance(val, str) and val:
            return val
    return None


def _to_dt(timestr: str | None) -> datetime | None:
    if not timestr:
        return None
    try:
        return datetime.fromisoformat(timestr.replace("Z", "+00:00"))
    except Exception:
        return None


def _to_dt_any(val: Any) -> datetime | None:
    """Accept ISO string or epoch-ms (int/float) → aware datetime (UTC)."""
    if val is None:
        return None
    if isinstance(val, (int, float)):
        try:
            return datetime.fromtimestamp(float(val) / 1000.0, tz=timezone.utc)
        except Exception:
            return None
    if isinstance(val, str):
        try:
            return datetime.fromisoformat(val.replace("Z", "+00:00"))
        except Exception:
            return None
    return None


def _fmt_local(dt: datetime | None, tz: ZoneInfo) -> str:
    if not dt:
        return "—"
    return dt.astimezone(tz).strftime("%b %-d, %H:%M")


def _time_left(expires: datetime | None, now: datetime) -> str:
    if not expires:
        return ""
    delta = expires - now
    secs = int(delta.total_seconds())
    if secs <= 0:
        return "(ended)"
    hours_total = secs // 3600
    days, hours = divmod(hours_total, 24)
    minutes = (secs % 3600) // 60
    if days:
        return f"({days}d {hours}h left)"
    if hours:
        return f"({hours}h {minutes}m left)"
    return f"({minutes}m left)"


def _is_usgs(ev: Event) -> bool:
    return str(ev.get("provider", "")).lower() == "usgs"


def _usgs_depth_km(ev: Event) -> float | None:
    props = ev.get("properties") or {}
    if isinstance(props.get("depth_km"), (int, float)):
        return float(props["depth_km"])
    geom = ev.get("geometry")
    if isinstance(geom, dict):
        coords = geom.get("coordinates")
        if (
            isinstance(coords, (list, tuple))
            and len(coords) >= 3
            and isinstance(coords[2], (int, float))
        ):
            return float(coords[2])
    return None


def _usgs_mag(ev: Event) -> float | None:
    props = ev.get("properties") or {}
    m = props.get("mag") or props.get("magnitude")
    try:
        return float(m) if m is not None else None
    except Exception:
        return None


# -----------------------------------------------------------------------------
# plaintext builder
# -----------------------------------------------------------------------------


def _format_text_lines(events: Iterable[Event], settings: Settings) -> List[str]:
    """Render plaintext rows (NWS/USGS-aware, deduped + sorted)."""

    def key_tuple(event: Event) -> Tuple:
        props = event.get("properties") or {}
        if _is_usgs(event):
            origin = _to_dt_any(props.get("time"))
            updated = _to_dt_any(props.get("updated"))
            return (event.get("title") or "", origin or "", updated or "")
        else:
            onset = _to_dt(_pick_time(props, "onset", "effective", "sent"))
            expires = _to_dt(_pick_time(props, "expires", "ends"))
            office = props.get("senderName") or ""
            return (
                event.get("title") or props.get("event") or "",
                office,
                onset or "",
                expires or "",
            )

    # dedupe
    uniq: Dict[Tuple, Event] = {}
    for ev in events:
        uniq[key_tuple(ev)] = ev
    events_dedup = list(uniq.values())

    # sort: severity desc; USGS by origin asc, NWS by expires asc
    events_dedup.sort(
        key=lambda ev: (
            -_sev_rank(str(ev.get("severity"))),
            (
                _to_dt_any((ev.get("properties") or {}).get("time"))
                if _is_usgs(ev)
                else _to_dt(_pick_time((ev.get("properties") or {}), "expires", "ends"))
            )
            or datetime.max.replace(tzinfo=timezone.utc),
        )
    )

    tz = _tz(settings)
    now_local = datetime.now(timezone.utc).astimezone(tz)

    out: List[str] = []
    for idx, ev in enumerate(events_dedup, 1):
        props = ev.get("properties") or {}
        link = str(ev.get("link") or ev.get("id") or "").strip()
        title = str(props.get("event") or ev.get("title") or "(untitled)")

        if _is_usgs(ev):
            origin = _to_dt_any(props.get("time"))
            updated = _to_dt_any(props.get("updated"))
            mag = _usgs_mag(ev)
            depth_km = _usgs_depth_km(ev)
            alert = (props.get("alert") or "—").title()
            tsunami = "Yes" if props.get("tsunami") in (1, "1", True) else "No"

            head = f"{idx}) {title}"
            when = (
                f"Origin: {_fmt_local(origin, tz)}   Updated: {_fmt_local(updated, tz)}"
            )
            magdepth = f"Magnitude: {mag:.1f}" if mag is not None else "Magnitude: —"
            if depth_km is not None:
                magdepth += f" • Depth: {depth_km:.1f} km"
            sevline = f"Alert: {alert} • Tsunami: {tsunami}"
            url = f"URL: {link}"

            row = [head, f"   {when}", f"   {magdepth}", f"   {sevline}", f"   {url}"]
        else:
            office = str(props.get("senderName") or "").strip()
            area = str(props.get("areaDesc") or "").strip()
            onset = _to_dt(_pick_time(props, "onset", "effective", "sent"))
            expires = _to_dt(_pick_time(props, "expires", "ends"))
            sev = (str(ev.get("severity") or "").strip()) or "—"
            cert = (str(props.get("certainty") or "").strip()) or "—"
            urg = (str(props.get("urgency") or "").strip()) or "—"

            head = f"{idx}) {title}"
            meta = " — ".join(part for part in (office or None, area or None) if part)
            when = f"When: {_fmt_local(onset, tz)} → {_fmt_local(expires, tz)}  {_time_left(expires, now_local)}"
            sevline = f"Severity: {sev} • Certainty: {cert} • Urgency: {urg}"
            url = f"URL: {link}"

            row = [head]
            if meta:
                row.append(f"   {meta}")
            row.extend([f"   {when}", f"   {sevline}", f"   {url}"])

        wkt = _wkt_for_event(ev)
        if wkt:
            trimmed = (wkt[:600] + "…") if len(wkt) > 600 else wkt
            row.append(f"   WKT: {trimmed}")

        out.append("\n".join(row))
        out.append("")  # spacer line between events

    return out


# -----------------------------------------------------------------------------
# HTML builder
# -----------------------------------------------------------------------------


def _format_html_rows(events: Iterable[Event]) -> str:
    """HTML table rows; add a detail row for USGS (origin/mag/depth/alert/tsunami) and WKT row."""
    rows: List[str] = []
    for ev in events:
        title = html.escape(str(ev.get("title") or "").strip() or "(untitled)")
        provider = html.escape(str(ev.get("provider") or "unknown").upper())
        severity = html.escape(str(ev.get("severity") or "").strip())
        updated = html.escape(str(ev.get("updated") or "").strip())
        link = str(ev.get("link") or "").strip()
        eid = html.escape(str(ev.get("id") or "").strip())
        link_html = f'<a href="{html.escape(link)}">link</a>' if link else ""
        sev_cell = severity or "&nbsp;"
        upd_cell = updated or "&nbsp;"

        rows.append(
            "<tr>"
            f"<td>{provider}</td>"
            f"<td>{title}</td>"
            f"<td>{sev_cell}</td>"
            f"<td>{upd_cell}</td>"
            f"<td>{link_html}</td>"
            f"<td><code>{eid}</code></td>"
            "</tr>"
        )

        if _is_usgs(ev):
            props = ev.get("properties") or {}
            origin = _to_dt_any(props.get("time"))
            mag = _usgs_mag(ev)
            depth_km = _usgs_depth_km(ev)
            alert = (props.get("alert") or "—").title()
            tsunami = "Yes" if props.get("tsunami") in (1, "1", True) else "No"

            detail_parts: List[str] = []
            detail_parts.append(
                f"Origin: {html.escape(origin.astimezone(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')) if origin else '—'}"
            )
            detail_parts.append(
                f"Magnitude: {mag:.1f}" if mag is not None else "Magnitude: —"
            )
            detail_parts.append(
                f"Depth: {depth_km:.1f} km" if depth_km is not None else "Depth: —"
            )
            detail_parts.append(f"Alert: {html.escape(alert)}")
            detail_parts.append(f"Tsunami: {tsunami}")

            rows.append(
                "<tr><td colspan='6' style='font-family:system-ui,Segoe UI,Arial;font-size:12px;'>"
                + " • ".join(detail_parts)
                + "</td></tr>"
            )

        wkt = _wkt_for_event(ev)
        if wkt:
            trimmed = (wkt[:600] + "…") if len(wkt) > 600 else wkt
            rows.append(
                "<tr>"
                "<td colspan='6' style='font-family:monospace;font-size:12px;white-space:nowrap;overflow:auto;'>"
                f"<strong>WKT:</strong> {html.escape(trimmed)}"
                "</td>"
                "</tr>"
            )
    return "\n".join(rows)


# -----------------------------------------------------------------------------
# message assembly
# -----------------------------------------------------------------------------


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _subject_for_group(group_key: str, events: List[Event]) -> str:
    """Compose concise subject summarizing alert types and counts."""
    from collections import Counter

    titles: List[str] = []
    for ev in events:
        props = ev.get("properties") or {}
        t = str(props.get("event") or ev.get("title") or "")
        titles.append(t.split(" issued", 1)[0])
    counts = Counter(titles)
    types = ", ".join(f"{k} ×{v}" for k, v in counts.most_common())
    total = len(events)
    plural = "" if total == 1 else "s"
    return f"[disaster-alerts] {total} new event{plural} — {types}  ({group_key})"


def _build_bodies(
    subject: str, events: List[Event], settings: Settings
) -> Tuple[str, str]:
    """Return (html_body, text_body) before templating."""
    # plaintext
    lines = _format_text_lines(events, settings)
    text_body = "\n".join(lines)

    # html table
    rows_html = _format_html_rows(events)
    table = (
        "<table border='1' cellpadding='6' cellspacing='0' style='border-collapse:collapse;'>"
        "<thead><tr>"
        "<th>Provider</th><th>Title</th><th>Severity</th><th>Updated</th><th>Link</th><th>ID</th>"
        "</tr></thead>"
        f"<tbody>{rows_html}</tbody>"
        "</table>"
    )
    html_body = table
    return html_body, text_body


# -----------------------------------------------------------------------------
# public API
# -----------------------------------------------------------------------------


def build_message(
    settings: Settings, events: List[Event], group_key: str
) -> Tuple[str, str, str]:
    """
    Compose subject + HTML + plaintext for a group of events.

    Templates `alert.html` and `alert.txt` support tokens:
      {{ subject }}  {{ body }}  {{ generated_at }}  {{ group }}
    """
    subject = _subject_for_group(group_key, events)
    html_table, text_lines = _build_bodies(subject, events, settings)

    html_tpl = _read_template("alert.html")
    txt_tpl = _read_template("alert.txt")

    context_html = {
        "subject": subject,
        "body": html_table,  # HTML table goes here
        "generated_at": _now_utc_iso(),
        "group": group_key,
    }
    html_body = _render(html_tpl, context_html)

    context_txt = {
        "subject": subject,
        "body": text_lines,  # Plain text lines joined above
        "generated_at": _now_utc_iso(),
        "group": group_key,
    }
    text_body = _render(txt_tpl, context_txt)

    return subject, html_body, text_body


def send(
    settings: Settings,
    recipients: List[str],
    subject: str,
    html_body: str,
    text_body: str,
) -> None:
    """Send the message with yagmail. Raises on failure."""
    settings.require_email()
    user = settings.email.user
    app_password = settings.email.app_password

    log.debug("Connecting to SMTP as %s to send to %s", user, ", ".join(recipients))
    with yagmail.SMTP(user, app_password) as yag:
        yag.send(to=recipients, subject=subject, contents=[text_body, html_body])

    log.info(
        "Email sent to %d recipient(s): %s", len(recipients), ", ".join(recipients)
    )
