from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from .settings import EarthquakeThresholds, Thresholds, WeatherThresholds

Event = Dict[str, Any]

__all__ = [
    "filter_events",
]

# -----------------------------------------------------------------------------
# Geo helpers
# -----------------------------------------------------------------------------

Coord = Tuple[float, float]  # (lon, lat)


def _is_number(x: Any) -> bool:
    return isinstance(x, (int, float)) and not isinstance(x, bool)


def _as_point_from_geometry(geom: Optional[Dict[str, Any]]) -> Optional[Coord]:
    """
    Extract a representative point (lon, lat) from a GeoJSON geometry.
    - If Point: return coords.
    - If Polygon/MultiPolygon: return the first vertex of the outer ring (fast + deterministic).
    - Otherwise: return None (caller may skip AOI filtering for this event).
    """
    if not geom or not isinstance(geom, dict):
        return None
    gtype = geom.get("type")
    coords = geom.get("coordinates")
    if gtype == "Point" and isinstance(coords, (list, tuple)) and len(coords) >= 2:
        lon, lat = coords[:2]
        if _is_number(lon) and _is_number(lat):
            return float(lon), float(lat)
        return None
    if (
        gtype == "Polygon"
        and isinstance(coords, list)
        and coords
        and isinstance(coords[0], list)
        and coords[0]
    ):
        first = coords[0][0]
        if isinstance(first, (list, tuple)) and len(first) >= 2:
            lon, lat = first[:2]
            if _is_number(lon) and _is_number(lat):
                return float(lon), float(lat)
        return None
    if gtype == "MultiPolygon" and isinstance(coords, list) and coords:
        poly = coords[0]
        if isinstance(poly, list) and poly and isinstance(poly[0], list) and poly[0]:
            first = poly[0][0]
            if isinstance(first, (list, tuple)) and len(first) >= 2:
                lon, lat = first[:2]
                if _is_number(lon) and _is_number(lat):
                    return float(lon), float(lat)
        return None
    return None


def _point_in_ring(pt: Coord, ring: List[List[float]]) -> bool:
    """
    Ray casting point-in-polygon for a single ring (outer or hole).
    ring: list of [lon, lat]
    """
    x, y = pt
    inside = False
    n = len(ring)
    if n < 3:
        return False
    for i in range(n):
        x1, y1 = ring[i][0], ring[i][1]
        x2, y2 = ring[(i + 1) % n][0], ring[(i + 1) % n][1]
        # Check if the ray intersects the edge
        intersects = ((y1 > y) != (y2 > y)) and (
            x < (x2 - x1) * (y - y1) / (y2 - y1 + 1e-15) + x1
        )
        if intersects:
            inside = not inside
    return inside


def _point_in_polygon(pt: Coord, polygon_coords: List[List[List[float]]]) -> bool:
    """
    polygon_coords: [outer_ring, hole1, hole2, ...], each ring is [[lon,lat], ...]
    """
    if not polygon_coords:
        return False
    outer = polygon_coords[0]
    if not _point_in_ring(pt, outer):
        return False
    # If inside outer, ensure it's not inside any hole
    for hole in polygon_coords[1:]:
        if _point_in_ring(pt, hole):
            return False
    return True


def _point_in_multipolygon(
    pt: Coord, multipoly_coords: List[List[List[List[float]]]]
) -> bool:
    """
    multipoly_coords: [ polygon1, polygon2, ... ]
    Each polygon is [outer_ring, hole1, ...]; each ring is [[lon,lat], ...]
    """
    for polygon in multipoly_coords:
        if _point_in_polygon(pt, polygon):
            return True
    return False


def _aoi_contains(aoi: Dict[str, Any], pt: Coord) -> bool:
    """
    True if point is inside AOI (Polygon/MultiPolygon). Returns False on malformed AOI.
    """
    gtype = aoi.get("type")
    coords = aoi.get("coordinates")
    if gtype == "Polygon" and isinstance(coords, list):
        return _point_in_polygon(pt, coords)
    if gtype == "MultiPolygon" and isinstance(coords, list):
        return _point_in_multipolygon(pt, coords)
    return False


# -----------------------------------------------------------------------------
# Severity normalization (for global threshold)
# -----------------------------------------------------------------------------

# We convert provider-specific severities to a comparable rank.
# NWS typically: "Minor" | "Moderate" | "Severe" | "Extreme"
# USGS (derived here from magnitude buckets): "Minor" | "Light" | "Moderate" | "Strong" | "Major" | "Great"
_SEVERITY_RANK = {
    # NWS-ish
    "unknown": 0,
    "minor": 1,
    "moderate": 2,
    "severe": 3,
    "extreme": 4,
    # USGS-ish buckets mapped onto the same scale
    "light": 1,  # close to Minor
    "strong": 3,  # ≈ Severe
    "major": 4,  # ≈ Extreme
    "great": 4,  # ≥ 7.0, treat as Extreme
}


def _severity_rank(name: Optional[str]) -> int:
    if not name or not isinstance(name, str):
        return _SEVERITY_RANK["unknown"]
    return _SEVERITY_RANK.get(name.strip().lower(), _SEVERITY_RANK["unknown"])


def _passes_global_severity(e: Event, thresholds: Thresholds) -> bool:
    """
    Enforce thresholds.global_.min_severity, if configured.
    We compare normalized ranks across providers.
    """
    min_req = thresholds.global_.min_severity
    if not min_req:
        return True
    want = _severity_rank(min_req)
    have = _severity_rank(e.get("severity"))
    return have >= want


# -----------------------------------------------------------------------------
# Provider adapters (extract comparable values from provider-specific events)
# -----------------------------------------------------------------------------


def _as_earthquake_values(e: Event) -> Dict[str, Optional[float]]:
    """
    Extract magnitude & depth_km from a USGS-like earthquake event.
    USGS GeoJSON keys: feature.properties.mag, feature.geometry.coordinates[2] (depth in km)
    We also check common alternates like 'magnitude', 'depth'.
    """
    props = e.get("properties", {}) or {}
    mag = props.get("mag", props.get("magnitude"))
    depth = props.get("depth_km", props.get("depth"))

    # If not found in props, attempt from geometry z (lon, lat, depth_km)
    if depth is None:
        geom = e.get("geometry")
        if isinstance(geom, dict):
            coords = geom.get("coordinates")
            if (
                isinstance(coords, (list, tuple))
                and len(coords) >= 3
                and _is_number(coords[2])
            ):
                depth = float(coords[2])

    return {
        "magnitude": float(mag) if _is_number(mag) else None,
        "depth_km": float(depth) if _is_number(depth) else None,
    }


def _as_weather_values(e: Event) -> Dict[str, Optional[float]]:
    """
    Extract simple weather-related metrics if present.
    NWS alerts often don’t carry numeric gust/rainfall; we only enforce thresholds
    when numeric values exist in properties (so rules remain permissive).
    Common keys (if present): wind_gust_mps, rainfall_mm_hr.
    """
    props = e.get("properties", {}) or {}
    gust = props.get("wind_gust_mps")
    rain = props.get("rainfall_mm_hr")
    return {
        "wind_gust_mps": float(gust) if _is_number(gust) else None,
        "rainfall_mm_hr": float(rain) if _is_number(rain) else None,
    }


# -----------------------------------------------------------------------------
# Threshold checks
# -----------------------------------------------------------------------------


def _passes_earthquake_thresholds(
    e: Event, thr: Optional[EarthquakeThresholds]
) -> bool:
    if thr is None:
        return True
    vals = _as_earthquake_values(e)
    mag = vals["magnitude"]
    depth = vals["depth_km"]

    if thr.min_magnitude is not None and mag is not None and mag < thr.min_magnitude:
        return False
    # If magnitude missing and a min_magnitude threshold exists, keep (permissive).
    if thr.max_depth_km is not None and depth is not None and depth > thr.max_depth_km:
        return False
    return True


def _weather_event_text(e: Event) -> str:
    # Prefer NWS properties.event, then title/headline
    props = e.get("properties") or {}
    ev = props.get("event") or e.get("title") or ""
    return str(ev).strip()


def _matches_any(patterns: List[str], text: str) -> bool:
    t = text.lower()
    return any((pat or "").lower() in t for pat in patterns)


def _passes_weather_thresholds(e: Event, thr: Optional[WeatherThresholds]) -> bool:
    if thr is None:
        return True

    # 1) categorical filters
    evt = _weather_event_text(e)
    if thr.include_events:
        if not _matches_any(thr.include_events, evt):
            return False
    if thr.exclude_events:
        if _matches_any(thr.exclude_events, evt):
            return False

    # 2) numeric filters (as before)
    vals = _as_weather_values(e)
    gust = vals["wind_gust_mps"]
    rain = vals["rainfall_mm_hr"]

    if thr.wind_gust_mps is not None and gust is not None and gust < thr.wind_gust_mps:
        return False
    if (
        thr.rainfall_mm_hr is not None
        and rain is not None
        and rain < thr.rainfall_mm_hr
    ):
        return False
    return True


def _passes_provider_thresholds(e: Event, thresholds: Thresholds) -> bool:
    prov = str(e.get("provider", "")).lower()
    if prov == "usgs":
        return _passes_earthquake_thresholds(e, thresholds.earthquake)
    if prov == "nws":
        return _passes_weather_thresholds(e, thresholds.weather)
    # Unknown providers: keep permissive unless a global severity rule excludes them
    return True


# -----------------------------------------------------------------------------
# AOI filtering
# -----------------------------------------------------------------------------


def _in_aoi(e: Event, aoi: Optional[Dict[str, Any]]) -> bool:
    if not aoi:
        return True  # no AOI constraint
    pt = _as_point_from_geometry(e.get("geometry"))
    if pt is None:
        return True  # no geometry → do not exclude
    return _aoi_contains(aoi, pt)


# -----------------------------------------------------------------------------
# Public API
# -----------------------------------------------------------------------------


def filter_events(
    events: List[Event], thresholds: Thresholds, aoi: Optional[Dict[str, Any]]
) -> List[Event]:
    """
    Apply filters in order:
      1) global severity (provider-agnostic)
      2) provider-specific thresholds (earthquake, weather, …)
      3) AOI inclusion (if configured)
    """
    out: List[Event] = []
    for e in events:
        if not _passes_global_severity(e, thresholds):
            continue
        if not _passes_provider_thresholds(e, thresholds):
            continue
        if not _in_aoi(e, aoi):
            continue
        out.append(e)
    return out
