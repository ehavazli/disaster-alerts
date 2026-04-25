"""
Plot interactive HTML map for the activated events.

- add detailed description later .
"""

from __future__ import annotations

import colorsys
import hashlib
import ipaddress
import json
import logging
import socket
from pathlib import Path
from typing import Any, Dict, Iterable, List
from urllib.parse import urlparse

import requests

from .settings import Settings

# -----------------------------------------------------------------------------
# generate and save an interactive HTML map
# -----------------------------------------------------------------------------


Event = Dict[str, Any]
log = logging.getLogger(__name__)

FAMILY_HUES = {
    "flood": 210 / 360,  # blue
    "hurricane": 120 / 360,  # green
    "storm": 10 / 360,  # red/orange
    "thunderstorm": 270 / 360,  # purple
}

TRUSTED_URL_SUFFIXES = (
    "weather.gov",
    "noaa.gov",
    "usgs.gov",
)
MAX_GEOJSON_BYTES = 2 * 1024 * 1024  # 2 MiB


def _is_url(s: str) -> bool:
    parsed = urlparse(s)
    return parsed.scheme in ("http", "https")


def _host_is_trusted(hostname: str) -> bool:
    host = hostname.lower().strip(".")
    return any(
        host == suffix or host.endswith(f".{suffix}") for suffix in TRUSTED_URL_SUFFIXES
    )


def _host_resolves_public(hostname: str) -> bool:
    try:
        infos = socket.getaddrinfo(hostname, None)
    except socket.gaierror:
        return False

    for _, _, _, _, sockaddr in infos:
        ip = ipaddress.ip_address(sockaddr[0])
        if (
            ip.is_private
            or ip.is_loopback
            or ip.is_link_local
            or ip.is_multicast
            or ip.is_reserved
            or ip.is_unspecified
        ):
            return False
    return True


def _validate_remote_url(url: str) -> None:
    parsed = urlparse(url)
    if parsed.scheme != "https":
        raise ValueError("Only https URLs are allowed for AOI downloads")
    if not parsed.hostname:
        raise ValueError("URL must include a hostname")
    if not _host_is_trusted(parsed.hostname):
        raise ValueError(f"Untrusted host for AOI download: {parsed.hostname}")
    if not _host_resolves_public(parsed.hostname):
        raise ValueError(
            f"Host resolves to a non-public address: {
            parsed.hostname}"
        )


def _detect_family(event_type: str) -> str:
    s = event_type.lower()
    if "flood" in s:
        return "flood"
    if "hurricane" in s:
        return "hurricane"
    if "thunderstorm" in s:
        return "thunderstorm"
    # keep this order otherwise thunderstorm events
    # will be categorized as storm
    if "storm" in s:
        return "storm"
    return "storm"


def _color_from_event_type(event_type: str) -> str:
    family = _detect_family(event_type)
    base_hue = FAMILY_HUES[family]

    hash_hex = hashlib.md5(event_type.encode()).hexdigest()

    # Use more hash bits for stronger variation
    h_variation = int(hash_hex[:2], 16) / 255.0  # 0–1
    l_variation = int(hash_hex[2:4], 16) / 255.0  # 0–1

    # --- HUE variation (±15 degrees) ---
    hue_offset = (h_variation - 0.5) * (30 / 360)  # ±15°
    hue = (base_hue + hue_offset) % 1.0

    # --- LIGHTNESS variation (wide range) ---
    lightness = 0.35 + 0.35 * l_variation  # 0.35–0.70

    saturation = 0.75

    r, g, b = colorsys.hls_to_rgb(hue, lightness, saturation)

    return "#{:02x}{:02x}{:02x}".format(
        int(r * 255),
        int(g * 255),
        int(b * 255),
    )


def _generate_events_html_map(
    settings: "Settings",
    events: dict[str, list["Event"]],
    file_dir: "Path",
):
    """
    Create an interactive map displaying activated events,
    grouped by routing key, and enabling the user to draw a bounding box.
    """

    import folium
    from branca.element import MacroElement
    from folium.features import GeoJson
    from folium.plugins import Draw
    from jinja2 import Template
    from shapely.geometry import MultiPolygon

    class MapDashboardJS(MacroElement):
        def __init__(self):
            super().__init__()
            self._template = Template(
                """
                {% macro html(this, kwargs) %}
                <style>
                    #control-panel {
                        position: absolute;
                        top: 20px; left: 200px;
                        z-index: 1000;
                        background: rgba(255, 255, 255, 0.95);
                        padding: 8px 15px;
                        border-radius: 8px;
                        box-shadow: 0 2px 10px rgba(0,0,0,0.2);
                        display: flex;
                        gap: 10px;
                        align-items: center;
                        border: 2px solid #374151;
                        font-family: Arial, sans-serif;
                    }

                    .input-group {
                        display: flex;
                        align-items: center;
                    }

                    .field-label {
                        display: none;
                    }

                    #control-panel select, #control-panel input {
                        border: 1px solid #9ca3af;
                        border-radius: 4px;
                        padding: 4px 8px;
                        font-size: 14px;
                        background-color: #ffffff;
                        color: #111827;
                        height: 34px;
                    }

                    .multi-dropdown {
                        position: relative;
                        display: inline-block;
                    }

                    .multi-dropdown-btn {
                        position: relative;
                        border: 1px solid #9ca3af;
                        border-radius: 4px;
                        padding: 4px 28px 4px 8px;
                        font-size: 14px;
                        background-color: #ffffff;
                        color: #111827;
                        height: 34px;
                        cursor: pointer;
                        white-space: nowrap;
                        overflow: hidden;
                        text-overflow: ellipsis;
                        width: 140px;
                        text-align: left;
                        appearance: none;
                        -webkit-appearance: none;
                        padding-right: 24px;
                        font-family: Arial, sans-serif;
                    }

                    .multi-dropdown-btn:hover {
                        border-color: #6b7280;
                    }

                    .multi-dropdown-btn::after {
                        content: " ▾";
                        position: absolute;
                        right: 8px;
                    }

                    .multi-dropdown-list {
                        display: none;
                        position: absolute;
                        top: 36px;
                        left: 0;
                        background: #ffffff;
                        border: 1px solid #9ca3af;
                        border-radius: 4px;
                        box-shadow: 0 4px 12px rgba(0,0,0,0.15);
                        z-index: 9999;
                        min-width: 100%;
                        padding: 4px 0;
                    }

                    .multi-dropdown-list.open {
                        display: block;
                    }

                    .multi-dropdown-list label {
                        display: flex;
                        align-items: center;
                        gap: 8px;
                        padding: 5px 12px;
                        font-size: 14px;
                        color: #111827;
                        cursor: pointer;
                        white-space: nowrap;
                    }

                    .multi-dropdown-list label:hover {
                        background-color: #f3f4f6;
                    }

                    #search-btn {
                        background-color: #6b7280;
                        color: white;
                        border: none;
                        padding: 0 20px;
                        border-radius: 4px;
                        font-weight: bold;
                        font-size: 15px;
                        cursor: pointer;
                        height: 34px;
                    }

                    #search-btn:hover {
                        background-color: #4b5563;
                    }

                    .leaflet-control-layers {
                        border-radius: 8px !important;
                        border: 2px solid #374151 !important;
                        box-shadow: 0 2px 10px rgba(0,0,0,0.2) !important;
                        background: rgba(255,255,255,0.95) !important;
                        font-family: Arial, sans-serif !important;
                        min-width: 200px !important;
                        width: auto !important;
                    }

                    .leaflet-control-layers label {
                        display: flex;
                        align-items: center;
                        white-space: nowrap;
                        margin-bottom: 6px;
                        font-size: 14px;
                    }

                    .leaflet-control-layers-expanded {
                        padding: 10px !important;
                    }

                    .leaflet-control-layers-base,
                    .leaflet-control-layers-overlays {
                        margin-top: 6px;
                    }

                    /* Move legend slightly down */
                    .leaflet-top.leaflet-right {
                        top: 80px !important;
                    }

                </style>

                <div id="control-panel">
                    <div class="input-group">
                        <select id="search_type">
                            <option value="both">Functionality: Both</option>
                            <option value="overpasses">Functionality: Overpasses</option>
                            <option value="opera_search">
                                Functionality: Opera Search</option>
                        </select>
                    </div>

                    <div class="input-group">
                        <div class="multi-dropdown" id="sat-dropdown">
                            <button type="button"
                                class="multi-dropdown-btn"
                                id="sat-btn">All Satellites</button>
                            <div class="multi-dropdown-list" id="sat-list">
                                <label><input type="checkbox" value="all"
                                    checked> All Satellites</label>
                                <label><input type="checkbox"
                                    value="sentinel-1"> Sentinel-1</label>
                                <label><input type="checkbox"
                                    value="sentinel-2"> Sentinel-2</label>
                                <label><input type="checkbox"
                                    value="landsat"> Landsat</label>
                                <label><input type="checkbox"
                                    value="nisar"> NISAR</label>
                            </div>
                        </div>
                    </div>

                    <div class="input-group">
                        <div class="multi-dropdown" id="prod-dropdown">
                            <button type="button"
                                class="multi-dropdown-btn"
                                id="prod-btn">All Products</button>
                            <div class="multi-dropdown-list" id="prod-list">
                                <label><input type="checkbox" value="all"
                                    checked> All Products</label>
                                <label><input type="checkbox"
                                    value="DSWX-HLS_V1"> DSWX-HLS_V1</label>
                                <label><input type="checkbox"
                                    value="DSWX-S1_V1"> DSWX-S1_V1</label>
                                <label><input type="checkbox"
                                    value="DIST-ALERT-HLS_V1"> DIST-ALERT-HLS_V1</label>
                                <label><input type="checkbox"
                                    value="DIST-ANN-HLS_V1"> DIST-ANN-HLS_V1</label>
                                <label><input type="checkbox"
                                    value="RTC-S1_V1"> RTC-S1_V1</label>
                                <label><input type="checkbox"
                                    value="CSLC-S1_V1"> CSLC-S1_V1</label>
                                <label><input type="checkbox"
                                    value="DISP-S1_V1"> DISP-S1_V1</label>
                            </div>
                        </div>
                    </div>

                    <div class="input-group">
                        <input type="number" id="lookback"
                            placeholder="Number of lookback days"
                            min="1" max="11" style="width: 190px;">
                    </div>

                    <div class="input-group">
                        <select id="drcs_enabled" onchange="toggleDate(this.value)">
                            <option value="no">DRCS: No</option>
                            <option value="yes">DRCS: Yes</option>
                        </select>
                    </div>

                    <div class="input-group">
                        <input type="text" id="event_date"
                            placeholder="YYYY-MM-DDTHH:MM"
                            value="YYYY-MM-DDTHH:MM" disabled style="width: 180px;">
                    </div>

                    <button id="search-btn">Search</button>
                </div>
                {% endmacro %}

                {% macro script(this, kwargs) %}
                var currentBbox = null;
                var currentBboxLayer = null;
                var justDrawn = false;

                // ---- Custom multi-select dropdown logic ----
                function setupMultiDropdown(btnId, listId, allValue, defaultLabel) {
                    var btn = document.getElementById(btnId);
                    var list = document.getElementById(listId);
                    var checkboxes = list.querySelectorAll('input[type=checkbox]');
                    var allBox = list.querySelector('input[value="' + allValue + '"]');

                    btn.addEventListener('click', function(e) {
                        e.stopPropagation();
                        var open = '.multi-dropdown-list.open';
                        document.querySelectorAll(open).forEach(function(el) {
                            if (el !== list) el.classList.remove('open');
                        });
                        list.classList.toggle('open');
                    });

                    list.addEventListener('click', function(e) {
                        e.stopPropagation();
                    });

                    checkboxes.forEach(function(cb) {
                        cb.addEventListener('change', function() {
                            if (cb === allBox && cb.checked) {
                                checkboxes.forEach(function(c) { c.checked = false; });
                                allBox.checked = true;
                            } else if (cb !== allBox && cb.checked) {
                                allBox.checked = false;
                            }
                            updateLabel();
                        });
                    });

                    function updateLabel() {
                        if (allBox.checked) {
                            btn.textContent = defaultLabel;
                            return;
                        }
                        var selected = Array.from(checkboxes)
                            .filter(function(c) {
                                return c.checked && c !== allBox;
                            })
                            .map(function(c) { return c.value; });
                        btn.textContent = selected.length
                            ? selected.join(', ') : defaultLabel;
                    }
                }

                function getMultiValues(listId, allValue) {
                    var list = document.getElementById(listId);
                    var allBox = list.querySelector('input[value="' + allValue + '"]');
                    if (allBox.checked) return [allValue];
                    return Array.from(list.querySelectorAll('input[type=checkbox]'))
                        .filter(function(c) { return c.checked; })
                        .map(function(c) { return c.value; });
                }

                document.addEventListener('click', function() {
                    var open = '.multi-dropdown-list.open';
                    document.querySelectorAll(open).forEach(function(el) {
                        el.classList.remove('open');
                    });
                });

                setupMultiDropdown('sat-btn', 'sat-list', 'all', 'All Satellites');
                setupMultiDropdown('prod-btn', 'prod-list', 'all', 'All Products');

                function toggleDate(val) {
                    const input = document.getElementById('event_date');
                    if(val === 'yes') {
                        input.disabled = false;
                        input.value = '';
                        input.style.color = '#000000';
                    } else {
                        input.disabled = true;
                        input.value = 'YYYY-MM-DDTHH:MM';
                        input.style.color = '#6b7280';
                    }
                }

                {{this._parent.get_name()}}.on('draw:created', function(e) {
                    if (currentBboxLayer) {
                        {{this._parent.get_name()}}.removeLayer(currentBboxLayer);
                    }
                    currentBboxLayer = e.layer;
                    currentBboxLayer.addTo({{this._parent.get_name()}});
                    var bounds = currentBboxLayer.getBounds();
                    currentBbox = {
                        lat_min: bounds.getSouth(),
                        lat_max: bounds.getNorth(),
                        lon_min: bounds.getWest(),
                        lon_max: bounds.getEast()
                    };
                    justDrawn = true;
                    console.log("Bounding box ready.");
                });

                {{this._parent.get_name()}}.on('click', function() {
                    if (justDrawn) {
                        justDrawn = false;
                        return;
                    }
                    if (currentBboxLayer) {
                        {{this._parent.get_name()}}.removeLayer(currentBboxLayer);
                        currentBboxLayer = null;
                        currentBbox = null;
                    }
                });

                document.getElementById('search-btn').onclick = function() {
                    if (!currentBbox) {
                        alert("Please draw a bounding box on the map first!");
                        return;
                    }

                    const payload = {
                        ...currentBbox,
                        search_type: document.getElementById('search_type').value,
                        satellites: getMultiValues('sat-list', 'all'),
                        products: getMultiValues('prod-list', 'all'),
                        lookback: document.getElementById('lookback').value,
                        drcs: document.getElementById('drcs_enabled').value
                            .replace('DRCS: ', '').toLowerCase(),
                        event_date: document.getElementById('event_date').value
                    };

                    fetch("/process_bbox", {
                        method: "POST",
                        headers: {"Content-Type": "application/json"},
                        body: JSON.stringify(payload)
                    })
                    .then(resp => resp.json())
                    .then(data => {
                        alert("Search command sent. Processing...");
                        checkStatus();
                    });
                };

                function checkStatus() {
                    fetch("/processing_status")
                        .then(r => r.json())
                        .then(status => {
                            if (status.running) {
                                setTimeout(checkStatus, 2000);
                            } else if (status.error) {
                                alert("Processing failed: " + status.error);
                            } else {
                                window.location.href = "/show_maps";
                            }
                        });
                }
                {% endmacro %}
            """
            )

    output_file = file_dir / "activated_events_map.html"

    US_CENTER = [39.8283, -98.5795]
    map_object = folium.Map(location=US_CENTER, zoom_start=5, tiles=None)

    # Add base layers
    folium.TileLayer("Esri.WorldImagery", name="Satellite").add_to(map_object)
    folium.TileLayer("OpenStreetMap", name="OSM").add_to(map_object)

    # Add grouped event layers
    for event_type, group_events in events.items():
        color = _color_from_event_type(event_type)
        color_box = (
            "<span style='display:inline-block; width:12px; height:12px; "
            f"background:{color}; margin-right:6px; border:1px solid #333;'></span>"
        )
        legend_label = f"{color_box}{event_type} ({len(group_events)})"
        feature_group = folium.FeatureGroup(name=legend_label, show=True)

        for e in group_events:
            geom = e.get("aoi_polygon")
            if geom is None:
                log.debug("Event %s has no AOI geometry", e.get("id"))
                continue
            provider = str(e.get("provider", "")).upper()
            popup_html = "<br>".join(
                f"<b>{label}:</b> {value}"
                for label, value in [
                    ("Provider", provider),
                    ("Severity", e.get("severity")),
                    ("Description", e.get("title")),
                ]
                if value
            )
            if isinstance(geom, MultiPolygon):
                geometries = geom.geoms
            else:
                geometries = [geom]
            for g in geometries:
                GeoJson(
                    data=g.__geo_interface__,
                    style_function=lambda _, c=color: {
                        "color": c,
                        "weight": 2,
                        "fillColor": c,
                        "fillOpacity": 0.35,
                    },
                    highlight_function=lambda _: {"weight": 3, "fillOpacity": 0.6},
                    popup=folium.Popup(popup_html, max_width=350),
                ).add_to(feature_group)
        feature_group.add_to(map_object)

    # Add Layer controls
    folium.LayerControl(collapsed=False).add_to(map_object)

    draw = Draw(
        draw_options={
            "rectangle": True,
            "polygon": False,
            "circle": False,
            "marker": False,
            "polyline": False,
        },
        edit_options={"edit": True},
    )
    draw.add_to(map_object)

    # After adding draw controls, add the new dashboard
    map_object.add_child(MapDashboardJS())

    # Save HTML
    map_object.save(output_file)
    log.info("Event map written to %s", output_file)


def _bbox_to_geometry(bbox, timestamp_dir):
    from shapely import Point, wkt
    from shapely.geometry import box

    if isinstance(bbox, str):
        bbox_clean = bbox.strip()
        bbox_upper = bbox_clean.upper()
        if bbox_upper.startswith(("POINT", "POLYGON")):
            geometry = wkt.loads(bbox_clean)
        else:
            # if URL, download
            if _is_url(bbox_clean):
                filename = "AOI_from_url.geojson"
                file_path = Path(timestamp_dir) / filename
                bbox_path = _download_url_to_file(bbox_clean, file_path)
            else:
                raise ValueError(
                    "Local file paths are not allowed for event AOI sources"
                )
            geometry = _geometry_from_file(bbox_path)
    else:
        lat_min, lat_max, lon_min, lon_max = bbox
        if lat_min == lat_max and lon_min == lon_max:
            geometry = Point(lon_min, lat_min)
        else:
            geometry = box(lon_min, lat_min, lon_max, lat_max)

    return geometry, geometry.bounds, geometry.centroid


def _add_aoi_to_events(
    events: Iterable[Event],
    file_dir: str,
) -> List[Event]:
    """
    Enrich events with AOI geometry derived from their link.
    Adds:
      - event["aoi_polygon"]
      - event["aoi"]
      - event["centroid"]
    """
    out: List[Event] = []
    for e in events:
        props = e.get("properties")
        if not isinstance(props, dict):
            props = {}
        event_type = str(props.get("event") or "")
        link = ""
        event_lower = event_type.lower()
        if "flood" in event_lower:
            raw_link = e.get("link")
            link = str(raw_link).strip() if isinstance(raw_link, str) else ""
        elif "storm" in event_lower:
            affected_zones = props.get("affectedZones", [])
            link = str(affected_zones[0]) if affected_zones else ""
        if not link:
            log.debug("Event %s has no link; skipping AOI", e.get("id"))
            out.append(e)
            continue

        try:
            aoi_polygon, aoi, centroid = _bbox_to_geometry(link, file_dir)

            e["aoi_polygon"] = aoi_polygon
            e["aoi"] = aoi
            e["centroid"] = centroid

        except Exception as exc:
            log.warning(
                "Failed to build AOI for event %s (link=%r): %s",
                e.get("id"),
                link,
                exc,
            )
        out.append(e)
    return out


def _download_url_to_file(
    url: str,
    output_path: str | Path,
    timeout: int = 30,
    ensure_geojson: bool = True,
) -> Path:
    """
    Download a URL and save its content to a file (GeoJSON-safe).
    """
    output_path = Path(output_path)

    if ensure_geojson and output_path.suffix.lower() != ".geojson":
        output_path = output_path.with_suffix(".geojson")

    _validate_remote_url(url)

    response = requests.get(url, timeout=timeout, stream=True)
    response.raise_for_status()
    ctype = (response.headers.get("Content-Type") or "").lower()
    if "json" not in ctype:
        raise ValueError(f"Expected JSON payload from {url}, got Content-Type={ctype}")

    payload = bytearray()
    for chunk in response.iter_content(chunk_size=16384):
        if not chunk:
            continue
        payload.extend(chunk)
        if len(payload) > MAX_GEOJSON_BYTES:
            raise ValueError(
                f"Response from {url} exceeded max size ({MAX_GEOJSON_BYTES} bytes)"
            )

    # Parse JSON to ensure validity
    try:
        data = json.loads(payload.decode("utf-8"))
    except ValueError as e:
        raise ValueError(f"Response from {url} is not valid JSON") from e

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

    return output_path


def _geometry_from_file(path: str | Path):
    """
    Read a geometry from a spatial file (KML or GeoJSON).
    """
    from shapely.geometry import shape
    from shapely.ops import unary_union

    path = Path(path)
    suffix = path.suffix.lower()

    # # ---- KML ----
    # if suffix == ".kml":
    #     return create_polygon_from_kml(str(path))

    # ---- GeoJSON ----
    if suffix in (".geojson", ".json"):
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        # FeatureCollection
        if data["type"] == "FeatureCollection":
            geometries = [shape(f["geometry"]) for f in data["features"]]
            return geometries[0] if len(geometries) == 1 else unary_union(geometries)

        # Single Feature
        if data.get("type") == "Feature":
            geometry = data.get("geometry")

            if geometry is None:
                affected = data.get("properties", {}).get("affectedZones", [])

                geometries = []
                for zone_url in affected:
                    try:
                        zone_data = requests.get(zone_url).json()
                        if zone_data.get("geometry"):
                            geometries.append(shape(zone_data["geometry"]))
                    except Exception:
                        pass

                if geometries:
                    return (
                        geometries[0]
                        if len(geometries) == 1
                        else unary_union(geometries)
                    )
                else:
                    raise ValueError("No geometry found and affectedZones failed.")

            return shape(geometry)

        # Raw geometry
        return shape(data)

    raise ValueError(f"Unsupported spatial file format: {path}")
