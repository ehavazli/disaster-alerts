import os

os.environ.setdefault("PANDAS_FUTURE_INFER_STRING", "0")

import glob
import html
import json
import logging
import re
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path

from disasters.pipeline import (
    PipelineConfig,
    run_download_only,
    run_mosaic_only,
    run_pipeline,
    run_search_only,
)
from flask import Flask, jsonify, render_template_string, request, send_from_directory

print("Flask is running with Python:", sys.executable)
logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s"
)

app = Flask(__name__)

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")
HTML_FILE = "activated_events_map.html"
BASE_OUTPUT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

processing_runs = {}
processing_runs_lock = threading.Lock()


class _ThreadSafeSearchCache:
    def __init__(self):
        self._lock = threading.Lock()
        self._data = {"signature": None, "folder": None}

    def __getitem__(self, key):
        with self._lock:
            return self._data[key]

    def __setitem__(self, key, value):
        with self._lock:
            self._data[key] = value

    def get(self, key, default=None):
        with self._lock:
            return self._data.get(key, default)

    def update(self, **kwargs):
        with self._lock:
            self._data.update(kwargs)

    def snapshot(self):
        with self._lock:
            return dict(self._data)


LAST_SEARCH_CACHE = _ThreadSafeSearchCache()


def _get_search_signature(params):
    import json

    prods = params.get("products") or []
    sats = params.get("satellites") or []

    sig_dict = {
        "bbox": [
            params.get("lat_min"),
            params.get("lat_max"),
            params.get("lon_min"),
            params.get("lon_max"),
        ],
        "products": sorted(prods),
        "satellites": sorted(sats),
        "functionality": params.get("functionality", "opera_search"),
        "date_strat": params.get("dis_date_strat"),
        "recent_n": params.get("dis_recent_n"),
        "single_date": params.get("dis_single_date"),
        "start_date": params.get("dis_start_date"),
        "end_date": params.get("dis_end_date"),
        "opt_cloud": bool(params.get("opt_cloud", False)),
    }

    return json.dumps(sig_dict, sort_keys=True)


def _list_output_folders():
    return sorted(
        glob.glob(os.path.join(BASE_OUTPUT_DIR, "nextpass_outputs_*")),
        key=os.path.getmtime,
        reverse=True,
    )


def _create_run(search_type, task_count=1):
    run_id = uuid.uuid4().hex
    run_state = {
        "running": True,
        "active_tasks": task_count,
        "latest_folder": None,
        "error": None,
        "search_type": search_type or ["opera_search"],
        "started_at": time.time(),
    }
    with processing_runs_lock:
        processing_runs[run_id] = run_state
    return run_id


def _get_run_state(run_id):
    if not run_id:
        return None
    with processing_runs_lock:
        run_state = processing_runs.get(run_id)
        if run_state is None:
            return None
        return dict(run_state)


def _update_run_state(run_id, **updates):
    with processing_runs_lock:
        run_state = processing_runs.get(run_id)
        if run_state is None:
            return
        run_state.update(updates)


def _mark_task_complete(run_id):
    """Safely decrements the active task counter and marks run false when 0."""
    with processing_runs_lock:
        run_state = processing_runs.get(run_id)
        if run_state is None:
            return
        run_state["active_tasks"] -= 1
        if run_state["active_tasks"] <= 0:
            run_state["running"] = False


def run_overpasses_only(run_id, params):
    """
    Builds the command line arguments based on the dashboard panel selections.
    Executes a next_pass query for satellite overpasses ONLY.
    """
    run_state = _get_run_state(run_id)
    if run_state is None:
        return

    try:
        # Build the base terminal command using the bounding box
        cmd = [
            sys.executable,
            "-m",
            "next_pass",
            "-b",
            str(params["lat_min"]),
            str(params["lat_max"]),
            str(params["lon_min"]),
            str(params["lon_max"]),
            "-f",
            "overpasses",
        ]

        # Append UI parameters specific to the "Next Pass" panel
        if params.get("satellites") and "all" not in params["satellites"]:
            cmd.extend(["-s"] + params["satellites"])

        if params.get("np_lookback") and str(params["np_lookback"]).isdigit():
            cmd.extend(["-k", str(params["np_lookback"])])

        if params.get("drcs") == "yes" and params.get("np_event_date"):
            cmd.extend(["-g", params["np_event_date"]])

        # Take a snapshot of folders before running, execute,
        # then find the newly created folder
        before_folders = set(
            glob.glob(os.path.join(BASE_OUTPUT_DIR, "nextpass_outputs_*"))
        )
        subprocess.run(cmd, check=True, cwd=BASE_OUTPUT_DIR)
        after_folders = set(
            glob.glob(os.path.join(BASE_OUTPUT_DIR, "nextpass_outputs_*"))
        )

        new_folders = list(after_folders - before_folders)
        if new_folders:
            _update_run_state(
                run_id, latest_folder=max(new_folders, key=os.path.getmtime)
            )
        else:
            _update_run_state(run_id, error="No output folder could be matched.")
    except Exception as e:
        _update_run_state(run_id, error=str(e))
    finally:
        _mark_task_complete(run_id)


def run_opera_search(run_id, params):
    """
    Queries the Earthdata catalog for OPERA products using the 'disasters' pipeline.
    Caches the resulting output directory for faster downstream mosaicking.
    """
    run_state = _get_run_state(run_id)
    if run_state is None:
        return

    try:
        bbox = [
            float(params["lat_min"]),
            float(params["lat_max"]),
            float(params["lon_min"]),
            float(params["lon_max"]),
        ]

        # Parse product selections directly from the UI
        raw_products = params.get("products", [])
        target_products = [p for p in raw_products if p != "all"]

        # If "all" was chosen or nothing was selected,
        # pass None to search the full catalog
        search_products = (
            None if (not target_products or "all" in raw_products) else target_products
        )

        # Even though this is purely a search, map the
        # advanced Disasters date panel logic
        date_strat = params.get("dis_date_strat", "range")
        pipeline_date = None
        number_of_dates = 5

        if date_strat == "single":
            pipeline_date = params.get("dis_single_date")
        elif date_strat == "range":
            if params.get("dis_start_date") and params.get("dis_end_date"):
                pipeline_date = f"{params['dis_start_date']}/{params['dis_end_date']}"
        elif date_strat == "recent":
            r_val = params.get("dis_recent_n")
            if isinstance(r_val, str):
                r_val = r_val.strip()
            number_of_dates = int(r_val) if r_val and str(r_val).isdigit() else 5

        # Isolate the search output
        output_dir = Path(BASE_OUTPUT_DIR) / f"search_outputs_{run_id}"

        # Execute the search natively in Python
        result_dir = run_search_only(
            bbox=bbox,
            output_dir=output_dir,
            product=search_products,
            date=pipeline_date,
            number_of_dates=number_of_dates,
            functionality=params.get("functionality", "opera_search"),
            compute_cloudiness=bool(params.get("opt_cloud", False)),
            satellites=(
                params.get("satellites")
                if "all" not in params.get("satellites", [])
                else None
            ),
        )

        # Cache the search signature and folder path
        if result_dir:
            LAST_SEARCH_CACHE.update(
                signature=_get_search_signature(params), folder=result_dir
            )
            _update_run_state(run_id, latest_folder=str(result_dir))
        else:
            _update_run_state(run_id, error="Search exited gracefully without outputs.")

    except Exception as e:
        _update_run_state(run_id, error=str(e))
    finally:
        _mark_task_complete(run_id)


def run_disasters(run_id, params):
    """
    Runs the full end-to-end disasters pipeline.
    Phase 1: Generates a unified HTML map of all products.
    Phase 2: Loops through each product to mosaic individually.
    """
    run_state = _get_run_state(run_id)
    if run_state is None:
        return

    try:
        bbox = [
            float(params["lat_min"]),
            float(params["lat_max"]),
            float(params["lon_min"]),
            float(params["lon_max"]),
        ]

        # Parse product selections directly from the UI
        raw_products = params.get("products", [])
        target_products = [p for p in raw_products if p != "all"]

        # Phase 1 needs None for a unified comprehensive search if "all" is active
        search_products = (
            None if (not target_products or "all" in raw_products) else target_products
        )

        # Parse the Disasters Date panel logic
        date_strat = params.get("dis_date_strat", "range")
        pipeline_date = None
        number_of_dates = 5

        if date_strat == "single":
            pipeline_date = params.get("dis_single_date")
            if not pipeline_date:
                raise ValueError("dis_single_date is required for single-date mode")
        elif date_strat == "range":
            start_date = params.get("dis_start_date")
            end_date = params.get("dis_end_date")
            if not start_date or not end_date:
                raise ValueError(
                    "dis_start_date and dis_end_date are required for range mode"
                )
            pipeline_date = f"{start_date}/{end_date}"
        elif date_strat == "recent":
            r_val = params.get("dis_recent_n")
            if isinstance(r_val, str):
                r_val = r_val.strip()
            number_of_dates = int(r_val) if r_val and str(r_val).isdigit() else 5
        else:
            raise ValueError(f"Unsupported dis_date_strat: {date_strat}")

        output_dir = Path(BASE_OUTPUT_DIR) / f"disasters_outputs_{run_id}"
        output_dir.mkdir(parents=True, exist_ok=True)
        _update_run_state(run_id, latest_folder=str(output_dir))

        action = params.get("dis_action", "run")
        search_type = params.get("search_type", ["opera_search"])
        functionality = (
            "both"
            if "overpasses" in search_type or "all" in search_type
            else "opera_search"
        )
        satellites_list = (
            params.get("satellites")
            if "all" not in params.get("satellites", [])
            else None
        )

        # =========================================================
        # PHASE 1: UNIFIED SEARCH (Generates 1 Map for All Layers)
        # =========================================================
        current_sig = _get_search_signature(params)
        search_dir = None

        # Take a frozen snapshot to prevent race conditions (From main)
        cache_snap = LAST_SEARCH_CACHE.snapshot()

        if cache_snap["signature"] == current_sig and cache_snap["folder"]:
            search_dir = Path(cache_snap["folder"])
        else:
            search_dir = run_search_only(
                bbox=bbox,
                output_dir=output_dir,
                product=search_products,
                date=pipeline_date,
                number_of_dates=number_of_dates,
                functionality=functionality,
                compute_cloudiness=bool(params.get("opt_cloud", False)),
                satellites=satellites_list,
            )
            if search_dir:
                LAST_SEARCH_CACHE.update(signature=current_sig, folder=search_dir)

        # =========================================================
        # PHASE 2: PROCESSING (Mosaics Individually)
        # =========================================================
        if not target_products and search_dir:
            from disasters.pipeline import read_opera_metadata

            try:
                df_found = read_opera_metadata(search_dir)
                if not df_found.empty and "Dataset" in df_found.columns:
                    target_products = df_found["Dataset"].dropna().unique().tolist()
            except Exception as e:
                logging.warning(f"Failed to dynamically read catalog metadata: {e}")

        if not target_products:
            target_products = ["OPERA_L3_DSWX-HLS_V1"]

        # Pass the entire list to the newly upgraded pipeline functions
        mode_dir = None
        if action == "download":
            res_dir = run_download_only(
                bbox=bbox,
                output_dir=output_dir,
                date=pipeline_date,
                number_of_dates=number_of_dates,
                product=target_products,
                functionality=functionality,
                compute_cloudiness=bool(params.get("opt_cloud", False)),
            )
            if res_dir:
                mode_dir = res_dir

        elif action == "mosaic":
            data_dir = run_download_only(
                bbox=bbox,
                output_dir=output_dir,
                date=pipeline_date,
                number_of_dates=number_of_dates,
                product=target_products,
                functionality=functionality,
                compute_cloudiness=bool(params.get("opt_cloud", False)),
            )
            if data_dir:
                res_dir = run_mosaic_only(
                    input_dir=data_dir,
                    output_dir=output_dir,
                    bbox=bbox,
                    benchmark=False,
                )
                if res_dir:
                    mode_dir = res_dir

        else:
            config = PipelineConfig(
                bbox=bbox,
                output_dir=output_dir,
                local_dir=None,
                search_dir=search_dir,
                product=target_products,
                functionality=functionality,
                satellites=satellites_list,
                date=pipeline_date,
                number_of_dates=number_of_dates,
                layout_title=(
                    f"Disaster Analysis ({bbox[0]:.2f},{bbox[2]:.2f} –"
                    f" {bbox[1]:.2f},{bbox[3]:.2f})"
                ),
                reclassify_snow_ice=bool(params.get("opt_rc", False)),
                compute_cloudiness=bool(params.get("opt_cloud", False)),
                no_mask=bool(params.get("opt_nomask", False)),
                filter_date=params.get("opt_fd") or None,
                slope_threshold=(
                    int(params["opt_st"])
                    if str(params.get("opt_st")).isdigit()
                    else None
                ),
            )
            res_dir = run_pipeline(config)
            if res_dir:
                mode_dir = res_dir

        if mode_dir and mode_dir.exists():
            _update_run_state(run_id, latest_folder=str(output_dir))
        else:
            _update_run_state(
                run_id, error="Processing exited without generating a mode folder."
            )

    except Exception as e:
        _update_run_state(run_id, error=str(e))
    finally:
        _mark_task_complete(run_id)


# ---- Serve original map ----
@app.route("/")
def index():
    return send_from_directory(DATA_DIR, HTML_FILE)


# ---- Ping endpoint ----
@app.route("/test_ping", methods=["GET"])
def test_ping():
    print("Ping received!")
    return "pong", 200


# ---- Process bbox ----
@app.route("/process_bbox", methods=["POST"])
def process_bbox():
    data = request.get_json()
    if not data or not all(
        k in data for k in ("lat_min", "lat_max", "lon_min", "lon_max")
    ):
        return jsonify({"error": "Missing bounding box data"}), 400

    print(
        f"Received search request for bbox: {data.get('lat_min')},"
        f" {data.get('lon_min')}"
    )

    search_type = data.get("search_type", ["opera_search"])
    if isinstance(search_type, str):
        search_type = [search_type]

    targets = []

    # Determine which processing function to run based on the search type
    if "disasters" in search_type or "all" in search_type:
        targets.append(run_disasters)
    elif "opera_search" in search_type and "overpasses" in search_type:
        data["functionality"] = "both"
        targets.append(run_opera_search)
    elif "opera_search" in search_type:
        data["functionality"] = "opera_search"
        targets.append(run_opera_search)
    elif "overpasses" in search_type:
        targets.append(run_overpasses_only)

    if not targets:
        return jsonify({"error": "No valid workflows selected"}), 400

    # Create a unified run ID for this request, injecting the target count
    run_id = _create_run(search_type, task_count=len(targets))

    # Spawn a separate thread for every active target
    for target in targets:
        threading.Thread(target=target, args=(run_id, data), daemon=True).start()

    return jsonify({"status": "processing started", "run_id": run_id})


# ---- Status endpoint ----
@app.route("/processing_status")
def processing_status():
    run_id = request.args.get("run_id")
    run_state = _get_run_state(run_id)
    if run_state is None:
        return jsonify({"error": "Unknown or missing run_id"}), 404

    return jsonify(
        {
            "running": run_state["running"],
            "error": run_state["error"],
        }
    )


# ---- Serve maps from latest next-pass or disasters folder ----
@app.route("/maps/<run_id>/<path:filename>")
def maps(run_id, filename):
    run_state = _get_run_state(run_id)
    if run_state is None:
        return f"Unknown run: {run_id}", 404

    folder = run_state.get("latest_folder")
    if not folder:
        return f"No folder matched for run: {run_id}", 404

    file_path = os.path.join(folder, filename)

    if os.path.exists(file_path):
        directory = os.path.dirname(file_path)
        name = os.path.basename(file_path)
        return send_from_directory(directory, name)

    return f"File {filename} not found", 404


@app.route("/show_maps")
def show_maps():
    """
    Display run_output.txt first, then the maps below it.
    Renders GeoTIFFs natively for Disasters workflows, or HTML iframes for Next Pass.
    Shows a third DRCS map if opera_products_drcs_map.html was produced.
    """
    run_id = request.args.get("run_id")
    run_state = _get_run_state(run_id)
    if run_state is None:
        return "<h3>No run selected. Start a dashboard search first.</h3>", 404

    folder = run_state.get("latest_folder")
    if run_state.get("running") and not folder:
        return (
            "<h3>This search is still processing. Please wait a moment and try"
            " again.</h3>"
        )

    if run_state.get("error"):
        return f"<h3>Processing failed: {html.escape(run_state['error'])}</h3>", 500

    if not folder:
        return (
            "<h3>No output found for this search.</h3>",
            404,
        )

    log_file = os.path.join(folder, "run_output.txt")
    log_content = ""
    if os.path.exists(log_file):
        with open(log_file, "r") as f:
            log_content = html.escape(f.read())

    log_html = f"<pre>{log_content}</pre>" if log_content.strip() else ""

    search_type = run_state.get("search_type", ["opera_search"])
    if isinstance(search_type, str):
        search_type = [search_type]

    uses_disasters = "disasters" in search_type or "all" in search_type

    # Recursively search the parent folder for the HTML maps
    sat_map_path = next(Path(folder).rglob("satellite_overpasses_map.html"), None)
    opera_map_path = next(Path(folder).rglob("opera_products_map.html"), None)

    show_sat = sat_map_path is not None
    show_opera = opera_map_path is not None

    # Handle Next Pass & Opera Search (HTML iFrames)
    iframes = ""
    if show_sat:
        rel_sat = os.path.relpath(sat_map_path, folder).replace(os.sep, "/")
        iframes += f'<iframe src="/maps/{run_id}/{rel_sat}"></iframe>'
    if show_opera:
        rel_opera = os.path.relpath(opera_map_path, folder).replace(os.sep, "/")
        iframes += f'<iframe src="/maps/{run_id}/{rel_opera}"></iframe>'

    # Handle Disasters Workflow (Dynamic Leaflet GeoTIFF Layer Controls)
    geotiff_viewer = ""
    tif_layers_json = []

    if uses_disasters:
        # Walk through output folder to catch all mosaics
        for root, _, files in os.walk(folder):
            for file in files:
                # Skip RTC (too heavy for browser RAM) and temporary files
                if (
                    file.endswith(".tif")
                    and "RTC" not in file
                    and not file.startswith(("tmp_", "."))
                ):
                    rel_path = os.path.relpath(os.path.join(root, file), folder)
                    url_path = rel_path.replace(os.sep, "/")
                    url = f"/maps/{run_id}/{url_path}"

                    display_name = file.replace("_mosaic", "").replace(".tif", "")
                    display_name = display_name.replace("OPERA_L3_", "").replace(
                        "OPERA_L2_", ""
                    )

                    date_matches = re.findall(r"\d{8}T\d+[A-Za-z]*", display_name)

                    if date_matches:
                        for d in date_matches:
                            date_str = d[:8]
                            time_str = d[9:13]

                            # Format time
                            if len(time_str) == 4:
                                formatted_time = f"{time_str[:2]}:{time_str[2:]}"
                            else:
                                formatted_time = time_str

                            # Use square brackets so it looks clean
                            ds = date_str
                            f_date = f" [{ds[:4]}-{ds[4:6]}-{ds[6:]} {formatted_time}] "
                            display_name = display_name.replace(d, f_date)

                    # Cleanly replace underscores with spaces and fix any double spaces
                    display_name = display_name.replace("_", " ").strip()
                    display_name = re.sub(r"\s+", " ", display_name)

                    tif_layers_json.append({"url": url, "name": display_name})

        if tif_layers_json:
            geotiff_viewer = f"""
            <div id="geotiff-map" style="flex: 1; height: 100%;"></div>
            <link rel="stylesheet"
                  href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
            <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
            <script src="https://unpkg.com/georaster"></script>
            <script src="https://unpkg.com/georaster-layer-for-leaflet"></script>
            <script>
                var osmUrl = 'https://{{s}}.tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png';
                var osm = L.tileLayer(osmUrl, {{
                    attribution: '&copy; OpenStreetMap contributors'
                }});

                var satUrl = 'https://server.arcgisonline.com/ArcGIS/rest/' +
                             'services/World_Imagery/MapServer/tile/{{z}}/{{y}}/{{x}}';
                var satellite = L.tileLayer(satUrl, {{
                    attribution: 'Tiles &copy; Esri'
                }});

                var map = L.map('geotiff-map', {{
                    center: [0, 0],
                    zoom: 2,
                    layers: [satellite],
                    wheelDebounceTime: 150,
                    zoomAnimation: false
                }});

                var baseMaps = {{
                    "Satellite": satellite,
                    "OpenStreetMap": osm
                }};

                var overlayMaps = {{}};
                var layerControl = L.control.layers(
                    baseMaps, overlayMaps, {{ collapsed: false }}
                ).addTo(map);

                var tifLayers = {json.dumps(tif_layers_json)};
                var bounds = null;
                var loadedCount = 0;

                function tryFitBounds() {{
                    loadedCount++;
                    if (loadedCount === tifLayers.length && bounds) {{
                        map.fitBounds(bounds);
                    }}
                }}

                tifLayers.forEach(function(layerInfo, index) {{
                    fetch(layerInfo.url)
                      .then(response => {{
                          if (!response.ok) throw new Error("Fetch failed");
                          return response.arrayBuffer();
                      }})
                      .then(arrayBuffer => {{
                        parseGeoraster(arrayBuffer).then(function(georaster) {{

                          var layerOptions = {{
                              georaster: georaster,
                              opacity: 0.8,
                              resolution: 256,
                              pixelValuesToColorFn: function(values) {{
                                  var val = values[0];

                                  if (val === 255 || isNaN(val)) return null;

                                  if (georaster.palette && georaster.palette[val]) {{
                                    var c = georaster.palette[val];
                                    var rgbStr = 'rgb(' +
                                        c[0] + ',' +
                                        c[1] + ',' +
                                        c[2] + ')';
                                    return rgbStr;
                                }}

                                  if (layerInfo.name.includes('CONF')) {{
                                      return 'rgba(147, 51, 234, ' + (val/100) + ')';
                                  }}

                                  return '#10b981';
                              }}
                          }};

                          var layer = new GeoRasterLayer(layerOptions);

                          if (index === 0) {{
                              layer.addTo(map);
                          }}

                          layerControl.addOverlay(layer, layerInfo.name);

                          if (!bounds) {{
                              bounds = layer.getBounds();
                          }} else {{
                              bounds.extend(layer.getBounds());
                          }}

                          tryFitBounds();
                        }}).catch(e => {{
                            console.error("Parse error for " + layerInfo.name, e);
                            tryFitBounds();
                        }});
                      }}).catch(e => {{
                          var msg = "Network or Memory error for " + layerInfo.name;
                          console.error(msg, e);
                          tryFitBounds();
                      }});
                }});
            </script>
            """

    # Adjust styling based on what is being shown
    map_count = sum([show_sat, show_opera])
    if uses_disasters and tif_layers_json:
        map_count += 1

    iframe_width = f"{100 // map_count}%" if map_count else "100%"

    page_html = f"""
    <html>
      <head>
        <title>Results</title>
        <style>
          body {{ display:flex; flex-direction: column; margin:0;
                  height:100vh; font-family:sans-serif;
                  background:#f3f4f6; }}
          pre {{ flex:0 0 25%; overflow:auto; padding:15px; margin:0;
                 background:#111; color:#10b981; font-size:12px;
                 border-bottom:2px solid #374151; }}
          .maps-row {{ display:flex; flex:1; background:white; }}
          iframe {{ width:{iframe_width}; height:100%; border:none;
                    border-right:1px solid #d1d5db; }}
        </style>
      </head>
      <body>
        {log_html}
        <div class="maps-row">
            {iframes}
            {geotiff_viewer}
        </div>
      </body>
    </html>
    """
    return render_template_string(page_html)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
