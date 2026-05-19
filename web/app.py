import os

os.environ.setdefault("PANDAS_FUTURE_INFER_STRING", "0")

import glob
import html
import logging
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path

from disasters.pipeline import PipelineConfig, run_pipeline, run_search_only
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


LAST_SEARCH_CACHE = {
    "signature": None,
    "folder": None
}

def _get_search_signature(params):
    return str({
        "bbox": [params.get("lat_min"), params.get("lat_max"), params.get("lon_min"), params.get("lon_max")],
        "products": params.get("products", []),
        "date_strat": params.get("dis_date_strat"),
        "recent_n": params.get("dis_recent_n"),
        "single_date": params.get("dis_single_date"),
        "start_date": params.get("dis_start_date"),
        "end_date": params.get("dis_end_date")
    })

def _list_output_folders():
    return sorted(
        glob.glob(os.path.join(BASE_OUTPUT_DIR, "nextpass_outputs_*")),
        key=os.path.getmtime,
        reverse=True,
    )


def _create_run(search_type):
    run_id = uuid.uuid4().hex
    run_state = {
        "running": True,
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
        _update_run_state(run_id, running=False)


def run_opera_search(run_id, params):
    """
    Queries the Earthdata catalog for OPERA products using the 'disasters' pipeline.
    Caches the resulting output directory for faster downstream mosaicking.
    """
    run_state = _get_run_state(run_id)
    if run_state is None: return

    try:
        bbox = [
            float(params["lat_min"]),
            float(params["lat_max"]),
            float(params["lon_min"]),
            float(params["lon_max"])]
        
        # Parse product selections (Preserving the multi-product target fixed list format from PR1)
        products = params.get("products", [])
        target_products = products if products and "all" not in products else None

        # Even though this is purely a search, map the advanced Disasters date panel logic
        date_strat = params.get("dis_date_strat", "range")
        pipeline_date = None
        number_of_dates = 5
        if date_strat == "single": pipeline_date = params.get("dis_single_date")
        elif date_strat == "range":
            if params.get("dis_start_date") and params.get("dis_end_date"):
                pipeline_date = f"{params['dis_start_date']}/{params['dis_end_date']}"
        elif params.get("dis_recent_n"):
            number_of_dates = int(params["dis_recent_n"])
            
        # Isolate the search output
        output_dir = Path(BASE_OUTPUT_DIR) / f"search_outputs_{run_id}"
        
        # Strip prefixes for standard search engine compatibility if explicit targets are used
        np_prod = None
        if target_products:
            np_prod = [p.replace("OPERA_L3_", "").replace("OPERA_L2_", "") for p in target_products]

        # Execute the search natively in Python (instead of via subprocess)
        result_dir = run_search_only(
            bbox=bbox, 
            output_dir=output_dir,
            product=np_prod,
            date=pipeline_date,
            number_of_dates=number_of_dates,
            compute_cloudiness=bool(params.get("opt_cloud", False))
        )
        
        # Cache the search signature and folder path for potential reuse in the disasters workflow
        if result_dir:
            # Record a "signature" of the exact UI inputs used to generate this search and resulting folder path
            LAST_SEARCH_CACHE["signature"] = _get_search_signature(params)
            LAST_SEARCH_CACHE["folder"] = result_dir

            _update_run_state(run_id, latest_folder=str(result_dir))
        else:
            _update_run_state(run_id, error="Search exited gracefully without outputs.")
            
    except Exception as e:
        _update_run_state(run_id, error=str(e))
    finally:
        _update_run_state(run_id, running=False)


def run_disasters(run_id, params):
    """
    Runs the full end-to-end disasters pipeline.
    Checks the cache first to see if it can skip the cloud search phase.
    """
    run_state = _get_run_state(run_id)
    if run_state is None: return

    try:
        bbox = [
            float(params["lat_min"]),
            float(params["lat_max"]),
            float(params["lon_min"]),
            float(params["lon_max"])]
            
        products = params.get("products", [])
        target_products = products if products and "all" not in products else None

        # Parse the Disasters Date panel logic
        date_strat = params.get("dis_date_strat", "range")
        pipeline_date = None

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
        elif date_strat == "recent":
            recent_n = params.get("dis_recent_n")
            number_of_dates = int(recent_n) if str(recent_n).isdigit() else 5
        else:
            raise ValueError(f"Unsupported dis_date_strat: {date_strat}")

        # Calculate the signature of the current UI inputs (check cache)
        current_sig = _get_search_signature(params)
        local_dir = None
        
        # If current UI inputs match UI inputs of the last search, grab folder from cache
        if LAST_SEARCH_CACHE["signature"] == current_sig and LAST_SEARCH_CACHE["folder"]:
            local_dir = Path(LAST_SEARCH_CACHE["folder"])

        output_dir = Path(BASE_OUTPUT_DIR) / f"disasters_outputs_{run_id}"
        output_dir.mkdir(parents=True, exist_ok=True)

        config = PipelineConfig(
            bbox=bbox,
            output_dir=output_dir,
            search_dir=local_dir,
            product=target_products,
            date=pipeline_date,
            number_of_dates=number_of_dates,
            layout_title=(
                f"Disaster Analysis ({bbox[0]:.2f},{bbox[2]:.2f} – "
                f"{bbox[1]:.2f},{bbox[3]:.2f})"
            ),
            reclassify_snow_ice=bool(params.get("opt_rc", False)),
            compute_cloudiness=bool(params.get("opt_cloud", False)),
            no_mask=bool(params.get("opt_nomask", False)),
            filter_date=params.get("opt_fd") or None,
            slope_threshold=(
                int(params["opt_st"]) if str(params.get("opt_st")).isdigit() else None
            ),
        )

        print(
            f"Running disasters pipeline: product={config.product}, bbox={config.bbox}"
        )

        # Route execution based on UI dropdown
        dis_action = params.get("dis_action", "run")
        returned_dir = None

        if dis_action == "download":
            from disasters.pipeline import run_download_only

            returned_dir = run_download_only(
                bbox=config.bbox, output_dir=config.output_dir, product=config.product
            )
        else:
            returned_dir = run_pipeline(config)

        # Register Success/Failure using the returned artifacts.
        if returned_dir and Path(returned_dir).exists():
            _update_run_state(run_id, latest_folder=str(returned_dir))
            print(f"Success! Output folder: {returned_dir}")
        else:
            _update_run_state(
                run_id,
                error=(
                    "Processing finished, but no valid output artifacts were produced."
                ),
            )

    except Exception as e:
        _update_run_state(run_id, error=str(e))
    finally:
        _update_run_state(run_id, running=False)


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
    
    # Track targets dynamically to allow multi-select concurrency
    targets = []
    
    if "disasters" in search_type or "all" in search_type:
        targets.append(run_disasters)
    else:
        # Check independent toggles when a full pipeline run isn't requested
        if "opera_search" in search_type:
            targets.append(run_opera_search)
        if "overpasses" in search_type:
            targets.append(run_overpasses_only)

    if not targets:
        return jsonify({"error": "No valid workflows selected"}), 400

    # Create a unified run ID for this combination request
    run_id = _create_run(search_type)
    
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


# ---- Serve maps from latest next-pass folder ----
@app.route("/maps/<run_id>/<filename>")
def maps(run_id, filename):
    run_state = _get_run_state(run_id)
    if run_state is None:
        return f"Unknown run: {run_id}", 404

    folder = run_state.get("latest_folder")
    if folder and os.path.exists(os.path.join(folder, filename)):
        return send_from_directory(folder, filename)
    return f"File {filename} not found", 404


@app.route("/show_maps")
def show_maps():
    """
    Display run_output.txt first, then the maps below it.
    Shows a third DRCS map if opera_products_drcs_map.html was produced.
    """
    run_id = request.args.get("run_id")
    run_state = _get_run_state(run_id)
    if run_state is None:
        return "<h3>No run selected. Start a search from the dashboard first.</h3>", 404

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
            "<h3>No next-pass output found for this search.</h3>",
            404,
        )

    search_type = run_state.get("search_type", ["opera_search"])
    if isinstance(search_type, str):
        search_type = [search_type]
    sat_map = "satellite_overpasses_map.html"
    opera_map = "opera_products_map.html"
    drcs_map = "opera_products_drcs_map.html"

    uses_disasters = "all" in search_type or "disasters" in search_type
    show_sat = "overpasses" in search_type or "all" in search_type
    show_opera = any(v in search_type for v in ("opera_search", "disasters", "all"))
    show_drcs = (
        (not uses_disasters)
        and show_opera
        and os.path.exists(os.path.join(folder, drcs_map))
    )

    map_count = sum([show_sat, show_opera, show_drcs])
    iframe_width = f"{100 // map_count}%" if map_count else "100%"

    log_file = os.path.join(folder, "run_output.txt")
    log_content = ""
    if os.path.exists(log_file):
        with open(log_file, "r") as f:
            log_content = html.escape(f.read())

    iframes = ""
    if show_sat:
        iframes += f'<iframe src="/maps/{run_id}/{sat_map}"></iframe>'
    if show_opera:
        iframes += f'<iframe src="/maps/{run_id}/{opera_map}"></iframe>'
    if show_drcs:
        iframes += f'<iframe src="/maps/{run_id}/{drcs_map}"></iframe>'

    page_html = f"""
    <html>
      <head>
        <title>Next-Pass Results</title>
        <style>
          body {{ display:flex; flex-direction: column; margin:0;
                  height:100vh; font-family:sans-serif;
                  background:#f3f4f6; }}
          pre {{ flex:0 0 25%; overflow:auto; padding:15px;
                 background:#111; color:#10b981; font-size:12px;
                 border-bottom:2px solid #374151; }}
          .maps-row {{ display:flex; flex:1; background:white; }}
          iframe {{ width:{iframe_width}; height:100%; border:none;
                    border-right:1px solid #d1d5db; }}
        </style>
      </head>
      <body>
        <pre>{log_content}</pre>
        <div class="maps-row">{iframes}</div>
      </body>
    </html>
    """
    return render_template_string(page_html)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)