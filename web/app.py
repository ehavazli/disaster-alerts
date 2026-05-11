import glob
import html
import logging
import os
import subprocess
import sys
import threading
import time
import uuid

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
        "search_type": search_type or "both",
        "started_at": time.time(),
        "known_folders": set(_list_output_folders()),
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


def _find_run_output_folder(before_folders, started_at):
    candidate_folders = _list_output_folders()
    new_folders = [folder for folder in candidate_folders if folder not in before_folders]
    if new_folders:
        recent_new_folders = [
            folder
            for folder in new_folders
            if os.path.getmtime(folder) >= (started_at - 1)
        ]
        if recent_new_folders:
            new_folders = recent_new_folders
        return max(new_folders, key=os.path.getmtime)

    recent_folders = [
        folder
        for folder in candidate_folders
        if os.path.getmtime(folder) >= (started_at - 1)
    ]
    if recent_folders:
        return max(recent_folders, key=os.path.getmtime)

    return None


def run_next_pass(run_id, params):
    """
    Builds the command line arguments based on the dashboard panel selections
    and executes the next_pass.py script.
    """
    run_state = _get_run_state(run_id)
    if run_state is None:
        return

    try:
        # 1) Base command with Bounding Box
        cmd = [
            sys.executable,
            "-m",
            "next_pass",
            "-b",
            str(params["lat_min"]),
            str(params["lat_max"]),
            str(params["lon_min"]),
            str(params["lon_max"]),
        ]

        # 2) Add Search Type (-f)
        if params.get("search_type"):
            cmd += ["-f", params["search_type"]]

        # 3) Add Satellites (-s) - Allows multiple values
        if params.get("satellites"):
            cmd.append("-s")
            # If 'all' is selected, you might want to handle it specifically
            # or just pass it if the script handles 'all'
            cmd.extend(params["satellites"])

        # 4) Add Products (-p) - Allows multiple values; skip entirely if "all" selected
        products = params.get("products", [])
        if products and "all" not in products:
            cmd.append("-p")
            cmd.extend(products)

        # 5) Add Lookback (-k)
        if params.get("lookback") and str(params["lookback"]).isdigit():
            cmd += ["-k", str(params["lookback"])]

        # 6) Add Event Date (-g)
        # Logic: only add if DRCS is 'yes' and a valid date is provided
        if params.get("drcs") == "yes" and params.get("event_date") not in [
            None,
            "N/A",
            "",
        ]:
            cmd += ["-g", params["event_date"]]

        print("Executing command:", " ".join(cmd))
        subprocess.run(cmd, check=True, cwd=BASE_OUTPUT_DIR)

        output_folder = _find_run_output_folder(
            run_state["known_folders"], run_state["started_at"]
        )
        if output_folder:
            _update_run_state(run_id, latest_folder=output_folder)
            print(f"Success! Output folder: {output_folder}")
        else:
            _update_run_state(
                run_id,
                error="Processing finished, but no output folder could be matched to this run.",
            )
    except Exception as e:
        _update_run_state(run_id, error=str(e))
        print(f"Error running next_pass: {e}")
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

    # Passing 'data' dictionary ensures run_next_pass gets satellites,
    # products, etc.
    run_id = _create_run(data.get("search_type"))
    threading.Thread(target=run_next_pass, args=(run_id, data), daemon=True).start()
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
        return "<h3>This search is still processing. Please wait a moment and try again.</h3>"

    if run_state.get("error"):
        return f"<h3>Processing failed: {html.escape(run_state['error'])}</h3>", 500

    if not folder:
        return (
            "<h3>No next-pass output found for this search.</h3>",
            404,
        )

    search_type = run_state.get("search_type", "both")
    sat_map = "satellite_overpasses_map.html"
    opera_map = "opera_products_map.html"
    drcs_map = "opera_products_drcs_map.html"

    show_sat = search_type in ("both", "overpasses")
    show_opera = search_type in ("both", "opera_search")
    show_drcs = show_opera and os.path.exists(os.path.join(folder, drcs_map))

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
