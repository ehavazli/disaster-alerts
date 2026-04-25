import glob
import logging
import os
import subprocess
import sys
import threading

from flask import Flask, jsonify, render_template_string, request, send_from_directory

print("Flask is running with Python:", sys.executable)
logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s"
)

app = Flask(__name__)

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")
HTML_FILE = "activated_events_map.html"
BASE_OUTPUT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

# Shared state
processing_state = {
    "running": False,
    "latest_folder": None,
    "error": None,
    "search_type": None,
}


def run_next_pass(params):
    """
    Builds the command line arguments based on the dashboard panel selections
    and executes the next_pass.py script.
    """
    processing_state["running"] = True
    processing_state["latest_folder"] = None
    processing_state["error"] = None
    processing_state["search_type"] = params.get("search_type", "both")
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

        # Find latest output folder
        folders = sorted(
            glob.glob(os.path.join(BASE_OUTPUT_DIR, "nextpass_outputs_*")),
            key=os.path.getmtime,
            reverse=True,
        )
        if folders:
            processing_state["latest_folder"] = folders[0]
            print(f"Success! Output folder: {folders[0]}")
    except Exception as e:
        processing_state["error"] = str(e)
        print(f"Error running next_pass: {e}")
    finally:
        processing_state["running"] = False


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
    threading.Thread(target=run_next_pass, args=(data,)).start()
    return jsonify({"status": "processing started"})


# ---- Status endpoint ----
@app.route("/processing_status")
def processing_status():
    return jsonify(
        {
            "running": processing_state["running"],
            "error": processing_state["error"],
        }
    )


# ---- Serve maps from latest next-pass folder ----
@app.route("/maps/<filename>")
def maps(filename):
    folder = processing_state.get("latest_folder")
    if folder and os.path.exists(os.path.join(folder, filename)):
        return send_from_directory(folder, filename)
    return f"File {filename} not found", 404


@app.route("/show_maps")
def show_maps():
    """
    Display run_output.txt first, then the maps below it.
    Shows a third DRCS map if opera_products_drcs_map.html was produced.
    """
    folder = processing_state.get("latest_folder")
    if not folder:
        return (
            "<h3>No next-pass output yet. Draw a bounding box to start processing.</h3>"
        )

    search_type = processing_state.get("search_type", "both")
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
            log_content = f.read()

    iframes = ""
    if show_sat:
        iframes += f'<iframe src="/maps/{sat_map}"></iframe>'
    if show_opera:
        iframes += f'<iframe src="/maps/{opera_map}"></iframe>'
    if show_drcs:
        iframes += f'<iframe src="/maps/{drcs_map}"></iframe>'

    html = f"""
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
    return render_template_string(html)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
