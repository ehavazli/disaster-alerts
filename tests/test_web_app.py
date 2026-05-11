import importlib.util
from pathlib import Path

import pytest

pytest.importorskip("flask")

spec = importlib.util.spec_from_file_location(
    "test_web_app_module", Path(__file__).resolve().parents[1] / "web" / "app.py"
)
web_app = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(web_app)


@pytest.fixture(autouse=True)
def clear_processing_runs():
    with web_app.processing_runs_lock:
        web_app.processing_runs.clear()
    yield
    with web_app.processing_runs_lock:
        web_app.processing_runs.clear()


@pytest.fixture
def client():
    web_app.app.config["TESTING"] = True
    return web_app.app.test_client()


def test_process_bbox_returns_unique_run_ids_per_request(monkeypatch, client):
    started_threads = []

    class DummyThread:
        def __init__(self, target, args=(), daemon=None):
            self.target = target
            self.args = args
            self.daemon = daemon

        def start(self):
            started_threads.append((self.target, self.args, self.daemon))

    monkeypatch.setattr(web_app.threading, "Thread", DummyThread)

    payload = {
        "lat_min": 1,
        "lat_max": 2,
        "lon_min": 3,
        "lon_max": 4,
        "search_type": "both",
    }

    response_a = client.post("/process_bbox", json=payload)
    response_b = client.post(
        "/process_bbox", json={**payload, "search_type": "opera_search"}
    )

    run_id_a = response_a.get_json()["run_id"]
    run_id_b = response_b.get_json()["run_id"]

    assert run_id_a != run_id_b
    assert started_threads[0][0] is web_app.run_next_pass
    assert started_threads[0][1] == (run_id_a, payload)
    assert started_threads[1][1] == (
        run_id_b,
        {**payload, "search_type": "opera_search"},
    )
    assert web_app._get_run_state(run_id_a)["search_type"] == "both"
    assert web_app._get_run_state(run_id_b)["search_type"] == "opera_search"


def test_processing_status_is_scoped_to_run_id(client):
    run_id_a = web_app._create_run("both")
    run_id_b = web_app._create_run("opera_search")

    web_app._update_run_state(run_id_a, running=True, error="run A failed")
    web_app._update_run_state(run_id_b, running=False, error=None)

    response_a = client.get("/processing_status", query_string={"run_id": run_id_a})
    response_b = client.get("/processing_status", query_string={"run_id": run_id_b})

    assert response_a.status_code == 200
    assert response_a.get_json() == {"running": True, "error": "run A failed"}
    assert response_b.status_code == 200
    assert response_b.get_json() == {"running": False, "error": None}


def test_show_maps_and_map_assets_use_run_specific_output_folder(client, tmp_path):
    run_id_a = web_app._create_run("overpasses")
    run_id_b = web_app._create_run("opera_search")
    folder_a = tmp_path / "nextpass_outputs_a"
    folder_b = tmp_path / "nextpass_outputs_b"
    folder_a.mkdir()
    folder_b.mkdir()

    (folder_a / "run_output.txt").write_text("A log", encoding="utf-8")
    (folder_b / "run_output.txt").write_text("B log", encoding="utf-8")
    (folder_a / "satellite_overpasses_map.html").write_text(
        "sat-a", encoding="utf-8"
    )
    (folder_b / "opera_products_map.html").write_text("opera-b", encoding="utf-8")

    web_app._update_run_state(run_id_a, running=False, latest_folder=str(folder_a))
    web_app._update_run_state(run_id_b, running=False, latest_folder=str(folder_b))

    show_a = client.get("/show_maps", query_string={"run_id": run_id_a})
    show_b = client.get("/show_maps", query_string={"run_id": run_id_b})
    map_a = client.get(f"/maps/{run_id_a}/satellite_overpasses_map.html")
    map_b = client.get(f"/maps/{run_id_b}/opera_products_map.html")

    body_a = show_a.get_data(as_text=True)
    body_b = show_b.get_data(as_text=True)

    assert show_a.status_code == 200
    assert f'/maps/{run_id_a}/satellite_overpasses_map.html' in body_a
    assert "/opera_products_map.html" not in body_a
    assert show_b.status_code == 200
    assert f'/maps/{run_id_b}/opera_products_map.html' in body_b
    assert "/satellite_overpasses_map.html" not in body_b
    assert map_a.get_data(as_text=True) == "sat-a"
    assert map_b.get_data(as_text=True) == "opera-b"


def test_processing_status_rejects_unknown_run_id(client):
    response = client.get("/processing_status", query_string={"run_id": "missing"})

    assert response.status_code == 404
    assert response.get_json() == {"error": "Unknown or missing run_id"}
