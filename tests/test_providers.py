# tests/test_providers.py
from datetime import datetime, timezone

from disaster_alerts.providers import nws as nws_mod
from disaster_alerts.providers import usgs as usgs_mod
from disaster_alerts.settings import (
    AppConfig,
    EmailConfig,
    Paths,
    ProvidersConfig,
    Settings,
    Thresholds,
)


class DummySettings(Settings):
    """Lightweight Settings for provider unit tests without reading files."""

    @classmethod
    def minimal(cls) -> "Settings":
        root = Paths(
            root=".",
            config_dir=".",
            data_dir=".",
            logs_dir=".",
            state_file="./data/state.json",
        )
        app = AppConfig(
            log_level="INFO", aoi=None, providers=ProvidersConfig(nws=True, usgs=True)
        )
        return cls(
            paths=root,
            app=app,
            thresholds=Thresholds(),
            recipients={},
            email=EmailConfig(),
        )


def test_nws_fetch_events_monkeypatched(monkeypatch):
    # Sample NWS response (trimmed)
    sample = {
        "type": "FeatureCollection",
        "features": [
            {
                "id": "https://api.weather.gov/alerts/123",
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]],
                },
                "properties": {
                    "id": "NWS-123",
                    "event": "Severe Thunderstorm Warning",
                    "headline": "Severe Thunderstorm Warning for Test County",
                    "severity": "Severe",
                    "effective": "2025-11-03T10:00:00Z",
                    "url": "https://alerts.weather.gov/NWS-123",
                },
            }
        ],
    }

    def fake_get_json(
        url, params=None, headers=None, timeout=15, retries=2, backoff=1.5
    ):
        return sample

    monkeypatch.setattr(nws_mod, "get_json", fake_get_json)

    settings = DummySettings.minimal()
    events = nws_mod.fetch_events(settings)

    assert len(events) == 1
    e = events[0]
    assert e["provider"] == "nws"
    assert e["id"] == "https://api.weather.gov/alerts/123" or e["id"] == "NWS-123"
    assert e["title"].startswith("Severe Thunderstorm Warning")
    assert e["severity"] == "Severe"
    assert e["routing_key"] in {"severe", "default"}


def test_usgs_fetch_events_monkeypatched(monkeypatch):
    now = datetime.now(timezone.utc)
    now_ms = int(now.timestamp() * 1000)

    # Sample USGS response (trimmed)
    sample = {
        "type": "FeatureCollection",
        "features": [
            {
                "id": "usgs123",
                "geometry": {"type": "Point", "coordinates": [-120.0, 35.0, 8.0]},
                "properties": {
                    "mag": 4.6,
                    "time": now_ms,
                    "updated": now_ms,
                    "title": "M 4.6 - 10km SE of Testville",
                    "url": "https://earthquake.usgs.gov/earthquakes/eventpage/usgs123",
                },
            }
        ],
    }

    def fake_get_json(
        url, params=None, headers=None, timeout=15, retries=2, backoff=1.5
    ):
        # Assert we honor min magnitude in params
        assert "minmagnitude" in params
        return sample

    monkeypatch.setattr(usgs_mod, "get_json", fake_get_json)

    settings = DummySettings.minimal()
    events = usgs_mod.fetch_events(settings)

    assert len(events) == 1
    e = events[0]
    assert e["provider"] == "usgs"
    assert e["id"] == "usgs123"
    assert e["severity"] in {
        "Moderate",
        "Strong",
        "Major",
        "Great",
        "Light",
        "Minor",
    }  # bucketed
    assert "depth_km" in e["properties"]
    assert e["properties"]["depth_km"] == 8.0
