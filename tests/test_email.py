# tests/test_email.py
from disaster_alerts.email import build_message
from disaster_alerts.settings import (
    AppConfig,
    EmailConfig,
    Paths,
    ProvidersConfig,
    Settings,
    Thresholds,
)


class DummySettings(Settings):
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
            email=EmailConfig(user="u@e.com", app_password="x"),
        )


def test_build_message_shapes_html_and_text():
    settings = DummySettings.minimal()
    events = [
        {
            "id": "e1",
            "provider": "usgs",
            "title": "M 4.6 - near Somewhere",
            "severity": "Moderate",
            "updated": "2025-11-03T10:00:00Z",
            "link": "https://example.org/e1",
            "geometry": {"type": "Point", "coordinates": [-120, 35, 5]},
            "properties": {"mag": 4.6, "depth_km": 5.0},
        },
        {
            "id": "e2",
            "provider": "nws",
            "title": "Severe Thunderstorm Warning",
            "severity": "Severe",
            "updated": "2025-11-03T10:05:00Z",
            "link": "",
            "geometry": None,
            "properties": {"event": "Thunderstorm"},
        },
    ]

    subject, html_body, text_body = build_message(settings, events, group_key="default")

    assert "[disaster-alerts]" in subject
    assert "new event" in subject
    # HTML contains a table with rows
    assert "<table" in html_body and "<tr>" in html_body
    # Text body lists items with ids
    assert "e1" in text_body and "e2" in text_body
