from __future__ import annotations

import os
from pathlib import Path
from typing import Callable, Dict, List, Optional

import pytest

from disaster_alerts.settings import (
    AppConfig,
    EmailConfig,
    Paths,
    ProvidersConfig,
    Recipients,
    Settings,
    Thresholds,
)

# --------------------------- temp repo layout ---------------------------


@pytest.fixture
def tmp_repo(tmp_path: Path) -> Path:
    (tmp_path / "config").mkdir(parents=True, exist_ok=True)
    (tmp_path / "data").mkdir(parents=True, exist_ok=True)
    (tmp_path / "logs").mkdir(parents=True, exist_ok=True)
    (tmp_path / "data" / "state.json").write_text("{}", encoding="utf-8")
    return tmp_path


# --------------------------- Settings factory ---------------------------


@pytest.fixture
def settings_factory(
    tmp_repo: Path, monkeypatch: pytest.MonkeyPatch
) -> Callable[..., Settings]:
    """Build Settings rooted at tmp_repo without reading real YAML/.env."""
    monkeypatch.setenv("DISASTER_ALERTS_ROOT", str(tmp_repo))
    monkeypatch.setenv("DISASTER_ALERTS_STATE_LRU", "128")  # speed up state churn

    def _build(
        recipients: Optional[Dict[str, List[str]]] = None,
        enable_nws: bool = True,
        enable_usgs: bool = True,
        aoi: Optional[dict] = None,
        log_level: str = "ERROR",
    ) -> Settings:
        paths = Paths(
            root=tmp_repo,
            config_dir=tmp_repo / "config",
            data_dir=tmp_repo / "data",
            logs_dir=tmp_repo / "logs",
            state_file=tmp_repo / "data" / "state.json",
        )
        app = AppConfig(
            log_level=log_level,
            aoi=aoi,
            providers=ProvidersConfig(nws=enable_nws, usgs=enable_usgs),
        )
        thresholds = Thresholds()
        rcpts = Recipients.from_raw(recipients or {"default": ["alerts@example.com"]})
        email = EmailConfig(user="sender@example.com", app_password="test-token")
        return Settings(
            paths=paths, app=app, thresholds=thresholds, recipients=rcpts, email=email
        )

    return _build


# --------------------------- Network & SMTP hardening ---------------------------


@pytest.fixture(autouse=True)
def block_network(monkeypatch: pytest.MonkeyPatch):
    """Disallow real HTTP. If a test needs HTTP, it must stub the call explicitly."""
    import requests

    def _nope(*args, **kwargs):
        raise RuntimeError(
            "Network access disabled in tests. Monkeypatch provider HTTP calls."
        )

    monkeypatch.setattr(requests, "get", _nope)


@pytest.fixture(autouse=True)
def fast_backoff(monkeypatch: pytest.MonkeyPatch):
    """Ensure provider helper never sleeps/retries during tests."""
    monkeypatch.setattr(
        "disaster_alerts.providers.common.DEFAULT_TIMEOUT", 1, raising=False
    )
    monkeypatch.setattr(
        "disaster_alerts.providers.common.DEFAULT_RETRIES", 0, raising=False
    )
    monkeypatch.setattr(
        "disaster_alerts.providers.common.DEFAULT_BACKOFF", 0.0, raising=False
    )
    os.environ.setdefault("TZ", "UTC")


@pytest.fixture(autouse=True)
def stub_yagmail(monkeypatch: pytest.MonkeyPatch):
    """Stub yagmail.SMTP so tests don't touch SMTP."""

    class DummySMTP:
        def __init__(self, user, app_password):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def send(self, *a, **kw):
            return None

    monkeypatch.setattr("disaster_alerts.email.yagmail.SMTP", DummySMTP, raising=False)


# ------------------- Helpers to monkeypatch providers -------------------


@pytest.fixture
def patch_nws(monkeypatch: pytest.MonkeyPatch):
    """Return a function to set NWS fetch_events to a custom list."""

    def _set(events: List[dict]):
        monkeypatch.setattr(
            "disaster_alerts.providers.nws.fetch_events", lambda s: events, raising=True
        )

    return _set


@pytest.fixture
def patch_usgs(monkeypatch: pytest.MonkeyPatch):
    """Return a function to set USGS fetch_events to a custom list."""

    def _set(events: List[dict]):
        monkeypatch.setattr(
            "disaster_alerts.providers.usgs.fetch_events",
            lambda s: events,
            raising=True,
        )

    return _set
