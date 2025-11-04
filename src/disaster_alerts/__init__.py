from __future__ import annotations

from importlib import metadata

from . import pipeline as _pipeline
from .settings import Settings

# ---- version ----
# Try to read the installed package version; fall back to the in-repo default.
try:
    __version__ = metadata.version("disaster-alerts")
except (
    metadata.PackageNotFoundError
):  # running from source / editable install not built yet
    __version__ = "0.1.0"


# ---- public API ----
def run() -> int:
    """
    Convenience runner:
        from disaster_alerts import run
        run()

    Equivalent to:
        settings = Settings.load()
        pipeline.run(settings)
    """
    settings = Settings.load()
    return _pipeline.run(settings)


__all__ = ["__version__", "Settings", "run"]
