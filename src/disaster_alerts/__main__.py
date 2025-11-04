"""Module entrypoint to enable `python -m disaster_alerts`."""
from __future__ import annotations

from .cli import main

if __name__ == "__main__":
    raise SystemExit(main())
