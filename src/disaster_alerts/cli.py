from __future__ import annotations

import argparse
import json
import os
import sys
from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    from . import __version__ as PKG_VERSION  # type: ignore
except Exception:
    PKG_VERSION = None  # falls back to "0.0.0"

from . import pipeline
from .settings import Settings


def _parse_args(argv: List[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="disaster-alerts",
        description="Cron-friendly alerts from NWS/USGS with yagmail notifications.",
    )
    p.add_argument(
        "--config-dir",
        type=Path,
        help="Override config directory (default: ./config or $DISASTER_ALERTS_CONFIG_DIR)",
    )
    p.add_argument(
        "--root",
        type=Path,
        help="Override repo root (default: inferred from package or $DISASTER_ALERTS_ROOT)",
    )
    p.add_argument(
        "--dotenv",
        type=Path,
        help="Path to a .env file to load (default: <root>/.env).",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Run end-to-end but do NOT send emails (logs only).",
    )
    p.add_argument(
        "--print-settings",
        action="store_true",
        help="Print effective settings (with secrets redacted) and exit.",
    )
    p.add_argument(
        "--version",
        action="store_true",
        help="Print version and exit.",
    )
    return p.parse_args(argv)


def _redact(obj: Dict[str, Any]) -> Dict[str, Any]:
    out = deepcopy(obj)
    email = out.get("email")
    if isinstance(email, dict) and email.get("app_password"):
        email["app_password"] = "********"
    return out


def _as_dict(settings: Settings) -> Dict[str, Any]:
    def conv(v: Any) -> Any:
        if isinstance(v, Path):
            return str(v)
        if isinstance(v, list):
            return [conv(x) for x in v]
        if isinstance(v, dict):
            return {k: conv(x) for k, x in v.items()}
        # pydantic v2 models
        try:
            dumped = v.model_dump()  # type: ignore[attr-defined]
            return conv(dumped)
        except Exception:
            return v

    # Recipients is a flexible model; strip private attrs before dumping.
    recipients_public = {
        k: v
        for k, v in getattr(settings.recipients, "__dict__", {}).items()
        if not k.startswith("_")
    }

    return {
        "paths": conv(settings.paths),
        "app": conv(settings.app),
        "thresholds": conv(settings.thresholds),
        "recipients": conv(recipients_public),
        "email": conv(settings.email),
        "enabled_providers": settings.enabled_providers,
    }


def main(argv: Optional[List[str]] = None) -> int:
    ns = _parse_args(argv or sys.argv[1:])

    if ns.version:
        ver = PKG_VERSION or "0.0.0"
        print(f"disaster-alerts {ver}")
        return 0

    # Environment overrides (allow CLI to take precedence)
    if ns.root:
        os.environ["DISASTER_ALERTS_ROOT"] = str(ns.root.expanduser())
    if ns.config_dir:
        os.environ["DISASTER_ALERTS_CONFIG_DIR"] = str(ns.config_dir.expanduser())

    # Load settings
    try:
        settings = Settings.load(
            root=ns.root.expanduser() if ns.root else None,
            dotenv=ns.dotenv.expanduser() if ns.dotenv else None,
        )
    except Exception as e:
        print(f"[config] {e}", file=sys.stderr)
        return 1

    if ns.print_settings:
        data = _redact(_as_dict(settings))
        print(json.dumps(data, indent=2, ensure_ascii=False))
        return 0

    if ns.dry_run:
        # Monkey-patch email sending to no-op while still exercising build_message
        from . import email as email_mod

        real_send = email_mod.send

        def _noop_send(_settings, recipients, subject, _html_body, text_body):
            print("[dry-run] would send to:", ", ".join(recipients))
            print("[dry-run] subject:", subject)
            preview = text_body[:200].replace("\n", " ")
            print("[dry-run] text preview (first 200 chars):")
            print(preview + ("..." if len(text_body) > 200 else ""))
            return None

        email_mod.send = _noop_send  # type: ignore[assignment]
        try:
            count = pipeline.run(settings)
        except Exception as e:
            email_mod.send = real_send  # restore before exiting
            print(f"[runtime] {e}", file=sys.stderr)
            return 2
        finally:
            email_mod.send = real_send
        return 0

    # Normal run
    try:
        pipeline.run(settings)
    except Exception as e:
        print(f"[runtime] {e}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
