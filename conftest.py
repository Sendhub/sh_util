"""Pytest bootstrap for sh_util test runs inside the billing repo."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


def _bootstrap_settings() -> None:
    if "settings" in sys.modules and hasattr(sys.modules["settings"], "SH_UTIL_DB_DRIVER"):
        return

    sh_util_dir = Path(__file__).resolve().parent
    billing_root = sh_util_dir.parents[1]

    for candidate in (billing_root / "settings.py", sh_util_dir / "settings.py"):
        if not candidate.is_file():
            continue
        spec = importlib.util.spec_from_file_location("settings", candidate)
        if spec is None or spec.loader is None:
            continue
        module = importlib.util.module_from_spec(spec)
        try:
            spec.loader.exec_module(module)
        except Exception:
            continue
        if hasattr(module, "SH_UTIL_DB_DRIVER"):
            sys.modules["settings"] = module
            return


_bootstrap_settings()
