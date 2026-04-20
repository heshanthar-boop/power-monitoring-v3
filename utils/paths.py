"""utils.paths

Single source of truth for all filesystem locations.

Defaults:
  %APPDATA%/PowerMonitoringReporting/...

Patch-1 change:
  - Support a user-selected base folder (cfg['paths']['base_dir']).
  - When base_dir is set, everything (logs, reports, profiles, email queue,
    protection_events.csv, chart snapshots) lives under that folder.

This keeps deployments simple on customer PCs: one folder contains everything.
"""

from __future__ import annotations

import os
import sys
from typing import Optional


APP_NAME = "PowerMonitoringReporting"

# Runtime override (set early from config)
_BASE_DIR_OVERRIDE: Optional[str] = None


def _default_root() -> str:
    """Stable root used to store the base-dir hint.

    This always points to %APPDATA%/PowerMonitoringReporting (or CWD fallback) regardless
    of the override. It prevents a chicken-and-egg problem when the user moves
    the app's working folder.
    """
    base = os.environ.get("APPDATA") or os.getcwd()
    p = os.path.join(base, APP_NAME)
    os.makedirs(p, exist_ok=True)
    return p


def _hint_file() -> str:
    return os.path.join(_default_root(), "base_dir.txt")


def is_frozen() -> bool:
    return getattr(sys, "frozen", False)


def set_base_dir(path: str | None) -> None:
    """Set a global base folder override.

    - If path is empty/None: clears override (falls back to %APPDATA%).
    - Always creates the folder.
    """
    global _BASE_DIR_OVERRIDE
    p = (path or "").strip()
    if not p:
        _BASE_DIR_OVERRIDE = None
        # Clear hint
        try:
            if os.path.exists(_hint_file()):
                os.remove(_hint_file())
        except Exception:
            pass
        return
    p = os.path.abspath(os.path.expanduser(p))
    os.makedirs(p, exist_ok=True)
    _BASE_DIR_OVERRIDE = p
    # Persist hint so the next run can pick it up before loading config/loggers.
    try:
        with open(_hint_file(), "w", encoding="utf-8") as f:
            f.write(p)
    except Exception:
        pass


def get_base_dir() -> str:
    """Return active base folder."""
    if _BASE_DIR_OVERRIDE:
        return _BASE_DIR_OVERRIDE
    return _default_root()


def bootstrap_base_dir_from_hint() -> None:
    """If a hint file exists, apply it (best-effort).

    Safe to call multiple times.
    """
    try:
        hp = _hint_file()
        if os.path.exists(hp):
            p = (open(hp, "r", encoding="utf-8").read() or "").strip()
            if p:
                # Don't re-write hint file again: set internal override only.
                global _BASE_DIR_OVERRIDE
                _BASE_DIR_OVERRIDE = os.path.abspath(os.path.expanduser(p))
                os.makedirs(_BASE_DIR_OVERRIDE, exist_ok=True)
    except Exception:
        pass


# Apply hint at import time so early loggers already use the chosen folder.
bootstrap_base_dir_from_hint()


def _ensure(sub: str) -> str:
    p = os.path.join(get_base_dir(), sub)
    os.makedirs(p, exist_ok=True)
    return p


def appdata_dir() -> str:
    """Backward-compatible alias (now respects base_dir override)."""
    return get_base_dir()


def logs_dir() -> str:
    return _ensure("logs")


def profiles_dir() -> str:
    return _ensure("profiles")


def reports_dir() -> str:
    return _ensure("reports")


def db_dir() -> str:
    return _ensure("db")


def events_db_path() -> str:
    return os.path.join(db_dir(), "events.db")



def reports_db_path() -> str:
    return os.path.join(reports_dir(), "reports.db")


def email_dir() -> str:
    return _ensure("email")


def email_db_path() -> str:
    return os.path.join(email_dir(), "email_queue.db")


def config_path() -> str:
    return os.path.join(get_base_dir(), "config.json")
