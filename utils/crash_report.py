from __future__ import annotations

import json
import os
import platform
import sys
import threading
import traceback
from datetime import datetime
from typing import Any, Callable, Optional

from utils.paths import get_base_dir

_SECRET_KEY_PARTS = ("password", "secret", "token", "api_key", "apikey", "private", "credential")
_MAX_CONFIG_CHARS = 120_000

_cfg_getter: Optional[Callable[[], dict]] = None
_installed = False
_previous_sys_hook = None
_previous_thread_hook = None


def crash_reports_dir() -> str:
    path = os.path.join(get_base_dir(), "crash_reports")
    os.makedirs(path, exist_ok=True)
    return path


def _stamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S_%f")


def _safe_part(value: str) -> str:
    out = []
    for ch in str(value or "crash"):
        if ch.isalnum() or ch in ("-", "_"):
            out.append(ch)
        else:
            out.append("_")
    return "".join(out).strip("_")[:48] or "crash"


def _redact(obj: Any) -> Any:
    if isinstance(obj, dict):
        out: dict[str, Any] = {}
        for key, value in obj.items():
            key_s = str(key)
            if any(part in key_s.lower() for part in _SECRET_KEY_PARTS):
                out[key_s] = "<redacted>" if value not in (None, "") else value
            else:
                out[key_s] = _redact(value)
        return out
    if isinstance(obj, list):
        return [_redact(v) for v in obj]
    if isinstance(obj, tuple):
        return [_redact(v) for v in obj]
    return obj


def _format_traceback(exc_type, exc_value, exc_tb) -> str:
    if exc_type is None and exc_value is not None:
        exc_type = type(exc_value)
    if exc_type is None:
        return "No exception type supplied.\n"
    if exc_tb is None:
        return "".join(traceback.format_exception_only(exc_type, exc_value))
    return "".join(traceback.format_exception(exc_type, exc_value, exc_tb))


def _cfg_snapshot(cfg_getter: Optional[Callable[[], dict]]) -> str:
    if not callable(cfg_getter):
        return "No config getter installed.\n"
    try:
        cfg = cfg_getter()
    except Exception as exc:
        return f"Config snapshot unavailable: {type(exc).__name__}: {exc}\n"
    try:
        text = json.dumps(_redact(cfg), indent=2, sort_keys=True, default=str)
    except Exception as exc:
        return f"Config snapshot serialization failed: {type(exc).__name__}: {exc}\n"
    if len(text) > _MAX_CONFIG_CHARS:
        text = text[:_MAX_CONFIG_CHARS] + "\n... <truncated>"
    return text + "\n"


def write_crash_report(
    exc_type,
    exc_value,
    exc_tb,
    cfg_getter: Optional[Callable[[], dict]] = None,
    origin: str = "uncaught",
) -> str:
    """Write a local crash report and return its path.

    This function is intentionally dependency-light so it can run safely from
    Python/Tkinter exception hooks without causing secondary failures.
    """
    path = os.path.join(crash_reports_dir(), f"crash_{_stamp()}_{_safe_part(origin)}.txt")
    getter = cfg_getter if cfg_getter is not None else _cfg_getter
    lines = [
        "MFM384 Monitoring Crash Report\n",
        "================================\n",
        f"Timestamp: {datetime.now().isoformat(timespec='seconds')}\n",
        f"Origin: {origin}\n",
        f"Python: {sys.version.replace(os.linesep, ' ')}\n",
        f"Platform: {platform.platform()}\n",
        f"Executable: {sys.executable}\n",
        f"Frozen: {bool(getattr(sys, 'frozen', False))}\n",
        f"CWD: {os.getcwd()}\n",
        f"argv: {sys.argv!r}\n",
        "\nTraceback\n",
        "---------\n",
        _format_traceback(exc_type, exc_value, exc_tb),
        "\nRedacted Config Snapshot\n",
        "------------------------\n",
        _cfg_snapshot(getter),
    ]
    with open(path, "w", encoding="utf-8", errors="replace") as f:
        f.writelines(lines)
    return path


def handle_exception(exc_type, exc_value, exc_tb, origin: str = "uncaught") -> str:
    try:
        path = write_crash_report(exc_type, exc_value, exc_tb, origin=origin)
    except Exception:
        return ""
    try:
        import logging

        logging.getLogger("mfm384").error("Unhandled exception captured in crash report: %s", path)
    except Exception:
        pass
    return path


def _sys_excepthook(exc_type, exc_value, exc_tb) -> None:
    try:
        handle_exception(exc_type, exc_value, exc_tb, origin="sys")
    finally:
        previous = _previous_sys_hook or sys.__excepthook__
        if previous is not _sys_excepthook:
            try:
                previous(exc_type, exc_value, exc_tb)
            except Exception:
                try:
                    sys.__excepthook__(exc_type, exc_value, exc_tb)
                except Exception:
                    pass


def _thread_excepthook(args) -> None:
    thread = getattr(args, "thread", None)
    name = getattr(thread, "name", "thread") or "thread"
    try:
        handle_exception(args.exc_type, args.exc_value, args.exc_traceback, origin=f"thread_{name}")
    finally:
        previous = _previous_thread_hook
        if callable(previous) and previous is not _thread_excepthook:
            try:
                previous(args)
            except Exception:
                pass


def install_crash_reporter(cfg_getter: Optional[Callable[[], dict]] = None) -> None:
    """Install process-wide exception hooks for field diagnostics."""
    global _cfg_getter, _installed, _previous_sys_hook, _previous_thread_hook
    _cfg_getter = cfg_getter
    if _installed:
        return

    _previous_sys_hook = sys.excepthook
    sys.excepthook = _sys_excepthook

    if hasattr(threading, "excepthook"):
        _previous_thread_hook = threading.excepthook
        threading.excepthook = _thread_excepthook

    _installed = True
