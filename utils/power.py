# utils/power.py
"""
SCADA operator-station power helpers.

Goal:
- Optionally keep the PC awake (prevent sleep / display sleep) while the SCADA app is running.
- Must be safe: never crash the app if platform APIs are unavailable.
"""
from __future__ import annotations

import sys
from utils.logger import setup_logger

log = setup_logger("power")

# Windows: SetThreadExecutionState flags
_ES_CONTINUOUS = 0x80000000
_ES_SYSTEM_REQUIRED = 0x00000001
_ES_DISPLAY_REQUIRED = 0x00000002

_enabled = False


def set_keep_awake(enabled: bool) -> None:
    """
    Enable/disable sleep prevention for the current process.
    On Windows, calls SetThreadExecutionState. On other OS, no-op.
    """
    global _enabled
    enabled = bool(enabled)

    if sys.platform != "win32":
        _enabled = enabled
        return

    try:
        import ctypes  # type: ignore
        kernel32 = ctypes.windll.kernel32  # type: ignore

        if enabled:
            flags = _ES_CONTINUOUS | _ES_SYSTEM_REQUIRED | _ES_DISPLAY_REQUIRED
        else:
            flags = _ES_CONTINUOUS

        r = kernel32.SetThreadExecutionState(flags)
        if r == 0:
            # API failed (rare). Do not crash.
            log.warning("SetThreadExecutionState failed (return=0). keep_awake=%s", enabled)
        _enabled = enabled
    except Exception as e:
        log.warning("Keep-awake not supported on this system (%s).", e)
        _enabled = enabled


def is_keep_awake() -> bool:
    return bool(_enabled)


def apply_from_cfg(cfg: dict) -> None:
    ui = (cfg.get("ui") or {})
    set_keep_awake(bool(ui.get("keep_awake", False)))
