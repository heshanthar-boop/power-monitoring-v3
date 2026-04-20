"""Utilities for safe logging without spamming the log file.

SCADA/HMI apps often run forever; an exception inside a periodic callback can
repeat every 200 ms and flood logs. This helper logs a given key only once,
optionally with a cool-down.
"""

from __future__ import annotations

import time
from typing import Dict, Tuple


_SEEN: Dict[str, Tuple[float, int]] = {}


def log_once(logger, key: str, level: str, message: str, cooldown_sec: float = 0.0) -> None:
    """Log *message* for *key* only once (or once per cooldown window).

    Args:
        logger: a logging.Logger
        key: stable identifier for the repeating issue
        level: "debug"|"info"|"warning"|"error"|"exception"
        message: text to log
        cooldown_sec: if >0, allow re-logging after this many seconds
    """
    now = time.time()
    last_ts, count = _SEEN.get(key, (0.0, 0))
    if count > 0 and cooldown_sec <= 0:
        return
    if count > 0 and cooldown_sec > 0 and (now - last_ts) < cooldown_sec:
        return
    _SEEN[key] = (now, count + 1)
    fn = getattr(logger, level, None)
    if fn is None:
        fn = logger.info
    fn(message)
