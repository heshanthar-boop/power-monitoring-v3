from __future__ import annotations

import time
from typing import Any, Dict, List


def summarize_health(cfg: dict, meters: list, worker=None, logging_engine=None, stale_sec: float = 10.0) -> Dict[str, str]:
    """Build a small operator-facing health summary.

    Returns short text states for top-bar badges. Uses plain words so operators do
    not need to decode colors.
    """
    out = {
        "comm": "OFFLINE",
        "data": "OFFLINE",
        "logging": "OFF",
        "mode": "RUN",
        "detail": "No active communication",
    }

    # Comm state
    bus = {}
    if worker is not None:
        try:
            bus = worker.get_bus_health() or {}
        except Exception:
            bus = {}
    bus_state = str(bus.get("state", "OFFLINE") or "OFFLINE").upper()
    if bus_state == "OK":
        out["comm"] = "ONLINE"
        out["detail"] = "Communication healthy"
    elif bus_state == "STALE":
        out["comm"] = "STALE"
        out["detail"] = "Communication alive but data is aging"
    elif bus_state == "RECOVERING":
        out["comm"] = "RECOVERING"
        try:
            nxt = float(bus.get("next_in", 0.0) or 0.0)
            out["detail"] = f"Auto-reconnect active, next retry in {max(0, round(nxt))} s"
        except Exception:
            out["detail"] = "Auto-reconnect active"
    else:
        out["comm"] = "OFFLINE"
        out["detail"] = "No live bus communication"

    try:
        if bus.get("port_available", None) is False:
            out["comm"] = "RECOVERING" if bool(bus.get("connected", False)) is False else "STALE"
            out["detail"] = "Configured COM port is not present"
    except Exception:
        pass

    # Data state
    enabled = [m for m in (meters or []) if bool(getattr(m, "enabled", True))]
    if not enabled:
        out["data"] = "NO METERS"
    elif out["comm"] == "OFFLINE":
        out["data"] = "OFFLINE"
    else:
        stale = False
        now = time.time()
        fresh_count = 0
        for m in enabled:
            try:
                last_rx = float(worker.get_meter_last_rx_ts(getattr(m, "meter_id", 0)) or 0.0) if worker is not None else 0.0
            except Exception:
                last_rx = 0.0
            valid = bool(getattr(m, "data_valid", False))
            if valid and last_rx and (now - last_rx) <= float(stale_sec):
                fresh_count += 1
            else:
                stale = True
        if fresh_count == 0:
            out["data"] = "INVALID"
        elif stale:
            out["data"] = "MIXED"
        else:
            out["data"] = "GOOD"

    # Logging state
    if logging_engine is not None:
        state = str(getattr(logging_engine, "state", "DISABLED") or "DISABLED").upper()
        mapping = {
            "RUNNING": "RUNNING",
            "SCHEDULED_OFF": "PAUSED",
            "ERROR": "FAULT",
            "DISABLED": "OFF",
        }
        out["logging"] = mapping.get(state, state)
        try:
            wh = getattr(logging_engine, "write_health", {}) or {}
            if int(wh.get("write_error_count", 0) or 0) > 0 or int(wh.get("dropped_write_count", 0) or 0) > 0:
                out["logging"] = "FAULT"
                out["detail"] = "Logging write errors or dropped rows detected"
        except Exception:
            pass

    # Write mode if setup writes are globally unlocked
    try:
        from utils.write_guard import is_write_unlocked
        if is_write_unlocked():
            out["mode"] = "WRITE ENABLED"
    except Exception:
        pass

    return out
