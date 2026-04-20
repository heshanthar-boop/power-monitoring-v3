from __future__ import annotations

from typing import Any


def _state_text(value: Any) -> str:
    if value is None:
        return ""
    try:
        raw = value.value
    except Exception:
        raw = value
    return str(raw or "").strip().upper()


def plain_comm_state(value: Any) -> str:
    state = _state_text(value)
    if state in {"OK", "GOOD", "ONLINE", "CONNECTED", "LIVE"}:
        return "Live"
    if state in {"RECOVERING", "CONNECTING", "RECONNECTING", "COM_BUSY"}:
        return "Reconnecting"
    if state in {"STALE", "MIXED", "OLD DATA"} or "STALE" in state:
        return "Old Data"
    if state in {"COMM_LOST", "OFFLINE", "DISCONNECTED", "ERROR", "FAULT", "INVALID", "NO DATA"}:
        return "No Data"
    if state in {"DISABLED", "OFF"}:
        return "Off"
    return state.title() if state else "Unknown"


def plain_meter_quality(value: Any, *, first_data_received: bool = True) -> str:
    state = _state_text(value)
    if not first_data_received:
        return "Waiting"
    if state in {"GOOD", "OK", "ONLINE", "LIVE"}:
        return "Live"
    if state in {"STALE", "MIXED", "OLD DATA"} or "STALE" in state:
        return "Old Data"
    if state in {"COMM_LOST", "OFFLINE", "DISCONNECTED", "ERROR", "FAULT", "INVALID", "NO DATA"}:
        return "No Data"
    if state in {"DISABLED", "OFF"}:
        return "Disabled"
    return state.title() if state else "Unknown"


def plain_logging_state(value: Any) -> str:
    state = _state_text(value)
    if state in {"RUNNING", "ON", "GOOD", "OK"}:
        return "Recording"
    if state in {"SCHEDULED_OFF", "PAUSED"}:
        return "Scheduled Off"
    if state in {"ERROR", "FAULT"}:
        return "Logger Fault"
    if state in {"DISABLED", "OFF"}:
        return "Off"
    return state.title() if state else "Unknown"


def role_mode_name(role: str, *, master: bool = False) -> str:
    if master:
        return "Master Admin Mode"
    role_n = str(role or "").strip().lower()
    if role_n == "engineer":
        return "Engineer Mode"
    if role_n == "owner":
        return "Owner View"
    return "Technician Mode"


def role_mode_hint(role: str, *, master: bool = False) -> str:
    if master:
        return "Full access for commissioning, recovery, and user control."
    role_n = str(role or "").strip().lower()
    if role_n == "engineer":
        return "Diagnostics, configuration, and site support tools are available by permission."
    if role_n == "owner":
        return "Business view focused on production, alarms, and reports."
    return "Daily operation view focused on live status, alarms, and quick fixes."
