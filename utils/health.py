"""Helper utilities for evaluating meter health / freshness."""

from __future__ import annotations

from typing import Any, Dict, Optional

from utils.stale_override import get_stale_override


def get_meter_age(meter: Any) -> Optional[float]:
    """
    Return the age (seconds since last good read) for a meter.
    Returns None if the age cannot be determined.
    """
    if meter is None:
        return None

    age_fn = getattr(meter, "age", None)
    if callable(age_fn):
        try:
            return age_fn()
        except Exception:
            return None

    age_attr = getattr(meter, "age", None)
    if isinstance(age_attr, (int, float)):
        return float(age_attr)

    return None


def meter_is_fresh(meter: Any, stale_sec: float) -> bool:
    """
    Return True when the meter has valid data that is not older than `stale_sec`.
    If `stale_sec` is <= 0 the stale check is disabled and only data_valid is enforced.
    """
    if not getattr(meter, "data_valid", False):
        return False
    if stale_sec is None or stale_sec <= 0.0:
        return True

    age = get_meter_age(meter)
    if age is None:
        return False

    return age <= stale_sec


def stale_seconds(cfg: Optional[Dict[str, Any]]) -> float:
    """
    Normalize the configured stale threshold (seconds).
    """
    alarms = (cfg or {}).get("alarms", {}) or {}
    try:
        value = alarms.get("stale_sec", 0)
        stale = float(value) if value is not None else 0.0
    except (TypeError, ValueError):
        stale = 0.0

    if stale < 0.0:
        stale = 0.0

    override = get_stale_override()
    if override is not None:
        try:
            override_val = float(override)
        except (TypeError, ValueError):
            override_val = None
        if override_val is not None:
            return max(stale, override_val)

    return stale
