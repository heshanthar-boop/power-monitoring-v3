"""Runtime state for temporary stale-threshold overrides."""

from __future__ import annotations

from typing import Optional

_stale_override: Optional[float] = None


def get_stale_override() -> Optional[float]:
    """Return the currently active stale override, if any."""
    return _stale_override


def set_stale_override(value: Optional[float]) -> None:
    """Set or clear a temporary stale override (seconds)."""
    global _stale_override
    _stale_override = value
