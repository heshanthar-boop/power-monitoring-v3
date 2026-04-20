from __future__ import annotations

from dataclasses import dataclass

from core.snapshot_bus import MeterQuality


# Operator-facing quality states (SCADA-style)
SCADA_OK = "OK"
SCADA_STALE = "STALE"
SCADA_OFFLINE = "OFFLINE"


@dataclass(frozen=True)
class QualityDecision:
    """Resolved, operator-facing data quality.

    This is the single rule used across UI + engines.
    Keep it deterministic and cheap.
    """

    scada: str                 # OK / STALE / OFFLINE
    meter_quality: MeterQuality  # GOOD / STALE / COMM_LOST / DISABLED
    reason: str                # short operator text
    age_s: float               # seconds since last_good_ts (large if none)


def resolve_quality(
    *,
    connected: bool,
    enabled: bool,
    last_good_ts: float | None,
    age_s: float | None,
    stale_after_s: float,
    offline_after_s: float,
) -> QualityDecision:
    """Resolve quality from a minimal set of runtime facts.

    Rules (SCADA-grade):
      - Disabled -> OFFLINE/DISABLED
      - No data -> OFFLINE/NO DATA
      - If not connected and age >= offline_after -> OFFLINE/COMM LOST
      - If age >= stale_after -> STALE/STALE DATA
      - else OK/LIVE

    NOTE: We keep both operator-facing (OK/STALE/OFFLINE) and the internal
    MeterQuality enum so existing code paths stay stable.
    """

    if not enabled:
        return QualityDecision(SCADA_OFFLINE, MeterQuality.DISABLED, "DISABLED", 0.0)

    if last_good_ts is None:
        return QualityDecision(SCADA_OFFLINE, MeterQuality.COMM_LOST, "NO DATA", 1e9)

    a = float(age_s if age_s is not None else 0.0)
    if (not connected) and a >= float(offline_after_s):
        return QualityDecision(SCADA_OFFLINE, MeterQuality.COMM_LOST, "COMM LOST", a)

    if a >= float(stale_after_s):
        return QualityDecision(SCADA_STALE, MeterQuality.STALE, "STALE DATA", a)

    return QualityDecision(SCADA_OK, MeterQuality.GOOD, "LIVE", a)
