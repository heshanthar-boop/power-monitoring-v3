from __future__ import annotations

import threading
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, Optional, Tuple

from core.data_quality import summarize_issues, validate_meter_values
from core.quality import QualityDecision, SCADA_STALE, resolve_quality
from core.snapshot_bus import MeterSnapshot, MeterQuality


@dataclass(frozen=True)
class StoredSnapshot:
    """Authoritative latest snapshot per meter.

    - seq increments on every update (monotonic per meter)
    - q is the resolved, operator-facing quality
    """

    seq: int
    snapshot: MeterSnapshot
    q: QualityDecision


class DataStore:
    """Central, authoritative latest-value store.

    SnapshotBus is great for *events*, but for SCADA-grade UI reliability we
    also need an always-available "latest" store. This prevents "UI lag = data
    loss" and eliminates the classic "stale mystery" when a subscriber queue
    drops.

    Writer:
      - ModbusWorker (single writer)
    Readers:
      - UI, ProtectionEngine, AlarmEngine, Logging
    """

    def __init__(self, cfg: dict):
        self.cfg = cfg
        self._lock = threading.RLock()
        self._latest: Dict[int, StoredSnapshot] = {}
        self._seq: Dict[int, int] = {}
        # Duration-safe last GOOD marker per meter (monotonic time)
        self._last_good_mon: Dict[int, float] = {}
        self._last_wall_ts: Dict[int, float] = {}
        self._validation_issue_count: Dict[int, int] = defaultdict(int)
        self._timestamp_issue_count: Dict[int, int] = defaultdict(int)
        self._last_quality_issue: Dict[int, str] = {}

    def _thresholds(self) -> Tuple[float, float]:
        qcfg = self.cfg.get("quality", {}) or {}
        stale_after = float(qcfg.get("stale_after_s", 5.0))
        offline_after = float(qcfg.get("offline_after_s", 20.0))
        return stale_after, offline_after

    def update_from_snapshot(
        self,
        snap: MeterSnapshot,
        *,
        enabled: bool = True,
        connected: bool = True,
        last_good_ts: Optional[float] = None,
    ) -> StoredSnapshot:
        """Store a snapshot and compute resolved quality.

        last_good_ts:
          - should be the last successful poll timestamp if known
          - if None, will derive from snap.ts when snap.quality == GOOD
        """

        now_mon = time.monotonic()
        now_wall = time.time()
        stale_after, offline_after = self._thresholds()

        mid = int(snap.meter_id)
        quality_reason = str(getattr(snap, "quality_reason", "") or "")

        # Update last-good monotonic marker and compute age under the lock
        # so _last_good_mon is never accessed from two threads concurrently.
        with self._lock:
            try:
                wall_ts = float(snap.ts)
            except Exception:
                wall_ts = now_wall

            # Wall-clock jumps happen on field PCs. Keep persisted/latest sample
            # timestamps monotonic per meter unless the jump is tiny.
            prev_wall_ts = self._last_wall_ts.get(mid)
            max_back_s = float(((self.cfg.get("data_quality", {}) or {}).get("clock_jump_warn_s", 5.0)) or 5.0)
            if wall_ts > now_wall + max_back_s:
                self._timestamp_issue_count[mid] += 1
                quality_reason = quality_reason or "future timestamp corrected"
                self._last_quality_issue[mid] = quality_reason
                wall_ts = now_wall
            if prev_wall_ts is not None and wall_ts < (prev_wall_ts - max_back_s):
                self._timestamp_issue_count[mid] += 1
                quality_reason = quality_reason or "backward timestamp corrected"
                self._last_quality_issue[mid] = quality_reason
                wall_ts = prev_wall_ts
            self._last_wall_ts[mid] = wall_ts
            snap.ts = wall_ts
            if not getattr(snap, "monotonic_ts", 0.0):
                snap.monotonic_ts = now_mon

            raw_quality = snap.quality
            clean_values, issues = validate_meter_values(snap.values, self.cfg, meter_id=mid)
            if issues:
                snap.values = clean_values
                self._validation_issue_count[mid] += len(issues)
                quality_reason = quality_reason or summarize_issues(issues)
                self._last_quality_issue[mid] = quality_reason

            if snap.quality == MeterQuality.GOOD and not snap.values:
                snap.quality = MeterQuality.STALE
                quality_reason = quality_reason or "no valid values"

            snap.quality_reason = quality_reason
            raw_quality = snap.quality

            lg = last_good_ts
            if lg is None and snap.quality == MeterQuality.GOOD:
                lg = float(snap.ts)

            if snap.quality == MeterQuality.GOOD:
                self._last_good_mon[mid] = now_mon
            last_mon = self._last_good_mon.get(mid)
            age_s = max(0.0, now_mon - float(last_mon)) if last_mon is not None else None

            q = resolve_quality(
                connected=bool(connected),
                enabled=bool(enabled),
                last_good_ts=lg,
                age_s=age_s,
                stale_after_s=stale_after,
                offline_after_s=offline_after,
            )
            if raw_quality == MeterQuality.STALE and q.meter_quality == MeterQuality.GOOD:
                q = QualityDecision(SCADA_STALE, MeterQuality.STALE, quality_reason or "BAD DATA", q.age_s)

            # Enforce resolved quality back onto snapshot consistently.
            snap.quality = q.meter_quality

            self._seq[mid] = int(self._seq.get(mid, 0)) + 1
            seq = self._seq[mid]
            snap.seq = seq
            stored = StoredSnapshot(seq=seq, snapshot=snap, q=q)
            self._latest[mid] = stored
            return stored

    def mark_offline(self, meter_id: int, *, reason: str = "COMM LOST") -> StoredSnapshot:
        """Convenience for comm loss paths."""
        mid = int(meter_id)
        with self._lock:
            prev = self._latest.get(mid)
            last_vals = prev.snapshot.values if prev else {}
            last_good_ts = prev.snapshot.ts if prev else None
            raw_regs = getattr(prev.snapshot, "raw_regs", None) if prev else None

        snap = MeterSnapshot(
            meter_id=mid,
            ts=time.time(),
            values=dict(last_vals) if isinstance(last_vals, dict) else {},
            quality=MeterQuality.COMM_LOST,
            error=str(reason or "COMM LOST"),
            raw_regs=raw_regs,
        )
        return self.update_from_snapshot(snap, enabled=True, connected=False, last_good_ts=last_good_ts)

    def get_latest(self, meter_id: int) -> StoredSnapshot:
        mid = int(meter_id)
        with self._lock:
            stored = self._latest.get(mid)

        if stored is not None:
            return stored

        # safe OFFLINE shell
        snap = MeterSnapshot(meter_id=mid, ts=time.time(), values={}, quality=MeterQuality.COMM_LOST, error="NO DATA")
        return StoredSnapshot(
            seq=0,
            snapshot=snap,
            q=QualityDecision(scada="OFFLINE", meter_quality=MeterQuality.COMM_LOST, reason="NO DATA", age_s=1e9),
        )

    def get_latest_seq(self, meter_id: int) -> int:
        return int(self.get_latest(meter_id).seq)

    def get_quality_health(self) -> dict:
        with self._lock:
            per_meter = {}
            for mid in sorted(set(self._seq) | set(self._validation_issue_count) | set(self._timestamp_issue_count)):
                per_meter[int(mid)] = {
                    "seq": int(self._seq.get(mid, 0)),
                    "validation_issue_count": int(self._validation_issue_count.get(mid, 0)),
                    "timestamp_issue_count": int(self._timestamp_issue_count.get(mid, 0)),
                    "last_issue": str(self._last_quality_issue.get(mid, "") or ""),
                    "last_wall_ts": float(self._last_wall_ts.get(mid, 0.0) or 0.0),
                }
            return {
                "meter_count": len(per_meter),
                "total_validation_issue_count": sum(v["validation_issue_count"] for v in per_meter.values()),
                "total_timestamp_issue_count": sum(v["timestamp_issue_count"] for v in per_meter.values()),
                "per_meter": per_meter,
            }
