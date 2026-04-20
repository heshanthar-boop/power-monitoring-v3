from __future__ import annotations

import time
from dataclasses import dataclass


@dataclass
class SupervisorDecision:
    should_restart: bool
    reason: str = ""
    limit_hit: bool = False  # True when hourly restart cap is reached


class Supervisor:
    """Lightweight service supervisor (runs in the UI thread).

    Why UI-thread supervisor (no extra threads):
    - Tkinter is not thread-safe.
    - Restarting the worker must update UI references safely.

    This supervisor ONLY makes decisions. The UI owns the restart action.
    """

    def __init__(self, cfg: dict):
        serial = (cfg or {}).get("serial", {}) or {}
        self.hb_timeout_sec = float(serial.get("supervisor_hb_timeout_sec", 15.0))
        self.min_restart_gap_sec = float(serial.get("supervisor_min_restart_gap_sec", 10.0))
        self.max_restarts_per_hour = int(serial.get("supervisor_max_restarts_per_hour", 6))

        self._last_restart_ts = 0.0
        self._restart_ts_hist: list[float] = []

    def tick(self, worker) -> SupervisorDecision:
        """Return a restart decision.

        Worker API expected:
        - is_alive()
        - get_heartbeat_age_s()
        """
        now = time.time()
        if worker is None:
            return SupervisorDecision(False)

        # Dead thread
        try:
            if hasattr(worker, "is_alive") and (not worker.is_alive()):
                return self._gate_restart(now, "Worker thread stopped")
        except Exception:
            # If we can't query thread health, do nothing (fail safe)
            pass

        # Stuck heartbeat
        try:
            age = float(worker.get_heartbeat_age_s())
            if age >= self.hb_timeout_sec:
                return self._gate_restart(now, f"Worker heartbeat stale ({age:.1f}s)")
        except Exception:
            pass

        return SupervisorDecision(False)

    def _gate_restart(self, now: float, reason: str) -> SupervisorDecision:
        # Minimum gap between restarts
        if self._last_restart_ts and (now - self._last_restart_ts) < self.min_restart_gap_sec:
            return SupervisorDecision(False)

        # Rate limit: restarts per hour
        one_hour_ago = now - 3600.0
        self._restart_ts_hist = [t for t in self._restart_ts_hist if t >= one_hour_ago]
        if len(self._restart_ts_hist) >= self.max_restarts_per_hour:
            return SupervisorDecision(False, reason="Restart rate limit reached", limit_hit=True)

        self._last_restart_ts = now
        self._restart_ts_hist.append(now)
        return SupervisorDecision(True, reason)
