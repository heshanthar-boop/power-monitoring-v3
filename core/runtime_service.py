from __future__ import annotations

import threading
import time
from typing import Any, Dict, List, Optional

from utils.logger import setup_logger
from utils.log_once import log_once
from utils.health import stale_seconds

logger = setup_logger("runtime_service")


class RuntimeService:
    """Owns non-UI SCADA service ticks.

    Phase 1 scope:
      - LoggingEngine.tick()
      - AlarmEngine.evaluate_meter()
      - TOTAL mini snapshot refresh for journaling
      - MQTT/REST snapshot and app-status publishing
      - ProtectionEngine supervision evaluation

    Tkinter must not be touched from this thread. Chart overlays and widget
    refresh stay UI-driven.
    """

    handles_logging = True
    handles_alarms = True
    handles_connectivity = True
    handles_protection = True

    def __init__(
        self,
        *,
        cfg: Dict[str, Any],
        meters: List[Any],
        aggregator: Any,
        alarm_engine: Any,
        logging_engine: Any,
        protection_engine: Any = None,
        worker: Any = None,
        mqtt_publisher: Any = None,
        rest_api: Any = None,
        remote_sync: Any = None,
        total_mini_update_cb=None,
        tick_interval_s: float = 0.5,
        alarm_interval_s: float = 1.0,
        total_interval_s: float = 1.0,
        connectivity_interval_s: float = 1.0,
        protection_interval_s: float = 1.0,
    ) -> None:
        self.cfg = cfg
        self.meters = meters
        self.aggregator = aggregator
        self.alarm_engine = alarm_engine
        self.logging_engine = logging_engine
        self.protection_engine = protection_engine
        self.worker = worker
        self.mqtt_publisher = mqtt_publisher
        self.rest_api = rest_api
        self.remote_sync = remote_sync
        self.total_mini_update_cb = total_mini_update_cb
        self.tick_interval_s = max(0.2, float(tick_interval_s or 0.5))
        self.alarm_interval_s = max(0.5, float(alarm_interval_s or 1.0))
        self.total_interval_s = max(0.5, float(total_interval_s or 1.0))
        self.connectivity_interval_s = max(0.5, float(connectivity_interval_s or 1.0))
        self.protection_interval_s = max(0.5, float(protection_interval_s or 1.0))
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._last_alarm_ts = 0.0
        self._last_total_ts = 0.0
        self._last_connectivity_ts = 0.0
        self._last_protection_ts = 0.0
        self._last_total: Optional[Dict[str, Any]] = None

    @property
    def is_running(self) -> bool:
        t = self._thread
        return bool(t and t.is_alive() and not self._stop.is_set())

    def start(self) -> None:
        if self.is_running:
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="runtime-service", daemon=True)
        self._thread.start()
        logger.info("RuntimeService started")

    def stop(self, timeout: float = 3.0) -> None:
        self._stop.set()
        t = self._thread
        if t is not None:
            t.join(timeout=timeout)
            if t.is_alive():
                logger.warning("RuntimeService thread did not exit within %.1fs", timeout)
        self._thread = None

    def set_worker(self, worker: Any) -> None:
        self.worker = worker

    def set_protection_engine(self, protection_engine: Any) -> None:
        self.protection_engine = protection_engine

    def set_remote_sync(self, remote_sync: Any) -> None:
        self.remote_sync = remote_sync

    def _compute_total(self) -> Optional[Dict[str, Any]]:
        try:
            total = self.aggregator.compute(self.meters, cfg=self.cfg)
            return total if isinstance(total, dict) else None
        except Exception as exc:
            log_once(logger, "runtime.total", "exception", f"Runtime total compute failed: {type(exc).__name__}: {exc}", cooldown_sec=10)
            return None

    def _update_total_mini(self, total: Optional[Dict[str, Any]]) -> None:
        if not callable(self.total_mini_update_cb) or not isinstance(total, dict):
            return
        try:
            mini = {
                "quality": str(total.get("quality", "GOOD") or "GOOD"),
                "kW": total.get("kW"),
                "Vavg": total.get("Vavg"),
                "Iavg": total.get("Iavg"),
                "PFavg": total.get("PFavg"),
            }
            self.total_mini_update_cb(mini)
        except Exception as exc:
            log_once(logger, "runtime.total_mini", "exception", f"Runtime total mini update failed: {type(exc).__name__}: {exc}", cooldown_sec=10)

    def _tick_logging(self) -> None:
        if self.logging_engine is None:
            return
        try:
            self.logging_engine.tick(self.meters, total_dict=self._last_total)
        except Exception as exc:
            log_once(logger, "runtime.logging", "exception", f"Runtime logging tick failed: {type(exc).__name__}: {exc}", cooldown_sec=5)

    def _tick_alarms(self) -> None:
        if self.alarm_engine is None:
            return
        try:
            for m in list(self.meters or []):
                self.alarm_engine.evaluate_meter(m)
        except Exception as exc:
            log_once(logger, "runtime.alarms", "exception", f"Runtime alarm evaluation failed: {type(exc).__name__}: {exc}", cooldown_sec=5)

    def _build_protection_snapshot(self, now: float) -> Dict[str, Any]:
        stale_sec = stale_seconds(self.cfg)
        meters_snap: Dict[int, Dict[str, Any]] = {}

        for m in list(self.meters or []):
            ts = getattr(m, "last_good_ts", None) or getattr(m, "last_update_ts", None) or now
            try:
                ts = float(ts)
            except Exception:
                ts = now
            enabled = bool(getattr(m, "enabled", True))
            raw_valid = bool(getattr(m, "data_valid", False))
            data_valid = enabled and raw_valid and (now - ts) <= stale_sec
            try:
                mid = int(getattr(m, "meter_id", 0) or 0)
            except Exception:
                continue
            if mid <= 0:
                continue
            meters_snap[mid] = {
                "values": dict(getattr(m, "values", {}) or {}),
                "data_valid": data_valid,
                "ts": ts,
                "enabled": enabled,
            }

        total_values = dict(self._last_total or self._compute_total() or {})
        total_valid = any(v.get("enabled") and v.get("data_valid") for v in meters_snap.values())
        return {
            "total": {"values": total_values, "data_valid": total_valid, "ts": now},
            "meters": meters_snap,
        }

    def _tick_protection(self, now: float) -> None:
        if self.protection_engine is None:
            return
        try:
            self.protection_engine.tick(self._build_protection_snapshot(now))
        except Exception as exc:
            log_once(logger, "runtime.protection", "exception", f"Runtime protection tick failed: {type(exc).__name__}: {exc}", cooldown_sec=5)

    def _tick_connectivity(self, now: float) -> None:
        if self.remote_sync is None and self.mqtt_publisher is None and self.rest_api is None:
            return

        values_by_source: Dict[str, Dict[str, Any]] = {}
        quality_map: Dict[str, str] = {}

        try:
            total = self._last_total if isinstance(self._last_total, dict) else None
            if total:
                values_by_source["TOTAL"] = dict(total)
                quality_map["TOTAL"] = str(total.get("quality", "GOOD") or "GOOD")

            for m in list(self.meters or []):
                if not bool(getattr(m, "enabled", True)):
                    continue
                try:
                    mid = f"M{int(getattr(m, 'meter_id', 0) or 0)}"
                except Exception:
                    continue
                if mid == "M0":
                    continue
                values_by_source[mid] = dict(getattr(m, "values", {}) or {})
                quality_map[mid] = str(getattr(m, "quality", "GOOD") or "GOOD")
        except Exception as exc:
            log_once(logger, "runtime.connectivity.build", "exception", f"Connectivity payload build failed: {type(exc).__name__}: {exc}", cooldown_sec=10)
            return

        if values_by_source and self.remote_sync is not None:
            try:
                self.remote_sync.publish_snapshot(values_by_source, quality_map, now)
            except Exception as exc:
                log_once(logger, "runtime.remote_sync", "exception", f"Remote sync error: {type(exc).__name__}: {exc}", cooldown_sec=10)

        elif values_by_source and self.mqtt_publisher is not None:
            try:
                self.mqtt_publisher.publish_snapshot(values_by_source, quality_map, now)
            except Exception as exc:
                log_once(logger, "runtime.mqtt_pub", "exception", f"MQTT publish error: {type(exc).__name__}: {exc}", cooldown_sec=10)

        if values_by_source and self.remote_sync is None and self.rest_api is not None:
            try:
                self.rest_api.update_snapshot(values_by_source, quality_map, now)
            except Exception as exc:
                log_once(logger, "runtime.rest_snap", "exception", f"REST snapshot error: {type(exc).__name__}: {exc}", cooldown_sec=10)

        try:
            worker_state = str(getattr(self.worker, "status", "UNKNOWN") or "UNKNOWN") if self.worker else "NO_WORKER"
            meter_count = len([m for m in list(self.meters or []) if bool(getattr(m, "enabled", True))])
            if self.remote_sync is not None:
                self.remote_sync.publish_app_status(worker_state, meter_count)
            elif self.rest_api is not None:
                self.rest_api.update_app_status(worker_state, meter_count)
            if self.remote_sync is None and self.mqtt_publisher is not None:
                self.mqtt_publisher.publish_app_status(worker_state, meter_count)
        except Exception as exc:
            log_once(logger, "runtime.connectivity.status", "exception", f"Connectivity status update failed: {type(exc).__name__}: {exc}", cooldown_sec=10)

    def _run(self) -> None:
        while not self._stop.is_set():
            now = time.time()
            if (now - self._last_total_ts) >= self.total_interval_s:
                self._last_total = self._compute_total()
                self._update_total_mini(self._last_total)
                self._last_total_ts = now

            self._tick_logging()

            if (now - self._last_alarm_ts) >= self.alarm_interval_s:
                self._tick_alarms()
                self._last_alarm_ts = now

            if (now - self._last_protection_ts) >= self.protection_interval_s:
                self._tick_protection(now)
                self._last_protection_ts = now

            if (now - self._last_connectivity_ts) >= self.connectivity_interval_s:
                self._tick_connectivity(now)
                self._last_connectivity_ts = now

            self._stop.wait(self.tick_interval_s)
