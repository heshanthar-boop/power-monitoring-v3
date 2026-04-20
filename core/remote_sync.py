from __future__ import annotations

import json
import math
import os
import threading
import time
from collections import deque
from typing import Any, Deque, Dict, Optional

from utils.logger import setup_logger
from utils.paths import db_dir

logger = setup_logger("remote_sync")


def _safe_float(value: Any, default: float) -> float:
    try:
        out = float(value)
        return out if math.isfinite(out) else default
    except Exception:
        return default


def _safe_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _sanitize_values(values: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for key, value in (values or {}).items():
        k = str(key or "").strip()
        if not k:
            continue
        if isinstance(value, bool):
            out[k] = value
        elif isinstance(value, int):
            out[k] = value
        elif isinstance(value, float):
            if math.isfinite(value):
                out[k] = round(value, 4)
        elif isinstance(value, str):
            out[k] = value[:200]
    return out


def _sanitize_payload(
    values_by_source: Dict[str, Dict[str, Any]],
    quality_map: Dict[str, str],
    ts: Optional[float],
) -> Dict[str, Any]:
    now = time.time() if ts is None else _safe_float(ts, time.time())
    values: Dict[str, Dict[str, Any]] = {}
    quality: Dict[str, str] = {}

    for source, source_values in (values_by_source or {}).items():
        src = str(source or "").strip()
        if not src:
            continue
        clean = _sanitize_values(source_values or {})
        if not clean:
            continue
        values[src] = clean
        quality[src] = str((quality_map or {}).get(source, "GOOD") or "GOOD")[:40]

    return {
        "ts": now,
        "queued_ts": time.time(),
        "attempts": 0,
        "values_by_source": values,
        "quality_map": quality,
    }


class RemoteSyncManager:
    """Offline-first remote monitoring bridge.

    REST is treated as a local, latest-value endpoint and is updated every tick.
    MQTT is treated as the cloud/remote stream and gets a bounded disk-backed
    queue so short internet or broker outages do not silently drop snapshots.
    """

    def __init__(
        self,
        cfg: Dict[str, Any],
        *,
        mqtt_publisher: Any = None,
        rest_api: Any = None,
        queue_path: Optional[str] = None,
    ) -> None:
        self._cfg = cfg
        self._mqtt = mqtt_publisher
        self._rest = rest_api
        self._lock = threading.RLock()

        self._max_queue = max(10, self._cfg_int("max_queue", 300))
        self._queue: Deque[Dict[str, Any]] = deque(maxlen=self._max_queue)
        self._queue_path = queue_path or os.path.join(db_dir(), "remote_sync_queue.jsonl")

        self._sent_count = 0
        self._replayed_count = 0
        self._failed_count = 0
        self._dropped_count = 0
        self._last_success_ts = 0.0
        self._last_failure_ts = 0.0
        self._last_error = ""
        self._last_rest_update_ts = 0.0
        self._next_live_mqtt_ts = 0.0
        self._next_replay_ts = 0.0

        self._load_queue()

    @property
    def enabled(self) -> bool:
        return bool((self._cfg.get("remote_sync") or {}).get("enabled", True))

    def reconfigure(self, cfg: Dict[str, Any]) -> None:
        with self._lock:
            self._cfg = cfg
            new_max = max(10, self._cfg_int("max_queue", self._max_queue))
            if new_max != self._max_queue:
                old = list(self._queue)[-new_max:]
                self._max_queue = new_max
                self._queue = deque(old, maxlen=new_max)
                self._persist_queue_locked()

    def set_mqtt_publisher(self, publisher: Any) -> None:
        with self._lock:
            self._mqtt = publisher

    def set_rest_api(self, api: Any) -> None:
        with self._lock:
            self._rest = api

    def close(self) -> None:
        with self._lock:
            self._persist_queue_locked()

    def clear_queue(self) -> None:
        with self._lock:
            self._queue.clear()
            self._persist_queue_locked()

    def publish_snapshot(
        self,
        values_by_source: Dict[str, Dict[str, Any]],
        quality_map: Dict[str, str],
        ts: Optional[float] = None,
    ) -> None:
        payload = _sanitize_payload(values_by_source, quality_map, ts)
        if not payload["values_by_source"]:
            return

        now = time.time()
        self._update_rest_snapshot(payload)

        if not self._mqtt_enabled():
            return

        if self.enabled:
            self._replay_queued(now)

        if not self._live_publish_due(now):
            return

        ok = self._publish_mqtt(payload, force=False)
        if ok:
            return

        if self.enabled:
            self._enqueue(payload)

    def publish_app_status(self, state_str: str, meter_count: int) -> None:
        rest = self._rest
        mqtt = self._mqtt
        try:
            if rest is not None:
                rest.update_app_status(state_str, meter_count)
        except Exception as exc:
            self._record_failure(f"REST status update failed: {exc}")

        if mqtt is None or not self._mqtt_enabled():
            return
        try:
            mqtt.publish_app_status(state_str, meter_count)
        except Exception as exc:
            self._record_failure(f"MQTT status publish failed: {exc}")

    def health(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "enabled": self.enabled,
                "queue_depth": len(self._queue),
                "max_queue": self._max_queue,
                "sent_count": self._sent_count,
                "replayed_count": self._replayed_count,
                "failed_count": self._failed_count,
                "dropped_count": self._dropped_count,
                "last_success_ts": self._last_success_ts,
                "last_failure_ts": self._last_failure_ts,
                "last_rest_update_ts": self._last_rest_update_ts,
                "last_error": self._last_error,
                "mqtt_enabled": self._mqtt_enabled(),
                "mqtt_state": self._mqtt_state(),
                "rest_state": self._rest_state(),
                "queue_path": self._queue_path,
            }

    def _update_rest_snapshot(self, payload: Dict[str, Any]) -> None:
        rest = self._rest
        if rest is None:
            return
        try:
            rest.update_snapshot(payload["values_by_source"], payload["quality_map"], payload["ts"])
            self._last_rest_update_ts = time.time()
        except Exception as exc:
            self._record_failure(f"REST snapshot update failed: {exc}")

    def _live_publish_due(self, now: float) -> bool:
        if now < self._next_live_mqtt_ts:
            return False
        interval = self._mqtt_interval_sec()
        self._next_live_mqtt_ts = now + interval
        return True

    def _replay_queued(self, now: float) -> None:
        if not self._queue or not self._mqtt_connected():
            return
        if now < self._next_replay_ts:
            return

        batch = max(1, self._cfg_int("replay_batch", 5))
        changed = False
        sent = 0
        while self._queue and sent < batch:
            payload = dict(self._queue[0])
            payload["attempts"] = _safe_int(payload.get("attempts", 0), 0) + 1
            ok = self._publish_mqtt(payload, force=True)
            if not ok:
                self._queue[0]["attempts"] = payload["attempts"]
                changed = True
                break
            self._queue.popleft()
            self._replayed_count += 1
            sent += 1
            changed = True

        self._next_replay_ts = now + max(0.2, self._cfg_float("min_replay_interval_sec", 1.0))
        if changed:
            with self._lock:
                self._persist_queue_locked()

    def _publish_mqtt(self, payload: Dict[str, Any], *, force: bool) -> bool:
        mqtt = self._mqtt
        if mqtt is None or not self._mqtt_enabled():
            return False
        try:
            try:
                ok = bool(mqtt.publish_snapshot(
                    payload["values_by_source"],
                    payload["quality_map"],
                    payload["ts"],
                    force=force,
                ))
            except TypeError:
                ok = bool(mqtt.publish_snapshot(
                    payload["values_by_source"],
                    payload["quality_map"],
                    payload["ts"],
                ))
            if ok:
                self._sent_count += 1
                self._last_success_ts = time.time()
                self._last_error = ""
                return True
            if not self._mqtt_connected():
                self._record_failure(str(getattr(mqtt, "last_error", "") or "MQTT not connected"))
            elif force:
                self._record_failure(str(getattr(mqtt, "last_error", "") or "MQTT replay publish skipped"))
            return False
        except Exception as exc:
            self._record_failure(f"MQTT publish failed: {exc}")
            return False

    def _enqueue(self, payload: Dict[str, Any]) -> None:
        with self._lock:
            item = dict(payload)
            item["queued_ts"] = time.time()
            item["attempts"] = _safe_int(item.get("attempts", 0), 0) + 1
            if len(self._queue) >= self._max_queue:
                self._queue.popleft()
                self._dropped_count += 1
            self._queue.append(item)
            self._persist_queue_locked()

    def _record_failure(self, message: str) -> None:
        with self._lock:
            self._failed_count += 1
            self._last_failure_ts = time.time()
            self._last_error = str(message or "")[:300]

    def _cfg_int(self, key: str, default: int) -> int:
        return _safe_int((self._cfg.get("remote_sync") or {}).get(key, default), default)

    def _cfg_float(self, key: str, default: float) -> float:
        return _safe_float((self._cfg.get("remote_sync") or {}).get(key, default), default)

    def _mqtt_interval_sec(self) -> float:
        remote = self._cfg.get("remote_sync") or {}
        mqtt_cfg = self._cfg.get("mqtt") or {}
        if "live_interval_sec" in remote:
            return max(1.0, _safe_float(remote.get("live_interval_sec"), 5.0))
        return max(1.0, _safe_float(mqtt_cfg.get("publish_interval_sec", 5.0), 5.0))

    def _mqtt_enabled(self) -> bool:
        mqtt = self._mqtt
        if mqtt is None:
            return False
        try:
            return bool(getattr(mqtt, "enabled", False))
        except Exception:
            return False

    def _mqtt_connected(self) -> bool:
        return self._mqtt_state() == "CONNECTED"

    def _mqtt_state(self) -> str:
        mqtt = self._mqtt
        if mqtt is None:
            return "UNCONFIGURED"
        try:
            return str(getattr(mqtt, "status", "UNKNOWN") or "UNKNOWN").upper()
        except Exception:
            return "UNKNOWN"

    def _rest_state(self) -> str:
        rest = self._rest
        if rest is None:
            return "UNCONFIGURED"
        try:
            if bool(getattr(rest, "is_running", False)):
                return "RUNNING"
            if bool(getattr(rest, "enabled", False)):
                return "FAILED"
            return "DISABLED"
        except Exception:
            return "UNKNOWN"

    def _load_queue(self) -> None:
        try:
            if not os.path.exists(self._queue_path):
                return
            with open(self._queue_path, "r", encoding="utf-8") as fh:
                lines = fh.readlines()[-self._max_queue:]
            loaded = 0
            for line in lines:
                try:
                    item = json.loads(line)
                    if isinstance(item, dict) and isinstance(item.get("values_by_source"), dict):
                        self._queue.append(item)
                        loaded += 1
                except Exception:
                    continue
            if loaded:
                logger.info("Loaded %d queued remote-sync snapshots", loaded)
        except Exception as exc:
            logger.warning("Could not load remote-sync queue: %s", exc)

    def _persist_queue_locked(self) -> None:
        try:
            os.makedirs(os.path.dirname(self._queue_path), exist_ok=True)
            tmp = f"{self._queue_path}.tmp"
            with open(tmp, "w", encoding="utf-8") as fh:
                for item in self._queue:
                    fh.write(json.dumps(item, separators=(",", ":"), ensure_ascii=True))
                    fh.write("\n")
            os.replace(tmp, self._queue_path)
        except Exception as exc:
            logger.warning("Could not persist remote-sync queue: %s", exc)
