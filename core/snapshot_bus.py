from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import threading
from queue import Empty, Full, Queue
from typing import Any, Dict, List, Optional

from utils.logger import setup_logger
from utils.log_once import log_once

logger = setup_logger("snapshot_bus")


class MeterQuality(str, Enum):
    GOOD = "GOOD"
    STALE = "STALE"
    COMM_LOST = "COMM_LOST"
    DISABLED = "DISABLED"


@dataclass
class MeterSnapshot:
    meter_id: int
    ts: float
    values: Dict[str, Any]
    quality: MeterQuality = MeterQuality.GOOD
    error: Optional[str] = None
    raw_regs: Optional[Any] = None
    seq: int = 0
    monotonic_ts: float = 0.0
    quality_reason: str = ""


class SnapshotBus:
    """Fan-out bus for MeterSnapshot -> UI.

    Control-room rule: the polling thread must never block because the UI is
    slow. Therefore each subscriber gets a bounded queue, and when full we
    drop *old* items so the UI can catch up to the latest state.
    """

    def __init__(self, max_queue: int = 512):
        self._queues: List[Queue[MeterSnapshot]] = []
        self._lock = threading.Lock()
        self._published_count = 0
        self._dropped_count = 0

    def subscribe(self, max_queue: int = 512) -> Queue[MeterSnapshot]:
        q: Queue[MeterSnapshot] = Queue(maxsize=max_queue)
        with self._lock:
            self._queues.append(q)
        return q

    def unsubscribe(self, queue_obj: Queue[MeterSnapshot]) -> None:
        with self._lock:
            if queue_obj in self._queues:
                self._queues.remove(queue_obj)

    def publish(self, snapshot: MeterSnapshot) -> None:
        with self._lock:
            queues = list(self._queues)
            self._published_count += 1

        for q in queues:
            # If the UI falls behind, shed backlog so it can "jump" to latest.
            # (Never block the poller.)
            maxsize = getattr(q, "maxsize", 0) or 0
            if maxsize:
                try:
                    # Drop until queue is <= 70% full (keeps UI snappy).
                    while q.qsize() > int(maxsize * 0.70):
                        _ = q.get_nowait()
                        with self._lock:
                            self._dropped_count += 1
                except Empty:
                    pass

            try:
                q.put_nowait(snapshot)
            except Full:
                # Drop one old item then try once more.
                try:
                    _ = q.get_nowait()
                    with self._lock:
                        self._dropped_count += 1
                except Empty:
                    return
                try:
                    q.put_nowait(snapshot)
                except Full:
                    # Still full (extreme lag) -> drop this one.
                    pass

                # Prevent silent drops (classic SCADA "stale" mystery)
                log_once(logger, "snapshot_drop", "warning", "SnapshotBus queue full: dropping snapshots (UI lag / polling too fast).")

    def stats(self) -> Dict[str, int]:
        with self._lock:
            return {
                "subscribers": len(self._queues),
                "published_count": int(self._published_count),
                "dropped_count": int(self._dropped_count),
            }
