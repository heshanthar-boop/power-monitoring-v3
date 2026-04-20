"""
core/firebase_publisher.py
==========================
Subscribes to SnapshotBus and pushes live meter readings to Firebase Firestore.

Architecture:
  - Runs in its own daemon thread — never blocks the Modbus poller
  - Throttles writes: at most 1 Firestore write per meter per PUSH_INTERVAL seconds
  - Writes to:
      sites/{site_id}/meters/{meter_id}          <- latest live reading (overwritten)
      sites/{site_id}/history/{meter_id}/{ts}    <- historical log (appended, optional)
  - Graceful degradation: if Firebase is unavailable, logs warning and continues

Setup:
  1. In Firebase console -> Project Settings -> Service accounts
     -> "Generate new private key" -> save as firebase_key.json
     -> place firebase_key.json next to app.py  (or set FIREBASE_KEY_PATH env var)
  2. pip install firebase-admin
  3. Set firebase.enabled = true in config, firebase.site_id = "your_site_name"
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from queue import Empty, Queue
from typing import Any, Dict, List, Optional

from core.snapshot_bus import MeterQuality, MeterSnapshot, SnapshotBus

log = logging.getLogger("firebase_publisher")

# Default push interval — one Firestore write per meter every N seconds.
# Spark free plan: 20k writes/day. At 30s interval, 6 meters = 17280/day. Safe.
DEFAULT_PUSH_INTERVAL = 30


def _snapshot_to_doc(snap: MeterSnapshot, meter_name: str = "") -> Dict[str, Any]:
    """Convert a MeterSnapshot into a Firestore-safe dict."""
    doc: Dict[str, Any] = {
        "meter_id":   snap.meter_id,
        "meter_name": meter_name,
        "ts":         snap.ts,
        "ts_iso":     time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(snap.ts)),
        "quality":    snap.quality.value if hasattr(snap.quality, "value") else str(snap.quality),
        "error":      snap.error or "",
    }

    # Flatten values — keep only JSON-serialisable scalars
    for k, v in (snap.values or {}).items():
        if isinstance(v, (int, float, bool, str)) and v == v:  # exclude NaN
            doc[k] = round(float(v), 4) if isinstance(v, float) else v

    return doc


class FirebasePublisher:
    """
    Subscribes to a SnapshotBus and pushes meter readings to Firestore.

    Parameters
    ----------
    bus         : SnapshotBus to subscribe to
    site_id     : Firestore document ID for this site (e.g. "plant_01")
    key_path    : Path to firebase_key.json service account file
    push_interval : Seconds between Firestore writes per meter (default 30)
    enable_history: Also write to history sub-collection (uses more quota)
    meter_names : Dict[meter_id -> name] for display labels
    """

    def __init__(
        self,
        bus: SnapshotBus,
        site_id: str,
        key_path: str,
        push_interval: int = DEFAULT_PUSH_INTERVAL,
        enable_history: bool = False,
        meter_names: Optional[Dict[int, str]] = None,
    ):
        self._bus = bus
        self._site_id = site_id
        self._key_path = key_path
        self._push_interval = max(10, push_interval)
        self._enable_history = enable_history
        self._meter_names = meter_names or {}

        self._queue: Queue[MeterSnapshot] = bus.subscribe(max_queue=256)
        self._stop_evt = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._db = None  # firebase_admin Firestore client
        self._status = "DISABLED"
        self._last_error = ""

        # Throttle: track last push time per meter_id
        self._last_push: Dict[int, float] = {}

        # Site info shown on web dashboard
        self._plant_name: str = ""
        self._location:   str = ""

    @property
    def status(self) -> str:
        return self._status

    @property
    def last_error(self) -> str:
        return self._last_error

    @property
    def queue_depth(self) -> int:
        try:
            return int(self._queue.qsize())
        except Exception:
            return 0

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> bool:
        """Initialize Firebase and start background thread. Returns True on success."""
        if not self._init_firebase():
            self._status = "ERROR"
            return False
        self._thread = threading.Thread(
            target=self._run, name="firebase-publisher", daemon=True
        )
        self._thread.start()
        self._status = "RUNNING"
        self._last_error = ""
        log.info("FirebasePublisher started — site_id=%s, interval=%ds", self._site_id, self._push_interval)
        # Immediate heartbeat so dashboard shows site online before first meter push
        self._push_site_online()
        return True

    def stop(self) -> None:
        self._stop_evt.set()
        if self._thread:
            self._thread.join(timeout=5)
        self._status = "DISABLED"

    # ------------------------------------------------------------------
    # Firebase init
    # ------------------------------------------------------------------

    def _init_firebase(self) -> bool:
        try:
            import firebase_admin
            from firebase_admin import credentials, firestore as fb_firestore
        except ImportError:
            self._last_error = "firebase-admin not installed"
            log.warning(
                "firebase-admin not installed. Run: pip install firebase-admin\n"
                "Firebase cloud sync disabled."
            )
            return False

        key_path = self._key_path
        if not os.path.isfile(key_path):
            self._last_error = f"Firebase key file not found: {key_path}"
            log.warning(
                "Firebase key file not found: %s\n"
                "Firebase cloud sync disabled. Generate a service account key from\n"
                "Firebase console -> Project Settings -> Service accounts.", key_path
            )
            return False

        try:
            # Avoid re-initializing if already done (e.g. hot-reload)
            app_name = f"mfm384_{self._site_id}"
            try:
                app = firebase_admin.get_app(app_name)
            except ValueError:
                cred = credentials.Certificate(key_path)
                app = firebase_admin.initialize_app(cred, name=app_name)

            self._db = fb_firestore.client(app=app)
            self._last_error = ""
            log.info("Firebase Firestore connected (site=%s)", self._site_id)
            return True

        except Exception as e:
            self._last_error = str(e)
            log.error("Firebase init failed: %s", e, exc_info=True)
            return False

    # ------------------------------------------------------------------
    # Background thread
    # ------------------------------------------------------------------

    def _run(self) -> None:
        """Drain the snapshot queue and push to Firestore with throttling."""
        # Latest snapshot per meter — we only push the freshest
        pending: Dict[int, MeterSnapshot] = {}

        while not self._stop_evt.is_set():
            # Drain queue into pending dict (latest per meter wins)
            deadline = time.monotonic() + 1.0
            while time.monotonic() < deadline:
                try:
                    snap = self._queue.get_nowait()
                    pending[snap.meter_id] = snap
                except Empty:
                    break

            now = time.time()
            to_push: List[MeterSnapshot] = []

            for meter_id, snap in list(pending.items()):
                last = self._last_push.get(meter_id, 0)
                if (now - last) >= self._push_interval:
                    to_push.append(snap)
                    self._last_push[meter_id] = now
                    del pending[meter_id]

            for snap in to_push:
                self._push(snap)

            # Also push a site-level summary (total/aggregate) every interval
            if to_push:
                self._push_site_online()

            time.sleep(1)

    def _push(self, snap: MeterSnapshot) -> None:
        """Write one meter snapshot to Firestore."""
        if not self._db:
            return
        try:
            name = self._meter_names.get(snap.meter_id, f"Meter {snap.meter_id}")
            doc = _snapshot_to_doc(snap, name)

            # Live reading — overwrite
            ref = self._db.collection("sites").document(self._site_id) \
                          .collection("meters").document(str(snap.meter_id))
            ref.set(doc)

            # Optional history
            if self._enable_history and snap.quality == MeterQuality.GOOD:
                ts_key = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime(snap.ts))
                hist_ref = self._db.collection("sites").document(self._site_id) \
                                   .collection("history").document(str(snap.meter_id)) \
                                   .collection("readings").document(ts_key)
                hist_ref.set(doc)

            log.debug("Pushed meter %d (%s) to Firestore", snap.meter_id, snap.quality)
            self._status = "RUNNING"
            self._last_error = ""

        except Exception as e:
            self._status = "ERROR"
            self._last_error = str(e)
            log.warning("Firestore push failed (meter %d): %s", snap.meter_id, e)

    def _push_site_online(self) -> None:
        """Mark site as online with a heartbeat timestamp + site info."""
        if not self._db:
            return
        try:
            doc = {
                "online":        True,
                "last_seen":     time.time(),
                "last_seen_iso": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "site_id":       self._site_id,
            }
            # Include site info if set
            if self._plant_name:
                doc["plant_name"] = self._plant_name
            if self._location:
                doc["location"] = self._location
            self._db.collection("sites").document(self._site_id).set(doc, merge=True)
        except Exception as e:
            self._last_error = str(e)
            log.debug("Site heartbeat failed: %s", e)

    def push_site_info(self, plant_name: str = "", location: str = "") -> None:
        """Called from UI when operator updates Plant Name / Location.
        Updates immediately without waiting for next heartbeat cycle.
        """
        self._plant_name = plant_name
        self._location   = location
        self._push_site_online()
