import time
import threading
import sqlite3
import os
import sys
from typing import Optional

from core.snapshot_bus import MeterQuality
from utils.logger import setup_logger
from utils.paths import events_db_path

# ── Audible alarm (Windows winsound; graceful no-op on other platforms) ────────
def _beep_alarm() -> None:
    """Play a short double-beep for ALARM-severity events.

    Non-blocking: runs in a fire-and-forget daemon thread so it never stalls
    the alarm evaluation path. Safe to call even when winsound is unavailable.
    """
    if sys.platform != "win32":
        return
    def _play():
        try:
            import winsound
            # Two 880 Hz beeps, 150 ms each with 80 ms gap — distinct from
            # Windows system sounds, clearly audible in a noisy panel room.
            winsound.Beep(880, 150)
            time.sleep(0.08)
            winsound.Beep(880, 150)
        except Exception:
            pass
    t = threading.Thread(target=_play, daemon=True, name="alarm-beep")
    t.start()

logger = setup_logger("alarm_engine")

# ── Persistent active-alarm store ────────────────────────────────────────────
# Active alarms are written to a lightweight SQLite table so they survive
# process restarts. On startup, previously active alarms are reloaded into
# memory and re-evaluated against fresh data before being cleared or raised.
#
# Schema: alarm_state (meter_id TEXT, code TEXT, message TEXT, severity TEXT,
#                      ts REAL, acknowledged INTEGER, acked_at REAL,
#                      acked_by TEXT, PRIMARY KEY (meter_id, code))
#
# Writer thread: any thread that calls _raise / _clear (protected by RLock).
# Reader: startup only (single-threaded at that point).

def _alarm_state_db_path() -> str:
    """Co-locate with the events DB so backup/restore covers both."""
    base = os.path.dirname(events_db_path())
    return os.path.join(base, "alarm_state.db")


class _AlarmStateDB:
    """Minimal write-through SQLite store for active alarms."""

    _CREATE = """
    CREATE TABLE IF NOT EXISTS alarm_state (
        meter_id    TEXT NOT NULL,
        code        TEXT NOT NULL,
        message     TEXT NOT NULL DEFAULT '',
        severity    TEXT NOT NULL DEFAULT 'WARN',
        ts          REAL NOT NULL,
        acknowledged INTEGER NOT NULL DEFAULT 0,
        acked_at    REAL NOT NULL DEFAULT 0.0,
        acked_by    TEXT NOT NULL DEFAULT '',
        PRIMARY KEY (meter_id, code)
    )"""

    def __init__(self, path: str) -> None:
        self._path = path
        self._conn: Optional[sqlite3.Connection] = None
        self._init()

    def _conn_get(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = sqlite3.connect(self._path, check_same_thread=False, timeout=5)
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=NORMAL")
        return self._conn

    def _init(self) -> None:
        try:
            os.makedirs(os.path.dirname(self._path) or ".", exist_ok=True)
            c = self._conn_get()
            c.execute(self._CREATE)
            c.commit()
        except Exception as exc:
            logger.warning("[ALARM-DB] init failed: %s", exc)

    def upsert(self, ev: "AlarmEvent") -> None:
        try:
            c = self._conn_get()
            c.execute(
                "INSERT OR REPLACE INTO alarm_state "
                "(meter_id, code, message, severity, ts, acknowledged, acked_at, acked_by) "
                "VALUES (?,?,?,?,?,?,?,?)",
                (
                    str(ev.meter_id), str(ev.code), str(ev.message),
                    str(ev.severity), float(ev.ts),
                    1 if ev.acknowledged else 0,
                    float(ev.acknowledged_at or 0.0),
                    str(ev.acknowledged_by or ""),
                ),
            )
            c.commit()
        except Exception as exc:
            logger.warning("[ALARM-DB] upsert failed: %s", exc)

    def delete(self, meter_id, code: str) -> None:
        try:
            c = self._conn_get()
            c.execute("DELETE FROM alarm_state WHERE meter_id=? AND code=?",
                      (str(meter_id), str(code)))
            c.commit()
        except Exception as exc:
            logger.warning("[ALARM-DB] delete failed: %s", exc)

    def load_all(self) -> list:
        """Return list of dicts for all stored active alarms."""
        try:
            c = self._conn_get()
            c.row_factory = sqlite3.Row
            rows = c.execute(
                "SELECT meter_id, code, message, severity, ts, "
                "acknowledged, acked_at, acked_by FROM alarm_state"
            ).fetchall()
            c.row_factory = None
            return [dict(r) for r in rows]
        except Exception as exc:
            logger.warning("[ALARM-DB] load failed: %s", exc)
            return []

    def close(self) -> None:
        if self._conn:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None


class AlarmEvent:
    def __init__(self, meter_id, code, message, severity="WARN"):
        self.ts = time.time()
        self.meter_id = meter_id  # int or "TOTAL"
        self.code = code
        self.message = message
        self.severity = severity
        self.active = True
        # ACK workflow (IEC 62443 / IEC 61511 style)
        self.acknowledged = False
        self.acknowledged_at: float = 0.0
        self.acknowledged_by: str = ""
        # First-out: True when this alarm is the first in a burst window.
        # Helps operators identify root cause vs. consequence alarms.
        self.is_first_out: bool = False


class AlarmEngine:
    def __init__(self, startup_inhibit_sec=10, stale_sec=10, conn_loss_sec=15, event_journal=None):
        self.start_ts = time.time()
        self.startup_inhibit_sec = float(startup_inhibit_sec)
        self.stale_sec = float(stale_sec)
        self.conn_loss_sec = float(conn_loss_sec)

        self.active = {}   # (meter_id, code) -> AlarmEvent
        self.history = []  # list[AlarmEvent]
        self.event_journal = event_journal
        self._lock = threading.RLock()
        self._cfg: dict = {}  # set via set_cfg() after construction

        # Optional MQTT publisher for remote alarm event streaming.
        # Set via set_mqtt_publisher() after construction (avoids circular imports).
        self._mqtt_publisher = None

        # Rate-limit audible beep: no more than once per 10 s regardless of
        # how many ALARM events fire simultaneously.
        self._last_beep_ts: float = 0.0
        self._BEEP_COOLDOWN_SEC = 10.0

        # First-out tracking: alarms raised within _BURST_WINDOW_SEC of the
        # very first alarm in a quiet period are tagged is_first_out=True.
        # A "quiet period" is any time the active alarm set was empty.
        # The first alarm after quiet always gets the tag; subsequent alarms
        # raised within the burst window also get it (cascade identification).
        self._first_out_ts: float = 0.0          # ts of first alarm after quiet
        self._BURST_WINDOW_SEC: float = 5.0      # alarms within 5 s = same burst

        # Persistent active-alarm store (survives restarts)
        try:
            self._state_db = _AlarmStateDB(_alarm_state_db_path())
            self._restore_from_db()
        except Exception as exc:
            logger.warning("[ALARM] persistent state DB unavailable: %s", exc)
            self._state_db = None

        # ── Alarm shelving (maintenance suppression) ──────────────────────────
        # Maps (meter_id, code) → expiry_epoch (float) or None (indefinite).
        # A shelved alarm code is suppressed from raising during the shelf window.
        # Shelves are in-memory only — they reset on process restart (intentional:
        # an unattended restart must re-evaluate all alarms fresh).
        # Wildcard: (meter_id, "*") suppresses ALL codes for that meter.
        #           ("*", "*")       suppresses ALL alarms system-wide.
        self._shelves: dict = {}   # (meter_id, code) → expiry_epoch | None
        self._shelf_lock = threading.Lock()

        # ── SMS / WhatsApp alert service ──────────────────────────────────────
        # Set via set_sms_service() after construction.
        self._sms_service = None

    def set_cfg(self, cfg: dict) -> None:
        """Provide config dict so the engine can read alarm preferences (e.g. beep_critical)."""
        self._cfg = cfg if isinstance(cfg, dict) else {}

    def set_mqtt_publisher(self, publisher) -> None:
        """Attach an MQTTPublisher so alarm events are streamed remotely.
        Call after both objects are constructed (avoids circular import)."""
        self._mqtt_publisher = publisher

    def set_sms_service(self, sms_service) -> None:
        """Attach an SMSAlertService so ALARM events trigger SMS/WhatsApp.
        Call after both objects are constructed (avoids circular import)."""
        self._sms_service = sms_service

    def _publish_alarm_mqtt(self, meter_id, code: str, event: str, severity: str, message: str, ts: float = 0.0) -> None:
        """Fire-and-forget MQTT alarm event — never blocks alarm evaluation."""
        pub = self._mqtt_publisher
        if pub is None:
            return
        try:
            pub.publish_alarm_event(
                meter_id=meter_id,
                code=code,
                event=event,
                severity=severity,
                message=message,
                ts=ts or time.time(),
            )
        except Exception:
            pass  # MQTT failures must never affect local alarm state

    def _maybe_beep(self, severity: str) -> None:
        """Trigger audible alert for ALARM-grade events if beep_critical is enabled."""
        if str(severity or "").upper() != "ALARM":
            return
        if not bool((self._cfg.get("alarms") or {}).get("beep_critical", False)):
            return
        now = time.time()
        if (now - self._last_beep_ts) < self._BEEP_COOLDOWN_SEC:
            return
        self._last_beep_ts = now
        _beep_alarm()

    def _restore_from_db(self) -> None:
        """Reload previously active alarms from the DB on startup.

        These are marked RESTORED so the alarm list is immediately populated
        for the operator. The startup inhibit still applies to *new* evaluations,
        but pre-existing alarms (e.g. comm loss before restart) are visible right away.
        """
        if self._state_db is None:
            return
        rows = self._state_db.load_all()
        restored = 0
        for r in rows:
            try:
                ev = AlarmEvent(
                    meter_id=r["meter_id"],
                    code=r["code"],
                    message=r["message"],
                    severity=r["severity"],
                )
                ev.ts = float(r["ts"])
                ev.acknowledged = bool(r["acknowledged"])
                ev.acknowledged_at = float(r["acked_at"] or 0.0)
                ev.acknowledged_by = str(r["acked_by"] or "")
                k = (ev.meter_id, ev.code)
                with self._lock:
                    self.active[k] = ev
                    self.history.append(ev)
                restored += 1
            except Exception as exc:
                logger.warning("[ALARM] restore row failed: %s", exc)
        if restored:
            logger.info("[ALARM] restored %d active alarm(s) from persistent state", restored)

    def startup_inhibit(self):
        return (time.time() - self.start_ts) < self.startup_inhibit_sec

    def _mini_snapshot(self, meter) -> dict:
        """Small, stable snapshot for journaling (no business logic impact)."""
        try:
            q = str(getattr(meter, "quality", ""))
        except Exception:
            q = ""
        v = getattr(meter, "values", {}) or {}
        return {
            "quality":    q,
            "kW":         v.get("kW"),
            "Vavg":       v.get("Vavg"),
            "V1N":        v.get("V1N"),
            "V2N":        v.get("V2N"),
            "V3N":        v.get("V3N"),
            "Iavg":       v.get("Iavg"),
            "PFavg":      v.get("PFavg"),
            "Frequency":  v.get("Frequency"),
        }

    def _is_shelved(self, meter_id, code: str) -> bool:
        """
        Return True if this (meter_id, code) pair is currently shelved.

        Checks four keys in order (most specific → most general):
          (meter_id, code)  — exact shelf for this alarm
          (meter_id, "*")   — all alarms for this meter are shelved
          ("*", code)       — this code is shelved across all meters
          ("*", "*")        — system-wide maintenance mode
        Expired shelves are cleaned up lazily on check.
        """
        now = time.time()
        with self._shelf_lock:
            for key in ((meter_id, code), (meter_id, "*"), ("*", code), ("*", "*")):
                expiry = self._shelves.get(key)
                if expiry is None:
                    if key in self._shelves:
                        return True    # indefinite shelf
                elif expiry > now:
                    return True        # active timed shelf
                elif key in self._shelves:
                    # Expired — clean up lazily
                    del self._shelves[key]
        return False

    def _raise(self, meter_id, code, msg, severity="WARN", snapshot=None):
        # Suppress if this alarm is currently shelved (maintenance window)
        if self._is_shelved(meter_id, code):
            return
        with self._lock:
            k = (meter_id, code)
            if k in self.active:
                return
            ev = AlarmEvent(meter_id, code, msg, severity)
            now = ev.ts
            # First-out detection:
            # If the active set was empty before this raise, this is the
            # first alarm after a quiet period — tag it as first-out and
            # record the burst start time.
            if not self.active:
                ev.is_first_out = True
                self._first_out_ts = now
            elif (now - self._first_out_ts) <= self._BURST_WINDOW_SEC:
                # Still within the burst window — also mark as first-out
                # so operators see the whole initial-burst cluster.
                ev.is_first_out = True
            self.active[k] = ev
            self.history.append(ev)
        # Persist BEFORE journaling so the DB is consistent even if journal write fails
        if self._state_db is not None:
            try:
                self._state_db.upsert(ev)
            except Exception:
                logger.warning("alarm state DB upsert failed for RAISE %s/%s", meter_id, code, exc_info=True)
        # Audible alert (ALARM severity only, rate-limited)
        self._maybe_beep(severity)
        if self.event_journal:
            try:
                self.event_journal.log_alarm(meter_id, code, "RAISE", severity, msg, snapshot=snapshot)
            except Exception:
                logger.warning("journal write failed for RAISE %s/%s", meter_id, code, exc_info=True)
        # Remote: publish alarm RAISE event to MQTT broker
        self._publish_alarm_mqtt(meter_id, code, "RAISE", severity, msg, ts=ev.ts)
        # SMS / WhatsApp alert (ALARM severity, fire-and-forget, rate-limited)
        self._send_sms_alert(meter_id, code, msg, severity, event="RAISE")

    def _clear(self, meter_id, code, snapshot=None):
        with self._lock:
            k = (meter_id, code)
            ev = self.active.get(k)
            if ev:
                ev.active = False
                self.active.pop(k, None)
        # Remove from DB (alarm is no longer active)
        if ev and self._state_db is not None:
            try:
                self._state_db.delete(meter_id, code)
            except Exception:
                logger.warning("alarm state DB delete failed for CLEAR %s/%s", meter_id, code, exc_info=True)
        if ev and self.event_journal:
            try:
                self.event_journal.log_alarm(meter_id, code, "CLEAR", ev.severity, ev.message, snapshot=snapshot)
            except Exception:
                logger.warning("journal write failed for CLEAR %s/%s", meter_id, code, exc_info=True)
        # Remote: publish alarm CLEAR event to MQTT broker
        if ev:
            self._publish_alarm_mqtt(meter_id, code, "CLEAR", ev.severity, ev.message)
            # SMS clear notification (only if notify_clear=True in config)
            self._send_sms_alert(meter_id, code, ev.message, ev.severity, event="CLEAR")

    def _send_sms_alert(self, meter_id, code: str, message: str,
                        severity: str, event: str = "RAISE") -> None:
        """Fire-and-forget SMS alert via the attached SMSAlertService."""
        svc = self._sms_service
        if svc is None:
            return
        # Resolve a human-readable meter name from the active event if available
        meter_name = str(meter_id)
        with self._lock:
            ev = self.active.get((meter_id, code))
            if ev is None and event == "CLEAR":
                # meter_id from a just-cleared event — name still useful as string
                pass
        try:
            svc.send_alarm_alert(
                meter_id=str(meter_id),
                meter_name=meter_name,
                code=code,
                message=message,
                severity=severity,
                event=event,
            )
        except Exception:
            pass  # SMS failures must never affect local alarm state

    def _clear_all_for_meter(self, meter_id):
        with self._lock:
            keys = list(self.active.keys())
        for (mid, code) in keys:
            if mid == meter_id:
                self._clear(mid, code)

    def _clear_non_quality_for_meter(self, meter_id):
        # When data is STALE/OFFLINE, suppress process alarms (only show quality alarms).
        keep = {"CONN_LOSS", "STALE"}
        with self._lock:
            keys = list(self.active.keys())
        for (mid, code) in keys:
            if mid == meter_id and code not in keep:
                self._clear(mid, code)

    def evaluate_meter(self, meter):
        if self.startup_inhibit():
            return

        if not getattr(meter, "enabled", True):
            self._clear_all_for_meter(meter.meter_id)
            return

        if not getattr(meter, "first_data_received", False):
            return

        age = meter.age() if hasattr(meter, "age") else None
        quality = getattr(meter, "quality", MeterQuality.DISABLED)

        # When data is invalid, suppress process alarms and show only comm/quality alarms.
        if quality != MeterQuality.GOOD:
            self._clear_non_quality_for_meter(meter.meter_id)
            snap = self._mini_snapshot(meter)

            if quality == MeterQuality.COMM_LOST:
                self._clear(meter.meter_id, "STALE", snapshot=snap)
                self._raise(meter.meter_id, "CONN_LOSS", f"{meter.name}: Communication lost", "ALARM", snapshot=snap)
                return

            if quality == MeterQuality.STALE:
                self._clear(meter.meter_id, "CONN_LOSS", snapshot=snap)
                age_text = f" ({age:.1f}s ago)" if age is not None else ""
                self._raise(meter.meter_id, "STALE", f"{meter.name}: Data stale{age_text}", "WARN", snapshot=snap)
                return

            # DISABLED / other: clear quality alarms too.
            self._clear(meter.meter_id, "CONN_LOSS", snapshot=snap)
            self._clear(meter.meter_id, "STALE", snapshot=snap)
            return

        # GOOD data: clear comm/quality alarms and evaluate process alarms.
        snap = self._mini_snapshot(meter)
        self._clear(meter.meter_id, "CONN_LOSS", snapshot=snap)
        self._clear(meter.meter_id, "STALE", snapshot=snap)

        v = (getattr(meter, "values", {}) or {})
        self._eval_process_alarms(meter, v, snap)

    # ── Process alarm evaluation ──────────────────────────────────────────────

    def _analytics_cfg(self) -> dict:
        """Return analytics sub-config, safe empty dict if missing."""
        return (self._cfg.get("analytics") or {})

    def _eval_process_alarms(self, meter, v: dict, snap: dict) -> None:
        """
        Evaluate all configurable process alarms for a meter with GOOD data.

        Rules implemented (all thresholds sourced from cfg.analytics):
        ─────────────────────────────────────────────────────────────────
        NEG_KW     WARN  — negative active power (CT direction / sign error)
        UV         ALARM — Vavg below voltage_ln_min_v (undervoltage)
        OV         ALARM — Vavg above voltage_ln_max_v (overvoltage)
        FREQ_LOW   ALARM — grid frequency below freq_min_hz
        FREQ_HIGH  ALARM — grid frequency above freq_max_hz
        PF_LOW     WARN  — PFavg below pf_warn threshold
        V_UNBAL    WARN  — voltage unbalance above voltage_unbalance_warn_pct
                   ALARM — voltage unbalance above voltage_unbalance_alarm_pct

        All rules are individually suppressible by setting the threshold to 0.
        A threshold of 0 disables that specific check (safe default for sites
        where the measurement register is not available).
        """
        a = self._analytics_cfg()
        name = meter.name
        mid  = meter.meter_id

        # ── Negative kW ───────────────────────────────────────────────────────
        kw = v.get("kW")
        if isinstance(kw, (int, float)) and kw < 0:
            self._raise(mid, "NEG_KW",
                        f"{name}: kW negative ({kw:.2f} kW) — check CT direction",
                        "WARN", snapshot=snap)
        else:
            self._clear(mid, "NEG_KW", snapshot=snap)

        # ── Undervoltage / Overvoltage ────────────────────────────────────────
        # Use Vavg (phase-to-neutral average). If not available, skip.
        vavg = v.get("Vavg")
        if isinstance(vavg, (int, float)) and vavg > 0:
            vmin = float(a.get("voltage_ln_min_v") or 0.0)
            vmax = float(a.get("voltage_ln_max_v") or 0.0)

            if vmin > 0 and vavg < vmin:
                self._raise(mid, "UV",
                            f"{name}: Undervoltage — Vavg={vavg:.1f} V (limit {vmin:.0f} V)",
                            "ALARM", snapshot=snap)
            else:
                self._clear(mid, "UV", snapshot=snap)

            if vmax > 0 and vavg > vmax:
                self._raise(mid, "OV",
                            f"{name}: Overvoltage — Vavg={vavg:.1f} V (limit {vmax:.0f} V)",
                            "ALARM", snapshot=snap)
            else:
                self._clear(mid, "OV", snapshot=snap)
        else:
            # Register not present for this meter — keep any existing alarm state
            # rather than spuriously clearing it.
            pass

        # ── Frequency ─────────────────────────────────────────────────────────
        # MFM384 key_registry canonical: "Frequency"
        freq = v.get("Frequency") or v.get("freq") or v.get("Freq") or v.get("Hz")
        if isinstance(freq, (int, float)) and freq > 0:
            fmin = float(a.get("freq_min_hz") or 0.0)
            fmax = float(a.get("freq_max_hz") or 0.0)

            if fmin > 0 and freq < fmin:
                self._raise(mid, "FREQ_LOW",
                            f"{name}: Frequency low — {freq:.3f} Hz (limit {fmin:.1f} Hz)",
                            "ALARM", snapshot=snap)
            else:
                self._clear(mid, "FREQ_LOW", snapshot=snap)

            if fmax > 0 and freq > fmax:
                self._raise(mid, "FREQ_HIGH",
                            f"{name}: Frequency high — {freq:.3f} Hz (limit {fmax:.1f} Hz)",
                            "ALARM", snapshot=snap)
            else:
                self._clear(mid, "FREQ_HIGH", snapshot=snap)
        else:
            pass  # register not available — preserve existing state

        # ── Power Factor ──────────────────────────────────────────────────────
        # Use PFavg if available; fall back to PF.
        pf = v.get("PFavg") or v.get("PF")
        pf_warn = float(a.get("pf_warn") or 0.0)
        if pf_warn > 0 and isinstance(pf, (int, float)):
            # PF is typically reported as absolute value (0–1) by MFM384.
            # Negative PF can mean leading — compare magnitude.
            pf_abs = abs(pf)
            if pf_abs > 0.01 and pf_abs < pf_warn:
                self._raise(mid, "PF_LOW",
                            f"{name}: Power factor low — PF={pf_abs:.3f} (limit {pf_warn:.2f})",
                            "WARN", snapshot=snap)
            else:
                self._clear(mid, "PF_LOW", snapshot=snap)

        # ── Voltage Unbalance ─────────────────────────────────────────────────
        # Compute from V1N, V2N, V3N if available (NEMA definition: max
        # deviation from average / average × 100).
        # MFM384 canonical keys: V1N, V2N, V3N
        va = v.get("V1N") or v.get("Va") or v.get("V1")
        vb = v.get("V2N") or v.get("Vb") or v.get("V2")
        vc = v.get("V3N") or v.get("Vc") or v.get("V3")
        if all(isinstance(x, (int, float)) and x > 0 for x in (va, vb, vc)):
            avg3 = (va + vb + vc) / 3.0
            if avg3 > 0:
                unbal_pct = max(abs(va - avg3), abs(vb - avg3), abs(vc - avg3)) / avg3 * 100.0
                warn_pct  = float(a.get("voltage_unbalance_warn_pct")  or 0.0)
                alarm_pct = float(a.get("voltage_unbalance_alarm_pct") or 0.0)

                if alarm_pct > 0 and unbal_pct > alarm_pct:
                    # Escalate: clear any existing WARN first so the ALARM
                    # can be re-raised with the correct severity.
                    existing = self.active.get((mid, "V_UNBAL"))
                    if existing and str(getattr(existing, "severity", "")).upper() != "ALARM":
                        self._clear(mid, "V_UNBAL", snapshot=snap)
                    self._raise(mid, "V_UNBAL",
                                f"{name}: Voltage unbalance {unbal_pct:.1f}% (alarm >{alarm_pct:.0f}%)",
                                "ALARM", snapshot=snap)
                elif warn_pct > 0 and unbal_pct > warn_pct:
                    self._raise(mid, "V_UNBAL",
                                f"{name}: Voltage unbalance {unbal_pct:.1f}% (warn >{warn_pct:.0f}%)",
                                "WARN", snapshot=snap)
                else:
                    self._clear(mid, "V_UNBAL", snapshot=snap)

    # ── Alarm shelving (maintenance suppression) ─────────────────────────────

    def shelve(
        self,
        meter_id,
        code: str,
        duration_sec: Optional[float],
        actor: str = "",
    ) -> None:
        """
        Shelve an alarm code so it is suppressed from raising.

        Parameters
        ----------
        meter_id : int | str | "*"
            Meter whose alarms to suppress.  "*" = all meters.
        code : str | "*"
            Alarm code to suppress.  "*" = all codes for this meter.
        duration_sec : float | None
            Suppression duration in seconds.  None = indefinite (until
            manually unshelved or process restart).
        actor : str
            Operator name for audit log.

        Side-effects:
        • If the alarm is currently active it is cleared immediately
          (the condition existed before the shelf; the operator is
          accepting responsibility for the maintenance risk).
        • Journals a SHELVE event.
        """
        expiry = (time.time() + float(duration_sec)) if duration_sec is not None else None
        key = (meter_id, code)
        with self._shelf_lock:
            self._shelves[key] = expiry

        # Clear any currently active instance of this alarm so the alarm
        # list does not keep showing a silenced fault.
        with self._lock:
            to_clear = [
                (mid, c) for (mid, c) in list(self.active.keys())
                if (meter_id in (mid, "*") or mid == meter_id)
                and (code in (c, "*") or c == code)
            ]
        for (mid, c) in to_clear:
            self._clear(mid, c)

        exp_txt = (f"{duration_sec/60:.0f} min" if duration_sec is not None else "indefinite")
        logger.info("[ALARM] shelved (%s, %s) for %s by %s", meter_id, code, exp_txt, actor or "?")
        if self.event_journal:
            try:
                self.event_journal.log_alarm(
                    meter_id, code, "SHELVE", "INFO",
                    f"Alarm shelved for {exp_txt} by {actor or 'operator'}",
                    snapshot={"actor": actor, "duration_sec": duration_sec},
                )
            except Exception:
                pass

    def unshelve(self, meter_id, code: str, actor: str = "") -> bool:
        """
        Remove a shelf (re-enable the alarm).

        Returns True if a shelf was found and removed.
        """
        key = (meter_id, code)
        with self._shelf_lock:
            if key not in self._shelves:
                return False
            del self._shelves[key]
        logger.info("[ALARM] unshelved (%s, %s) by %s", meter_id, code, actor or "?")
        if self.event_journal:
            try:
                self.event_journal.log_alarm(
                    meter_id, code, "UNSHELVE", "INFO",
                    f"Alarm unshelved by {actor or 'operator'}",
                    snapshot={"actor": actor},
                )
            except Exception:
                pass
        return True

    def unshelve_expired(self) -> int:
        """
        Remove all expired timed shelves.

        Called periodically from the UI tick (once per minute is sufficient).
        Returns count of shelves removed.
        """
        now = time.time()
        expired = []
        with self._shelf_lock:
            for key, expiry in list(self._shelves.items()):
                if expiry is not None and expiry <= now:
                    expired.append(key)
            for key in expired:
                del self._shelves[key]
        if expired:
            logger.info("[ALARM] %d expired shelf(ves) removed", len(expired))
        return len(expired)

    def shelved_items(self) -> list:
        """
        Return list of currently active shelves as dicts:
          {meter_id, code, expiry_epoch (None=indefinite), remaining_sec (None=indefinite)}
        Expired shelves are excluded.
        """
        now = time.time()
        result = []
        with self._shelf_lock:
            for (mid, code), expiry in list(self._shelves.items()):
                if expiry is not None and expiry <= now:
                    continue  # expired (will be cleaned by unshelve_expired)
                result.append({
                    "meter_id":      mid,
                    "code":          code,
                    "expiry_epoch":  expiry,
                    "remaining_sec": round(expiry - now, 1) if expiry is not None else None,
                })
        return result

    # ── ACK workflow ──────────────────────────────────────────────────────────

    def acknowledge(self, meter_id, code: str, actor: str) -> bool:
        """
        Acknowledge an active alarm.

        Transitions: ACTIVE → ACKNOWLEDGED.
        The alarm stays in `self.active` until the condition clears (CLEARED).
        Returns True if the alarm was found and acknowledged.
        """
        k = (meter_id, code)
        ev = self.active.get(k)
        if ev is None:
            # Try history (alarm may have auto-cleared before operator clicked ACK)
            for h in reversed(self.history[-200:]):
                if h.meter_id == meter_id and h.code == code and not h.acknowledged:
                    h.acknowledged = True
                    h.acknowledged_at = time.time()
                    h.acknowledged_by = str(actor or "")
                    if self.event_journal:
                        try:
                            self.event_journal.log_alarm(
                                meter_id, code, "ACK", h.severity, h.message,
                                snapshot={"acknowledged_by": actor},
                            )
                        except Exception:
                            pass
                    return True
            return False

        ev.acknowledged = True
        ev.acknowledged_at = time.time()
        ev.acknowledged_by = str(actor or "")
        if self._state_db is not None:
            try:
                self._state_db.upsert(ev)
            except Exception:
                pass
        if self.event_journal:
            try:
                self.event_journal.log_alarm(
                    meter_id, code, "ACK", ev.severity, ev.message,
                    snapshot={"acknowledged_by": actor},
                )
            except Exception:
                pass
        # Remote: publish ACK event to MQTT broker
        self._publish_alarm_mqtt(meter_id, code, "ACK", ev.severity, ev.message)
        return True

    def acknowledge_all(self, actor: str) -> int:
        """Acknowledge all currently active, unacknowledged alarms. Returns count."""
        count = 0
        with self._lock:
            events = list(self.active.values())
        for ev in events:
            if not ev.acknowledged:
                self.acknowledge(ev.meter_id, ev.code, actor)
                count += 1
        return count

    def history_snapshot(self, limit: int = 500):
        """Return a stable alarm history copy for UI/export readers."""
        with self._lock:
            items = list(self.history)
        if limit and limit > 0:
            return items[-int(limit):]
        return items

    def clear_history(self) -> None:
        """Clear ALL alarm history rows (active + inactive) from the in-memory log."""
        with self._lock:
            self.history.clear()

    def clear_inactive_history(self) -> int:
        """Remove only cleared (inactive) alarms from the history log.

        Active alarms are preserved so the operator cannot accidentally dismiss
        a live fault by clearing the log.  Returns the number of rows removed.
        """
        with self._lock:
            before = len(self.history)
            self.history = [ev for ev in self.history if ev.active]
            return before - len(self.history)

    @property
    def unacknowledged_count(self) -> int:
        """Number of active alarms that have not been acknowledged."""
        with self._lock:
            return sum(1 for ev in self.active.values() if not ev.acknowledged)
