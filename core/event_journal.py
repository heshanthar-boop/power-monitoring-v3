"""core.event_journal

Commercial-grade event journal (SQLite, WAL).

- Writes are queued and handled by a single background thread (no UI blocking).
- Stores: alarms (raise/clear), comm/bus state transitions, and optional notes.
- Adds: retention (default 180 days) + optional VACUUM, executed on writer thread.

Design goals:
- Never block UI or comm threads.
- WAL mode for resilience; synchronous=NORMAL compromise.
- Single writer thread owns the sqlite connection (no concurrent access).
"""

from __future__ import annotations

import csv
import json
import queue
import sqlite3
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional

from utils.paths import events_db_path
from utils.logger import setup_logger

logger = setup_logger("journal")


@dataclass
class JournalRow:
    ts: float
    kind: str                 # 'ALARM' | 'COMM' | 'NOTE'
    meter_id: str             # 'TOTAL' or '1'..'6' or 'BUS'
    code: str                 # alarm code / bus state / note code
    severity: str             # 'INFO'|'WARN'|'ALARM' etc.
    message: str
    data: Optional[Dict[str, Any]] = None


class EventJournal:
    def __init__(self, cfg: Optional[Dict[str, Any]] = None, path: Optional[str] = None):
        self.cfg = cfg or {}
        self.path = path or events_db_path()
        self._q: queue.Queue[Optional[JournalRow]] = queue.Queue(maxsize=10000)
        self._stop = threading.Event()
        self._th: Optional[threading.Thread] = None

        jcfg = (self.cfg.get("journal", {}) or {})
        self.retain_days = int(jcfg.get("retain_days", 180))
        self.vacuum = bool(jcfg.get("vacuum", True))
        self.maint_every_s = float(jcfg.get("maint_every_s", 12 * 3600))

    # ---------------- lifecycle ----------------
    def request_maintenance(self, *, retain_days: int, vacuum: bool) -> None:
        """Update retention policy at runtime (called after config is loaded)."""
        self.retain_days = int(retain_days)
        self.vacuum = bool(vacuum)

    def start(self) -> None:
        if self._th and self._th.is_alive():
            return
        self._stop.clear()
        self._th = threading.Thread(target=self._writer_loop, name="EventJournal", daemon=True)
        self._th.start()
        logger.info(f"[JOURNAL] started: {self.path}")

    def stop(self, timeout: float = 2.0) -> None:
        self._stop.set()
        try:
            self._q.put_nowait(None)
        except Exception:
            logger.debug("[JOURNAL] stop signal put failed (queue full at shutdown)")
        t0 = time.time()
        while self._th and self._th.is_alive() and (time.time() - t0) < timeout:
            time.sleep(0.05)
        logger.info("[JOURNAL] stopped")

    # ---------------- public logging API ----------------
    def log_alarm(self, meter_id: Any, code: str, action: str, severity: str, message: str, snapshot: Optional[Dict[str, Any]] = None) -> None:
        # action: 'RAISE'|'CLEAR'
        mid = str(meter_id)
        self._enqueue(JournalRow(time.time(), "ALARM", mid, f"{code}:{action}", severity, message, snapshot))

    def log_comm(self, meter_id: Any, state: str, message: str = "", data: Optional[Dict[str, Any]] = None) -> None:
        mid = str(meter_id)
        self._enqueue(JournalRow(time.time(), "COMM", mid, state, "INFO", message or state, data))

    def log_note(self, code: str, message: str, data: Optional[Dict[str, Any]] = None) -> None:
        self._enqueue(JournalRow(time.time(), "NOTE", "BUS", code, "INFO", message, data))

    # ---------------- querying (UI) ----------------
    def query(self, since_ts: float, meter_id: Optional[str] = None, kind: Optional[str] = None, limit: int = 2000):
        """Return rows newest-first (sqlite3.Row objects)."""
        con = sqlite3.connect(self.path)
        try:
            con.row_factory = sqlite3.Row
            sql = """SELECT ts, ts_iso, kind, meter_id, code, severity, message, data_json
                     FROM events
                     WHERE ts >= ?
                """
            args: List[Any] = [float(since_ts)]
            if meter_id and meter_id != "ALL":
                sql += " AND meter_id = ?"
                args.append(str(meter_id))
            if kind and kind != "ALL":
                sql += " AND kind = ?"
                args.append(str(kind))
            sql += " ORDER BY ts DESC LIMIT ?"
            args.append(int(limit))
            cur = con.execute(sql, args)
            return cur.fetchall()
        finally:
            try:
                con.close()
            except Exception:
                logger.warning("[JOURNAL] query connection close failed", exc_info=True)

    def export_csv(self, filepath: str, since_ts: float, meter_id: str = "ALL", kind: str = "ALL") -> None:
        rows = self.query(since_ts=since_ts, meter_id=meter_id, kind=kind, limit=200000)
        with open(filepath, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["ts_iso", "ts", "kind", "meter_id", "code", "severity", "message", "data_json"])
            for r in rows:
                w.writerow([r["ts_iso"], r["ts"], r["kind"], r["meter_id"], r["code"], r["severity"], r["message"], r["data_json"]])

    # ---------------- internals ----------------
    def _enqueue(self, row: JournalRow) -> None:
        try:
            self._q.put_nowait(row)
        except queue.Full:
            # Drop oldest by draining a bit, then push latest (operator-relevant)
            logger.warning("[JOURNAL] queue full — dropping 200 oldest events to make room")
            try:
                for _ in range(200):
                    self._q.get_nowait()
            except Exception:
                logger.debug("[JOURNAL] queue drain incomplete", exc_info=True)
            try:
                self._q.put_nowait(row)
            except Exception:
                logger.error("[JOURNAL] enqueue failed even after drain — event lost: %s/%s", row.kind, row.code)

    def _ensure_schema(self, con: sqlite3.Connection) -> None:
        con.execute(
            """CREATE TABLE IF NOT EXISTS events(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts REAL NOT NULL,
                ts_iso TEXT NOT NULL,
                kind TEXT NOT NULL,
                meter_id TEXT NOT NULL,
                code TEXT NOT NULL,
                severity TEXT NOT NULL,
                message TEXT NOT NULL,
                data_json TEXT
            )"""
        )
        con.execute("CREATE INDEX IF NOT EXISTS idx_events_ts ON events(ts)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_events_kind ON events(kind)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_events_meter ON events(meter_id)")

    def _maintenance(self, con: sqlite3.Connection, *, retain_days: int, vacuum: bool) -> None:
        now = time.time()
        cutoff_ts = now - float(retain_days) * 86400.0
        try:
            con.execute("DELETE FROM events WHERE ts < ?", (cutoff_ts,))
            con.commit()
        except Exception as e:
            logger.error(f"[JOURNAL] retention delete error: {type(e).__name__}: {e}")
            try:
                con.commit()
            except Exception:
                logger.warning("[JOURNAL] commit after retention error also failed", exc_info=True)

        if vacuum:
            try:
                con.execute("PRAGMA wal_checkpoint(TRUNCATE);")
            except Exception:
                logger.debug("[JOURNAL] WAL checkpoint failed", exc_info=True)
            try:
                con.execute("VACUUM;")
            except Exception as e:
                logger.error(f"[JOURNAL] vacuum error: {type(e).__name__}: {e}")
            try:
                con.commit()
            except Exception:
                logger.warning("[JOURNAL] commit after vacuum failed", exc_info=True)

    def _write_row(self, con: sqlite3.Connection, row: JournalRow) -> None:
        ts_iso = datetime.fromtimestamp(row.ts).strftime("%Y-%m-%d %H:%M:%S")
        data_json = None
        if row.data is not None:
            try:
                data_json = json.dumps(row.data, ensure_ascii=False)
            except Exception:
                logger.debug("[JOURNAL] data serialisation failed for %s/%s — storing without snapshot", row.kind, row.code)
        try:
            con.execute(
                "INSERT INTO events(ts, ts_iso, kind, meter_id, code, severity, message, data_json) VALUES(?,?,?,?,?,?,?,?)",
                (float(row.ts), ts_iso, row.kind, row.meter_id, row.code, row.severity, row.message, data_json),
            )
        except Exception as e:
            logger.error("[JOURNAL] write error: %s: %s", type(e).__name__, e)

    def _writer_loop(self) -> None:
        con = sqlite3.connect(self.path, check_same_thread=False)
        try:
            con.execute("PRAGMA journal_mode=WAL")
            con.execute("PRAGMA synchronous=NORMAL")
            con.execute("PRAGMA temp_store=MEMORY")
            con.execute("PRAGMA cache_size=-20000")
            self._ensure_schema(con)

            last_commit = time.time()
            last_maint = 0.0

            # Run maintenance once at start
            try:
                self._maintenance(con, retain_days=self.retain_days, vacuum=self.vacuum)
                last_maint = time.time()
            except Exception:
                logger.warning("[JOURNAL] initial maintenance failed", exc_info=True)

            while not self._stop.is_set():
                # periodic maintenance
                if self.maint_every_s and (time.time() - last_maint) > self.maint_every_s:
                    try:
                        self._maintenance(con, retain_days=self.retain_days, vacuum=self.vacuum)
                    except Exception:
                        logger.warning("[JOURNAL] periodic maintenance failed", exc_info=True)
                    last_maint = time.time()

                try:
                    row = self._q.get(timeout=0.5)
                except queue.Empty:
                    if time.time() - last_commit > 1.0:
                        try:
                            con.commit()
                        except Exception:
                            logger.error("[JOURNAL] periodic commit failed — data may not be flushed to disk", exc_info=True)
                        last_commit = time.time()
                    continue

                if row is None:
                    # Stop sentinel — drain any remaining items queued before stop()
                    break

                self._write_row(con, row)

                if time.time() - last_commit > 1.0:
                    try:
                        con.commit()
                    except Exception:
                        logger.error("[JOURNAL] post-insert commit failed — data may not be flushed to disk", exc_info=True)
                    last_commit = time.time()

            # Drain any events enqueued just before stop() was called.
            drained = 0
            while True:
                try:
                    row = self._q.get_nowait()
                except queue.Empty:
                    break
                if row is None:
                    continue
                self._write_row(con, row)
                drained += 1
            if drained:
                logger.info("[JOURNAL] flushed %d queued events on shutdown", drained)

            try:
                con.commit()
            except Exception:
                logger.error("[JOURNAL] final commit on exit failed — recent events may be lost", exc_info=True)
        finally:
            try:
                con.close()
            except Exception:
                logger.warning("[JOURNAL] connection close failed", exc_info=True)
