"""Report tracking DB (SQLite).

Stores generated report metadata so UI can show history and enable retention cleanup.
SQLite is in stdlib -> PyInstaller friendly.
"""

from __future__ import annotations

import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional

from utils.paths import reports_db_path


@dataclass
class ReportRow:
    report_id: int
    report_type: str
    generation_time: str
    report_period_start: str
    report_period_end: str
    file_path: str
    file_size_kb: int
    meter_ids_included: str
    generated_by: str
    report_hash: str


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS reports (
  report_id INTEGER PRIMARY KEY AUTOINCREMENT,
  report_type TEXT NOT NULL,
  generation_time TEXT NOT NULL,
  report_period_start TEXT NOT NULL,
  report_period_end TEXT NOT NULL,
  file_path TEXT NOT NULL,
  file_size_kb INTEGER NOT NULL,
  meter_ids_included TEXT NOT NULL,
  generated_by TEXT NOT NULL,
  report_hash TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_reports_time ON reports(generation_time);
"""


class ReportDB:
    def __init__(self, db_path: Optional[str] = None):
        self.db_path = db_path or reports_db_path()
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self._init()

    def _connect(self) -> sqlite3.Connection:
        con = sqlite3.connect(self.db_path)
        con.row_factory = sqlite3.Row
        return con

    def _init(self) -> None:
        with self._connect() as con:
            con.executescript(SCHEMA_SQL)

    def add_report(
        self,
        report_type: str,
        period_start: str,
        period_end: str,
        file_path: str,
        meter_ids_included: str,
        generated_by: str,
        report_hash: str,
    ) -> None:
        try:
            size_kb = int(round(os.path.getsize(file_path) / 1024.0))
        except Exception:
            size_kb = 0

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with self._connect() as con:
            con.execute(
                """
                INSERT INTO reports(
                  report_type, generation_time, report_period_start, report_period_end,
                  file_path, file_size_kb, meter_ids_included, generated_by, report_hash
                ) VALUES (?,?,?,?,?,?,?,?,?)
                """,
                (
                    report_type,
                    now,
                    period_start,
                    period_end,
                    file_path,
                    size_kb,
                    meter_ids_included,
                    generated_by,
                    report_hash,
                ),
            )

    def list_reports(self, limit: int = 200) -> List[ReportRow]:
        with self._connect() as con:
            rows = con.execute(
                "SELECT * FROM reports ORDER BY generation_time DESC LIMIT ?", (int(limit),)
            ).fetchall()
        out: List[ReportRow] = []
        for r in rows:
            out.append(
                ReportRow(
                    report_id=int(r["report_id"]),
                    report_type=str(r["report_type"]),
                    generation_time=str(r["generation_time"]),
                    report_period_start=str(r["report_period_start"]),
                    report_period_end=str(r["report_period_end"]),
                    file_path=str(r["file_path"]),
                    file_size_kb=int(r["file_size_kb"]),
                    meter_ids_included=str(r["meter_ids_included"]),
                    generated_by=str(r["generated_by"]),
                    report_hash=str(r["report_hash"]),
                )
            )
        return out

    def prune_older_than_days(self, days: int) -> int:
        """Delete DB rows + files older than `days`. Returns deleted count."""
        days = max(1, int(days))
        cutoff = datetime.now().timestamp() - days * 86400

        deleted = 0
        with self._connect() as con:
            rows = con.execute("SELECT report_id, generation_time, file_path FROM reports").fetchall()
            for r in rows:
                try:
                    ts = datetime.strptime(r["generation_time"], "%Y-%m-%d %H:%M:%S").timestamp()
                except Exception:
                    continue
                if ts < cutoff:
                    fp = str(r["file_path"])
                    try:
                        if fp and os.path.exists(fp):
                            os.remove(fp)
                    except Exception:
                        pass
                    con.execute("DELETE FROM reports WHERE report_id=?", (int(r["report_id"]),))
                    deleted += 1
        return deleted
