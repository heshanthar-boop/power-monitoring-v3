from __future__ import annotations

import json
import os
import sqlite3
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from utils.paths import email_db_path


@dataclass
class EmailJob:
    id: int
    created_ts: float
    to_addrs: List[str]
    subject: str
    body_html: str
    attachments: List[str]
    status: str
    retry_count: int
    next_retry_ts: float
    last_error: str


class EmailQueueDB:
    """Persistent email queue (SQLite).

    Phase-1 scope:
    - queue pending jobs
    - retry metadata
    - simple status tracking
    """

    def __init__(self, path: Optional[str] = None):
        self.path = path or email_db_path()
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        self._init_db()

    def _conn(self) -> sqlite3.Connection:
        c = sqlite3.connect(self.path, timeout=10)
        c.execute("PRAGMA journal_mode=WAL")
        c.execute("PRAGMA synchronous=NORMAL")
        return c

    def _init_db(self) -> None:
        with self._conn() as con:
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS email_queue (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_ts REAL NOT NULL,
                    to_addrs TEXT NOT NULL,
                    subject TEXT NOT NULL,
                    body_html TEXT NOT NULL,
                    attachments TEXT NOT NULL,
                    status TEXT NOT NULL,
                    retry_count INTEGER NOT NULL,
                    next_retry_ts REAL NOT NULL,
                    last_error TEXT NOT NULL
                )
                """
            )
            con.execute("CREATE INDEX IF NOT EXISTS idx_email_due ON email_queue(status, next_retry_ts)")

    # ---------------- CRUD ----------------
    def enqueue(self, to_addrs: List[str], subject: str, body_html: str, attachments: Optional[List[str]] = None) -> int:
        attachments = attachments or []
        now = time.time()
        with self._conn() as con:
            cur = con.execute(
                """INSERT INTO email_queue(created_ts,to_addrs,subject,body_html,attachments,status,retry_count,next_retry_ts,last_error)
                   VALUES(?,?,?,?,?,?,?,?,?)""",
                (
                    now,
                    json.dumps(to_addrs),
                    subject,
                    body_html,
                    json.dumps(attachments),
                    "PENDING",
                    0,
                    now,
                    "",
                ),
            )
            return int(cur.lastrowid)

    def list_jobs(self, limit: int = 200) -> List[EmailJob]:
        with self._conn() as con:
            rows = con.execute(
                """SELECT id,created_ts,to_addrs,subject,body_html,attachments,status,retry_count,next_retry_ts,last_error
                   FROM email_queue ORDER BY id DESC LIMIT ?""",
                (int(limit),),
            ).fetchall()
        out: List[EmailJob] = []
        for r in rows:
            out.append(
                EmailJob(
                    id=int(r[0]),
                    created_ts=float(r[1]),
                    to_addrs=list(json.loads(r[2] or "[]")),
                    subject=str(r[3] or ""),
                    body_html=str(r[4] or ""),
                    attachments=list(json.loads(r[5] or "[]")),
                    status=str(r[6] or ""),
                    retry_count=int(r[7] or 0),
                    next_retry_ts=float(r[8] or 0),
                    last_error=str(r[9] or ""),
                )
            )
        return out

    def fetch_due(self, now_ts: float, max_rows: int = 5) -> List[EmailJob]:
        with self._conn() as con:
            rows = con.execute(
                """SELECT id,created_ts,to_addrs,subject,body_html,attachments,status,retry_count,next_retry_ts,last_error
                   FROM email_queue
                   WHERE (status='PENDING' OR status='RETRY') AND next_retry_ts <= ?
                   ORDER BY id ASC LIMIT ?""",
                (float(now_ts), int(max_rows)),
            ).fetchall()
        out: List[EmailJob] = []
        for r in rows:
            out.append(
                EmailJob(
                    id=int(r[0]),
                    created_ts=float(r[1]),
                    to_addrs=list(json.loads(r[2] or "[]")),
                    subject=str(r[3] or ""),
                    body_html=str(r[4] or ""),
                    attachments=list(json.loads(r[5] or "[]")),
                    status=str(r[6] or ""),
                    retry_count=int(r[7] or 0),
                    next_retry_ts=float(r[8] or 0),
                    last_error=str(r[9] or ""),
                )
            )
        return out

    def mark_sending(self, job_id: int) -> None:
        with self._conn() as con:
            con.execute("UPDATE email_queue SET status='SENDING' WHERE id=?", (int(job_id),))

    def mark_sent(self, job_id: int) -> None:
        with self._conn() as con:
            con.execute("UPDATE email_queue SET status='SENT', last_error='' WHERE id=?", (int(job_id),))

    def mark_failed(self, job_id: int, retry_count: int, next_retry_ts: float, err: str, terminal: bool = False) -> None:
        status = "FAILED" if terminal else "RETRY"
        with self._conn() as con:
            con.execute(
                "UPDATE email_queue SET status=?, retry_count=?, next_retry_ts=?, last_error=? WHERE id=?",
                (status, int(retry_count), float(next_retry_ts), str(err)[:500], int(job_id)),
            )

    def delete_sent(self) -> int:
        with self._conn() as con:
            cur = con.execute("DELETE FROM email_queue WHERE status='SENT'")
            return int(cur.rowcount or 0)

    def pending_count(self) -> int:
        """Count queued/retrying jobs not yet sent or failed."""
        with self._conn() as con:
            row = con.execute(
                "SELECT COUNT(*) FROM email_queue WHERE status IN ('QUEUED','SENDING','RETRY')"
            ).fetchone()
        return int((row or [0])[0])

    def dead_letter_count(self) -> int:
        """Count permanently failed (FAILED) jobs — for UI health display."""
        with self._conn() as con:
            row = con.execute("SELECT COUNT(*) FROM email_queue WHERE status='FAILED'").fetchone()
        return int((row or [0])[0])

    def prune_dead_letter(self, older_than_days: int = 7) -> int:
        """Delete FAILED jobs older than N days to prevent DB bloat."""
        cutoff = time.time() - (older_than_days * 86400)
        with self._conn() as con:
            cur = con.execute(
                "DELETE FROM email_queue WHERE status='FAILED' AND created_ts < ?",
                (float(cutoff),),
            )
        return int(cur.rowcount or 0)
