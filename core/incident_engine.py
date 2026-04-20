from __future__ import annotations

import csv
import os
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional, Tuple

from utils.paths import logs_dir, events_db_path, reports_dir
from utils.logger import setup_logger

logger = setup_logger("incident")


@dataclass(frozen=True)
class IncidentSummary:
    start_ts: float
    end_ts: float
    source: str               # "TOTAL" or "M1".."M6"
    meter_id: str             # for alarm filtering ("TOTAL" or "1".."6" or "ALL")
    v_max: Optional[float]
    v_min: Optional[float]
    i_max: Optional[float]
    i_min: Optional[float]
    kw_max: Optional[float]
    kw_min: Optional[float]
    alarms_raised: int
    offline_events: int
    longest_offline_s: float
    stale_s: float
    recovering_s: float
    note: str = ""            # non-fatal warnings (e.g., logging missing)


def _parse_ts_str(ts_str: str) -> Optional[float]:
    """Parse logging_engine Timestamp format: YYYY-MM-DD HH:MM:SS (local time)."""
    s = (ts_str or "").strip()
    if not s:
        return None
    try:
        dt = datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
        return dt.timestamp()
    except Exception:
        return None


def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        s = str(x).strip()
        if not s:
            return None
        return float(s)
    except Exception:
        return None


def _fmt_duration(seconds: float) -> str:
    s = max(0.0, float(seconds or 0.0))
    sec = int(round(s))
    hh = sec // 3600
    mm = (sec % 3600) // 60
    ss = sec % 60
    return f"{hh:02d}:{mm:02d}:{ss:02d}"


class IncidentEngine:
    """Compute 'Last 24h Incident Summary' from CSV logs + EventJournal SQLite.

    Design goals:
    - Never block UI (caller should run in a background thread).
    - Graceful if logging is missing (returns N/A with note).
    - Uses BUS comm state transitions from events.db to compute OFFLINE/STALE durations.
    """

    def __init__(self, cfg: Optional[Dict[str, Any]] = None):
        self.cfg = cfg or {}

    # ---------------------- public API ----------------------
    def compute_last_24h(self, *, source: str, meter_id: str) -> IncidentSummary:
        end_ts = time.time()
        start_ts = end_ts - 24 * 3600.0

        note_parts = []

        # 1) Meter stats from CSV (preferred, defensible)
        v_max, v_min, i_max, i_min, kw_max, kw_min, n_found = self._scan_csv_last24(source=source, start_ts=start_ts, end_ts=end_ts)
        if n_found == 0:
            note_parts.append("No CSV logging samples found for last 24h (logging may be OFF).")

        # 2) Alarm count from EventJournal
        alarms_raised = self._count_alarms(start_ts=start_ts, end_ts=end_ts, meter_id=meter_id)

        # 3) Bus quality durations from EventJournal (BUS COMM state)
        offline_events, longest_offline_s, stale_s, recovering_s = self._bus_state_stats(start_ts=start_ts, end_ts=end_ts)

        note = " ".join(note_parts).strip()

        return IncidentSummary(
            start_ts=start_ts,
            end_ts=end_ts,
            source=str(source),
            meter_id=str(meter_id),
            v_max=v_max, v_min=v_min,
            i_max=i_max, i_min=i_min,
            kw_max=kw_max, kw_min=kw_min,
            alarms_raised=int(alarms_raised),
            offline_events=int(offline_events),
            longest_offline_s=float(longest_offline_s),
            stale_s=float(stale_s),
            recovering_s=float(recovering_s),
            note=note,
        )

    def export_csv(self, summary: IncidentSummary) -> str:
        out_dir = os.path.join(reports_dir(), "incident_summaries")
        os.makedirs(out_dir, exist_ok=True)
        fn = self._export_basename(summary, ext="csv")
        path = os.path.join(out_dir, fn)

        rows = [
            ("Source", summary.source),
            ("Period Start", self._fmt_ts(summary.start_ts)),
            ("Period End", self._fmt_ts(summary.end_ts)),
            ("Max Voltage (V)", self._fmt_num(summary.v_max)),
            ("Min Voltage (V)", self._fmt_num(summary.v_min)),
            ("Max Current (A)", self._fmt_num(summary.i_max)),
            ("Min Current (A)", self._fmt_num(summary.i_min)),
            ("Max Power (kW)", self._fmt_num(summary.kw_max)),
            ("Min Power (kW)", self._fmt_num(summary.kw_min)),
            ("Total Alarms Raised", str(summary.alarms_raised)),
            ("Offline Events (BUS)", str(summary.offline_events)),
            ("Longest OFFLINE (BUS)", _fmt_duration(summary.longest_offline_s)),
            ("Time in STALE (BUS)", _fmt_duration(summary.stale_s)),
            ("Time in RECOVERING (BUS)", _fmt_duration(summary.recovering_s)),
        ]
        if summary.note:
            rows.append(("Note", summary.note))

        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["Metric", "Value"])
            w.writerows(rows)

        return path

    def export_pdf(self, summary: IncidentSummary) -> str:
        # ReportLab is already used elsewhere; keep this PDF minimal + B/W.
        out_dir = os.path.join(reports_dir(), "incident_summaries")
        os.makedirs(out_dir, exist_ok=True)
        fn = self._export_basename(summary, ext="pdf")
        path = os.path.join(out_dir, fn)

        try:
            from reportlab.lib.pagesizes import A4
            from reportlab.lib.units import mm
            from reportlab.pdfgen import canvas
        except Exception as e:
            raise RuntimeError("PDF export requires reportlab") from e

        c = canvas.Canvas(path, pagesize=A4)
        w, h = A4

        x = 18 * mm
        y = h - 20 * mm

        def line(txt: str, dy=6*mm, size=11, bold=False):
            nonlocal y
            try:
                c.setFont("Helvetica-Bold" if bold else "Helvetica", size)
            except Exception:
                c.setFont("Helvetica", size)
            c.drawString(x, y, txt)
            y -= dy

        # Header
        line("LAST 24 HOURS – INCIDENT SUMMARY", dy=8*mm, size=14, bold=True)
        line(f"Source: {summary.source}", dy=6*mm, size=11)
        line(f"Period: {self._fmt_ts(summary.start_ts)}  →  {self._fmt_ts(summary.end_ts)}", dy=8*mm, size=10)

        # Divider
        c.line(x, y, w - x, y)
        y -= 8*mm

        # Key stats (table-like)
        line("METER STATISTICS", dy=7*mm, size=12, bold=True)

        stats = [
            ("Max Voltage (V)", self._fmt_num(summary.v_max)),
            ("Min Voltage (V)", self._fmt_num(summary.v_min)),
            ("Max Current (A)", self._fmt_num(summary.i_max)),
            ("Min Current (A)", self._fmt_num(summary.i_min)),
            ("Max Power (kW)", self._fmt_num(summary.kw_max)),
            ("Min Power (kW)", self._fmt_num(summary.kw_min)),
        ]
        for k, v in stats:
            line(f"{k:<22} : {v}", dy=6*mm, size=10)

        y -= 4*mm
        c.line(x, y, w - x, y)
        y -= 8*mm

        line("INCIDENT STATISTICS", dy=7*mm, size=12, bold=True)
        inc = [
            ("Total Alarms Raised", str(summary.alarms_raised)),
            ("Offline Events (BUS)", str(summary.offline_events)),
            ("Longest OFFLINE (BUS)", _fmt_duration(summary.longest_offline_s)),
            ("Time in STALE (BUS)", _fmt_duration(summary.stale_s)),
            ("Time in RECOVERING (BUS)", _fmt_duration(summary.recovering_s)),
        ]
        for k, v in inc:
            line(f"{k:<22} : {v}", dy=6*mm, size=10)

        if summary.note:
            y -= 4*mm
            c.line(x, y, w - x, y)
            y -= 8*mm
            line("NOTES", dy=7*mm, size=12, bold=True)
            # wrap note
            note = summary.note.strip()
            max_chars = 95
            while note:
                chunk = note[:max_chars]
                note = note[max_chars:]
                line(chunk, dy=5.5*mm, size=9)

        # Footer
        c.setFont("Helvetica", 8)
        c.drawRightString(w - x, 12*mm, "Generated by MFM384 Monitoring – Heshan Engineering Solutions")

        c.showPage()
        c.save()
        return path

    # ---------------------- internals ----------------------
    def _export_basename(self, summary: IncidentSummary, ext: str) -> str:
        dt = datetime.fromtimestamp(summary.end_ts)
        stamp = dt.strftime("%Y-%m-%d_%H%M%S")
        src = "".join(c if c.isalnum() else "_" for c in (summary.source or "SRC"))
        return f"Incident_Summary_{src}_{stamp}.{ext}"

    def _fmt_ts(self, ts: float) -> str:
        try:
            return datetime.fromtimestamp(float(ts)).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return "N/A"

    def _fmt_num(self, v: Optional[float]) -> str:
        if v is None:
            return "N/A"
        try:
            return f"{float(v):.3f}"
        except Exception:
            return "N/A"

    def _scan_csv_last24(self, *, source: str, start_ts: float, end_ts: float) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float], Optional[float], Optional[float], int]:
        folder = (self.cfg.get("logging", {}).get("folder", "") or "").strip() or logs_dir()
        # Only need today + yesterday folders to cover last 24h
        days = set()
        days.add(datetime.fromtimestamp(end_ts).strftime("%Y-%m-%d"))
        days.add(datetime.fromtimestamp(start_ts).strftime("%Y-%m-%d"))

        v_max = v_min = i_max = i_min = kw_max = kw_min = None
        n = 0

        # Candidate keys (fallbacks if Total_* used)
        v_keys = ["Vavg", "Vavg_ref"]
        i_keys = ["Iavg"]
        kw_keys = ["kW", "Total_kW"]

        for d in sorted(days):
            day_dir = os.path.join(folder, d)
            if not os.path.isdir(day_dir):
                continue
            # Find matching CSVs for this source
            for fn in os.listdir(day_dir):
                if not fn.lower().endswith(".csv"):
                    continue
                if not fn.startswith(f"{source}_"):
                    continue
                path = os.path.join(day_dir, fn)
                try:
                    with open(path, "r", encoding="utf-8", newline="") as f:
                        r = csv.DictReader(f)
                        for row in r:
                            ts = _parse_ts_str(row.get("Timestamp", ""))
                            if ts is None:
                                continue
                            if ts < start_ts or ts > end_ts:
                                continue
                            # Prefer avg rows (more stable for summary)
                            mode = (row.get("Mode", "") or "").strip().lower()
                            if mode and mode not in ("avg", "average"):
                                continue
                            if str(row.get("DataValid", "1")).strip() not in ("1", "True", "true"):
                                continue
                            # Skip disabled
                            if (row.get("Quality", "") or "").strip().upper() in ("DISABLED",):
                                continue

                            v = self._pick_first_float(row, v_keys)
                            i = self._pick_first_float(row, i_keys)
                            kw = self._pick_first_float(row, kw_keys)

                            if v is not None:
                                v_max = v if v_max is None else max(v_max, v)
                                v_min = v if v_min is None else min(v_min, v)
                            if i is not None:
                                i_max = i if i_max is None else max(i_max, i)
                                i_min = i if i_min is None else min(i_min, i)
                            if kw is not None:
                                kw_max = kw if kw_max is None else max(kw_max, kw)
                                kw_min = kw if kw_min is None else min(kw_min, kw)

                            n += 1
                except Exception:
                    continue

        return v_max, v_min, i_max, i_min, kw_max, kw_min, n

    def _pick_first_float(self, row: Dict[str, Any], keys) -> Optional[float]:
        for k in keys:
            if k in row:
                v = _safe_float(row.get(k))
                if v is not None:
                    return v
        return None

    def _count_alarms(self, *, start_ts: float, end_ts: float, meter_id: str) -> int:
        path = events_db_path()
        if not os.path.exists(path):
            return 0
        mid = (meter_id or "ALL").strip()
        with sqlite3.connect(path) as con:
            cur = con.cursor()
            sql = "SELECT COUNT(*) FROM events WHERE kind='ALARM' AND ts >= ? AND ts <= ? AND code LIKE ?"
            args = [float(start_ts), float(end_ts), "%:RAISE"]
            if mid and mid != "ALL":
                sql += " AND meter_id = ?"
                args.append(str(mid))
            try:
                cur.execute(sql, args)
                row = cur.fetchone()
                return int(row[0] or 0) if row else 0
            except Exception:
                return 0

    def _bus_state_stats(self, *, start_ts: float, end_ts: float) -> Tuple[int, float, float, float]:
        path = events_db_path()
        if not os.path.exists(path):
            return 0, 0.0, 0.0, 0.0

        # Get last state before window
        initial_state = "OFFLINE"
        initial_ts = start_ts
        with sqlite3.connect(path) as con:
            cur = con.cursor()
            try:
                cur.execute(
                    "SELECT ts, code FROM events WHERE kind='COMM' AND meter_id='BUS' AND ts < ? ORDER BY ts DESC LIMIT 1",
                    (float(start_ts),),
                )
                r = cur.fetchone()
                if r and r[1]:
                    initial_state = str(r[1])
                if r and r[0]:
                    initial_ts = float(start_ts)
            except Exception:
                pass

            # Fetch all transitions in-window
            try:
                cur.execute(
                    "SELECT ts, code FROM events WHERE kind='COMM' AND meter_id='BUS' AND ts >= ? AND ts <= ? ORDER BY ts ASC",
                    (float(start_ts), float(end_ts)),
                )
                rows = cur.fetchall() or []
            except Exception:
                rows = []

        # Build segments
        offline_events = 0
        longest_offline = 0.0
        stale_s = 0.0
        recovering_s = 0.0

        state = initial_state
        seg_start = start_ts

        def add_segment(st: str, a: float, b: float):
            nonlocal offline_events, longest_offline, stale_s, recovering_s
            dur = max(0.0, float(b) - float(a))
            st_u = (st or "").strip().upper()
            if st_u == "OFFLINE":
                longest_offline = max(longest_offline, dur)
                stale_s += 0.0
            if st_u == "STALE":
                stale_s += dur
            if st_u == "RECOVERING":
                recovering_s += dur

        for ts, code in rows:
            t = float(ts)
            add_segment(state, seg_start, t)
            # Count transitions into OFFLINE
            new_state = str(code or "")
            if (new_state or "").strip().upper() == "OFFLINE" and (state or "").strip().upper() != "OFFLINE":
                offline_events += 1
            state = new_state
            seg_start = t

        add_segment(state, seg_start, end_ts)

        return offline_events, longest_offline, stale_s, recovering_s
