"""PDF Reporting engine (ReportLab) + scheduler.

Design goals:
- Operator-grade, reliable, no UI freezes (work done in background thread)
- Uses current snapshot for instant reports
- Uses existing CSV logs for range/daily reports
- Tracks generated reports in SQLite DB (core.report_db.ReportDB)

This module is intentionally self-contained and PyInstaller-friendly.
"""

from __future__ import annotations

import csv
import hashlib
import os
import queue
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

from core.report_db import ReportDB
from utils.logger import setup_logger
from utils.paths import reports_dir, logs_dir

log = setup_logger("reports")


# -----------------------------------------------------------------------------
# Data helpers
# -----------------------------------------------------------------------------

PARAM_ALIASES = {
    # Report labels -> snapshot keys (best-effort)
    "Vavg": "Vavg",
    "Frequency": "Frequency",
    "ROCOF": "ROCOF",
    "Unbalance": "Unbalance",
    "Iavg": "Iavg",
    "kVA%": "kVA_LoadPct",
    "THD-V": "THD_V",
    "THD-I": "THD_I",
    "PF": "PF",
    "kW": "kW",
    "kVA": "kVA",
    "kVAr": "kVAr",
    "ExportKW": "Export_kW",
    "V1N": "V1N",
    "I1": "I1",
}


def _now_iso() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _parse_ts(s: str) -> Optional[float]:
    """Parse timestamps produced by LoggingEngine."""
    try:
        # Logging CSV uses ISO-like local timestamps.
        return datetime.strptime(s, "%Y-%m-%d %H:%M:%S").timestamp()
    except Exception:
        try:
            return datetime.fromisoformat(s).timestamp()
        except Exception:
            return None


def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        if isinstance(x, (int, float)):
            return float(x)
        s = str(x).strip()
        if not s or s in ("—", "-", "N/A", "nan", "NaN"):
            return None
        return float(s)
    except Exception:
        return None


def read_csv_rows_in_range(csv_path: str, start_ts: float, end_ts: float) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if not csv_path or not os.path.exists(csv_path):
        return rows
    try:
        with open(csv_path, "r", encoding="utf-8", newline="") as f:
            # skip comment blocks (site header)
            for line in f:
                if line.startswith("Timestamp,"):
                    header = line.strip().split(",")
                    reader = csv.DictReader(f, fieldnames=header)
                    for r in reader:
                        ts = _parse_ts(str(r.get("Timestamp", "")))
                        if ts is None:
                            continue
                        if ts < start_ts:
                            continue
                        if ts > end_ts:
                            break
                        rows.append(r)
                    break
    except Exception as e:
        log.warning("CSV read failed: %s (%s)", csv_path, e)
    return rows


def find_log_files(folder: str, sources: List[str], start: datetime, end: datetime) -> Dict[str, List[str]]:
    """Return {source: [filepaths]} intersecting the date range."""
    out: Dict[str, List[str]] = {s: [] for s in sources}
    try:
        day = start.date()
        end_day = end.date()
        while day <= end_day:
            day_folder = os.path.join(folder, day.strftime("%Y-%m-%d"))
            if os.path.isdir(day_folder):
                for fn in os.listdir(day_folder):
                    for s in sources:
                        if fn.startswith(f"{s}_") and fn.endswith(f"_{day.strftime('%Y-%m-%d')}.csv"):
                            out[s].append(os.path.join(day_folder, fn))
            day = day + timedelta(days=1)
    except Exception:
        pass
    return out


def compute_stats(rows: List[Dict[str, Any]], key: str) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
    vals: List[float] = []
    for r in rows:
        v = _safe_float(r.get(key))
        if v is None:
            continue
        vals.append(v)
    if not vals:
        return None, None, None, None
    mn = min(vals)
    mx = max(vals)
    avg = sum(vals) / len(vals)
    # std dev
    var = sum((x - avg) ** 2 for x in vals) / max(1, (len(vals) - 1))
    std = var ** 0.5
    return mn, mx, avg, std


# -----------------------------------------------------------------------------
# Report request/response
# -----------------------------------------------------------------------------


@dataclass
class ReportRequest:
    report_type: str  # INSTANT, HOURLY, DAILY, CUSTOM
    start_ts: float
    end_ts: float
    meter_ids: List[int]  # [] means ALL enabled
    generated_by: str  # "USER" or "AUTO"
    output: str = "HTML"  # HTML or PDF


@dataclass
class ReportResult:
    ok: bool
    file_path: str = ""
    error: str = ""


# -----------------------------------------------------------------------------
# Engine
# -----------------------------------------------------------------------------


class ReportsEngine:
    def __init__(
        self,
        cfg: Dict[str, Any],
        meters: List[Any],
        snapshot_fn: Callable[[], Dict[str, Any]],
        protection_events_csv: Optional[str] = None,
    ):
        self.cfg = cfg
        self.meters = meters
        self.snapshot_fn = snapshot_fn
        self.protection_events_csv = protection_events_csv or os.path.join(logs_dir(), "protection_events.csv")

        self.db = ReportDB()

        self._job_q: queue.Queue = queue.Queue()
        self._res_q: queue.Queue = queue.Queue()
        self._stop = threading.Event()
        # _generate() touches filesystem and may be called by multiple background services.
        # Protect it so report generation cannot interleave and produce mismatched outputs.
        self._gen_lock = threading.Lock()
        self._worker = threading.Thread(target=self._worker_loop, daemon=True)
        self._worker.start()

        self._sched_stop = threading.Event()
        self._sched = threading.Thread(target=self._scheduler_loop, daemon=True)
        self._sched.start()

    # ---------------- public ----------------
    def stop(self):
        self._stop.set()
        self._sched_stop.set()

    def enqueue(self, req: ReportRequest):
        self._job_q.put(req)

    def generate_now(self, req: ReportRequest) -> ReportResult:
        """Generate a report synchronously (thread-safe).

        Use this when a caller must get the *exact* report they requested (e.g. email attachments)
        without relying on the async result queue (which could contain other jobs).
        """
        with self._gen_lock:
            return self._generate(req)

    def poll_result(self) -> Optional[ReportResult]:
        try:
            return self._res_q.get_nowait()
        except Exception:
            return None

    def list_history(self, limit: int = 200):
        return self.db.list_reports(limit=limit)

    def retention_cleanup(self) -> int:
        days = int((self.cfg.get("reports", {}) or {}).get("retention_days", 30))
        return self.db.prune_older_than_days(days)

    # ---------------- scheduler ----------------
    def _scheduler_loop(self):
        # Seed last_fire at startup so nothing fires immediately.
        # interval:  waits at least one full period from now
        # daily:     any schedule whose time has already passed today is skipped
        _startup = time.time()
        last_fire: Dict[str, float] = {}

        # Pre-seed daily schedules that have already passed today so they are
        # not treated as missed and fired immediately on first loop tick.
        try:
            rcfg_init = (self.cfg.get("reports") or {})
            now_init = datetime.now()
            for sch in (rcfg_init.get("schedules") or []):
                if not isinstance(sch, dict):
                    continue
                if str(sch.get("type", "interval")) != "daily":
                    continue
                sid = str(sch.get("id", "")) or f"sch_{id(sch)}"
                tstr = str(sch.get("time", "23:59"))
                try:
                    hh, mm = [int(x) for x in tstr.split(":")[:2]]
                except Exception:
                    hh, mm = 23, 59
                target = now_init.replace(hour=hh, minute=mm, second=0, microsecond=0)
                key = f"{sid}_{now_init.strftime('%Y-%m-%d')}"
                if now_init >= target:
                    # Already past for today — mark as fired so it won't re-fire
                    last_fire[key] = _startup
        except Exception:
            pass

        while not self._sched_stop.is_set():
            try:
                rcfg = self.cfg.get("reports", {}) or {}
                schedules = rcfg.get("schedules", []) or []
                now = datetime.now()
                for sch in schedules:
                    if not isinstance(sch, dict):
                        continue
                    sid = str(sch.get("id", "")) or f"sch_{id(sch)}"
                    if not bool(sch.get("enabled", False)):
                        continue
                    stype = str(sch.get("type", "interval"))
                    if stype == "interval":
                        minutes = int(sch.get("minutes", 60))
                        period = max(1, minutes) * 60
                        last = float(last_fire.get(sid, _startup))
                        if time.time() - last >= period:
                            last_fire[sid] = time.time()
                            self._enqueue_auto(sch, now)
                    elif stype == "daily":
                        tstr = str(sch.get("time", "23:59"))
                        try:
                            hh, mm = [int(x) for x in tstr.split(":")[:2]]
                        except Exception:
                            hh, mm = 23, 59
                        key = f"{sid}_{now.strftime('%Y-%m-%d')}"
                        last = float(last_fire.get(key, 0.0))
                        target = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
                        if now >= target and last == 0.0:
                            last_fire[key] = time.time()
                            self._enqueue_auto(sch, now)
                # retention
                self.retention_cleanup()
            except Exception as e:
                log.warning("scheduler error: %s", e)
            self._sched_stop.wait(10.0)

    def _enqueue_auto(self, sch: Dict[str, Any], now: datetime):
        rtype = str(sch.get("report_type", "INSTANT")).upper()
        outfmt = str(sch.get("output", "HTML") or "HTML").upper()
        meters = sch.get("meters", "ALL")
        meter_ids: List[int] = []
        if isinstance(meters, list):
            meter_ids = [int(x) for x in meters if str(x).isdigit()]
        # Period
        if rtype == "INSTANT":
            start_ts = end_ts = time.time()
        elif rtype == "HOURLY":
            end_ts = now.timestamp()
            start_ts = (now - timedelta(hours=1)).timestamp()
        elif rtype == "DAILY":
            end_ts = now.timestamp()
            start_ts = (now - timedelta(days=1)).timestamp()
        else:
            start_ts = end_ts = time.time()
        self.enqueue(
            ReportRequest(
                report_type=rtype,
                start_ts=start_ts,
                end_ts=end_ts,
                meter_ids=meter_ids,
                generated_by="AUTO",
                output=outfmt,
            )
        )

    # ---------------- worker ----------------
    def _worker_loop(self):
        while not self._stop.is_set():
            try:
                req: ReportRequest = self._job_q.get(timeout=0.3)
            except Exception:
                continue
            try:
                with self._gen_lock:
                    res = self._generate(req)
            except Exception as e:
                res = ReportResult(ok=False, error=str(e))
            self._res_q.put(res)

    # ---------------- core generation ----------------
    def _generate(self, req: ReportRequest) -> ReportResult:
        rcfg = self.cfg.get("reports", {}) or {}
        facility_code = str(rcfg.get("facility_code", "PLANT01") or "PLANT01")

        # Determine meter ids (enabled by default)
        enabled_ids = [m.meter_id for m in self.meters if getattr(m, "enabled", False)]
        meter_ids = req.meter_ids or enabled_ids
        meter_ids = [i for i in meter_ids if i in enabled_ids]
        if not meter_ids:
            meter_ids = enabled_ids[:]

        # Save folder
        base_dir = str(rcfg.get("auto_save_dir") or "").strip() or reports_dir()
        # Year/Month subfolders
        dt = datetime.fromtimestamp(req.end_ts)
        out_dir = os.path.join(base_dir, dt.strftime("%Y"), dt.strftime("%m"))
        os.makedirs(out_dir, exist_ok=True)

        report_type = req.report_type.upper()
        outfmt = str(getattr(req, "output", "HTML") or "HTML").upper()
        ext = "html" if outfmt == "HTML" else "pdf"
        name = f"{facility_code}_{report_type}_{dt.strftime('%Y%m%d')}_{dt.strftime('%H%M')}.{ext}"
        out_path = os.path.join(out_dir, name)

        # Collect data
        data = self._collect_data(req, meter_ids)

        # Render
        if outfmt == "HTML":
            from core.reports_html import build_report_html

            build_report_html(cfg=self.cfg, req=req, meter_ids=meter_ids, data=data, out_path=out_path)
        else:
            # PDF needs ReportLab. If missing, fail with a clear message.
            try:
                from core.reports_pdf import build_report_pdf
            except Exception as e:
                return ReportResult(
                    ok=False,
                    error=(
                        "PDF engine missing (reportlab). Choose HTML output or install reportlab.\n\n"
                        f"Details: {e}"
                    ),
                )
            build_report_pdf(cfg=self.cfg, req=req, meter_ids=meter_ids, data=data, out_path=out_path)

        # Hash + DB row
        h = hashlib.sha256()
        with open(out_path, "rb") as f:
            while True:
                b = f.read(1024 * 128)
                if not b:
                    break
                h.update(b)
        digest = h.hexdigest()
        self.db.add_report(
            report_type=f"{report_type}_{outfmt}",
            period_start=datetime.fromtimestamp(req.start_ts).strftime("%Y-%m-%d %H:%M:%S"),
            period_end=datetime.fromtimestamp(req.end_ts).strftime("%Y-%m-%d %H:%M:%S"),
            file_path=out_path,
            meter_ids_included=",".join(str(i) for i in meter_ids),
            generated_by=req.generated_by,
            report_hash=digest,
        )

        return ReportResult(ok=True, file_path=out_path)

    def _collect_data(self, req: ReportRequest, meter_ids: List[int]) -> Dict[str, Any]:
        report_type = req.report_type.upper()
        snap = self.snapshot_fn()  # always capture for summary even for range reports

        # Period data (from logs)
        start = datetime.fromtimestamp(req.start_ts)
        end = datetime.fromtimestamp(req.end_ts)

        folder = (self.cfg.get("logging", {}) or {}).get("folder", "") or logs_dir()
        sources = ["TOTAL"] + [f"M{i}" for i in meter_ids]
        files = find_log_files(folder, sources=sources, start=start, end=end)

        # Read relevant rows per source
        per_source_rows: Dict[str, List[Dict[str, Any]]] = {}
        for src, fps in files.items():
            rr: List[Dict[str, Any]] = []
            for fp in fps:
                rr.extend(read_csv_rows_in_range(fp, req.start_ts, req.end_ts))
            per_source_rows[src] = rr

        # Stats table for daily/hourly/custom
        stats: Dict[str, Dict[str, Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]]] = {}
        if report_type in ("HOURLY", "DAILY", "CUSTOM"):
            # Choose a few key parameters
            keys = ['Vavg','Iavg','kW','PFavg','Frequency','THD_V_worst','THD_I_worst']
            for src, rows in per_source_rows.items():
                stats[src] = {k: compute_stats(rows, k) for k in keys}

        # Alarms/events
        events = self._read_events(req.start_ts, req.end_ts)

        return {
            "snapshot": snap,
            "rows": per_source_rows,
            "stats": stats,
            "events": events,
        }

    def _read_events(self, start_ts: float, end_ts: float) -> List[Dict[str, Any]]:
        evs: List[Dict[str, Any]] = []
        p = self.protection_events_csv
        if not p or not os.path.exists(p):
            return evs
        try:
            with open(p, "r", encoding="utf-8", newline="") as f:
                r = csv.DictReader(f)
                for row in r:
                    ts = _parse_ts(row.get("time") or row.get("Time") or row.get("Timestamp") or "")
                    if ts is None:
                        continue
                    if ts < start_ts or ts > end_ts:
                        continue
                    evs.append(row)
        except Exception:
            pass
        # sort chronological
        evs.sort(key=lambda x: (x.get("time") or x.get("Time") or ""))
        return evs
