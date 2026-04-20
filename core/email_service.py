from __future__ import annotations

import os
import threading
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from core.email_queue_db import EmailQueueDB
from core.email_sender import EmailSendError, send_smtp_email
from core.email_templates import build_simple_html, build_params_table
from core.reports_engine import ReportsEngine, ReportRequest
from core.reports_engine import find_log_files
from utils.logger import setup_logger
from utils.paths import logs_dir

log = setup_logger("email")


RETRY_SECONDS = [60, 300, 900, 1800, 3600, 21600]  # 1m,5m,15m,30m,1h,6h


class EmailService:
    """Background email queue + retry, plus daily schedule.

    Phase-1:
      - SMTP settings
      - Persistent queue + retry
      - Manual send + daily report email
    """

    def __init__(self, cfg: Dict[str, Any], reports_engine: ReportsEngine):
        self.cfg = cfg
        self.reports_engine = reports_engine
        self.db = EmailQueueDB()

        self._stop = threading.Event()
        self._worker = threading.Thread(target=self._worker_loop, daemon=True)
        self._worker.start()

        self._sched = threading.Thread(target=self._schedule_loop, daemon=True)
        self._sched.start()

        # Phase-2 (basic): alarm-triggered emails by watching Protection events CSV.
        self._alarm_stop = threading.Event()
        self._alarm_thread = threading.Thread(target=self._alarm_watch_loop, daemon=True)
        self._alarm_thread.start()

        self._last_daily_date: Optional[str] = None

    def stop(self) -> None:
        self._stop.set()
        self._alarm_stop.set()

    # ---------------- public APIs ----------------
    def enqueue_email(self, to_addrs: List[str], subject: str, body_html: str, attachments: Optional[List[str]] = None) -> int:
        return self.db.enqueue(to_addrs=to_addrs, subject=subject, body_html=body_html, attachments=attachments or [])

    def list_queue(self, limit: int = 200):
        return self.db.list_jobs(limit=limit)

    def delete_sent(self) -> int:
        return self.db.delete_sent()

    def dead_letter_count(self) -> int:
        """Number of permanently failed (undeliverable) email jobs."""
        return self.db.dead_letter_count()

    def pending_count(self) -> int:
        """Number of queued/retrying jobs not yet delivered."""
        return self.db.pending_count()

    def enqueue_daily_report_manual(self, to_addrs: List[str]) -> None:
        """Manual send: generate a DAILY HTML report (last 24h) and queue email to provided recipients."""
        self._enqueue_daily_report_email(generated_by="USER", override_recipients=to_addrs)

    def _enabled_recipients(self) -> List[str]:
        """Legacy: returns enabled recipient emails only."""
        recs = (self.cfg.get("email", {}) or {}).get("recipients", []) or []
        out: List[str] = []
        for r in recs:
            if not isinstance(r, dict):
                continue
            if not bool(r.get("enabled", True)):
                continue
            em = str(r.get("email", "")).strip()
            if em:
                out.append(em)
        return out


    def _enabled_recipient_dicts(self) -> List[Dict[str, Any]]:
        """Return enabled recipient dicts (preserves per-recipient overrides)."""
        recs = (self.cfg.get("email", {}) or {}).get("recipients", []) or []
        out: List[Dict[str, Any]] = []
        for r in recs:
            if not isinstance(r, dict):
                continue
            if not bool(r.get("enabled", True)):
                continue
            em = str(r.get("email", "")).strip()
            if not em:
                continue
            rr = dict(r)
            rr["email"] = em
            out.append(rr)
        return out

    def _effective_bool(self, rec: Dict[str, Any], key: str, global_val: bool) -> bool:
        v = rec.get(key, None)
        if v is None:
            return bool(global_val)
        return bool(v)
    # ---------------- internal ----------------
    def _worker_loop(self):
        _last_prune_ts = 0.0
        while not self._stop.is_set():
            try:
                now = time.time()
                # Prune dead-letter jobs once per hour to prevent unbounded DB growth.
                if (now - _last_prune_ts) >= 3600.0:
                    _last_prune_ts = now
                    retain_days = int(
                        ((self.cfg.get("email", {}) or {}).get("queue", {}) or {}).get("dead_letter_retain_days", 7)
                    )
                    pruned = self.db.prune_dead_letter(older_than_days=retain_days)
                    if pruned:
                        log.info("Pruned %d dead-letter email job(s) older than %d days", pruned, retain_days)

                due = self.db.fetch_due(now_ts=now, max_rows=4)
                if not due:
                    time.sleep(1.0)
                    continue

                smtp_cfg = (self.cfg.get("email", {}) or {}).get("smtp", {}) or {}
                max_retry = int(((self.cfg.get("email", {}) or {}).get("queue", {}) or {}).get("max_retry", 10))

                for job in due:
                    if self._stop.is_set():
                        break
                    self.db.mark_sending(job.id)
                    try:
                        send_smtp_email(
                            smtp_cfg=smtp_cfg,
                            to_addrs=job.to_addrs,
                            subject=job.subject,
                            body_html=job.body_html,
                            attachments=job.attachments,
                        )
                        self.db.mark_sent(job.id)
                    except EmailSendError as e:
                        rc = int(job.retry_count) + 1
                        if rc >= max_retry:
                            self.db.mark_failed(job.id, retry_count=rc, next_retry_ts=time.time() + 86400, err=str(e), terminal=True)
                        else:
                            backoff = RETRY_SECONDS[min(rc - 1, len(RETRY_SECONDS) - 1)]
                            self.db.mark_failed(job.id, retry_count=rc, next_retry_ts=time.time() + backoff, err=str(e), terminal=False)
            except Exception:
                time.sleep(1.0)

    def _schedule_loop(self):
        while not self._stop.is_set():
            try:
                ecfg = (self.cfg.get("email", {}) or {})
                daily = ecfg.get("daily", {}) or {}
                if not bool(daily.get("enabled", False)):
                    time.sleep(1.0)
                    continue

                t = str(daily.get("time", "23:59")).strip()
                hh, mm = 23, 59
                try:
                    parts = t.split(":")
                    hh, mm = int(parts[0]), int(parts[1])
                except Exception:
                    pass

                now = datetime.now()
                key = now.strftime("%Y-%m-%d")

                # Days-of-week filter: 0=Mon … 6=Sun. Empty list = all days.
                allowed_days = daily.get("days", None)
                if allowed_days is not None and isinstance(allowed_days, list) and len(allowed_days) > 0:
                    if now.weekday() not in allowed_days:
                        time.sleep(1.0)
                        continue

                # Fire within a 60-second window, once per day
                if now.hour == hh and now.minute == mm and self._last_daily_date != key:
                    self._last_daily_date = key
                    self._enqueue_daily_report_email(generated_by="AUTO")
                time.sleep(1.0)
            except Exception:
                time.sleep(1.0)

    def _enqueue_daily_report_email(self, generated_by: str = "AUTO", override_recipients: Optional[List[str]] = None) -> None:
        # Gather recipients (or use override)
        recs = list(override_recipients or [])
        if not recs:
            for r in (self.cfg.get("email", {}) or {}).get("recipients", []) or []:
                if not isinstance(r, dict):
                    continue
                if bool(r.get("enabled", True)):
                    email = str(r.get("email", "")).strip()
                    if email:
                        recs.append(email)

        if not recs:
            log.info("Daily email skipped: no recipients")
            return

        site = (self.cfg.get("site", {}) or {})
        facility = str(site.get("plant_name", "")) or "Facility"

        # Use ReportsEngine to generate a DAILY report for last 24h.
        # IMPORTANT: generate_now() guarantees we get *this* request's output,
        # not a different report that might be in the async queue.
        end = time.time()
        start = end - 24 * 3600

        ecfg = (self.cfg.get("email", {}) or {})
        content = (ecfg.get("content", {}) or {})
        outfmt = str(content.get("report_output", "HTML") or "HTML").upper()
        req = ReportRequest(
            report_type="DAILY",
            start_ts=start,
            end_ts=end,
            meter_ids=[],
            generated_by=generated_by,
            output=outfmt,
        )

        report_path = ""
        try:
            res = self.reports_engine.generate_now(req)
            if res.ok:
                report_path = res.file_path
            else:
                log.warning("Daily report generation failed: %s", res.error)
        except Exception as e:
            log.warning("Daily report generation exception: %s", e)

        # Attachments (selected by checklist)
        attachments: List[str] = []

        if bool(content.get("attach_report", True)) and report_path and os.path.exists(report_path):
            attachments.append(report_path)

        # Alarm/events CSV
        if bool(content.get("attach_alarm_events", False)):
            p = self.reports_engine.protection_events_csv
            if p and os.path.exists(p):
                attachments.append(p)

        # Power log CSVs (TOTAL + meters included)
        if bool(content.get("attach_power_logs", False)):
            try:
                folder = (self.cfg.get("logging", {}) or {}).get("folder", "") or logs_dir()
                sources = ["TOTAL"]
                # attach ALL enabled meters (same scope as report)
                enabled_ids = [m.meter_id for m in self.reports_engine.meters if getattr(m, "enabled", False)]
                sources += [f"M{i}" for i in enabled_ids]
                files = find_log_files(folder, sources=sources, start=datetime.fromtimestamp(start), end=datetime.fromtimestamp(end))
                for src, fps in (files or {}).items():
                    for fp in fps:
                        if fp and os.path.exists(fp) and fp not in attachments:
                            attachments.append(fp)
            except Exception:
                pass

        # Chart snapshots (attach last few PNGs from today's charts folder)
        if bool(content.get("attach_chart_snapshots", False)):
            try:
                date_folder = datetime.now().strftime("%Y-%m-%d")
                charts_dir = os.path.join(logs_dir(), date_folder, "charts")
                if os.path.isdir(charts_dir):
                    pngs = [os.path.join(charts_dir, f) for f in os.listdir(charts_dir) if f.lower().endswith(".png")]
                    pngs.sort(key=lambda p: os.path.getmtime(p), reverse=True)
                    for p in pngs[: int(content.get("snapshots_max", 3))]:
                        if p not in attachments:
                            attachments.append(p)
            except Exception:
                pass

        subject = f"[{(self.cfg.get('reports', {}) or {}).get('facility_code', 'PLANT')}] Daily Power Report - {datetime.now().strftime('%Y-%m-%d')}"
        notes = [f"Daily report attached ({outfmt})."]
        if outfmt == "HTML":
            notes.append("Open in browser and Print → Save as PDF if needed.")

        base_body = build_simple_html(
            title="Daily Electrical Power Report",
            subtitle="Automated email",
            facility_name=facility,
            notes=notes + ([f"Attachments will follow your checklist."] if recs else []),
        )

        # Inline parameters snapshot (prepare once)
        snap = {}
        try:
            snap = self.reports_engine.snapshot_fn() or {}
        except Exception:
            snap = {}
        inline_keys = content.get("inline_keys") or ["Vavg", "Iavg", "kW", "PFavg", "Frequency"]

        # Precompute shared attachment pools (we pick subsets per recipient)
        alarm_csv_path = self.reports_engine.protection_events_csv

        power_csv_files: List[str] = []
        if bool(content.get("attach_power_logs", False)):
            # already built into attachments list earlier; reuse by re-scanning quickly
            try:
                folder = (self.cfg.get("logging", {}) or {}).get("folder", "") or logs_dir()
                sources = ["TOTAL"]
                enabled_ids = [m.meter_id for m in self.reports_engine.meters if getattr(m, "enabled", False)]
                sources += [f"M{i}" for i in enabled_ids]
                files = find_log_files(folder, sources=sources, start=datetime.fromtimestamp(start), end=datetime.fromtimestamp(end))
                for _src, fps in (files or {}).items():
                    for fp in fps:
                        if fp and os.path.exists(fp) and fp not in power_csv_files:
                            power_csv_files.append(fp)
            except Exception:
                power_csv_files = []

        chart_pngs: List[str] = []
        if bool(content.get("attach_chart_snapshots", False)):
            try:
                date_folder = datetime.now().strftime("%Y-%m-%d")
                charts_dir = os.path.join(logs_dir(), date_folder, "charts")
                if os.path.isdir(charts_dir):
                    pngs = [os.path.join(charts_dir, f) for f in os.listdir(charts_dir) if f.lower().endswith(".png")]
                    pngs.sort(key=lambda p: os.path.getmtime(p), reverse=True)
                    chart_pngs = pngs[: int(content.get("snapshots_max", 3))]
            except Exception:
                chart_pngs = []

        # Per-recipient checklist overrides: queue one email per recipient
        rec_dicts = self._enabled_recipient_dicts() if not override_recipients else [{"email": e} for e in recs]
        for r in rec_dicts:
            email = str(r.get("email", "")).strip()
            if not email:
                continue

            # Effective checklist
            send_report = self._effective_bool(r, "send_report", bool(content.get("attach_report", True)))
            send_alarm = self._effective_bool(r, "send_alarm_events", bool(content.get("attach_alarm_events", False)))
            send_power = self._effective_bool(r, "send_power_logs", bool(content.get("attach_power_logs", False)))
            send_snap = self._effective_bool(r, "send_chart_snapshots", bool(content.get("attach_chart_snapshots", False)))
            send_inline = self._effective_bool(r, "send_inline_params", bool(content.get("inline_params", False)))

            att: List[str] = []
            if send_report and report_path and os.path.exists(report_path):
                att.append(report_path)
            if send_alarm and alarm_csv_path and os.path.exists(alarm_csv_path):
                att.append(alarm_csv_path)
            if send_power:
                for fp in power_csv_files:
                    if fp not in att:
                        att.append(fp)
            if send_snap:
                for fp in chart_pngs:
                    if fp and os.path.exists(fp) and fp not in att:
                        att.append(fp)

            body = base_body
            if send_inline:
                try:
                    body = body.replace("</div>\n        </div>", build_params_table(snap, inline_keys) + "</div>\n        </div>")
                except Exception:
                    pass

            self.enqueue_email(to_addrs=[email], subject=subject, body_html=body, attachments=att)



    def _alarm_watch_loop(self) -> None:
        """Watch protection_events.csv and enqueue emails on new ALARM_ON rows.

        This is intentionally simple and robust: it tails the CSV file and only reacts to newly appended lines.
        """
        last_size = 0
        last_mtime = 0.0
        while not self._alarm_stop.is_set():
            try:
                ecfg = (self.cfg.get("email", {}) or {})
                tcfg = (ecfg.get("triggers", {}) or {})
                if not bool(tcfg.get("protection_alarm", False)):
                    time.sleep(1.0)
                    continue

                path = self.reports_engine.protection_events_csv
                if (not path) or (not os.path.exists(path)):
                    time.sleep(1.0)
                    continue

                st = os.stat(path)
                if st.st_mtime != last_mtime:
                    last_mtime = st.st_mtime
                # Truncate reset
                if st.st_size < last_size:
                    last_size = 0

                if st.st_size == last_size:
                    time.sleep(0.8)
                    continue

                # Read newly appended content
                with open(path, "r", encoding="utf-8", newline="") as f:
                    f.seek(last_size)
                    chunk = f.read()
                    last_size = st.st_size

                lines = [ln for ln in chunk.splitlines() if ln.strip()]
                if not lines:
                    time.sleep(0.5)
                    continue

                # Parse CSV rows naively: use DictReader on full file if header appears in chunk.
                # For reliability, re-open and read last ~50 lines.
                try:
                    with open(path, "r", encoding="utf-8", newline="") as f:
                        all_lines = f.read().splitlines()
                    tail = all_lines[-60:]
                    import csv
                    from io import StringIO

                    rdr = csv.DictReader(StringIO("\n".join(tail)))
                    new_rows = list(rdr)
                except Exception:
                    new_rows = []

                # Filter ALARM_ON / ACTIVE rows.
                for row in new_rows[-15:]:
                    state = str(row.get("state") or row.get("State") or row.get("status") or row.get("Status") or "").upper()
                    if state not in ("ALARM_ON", "ACTIVE", "TRIP", "ON"):
                        continue
                    rule = str(row.get("rule") or row.get("Rule") or row.get("alarm") or row.get("Alarm") or "Protection Alarm")
                    meter = str(row.get("meter") or row.get("Meter") or row.get("meter_id") or row.get("Meter ID") or "")
                    val = str(row.get("value") or row.get("Value") or "")

                    rec_dicts = self._enabled_recipient_dicts()
                    if not rec_dicts:
                        continue

                    subject = f"[{(self.cfg.get('reports', {}) or {}).get('facility_code', 'PLANT')}] ALARM: {rule}"
                    notes = [
                        f"Rule: {rule}",
                        f"Meter: {meter}",
                        f"Value: {val}",
                    ]
                    body = build_simple_html(
                        title="Protection Alarm Trigger",
                        subtitle="Automated alarm email",
                        facility_name=str((self.cfg.get('site', {}) or {}).get('plant_name', '') or 'Facility'),
                        notes=notes,
                    )

                    # Attachments follow checklist (per recipient overrides)
                    content = (ecfg.get("content", {}) or {})

                    # Precompute chart pngs once per trigger
                    chart_pngs: List[str] = []
                    if bool(content.get("attach_chart_snapshots", False)):
                        try:
                            date_folder = datetime.now().strftime("%Y-%m-%d")
                            charts_dir = os.path.join(logs_dir(), date_folder, "charts")
                            if os.path.isdir(charts_dir):
                                pngs = [os.path.join(charts_dir, f) for f in os.listdir(charts_dir) if f.lower().endswith(".png")]
                                pngs.sort(key=lambda p: os.path.getmtime(p), reverse=True)
                                chart_pngs = pngs[: int(content.get("snapshots_max", 3))]
                        except Exception:
                            chart_pngs = []

                    for r in rec_dicts:
                        email = str(r.get("email", "")).strip()
                        if not email:
                            continue
                        send_alarm_csv = self._effective_bool(r, "send_alarm_events", bool(content.get("attach_alarm_events", True)))
                        send_snap = self._effective_bool(r, "send_chart_snapshots", bool(content.get("attach_chart_snapshots", False)))

                        attachments: List[str] = []
                        if send_alarm_csv:
                            attachments.append(path)
                        if send_snap:
                            for pp in chart_pngs:
                                if pp and os.path.exists(pp) and pp not in attachments:
                                    attachments.append(pp)

                        self.enqueue_email(to_addrs=[email], subject=subject, body_html=body, attachments=attachments)

            except Exception:
                time.sleep(1.0)
