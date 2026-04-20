import re
import threading
import time
import tkinter as tk
from tkinter import ttk, messagebox
from datetime import datetime
from typing import List

from core.email_templates import build_simple_html


EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


class EmailTab(ttk.Frame):
    """Email tab (Phase 1): SMTP config, recipients, queue, manual send, daily schedule."""

    PROVIDERS = ["CUSTOM", "GMAIL", "OUTLOOK"]
    SECOPTS = ["TLS", "SSL", "NONE"]

    def __init__(self, parent, cfg, email_service, on_save_config=None):
        super().__init__(parent)
        self.cfg = cfg
        self._privacy = bool((self.cfg.get("ui") or {}).get("privacy_mode", True))
        # For privacy-safe UI: keep raw recipient emails out of the visible table.
        self._rec_raw = {}
        self.email_service = email_service
        self.on_save_config = on_save_config

        self._build_ui()
        self.after(800, self._tick)

    # ------------------------------------------------------------------
    def _build_ui(self):
        self.columnconfigure(0, weight=1)
        # NOTE: Do not force an expanding row here.
        # This tab may be embedded inside a ScrollableContainer; row weights can block scrolling.
        self.rowconfigure(4, weight=0)

        # ---- SMTP settings ----
        smtp_box = ttk.Labelframe(self, text="SMTP Configuration")
        smtp_box.grid(row=0, column=0, sticky="ew", padx=10, pady=8)
        for i in range(8):
            smtp_box.columnconfigure(i, weight=1)

        ecfg = (self.cfg.get("email", {}) or {})
        scfg = (ecfg.get("smtp", {}) or {})

        self.provider_var = tk.StringVar(value=str(scfg.get("provider", "CUSTOM")))
        self.server_var = tk.StringVar(value=str(scfg.get("server", "")))
        self.port_var = tk.StringVar(value=str(scfg.get("port", 587)))
        self.sec_var = tk.StringVar(value=str(scfg.get("security", "TLS")).upper())
        self.user_var = tk.StringVar(value=str(scfg.get("username", "")))
        self.pass_var = tk.StringVar(value=str(scfg.get("password", "")))
        self.from_var = tk.StringVar(value=str(scfg.get("from_addr", "")))
        self.name_var = tk.StringVar(value=str(scfg.get("display_name", "")))

        ttk.Label(smtp_box, text="Provider").grid(row=0, column=0, sticky="w", padx=6, pady=6)
        ttk.Combobox(smtp_box, values=self.PROVIDERS, textvariable=self.provider_var, width=10, state="readonly").grid(row=0, column=1, sticky="ew", padx=6)

        ttk.Label(smtp_box, text="Server").grid(row=0, column=2, sticky="w", padx=6)
        ttk.Entry(smtp_box, textvariable=self.server_var).grid(row=0, column=3, sticky="ew", padx=6)

        ttk.Label(smtp_box, text="Port").grid(row=0, column=4, sticky="w", padx=6)
        ttk.Entry(smtp_box, textvariable=self.port_var, width=6).grid(row=0, column=5, sticky="w", padx=6)

        ttk.Label(smtp_box, text="Security").grid(row=0, column=6, sticky="w", padx=6)
        ttk.Combobox(smtp_box, values=self.SECOPTS, textvariable=self.sec_var, width=7, state="readonly").grid(row=0, column=7, sticky="w", padx=6)

        ttk.Label(smtp_box, text="Username").grid(row=1, column=0, sticky="w", padx=6, pady=6)
        ttk.Entry(smtp_box, textvariable=self.user_var).grid(row=1, column=1, columnspan=2, sticky="ew", padx=6)

        ttk.Label(smtp_box, text="Password").grid(row=1, column=3, sticky="w", padx=6)
        ttk.Entry(smtp_box, textvariable=self.pass_var, show="*").grid(row=1, column=4, columnspan=2, sticky="ew", padx=6)

        ttk.Label(smtp_box, text="From").grid(row=1, column=6, sticky="w", padx=6)
        ttk.Entry(smtp_box, textvariable=self.from_var).grid(row=1, column=7, sticky="ew", padx=6)

        ttk.Label(smtp_box, text="Display Name").grid(row=2, column=0, sticky="w", padx=6, pady=6)
        ttk.Entry(smtp_box, textvariable=self.name_var).grid(row=2, column=1, columnspan=3, sticky="ew", padx=6)

        ttk.Button(smtp_box, text="Save", command=self._save_smtp).grid(row=2, column=6, sticky="e", padx=6, pady=6)
        ttk.Button(smtp_box, text="Test Email", command=self._test_email).grid(row=2, column=7, sticky="e", padx=6, pady=6)

        # ---- Recipients ----
        rec_box = ttk.Labelframe(self, text="Recipients (max 10)")
        rec_box.grid(row=1, column=0, sticky="ew", padx=10)
        rec_box.columnconfigure(0, weight=1)

        self.rec_tree = ttk.Treeview(rec_box, columns=("enabled", "name", "email"), show="headings", height=5)
        self.rec_tree.heading("enabled", text="Enable")
        self.rec_tree.heading("name", text="Name")
        self.rec_tree.heading("email", text="Email")
        self.rec_tree.column("enabled", width=70, anchor="center")
        self.rec_tree.column("name", width=180)
        self.rec_tree.column("email", width=260)
        self.rec_tree.grid(row=0, column=0, sticky="ew", padx=6, pady=6)

        btns = ttk.Frame(rec_box)
        btns.grid(row=0, column=1, sticky="ns", padx=6)
        ttk.Button(btns, text="Add", command=self._add_rec).pack(fill="x", pady=(6, 4))
        ttk.Button(btns, text="Remove", command=self._remove_rec).pack(fill="x", pady=4)
        ttk.Button(btns, text="Toggle", command=self._toggle_rec).pack(fill="x", pady=4)

        self._load_recipients()

        # ---- Per-recipient checklist overrides ----
        ov_box = ttk.Labelframe(self, text="Recipient Content Overrides (selected recipient)")
        ov_box.grid(row=2, column=0, sticky="ew", padx=10, pady=(0, 8))
        for i in range(8):
            ov_box.columnconfigure(i, weight=1)

        self._sel_rec_idx = None
        self.r_use_global = tk.BooleanVar(value=True)
        self.r_send_report = tk.BooleanVar(value=True)
        self.r_send_alarm = tk.BooleanVar(value=False)
        self.r_send_power = tk.BooleanVar(value=False)
        self.r_send_snap = tk.BooleanVar(value=False)
        self.r_send_inline = tk.BooleanVar(value=False)

        ttk.Checkbutton(
            ov_box,
            text="Use global checklist (ignore overrides)",
            variable=self.r_use_global,
            command=self._update_override_enable,
        ).grid(row=0, column=0, columnspan=8, sticky="w", padx=6, pady=(6, 2))

        w1 = ttk.Checkbutton(ov_box, text="Daily Report", variable=self.r_send_report)
        w2 = ttk.Checkbutton(ov_box, text="Alarm Events (CSV)", variable=self.r_send_alarm)
        w3 = ttk.Checkbutton(ov_box, text="Power Logs (CSV)", variable=self.r_send_power)
        w4 = ttk.Checkbutton(ov_box, text="Chart Snapshots (PNG)", variable=self.r_send_snap)
        w5 = ttk.Checkbutton(ov_box, text="Inline Parameters", variable=self.r_send_inline)

        w1.grid(row=1, column=0, sticky="w", padx=6, pady=4)
        w2.grid(row=1, column=1, sticky="w", padx=6, pady=4)
        w3.grid(row=1, column=2, sticky="w", padx=6, pady=4)
        w4.grid(row=1, column=3, sticky="w", padx=6, pady=4)
        w5.grid(row=1, column=4, sticky="w", padx=6, pady=4)

        self._ov_widgets = [w1, w2, w3, w4, w5]

        ttk.Button(ov_box, text="Save to Selected Recipient", command=self._save_selected_recipient_overrides).grid(
            row=1, column=7, sticky="e", padx=6
        )
        ttk.Label(ov_box, text="(Select a recipient above. Overrides apply to Daily + Alarm-trigger emails.)").grid(
            row=2, column=0, columnspan=8, sticky="w", padx=6, pady=(0, 6)
        )

        self.rec_tree.bind("<<TreeviewSelect>>", self._on_recipient_select)
        self._update_override_enable()

        # ---- Manual send + daily schedule ----
        send_box = ttk.Labelframe(self, text="Email Send (Daily / Manual) + What To Send")
        send_box.grid(row=3, column=0, sticky="ew", padx=10, pady=8)
        # Keep a predictable layout: left controls compact, middle stretches, right buttons align.
        for i in range(10):
            send_box.columnconfigure(i, weight=0)
        send_box.columnconfigure(5, weight=1)
        send_box.columnconfigure(6, weight=1)

        ecfg = (self.cfg.get("email", {}) or {})
        dcfg = ecfg.get("daily", {}) or {}
        ccfg = ecfg.get("content", {}) or {}

        self.daily_enable = tk.BooleanVar(value=bool(dcfg.get("enabled", False)))
        self.daily_time = tk.StringVar(value=str(dcfg.get("time", "23:59")))

        # What to send (checklist)
        self.c_attach_report = tk.BooleanVar(value=bool(ccfg.get("attach_report", True)))
        self.c_report_output = tk.StringVar(value=str(ccfg.get("report_output", "HTML")).upper())
        self.c_attach_alarm = tk.BooleanVar(value=bool(ccfg.get("attach_alarm_events", False)))
        self.c_attach_power = tk.BooleanVar(value=bool(ccfg.get("attach_power_logs", False)))
        self.c_attach_snap = tk.BooleanVar(value=bool(ccfg.get("attach_chart_snapshots", False)))
        self.c_inline_params = tk.BooleanVar(value=bool(ccfg.get("inline_params", False)))

        # Alarm-triggered emails (Phase 2)
        tcfg = ecfg.get("triggers", {}) or {}
        self.t_alarm_enable = tk.BooleanVar(value=bool(tcfg.get("protection_alarm", False)))

        ttk.Checkbutton(send_box, text="Enable scheduled email", variable=self.daily_enable, command=self._save_daily).grid(row=0, column=0, sticky="w", padx=6, pady=6)
        ttk.Label(send_box, text="Send time (HH:MM)").grid(row=0, column=1, sticky="w", padx=6)
        ttk.Entry(send_box, textvariable=self.daily_time, width=8).grid(row=0, column=2, sticky="w", padx=6)

        ttk.Label(send_box, text="Report output").grid(row=0, column=3, sticky="w", padx=6)
        ttk.Combobox(send_box, values=["HTML", "PDF"], textvariable=self.c_report_output, width=6, state="readonly").grid(row=0, column=4, sticky="w", padx=6)

        self.send_progress = ttk.Progressbar(send_box, mode="indeterminate")
        self.send_progress.grid(row=0, column=7, sticky="e", padx=6)
        ttk.Button(send_box, text="Save Settings", command=self._save_daily).grid(row=0, column=8, sticky="e", padx=6)
        ttk.Button(send_box, text="Send Now", command=self._send_daily_now).grid(row=0, column=9, sticky="e", padx=6)

        # Days-of-week row
        days_row = ttk.Frame(send_box)
        days_row.grid(row=1, column=0, columnspan=10, sticky="ew", padx=6, pady=(0, 4))
        ttk.Label(days_row, text="Send on days:").pack(side="left", padx=(0, 8))
        _day_names = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
        _saved_days = set(dcfg.get("days", list(range(7))) or list(range(7)))
        self._day_vars = []
        for i, name in enumerate(_day_names):
            v = tk.BooleanVar(value=(i in _saved_days))
            ttk.Checkbutton(days_row, text=name, variable=v, command=self._save_daily).pack(side="left", padx=2)
            self._day_vars.append(v)
        ttk.Label(days_row, text="(uncheck to skip specific days)", style="Muted.TLabel").pack(side="left", padx=(12, 0))

        # Checklist row
        chk = ttk.Frame(send_box)
        chk.grid(row=2, column=0, columnspan=10, sticky="ew", padx=6, pady=(0, 6))
        for i in range(6):
            chk.columnconfigure(i, weight=1)

        ttk.Label(chk, text="What to Send (Checklist)").grid(row=0, column=0, sticky="w")
        ttk.Checkbutton(chk, text="Daily PDF / HTML Report", variable=self.c_attach_report, command=self._save_daily).grid(row=0, column=1, sticky="w", padx=8)
        ttk.Checkbutton(chk, text="Alarm Events (CSV)", variable=self.c_attach_alarm, command=self._save_daily).grid(row=0, column=2, sticky="w", padx=8)
        ttk.Checkbutton(chk, text="Power Logs (CSV)", variable=self.c_attach_power, command=self._save_daily).grid(row=0, column=3, sticky="w", padx=8)
        ttk.Checkbutton(chk, text="Chart Snapshots (PNG)", variable=self.c_attach_snap, command=self._save_daily).grid(row=0, column=4, sticky="w", padx=8)
        ttk.Checkbutton(chk, text="Current Parameters (inline HTML)", variable=self.c_inline_params, command=self._save_daily).grid(row=0, column=5, sticky="w", padx=8)

        # Triggers row
        trig = ttk.Frame(send_box)
        trig.grid(row=3, column=0, columnspan=10, sticky="ew", padx=6, pady=(0, 6))
        trig.columnconfigure(1, weight=1)
        ttk.Checkbutton(trig, text="Enable alarm-triggered emails (Protection ALARM_ON)", variable=self.t_alarm_enable, command=self._save_daily).grid(row=0, column=0, sticky="w")
        ttk.Label(trig, text="(Uses checklist above. Make sure recipients are enabled.)").grid(row=0, column=1, sticky="w", padx=10)

        # ---- Queue ----
        q_box = ttk.Labelframe(self, text="Email Queue")
        q_box.grid(row=4, column=0, sticky="nsew", padx=10, pady=(0, 10))
        q_box.columnconfigure(0, weight=1)
        q_box.rowconfigure(0, weight=0)

        self.q_tree = ttk.Treeview(q_box, columns=("time", "to", "subject", "status", "retry"), show="headings", height=8)
        for c, w in [("time", 150), ("to", 220), ("subject", 280), ("status", 90), ("retry", 60)]:
            self.q_tree.heading(c, text=c.title())
            self.q_tree.column(c, width=w, anchor="w")
        self.q_tree.column("retry", anchor="center")
        self.q_tree.grid(row=0, column=0, sticky="nsew", padx=6, pady=6)

        qbtn = ttk.Frame(q_box)
        qbtn.grid(row=0, column=1, sticky="ns", padx=6)
        ttk.Button(qbtn, text="Refresh", command=self._refresh_queue).pack(fill="x", pady=(6, 4))
        ttk.Button(qbtn, text="Clear Sent", command=self._clear_sent).pack(fill="x", pady=4)

        self._refresh_queue()

    # ------------------------------------------------------------------
    def _save_smtp(self):
        try:
            port = int(self.port_var.get().strip() or "587")
        except Exception:
            port = 587
        ecfg = self.cfg.setdefault("email", {})
        scfg = ecfg.setdefault("smtp", {})
        scfg.update(
            {
                "provider": self.provider_var.get().strip() or "CUSTOM",
                "server": self.server_var.get().strip(),
                "port": port,
                "security": self.sec_var.get().strip().upper() or "TLS",
                "username": self.user_var.get().strip(),
                "password": self.pass_var.get(),
                "from_addr": self.from_var.get().strip(),
                "display_name": self.name_var.get().strip(),
            }
        )
        if self.on_save_config:
            self.on_save_config()
        messagebox.showinfo("Email", "SMTP settings saved")

    def _test_email(self):
        self._save_smtp()
        recs = self._enabled_emails()
        if not recs:
            messagebox.showwarning("Email", "No enabled recipients")
            return
        site = (self.cfg.get("site", {}) or {})
        facility = str(site.get("plant_name", "")) or "Facility"
        subject = f"[{(self.cfg.get('reports', {}) or {}).get('facility_code','PLANT')}] Test Email"
        body = build_simple_html(title="Test Email", subtitle="SMTP test from MFM384 Monitoring", facility_name=facility)

        def run():
            self.send_progress.start(10)
            try:
                self.email_service.enqueue_email(recs, subject, body, attachments=[])
                messagebox.showinfo("Email", "Queued. Check Email Queue status.")
            finally:
                self.send_progress.stop()
                self._refresh_queue()

        threading.Thread(target=run, daemon=True).start()

    def _load_recipients(self):
        for i in self.rec_tree.get_children():
            self.rec_tree.delete(i)
        self._rec_raw = {}
        recs = (self.cfg.get("email", {}) or {}).get("recipients", []) or []
        for idx, r in enumerate(recs):
            if not isinstance(r, dict):
                continue
            en = "Yes" if bool(r.get("enabled", True)) else "No"
            raw_email = str(r.get("email", ""))
            self._rec_raw[str(idx)] = raw_email
            disp_email = self._mask_email(raw_email) if self._privacy else raw_email
            self.rec_tree.insert("", "end", iid=str(idx), values=(en, str(r.get("name", "")), disp_email))

    def _save_recipients(self):
        recs = []
        for iid in self.rec_tree.get_children():
            en, name, email = self.rec_tree.item(iid, "values")
            raw = self._rec_raw.get(str(iid), str(email))
            recs.append({"enabled": (en == "Yes"), "name": name, "email": raw})
        self.cfg.setdefault("email", {})["recipients"] = recs
        if self.on_save_config:
            self.on_save_config()

    def _mask_email(self, email: str) -> str:
        """Privacy-safe email display: keep domain, mask local-part."""
        e = (email or "").strip()
        if "@" not in e:
            return "(set)" if e else ""
        local, domain = e.split("@", 1)
        if len(local) <= 2:
            masked = "*" * len(local)
        else:
            masked = local[:2] + "***"
        return f"{masked}@{domain}"

    def _reindex_recipients(self):
        """Rebuild iids (0..n-1) and keep raw-email cache consistent."""
        items = [self.rec_tree.item(i, "values") for i in self.rec_tree.get_children()]
        raw_by_old = {str(i): self._rec_raw.get(str(i), "") for i in self._rec_raw.keys()}

        for i in self.rec_tree.get_children():
            self.rec_tree.delete(i)
        self._rec_raw = {}
        for new_idx, vals in enumerate(items):
            en, name, disp_email = vals
            # If we have a cached raw for the old row, keep it; otherwise fall back.
            # In practice, add/edit routes will update _rec_raw.
            raw = ""
            # Try to recover from display if needed
            raw = raw_by_old.get(str(new_idx), "") or raw_by_old.get(str(new_idx), "")
            if not raw and not self._privacy:
                raw = str(disp_email)
            self._rec_raw[str(new_idx)] = raw
            self.rec_tree.insert("", "end", iid=str(new_idx), values=(en, name, (self._mask_email(raw) if self._privacy else raw)))

    def _add_rec(self):
        recs = (self.cfg.get("email", {}) or {}).get("recipients", []) or []
        if len(recs) >= 10:
            messagebox.showwarning("Recipients", "Max 10 recipients")
            return

        win = tk.Toplevel(self)
        win.title("Add Recipient")
        win.resizable(False, False)

        name_var = tk.StringVar(value="")
        email_var = tk.StringVar(value="")

        ttk.Label(win, text="Name").grid(row=0, column=0, padx=10, pady=8, sticky="w")
        ttk.Entry(win, textvariable=name_var, width=30).grid(row=0, column=1, padx=10, pady=8)

        ttk.Label(win, text="Email").grid(row=1, column=0, padx=10, pady=8, sticky="w")
        ttk.Entry(win, textvariable=email_var, width=30).grid(row=1, column=1, padx=10, pady=8)

        def ok():
            email = email_var.get().strip()
            if not EMAIL_RE.match(email):
                messagebox.showerror("Recipient", "Invalid email")
                return
            name = name_var.get().strip() or ""
            idx = len(self.rec_tree.get_children())
            iid = str(idx)
            self._rec_raw[iid] = email
            disp = self._mask_email(email) if self._privacy else email
            self.rec_tree.insert("", "end", iid=iid, values=("Yes", name, disp))
            self._save_recipients()
            win.destroy()

        ttk.Button(win, text="Add", command=ok).grid(row=2, column=1, padx=10, pady=10, sticky="e")

    def _remove_rec(self):
        sel = self.rec_tree.selection()
        if not sel:
            return
        # Delete selected rows
        for iid in sel:
            self.rec_tree.delete(iid)
            self._rec_raw.pop(str(iid), None)

        # Rebuild sequential iids and keep raw-email cache consistent
        remaining = []
        for iid in self.rec_tree.get_children():
            en, name, _disp = self.rec_tree.item(iid, "values")
            raw = self._rec_raw.get(str(iid), "")
            remaining.append((en, name, raw))

        for i in self.rec_tree.get_children():
            self.rec_tree.delete(i)
        self._rec_raw = {}
        for idx, (en, name, raw) in enumerate(remaining):
            iid = str(idx)
            self._rec_raw[iid] = raw
            disp = self._mask_email(raw) if self._privacy else raw
            self.rec_tree.insert("", "end", iid=iid, values=(en, name, disp))
        self._save_recipients()

    def _toggle_rec(self):
        sel = self.rec_tree.selection()
        if not sel:
            return
        for iid in sel:
            en, name, email = self.rec_tree.item(iid, "values")
            self.rec_tree.item(iid, values=(("No" if en == "Yes" else "Yes"), name, email))
        self._save_recipients()

    def _enabled_emails(self) -> List[str]:
        out = []
        for iid in self.rec_tree.get_children():
            en, _, _disp = self.rec_tree.item(iid, "values")
            raw = self._rec_raw.get(str(iid), "")
            if en == "Yes" and raw:
                out.append(raw)
        return out

    def _save_daily(self):
        ecfg = self.cfg.setdefault("email", {})
        d = ecfg.setdefault("daily", {})
        d["enabled"] = bool(self.daily_enable.get())
        d["time"] = self.daily_time.get().strip() or "23:59"
        d["days"] = [i for i, v in enumerate(getattr(self, "_day_vars", [])) if v.get()]

        # What-to-send checklist
        c = ecfg.setdefault("content", {})
        c["report_output"] = (self.c_report_output.get().strip() or "HTML").upper()
        c["attach_report"] = bool(self.c_attach_report.get())
        c["attach_alarm_events"] = bool(self.c_attach_alarm.get())
        c["attach_power_logs"] = bool(self.c_attach_power.get())
        c["attach_chart_snapshots"] = bool(self.c_attach_snap.get())
        c["inline_params"] = bool(self.c_inline_params.get())

        # Triggers (Phase 2)
        t = ecfg.setdefault("triggers", {})
        t["protection_alarm"] = bool(self.t_alarm_enable.get())

        if self.on_save_config:
            self.on_save_config()

    def _send_daily_now(self):
        self._save_smtp()
        self._save_recipients()
        recs = self._enabled_emails()
        if not recs:
            messagebox.showwarning("Email", "No enabled recipients")
            return

        def run():
            self.send_progress.start(10)
            try:
                # Generate daily report (last 24h) and queue one email with attachment
                self.email_service.enqueue_daily_report_manual(recs)
                messagebox.showinfo("Email", "Queued daily email. Check queue status.")
            finally:
                self.send_progress.stop()
                self._refresh_queue()

        threading.Thread(target=run, daemon=True).start()

    def _refresh_queue(self):
        for i in self.q_tree.get_children():
            self.q_tree.delete(i)
        jobs = self.email_service.list_queue(limit=200)
        for j in reversed(jobs):
            ts = datetime.fromtimestamp(j.created_ts).strftime("%Y-%m-%d %H:%M:%S")
            to_s = ",".join(j.to_addrs)[:60]
            subj = j.subject[:80]
            self.q_tree.insert("", "end", values=(ts, to_s, subj, j.status, str(j.retry_count)))

    def _clear_sent(self):
        n = self.email_service.delete_sent()
        self._refresh_queue()
        messagebox.showinfo("Email", f"Cleared {n} sent emails")

    # ------------------------------------------------------------------
    # Per-recipient override helpers
    # ------------------------------------------------------------------
    def _update_override_enable(self):
        use_global = bool(self.r_use_global.get())
        state = "disabled" if use_global else "normal"
        for w in (getattr(self, '_ov_widgets', None) or []):
            try:
                w.configure(state=state)
            except Exception:
                pass

    def _on_recipient_select(self, _evt=None):
        sel = self.rec_tree.selection()
        if not sel:
            self._sel_rec_idx = None
            return
        try:
            self._sel_rec_idx = int(sel[0])
        except Exception:
            self._sel_rec_idx = None
            return

        ecfg = (self.cfg.get('email', {}) or {})
        g = (ecfg.get('content', {}) or {})
        recs = ecfg.get('recipients', []) or []
        if self._sel_rec_idx < 0 or self._sel_rec_idx >= len(recs):
            return
        r = recs[self._sel_rec_idx] if isinstance(recs[self._sel_rec_idx], dict) else {}

        # None => inherit global
        def eff(key, gkey, default=False):
            v = r.get(key, None)
            if v is None:
                return bool(g.get(gkey, default))
            return bool(v)

        # Determine if this recipient is using global
        uses_global = all(r.get(k, None) is None for k in (
            'send_report','send_alarm_events','send_power_logs','send_chart_snapshots','send_inline_params'
        ))
        self.r_use_global.set(bool(uses_global))
        self.r_send_report.set(eff('send_report', 'attach_report', True))
        self.r_send_alarm.set(eff('send_alarm_events', 'attach_alarm_events', False))
        self.r_send_power.set(eff('send_power_logs', 'attach_power_logs', False))
        self.r_send_snap.set(eff('send_chart_snapshots', 'attach_chart_snapshots', False))
        self.r_send_inline.set(eff('send_inline_params', 'inline_params', False))

        self._update_override_enable()

    def _save_selected_recipient_overrides(self):
        if self._sel_rec_idx is None:
            messagebox.showwarning('Recipients', 'Select a recipient first')
            return
        ecfg = self.cfg.setdefault('email', {})
        recs = ecfg.setdefault('recipients', [])
        if self._sel_rec_idx < 0 or self._sel_rec_idx >= len(recs) or not isinstance(recs[self._sel_rec_idx], dict):
            messagebox.showwarning('Recipients', 'Invalid recipient selection')
            return

        r = recs[self._sel_rec_idx]
        if bool(self.r_use_global.get()):
            # store None to inherit
            for k in ('send_report','send_alarm_events','send_power_logs','send_chart_snapshots','send_inline_params'):
                r[k] = None
        else:
            r['send_report'] = bool(self.r_send_report.get())
            r['send_alarm_events'] = bool(self.r_send_alarm.get())
            r['send_power_logs'] = bool(self.r_send_power.get())
            r['send_chart_snapshots'] = bool(self.r_send_snap.get())
            r['send_inline_params'] = bool(self.r_send_inline.get())

        if self.on_save_config:
            self.on_save_config()
        messagebox.showinfo('Recipients', 'Recipient overrides saved')


    def _tick(self):
        self._refresh_queue()
        self.after(1500, self._tick)
