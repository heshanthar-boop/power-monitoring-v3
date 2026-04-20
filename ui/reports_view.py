import os
import time
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from datetime import datetime, timedelta

from core.reports_engine import ReportsEngine, ReportRequest
from utils.paths import reports_dir
from ui.styles import get_theme
from ui import pro_chrome


class ReportsTab(ttk.Frame):
    """Reports tab: generate PDF now + schedule + history + settings.

    This UI is purposely operator-simple. It covers required production flows:
    - One-click instant report
    - Hourly/daily/custom period reports
    - Multiple schedules (interval + daily)
    - History list with open-folder action
    - Retention + logo + facility details
    """

    REPORT_TYPES = ["INSTANT", "HOURLY", "DAILY", "CUSTOM"]
    OUTPUT_TYPES = ["HTML", "PDF"]
    INTERVALS = [("30 min", 30), ("1 hour", 60), ("3 hours", 180), ("6 hours", 360), ("12 hours", 720), ("24 hours", 1440)]

    def __init__(self, parent, cfg, meters, snapshot_fn, protection_events_csv=None,
                 on_save_config=None, on_print_snapshot=None):
        super().__init__(parent, padding=8)
        self.cfg = cfg
        self._privacy = bool((self.cfg.get("ui") or {}).get("privacy_mode", True))
        self.meters = meters
        self.snapshot_fn = snapshot_fn
        self.on_save_config = on_save_config
        # Callback for live snapshot print (wired from MainWindow.print_snapshot_now)
        self._on_print_snapshot = on_print_snapshot

        self.engine = ReportsEngine(cfg=cfg, meters=meters, snapshot_fn=snapshot_fn, protection_events_csv=protection_events_csv)

        self._build_ui()
        self.after(400, self._tick)

    # ------------------------------------------------------------------
    def _build_ui(self):
        t = get_theme()
        root = pro_chrome.page(self)

        # Header row: title on left, Print Snapshot button on right
        hdr_row = ttk.Frame(root)
        hdr_row.pack(fill="x", padx=0, pady=0)
        pro_chrome.header(
            hdr_row,
            "Reports",
            "One-click report generation, schedules and report history.",
            accent=t.accent,
        )
        # Print Snapshot button — always visible, right-aligned in the header row
        _btn = ttk.Button(
            hdr_row,
            text="🖨  Print Snapshot  (Ctrl+P)",
            command=self._do_print_snapshot,
        )
        _btn.pack(side="right", padx=(0, 16), pady=8)

        content = tk.Frame(root, bg=t.bg)
        content.pack(fill="both", expand=True)
        content.columnconfigure(0, weight=1)
        content.rowconfigure(2, weight=1)

        # --- Generate now ---
        gen = ttk.Labelframe(content, text="Generate Report Now")
        gen.grid(row=0, column=0, sticky="ew", padx=10, pady=8)
        gen.columnconfigure(5, weight=1)

        ttk.Label(gen, text="Type:").grid(row=0, column=0, sticky="w", padx=6, pady=6)
        self.type_var = tk.StringVar(value="INSTANT")
        ttk.Combobox(gen, textvariable=self.type_var, values=self.REPORT_TYPES, width=12, state="readonly").grid(row=0, column=1, sticky="w", padx=4)

        ttk.Label(gen, text="Output:").grid(row=0, column=2, sticky="w", padx=(14, 6), pady=6)
        self.output_var = tk.StringVar(value="HTML")
        ttk.Combobox(gen, textvariable=self.output_var, values=self.OUTPUT_TYPES, width=8, state="readonly").grid(row=0, column=3, sticky="w", padx=4)

        ttk.Label(gen, text="Start:").grid(row=0, column=4, sticky="w", padx=6)
        self.start_var = tk.StringVar(value=(datetime.now() - timedelta(hours=1)).strftime("%Y-%m-%d %H:%M"))
        ttk.Entry(gen, textvariable=self.start_var, width=18).grid(row=0, column=5, sticky="w", padx=4)

        ttk.Label(gen, text="End:").grid(row=0, column=6, sticky="w", padx=6)
        self.end_var = tk.StringVar(value=datetime.now().strftime("%Y-%m-%d %H:%M"))
        ttk.Entry(gen, textvariable=self.end_var, width=18).grid(row=0, column=7, sticky="w", padx=4)

        # meters selection
        self.meter_vars = {}
        mwrap = ttk.Frame(gen)
        mwrap.grid(row=1, column=0, columnspan=8, sticky="w", padx=6, pady=(0, 6))
        ttk.Label(mwrap, text="Meters:").pack(side="left")
        all_var = tk.BooleanVar(value=True)
        self.all_meters_var = all_var
        ttk.Checkbutton(mwrap, text="All", variable=all_var, command=self._sync_all).pack(side="left", padx=(6, 10))
        for m in self.meters:
            if getattr(m, "meter_id", 0) <= 0:
                continue
            v = tk.BooleanVar(value=False)
            self.meter_vars[m.meter_id] = v
            ttk.Checkbutton(mwrap, text=f"M{m.meter_id}", variable=v, command=self._sync_one).pack(side="left")

        self.progress = ttk.Progressbar(gen, mode="indeterminate")
        self.progress.grid(row=0, column=8, sticky="e", padx=6)
        ttk.Button(gen, text="Generate", command=self._generate_now).grid(row=0, column=9, sticky="e", padx=6, pady=6)

        self.msg_var = tk.StringVar(value="")
        ttk.Label(gen, textvariable=self.msg_var).grid(row=1, column=8, columnspan=2, sticky="e", padx=6)

        # --- Schedules ---
        sch = ttk.Labelframe(content, text="Auto Report Schedules")
        sch.grid(row=1, column=0, sticky="ew", padx=10)
        sch.columnconfigure(0, weight=1)

        top = ttk.Frame(sch)
        top.grid(row=0, column=0, sticky="ew", padx=6, pady=6)
        top.columnconfigure(1, weight=1)

        ttk.Label(top, text="Interval schedules:").grid(row=0, column=0, sticky="w")

        # Output for schedules (default HTML to avoid extra dependencies)
        ttk.Label(top, text="Output:").grid(row=0, column=2, sticky="e", padx=(12, 4))
        self.schedule_output = tk.StringVar(value=self._schedule_output_value())
        ttk.Combobox(top, textvariable=self.schedule_output, values=self.OUTPUT_TYPES, width=8, state="readonly").grid(row=0, column=3, sticky="e")

        # Interval checkboxes in a clean 2-row grid
        interval_grid = ttk.Frame(sch)
        interval_grid.grid(row=1, column=0, sticky="ew", padx=6, pady=(0, 6))
        for i in range(6):
            interval_grid.columnconfigure(i, weight=1)

        self.interval_vars = {}
        for idx, (label, mins) in enumerate(self.INTERVALS):
            r = 0 if idx < 3 else 1
            c = idx if idx < 3 else (idx - 3)
            v = tk.BooleanVar(value=self._is_interval_enabled(mins))
            self.interval_vars[mins] = v
            ttk.Checkbutton(interval_grid, text=label, variable=v, command=self._save_schedules).grid(row=r, column=c, sticky="w", padx=6, pady=2)

        bottom = ttk.Frame(sch)
        bottom.grid(row=2, column=0, sticky="ew", padx=6, pady=(0, 6))
        bottom.columnconfigure(2, weight=1)

        self.daily_enable = tk.BooleanVar(value=self._is_daily_enabled())
        self.daily_time = tk.StringVar(value=self._daily_time_value())
        ttk.Checkbutton(bottom, text="Daily summary", variable=self.daily_enable, command=self._save_schedules).grid(row=0, column=0, sticky="w")
        ttk.Label(bottom, text="Time (HH:MM):").grid(row=0, column=1, sticky="e", padx=(12, 4))
        ttk.Entry(bottom, textvariable=self.daily_time, width=8).grid(row=0, column=2, sticky="w")
        ttk.Button(bottom, text="Save schedules", command=self._save_schedules).grid(row=0, column=3, sticky="e")

        # --- History ---
        hist = ttk.Labelframe(content, text="Report History")
        hist.grid(row=2, column=0, sticky="nsew", padx=10, pady=8)
        hist.rowconfigure(0, weight=1)
        hist.columnconfigure(0, weight=1)

        cols = ("Generated", "Type", "Period", "Meters", "File", "SizeKB")
        self.history = ttk.Treeview(hist, columns=cols, show="headings", height=10)
        for c in cols:
            self.history.heading(c, text=c)
            self.history.column(c, width=140, anchor="w")
        self.history.column("Type", width=80)
        self.history.column("Meters", width=80)
        self.history.column("SizeKB", width=60, anchor="e")
        self.history.grid(row=0, column=0, sticky="nsew", padx=(6, 0), pady=6)
        vs = ttk.Scrollbar(hist, orient="vertical", command=self.history.yview)
        self.history.configure(yscrollcommand=vs.set)
        vs.grid(row=0, column=1, sticky="ns", pady=6)
        self.history.bind("<Double-1>", self._open_selected)

        btns = ttk.Frame(hist)
        btns.grid(row=1, column=0, columnspan=2, sticky="ew", padx=6, pady=(0, 6))
        ttk.Button(btns, text="Refresh", command=self._refresh_history).pack(side="left")
        ttk.Button(btns, text="Open Reports Folder", command=self._open_reports_folder).pack(side="left", padx=6)
        ttk.Button(btns, text="Retention Cleanup", command=self._cleanup).pack(side="right")

        # --- Settings ---
        st = ttk.Labelframe(content, text="Report Settings")
        st.grid(row=3, column=0, sticky="ew", padx=10, pady=(0, 10))
        st.columnconfigure(3, weight=1)

        rcfg = self.cfg.get("reports", {}) or {}
        self.facility_code = tk.StringVar(value=str(rcfg.get("facility_code", "PLANT01")))
        self.company_name = tk.StringVar(value=str(rcfg.get("company_name", "")))
        self.company_contact = tk.StringVar(value=str(rcfg.get("company_contact", "")))
        self.logo_path = tk.StringVar(value=str(rcfg.get("logo_path", "")))
        self.save_dir = tk.StringVar(value=str(rcfg.get("auto_save_dir", "")))
        self.retention_days = tk.StringVar(value=str(rcfg.get("retention_days", 30)))
        self.watermark = tk.StringVar(value=str(rcfg.get("watermark", "")))
        self.pdf_password = tk.StringVar(value=str(rcfg.get("pdf_password", "")))

        ttk.Label(st, text="Facility Code:").grid(row=0, column=0, sticky="w", padx=6, pady=4)
        ttk.Entry(st, textvariable=self.facility_code, width=12).grid(row=0, column=1, sticky="w")
        ttk.Label(st, text="Save Location:").grid(row=0, column=2, sticky="e", padx=6)
        # Privacy mode: do not display full folder paths on the UI.
        self.save_dir_display = tk.StringVar(value="")
        self._sync_save_dir_display()
        ttk.Label(st, textvariable=self.save_dir_display).grid(row=0, column=3, sticky="ew")
        ttk.Button(st, text="Browse", command=self._browse_dir).grid(row=0, column=4, padx=6)

        ttk.Label(st, text="Company Name:").grid(row=1, column=0, sticky="w", padx=6, pady=4)
        ttk.Entry(st, textvariable=self.company_name, width=26).grid(row=1, column=1, sticky="w")
        ttk.Label(st, text="Contact:").grid(row=1, column=2, sticky="e", padx=6)
        ttk.Entry(st, textvariable=self.company_contact).grid(row=1, column=3, sticky="ew")

        ttk.Label(st, text="Logo:").grid(row=2, column=0, sticky="w", padx=6, pady=4)
        ttk.Entry(st, textvariable=self.logo_path).grid(row=2, column=1, columnspan=3, sticky="ew")
        ttk.Button(st, text="Browse", command=self._browse_logo).grid(row=2, column=4, padx=6)

        ttk.Label(st, text="Retention Days:").grid(row=3, column=0, sticky="w", padx=6, pady=4)
        ttk.Entry(st, textvariable=self.retention_days, width=8).grid(row=3, column=1, sticky="w")
        ttk.Label(st, text="Watermark:").grid(row=3, column=2, sticky="e", padx=6)
        ttk.Entry(st, textvariable=self.watermark).grid(row=3, column=3, sticky="ew")

        ttk.Label(st, text="PDF Password (optional):").grid(row=4, column=0, sticky="w", padx=6, pady=4)
        ttk.Entry(st, textvariable=self.pdf_password, width=18, show="*").grid(row=4, column=1, sticky="w")
        ttk.Button(st, text="Save Settings", command=self._save_settings).grid(row=4, column=4, sticky="e", padx=6)

        self._refresh_history()

    # ------------------------------------------------------------------
    def _sync_all(self):
        if self.all_meters_var.get():
            for v in self.meter_vars.values():
                v.set(False)

    def _sync_one(self):
        if any(v.get() for v in self.meter_vars.values()):
            self.all_meters_var.set(False)

    def _selected_meters(self):
        if self.all_meters_var.get():
            return []  # means ALL enabled
        out = [mid for mid, v in self.meter_vars.items() if v.get()]
        return out

    def _generate_now(self):
        rtype = self.type_var.get().strip().upper()
        outfmt = (self.output_var.get() or "HTML").strip().upper()
        try:
            if rtype == "INSTANT":
                ts = time.time()
                start_ts = end_ts = ts
            else:
                start_ts = datetime.strptime(self.start_var.get().strip(), "%Y-%m-%d %H:%M").timestamp()
                end_ts = datetime.strptime(self.end_var.get().strip(), "%Y-%m-%d %H:%M").timestamp()
        except Exception:
            messagebox.showerror("Invalid time", "Use format: YYYY-MM-DD HH:MM")
            return

        meter_ids = self._selected_meters()
        self.progress.start(12)
        self.msg_var.set("Generating...")
        self.engine.enqueue(
            ReportRequest(
                report_type=rtype,
                start_ts=start_ts,
                end_ts=end_ts,
                meter_ids=meter_ids,
                generated_by="USER",
                output=outfmt,
            )
        )

    def _tick(self):
        res = self.engine.poll_result()
        if res:
            self.progress.stop()
            if res.ok:
                self.msg_var.set("Done")
                self._refresh_history()
                messagebox.showinfo("Report Generated", f"Saved to:\n{res.file_path}")
            else:
                self.msg_var.set("Failed")
                messagebox.showerror("Report failed", res.error or "Unknown error")
        self.after(400, self._tick)

    # ------------------------------------------------------------------
    def _refresh_history(self):
        self.history.delete(*self.history.get_children())
        rows = self.engine.list_history(limit=200)
        for r in rows:
            period = f"{r.report_period_start} → {r.report_period_end}"
            self.history.insert("", "end", values=(r.generation_time, r.report_type, period, r.meter_ids_included, r.file_path, r.file_size_kb))

    def _open_selected(self, _evt=None):
        sel = self.history.selection()
        if not sel:
            return
        fp = self.history.item(sel[0], "values")[4]
        if not fp:
            return
        try:
            os.startfile(fp)  # windows
        except Exception:
            try:
                import subprocess
                subprocess.Popen(["xdg-open", fp])
            except Exception:
                messagebox.showinfo("File", fp)

    def _open_reports_folder(self):
        rcfg = self.cfg.get("reports", {}) or {}
        base = (rcfg.get("auto_save_dir") or "").strip() or reports_dir()
        try:
            os.startfile(base)
        except Exception:
            messagebox.showinfo("Folder", base)

    def _cleanup(self):
        n = self.engine.retention_cleanup()
        self._refresh_history()
        messagebox.showinfo("Cleanup", f"Deleted {n} old reports")

    def _do_print_snapshot(self):
        """Print Snapshot button handler — delegates to MainWindow.print_snapshot_now()."""
        if callable(self._on_print_snapshot):
            try:
                self._on_print_snapshot()
            except Exception as exc:
                messagebox.showerror("Print Snapshot", f"Failed to generate snapshot:\n{exc}")
        else:
            messagebox.showinfo(
                "Print Snapshot",
                "Snapshot printing is not available.\n"
                "Use keyboard shortcut Ctrl+P from the main window.",
            )

    # ------------------------------------------------------------------
    def _browse_dir(self):
        p = filedialog.askdirectory(initialdir=self.save_dir.get() or reports_dir())
        if p:
            self.save_dir.set(p)
            self._sync_save_dir_display()

    def _sync_save_dir_display(self):
        """Update the UI-friendly save path hint (privacy-safe)."""
        raw = (self.save_dir.get() or "").strip()
        if not raw:
            self.save_dir_display.set("Default (Reports folder)")
            return
        if self._privacy:
            self.save_dir_display.set("Custom folder set")
        else:
            self.save_dir_display.set(raw)

    def _browse_logo(self):
        p = filedialog.askopenfilename(filetypes=[("Images", "*.png;*.jpg;*.jpeg;*.bmp")])
        if p:
            self.logo_path.set(p)

    def _save_settings(self):
        rcfg = self.cfg.setdefault("reports", {})
        rcfg["facility_code"] = self.facility_code.get().strip() or "PLANT01"
        rcfg["company_name"] = self.company_name.get().strip()
        rcfg["company_contact"] = self.company_contact.get().strip()
        rcfg["logo_path"] = self.logo_path.get().strip()
        rcfg["auto_save_dir"] = self.save_dir.get().strip()
        try:
            rcfg["retention_days"] = int(self.retention_days.get().strip() or "30")
        except Exception:
            rcfg["retention_days"] = 30
        rcfg["watermark"] = self.watermark.get().strip()
        rcfg["pdf_password"] = self.pdf_password.get().strip()

        if self.on_save_config:
            self.on_save_config()
        messagebox.showinfo("Saved", "Report settings saved")

    # ------------------------------------------------------------------
    def _is_interval_enabled(self, minutes: int) -> bool:
        sch = (self.cfg.get("reports", {}) or {}).get("schedules", []) or []
        for s in sch:
            if s.get("type") == "interval" and int(s.get("minutes", 0)) == int(minutes):
                return bool(s.get("enabled", False))
        return False

    def _is_daily_enabled(self) -> bool:
        sch = (self.cfg.get("reports", {}) or {}).get("schedules", []) or []
        for s in sch:
            if s.get("type") == "daily":
                return bool(s.get("enabled", False))
        return False

    def _daily_time_value(self) -> str:
        sch = (self.cfg.get("reports", {}) or {}).get("schedules", []) or []
        for s in sch:
            if s.get("type") == "daily":
                return str(s.get("time", "23:59"))
        return "23:59"

    def _save_schedules(self):
        rcfg = self.cfg.setdefault("reports", {})
        outfmt = (self.schedule_output.get() or "HTML").strip().upper()
        schedules = []
        # interval schedules
        for mins, var in self.interval_vars.items():
            schedules.append({
                "id": f"interval_{mins}",
                "enabled": bool(var.get()),
                "type": "interval",
                "minutes": int(mins),
                "report_type": "HOURLY" if mins >= 60 else "INSTANT",
                "meters": "ALL",
                "output": outfmt,
            })
        # daily
        schedules.append({
            "id": "daily_summary",
            "enabled": bool(self.daily_enable.get()),
            "type": "daily",
            "time": self.daily_time.get().strip() or "23:59",
            "report_type": "DAILY",
            "meters": "ALL",
            "output": outfmt,
        })
        rcfg["schedules"] = schedules
        if self.on_save_config:
            self.on_save_config()

    def _schedule_output_value(self) -> str:
        """Return the configured output type for schedules (HTML/PDF)."""
        try:
            rcfg = self.cfg.get("reports", {}) or {}
            sch = (rcfg.get("schedules") or [])
            for s in sch:
                if isinstance(s, dict) and s.get("output"):
                    return str(s.get("output")).upper()
        except Exception:
            pass
        return "HTML"
