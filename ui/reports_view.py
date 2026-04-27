"""
Reports Tab — Power Monitoring v3
==================================
Redesigned for clarity, professional usability and IEC/IEEE-aligned workflows.

Layout (top → bottom):
  1. Header bar   — title, subtitle, Print Snapshot (Ctrl+P) right-anchored
  2. KPI strip    — report count, last generated, storage used, next scheduled
  3. Generate Now — report type selector with period presets + custom range
  4. Auto Schedules — interval grid + daily summary, clearly separated
  5. Report History — sortable Treeview with status badges, open / delete actions
  6. Site & Format Settings — IEC-grade facility details, retention, output format
"""

from __future__ import annotations

import os
import time
import threading
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from datetime import datetime, timedelta
from typing import Optional

from core.reports_engine import ReportsEngine, ReportRequest
from utils.paths import reports_dir
from ui.styles import get_theme
from ui import pro_chrome


# ── Constants ─────────────────────────────────────────────────────────────────

_REPORT_TYPES = [
    ("Instant Snapshot",    "INSTANT",  "Current live readings — generated immediately"),
    ("Hourly Summary",      "HOURLY",   "Aggregated stats for the selected 1-hour window"),
    ("Daily Summary",       "DAILY",    "Full-day energy & demand report (00:00–24:00)"),
    ("Custom Range",        "CUSTOM",   "User-defined start/end period"),
]

_OUTPUT_TYPES = [
    ("HTML (browser view)",  "HTML"),
    ("PDF (print-ready)",    "PDF"),
]

_PERIOD_PRESETS = [
    ("Last 1 h",     1),
    ("Last 4 h",     4),
    ("Last 8 h",     8),
    ("Last 24 h",   24),
    ("Last 48 h",   48),
    ("Last 7 d",  7*24),
]

_INTERVALS = [
    ("30 min",   30),
    ("1 hour",   60),
    ("3 hours",  180),
    ("6 hours",  360),
    ("12 hours", 720),
    ("24 hours", 1440),
]

_HIST_COLS = [
    ("Generated",   160, "w"),
    ("Type",         90, "w"),
    ("Period Start", 150, "w"),
    ("Period End",   150, "w"),
    ("Meters",        80, "center"),
    ("Format",        60, "center"),
    ("Size (KB)",     75, "e"),
    ("By",            70, "center"),
]


# ── Main class ─────────────────────────────────────────────────────────────────

class ReportsTab(ttk.Frame):
    """
    Production-grade Reports tab.

    Parameters
    ----------
    on_print_snapshot : callable | None
        Called when operator clicks "Print Snapshot" or presses Ctrl+P.
        Wired from MainWindow.print_snapshot_now().
    """

    def __init__(self, parent, cfg, meters, snapshot_fn,
                 protection_events_csv=None, on_save_config=None,
                 on_print_snapshot=None):
        super().__init__(parent, padding=0)
        self.cfg              = cfg
        self.meters           = meters
        self.snapshot_fn      = snapshot_fn
        self.on_save_config   = on_save_config
        self._on_print_snapshot = on_print_snapshot
        self._privacy = bool((cfg.get("ui") or {}).get("privacy_mode", True))

        self.engine = ReportsEngine(
            cfg=cfg,
            meters=meters,
            snapshot_fn=snapshot_fn,
            protection_events_csv=protection_events_csv,
        )

        self._sort_col   = "Generated"
        self._sort_rev   = True
        self._gen_start  = None   # for elapsed timer

        self._build_ui()
        self.after(500, self._tick)

    # ── UI construction ────────────────────────────────────────────────────────

    def _build_ui(self):
        t   = get_theme()
        root = pro_chrome.page(self)

        # ── 1. Header bar ─────────────────────────────────────────────────────
        hdr = ttk.Frame(root)
        hdr.pack(fill="x", padx=0, pady=0)

        pro_chrome.header(
            hdr,
            "Reports",
            "Generate, schedule and archive power quality reports — IEC 61000 / IEEE 1159",
            accent=t.accent,
        )

        snap_btn = ttk.Button(
            hdr,
            text="🖨  Print Snapshot  (Ctrl+P)",
            command=self._do_print_snapshot,
            style="Accent.TButton",
        )
        snap_btn.pack(side="right", padx=(0, 16), pady=10)

        # ── 2. KPI strip ──────────────────────────────────────────────────────
        self._kpi_bar = ttk.Frame(root, style="Card.TFrame")
        self._kpi_bar.pack(fill="x", padx=12, pady=(0, 10))
        self._kpi_bar.columnconfigure((0, 1, 2, 3), weight=1)

        self._kpi_report_count = self._kpi_tile(self._kpi_bar, "Reports Generated", "—", 0)
        self._kpi_last_gen     = self._kpi_tile(self._kpi_bar, "Last Generated",     "—", 1)
        self._kpi_storage      = self._kpi_tile(self._kpi_bar, "Storage Used",       "—", 2)
        self._kpi_next_sched   = self._kpi_tile(self._kpi_bar, "Next Scheduled",     "—", 3)

        # ── Scrollable main content area ──────────────────────────────────────
        from ui.widgets.scrollable_container import ScrollableContainer
        scroll = ScrollableContainer(root)
        scroll.pack(fill="both", expand=True, padx=0, pady=0)
        inner = scroll.body   # inner scrollable frame

        # ── 3. Generate Now ───────────────────────────────────────────────────
        self._build_generate_section(inner, t)

        # ── 4. Auto Schedules ─────────────────────────────────────────────────
        self._build_schedule_section(inner, t)

        # ── 5. History ────────────────────────────────────────────────────────
        self._build_history_section(inner, t)

        # ── 6. Settings ───────────────────────────────────────────────────────
        self._build_settings_section(inner, t)

        self._refresh_history()
        self._refresh_kpis()

    # ── Section builders ───────────────────────────────────────────────────────

    def _build_generate_section(self, parent, t):
        sec = self._section(parent, "Generate Report")

        # ── Report type ───────────────────────────────────────────────────────
        rt_row = ttk.Frame(sec)
        rt_row.pack(fill="x", pady=(0, 8))

        ttk.Label(rt_row, text="Report Type:", font=("", 9, "bold")).pack(side="left", padx=(0, 10))

        self._rt_var    = tk.StringVar(value="INSTANT")
        self._rt_desc   = tk.StringVar(value=_REPORT_TYPES[0][2])
        self._rt_frames = {}

        for label, code, desc in _REPORT_TYPES:
            rb = ttk.Radiobutton(
                rt_row, text=label, value=code, variable=self._rt_var,
                command=self._on_report_type_change,
            )
            rb.pack(side="left", padx=6)

        desc_lbl = ttk.Label(rt_row, textvariable=self._rt_desc,
                              foreground=t.text_muted, font=("", 8))
        desc_lbl.pack(side="left", padx=(14, 0))

        # ── Period row ────────────────────────────────────────────────────────
        period_row = ttk.Frame(sec)
        period_row.pack(fill="x", pady=(0, 8))

        ttk.Label(period_row, text="Period:", font=("", 9, "bold")).pack(side="left", padx=(0, 8))

        # Preset quick buttons
        for label, hours in _PERIOD_PRESETS:
            ttk.Button(
                period_row, text=label, width=8,
                command=lambda h=hours: self._apply_preset(h),
            ).pack(side="left", padx=2)

        ttk.Separator(period_row, orient="vertical").pack(side="left", fill="y", padx=8)

        ttk.Label(period_row, text="From:").pack(side="left")
        self.start_var = tk.StringVar(
            value=(datetime.now() - timedelta(hours=1)).strftime("%Y-%m-%d %H:%M")
        )
        self._start_entry = ttk.Entry(period_row, textvariable=self.start_var, width=17)
        self._start_entry.pack(side="left", padx=(4, 8))

        ttk.Label(period_row, text="To:").pack(side="left")
        self.end_var = tk.StringVar(value=datetime.now().strftime("%Y-%m-%d %H:%M"))
        self._end_entry = ttk.Entry(period_row, textvariable=self.end_var, width=17)
        self._end_entry.pack(side="left", padx=(4, 12))

        ttk.Label(period_row, text="(YYYY-MM-DD HH:MM)", foreground=t.text_muted,
                  font=("", 8)).pack(side="left")

        # ── Output format + meter selection ───────────────────────────────────
        bottom_row = ttk.Frame(sec)
        bottom_row.pack(fill="x", pady=(0, 4))

        ttk.Label(bottom_row, text="Format:", font=("", 9, "bold")).pack(side="left", padx=(0, 6))
        self.output_var = tk.StringVar(value="HTML")
        for label, code in _OUTPUT_TYPES:
            ttk.Radiobutton(
                bottom_row, text=label, value=code, variable=self.output_var,
            ).pack(side="left", padx=4)

        ttk.Separator(bottom_row, orient="vertical").pack(side="left", fill="y", padx=14)

        ttk.Label(bottom_row, text="Meters:", font=("", 9, "bold")).pack(side="left", padx=(0, 6))
        self.meter_vars = {}
        self.all_meters_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(
            bottom_row, text="All", variable=self.all_meters_var,
            command=self._sync_all,
        ).pack(side="left", padx=2)

        for m in self.meters:
            if getattr(m, "meter_id", 0) <= 0:
                continue
            v = tk.BooleanVar(value=False)
            self.meter_vars[m.meter_id] = v
            ttk.Checkbutton(
                bottom_row,
                text=getattr(m, "name", f"M{m.meter_id}") or f"M{m.meter_id}",
                variable=v,
                command=self._sync_one,
            ).pack(side="left", padx=2)

        # ── Action row ────────────────────────────────────────────────────────
        action_row = ttk.Frame(sec)
        action_row.pack(fill="x", pady=(6, 0))

        self._gen_btn = ttk.Button(
            action_row, text="▶  Generate Now",
            command=self._generate_now, style="Accent.TButton",
        )
        self._gen_btn.pack(side="left")

        self._progress = ttk.Progressbar(action_row, mode="indeterminate", length=160)
        self._progress.pack(side="left", padx=(12, 0))

        self._msg_var = tk.StringVar(value="")
        self._msg_lbl = ttk.Label(action_row, textvariable=self._msg_var, font=("", 9))
        self._msg_lbl.pack(side="left", padx=10)

        self._elapsed_var = tk.StringVar(value="")
        ttk.Label(action_row, textvariable=self._elapsed_var,
                  foreground=t.text_muted, font=("", 8)).pack(side="left")

        # initial state — disable period fields for INSTANT
        self._on_report_type_change()

    def _build_schedule_section(self, parent, t):
        sec = self._section(parent, "Automatic Report Schedules")

        ttk.Label(
            sec,
            text="Scheduled reports run automatically in the background — output saved to the configured reports folder.",
            foreground=t.text_muted, font=("", 8), wraplength=700,
        ).pack(anchor="w", pady=(0, 8))

        # ── Interval grid ─────────────────────────────────────────────────────
        iv_lbl = ttk.Label(sec, text="Interval Schedules:", font=("", 9, "bold"))
        iv_lbl.pack(anchor="w")

        grid_frm = ttk.Frame(sec)
        grid_frm.pack(fill="x", pady=(4, 8))
        for i in range(6):
            grid_frm.columnconfigure(i, weight=1)

        self.interval_vars = {}
        for idx, (label, mins) in enumerate(_INTERVALS):
            r, c = divmod(idx, 3)
            v = tk.BooleanVar(value=self._is_interval_enabled(mins))
            self.interval_vars[mins] = v
            frm = ttk.Frame(grid_frm, style="Card.TFrame")
            frm.grid(row=r, column=c, padx=4, pady=3, sticky="ew")
            ttk.Checkbutton(
                frm, text=label, variable=v,
                command=self._save_schedules,
            ).pack(anchor="w", padx=8, pady=4)

        # ── Daily summary ─────────────────────────────────────────────────────
        daily_row = ttk.Frame(sec)
        daily_row.pack(fill="x", pady=(4, 0))

        ttk.Label(daily_row, text="Daily Summary:", font=("", 9, "bold")).pack(side="left", padx=(0, 8))

        self.daily_enable = tk.BooleanVar(value=self._is_daily_enabled())
        ttk.Checkbutton(
            daily_row, text="Enabled", variable=self.daily_enable,
            command=self._save_schedules,
        ).pack(side="left")

        ttk.Separator(daily_row, orient="vertical").pack(side="left", fill="y", padx=12)

        ttk.Label(daily_row, text="Time (HH:MM):").pack(side="left")
        self.daily_time = tk.StringVar(value=self._daily_time_value())
        ttk.Entry(daily_row, textvariable=self.daily_time, width=7).pack(side="left", padx=6)

        ttk.Separator(daily_row, orient="vertical").pack(side="left", fill="y", padx=12)

        ttk.Label(daily_row, text="Schedule Output:").pack(side="left")
        self.schedule_output = tk.StringVar(value=self._schedule_output_value())
        ttk.Combobox(
            daily_row, textvariable=self.schedule_output,
            values=[c for _, c in _OUTPUT_TYPES], width=6, state="readonly",
        ).pack(side="left", padx=6)

        ttk.Button(
            daily_row, text="Save Schedules",
            command=self._save_schedules,
        ).pack(side="right")

    def _build_history_section(self, parent, t):
        sec = self._section(parent, "Report History", expand=True)

        # Treeview
        tree_frm = ttk.Frame(sec)
        tree_frm.pack(fill="both", expand=True)
        tree_frm.rowconfigure(0, weight=1)
        tree_frm.columnconfigure(0, weight=1)

        cols = [c[0] for c in _HIST_COLS]
        self.history = ttk.Treeview(
            tree_frm, columns=cols, show="headings", height=10,
            selectmode="browse",
        )
        for col, width, anchor in _HIST_COLS:
            self.history.heading(
                col, text=col,
                command=lambda c=col: self._sort_history(c),
            )
            self.history.column(col, width=width, anchor=anchor, stretch=(col == "Period Start"))

        # Row tags for visual status
        self.history.tag_configure("html_row", foreground=t.accent)
        self.history.tag_configure("pdf_row",  foreground=t.good)
        self.history.tag_configure("missing",  foreground=t.warn)

        ys = ttk.Scrollbar(tree_frm, orient="vertical",   command=self.history.yview)
        xs = ttk.Scrollbar(tree_frm, orient="horizontal",  command=self.history.xview)
        self.history.configure(yscrollcommand=ys.set, xscrollcommand=xs.set)

        self.history.grid(row=0, column=0, sticky="nsew")
        ys.grid(row=0, column=1, sticky="ns")
        xs.grid(row=1, column=0, sticky="ew")

        self.history.bind("<Double-1>",    self._open_selected)
        self.history.bind("<Delete>",      self._delete_selected)
        self.history.bind("<BackSpace>",   self._delete_selected)

        # Context menu
        self._hist_menu = tk.Menu(self, tearoff=0)
        self._hist_menu.add_command(label="📂  Open file",          command=self._open_selected)
        self._hist_menu.add_command(label="📁  Open reports folder", command=self._open_reports_folder)
        self._hist_menu.add_separator()
        self._hist_menu.add_command(label="🗑  Delete entry",        command=self._delete_selected)
        self.history.bind("<Button-3>",    self._show_hist_menu)
        self.history.bind("<Button-2>",    self._show_hist_menu)  # macOS

        # ── Action buttons ────────────────────────────────────────────────────
        btn_row = ttk.Frame(sec)
        btn_row.pack(fill="x", pady=(6, 0))

        ttk.Button(btn_row, text="↻  Refresh",
                   command=self._refresh_history).pack(side="left")
        ttk.Button(btn_row, text="📁  Open Reports Folder",
                   command=self._open_reports_folder).pack(side="left", padx=6)
        ttk.Button(btn_row, text="📂  Open Selected",
                   command=self._open_selected).pack(side="left")

        ttk.Separator(btn_row, orient="vertical").pack(side="left", fill="y", padx=12)

        self._hist_count_var = tk.StringVar(value="")
        ttk.Label(btn_row, textvariable=self._hist_count_var,
                  foreground=t.text_muted, font=("", 8)).pack(side="left")

        ttk.Button(
            btn_row, text="🗑  Retention Cleanup",
            command=self._cleanup,
        ).pack(side="right")

    def _build_settings_section(self, parent, t):
        sec = self._section(parent, "Site & Report Settings")

        # ── Two-column form ───────────────────────────────────────────────────
        form = ttk.Frame(sec)
        form.pack(fill="x")
        form.columnconfigure(1, weight=1)
        form.columnconfigure(3, weight=1)

        rcfg = self.cfg.get("reports", {}) or {}
        site_cfg = self.cfg.get("site", {}) or {}

        self.facility_code    = tk.StringVar(value=rcfg.get("facility_code")    or site_cfg.get("plant_name") or "PLANT01")
        self.facility_name    = tk.StringVar(value=rcfg.get("facility_name")    or "")
        self.company_name     = tk.StringVar(value=rcfg.get("company_name")     or "")
        self.company_contact  = tk.StringVar(value=rcfg.get("company_contact")  or "")
        self.nominal_vln      = tk.StringVar(value=str(site_cfg.get("nominal_vln") or rcfg.get("nominal_v_ln") or 230))
        self.nominal_freq     = tk.StringVar(value=str(rcfg.get("nominal_freq_hz") or 50))
        self.logo_path        = tk.StringVar(value=rcfg.get("logo_path")        or "")
        self.save_dir         = tk.StringVar(value=rcfg.get("auto_save_dir")    or "")
        self.retention_days   = tk.StringVar(value=str(rcfg.get("retention_days", 30)))
        self.watermark        = tk.StringVar(value=rcfg.get("watermark")        or "")
        self.pdf_password     = tk.StringVar(value=rcfg.get("pdf_password")     or "")

        self.save_dir_display = tk.StringVar(value="")
        self._sync_save_dir_display()

        def _row(r, lbl0, var0, w0=14, lbl1=None, var1=None, browse=None, show=None):
            ttk.Label(form, text=lbl0).grid(row=r, column=0, sticky="w", padx=(0, 6), pady=4)
            kw = {"show": show} if show else {}
            e0 = ttk.Entry(form, textvariable=var0, width=w0, **kw)
            e0.grid(row=r, column=1, sticky="ew", padx=(0, 12))
            if lbl1:
                ttk.Label(form, text=lbl1).grid(row=r, column=2, sticky="w", padx=(0, 6))
                e1 = ttk.Entry(form, textvariable=var1)
                e1.grid(row=r, column=3, sticky="ew")
            if browse:
                ttk.Button(form, text="Browse", width=8,
                           command=browse).grid(row=r, column=4, padx=(6, 0))

        _row(0, "Facility Code:",    self.facility_code,  14, "Facility Name:", self.facility_name)
        _row(1, "Company Name:",     self.company_name,   22, "Contact / Email:", self.company_contact)
        _row(2, "Nominal V L-N (V):", self.nominal_vln,    8, "Nominal Freq (Hz):", self.nominal_freq)

        # Logo row
        ttk.Label(form, text="Logo File:").grid(row=3, column=0, sticky="w", padx=(0, 6), pady=4)
        ttk.Entry(form, textvariable=self.logo_path).grid(row=3, column=1, columnspan=3, sticky="ew")
        ttk.Button(form, text="Browse", width=8, command=self._browse_logo).grid(row=3, column=4, padx=(6, 0))

        # Save dir
        ttk.Label(form, text="Save Folder:").grid(row=4, column=0, sticky="w", padx=(0, 6), pady=4)
        if self._privacy:
            ttk.Label(form, textvariable=self.save_dir_display,
                      foreground=t.text_muted).grid(row=4, column=1, sticky="w")
        else:
            ttk.Entry(form, textvariable=self.save_dir_display,
                      state="readonly").grid(row=4, column=1, columnspan=3, sticky="ew")
        ttk.Button(form, text="Browse", width=8,
                   command=self._browse_dir).grid(row=4, column=4, padx=(6, 0))

        _row(5, "Retention (days):",  self.retention_days,  6, "Watermark:",     self.watermark)
        _row(6, "PDF Password:",      self.pdf_password,   16, lbl1=None, browse=None, show="*")

        ttk.Label(
            form,
            text="Nominal V L-N used for voltage deviation calculations (IEC 60038). "
                 "Password-protected PDFs require a PDF reader.",
            foreground=t.text_muted, font=("", 7), wraplength=620,
        ).grid(row=7, column=0, columnspan=5, sticky="w", pady=(6, 0))

        # Save button
        save_row = ttk.Frame(sec)
        save_row.pack(fill="x", pady=(10, 0))
        ttk.Button(
            save_row, text="💾  Save Settings",
            command=self._save_settings, style="Accent.TButton",
        ).pack(side="right")

        self._settings_status = tk.StringVar(value="")
        ttk.Label(save_row, textvariable=self._settings_status,
                  foreground=t.good).pack(side="right", padx=12)

    # ── Section / tile helpers ─────────────────────────────────────────────────

    def _section(self, parent, title: str, expand: bool = False) -> ttk.Frame:
        """Card-style section with bold title bar."""
        t = get_theme()
        outer = ttk.Frame(parent, style="Card.TFrame")
        outer.pack(fill="both" if expand else "x",
                   expand=expand, padx=12, pady=(0, 10))

        title_bar = ttk.Frame(outer, style="Card.TFrame")
        title_bar.pack(fill="x")
        ttk.Label(
            title_bar, text=title,
            font=("", 10, "bold"), foreground=t.accent,
            background=t.card,
        ).pack(side="left", padx=12, pady=(10, 4))

        ttk.Separator(outer, orient="horizontal").pack(fill="x", padx=8)

        inner = ttk.Frame(outer, style="Card.TFrame")
        inner.pack(fill="both", expand=expand, padx=12, pady=10)
        return inner

    def _kpi_tile(self, parent, label: str, value: str, col: int) -> tk.StringVar:
        """Insert a KPI tile into the KPI bar; returns the value StringVar."""
        t    = get_theme()
        frm  = ttk.Frame(parent, style="Card.TFrame")
        frm.grid(row=0, column=col, sticky="ew", padx=6, pady=8)

        ttk.Label(
            frm, text=label.upper(),
            font=("", 7, "bold"), foreground=t.text_muted, background=t.card,
        ).pack(anchor="w", padx=10, pady=(6, 0))

        var = tk.StringVar(value=value)
        ttk.Label(
            frm, textvariable=var,
            font=("", 12, "bold"), foreground=t.accent, background=t.card,
        ).pack(anchor="w", padx=10, pady=(0, 6))

        return var

    # ── Generate logic ─────────────────────────────────────────────────────────

    def _on_report_type_change(self, *_):
        code = self._rt_var.get()
        for _, c, desc in _REPORT_TYPES:
            if c == code:
                self._rt_desc.set(desc)
                break
        instant = (code == "INSTANT")
        state   = "disabled" if instant else "normal"
        self._start_entry.configure(state=state)
        self._end_entry.configure(state=state)

    def _apply_preset(self, hours: int):
        end   = datetime.now()
        start = end - timedelta(hours=hours)
        self.start_var.set(start.strftime("%Y-%m-%d %H:%M"))
        self.end_var.set(end.strftime("%Y-%m-%d %H:%M"))
        # Auto-select appropriate type
        if hours <= 1:
            self._rt_var.set("HOURLY")
        elif hours >= 24:
            self._rt_var.set("DAILY")
        else:
            self._rt_var.set("CUSTOM")
        self._on_report_type_change()

    def _generate_now(self):
        rtype  = self._rt_var.get().strip().upper()
        outfmt = (self.output_var.get() or "HTML").strip().upper()

        try:
            if rtype == "INSTANT":
                ts = time.time()
                start_ts = end_ts = ts
            else:
                start_ts = datetime.strptime(
                    self.start_var.get().strip(), "%Y-%m-%d %H:%M"
                ).timestamp()
                end_ts = datetime.strptime(
                    self.end_var.get().strip(), "%Y-%m-%d %H:%M"
                ).timestamp()
                if end_ts <= start_ts:
                    messagebox.showerror("Invalid Period",
                        "End time must be after start time.")
                    return
        except ValueError:
            messagebox.showerror("Invalid Time Format",
                "Use format: YYYY-MM-DD HH:MM\n"
                "Example: 2025-04-21 14:30")
            return

        meter_ids  = self._selected_meters()
        self._gen_start = time.time()

        self._gen_btn.configure(state="disabled")
        self._progress.start(10)
        self._set_msg("Generating…", "normal")
        self._elapsed_var.set("")

        self.engine.enqueue(
            ReportRequest(
                report_type   = rtype,
                start_ts      = start_ts,
                end_ts        = end_ts,
                meter_ids     = meter_ids,
                generated_by  = "USER",
                output        = outfmt,
            )
        )

    # ── Tick / polling ─────────────────────────────────────────────────────────

    def _tick(self):
        # Poll generation result
        res = self.engine.poll_result()
        if res:
            self._progress.stop()
            self._gen_btn.configure(state="normal")
            elapsed = f"{time.time() - self._gen_start:.1f} s" if self._gen_start else ""
            self._gen_start = None
            self._elapsed_var.set(elapsed)

            if res.ok:
                self._set_msg(f"✔  Report saved  ({elapsed})", "ok")
                self._refresh_history()
                self._refresh_kpis()
                fname = os.path.basename(res.file_path or "")
                messagebox.showinfo(
                    "Report Generated",
                    f"File: {fname}\n\nDouble-click the entry below to open it.",
                )
            else:
                self._set_msg("✖  Generation failed", "error")
                messagebox.showerror("Report Failed",
                    res.error or "Unknown error — check app log for details.")
        else:
            # Update elapsed timer while generating
            if self._gen_start:
                self._elapsed_var.set(f"{time.time() - self._gen_start:.0f} s…")

        self.after(400, self._tick)

    # ── History helpers ────────────────────────────────────────────────────────

    def _refresh_history(self):
        self.history.delete(*self.history.get_children())
        rows = self.engine.list_history(limit=500)

        # Sort
        col_map = {
            "Generated":    "generation_time",
            "Type":         "report_type",
            "Period Start": "report_period_start",
            "Period End":   "report_period_end",
            "Meters":       "meter_ids_included",
            "Size (KB)":    "file_size_kb",
        }
        attr = col_map.get(self._sort_col)
        if attr:
            rows = sorted(rows, key=lambda r: getattr(r, attr, "") or "",
                          reverse=self._sort_rev)

        for r in rows:
            ext  = os.path.splitext(r.file_path or "")[1].upper().lstrip(".")
            tag  = "html_row" if ext == "HTML" else ("pdf_row" if ext == "PDF" else "")
            if r.file_path and not os.path.exists(r.file_path):
                tag = "missing"

            self.history.insert(
                "", "end",
                values=(
                    r.generation_time,
                    r.report_type,
                    r.report_period_start,
                    r.report_period_end,
                    r.meter_ids_included or "All",
                    ext or "—",
                    r.file_size_kb,
                    r.generated_by or "—",
                ),
                tags=(tag,),
            )

        n = len(rows)
        self._hist_count_var.set(
            f"{n} report{'s' if n != 1 else ''} — double-click to open · right-click for options"
        )

    def _sort_history(self, col: str):
        if self._sort_col == col:
            self._sort_rev = not self._sort_rev
        else:
            self._sort_col = col
            self._sort_rev = True
        self._refresh_history()

    def _open_selected(self, _evt=None):
        sel = self.history.selection()
        if not sel:
            return
        fp = self.history.item(sel[0], "values")[0]
        # fp is at column 0 (Generated) — we store path differently;
        # re-fetch from engine by matching generation_time
        gen_time = self.history.item(sel[0], "values")[0]
        rows = self.engine.list_history(limit=500)
        for r in rows:
            if r.generation_time == gen_time:
                fp = r.file_path
                break
        if not fp or not os.path.exists(fp):
            messagebox.showwarning("File Not Found",
                f"The report file could not be found:\n{fp or '(unknown)'}")
            return
        try:
            os.startfile(fp)
        except AttributeError:
            try:
                import subprocess
                subprocess.Popen(["xdg-open", fp])
            except Exception:
                messagebox.showinfo("File Path", fp)

    def _delete_selected(self, _evt=None):
        sel = self.history.selection()
        if not sel:
            return
        gen_time = self.history.item(sel[0], "values")[0]
        if not messagebox.askyesno("Delete Entry",
            f"Remove report history entry for:\n{gen_time}\n\n"
            "(The file on disk is not deleted.)"):
            return
        # Prune from engine DB by finding the matching row
        rows = self.engine.list_history(limit=500)
        for r in rows:
            if r.generation_time == gen_time:
                try:
                    self.engine.delete_report(r.report_id, delete_file=False)
                except Exception:
                    pass
                break
        self._refresh_history()
        self._refresh_kpis()

    def _show_hist_menu(self, event):
        try:
            self.history.selection_set(self.history.identify_row(event.y))
            self._hist_menu.tk_popup(event.x_root, event.y_root)
        finally:
            self._hist_menu.grab_release()

    def _open_reports_folder(self, _evt=None):
        rcfg = self.cfg.get("reports", {}) or {}
        base = (rcfg.get("auto_save_dir") or "").strip() or reports_dir()
        try:
            os.startfile(base)
        except AttributeError:
            try:
                import subprocess
                subprocess.Popen(["xdg-open", base])
            except Exception:
                messagebox.showinfo("Reports Folder", base)

    def _cleanup(self):
        days = self._safe_int(self.retention_days.get(), 30)
        if not messagebox.askyesno("Retention Cleanup",
            f"Delete report history entries older than {days} days?\n"
            "(Files on disk are also removed.)"):
            return
        n = self.engine.retention_cleanup()
        self._refresh_history()
        self._refresh_kpis()
        messagebox.showinfo("Cleanup Complete",
            f"Deleted {n} report record{'s' if n != 1 else ''}.")

    # ── KPI refresh ───────────────────────────────────────────────────────────

    def _refresh_kpis(self):
        rows = self.engine.list_history(limit=500)
        n    = len(rows)
        self._kpi_report_count.set(str(n))

        if rows:
            latest = max(rows, key=lambda r: r.generation_time or "")
            self._kpi_last_gen.set(latest.generation_time or "—")
        else:
            self._kpi_last_gen.set("—")

        # Storage used
        total_kb = sum(r.file_size_kb or 0 for r in rows)
        if total_kb >= 1024:
            self._kpi_storage.set(f"{total_kb/1024:.1f} MB")
        else:
            self._kpi_storage.set(f"{total_kb} KB")

        # Next scheduled — find earliest enabled schedule
        self._kpi_next_sched.set(self._next_schedule_str())

    def _next_schedule_str(self) -> str:
        sch_list = (self.cfg.get("reports", {}) or {}).get("schedules", []) or []
        now = datetime.now()
        candidates = []

        for s in sch_list:
            if not s.get("enabled"):
                continue
            if s.get("type") == "interval":
                mins = int(s.get("minutes", 0))
                if mins > 0:
                    # Next firing is within next `mins` minutes
                    nxt = now + timedelta(minutes=mins)
                    candidates.append(nxt)
            elif s.get("type") == "daily":
                t_str = s.get("time", "23:59")
                try:
                    hh, mm = map(int, t_str.split(":"))
                    nxt = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
                    if nxt <= now:
                        nxt += timedelta(days=1)
                    candidates.append(nxt)
                except Exception:
                    pass

        if not candidates:
            return "None"
        soonest = min(candidates)
        delta   = soonest - now
        mins    = int(delta.total_seconds() / 60)
        if mins < 60:
            return f"~{mins} min"
        hrs = mins // 60
        return f"~{hrs} h  ({soonest.strftime('%H:%M')})"

    # ── Meter selection ────────────────────────────────────────────────────────

    def _sync_all(self):
        if self.all_meters_var.get():
            for v in self.meter_vars.values():
                v.set(False)

    def _sync_one(self):
        if any(v.get() for v in self.meter_vars.values()):
            self.all_meters_var.set(False)
        else:
            self.all_meters_var.set(True)

    def _selected_meters(self):
        if self.all_meters_var.get():
            return []
        return [mid for mid, v in self.meter_vars.items() if v.get()]

    # ── Settings ──────────────────────────────────────────────────────────────

    def _browse_dir(self):
        p = filedialog.askdirectory(initialdir=self.save_dir.get() or reports_dir())
        if p:
            self.save_dir.set(p)
            self._sync_save_dir_display()

    def _sync_save_dir_display(self):
        raw = (self.save_dir.get() or "").strip()
        if not raw:
            self.save_dir_display.set("Default (Reports folder)")
        elif self._privacy:
            self.save_dir_display.set("Custom folder configured")
        else:
            self.save_dir_display.set(raw)

    def _browse_logo(self):
        p = filedialog.askopenfilename(
            title="Select Logo Image",
            filetypes=[("Image files", "*.png *.jpg *.jpeg *.bmp *.gif"), ("All files", "*.*")],
        )
        if p:
            self.logo_path.set(p)

    def _save_settings(self):
        rcfg     = self.cfg.setdefault("reports", {})
        site_cfg = self.cfg.setdefault("site", {})

        rcfg["facility_code"]   = self.facility_code.get().strip()   or "PLANT01"
        rcfg["facility_name"]   = self.facility_name.get().strip()
        rcfg["company_name"]    = self.company_name.get().strip()
        rcfg["company_contact"] = self.company_contact.get().strip()
        rcfg["logo_path"]       = self.logo_path.get().strip()
        rcfg["auto_save_dir"]   = self.save_dir.get().strip()
        rcfg["watermark"]       = self.watermark.get().strip()
        rcfg["pdf_password"]    = self.pdf_password.get().strip()
        rcfg["retention_days"]  = self._safe_int(self.retention_days.get(), 30)

        nom_v = self._safe_float(self.nominal_vln.get(), 230.0)
        nom_f = self._safe_float(self.nominal_freq.get(), 50.0)
        rcfg["nominal_v_ln"]    = nom_v
        rcfg["nominal_freq_hz"] = nom_f
        site_cfg["nominal_vln"] = nom_v   # keep in sync with site block

        if self.on_save_config:
            self.on_save_config()

        self._settings_status.set("✔ Saved")
        self.after(3000, lambda: self._settings_status.set(""))

    # ── Schedule config helpers ────────────────────────────────────────────────

    def _is_interval_enabled(self, minutes: int) -> bool:
        for s in self._get_schedules():
            if s.get("type") == "interval" and int(s.get("minutes", 0)) == minutes:
                return bool(s.get("enabled", False))
        return False

    def _is_daily_enabled(self) -> bool:
        for s in self._get_schedules():
            if s.get("type") == "daily":
                return bool(s.get("enabled", False))
        return False

    def _daily_time_value(self) -> str:
        for s in self._get_schedules():
            if s.get("type") == "daily":
                return str(s.get("time", "23:59"))
        return "23:59"

    def _schedule_output_value(self) -> str:
        for s in self._get_schedules():
            if isinstance(s, dict) and s.get("output"):
                return str(s["output"]).upper()
        return "HTML"

    def _get_schedules(self) -> list:
        return (self.cfg.get("reports", {}) or {}).get("schedules", []) or []

    def _save_schedules(self):
        rcfg   = self.cfg.setdefault("reports", {})
        outfmt = (self.schedule_output.get() or "HTML").strip().upper()
        schedules = []

        for mins, var in self.interval_vars.items():
            schedules.append({
                "id":          f"interval_{mins}",
                "enabled":     bool(var.get()),
                "type":        "interval",
                "minutes":     int(mins),
                "report_type": "HOURLY" if mins >= 60 else "INSTANT",
                "meters":      "ALL",
                "output":      outfmt,
            })

        schedules.append({
            "id":          "daily_summary",
            "enabled":     bool(self.daily_enable.get()),
            "type":        "daily",
            "time":        self.daily_time.get().strip() or "23:59",
            "report_type": "DAILY",
            "meters":      "ALL",
            "output":      outfmt,
        })

        rcfg["schedules"] = schedules
        if self.on_save_config:
            self.on_save_config()
        self._refresh_kpis()

    # ── Snapshot print ────────────────────────────────────────────────────────

    def _do_print_snapshot(self):
        if callable(self._on_print_snapshot):
            try:
                self._on_print_snapshot()
            except Exception as exc:
                messagebox.showerror("Print Snapshot",
                    f"Failed to generate snapshot:\n{exc}")
        else:
            messagebox.showinfo("Print Snapshot",
                "Snapshot printing is not available.\n"
                "Use Ctrl+P from the main window.")

    # ── Utilities ─────────────────────────────────────────────────────────────

    def _set_msg(self, text: str, kind: str = "normal"):
        t = get_theme()
        color = {"ok": t.good, "error": t.alarm, "normal": t.text}.get(kind, t.text)
        self._msg_var.set(text)
        self._msg_lbl.configure(foreground=color)

    @staticmethod
    def _safe_int(s, default: int) -> int:
        try:
            return int(str(s).strip())
        except Exception:
            return default

    @staticmethod
    def _safe_float(s, default: float) -> float:
        try:
            return float(str(s).strip())
        except Exception:
            return default
