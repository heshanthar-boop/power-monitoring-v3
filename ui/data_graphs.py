from __future__ import annotations

import csv
import math
import time
import datetime as _dt
import tkinter as tk
from tkinter import ttk, filedialog
from typing import Any, Dict, List, Tuple, Optional

from ui.charts import TrendChartFrame, QUICK_WINDOWS
from ui.key_registry import COMMON_CHART_KEYS, key_label, canonical_key
from config.store import save_config
from utils.health import get_meter_age, stale_seconds


# ---------- Helpers (energy bucketing) ----------

def _safe_float(x) -> Optional[float]:
    try:
        v = float(x)
        if v != v or math.isinf(v):
            return None
        return v
    except Exception:
        return None


def _bucket_key(ts: float, mode: str) -> str:
    t = _dt.datetime.fromtimestamp(ts)
    if mode == "daily":
        return t.strftime("%Y-%m-%d")
    if mode == "weekly":
        y, w, _ = t.isocalendar()
        return f"{y}-W{int(w):02d}"
    return t.strftime("%Y-%m")


def compute_energy_bars(t_list: List[float], y_list: List[float], mode: str, max_buckets: int) -> Tuple[List[str], List[float]]:
    """Compute consumption per bucket from cumulative energy series (kWh).
    Uses last - first per bucket. Drops negative deltas (meter reset / rollover).
    """
    if not t_list or not y_list:
        return [], []

    buckets: Dict[str, Dict[str, float]] = {}
    for ts, y in zip(t_list, y_list):
        tsf = _safe_float(ts)
        yf = _safe_float(y)
        if tsf is None or yf is None:
            continue
        bk = _bucket_key(tsf, mode)
        b = buckets.get(bk)
        if b is None:
            buckets[bk] = {"first": yf, "last": yf}
        else:
            b["last"] = yf

    keys = sorted(buckets.keys())
    if max_buckets and len(keys) > max_buckets:
        keys = keys[-max_buckets:]

    labels: List[str] = []
    vals: List[float] = []
    for k in keys:
        delta = buckets[k]["last"] - buckets[k]["first"]
        if delta < 0:
            continue
        labels.append(k)
        vals.append(delta)

    return labels, vals


# ---------- Energy bar chart widget ----------

class EnergyBarFrame(ttk.Frame):
    """Themed matplotlib bar chart for energy bucketing with summary stats strip."""

    def __init__(self, parent):
        super().__init__(parent)
        self._labels: List[str] = []
        self._values: List[float] = []
        self._build()

    def _build(self):
        from matplotlib.figure import Figure
        from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
        from ui.styles import get_theme
        t = get_theme()

        # ── Summary stats strip ──────────────────────────────────────────────
        stats = ttk.Frame(self, style="TFrame")
        stats.pack(side="top", fill="x", padx=2, pady=(4, 0))

        self._lbl_total  = self._stat_cell(stats, "Total", "—")
        ttk.Separator(stats, orient="vertical").pack(side="left", fill="y", padx=8, pady=4)
        self._lbl_peak   = self._stat_cell(stats, "Peak", "—")
        ttk.Separator(stats, orient="vertical").pack(side="left", fill="y", padx=8, pady=4)
        self._lbl_avg    = self._stat_cell(stats, "Average/period", "—")
        ttk.Separator(stats, orient="vertical").pack(side="left", fill="y", padx=8, pady=4)
        self._lbl_count  = self._stat_cell(stats, "Periods", "—")

        ttk.Separator(self, orient="horizontal").pack(side="top", fill="x", pady=(4, 0))

        # ── Matplotlib figure — fully themed ────────────────────────────────
        BG    = t.card
        FG    = t.text
        MUTED = t.text_muted
        GRID  = t.divider

        self.fig = Figure(dpi=96, facecolor=BG)
        self.ax  = self.fig.add_subplot(111, facecolor=BG)
        self._style_axes(BG, FG, MUTED, GRID)

        self.canvas = FigureCanvasTkAgg(self.fig, master=self)
        self.canvas_widget = self.canvas.get_tk_widget()
        self.canvas_widget.pack(fill="both", expand=True)

        # store colours for update_bars
        self._c_bg    = BG
        self._c_fg    = FG
        self._c_muted = MUTED
        self._c_grid  = GRID
        self._c_bar   = t.good       # normal bar
        self._c_peak  = t.accent     # peak bar highlight
        self._c_warn  = t.warn

    def _stat_cell(self, parent, title: str, initial: str):
        f = ttk.Frame(parent)
        f.pack(side="left", padx=(0, 0))
        ttk.Label(f, text=title, style="Dim.TLabel").pack(anchor="w")
        lbl = ttk.Label(f, text=initial, style="MonoBold.TLabel")
        lbl.pack(anchor="w")
        return lbl

    def _style_axes(self, bg, fg, muted, grid):
        ax = self.ax
        for spine in ax.spines.values():
            spine.set_color(grid)
            spine.set_linewidth(0.8)
        ax.tick_params(colors=muted, labelsize=8, length=3)
        ax.xaxis.label.set_color(muted)
        ax.yaxis.label.set_color(muted)
        ax.title.set_color(fg)

    def update_bars(self, labels: List[str], values: List[float], title: str, unit: str = "kWh"):
        self._labels = labels or []
        self._values = values or []
        self.ax.clear()
        self._style_axes(self._c_bg, self._c_fg, self._c_muted, self._c_grid)

        # Update stats strip
        if self._values:
            total  = sum(self._values)
            peak   = max(self._values)
            avg    = total / len(self._values)
            self._lbl_total.config(text=f"{total:,.1f} {unit}")
            self._lbl_peak.config( text=f"{peak:,.1f} {unit}")
            self._lbl_avg.config(  text=f"{avg:,.1f} {unit}")
            self._lbl_count.config(text=str(len(self._values)))
        else:
            for lbl in (self._lbl_total, self._lbl_peak, self._lbl_avg, self._lbl_count):
                lbl.config(text="—")

        if not self._labels:
            self.ax.set_facecolor(self._c_bg)
            self.ax.text(
                0.5, 0.5,
                "No energy data in this range.\nLogging must be running to accumulate kWh history.",
                ha="center", va="center", transform=self.ax.transAxes,
                color=self._c_muted, fontsize=9, linespacing=1.7,
            )
            self.fig.tight_layout(pad=1.2)
            self.canvas.draw_idle()
            return

        x    = list(range(len(self._labels)))
        peak_i = self._values.index(max(self._values)) if self._values else -1
        colours = [self._c_peak if i == peak_i else self._c_bar for i in x]

        bars = self.ax.bar(x, self._values, color=colours, edgecolor="none", width=0.65)

        # Value labels on top of bars (skip if too many bars)
        if len(self._labels) <= 20:
            for bar, val in zip(bars, self._values):
                h = bar.get_height()
                if h > 0:
                    self.ax.text(
                        bar.get_x() + bar.get_width() / 2, h,
                        f"{val:.1f}",
                        ha="center", va="bottom",
                        color=self._c_muted, fontsize=7,
                    )

        self.ax.set_xticks(x)
        if len(self._labels) > 16:
            step = max(1, len(self._labels) // 10)
            shown = [lbl if i % step == 0 else "" for i, lbl in enumerate(self._labels)]
            self.ax.set_xticklabels(shown, rotation=30, ha="right", fontsize=8, color=self._c_muted)
        else:
            self.ax.set_xticklabels(self._labels, rotation=30, ha="right", fontsize=8, color=self._c_muted)

        self.ax.set_ylabel(unit, color=self._c_muted, fontsize=8)
        self.ax.set_title(title, color=self._c_fg, fontsize=9, pad=6)
        self.ax.grid(True, axis="y", color=self._c_grid, linestyle="--", linewidth=0.6, alpha=0.7)
        self.ax.set_axisbelow(True)
        self.fig.tight_layout(pad=1.2)
        self.canvas.draw_idle()

    def export_csv(self, path: str, source: str, key: str, mode: str):
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["bucket", "kWh", "source", "key", "mode"])
            for lbl, val in zip(self._labels, self._values):
                w.writerow([lbl, f"{float(val):.6f}", source, key, mode])


# ---------- Main Data & Graphs page ----------

class DataGraphsTab(ttk.Frame):
    """Two-chart compare view + energy bar chart + historical trend viewer."""

    def __init__(self, parent, meters, selector, cfg: dict, trend_manager, journal=None, historian_fn=None):
        super().__init__(parent)
        self.meters = meters
        self.selector = selector
        self.cfg = cfg
        self.trend_manager = trend_manager
        self.journal = journal
        self._historian_fn = historian_fn  # callable() -> Historian | None

        self._markers_dirty = True
        self._markers_last_fetch = 0.0
        self._markers_sigA = None
        self._markers_sigB = None
        self._graphs_save_after_id = None
        self._last_update_ts = 0.0
        self._last_energy_update_ts = 0.0

        # Chart selections — set defaults before loading state
        self.selA_meter: Any = "TOTAL"
        self.selA_key: str = "kW"
        self.selB_meter: Any = "TOTAL"
        self.selB_key: str = "Frequency"
        self.window_minutes: int = 15

        # Energy view selection
        self.energy_mode = tk.StringVar(value="daily")
        self.energy_key = tk.StringVar(value="Import_kWh")
        self.energy_source = tk.StringVar(value="TOTAL")

        self._load_graph_state_from_cfg()
        self._build()

    # ----- UI build -----

    def _build(self):
        self.columnconfigure(0, weight=0)   # left panel fixed
        self.columnconfigure(1, weight=1)   # right panel grows
        self.rowconfigure(0, weight=1)

        # ---- Left panel: source selectors ----
        left = ttk.Frame(self, width=240)
        left.grid(row=0, column=0, sticky="nsew", padx=(8, 4), pady=8)
        left.pack_propagate(False)
        left.columnconfigure(0, weight=1)

        ttk.Label(left, text="Chart A — Source", style="H2.TLabel").grid(row=0, column=0, sticky="w", pady=(0, 4))

        # Chart A: meter + key dropdowns
        fA = ttk.Frame(left)
        fA.grid(row=1, column=0, sticky="ew", pady=(0, 6))
        fA.columnconfigure(0, weight=1)
        fA.columnconfigure(1, weight=1)

        self._cmb_A_meter = ttk.Combobox(fA, state="readonly", width=9)
        self._cmb_A_meter.grid(row=0, column=0, sticky="ew", padx=(0, 4))
        self._cmb_A_key = ttk.Combobox(fA, state="readonly", width=14)
        self._cmb_A_key.grid(row=0, column=1, sticky="ew")

        ttk.Label(left, text="Chart B — Source", style="H2.TLabel").grid(row=2, column=0, sticky="w", pady=(8, 4))

        fB = ttk.Frame(left)
        fB.grid(row=3, column=0, sticky="ew", pady=(0, 10))
        fB.columnconfigure(0, weight=1)
        fB.columnconfigure(1, weight=1)

        self._cmb_B_meter = ttk.Combobox(fB, state="readonly", width=9)
        self._cmb_B_meter.grid(row=0, column=0, sticky="ew", padx=(0, 4))
        self._cmb_B_key = ttk.Combobox(fB, state="readonly", width=14)
        self._cmb_B_key.grid(row=0, column=1, sticky="ew")

        self._populate_source_combos()

        self._cmb_A_meter.bind("<<ComboboxSelected>>", lambda _e: self._on_A_changed())
        self._cmb_A_key.bind("<<ComboboxSelected>>", lambda _e: self._on_A_changed())
        self._cmb_B_meter.bind("<<ComboboxSelected>>", lambda _e: self._on_B_changed())
        self._cmb_B_key.bind("<<ComboboxSelected>>", lambda _e: self._on_B_changed())

        ttk.Separator(left, orient="horizontal").grid(row=4, column=0, sticky="ew", pady=6)

        # Time window buttons
        ttk.Label(left, text="Time Window", style="H2.TLabel").grid(row=5, column=0, sticky="w", pady=(0, 4))
        wf = ttk.Frame(left)
        wf.grid(row=6, column=0, sticky="ew")
        self._win_btns = {}
        for i, (mins, label) in enumerate(QUICK_WINDOWS):
            b = ttk.Button(wf, text=label, width=5, command=lambda m=mins: self._set_window(m))
            b.grid(row=0, column=i, padx=2)
            self._win_btns[mins] = b

        ttk.Separator(left, orient="horizontal").grid(row=7, column=0, sticky="ew", pady=6)

        # Action buttons
        ttk.Button(left, text="Go Live", command=self._go_live).grid(row=8, column=0, sticky="ew", pady=(0, 4))
        ttk.Button(left, text="Export CSV — Chart A", command=lambda: self._export_series_csv("A")).grid(row=9, column=0, sticky="ew", pady=(0, 4))
        ttk.Button(left, text="Export CSV — Chart B", command=lambda: self._export_series_csv("B")).grid(row=10, column=0, sticky="ew")

        # ---- Right panel: Notebook ----
        right = ttk.Frame(self)
        right.grid(row=0, column=1, sticky="nsew", padx=(4, 8), pady=8)
        right.columnconfigure(0, weight=1)
        right.rowconfigure(0, weight=1)

        self.nb = ttk.Notebook(right)
        self.nb.grid(row=0, column=0, sticky="nsew")

        # --- Trends tab ---
        trends = ttk.Frame(self.nb)
        trends.columnconfigure(0, weight=1)
        trends.rowconfigure(0, weight=1)   # Chart A
        trends.rowconfigure(1, weight=1)   # Chart B
        self.nb.add(trends, text="Trends")

        max_pts = int((self.cfg.get("charts") or {}).get("max_plot_points", 1000) or 1000)

        self.chartA = TrendChartFrame(
            trends,
            title="Chart A",
            get_available_keys_fn=lambda: list(COMMON_CHART_KEYS),
            on_config_change=None,
            max_plot_points=max_pts,
        )
        self.chartA.grid(row=0, column=0, sticky="nsew", pady=(0, 3))

        self.chartB = TrendChartFrame(
            trends,
            title="Chart B",
            get_available_keys_fn=lambda: list(COMMON_CHART_KEYS),
            on_config_change=None,
            max_plot_points=max_pts,
        )
        self.chartB.grid(row=1, column=0, sticky="nsew", pady=(3, 0))

        self.chartA.set_key(self.selA_key)
        self.chartB.set_key(self.selB_key)
        self.chartA.set_window(self.window_minutes)
        self.chartB.set_window(self.window_minutes)

        # --- Energy tab ---
        energy = ttk.Frame(self.nb)
        energy.columnconfigure(0, weight=1)
        self.nb.add(energy, text="Energy (kWh)")

        # Control bar — two rows so it doesn't crowd
        ectrl = ttk.Frame(energy)
        ectrl.grid(row=0, column=0, sticky="ew", padx=6, pady=(6, 2))

        # Row 1: source + key
        row1 = ttk.Frame(ectrl)
        row1.pack(fill="x", pady=(0, 4))

        ttk.Label(row1, text="Meter:", style="Muted.TLabel").pack(side="left")
        self.cmb_src = ttk.Combobox(row1, state="readonly", width=10, textvariable=self.energy_source)
        self.cmb_src["values"] = self._energy_sources()
        self.cmb_src.pack(side="left", padx=(4, 16))
        self.cmb_src.bind("<<ComboboxSelected>>", lambda _e: self._refresh_energy())

        ttk.Label(row1, text="Register:", style="Muted.TLabel").pack(side="left")
        self.cmb_key = ttk.Combobox(row1, state="readonly", width=18, textvariable=self.energy_key)
        self.cmb_key["values"] = (
            "Import_kWh",
            "Export_kWh",
            "Net_kWh",
            "Lifetime_kWh",
            "Today_kWh",
        )
        self.cmb_key.pack(side="left", padx=(4, 0))
        self.cmb_key.bind("<<ComboboxSelected>>", lambda _e: self._refresh_energy())

        ttk.Button(row1, text="Export CSV", style="Small.TButton",
                   command=self._export_energy_csv).pack(side="right")

        # Row 2: grouping
        row2 = ttk.Frame(ectrl)
        row2.pack(fill="x")

        ttk.Label(row2, text="Group by:", style="Muted.TLabel").pack(side="left")
        for val, txt in (("daily", "Daily"), ("weekly", "Weekly"), ("monthly", "Monthly")):
            ttk.Radiobutton(row2, text=txt, value=val, variable=self.energy_mode,
                            command=self._refresh_energy).pack(side="left", padx=(8, 0))

        ttk.Separator(energy, orient="horizontal").grid(row=1, column=0, sticky="ew", pady=0)

        self.energy_chart = EnergyBarFrame(energy)
        self.energy_chart.grid(row=2, column=0, sticky="nsew", padx=4, pady=(0, 4))
        energy.rowconfigure(2, weight=1)

        # --- Historical tab ---
        self._build_historical_tab()

        self._apply_win_styles()
        self._sync_combos_to_state()
        self._refresh_energy()

    # =====================================================================
    # Historical trend tab
    # =====================================================================

    def _build_historical_tab(self):
        from matplotlib.figure import Figure
        from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
        from ui.styles import get_theme

        hist = ttk.Frame(self.nb)
        hist.columnconfigure(0, weight=1)
        self.nb.add(hist, text="Historical")

        t = get_theme()
        BG = t.card; FG = t.text; MUTED = t.text_muted; GRID = t.divider

        # ---- Control bar ----
        ctrl = ttk.Frame(hist)
        ctrl.grid(row=0, column=0, sticky="ew", padx=6, pady=(6, 2))

        # Row 1: quick presets
        presets_row = ttk.Frame(ctrl)
        presets_row.pack(fill="x", pady=(0, 4))

        ttk.Label(presets_row, text="Range:", style="Muted.TLabel").pack(side="left")
        _presets = [
            ("Today",    0),
            ("Yesterday", 1),
            ("Last 7d",  7),
            ("Last 30d", 30),
            ("Last 90d", 90),
        ]
        self._hist_preset_btns = {}
        for label, days in _presets:
            b = ttk.Button(presets_row, text=label, width=9,
                           command=lambda d=days: self._hist_apply_preset(d))
            b.pack(side="left", padx=2)
            self._hist_preset_btns[days] = b

        # Row 2: custom date range + source + key
        custom_row = ttk.Frame(ctrl)
        custom_row.pack(fill="x", pady=(0, 4))

        ttk.Label(custom_row, text="From:", style="Muted.TLabel").pack(side="left")
        self._hist_from_var = tk.StringVar(value=_dt.date.today().strftime("%Y-%m-%d"))
        ttk.Entry(custom_row, textvariable=self._hist_from_var, width=12).pack(side="left", padx=(4, 12))

        ttk.Label(custom_row, text="To:", style="Muted.TLabel").pack(side="left")
        self._hist_to_var = tk.StringVar(value=_dt.date.today().strftime("%Y-%m-%d"))
        ttk.Entry(custom_row, textvariable=self._hist_to_var, width=12).pack(side="left", padx=(4, 16))

        ttk.Label(custom_row, text="Meter:", style="Muted.TLabel").pack(side="left")
        self._hist_src_var = tk.StringVar(value="TOTAL")
        self._hist_src_cmb = ttk.Combobox(custom_row, textvariable=self._hist_src_var,
                                           state="readonly", width=9)
        self._hist_src_cmb["values"] = self._energy_sources()
        self._hist_src_cmb.pack(side="left", padx=(4, 12))

        ttk.Label(custom_row, text="Parameter:", style="Muted.TLabel").pack(side="left")
        self._hist_key_var = tk.StringVar(value="kW")
        self._hist_key_cmb = ttk.Combobox(custom_row, textvariable=self._hist_key_var,
                                           state="readonly", width=18)
        self._hist_key_cmb["values"] = [key_label(k) for k in COMMON_CHART_KEYS]
        self._hist_key_cmb.set(key_label("kW") or "kW")
        self._hist_key_cmb.pack(side="left", padx=(4, 12))

        ttk.Button(custom_row, text="Query", style="Primary.TButton",
                   command=self._hist_query).pack(side="left", padx=4)
        ttk.Button(custom_row, text="Export CSV",
                   command=self._hist_export_csv).pack(side="left", padx=2)
        ttk.Button(custom_row, text="Export PNG",
                   command=self._hist_export_png).pack(side="left", padx=2)

        # Status label
        self._hist_status_var = tk.StringVar(value="Select a range and press Query.")
        ttk.Label(ctrl, textvariable=self._hist_status_var, style="Muted.TLabel").pack(anchor="w")

        ttk.Separator(hist, orient="horizontal").grid(row=1, column=0, sticky="ew", pady=0)

        # ---- Matplotlib chart ----
        self._hist_fig = Figure(dpi=96, facecolor=BG)
        self._hist_ax  = self._hist_fig.add_subplot(111, facecolor=BG)
        self._hist_canvas = FigureCanvasTkAgg(self._hist_fig, master=hist)
        self._hist_canvas_widget = self._hist_canvas.get_tk_widget()
        self._hist_canvas_widget.grid(row=2, column=0, sticky="nsew", padx=4, pady=(0, 4))
        hist.rowconfigure(2, weight=1)

        # colour cache
        self._hist_theme = {"bg": BG, "fg": FG, "muted": MUTED, "grid": GRID, "line": t.accent}

        # data cache for export
        self._hist_t_list: List[float] = []
        self._hist_y_list: List[float] = []
        self._hist_last_label: str = ""
        self._hist_last_unit: str = ""

        self._hist_draw_empty("Select a date range and press Query.")

    def _hist_style_axes(self):
        c = self._hist_theme
        ax = self._hist_ax
        for spine in ax.spines.values():
            spine.set_color(c["grid"])
            spine.set_linewidth(0.8)
        ax.tick_params(colors=c["muted"], labelsize=8, length=3)
        ax.xaxis.label.set_color(c["muted"])
        ax.yaxis.label.set_color(c["muted"])
        ax.title.set_color(c["fg"])
        ax.set_facecolor(c["bg"])
        self._hist_fig.set_facecolor(c["bg"])

    def _hist_draw_empty(self, msg: str):
        self._hist_ax.clear()
        self._hist_style_axes()
        self._hist_ax.text(
            0.5, 0.5, msg,
            ha="center", va="center", transform=self._hist_ax.transAxes,
            color=self._hist_theme["muted"], fontsize=9, linespacing=1.7,
        )
        self._hist_fig.tight_layout(pad=1.2)
        self._hist_canvas.draw_idle()

    def _hist_apply_preset(self, days: int):
        today = _dt.date.today()
        if days == 0:
            # Today
            self._hist_from_var.set(today.strftime("%Y-%m-%d"))
            self._hist_to_var.set(today.strftime("%Y-%m-%d"))
        elif days == 1:
            # Yesterday
            yesterday = today - _dt.timedelta(days=1)
            self._hist_from_var.set(yesterday.strftime("%Y-%m-%d"))
            self._hist_to_var.set(yesterday.strftime("%Y-%m-%d"))
        else:
            start = today - _dt.timedelta(days=days)
            self._hist_from_var.set(start.strftime("%Y-%m-%d"))
            self._hist_to_var.set(today.strftime("%Y-%m-%d"))
        self._hist_query()

    def _hist_parse_dates(self):
        """Return (from_epoch, to_epoch) or raise ValueError."""
        from_s = self._hist_from_var.get().strip()
        to_s   = self._hist_to_var.get().strip()
        d_from = _dt.datetime.strptime(from_s, "%Y-%m-%d")
        d_to   = _dt.datetime.strptime(to_s,   "%Y-%m-%d") + _dt.timedelta(days=1)  # inclusive end
        if d_from > d_to:
            raise ValueError("'From' date must be before 'To' date")
        return d_from.timestamp(), d_to.timestamp()

    def _hist_query(self):
        """Query historian and render the line chart."""
        historian = self._historian_fn() if callable(self._historian_fn) else None
        if historian is None:
            self._hist_status_var.set("No historian available. Enable SQLite logging first.")
            self._hist_draw_empty("No historian database.\nEnable logging (SQLite backend) to record history.")
            return

        try:
            from_epoch, to_epoch = self._hist_parse_dates()
        except ValueError as e:
            self._hist_status_var.set(f"Date error: {e}")
            return

        src_txt = self._hist_src_var.get() or "TOTAL"
        source  = "TOTAL" if src_txt == "TOTAL" else src_txt  # e.g. "M1"
        raw_key_lbl = self._hist_key_cmb.get()
        key = self._label_to_key(raw_key_lbl)

        self._hist_status_var.set("Querying…")
        self.update_idletasks()

        try:
            rows = historian.query_range(from_epoch, to_epoch, sources=[source])
        except Exception as exc:
            self._hist_status_var.set(f"Query failed: {exc}")
            self._hist_draw_empty(f"Query failed:\n{exc}")
            return

        t_list: List[float] = []
        y_list: List[float] = []
        for r in rows:
            ts_e = _safe_float(r.get("ts_epoch"))
            val  = _safe_float((r.get("values") or {}).get(key))
            if ts_e is not None and val is not None:
                t_list.append(ts_e)
                y_list.append(val)

        self._hist_t_list = t_list
        self._hist_y_list = y_list
        unit = _key_unit(key)
        self._hist_last_unit = unit
        self._hist_last_label = f"{source} / {key_label(key) or key}"

        if not t_list:
            self._hist_status_var.set(f"No data found for {source} / {key} in selected range.")
            self._hist_draw_empty("No data found.\nCheck that logging was running during this period.")
            return

        self._hist_render(t_list, y_list, unit)
        self._hist_status_var.set(
            f"{len(t_list):,} points  |  {source} / {key_label(key) or key}  |  "
            f"{_dt.datetime.fromtimestamp(t_list[0]).strftime('%d %b %H:%M')} → "
            f"{_dt.datetime.fromtimestamp(t_list[-1]).strftime('%d %b %H:%M')}"
        )

    def _hist_render(self, t_list: List[float], y_list: List[float], unit: str):
        import matplotlib.dates as mdates
        c = self._hist_theme

        self._hist_ax.clear()
        self._hist_style_axes()

        # Convert epoch to matplotlib date numbers
        dts = [_dt.datetime.fromtimestamp(ts) for ts in t_list]

        self._hist_ax.plot(dts, y_list, color=c["line"], linewidth=1.0, alpha=0.9)

        # Smart x-axis formatting based on span
        span_days = (t_list[-1] - t_list[0]) / 86400.0
        if span_days <= 1:
            self._hist_ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
        elif span_days <= 7:
            self._hist_ax.xaxis.set_major_formatter(mdates.DateFormatter("%d %b %H:%M"))
        else:
            self._hist_ax.xaxis.set_major_formatter(mdates.DateFormatter("%d %b"))
        self._hist_ax.xaxis.set_major_locator(mdates.AutoDateLocator(maxticks=10))
        self._hist_fig.autofmt_xdate(rotation=30, ha="right")

        self._hist_ax.set_ylabel(unit, color=c["muted"], fontsize=8)
        self._hist_ax.set_title(self._hist_last_label, color=c["fg"], fontsize=9, pad=6)
        self._hist_ax.grid(True, axis="y", color=c["grid"], linestyle="--", linewidth=0.6, alpha=0.7)
        self._hist_ax.set_axisbelow(True)
        self._hist_fig.tight_layout(pad=1.2)
        self._hist_canvas.draw_idle()

    def _hist_export_csv(self):
        if not self._hist_t_list:
            return
        path = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV", "*.csv")],
            initialfile=f"history_{self._hist_src_var.get()}_{self._hist_from_var.get()}.csv",
            title="Export Historical Data CSV",
        )
        if not path:
            return
        try:
            with open(path, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow(["ts_epoch", "ts_local", "value", "source", "parameter", "unit"])
                key = self._label_to_key(self._hist_key_cmb.get())
                for ts, val in zip(self._hist_t_list, self._hist_y_list):
                    w.writerow([
                        f"{ts:.3f}",
                        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts)),
                        f"{val:.6f}" if val is not None else "",
                        self._hist_src_var.get(),
                        key,
                        self._hist_last_unit,
                    ])
        except Exception as exc:
            from tkinter import messagebox
            messagebox.showerror("Export failed", str(exc))

    def _hist_export_png(self):
        if not self._hist_t_list:
            return
        path = filedialog.asksaveasfilename(
            defaultextension=".png",
            filetypes=[("PNG image", "*.png")],
            initialfile=f"history_{self._hist_src_var.get()}_{self._hist_from_var.get()}.png",
            title="Export Chart PNG",
        )
        if not path:
            return
        try:
            self._hist_fig.savefig(path, dpi=150, bbox_inches="tight",
                                   facecolor=self._hist_theme["bg"])
        except Exception as exc:
            from tkinter import messagebox
            messagebox.showerror("Export failed", str(exc))

    def _populate_source_combos(self):
        meter_vals = ["TOTAL"] + [f"M{getattr(m, 'meter_id', i)}" for i, m in enumerate(list(self.meters)[:16])
                                   if isinstance(getattr(m, 'meter_id', None), int)]
        key_vals = [key_label(k) for k in COMMON_CHART_KEYS]

        self._cmb_A_meter["values"] = meter_vals
        self._cmb_A_key["values"] = key_vals
        self._cmb_B_meter["values"] = meter_vals
        self._cmb_B_key["values"] = key_vals

    def _sync_combos_to_state(self):
        """Sync dropdown display to internal selA/B state."""
        def _meter_str(m):
            return "TOTAL" if str(m).upper() == "TOTAL" else f"M{m}"

        self._cmb_A_meter.set(_meter_str(self.selA_meter))
        self._cmb_B_meter.set(_meter_str(self.selB_meter))

        # find label matching canonical key
        def _key_lbl(k):
            lbl = key_label(k)
            return lbl if lbl else k

        self._cmb_A_key.set(_key_lbl(self.selA_key))
        self._cmb_B_key.set(_key_lbl(self.selB_key))

    def _energy_sources(self):
        vals = ["TOTAL"]
        for m in list(self.meters)[:16]:
            mid = getattr(m, "meter_id", None)
            if isinstance(mid, int):
                vals.append(f"M{mid}")
        return vals

    # ----- Combo change handlers -----

    def _on_A_changed(self):
        meter_txt = self._cmb_A_meter.get()
        key_txt = self._cmb_A_key.get()
        self.selA_meter = self._parse_meter(meter_txt)
        self.selA_key = self._label_to_key(key_txt)
        self.chartA.set_key(self.selA_key)
        self._markers_dirty = True
        self._force_redraw()
        self._schedule_save_graph_state()

    def _on_B_changed(self):
        meter_txt = self._cmb_B_meter.get()
        key_txt = self._cmb_B_key.get()
        self.selB_meter = self._parse_meter(meter_txt)
        self.selB_key = self._label_to_key(key_txt)
        self.chartB.set_key(self.selB_key)
        self._markers_dirty = True
        self._force_redraw()
        self._schedule_save_graph_state()

    def _parse_meter(self, txt: str):
        if not txt or txt.upper() == "TOTAL":
            return "TOTAL"
        try:
            return int(txt.replace("M", "").replace("m", "").strip())
        except Exception:
            return "TOTAL"

    def _label_to_key(self, label: str) -> str:
        # reverse lookup label → canonical key
        for k in COMMON_CHART_KEYS:
            if key_label(k) == label:
                return canonical_key(k)
        return canonical_key(label)

    # ----- Window / Live -----

    def _set_window(self, minutes: int):
        self.window_minutes = int(minutes)
        self.chartA.set_window(self.window_minutes)
        self.chartB.set_window(self.window_minutes)
        self._apply_win_styles()
        self._force_redraw()

    def _apply_win_styles(self):
        for mins, b in self._win_btns.items():
            try:
                b.configure(style="Accent.TButton" if int(mins) == int(self.window_minutes) else "TButton")
            except Exception:
                pass

    def _go_live(self):
        try:
            self.chartA.go_live()
            self.chartB.go_live()
        except Exception:
            pass
        self._force_redraw()

    def _force_redraw(self):
        self._last_update_ts = 0.0
        self.update_view()
        self._refresh_energy()

    # ----- External navigation hook -----

    def select_source(self, meter_id, slot=None):
        try:
            slot = str(slot or "A").upper()
            mid = "TOTAL" if str(meter_id).upper() == "TOTAL" else int(meter_id)
            if slot == "B":
                self.selB_meter = mid
            else:
                self.selA_meter = mid
            self._sync_combos_to_state()
            self._markers_dirty = True
            self._schedule_save_graph_state()
            self._force_redraw()
        except Exception:
            pass

    def on_show(self):
        self._force_redraw()
        # Keep historian source combo in sync with current meter list
        try:
            self._hist_src_cmb["values"] = self._energy_sources()
        except Exception:
            pass
        # Pre-seed live trend charts from historian on first show (or after
        # window_minutes changes).  This ensures the operator sees history
        # immediately after startup instead of waiting for the ring buffer to fill.
        self._seed_trends_from_historian()

    # -----------------------------------------------------------------------
    # Historian seed for live trend charts
    # -----------------------------------------------------------------------

    def _seed_trends_from_historian(self) -> None:
        """Back-fill the in-memory trend series from the SQLite historian.

        Called once on tab show.  Silently skips if:
        - No historian available.
        - trend_manager has no set_series() method (old interface).
        - Already seeded for the current (meter, key, window) combination.
        """
        historian = self._historian_fn() if callable(self._historian_fn) else None
        if historian is None:
            return

        if not hasattr(self.trend_manager, "set_series"):
            return  # TrendManager doesn't support back-fill

        seed_key = (
            str(self.selA_meter), self.selA_key,
            str(self.selB_meter), self.selB_key,
            self.window_minutes,
        )
        if getattr(self, "_hist_seed_key", None) == seed_key:
            return  # already seeded for this combo
        self._hist_seed_key = seed_key

        now = _dt.datetime.now().timestamp()
        from_epoch = now - self.window_minutes * 60.0

        # Build list of (source, key) pairs we need
        queries = [
            (self._source_to_historian_src(self.selA_meter), self.selA_key),
            (self._source_to_historian_src(self.selB_meter), self.selB_key),
        ]

        for src_hist, key in queries:
            try:
                rows = historian.query_range(from_epoch, now, sources=[src_hist])
                t_list: List[float] = []
                y_list: List[float] = []
                for r in rows:
                    if not r.get("data_valid", 1):
                        continue
                    ts_e = _safe_float(r.get("ts_epoch"))
                    val  = _safe_float((r.get("values") or {}).get(key))
                    if ts_e is not None and val is not None:
                        t_list.append(ts_e)
                        y_list.append(val)

                if t_list:
                    # Convert historian source ("TOTAL", "M1") back to meter_id form
                    meter_id = self._hist_src_to_meter_id(src_hist)
                    self.trend_manager.set_series(meter_id, key, t_list, y_list)
            except Exception:
                pass  # historian unavailable or method missing — silently skip

    @staticmethod
    def _source_to_historian_src(meter_id) -> str:
        """Convert selA/B meter value to historian source string."""
        if str(meter_id).upper() == "TOTAL":
            return "TOTAL"
        return f"M{meter_id}"

    @staticmethod
    def _hist_src_to_meter_id(src: str):
        """Convert historian source string back to meter_id used by trend_manager."""
        if src == "TOTAL":
            return "TOTAL"
        try:
            return int(src[1:])
        except Exception:
            return "TOTAL"

    # ----- Rendering -----

    def update_view(self):
        now = time.time()
        if (now - float(self._last_update_ts or 0.0)) < 0.5:
            return
        self._last_update_ts = now

        try:
            tA, yA = self.trend_manager.get_series(self.selA_meter, self.selA_key, window_minutes=self.window_minutes)
            vA = self.trend_manager.get_version(self.selA_meter, self.selA_key)
        except Exception:
            tA, yA, vA = [], [], 0
        try:
            tB, yB = self.trend_manager.get_series(self.selB_meter, self.selB_key, window_minutes=self.window_minutes)
            vB = self.trend_manager.get_version(self.selB_meter, self.selB_key)
        except Exception:
            tB, yB, vB = [], [], 0

        self.chartA.update_series(tA, yA, vA)
        self.chartB.update_series(tB, yB, vB)

        # Source labels
        try:
            self.chartA.set_source(self._source_label(self.selA_meter))
            self.chartB.set_source(self._source_label(self.selB_meter))
        except Exception:
            pass

        # Quality overlays
        try:
            ss = stale_seconds(self.cfg)
            self.chartA.set_quality(_quality_for_meter(self.meters, self.selA_meter, ss))
            self.chartB.set_quality(_quality_for_meter(self.meters, self.selB_meter, ss))
        except Exception:
            pass

        self._update_alarm_markers_if_needed()
        self._update_energy_if_visible()

    def _source_label(self, meter_id) -> str:
        if str(meter_id).upper() == "TOTAL":
            valid = sum(1 for m in (self.meters or []) if bool(getattr(m, "data_valid", False)))
            total = len(list(self.meters or []))
            return f"TOTAL ({valid}/{total})"
        return f"M{meter_id}"

    # ----- State persistence -----

    def _load_graph_state_from_cfg(self):
        try:
            ui = (self.cfg or {}).setdefault("ui", {})
            gs = ui.get("graphs") or {}
            wm = gs.get("window_minutes")
            if isinstance(wm, (int, float)) and wm > 0:
                self.window_minutes = int(wm)
            self.selA_meter = gs.get("selA_meter", self.selA_meter)
            self.selA_key = gs.get("selA_key", self.selA_key)
            self.selB_meter = gs.get("selB_meter", self.selB_meter)
            self.selB_key = gs.get("selB_key", self.selB_key)

            ge = ui.get("graphs_energy") or {}
            self.energy_source.set(ge.get("source", self.energy_source.get()))
            self.energy_key.set(ge.get("key", self.energy_key.get()))
            self.energy_mode.set(ge.get("mode", self.energy_mode.get()))
        except Exception:
            pass

    def _schedule_save_graph_state(self):
        try:
            if self._graphs_save_after_id:
                self.after_cancel(self._graphs_save_after_id)
            self._graphs_save_after_id = self.after(600, self._save_graph_state_now)
        except Exception:
            pass

    def _save_graph_state_now(self):
        try:
            self._graphs_save_after_id = None
            ui = (self.cfg or {}).setdefault("ui", {})
            gs = ui.setdefault("graphs", {})
            gs["window_minutes"] = float(self.window_minutes)
            gs["selA_meter"] = self.selA_meter
            gs["selA_key"] = self.selA_key
            gs["selB_meter"] = self.selB_meter
            gs["selB_key"] = self.selB_key

            ge = ui.setdefault("graphs_energy", {})
            ge["source"] = self.energy_source.get()
            ge["key"] = self.energy_key.get()
            ge["mode"] = self.energy_mode.get()

            save_config(self.cfg)
        except Exception:
            pass

    # ----- Keys needed by MainWindow trend feeder -----

    def collect_needed_keys(self) -> Dict[Any, set]:
        needed: Dict[Any, set] = {}
        try:
            for meter, key, chart in [
                (self.selA_meter, self.selA_key, getattr(self, "chartA", None)),
                (self.selB_meter, self.selB_key, getattr(self, "chartB", None)),
            ]:
                kc = canonical_key(str(key or ""))
                if not kc:
                    continue
                needed.setdefault(meter, set()).add(kc)
                for ok in (getattr(chart, "overlay_keys", []) or []):
                    okc = canonical_key(ok)
                    if okc:
                        needed[meter].add(okc)

            esrc = str(self.energy_source.get() or "TOTAL")
            ekey = canonical_key(str(self.energy_key.get() or "Import_kWh"))
            if ekey:
                needed.setdefault(esrc, set()).add(ekey)
        except Exception:
            pass
        return needed

    # ----- Alarm markers -----

    def _severity_style(self, severity: str):
        s = (severity or "").upper()
        if s in ("ALARM", "TRIP", "FAULT", "CRITICAL"):
            return ("red", "--")
        if s in ("WARN", "WARNING"):
            return ("orange", ":")
        return ("gray", "-")

    def _meter_id_for_journal(self, meter_id):
        if meter_id in (None, ""):
            return None
        if str(meter_id).upper() == "TOTAL":
            return "TOTAL"
        try:
            return str(int(meter_id))
        except Exception:
            return str(meter_id)

    def _rows_to_markers(self, rows):
        out = []
        try:
            for r in reversed(list(rows or [])):
                ts = float(r["ts"])
                col, ls = self._severity_style(r.get("severity") if hasattr(r, "get") else r["severity"])
                out.append((ts, col, ls))
        except Exception:
            return []
        if len(out) > 80:
            out = out[-80:]
        return out

    def _update_alarm_markers_if_needed(self):
        if not self.journal:
            return
        now = time.time()
        if (not self._markers_dirty) and (now - float(self._markers_last_fetch or 0.0)) < 2.0:
            return
        self._markers_last_fetch = now
        self._markers_dirty = False

        since_ts = now - (float(self.window_minutes) * 60.0)
        midA = self._meter_id_for_journal(self.selA_meter)
        midB = self._meter_id_for_journal(self.selB_meter)

        try:
            rowsA = self.journal.query(since_ts=since_ts, meter_id=midA, kind="ALARM", limit=400)
        except Exception:
            rowsA = []
        try:
            rowsB = self.journal.query(since_ts=since_ts, meter_id=midB, kind="ALARM", limit=400)
        except Exception:
            rowsB = []

        markersA = self._rows_to_markers(rowsA)
        markersB = self._rows_to_markers(rowsB)

        sigA = (midA, len(markersA), markersA[-1][0] if markersA else None)
        sigB = (midB, len(markersB), markersB[-1][0] if markersB else None)

        if sigA != self._markers_sigA:
            self._markers_sigA = sigA
            try:
                self.chartA.set_markers(markersA)
            except Exception:
                pass
        if sigB != self._markers_sigB:
            self._markers_sigB = sigB
            try:
                self.chartB.set_markers(markersB)
            except Exception:
                pass

    def _update_energy_if_visible(self):
        try:
            if self.nb.index("current") != 1:
                return
        except Exception:
            return

        now = time.time()
        if (now - float(self._last_energy_update_ts or 0.0)) < 1.0:
            return
        self._last_energy_update_ts = now
        self._refresh_energy()

    # ----- Energy -----

    def _refresh_energy(self):
        try:
            mode    = self.energy_mode.get()
            raw_key = str(self.energy_key.get() or "Import_kWh")
            key     = canonical_key(raw_key)
            src_txt = self.energy_source.get() or "TOTAL"
            meter   = "TOTAL" if src_txt == "TOTAL" else self._parse_meter(src_txt)
            src_label = "TOTAL" if meter == "TOTAL" else f"M{meter}"

            if mode == "daily":
                wmins, max_b = 60 * 24 * 32, 31
            elif mode == "weekly":
                wmins, max_b = 60 * 24 * 7 * 14, 12
            else:
                wmins, max_b = 60 * 24 * 365, 12

            t_list, y_list = self.trend_manager.get_series(meter, key, window_minutes=wmins)
            labels, vals = compute_energy_bars(t_list, y_list, mode, max_b)

            # Build readable title  e.g. "TOTAL / Import Energy  —  Daily"
            key_lbl = key_label(key) or raw_key
            title = f"{src_label}  /  {key_lbl}  —  {mode.title()}"
            self.energy_chart.update_bars(labels, vals, title, unit="kWh")
        except Exception:
            pass

    def _export_energy_csv(self):
        try:
            mode = self.energy_mode.get()
            key = self.energy_key.get()
            src = self.energy_source.get() or "TOTAL"
            path = filedialog.asksaveasfilename(
                defaultextension=".csv",
                filetypes=[("CSV", "*.csv")],
                initialfile=f"{src}_{key}_{mode}_summary.csv",
                title="Export Energy Summary CSV"
            )
            if not path:
                return
            self.energy_chart.export_csv(path, source=src, key=key, mode=mode)
        except Exception:
            pass

    # ----- CSV export (series) -----

    def _export_series_csv(self, slot: str):
        meter = self.selA_meter if slot == "A" else self.selB_meter
        key = self.selA_key if slot == "A" else self.selB_key
        try:
            win = float(self.window_minutes)
            t_list, y_list = self.trend_manager.get_series(meter, key, window_minutes=win)
            if not t_list:
                return
            src = "TOTAL" if meter == "TOTAL" else f"M{meter}"
            path = filedialog.asksaveasfilename(
                defaultextension=".csv",
                filetypes=[("CSV", "*.csv")],
                initialfile=f"{src}_{key}_{int(win)}m.csv",
                title=f"Export CSV (Chart {slot})"
            )
            if not path:
                return
            with open(path, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow(["ts_epoch", "ts_local", "value", "source", "key"])
                for ts, val in zip(t_list, y_list):
                    tsf = _safe_float(ts)
                    if tsf is None:
                        continue
                    vf = _safe_float(val)
                    w.writerow([
                        f"{tsf:.3f}",
                        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(tsf)),
                        "" if vf is None else vf,
                        src,
                        key
                    ])
        except Exception:
            pass


# ---------- Helpers ----------

_KEY_UNITS: Dict[str, str] = {
    "kW": "kW", "kVA": "kVA", "kVAR": "kVAR",
    "PFavg": "PF", "PF": "PF",
    "Vavg": "V", "V1": "V", "V2": "V", "V3": "V",
    "V12": "V", "V23": "V", "V31": "V",
    "Iavg": "A", "I1": "A", "I2": "A", "I3": "A",
    "Frequency": "Hz",
    "Import_kWh": "kWh", "Export_kWh": "kWh", "Net_kWh": "kWh",
    "Lifetime_kWh": "kWh", "Today_kWh": "kWh",
    "kVARh": "kVARh",
    "THD_V1": "%", "THD_V2": "%", "THD_V3": "%",
    "THD_I1": "%", "THD_I2": "%", "THD_I3": "%",
}


def _key_unit(key: str) -> str:
    return _KEY_UNITS.get(str(key or ""), "")


def _quality_for_meter(meters, meter_id, stale_sec: float):
    try:
        if str(meter_id).upper() == "TOTAL":
            if not meters:
                return "OFFLINE"
            ok = stale = 0
            for m in meters:
                if not getattr(m, "first_data_received", False):
                    continue
                if not bool(getattr(m, "data_valid", False)):
                    continue
                age = get_meter_age(m)
                if age is None or age > float(stale_sec):
                    stale += 1
                else:
                    ok += 1
            if ok <= 0 and stale <= 0:
                return "OFFLINE"
            return "STALE" if stale > 0 else "OK"

        try:
            mid = int(meter_id)
        except Exception:
            mid = meter_id
        target = next((m for m in (meters or []) if getattr(m, "meter_id", None) == mid), None)
        if not target or not getattr(target, "first_data_received", False):
            return "WAIT"
        if not bool(getattr(target, "data_valid", False)):
            return "OFFLINE"
        age = get_meter_age(target)
        if age is None or age > float(stale_sec):
            return "STALE"
        return "OK"
    except Exception:
        return "OK"
