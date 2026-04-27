"""
Data & Graphs Tab — Power Monitoring v3
=========================================
Redesigned for professional power quality analysis workflows.

Layout:
  ┌──────────────────────────────────────────────────────────────┐
  │  Control Panel (left, fixed 260 px)  │  Notebook (right, flex)│
  │  ┌─ Signal A ─────────────────────┐  │  ┣ Trends              │
  │  │  Meter  ▾   Parameter  ▾       │  │  ┣ Energy (kWh)        │
  │  └───────────────────────────────┘  │  ┣ Historical           │
  │  ┌─ Signal B ─────────────────────┐  │  ┗ Power Quality       │
  │  │  Meter  ▾   Parameter  ▾       │  │                        │
  │  └───────────────────────────────┘  │                        │
  │  ┌─ Time Window ──────────────────┐  │                        │
  │  │  [5m][15m][1h][6h][24h]        │  │                        │
  │  └───────────────────────────────┘  │                        │
  │  ┌─ Actions ──────────────────────┐  │                        │
  │  │  ▶ Go Live  ↩ Export A  ↩ B   │  │                        │
  │  └───────────────────────────────┘  │                        │
  └──────────────────────────────────────────────────────────────┘

IEC / IEEE references:
  • IEC 61000-4-30  — power quality measurement methods
  • IEEE 519-2022   — harmonic limits (THD-V 5%, THD-I per table)
  • IEC 60038       — nominal voltage levels
  • EN 50160        — supply voltage characteristics
"""

from __future__ import annotations

import csv
import math
import time
import datetime as _dt
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from typing import Any, Dict, List, Optional, Tuple

from ui.charts import TrendChartFrame, QUICK_WINDOWS
from ui.key_registry import COMMON_CHART_KEYS, key_label, key_unit, canonical_key
from config.store import save_config
from utils.health import get_meter_age, stale_seconds


# ── Helpers ────────────────────────────────────────────────────────────────────

def _safe_float(x) -> Optional[float]:
    try:
        v = float(x)
        return None if (v != v or math.isinf(v)) else v
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


def compute_energy_bars(
    t_list: List[float], y_list: List[float],
    mode: str, max_buckets: int,
) -> Tuple[List[str], List[float]]:
    """Compute consumption per bucket from a cumulative energy series (kWh).

    Uses last − first value per bucket.  Negative deltas (meter reset / rollover)
    are silently dropped per IEC 62056 guidance.
    """
    if not t_list or not y_list:
        return [], []
    buckets: Dict[str, Dict[str, float]] = {}
    for ts, y in zip(t_list, y_list):
        tsf = _safe_float(ts); yf = _safe_float(y)
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
        if delta >= 0:
            labels.append(k)
            vals.append(delta)
    return labels, vals


_KEY_UNITS: Dict[str, str] = {
    "kW": "kW", "kVA": "kVA", "kVAr": "kVAr",
    "PFavg": "p.u.", "PF": "p.u.",
    "Vavg": "V", "V1N": "V", "V2N": "V", "V3N": "V",
    "V12": "V", "V23": "V", "V31": "V",
    "Iavg": "A", "I1": "A", "I2": "A", "I3": "A",
    "Frequency": "Hz",
    "Import_kWh": "kWh", "Export_kWh": "kWh",
    "Net_kWh": "kWh", "Lifetime_kWh": "kWh", "Today_kWh": "kWh",
    "kVARh": "kVArh",
    "THD_V1": "%", "THD_V2": "%", "THD_V3": "%",
    "THD_I1": "%", "THD_I2": "%", "THD_I3": "%",
    "Unbalance": "%", "ROCOF": "Hz/s",
}


def _key_unit(key: str) -> str:
    u = _KEY_UNITS.get(str(key or ""), "")
    if not u:
        try:
            u = key_unit(key) or ""
        except Exception:
            pass
    return u


def _quality_for_meter(meters, meter_id, stale_sec: float) -> str:
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
            if ok == 0 and stale == 0:
                return "OFFLINE"
            return "STALE" if stale > 0 else "OK"
        try:
            mid = int(meter_id)
        except Exception:
            mid = meter_id
        target = next(
            (m for m in (meters or []) if getattr(m, "meter_id", None) == mid), None
        )
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


def _series_stats(y_list: List[float]) -> Dict[str, Optional[float]]:
    """Compute min/max/avg/last from a filtered value list."""
    vals = [v for v in y_list if v is not None]
    if not vals:
        return {"min": None, "max": None, "avg": None, "last": None, "n": 0}
    return {
        "min":  min(vals),
        "max":  max(vals),
        "avg":  sum(vals) / len(vals),
        "last": vals[-1],
        "n":    len(vals),
    }


def _fmt_stat(v: Optional[float], unit: str, ndp: int = 2) -> str:
    if v is None:
        return "—"
    s = f"{v:.{ndp}f}"
    return f"{s} {unit}".strip() if unit else s


# ── Energy bar chart ───────────────────────────────────────────────────────────

class EnergyBarFrame(ttk.Frame):
    """Themed matplotlib bar chart for energy consumption bucketing.

    Statistics strip shows: Total / Peak period / Average / Periods.
    Peak bar is highlighted in accent colour.
    Value labels shown on bars when ≤ 20 buckets.
    """

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

        # ── Stats strip ──────────────────────────────────────────────────────
        stats = ttk.Frame(self, style="Card.TFrame")
        stats.pack(side="top", fill="x", padx=0, pady=(0, 0))

        def _cell(title, col):
            frm = ttk.Frame(stats, style="Card.TFrame")
            frm.pack(side="left", padx=16, pady=6)
            ttk.Label(frm, text=title.upper(),
                      font=("", 7, "bold"), foreground=t.text_muted,
                      background=t.card).pack(anchor="w")
            lbl = ttk.Label(frm, text="—",
                            font=("", 11, "bold"), foreground=t.accent,
                            background=t.card)
            lbl.pack(anchor="w")
            return lbl

        self._lbl_total  = _cell("Total", 0)
        self._lbl_peak   = _cell("Peak period", 1)
        self._lbl_avg    = _cell("Avg / period", 2)
        self._lbl_count  = _cell("Periods", 3)

        ttk.Separator(self, orient="horizontal").pack(fill="x")

        BG = t.card; FG = t.text; MUTED = t.text_muted; GRID = t.divider
        self.fig = Figure(dpi=96, facecolor=BG)
        self.ax  = self.fig.add_subplot(111, facecolor=BG)
        self._style_axes(BG, FG, MUTED, GRID)

        self.canvas = FigureCanvasTkAgg(self.fig, master=self)
        self.canvas.get_tk_widget().pack(fill="both", expand=True)

        self._c = {
            "bg": BG, "fg": FG, "muted": MUTED, "grid": GRID,
            "bar": t.good, "peak": t.accent, "warn": t.warn,
        }

    def _style_axes(self, bg, fg, muted, grid):
        ax = self.ax
        for sp in ax.spines.values():
            sp.set_color(grid); sp.set_linewidth(0.6)
        ax.tick_params(colors=muted, labelsize=8, length=3)
        ax.xaxis.label.set_color(muted)
        ax.yaxis.label.set_color(muted)
        ax.title.set_color(fg)
        self.ax.set_facecolor(bg)

    def update_bars(self, labels: List[str], values: List[float],
                    title: str, unit: str = "kWh"):
        self._labels = labels or []
        self._values = values or []
        c = self._c
        self.ax.clear()
        self._style_axes(c["bg"], c["fg"], c["muted"], c["grid"])

        if self._values:
            total = sum(self._values)
            peak  = max(self._values)
            avg   = total / len(self._values)
            self._lbl_total.config( text=f"{total:,.1f} {unit}")
            self._lbl_peak.config(  text=f"{peak:,.1f} {unit}")
            self._lbl_avg.config(   text=f"{avg:,.1f} {unit}")
            self._lbl_count.config( text=str(len(self._values)))
        else:
            for lbl in (self._lbl_total, self._lbl_peak, self._lbl_avg, self._lbl_count):
                lbl.config(text="—")

        if not self._labels:
            self.ax.text(
                0.5, 0.5,
                "No energy data in this range.\n"
                "Enable CSV or SQLite logging to accumulate kWh history.",
                ha="center", va="center", transform=self.ax.transAxes,
                color=c["muted"], fontsize=9, linespacing=1.8,
            )
            self.fig.tight_layout(pad=1.2)
            self.canvas.draw_idle()
            return

        x = list(range(len(self._labels)))
        peak_i = self._values.index(max(self._values))
        colours = [c["peak"] if i == peak_i else c["bar"] for i in x]
        bars = self.ax.bar(x, self._values, color=colours, edgecolor="none", width=0.65)

        if len(self._labels) <= 20:
            for bar, val in zip(bars, self._values):
                h = bar.get_height()
                if h > 0:
                    self.ax.text(
                        bar.get_x() + bar.get_width() / 2, h + 0.02 * max(self._values),
                        f"{val:.1f}", ha="center", va="bottom",
                        color=c["muted"], fontsize=7,
                    )

        self.ax.set_xticks(x)
        if len(self._labels) > 16:
            step = max(1, len(self._labels) // 10)
            shown = [lbl if i % step == 0 else "" for i, lbl in enumerate(self._labels)]
            self.ax.set_xticklabels(shown, rotation=30, ha="right", fontsize=8, color=c["muted"])
        else:
            self.ax.set_xticklabels(self._labels, rotation=30, ha="right",
                                    fontsize=8, color=c["muted"])

        self.ax.set_ylabel(unit, color=c["muted"], fontsize=8)
        self.ax.set_title(title, color=c["fg"], fontsize=9, pad=6)
        self.ax.grid(True, axis="y", color=c["grid"], linestyle="--", linewidth=0.5, alpha=0.7)
        self.ax.set_axisbelow(True)
        self.fig.tight_layout(pad=1.2)
        self.canvas.draw_idle()

    def export_csv(self, path: str, source: str, key: str, mode: str):
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["period", "kWh", "source", "register", "grouping"])
            for lbl, val in zip(self._labels, self._values):
                w.writerow([lbl, f"{float(val):.6f}", source, key, mode])


# ── Main tab ───────────────────────────────────────────────────────────────────

class DataGraphsTab(ttk.Frame):
    """
    Data & Graphs tab — Power Monitoring v3.

    Four sub-tabs:
      1. Trends       — dual live trend charts (Chart A / Chart B) with stats strip
      2. Energy (kWh) — daily/weekly/monthly bar chart with IEC-style stats
      3. Historical   — date-range query from historian with export
      4. Power Quality — THD, unbalance, PF trend with IEEE 519 reference lines
    """

    def __init__(self, parent, meters, selector, cfg: dict,
                 trend_manager, journal=None, historian_fn=None):
        super().__init__(parent)
        self.meters        = meters
        self.selector      = selector
        self.cfg           = cfg
        self.trend_manager = trend_manager
        self.journal       = journal
        self._historian_fn = historian_fn

        self._markers_dirty      = True
        self._markers_last_fetch = 0.0
        self._markers_sigA       = None
        self._markers_sigB       = None
        self._graphs_save_after_id = None
        self._last_update_ts     = 0.0
        self._last_energy_update_ts = 0.0

        self.selA_meter:    Any = "TOTAL"
        self.selA_key:      str = "kW"
        self.selB_meter:    Any = "TOTAL"
        self.selB_key:      str = "Frequency"
        self.window_minutes: int = 15

        self.energy_mode   = tk.StringVar(value="daily")
        self.energy_key    = tk.StringVar(value="Import_kWh")
        self.energy_source = tk.StringVar(value="TOTAL")

        self._load_graph_state_from_cfg()
        self._build()

    # ── Build ──────────────────────────────────────────────────────────────────

    def _build(self):
        from ui.styles import get_theme
        t = get_theme()

        self.columnconfigure(0, weight=0)   # control panel fixed
        self.columnconfigure(1, weight=1)   # notebook grows
        self.rowconfigure(0, weight=1)

        # ── Left control panel ────────────────────────────────────────────────
        self._build_control_panel(t)

        # ── Right notebook ────────────────────────────────────────────────────
        right = ttk.Frame(self)
        right.grid(row=0, column=1, sticky="nsew", padx=(4, 8), pady=8)
        right.columnconfigure(0, weight=1)
        right.rowconfigure(0, weight=1)

        self.nb = ttk.Notebook(right)
        self.nb.grid(row=0, column=0, sticky="nsew")

        self._build_trends_tab()
        self._build_energy_tab()
        self._build_historical_tab()
        self._build_pq_tab()

        self._apply_win_styles()
        self._sync_combos_to_state()
        self._refresh_energy()

    # ── Control panel ─────────────────────────────────────────────────────────

    def _build_control_panel(self, t):
        left = ttk.Frame(self, width=264)
        left.grid(row=0, column=0, sticky="nsew", padx=(8, 4), pady=8)
        left.pack_propagate(False)
        left.columnconfigure(0, weight=1)
        row = 0

        # ── Signal A card ─────────────────────────────────────────────────────
        self._cp_card(left, "Signal A", row); row += 1
        fA = ttk.Frame(left, style="Card.TFrame")
        fA.grid(row=row, column=0, sticky="ew", padx=2, pady=(0, 6)); row += 1
        fA.columnconfigure(0, weight=1); fA.columnconfigure(1, weight=1)

        ttk.Label(fA, text="Meter", foreground=t.text_muted,
                  font=("", 7), background=t.card).grid(row=0, column=0, sticky="w", padx=8, pady=(4, 0))
        ttk.Label(fA, text="Parameter", foreground=t.text_muted,
                  font=("", 7), background=t.card).grid(row=0, column=1, sticky="w", padx=4, pady=(4, 0))

        self._cmb_A_meter = ttk.Combobox(fA, state="readonly", width=9)
        self._cmb_A_meter.grid(row=1, column=0, sticky="ew", padx=(8, 4), pady=(0, 8))
        self._cmb_A_key = ttk.Combobox(fA, state="readonly", width=14)
        self._cmb_A_key.grid(row=1, column=1, sticky="ew", padx=(0, 8), pady=(0, 8))

        # Signal A live stats strip
        self._statA = self._stats_strip(left, row); row += 1

        ttk.Separator(left, orient="horizontal").grid(row=row, column=0, sticky="ew", pady=4); row += 1

        # ── Signal B card ─────────────────────────────────────────────────────
        self._cp_card(left, "Signal B", row); row += 1
        fB = ttk.Frame(left, style="Card.TFrame")
        fB.grid(row=row, column=0, sticky="ew", padx=2, pady=(0, 6)); row += 1
        fB.columnconfigure(0, weight=1); fB.columnconfigure(1, weight=1)

        ttk.Label(fB, text="Meter", foreground=t.text_muted,
                  font=("", 7), background=t.card).grid(row=0, column=0, sticky="w", padx=8, pady=(4, 0))
        ttk.Label(fB, text="Parameter", foreground=t.text_muted,
                  font=("", 7), background=t.card).grid(row=0, column=1, sticky="w", padx=4, pady=(4, 0))

        self._cmb_B_meter = ttk.Combobox(fB, state="readonly", width=9)
        self._cmb_B_meter.grid(row=1, column=0, sticky="ew", padx=(8, 4), pady=(0, 8))
        self._cmb_B_key = ttk.Combobox(fB, state="readonly", width=14)
        self._cmb_B_key.grid(row=1, column=1, sticky="ew", padx=(0, 8), pady=(0, 8))

        self._statB = self._stats_strip(left, row); row += 1

        ttk.Separator(left, orient="horizontal").grid(row=row, column=0, sticky="ew", pady=4); row += 1

        # ── Time window card ─────────────────────────────────────────────────
        self._cp_card(left, "Time Window", row); row += 1
        wf = ttk.Frame(left)
        wf.grid(row=row, column=0, sticky="ew", padx=4, pady=(0, 6)); row += 1
        self._win_btns = {}
        for i, (mins, label) in enumerate(QUICK_WINDOWS):
            b = ttk.Button(wf, text=label, width=5,
                           command=lambda m=mins: self._set_window(m))
            b.grid(row=0, column=i, padx=2)
            self._win_btns[mins] = b

        # ── Actions card ──────────────────────────────────────────────────────
        ttk.Separator(left, orient="horizontal").grid(row=row, column=0, sticky="ew", pady=4); row += 1
        self._cp_card(left, "Actions", row); row += 1
        af = ttk.Frame(left)
        af.grid(row=row, column=0, sticky="ew", padx=4, pady=(0, 6)); row += 1
        af.columnconfigure(0, weight=1)

        ttk.Button(af, text="▶  Go Live",
                   command=self._go_live,
                   style="Accent.TButton").grid(row=0, column=0, sticky="ew", pady=(0, 3))
        ttk.Button(af, text="↓  Export CSV — Signal A",
                   command=lambda: self._export_series_csv("A")).grid(row=1, column=0, sticky="ew", pady=(0, 3))
        ttk.Button(af, text="↓  Export CSV — Signal B",
                   command=lambda: self._export_series_csv("B")).grid(row=2, column=0, sticky="ew")

        # ── Quality badge ─────────────────────────────────────────────────────
        ttk.Separator(left, orient="horizontal").grid(row=row, column=0, sticky="ew", pady=4); row += 1
        self._quality_var = tk.StringVar(value="")
        self._quality_lbl = ttk.Label(left, textvariable=self._quality_var,
                                       font=("", 7), foreground=t.text_muted)
        self._quality_lbl.grid(row=row, column=0, sticky="w", padx=8, pady=(0, 4))

        # Wire combo events
        self._populate_source_combos()
        self._cmb_A_meter.bind("<<ComboboxSelected>>", lambda _e: self._on_A_changed())
        self._cmb_A_key.bind(  "<<ComboboxSelected>>", lambda _e: self._on_A_changed())
        self._cmb_B_meter.bind("<<ComboboxSelected>>", lambda _e: self._on_B_changed())
        self._cmb_B_key.bind(  "<<ComboboxSelected>>", lambda _e: self._on_B_changed())

    def _cp_card(self, parent, title: str, row: int):
        """Small section title label for the control panel."""
        from ui.styles import get_theme
        t = get_theme()
        ttk.Label(parent, text=title.upper(),
                  font=("", 7, "bold"), foreground=t.accent
                  ).grid(row=row, column=0, sticky="w", padx=6, pady=(6, 2))

    def _stats_strip(self, parent, row: int) -> Dict[str, tk.StringVar]:
        """4-cell stats strip (Min / Max / Avg / Last) — returns dict of StringVars."""
        from ui.styles import get_theme
        t = get_theme()
        frm = ttk.Frame(parent, style="Card.TFrame")
        frm.grid(row=row, column=0, sticky="ew", padx=2, pady=(0, 2))
        for i in range(4):
            frm.columnconfigure(i, weight=1)

        svars = {}
        for col, name in enumerate(("Min", "Max", "Avg", "Last")):
            cell = ttk.Frame(frm, style="Card.TFrame")
            cell.grid(row=0, column=col, padx=4, pady=4, sticky="ew")
            ttk.Label(cell, text=name, font=("", 7), foreground=t.text_muted,
                      background=t.card).pack(anchor="w")
            sv = tk.StringVar(value="—")
            svars[name.lower()] = sv
            ttk.Label(cell, textvariable=sv, font=("", 8, "bold"),
                      foreground=t.text, background=t.card).pack(anchor="w")
        return svars

    def _update_stats_strip(self, svars: Dict[str, tk.StringVar],
                             y_list: List[float], unit: str):
        s = _series_stats(y_list)
        ndp = 3 if unit in ("p.u.", "Hz") else (0 if unit in ("V", "A") and
              any(abs(v) > 100 for v in y_list if v is not None) else 2)
        svars["min"].set( _fmt_stat(s["min"],  unit, ndp))
        svars["max"].set( _fmt_stat(s["max"],  unit, ndp))
        svars["avg"].set( _fmt_stat(s["avg"],  unit, ndp))
        svars["last"].set(_fmt_stat(s["last"], unit, ndp))

    # ── Trends tab ────────────────────────────────────────────────────────────

    def _build_trends_tab(self):
        trends = ttk.Frame(self.nb)
        trends.columnconfigure(0, weight=1)
        trends.rowconfigure(0, weight=1)
        trends.rowconfigure(1, weight=1)
        self.nb.add(trends, text="📈 Trends")

        max_pts = int((self.cfg.get("charts") or {}).get("max_plot_points", 1000) or 1000)

        self.chartA = TrendChartFrame(
            trends, title="Signal A",
            get_available_keys_fn=lambda: list(COMMON_CHART_KEYS),
            on_config_change=None,
            max_plot_points=max_pts,
        )
        self.chartA.grid(row=0, column=0, sticky="nsew", pady=(0, 2))

        self.chartB = TrendChartFrame(
            trends, title="Signal B",
            get_available_keys_fn=lambda: list(COMMON_CHART_KEYS),
            on_config_change=None,
            max_plot_points=max_pts,
        )
        self.chartB.grid(row=1, column=0, sticky="nsew", pady=(2, 0))

        self.chartA.set_key(self.selA_key)
        self.chartB.set_key(self.selB_key)
        self.chartA.set_window(self.window_minutes)
        self.chartB.set_window(self.window_minutes)

    # ── Energy tab ────────────────────────────────────────────────────────────

    def _build_energy_tab(self):
        energy = ttk.Frame(self.nb)
        energy.columnconfigure(0, weight=1)
        self.nb.add(energy, text="⚡ Energy (kWh)")

        ctrl = ttk.Frame(energy)
        ctrl.grid(row=0, column=0, sticky="ew", padx=8, pady=(8, 4))

        # Row 1: Meter + Register
        row1 = ttk.Frame(ctrl)
        row1.pack(fill="x", pady=(0, 4))

        ttk.Label(row1, text="Meter:").pack(side="left")
        self.cmb_src = ttk.Combobox(row1, state="readonly", width=10,
                                     textvariable=self.energy_source)
        self.cmb_src["values"] = self._energy_sources()
        self.cmb_src.pack(side="left", padx=(4, 16))
        self.cmb_src.bind("<<ComboboxSelected>>", lambda _e: self._refresh_energy())

        ttk.Label(row1, text="Register:").pack(side="left")
        self.cmb_key = ttk.Combobox(row1, state="readonly", width=16,
                                     textvariable=self.energy_key)
        self.cmb_key["values"] = [
            "Import_kWh", "Export_kWh", "Net_kWh",
            "Lifetime_kWh", "Today_kWh",
        ]
        self.cmb_key.pack(side="left", padx=(4, 16))
        self.cmb_key.bind("<<ComboboxSelected>>", lambda _e: self._refresh_energy())

        ttk.Button(row1, text="↓ Export CSV",
                   command=self._export_energy_csv).pack(side="right")

        # Row 2: Grouping
        row2 = ttk.Frame(ctrl)
        row2.pack(fill="x")

        ttk.Label(row2, text="Group by:").pack(side="left")
        for val, txt in (("daily", "Daily"), ("weekly", "Weekly"), ("monthly", "Monthly")):
            ttk.Radiobutton(row2, text=txt, value=val, variable=self.energy_mode,
                            command=self._refresh_energy).pack(side="left", padx=(8, 0))

        ttk.Separator(energy, orient="horizontal").grid(row=1, column=0, sticky="ew")
        self.energy_chart = EnergyBarFrame(energy)
        self.energy_chart.grid(row=2, column=0, sticky="nsew", padx=4, pady=(0, 4))
        energy.rowconfigure(2, weight=1)

    # ── Historical tab ────────────────────────────────────────────────────────

    def _build_historical_tab(self):
        from matplotlib.figure import Figure
        from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
        from ui.styles import get_theme
        t = get_theme()

        hist = ttk.Frame(self.nb)
        hist.columnconfigure(0, weight=1)
        self.nb.add(hist, text="📅 Historical")

        BG = t.card; FG = t.text; MUTED = t.text_muted; GRID = t.divider

        # ── Controls ─────────────────────────────────────────────────────────
        ctrl = ttk.Frame(hist)
        ctrl.grid(row=0, column=0, sticky="ew", padx=8, pady=(8, 4))

        # Presets row
        presets = ttk.Frame(ctrl)
        presets.pack(fill="x", pady=(0, 6))

        ttk.Label(presets, text="Preset range:", font=("", 8, "bold")).pack(side="left", padx=(0, 8))
        _PRESETS = [
            ("Today",       0),
            ("Yesterday",   1),
            ("Last 7 d",    7),
            ("Last 30 d",  30),
            ("Last 90 d",  90),
            ("Last 365 d", 365),
        ]
        self._hist_preset_btns = {}
        for label, days in _PRESETS:
            b = ttk.Button(presets, text=label, width=10,
                           command=lambda d=days: self._hist_apply_preset(d))
            b.pack(side="left", padx=2)
            self._hist_preset_btns[days] = b

        # Custom date + source + key row
        custom = ttk.Frame(ctrl)
        custom.pack(fill="x", pady=(0, 4))

        ttk.Label(custom, text="From:").pack(side="left")
        self._hist_from_var = tk.StringVar(value=_dt.date.today().strftime("%Y-%m-%d"))
        ttk.Entry(custom, textvariable=self._hist_from_var, width=12).pack(side="left", padx=(4, 12))

        ttk.Label(custom, text="To:").pack(side="left")
        self._hist_to_var = tk.StringVar(value=_dt.date.today().strftime("%Y-%m-%d"))
        ttk.Entry(custom, textvariable=self._hist_to_var, width=12).pack(side="left", padx=(4, 16))

        ttk.Label(custom, text="Meter:").pack(side="left")
        self._hist_src_var = tk.StringVar(value="TOTAL")
        self._hist_src_cmb = ttk.Combobox(custom, textvariable=self._hist_src_var,
                                           state="readonly", width=10)
        self._hist_src_cmb["values"] = self._energy_sources()
        self._hist_src_cmb.pack(side="left", padx=(4, 12))

        ttk.Label(custom, text="Parameter:").pack(side="left")
        self._hist_key_var = tk.StringVar(value="kW")
        self._hist_key_cmb = ttk.Combobox(custom, textvariable=self._hist_key_var,
                                           state="readonly", width=22)
        self._hist_key_cmb["values"] = [key_label(k) or k for k in COMMON_CHART_KEYS]
        self._hist_key_cmb.set(key_label("kW") or "kW")
        self._hist_key_cmb.pack(side="left", padx=(4, 12))

        ttk.Button(custom, text="▶  Query", style="Accent.TButton",
                   command=self._hist_query).pack(side="left", padx=4)
        ttk.Button(custom, text="↓ CSV",
                   command=self._hist_export_csv).pack(side="left", padx=2)
        ttk.Button(custom, text="↓ PNG",
                   command=self._hist_export_png).pack(side="left", padx=2)

        # Status + stats
        status_row = ttk.Frame(ctrl)
        status_row.pack(fill="x")

        self._hist_status_var = tk.StringVar(value="Select a range and press Query.")
        ttk.Label(status_row, textvariable=self._hist_status_var,
                  foreground=t.text_muted, font=("", 8)).pack(side="left")

        # Historical stats strip
        sf = ttk.Frame(status_row, style="Card.TFrame")
        sf.pack(side="right", padx=8)
        self._hist_stats: Dict[str, tk.StringVar] = {}
        for name in ("Min", "Max", "Avg", "Points"):
            ttk.Label(sf, text=f"  {name}:", font=("", 7), foreground=t.text_muted,
                      background=t.card).pack(side="left")
            sv = tk.StringVar(value="—")
            self._hist_stats[name.lower()] = sv
            ttk.Label(sf, textvariable=sv, font=("", 8, "bold"),
                      foreground=t.accent, background=t.card).pack(side="left", padx=(0, 6))

        ttk.Separator(hist, orient="horizontal").grid(row=1, column=0, sticky="ew")

        # ── Chart ─────────────────────────────────────────────────────────────
        self._hist_fig = Figure(dpi=96, facecolor=BG)
        self._hist_ax  = self._hist_fig.add_subplot(111, facecolor=BG)
        self._hist_canvas = FigureCanvasTkAgg(self._hist_fig, master=hist)
        self._hist_canvas.get_tk_widget().grid(
            row=2, column=0, sticky="nsew", padx=4, pady=(0, 4)
        )
        hist.rowconfigure(2, weight=1)

        self._hist_theme = {
            "bg": BG, "fg": FG, "muted": MUTED, "grid": GRID, "line": t.accent,
            "fill": t.accent, "warn": t.warn, "alarm": t.alarm,
        }
        self._hist_t_list: List[float] = []
        self._hist_y_list: List[float] = []
        self._hist_last_label = ""
        self._hist_last_unit  = ""

        self._hist_draw_empty("Select a date range and press  ▶ Query.")

    # ── Power Quality tab ─────────────────────────────────────────────────────

    def _build_pq_tab(self):
        """
        Power Quality analysis — IEC 61000-4-30 / IEEE 519-2022 / EN 50160.

        Parameters shown (when available from historian):
          • THD-V (R/Y/B)   — IEEE 519 limit: 5 % at PCC
          • THD-I (R/Y/B)   — limit per load current category (shown as advisory)
          • Voltage Unbalance % — EN 50160 limit: 2 %
          • Power Factor trend  — target ≥ 0.95 (CEB/LECO standard)
        """
        from matplotlib.figure import Figure
        from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
        from ui.styles import get_theme
        t = get_theme()

        pq = ttk.Frame(self.nb)
        pq.columnconfigure(0, weight=1)
        self.nb.add(pq, text="〰 Power Quality")

        BG = t.card; FG = t.text; MUTED = t.text_muted; GRID = t.divider

        # ── Controls ──────────────────────────────────────────────────────────
        ctrl = ttk.Frame(pq)
        ctrl.grid(row=0, column=0, sticky="ew", padx=8, pady=(8, 4))
        ctrl_row = ttk.Frame(ctrl)
        ctrl_row.pack(fill="x", pady=(0, 4))

        ttk.Label(ctrl_row, text="Preset range:").pack(side="left", padx=(0, 6))
        _PQ_PRESETS = [
            ("Last 1 h",   1),
            ("Last 4 h",   4),
            ("Last 24 h", 24),
            ("Last 7 d",  7*24),
        ]
        for label, hours in _PQ_PRESETS:
            ttk.Button(ctrl_row, text=label, width=9,
                       command=lambda h=hours: self._pq_apply_preset(h)
                       ).pack(side="left", padx=2)

        ttk.Separator(ctrl_row, orient="vertical").pack(side="left", fill="y", padx=10)

        ttk.Label(ctrl_row, text="Meter:").pack(side="left")
        self._pq_src_var = tk.StringVar(value="TOTAL")
        self._pq_src_cmb = ttk.Combobox(ctrl_row, textvariable=self._pq_src_var,
                                          state="readonly", width=9)
        self._pq_src_cmb["values"] = self._energy_sources()
        self._pq_src_cmb.pack(side="left", padx=(4, 12))

        ttk.Button(ctrl_row, text="▶  Query PQ",
                   style="Accent.TButton",
                   command=self._pq_query).pack(side="left")

        self._pq_status_var = tk.StringVar(value="Select a range and press  ▶ Query PQ.")
        ttk.Label(ctrl, textvariable=self._pq_status_var,
                  foreground=t.text_muted, font=("", 8)).pack(anchor="w")

        # Reference lines legend
        ref_row = ttk.Frame(ctrl, style="Card.TFrame")
        ref_row.pack(fill="x", pady=(4, 0))
        ttk.Label(ref_row, text="Reference limits: ",
                  font=("", 7, "bold"), foreground=MUTED,
                  background=t.card).pack(side="left", padx=6)
        ttk.Label(ref_row,
                  text="THD-V 5 % (IEEE 519)  │  Unbalance 2 % (EN 50160)  │  PF 0.95 (CEB/LECO)",
                  font=("", 7), foreground=MUTED,
                  background=t.card).pack(side="left")

        ttk.Separator(pq, orient="horizontal").grid(row=1, column=0, sticky="ew")

        # ── 2×2 chart grid ────────────────────────────────────────────────────
        chart_frame = ttk.Frame(pq)
        chart_frame.grid(row=2, column=0, sticky="nsew", padx=4, pady=(4, 4))
        chart_frame.columnconfigure((0, 1), weight=1)
        chart_frame.rowconfigure((0, 1), weight=1)
        pq.rowconfigure(2, weight=1)

        # Create 4 subplots: THD-V, THD-I, Unbalance, PF
        fig = Figure(dpi=96, facecolor=BG)
        self._pq_fig = fig
        gs = fig.add_gridspec(2, 2, hspace=0.45, wspace=0.3)
        self._pq_axes = {
            "thdv": fig.add_subplot(gs[0, 0], facecolor=BG),
            "thdi": fig.add_subplot(gs[0, 1], facecolor=BG),
            "unbal": fig.add_subplot(gs[1, 0], facecolor=BG),
            "pf":   fig.add_subplot(gs[1, 1], facecolor=BG),
        }

        self._pq_canvas = FigureCanvasTkAgg(fig, master=chart_frame)
        self._pq_canvas.get_tk_widget().grid(
            row=0, column=0, columnspan=2, sticky="nsew"
        )

        self._pq_theme = {
            "bg": BG, "fg": FG, "muted": MUTED, "grid": GRID,
            "r": t.alarm, "y": t.warn, "b": t.accent,
            "limit": "#ff4444", "ok": t.good,
        }

        self._pq_t_cache: List[float] = []
        self._pq_data_cache: Dict[str, List[float]] = {}
        self._pq_style_all_axes()
        self._pq_draw_empty("Select a range and press  ▶ Query PQ.")

    # ── Power Quality logic ───────────────────────────────────────────────────

    def _pq_apply_preset(self, hours: int):
        end = _dt.datetime.now()
        start = end - _dt.timedelta(hours=hours)
        self._pq_from_epoch = start.timestamp()
        self._pq_to_epoch   = end.timestamp()
        self._pq_query()

    def _pq_query(self):
        historian = self._historian_fn() if callable(self._historian_fn) else None
        if historian is None:
            self._pq_status_var.set(
                "No historian available. Enable SQLite logging first."
            )
            self._pq_draw_empty("No historian database.\nEnable logging to record power quality history.")
            return

        from_epoch = getattr(self, "_pq_from_epoch",
                             _dt.datetime.now().timestamp() - 3600.0)
        to_epoch   = getattr(self, "_pq_to_epoch",
                             _dt.datetime.now().timestamp())

        src_txt = self._pq_src_var.get() or "TOTAL"
        self._pq_status_var.set("Querying…")
        self.update_idletasks()

        try:
            rows = historian.query_range(from_epoch, to_epoch, sources=[src_txt])
        except Exception as exc:
            self._pq_status_var.set(f"Query failed: {exc}")
            self._pq_draw_empty(f"Query failed:\n{exc}")
            return

        PQ_KEYS = [
            "THD_V1", "THD_V2", "THD_V3",
            "THD_I1", "THD_I2", "THD_I3",
            "Unbalance",
            "PFavg",
        ]

        t_list: List[float] = []
        data: Dict[str, List[float]] = {k: [] for k in PQ_KEYS}

        for r in rows:
            ts_e = _safe_float(r.get("ts_epoch"))
            if ts_e is None:
                continue
            vals = r.get("values") or {}
            # Only include rows where at least one PQ key has data
            if not any(_safe_float(vals.get(k)) is not None for k in PQ_KEYS):
                continue
            t_list.append(ts_e)
            for k in PQ_KEYS:
                data[k].append(_safe_float(vals.get(k)))

        self._pq_t_cache    = t_list
        self._pq_data_cache = data

        if not t_list:
            self._pq_status_var.set(
                f"No PQ data found for {src_txt} in selected range. "
                "Ensure THD/Unbalance keys are enabled in logging."
            )
            self._pq_draw_empty("No power quality data found.\nEnable THD logging for this meter.")
            return

        self._pq_render(t_list, data, src_txt)
        span_str = (
            f"{_dt.datetime.fromtimestamp(t_list[0]).strftime('%d %b %H:%M')} → "
            f"{_dt.datetime.fromtimestamp(t_list[-1]).strftime('%d %b %H:%M')}"
        )
        self._pq_status_var.set(
            f"{len(t_list):,} points  |  {src_txt}  |  {span_str}"
        )

    def _pq_style_axes(self, ax, title: str, ylabel: str,
                        limit_val: Optional[float] = None,
                        limit_label: str = "Limit"):
        c = self._pq_theme
        for sp in ax.spines.values():
            sp.set_color(c["grid"]); sp.set_linewidth(0.6)
        ax.tick_params(colors=c["muted"], labelsize=7, length=2)
        ax.set_facecolor(c["bg"])
        ax.set_title(title, color=c["fg"], fontsize=8, pad=4)
        ax.set_ylabel(ylabel, color=c["muted"], fontsize=7)
        ax.grid(True, axis="y", color=c["grid"], linestyle="--",
                linewidth=0.5, alpha=0.6)
        ax.set_axisbelow(True)
        if limit_val is not None:
            ax.axhline(limit_val, color=c["limit"], linewidth=1.0,
                       linestyle="--", alpha=0.8, label=limit_label)

    def _pq_style_all_axes(self):
        axes = self._pq_axes
        self._pq_style_axes(axes["thdv"],  "THD — Voltage (IEEE 519)", "%",
                             limit_val=5.0, limit_label="IEEE 519: 5%")
        self._pq_style_axes(axes["thdi"],  "THD — Current (IEEE 519)", "%")
        self._pq_style_axes(axes["unbal"], "Voltage Unbalance (EN 50160)", "%",
                             limit_val=2.0, limit_label="EN 50160: 2%")
        self._pq_style_axes(axes["pf"],    "Power Factor Trend", "p.u.",
                             limit_val=0.95, limit_label="Target: 0.95")

    def _pq_draw_empty(self, msg: str):
        c = self._pq_theme
        for ax in self._pq_axes.values():
            ax.clear()
        self._pq_style_all_axes()
        # Put message only in top-left
        self._pq_axes["thdv"].text(
            0.5, 0.5, msg,
            ha="center", va="center",
            transform=self._pq_axes["thdv"].transAxes,
            color=c["muted"], fontsize=8, linespacing=1.7,
        )
        self._pq_fig.tight_layout(pad=1.2)
        self._pq_canvas.draw_idle()

    def _pq_render(self, t_list: List[float], data: Dict[str, List[float]],
                   source: str):
        import matplotlib.dates as mdates
        c = self._pq_theme

        dts = [_dt.datetime.fromtimestamp(ts) for ts in t_list]
        span_days = (t_list[-1] - t_list[0]) / 86400.0 if len(t_list) > 1 else 0

        def _fmt_x(ax):
            if span_days <= 1:
                ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
            elif span_days <= 7:
                ax.xaxis.set_major_formatter(mdates.DateFormatter("%d %b %H:%M"))
            else:
                ax.xaxis.set_major_formatter(mdates.DateFormatter("%d %b"))
            ax.xaxis.set_major_locator(mdates.AutoDateLocator(maxticks=6))

        axes = self._pq_axes

        # ── THD-V ─────────────────────────────────────────────────────────────
        ax = axes["thdv"]
        ax.clear()
        self._pq_style_axes(ax, "THD — Voltage  (IEEE 519)", "%",
                             limit_val=5.0, limit_label="IEEE 519: 5%")
        for key, color, lbl in (
            ("THD_V1", c["r"],  "R (L1)"),
            ("THD_V2", c["y"],  "Y (L2)"),
            ("THD_V3", c["b"],  "B (L3)"),
        ):
            ys = [v for v in data.get(key, []) if v is not None]
            if ys and len(ys) == len(dts):
                ax.plot(dts, ys, linewidth=0.9, color=color, label=lbl, alpha=0.9)
            elif ys:
                _dts_f = [d for d, v in zip(dts, data.get(key, [])) if v is not None]
                ax.plot(_dts_f, ys, linewidth=0.9, color=color, label=lbl, alpha=0.9)
        ax.legend(fontsize=6, loc="upper right",
                  facecolor=c["bg"], edgecolor=c["grid"], labelcolor=c["muted"])
        _fmt_x(ax)

        # ── THD-I ─────────────────────────────────────────────────────────────
        ax = axes["thdi"]
        ax.clear()
        self._pq_style_axes(ax, "THD — Current  (IEEE 519)", "%")
        for key, color, lbl in (
            ("THD_I1", c["r"],  "R (L1)"),
            ("THD_I2", c["y"],  "Y (L2)"),
            ("THD_I3", c["b"],  "B (L3)"),
        ):
            ys = [v for v in data.get(key, []) if v is not None]
            if ys:
                _dts_f = [d for d, v in zip(dts, data.get(key, [])) if v is not None]
                ax.plot(_dts_f, ys, linewidth=0.9, color=color, label=lbl, alpha=0.9)
        ax.legend(fontsize=6, loc="upper right",
                  facecolor=c["bg"], edgecolor=c["grid"], labelcolor=c["muted"])
        ax.text(0.02, 0.96,
                "Advisory — IEEE 519 current limits\ndepend on ISC/IL ratio at PCC",
                transform=ax.transAxes, fontsize=6,
                color=c["muted"], va="top", ha="left",
                bbox={"facecolor": c["bg"], "edgecolor": "none", "pad": 2})
        _fmt_x(ax)

        # ── Unbalance ─────────────────────────────────────────────────────────
        ax = axes["unbal"]
        ax.clear()
        self._pq_style_axes(ax, "Voltage Unbalance  (EN 50160)", "%",
                             limit_val=2.0, limit_label="EN 50160: 2%")
        unbal = [v for v in data.get("Unbalance", []) if v is not None]
        if unbal:
            _dts_f = [d for d, v in zip(dts, data.get("Unbalance", [])) if v is not None]
            # Fill red zone above limit
            ax.fill_between(_dts_f, unbal, 2.0,
                            where=[v > 2.0 for v in unbal],
                            alpha=0.25, color=c["r"], label=">2% (out of EN 50160)")
            ax.plot(_dts_f, unbal, linewidth=1.0, color=c["y"],
                    label="Unbalance %", alpha=0.9)
            ax.legend(fontsize=6, loc="upper right",
                      facecolor=c["bg"], edgecolor=c["grid"], labelcolor=c["muted"])
        _fmt_x(ax)

        # ── Power Factor ──────────────────────────────────────────────────────
        ax = axes["pf"]
        ax.clear()
        self._pq_style_axes(ax, "Power Factor Trend", "p.u.",
                             limit_val=0.95, limit_label="Target: 0.95")
        pf_vals = [v for v in data.get("PFavg", []) if v is not None]
        if pf_vals:
            _dts_f = [d for d, v in zip(dts, data.get("PFavg", [])) if v is not None]
            # Colour below-limit zone
            ax.fill_between(_dts_f, pf_vals, 0.95,
                            where=[v < 0.95 for v in pf_vals],
                            alpha=0.20, color=c["r"], label="Below 0.95")
            ax.fill_between(_dts_f, pf_vals, 0.95,
                            where=[v >= 0.95 for v in pf_vals],
                            alpha=0.12, color=c["ok"])
            ax.plot(_dts_f, pf_vals, linewidth=1.0, color=c["ok"],
                    alpha=0.9, label="PF")
            ax.set_ylim(0.5, 1.02)
            ax.legend(fontsize=6, loc="lower right",
                      facecolor=c["bg"], edgecolor=c["grid"], labelcolor=c["muted"])
        _fmt_x(ax)

        self._pq_fig.autofmt_xdate(rotation=25, ha="right")
        self._pq_fig.tight_layout(pad=1.0)
        self._pq_canvas.draw_idle()

    # ── Historical rendering ───────────────────────────────────────────────────

    def _hist_style_axes(self):
        c = self._hist_theme
        ax = self._hist_ax
        for sp in ax.spines.values():
            sp.set_color(c["grid"]); sp.set_linewidth(0.6)
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
            self._hist_from_var.set(today.strftime("%Y-%m-%d"))
            self._hist_to_var.set(today.strftime("%Y-%m-%d"))
        elif days == 1:
            y = today - _dt.timedelta(days=1)
            self._hist_from_var.set(y.strftime("%Y-%m-%d"))
            self._hist_to_var.set(y.strftime("%Y-%m-%d"))
        else:
            self._hist_from_var.set((today - _dt.timedelta(days=days)).strftime("%Y-%m-%d"))
            self._hist_to_var.set(today.strftime("%Y-%m-%d"))
        self._hist_query()

    def _hist_parse_dates(self) -> Tuple[float, float]:
        from_s = self._hist_from_var.get().strip()
        to_s   = self._hist_to_var.get().strip()
        d_from = _dt.datetime.strptime(from_s, "%Y-%m-%d")
        d_to   = _dt.datetime.strptime(to_s,   "%Y-%m-%d") + _dt.timedelta(days=1)
        if d_from > d_to:
            raise ValueError("'From' date must be ≤ 'To' date")
        return d_from.timestamp(), d_to.timestamp()

    def _hist_query(self):
        historian = self._historian_fn() if callable(self._historian_fn) else None
        if historian is None:
            self._hist_status_var.set("No historian. Enable SQLite logging first.")
            self._hist_draw_empty(
                "No historian database.\nEnable logging (SQLite backend) to record history."
            )
            return
        try:
            from_epoch, to_epoch = self._hist_parse_dates()
        except ValueError as e:
            self._hist_status_var.set(f"Date error: {e}")
            return

        src_txt = self._hist_src_var.get() or "TOTAL"
        raw_lbl = self._hist_key_cmb.get()
        key     = self._label_to_key(raw_lbl)

        self._hist_status_var.set("Querying…")
        self.update_idletasks()

        try:
            rows = historian.query_range(from_epoch, to_epoch, sources=[src_txt])
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

        self._hist_t_list    = t_list
        self._hist_y_list    = y_list
        unit = _key_unit(key)
        self._hist_last_unit  = unit
        self._hist_last_label = f"{src_txt} / {key_label(key) or key}"

        # Update stats strip
        if y_list:
            s = _series_stats(y_list)
            ndp = 3 if unit in ("p.u.", "Hz") else 2
            self._hist_stats["min"].set(    _fmt_stat(s["min"],  unit, ndp))
            self._hist_stats["max"].set(    _fmt_stat(s["max"],  unit, ndp))
            self._hist_stats["avg"].set(    _fmt_stat(s["avg"],  unit, ndp))
            self._hist_stats["points"].set( str(s["n"]))

        if not t_list:
            self._hist_status_var.set(
                f"No data found for {src_txt} / {key_label(key) or key}."
            )
            self._hist_draw_empty("No data found.\nCheck that logging was running during this period.")
            return

        self._hist_render(t_list, y_list, unit)
        self._hist_status_var.set(
            f"{len(t_list):,} points  ·  {src_txt} / {key_label(key) or key}  ·  "
            f"{_dt.datetime.fromtimestamp(t_list[0]).strftime('%d %b %H:%M')} → "
            f"{_dt.datetime.fromtimestamp(t_list[-1]).strftime('%d %b %H:%M')}"
        )

    def _hist_render(self, t_list: List[float], y_list: List[float], unit: str):
        import matplotlib.dates as mdates
        c = self._hist_theme

        self._hist_ax.clear()
        self._hist_style_axes()

        dts = [_dt.datetime.fromtimestamp(ts) for ts in t_list]
        span_days = (t_list[-1] - t_list[0]) / 86400.0 if len(t_list) > 1 else 0

        # Subtle fill under line
        self._hist_ax.fill_between(dts, y_list, alpha=0.08, color=c["line"])
        self._hist_ax.plot(dts, y_list, color=c["line"], linewidth=1.0, alpha=0.9)

        # Annotate min/max on chart
        try:
            ymin_i = y_list.index(min(y_list))
            ymax_i = y_list.index(max(y_list))
            for idx, sym, va in ((ymin_i, "▼", "top"), (ymax_i, "▲", "bottom")):
                self._hist_ax.annotate(
                    f"{sym} {y_list[idx]:.2f} {unit}",
                    xy=(dts[idx], y_list[idx]),
                    fontsize=7, color=c["muted"],
                    xytext=(0, 8 if va == "bottom" else -8),
                    textcoords="offset points",
                    ha="center", va=va,
                )
        except Exception:
            pass

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
        self._hist_ax.grid(True, axis="y", color=c["grid"],
                            linestyle="--", linewidth=0.5, alpha=0.7)
        self._hist_ax.set_axisbelow(True)
        self._hist_fig.tight_layout(pad=1.2)
        self._hist_canvas.draw_idle()

    def _hist_export_csv(self):
        if not self._hist_t_list:
            messagebox.showinfo("No data", "Run a query first.")
            return
        path = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV", "*.csv")],
            initialfile=f"history_{self._hist_src_var.get()}_{self._hist_from_var.get()}.csv",
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
                        self._hist_src_var.get(), key, self._hist_last_unit,
                    ])
        except Exception as exc:
            messagebox.showerror("Export failed", str(exc))

    def _hist_export_png(self):
        if not self._hist_t_list:
            messagebox.showinfo("No data", "Run a query first.")
            return
        path = filedialog.asksaveasfilename(
            defaultextension=".png",
            filetypes=[("PNG image", "*.png")],
            initialfile=f"history_{self._hist_src_var.get()}_{self._hist_from_var.get()}.png",
        )
        if not path:
            return
        try:
            self._hist_fig.savefig(path, dpi=150, bbox_inches="tight",
                                    facecolor=self._hist_theme["bg"])
        except Exception as exc:
            messagebox.showerror("Export failed", str(exc))

    # ── Source combo population ────────────────────────────────────────────────

    def _populate_source_combos(self):
        meter_vals = self._energy_sources()
        key_vals   = [key_label(k) or k for k in COMMON_CHART_KEYS]

        self._cmb_A_meter["values"] = meter_vals
        self._cmb_A_key["values"]   = key_vals
        self._cmb_B_meter["values"] = meter_vals
        self._cmb_B_key["values"]   = key_vals

    def _energy_sources(self) -> List[str]:
        vals = ["TOTAL"]
        for m in list(self.meters)[:16]:
            mid  = getattr(m, "meter_id", None)
            name = getattr(m, "name", None) or f"M{mid}"
            if isinstance(mid, int):
                vals.append(f"M{mid}" if not name or name == f"M{mid}"
                            else f"M{mid} – {name}")
        return vals

    def _sync_combos_to_state(self):
        def _ms(m):
            return "TOTAL" if str(m).upper() == "TOTAL" else f"M{m}"
        def _kl(k):
            return key_label(k) or k

        # Match meter string to the values list (may include name suffix)
        def _set_meter_cmb(cmb, meter):
            ms = _ms(meter)
            for v in (cmb["values"] or []):
                if str(v).startswith(ms):
                    cmb.set(v); return
            cmb.set(ms)

        _set_meter_cmb(self._cmb_A_meter, self.selA_meter)
        _set_meter_cmb(self._cmb_B_meter, self.selB_meter)
        self._cmb_A_key.set(_kl(self.selA_key))
        self._cmb_B_key.set(_kl(self.selB_key))

    # ── Combo event handlers ──────────────────────────────────────────────────

    def _on_A_changed(self):
        self.selA_meter = self._parse_meter(self._cmb_A_meter.get())
        self.selA_key   = self._label_to_key(self._cmb_A_key.get())
        self.chartA.set_key(self.selA_key)
        self._markers_dirty = True
        self._force_redraw()
        self._schedule_save_graph_state()

    def _on_B_changed(self):
        self.selB_meter = self._parse_meter(self._cmb_B_meter.get())
        self.selB_key   = self._label_to_key(self._cmb_B_key.get())
        self.chartB.set_key(self.selB_key)
        self._markers_dirty = True
        self._force_redraw()
        self._schedule_save_graph_state()

    def _parse_meter(self, txt: str):
        if not txt:
            return "TOTAL"
        # Strip optional name suffix "M1 – Name" → "M1"
        txt = txt.split("–")[0].split("—")[0].strip()
        if txt.upper() == "TOTAL":
            return "TOTAL"
        try:
            return int(txt.replace("M", "").replace("m", "").strip())
        except Exception:
            return "TOTAL"

    def _label_to_key(self, label: str) -> str:
        for k in COMMON_CHART_KEYS:
            if (key_label(k) or k) == label:
                return canonical_key(k)
        return canonical_key(label)

    # ── Window / Live ─────────────────────────────────────────────────────────

    def _set_window(self, minutes: int):
        self.window_minutes = int(minutes)
        self.chartA.set_window(self.window_minutes)
        self.chartB.set_window(self.window_minutes)
        self._apply_win_styles()
        self._force_redraw()

    def _apply_win_styles(self):
        for mins, b in self._win_btns.items():
            try:
                b.configure(
                    style="Accent.TButton" if int(mins) == int(self.window_minutes)
                    else "TButton"
                )
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

    # ── External hooks ────────────────────────────────────────────────────────

    def select_source(self, meter_id, slot=None):
        try:
            slot = str(slot or "A").upper()
            mid  = "TOTAL" if str(meter_id).upper() == "TOTAL" else int(meter_id)
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
        try:
            sources = self._energy_sources()
            self._hist_src_cmb["values"]  = sources
            self._pq_src_cmb["values"]    = sources
            self.cmb_src["values"]        = sources
            self._populate_source_combos()
        except Exception:
            pass
        self._seed_trends_from_historian()

    # ── Historian seed for live charts ────────────────────────────────────────

    def _seed_trends_from_historian(self) -> None:
        historian = self._historian_fn() if callable(self._historian_fn) else None
        if historian is None or not hasattr(self.trend_manager, "set_series"):
            return

        seed_key = (
            str(self.selA_meter), self.selA_key,
            str(self.selB_meter), self.selB_key,
            self.window_minutes,
        )
        if getattr(self, "_hist_seed_key", None) == seed_key:
            return
        self._hist_seed_key = seed_key

        now        = _dt.datetime.now().timestamp()
        from_epoch = now - self.window_minutes * 60.0

        for src_hist, key in [
            (self._source_to_historian_src(self.selA_meter), self.selA_key),
            (self._source_to_historian_src(self.selB_meter), self.selB_key),
        ]:
            try:
                rows   = historian.query_range(from_epoch, now, sources=[src_hist])
                t_list = []
                y_list = []
                for r in rows:
                    if not r.get("data_valid", 1):
                        continue
                    ts_e = _safe_float(r.get("ts_epoch"))
                    val  = _safe_float((r.get("values") or {}).get(key))
                    if ts_e is not None and val is not None:
                        t_list.append(ts_e)
                        y_list.append(val)
                if t_list:
                    self.trend_manager.set_series(
                        self._hist_src_to_meter_id(src_hist), key, t_list, y_list
                    )
            except Exception:
                pass

    @staticmethod
    def _source_to_historian_src(meter_id) -> str:
        return "TOTAL" if str(meter_id).upper() == "TOTAL" else f"M{meter_id}"

    @staticmethod
    def _hist_src_to_meter_id(src: str):
        if src == "TOTAL":
            return "TOTAL"
        try:
            return int(src[1:])
        except Exception:
            return "TOTAL"

    # ── Live update ───────────────────────────────────────────────────────────

    def update_view(self):
        now = time.time()
        if (now - float(self._last_update_ts or 0.0)) < 0.5:
            return
        self._last_update_ts = now

        try:
            tA, yA = self.trend_manager.get_series(
                self.selA_meter, self.selA_key, window_minutes=self.window_minutes)
            vA = self.trend_manager.get_version(self.selA_meter, self.selA_key)
        except Exception:
            tA, yA, vA = [], [], 0

        try:
            tB, yB = self.trend_manager.get_series(
                self.selB_meter, self.selB_key, window_minutes=self.window_minutes)
            vB = self.trend_manager.get_version(self.selB_meter, self.selB_key)
        except Exception:
            tB, yB, vB = [], [], 0

        self.chartA.update_series(tA, yA, vA)
        self.chartB.update_series(tB, yB, vB)

        try:
            self.chartA.set_source(self._source_label(self.selA_meter))
            self.chartB.set_source(self._source_label(self.selB_meter))
        except Exception:
            pass

        try:
            ss = stale_seconds(self.cfg)
            qA = _quality_for_meter(self.meters, self.selA_meter, ss)
            qB = _quality_for_meter(self.meters, self.selB_meter, ss)
            self.chartA.set_quality(qA)
            self.chartB.set_quality(qB)
            self._quality_var.set(
                f"A: {self._source_label(self.selA_meter)} [{qA}]   "
                f"B: {self._source_label(self.selB_meter)} [{qB}]"
            )
        except Exception:
            pass

        # Update control panel stats strips
        try:
            uA = _key_unit(self.selA_key)
            self._update_stats_strip(self._statA, yA, uA)
        except Exception:
            pass
        try:
            uB = _key_unit(self.selB_key)
            self._update_stats_strip(self._statB, yB, uB)
        except Exception:
            pass

        self._update_alarm_markers_if_needed()
        self._update_energy_if_visible()

    def _source_label(self, meter_id) -> str:
        if str(meter_id).upper() == "TOTAL":
            valid = sum(1 for m in (self.meters or [])
                        if bool(getattr(m, "data_valid", False)))
            total = len(list(self.meters or []))
            return f"TOTAL ({valid}/{total})"
        # Try to get name
        try:
            mid = int(meter_id)
            m = next((x for x in (self.meters or [])
                      if getattr(x, "meter_id", None) == mid), None)
            if m:
                name = getattr(m, "name", None)
                return f"M{mid}" if not name else f"M{mid} – {name}"
        except Exception:
            pass
        return f"M{meter_id}"

    # ── State persistence ─────────────────────────────────────────────────────

    def _load_graph_state_from_cfg(self):
        try:
            ui = (self.cfg or {}).setdefault("ui", {})
            gs = ui.get("graphs") or {}
            wm = gs.get("window_minutes")
            if isinstance(wm, (int, float)) and wm > 0:
                self.window_minutes = int(wm)
            self.selA_meter = gs.get("selA_meter", self.selA_meter)
            self.selA_key   = gs.get("selA_key",   self.selA_key)
            self.selB_meter = gs.get("selB_meter", self.selB_meter)
            self.selB_key   = gs.get("selB_key",   self.selB_key)

            ge = ui.get("graphs_energy") or {}
            self.energy_source.set(ge.get("source", self.energy_source.get()))
            self.energy_key.set(   ge.get("key",    self.energy_key.get()))
            self.energy_mode.set(  ge.get("mode",   self.energy_mode.get()))
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
            gs["selA_meter"]     = self.selA_meter
            gs["selA_key"]       = self.selA_key
            gs["selB_meter"]     = self.selB_meter
            gs["selB_key"]       = self.selB_key

            ge = ui.setdefault("graphs_energy", {})
            ge["source"] = self.energy_source.get()
            ge["key"]    = self.energy_key.get()
            ge["mode"]   = self.energy_mode.get()

            save_config(self.cfg)
        except Exception:
            pass

    # ── Needed keys for trend feeder ──────────────────────────────────────────

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

    # ── Alarm markers ─────────────────────────────────────────────────────────

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
                ts  = float(r["ts"])
                col, ls = self._severity_style(
                    r.get("severity") if hasattr(r, "get") else r["severity"]
                )
                out.append((ts, col, ls))
        except Exception:
            return []
        return out[-80:] if len(out) > 80 else out

    def _update_alarm_markers_if_needed(self):
        if not self.journal:
            return
        now = time.time()
        if (not self._markers_dirty) and (now - float(self._markers_last_fetch or 0.0)) < 2.0:
            return
        self._markers_last_fetch = now
        self._markers_dirty      = False

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

    # ── Energy refresh ────────────────────────────────────────────────────────

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

    def _refresh_energy(self):
        try:
            mode    = self.energy_mode.get()
            raw_key = str(self.energy_key.get() or "Import_kWh")
            key     = canonical_key(raw_key)
            src_txt = self.energy_source.get() or "TOTAL"
            meter   = "TOTAL" if src_txt.startswith("TOTAL") else self._parse_meter(src_txt)
            src_lbl = "TOTAL" if meter == "TOTAL" else f"M{meter}"

            if mode == "daily":
                wmins, max_b = 60 * 24 * 32, 31
            elif mode == "weekly":
                wmins, max_b = 60 * 24 * 7 * 14, 12
            else:
                wmins, max_b = 60 * 24 * 365, 12

            t_list, y_list = self.trend_manager.get_series(meter, key, window_minutes=wmins)
            labels, vals   = compute_energy_bars(t_list, y_list, mode, max_b)

            key_lbl = key_label(key) or raw_key
            title   = f"{src_lbl}  /  {key_lbl}  —  {mode.title()}"
            self.energy_chart.update_bars(labels, vals, title, unit="kWh")
        except Exception:
            pass

    def _export_energy_csv(self):
        try:
            mode = self.energy_mode.get()
            key  = self.energy_key.get()
            src  = self.energy_source.get() or "TOTAL"
            path = filedialog.asksaveasfilename(
                defaultextension=".csv",
                filetypes=[("CSV", "*.csv")],
                initialfile=f"{src}_{key}_{mode}_summary.csv",
                title="Export Energy Summary CSV",
            )
            if path:
                self.energy_chart.export_csv(path, source=src, key=key, mode=mode)
        except Exception:
            pass

    # ── Series CSV export ─────────────────────────────────────────────────────

    def _export_series_csv(self, slot: str):
        meter = self.selA_meter if slot == "A" else self.selB_meter
        key   = self.selA_key   if slot == "A" else self.selB_key
        try:
            win       = float(self.window_minutes)
            t_list, y_list = self.trend_manager.get_series(meter, key, window_minutes=win)
            if not t_list:
                messagebox.showinfo("No data", "No trend data in the current window.")
                return
            src  = "TOTAL" if meter == "TOTAL" else f"M{meter}"
            path = filedialog.asksaveasfilename(
                defaultextension=".csv",
                filetypes=[("CSV", "*.csv")],
                initialfile=f"{src}_{key}_{int(win)}m.csv",
                title=f"Export CSV — Signal {slot}",
            )
            if not path:
                return
            unit = _key_unit(key)
            with open(path, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow(["ts_epoch", "ts_local", "value", "unit", "source", "parameter"])
                for ts, val in zip(t_list, y_list):
                    tsf = _safe_float(ts)
                    if tsf is None:
                        continue
                    vf = _safe_float(val)
                    w.writerow([
                        f"{tsf:.3f}",
                        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(tsf)),
                        "" if vf is None else f"{vf:.6f}",
                        unit, src, key,
                    ])
        except Exception as exc:
            messagebox.showerror("Export failed", str(exc))
