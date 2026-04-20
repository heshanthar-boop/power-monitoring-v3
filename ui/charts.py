import os
import math
import bisect
import tkinter as tk
from tkinter import filedialog, ttk
from datetime import datetime, timedelta
from typing import Callable, List, Optional

from ui.key_registry import COMMON_CHART_KEYS, canonical_key, key_label
from utils.paths import logs_dir
from utils.logger import setup_logger
from utils.log_once import log_once

logger = setup_logger("charts")

CHARTS_DISABLED_MESSAGE = "Charts disabled: install matplotlib"

TIME_WINDOWS = [
    (15, "15 min"),
    (30, "30 min"),
    (60, "1 hr"),
    (120, "2 hr"),
    (240, "4 hr"),
    (480, "8 hr"),
    (1440, "24 hr"),
]

# Operator-facing quick buttons (kept small + safe).
# NOTE: these can include values not present in TIME_WINDOWS.
QUICK_WINDOWS = [
    (5, "5m"),
    (15, "15m"),
    (60, "1h"),
    (360, "6h"),
    (1440, "24h"),
]

def _decimate_minmax(x_list: List[float], y_list: List[float], max_points: int) -> tuple[List[float], List[float]]:
    """Downsample a time series while preserving spikes (min/max envelope).

    Returns lists with <= max_points points. Safe on NaNs and short lists.
    """
    n = len(x_list)
    if max_points is None or max_points <= 0 or n <= max_points:
        return x_list, y_list
    if max_points < 10:
        step = max(1, n // max_points)
        return x_list[::step], y_list[::step]

    # We emit ~2 points per bucket (min + max).
    target_buckets = max(1, max_points // 2)
    bucket_size = int(math.ceil(n / target_buckets))
    out_x: List[float] = []
    out_y: List[float] = []

    def _finite_pairs(xs, ys):
        for j, (xx, yy) in enumerate(zip(xs, ys)):
            try:
                fy = float(yy)
            except Exception:
                continue
            if fy == fy:  # not NaN
                yield j, xx, fy

    for i in range(0, n, bucket_size):
        xs = x_list[i : i + bucket_size]
        ys = y_list[i : i + bucket_size]
        fin = list(_finite_pairs(xs, ys))
        if not fin:
            # All NaN/invalid: keep the first point in bucket to keep time monotonic.
            out_x.append(xs[0])
            out_y.append(float('nan'))
            continue
        j_min, x_min, y_min = min(fin, key=lambda t: t[2])
        j_max, x_max, y_max = max(fin, key=lambda t: t[2])
        if j_min <= j_max:
            out_x.extend([x_min, x_max])
            out_y.extend([y_min, y_max])
        else:
            out_x.extend([x_max, x_min])
            out_y.extend([y_max, y_min])

    # Hard cap (envelope can slightly exceed).
    if len(out_x) > max_points:
        step = max(1, len(out_x) // max_points)
        out_x = out_x[::step]
        out_y = out_y[::step]
    return out_x, out_y


# Matplotlib dark theme colors (match ui.styles control-room palette)
CHART_BG = '#0b1220'
CHART_AX = '#0f172a'
CHART_GRID = '#334155'
CHART_TEXT = '#e5e7eb'
CHART_MUTED = '#9ca3af'
CHART_LINE = '#38bdf8'
QUALITY_STALE = '#fbbf24'
QUALITY_OFFLINE = '#f87171'

# --- Unit-aware overlays (Graph+ behavior) ---
# We never let unlike units fight on the same y-axis; it causes autoscale thrash
# and "soggy" UI. Overlays with different units are routed to the right axis.
_UNIT_FIXED_RANGES = {
    # unit_group: (ymin, ymax)
    "PF": (0.0, 1.05),
    "THD": (0.0, 100.0),
}

def _infer_unit_group(label_or_key: str) -> str:
    """Best-effort unit group detection from a key or label.

    Returns a small set of groups so we can decide left-vs-right axis.
    """
    s = str(label_or_key or "").strip()
    if not s:
        return ""
    low = s.lower()
    # Prefer explicit tokens first
    if "power factor" in low or low.endswith("pf") or " pf" in low or "pfavg" in low or "pf" == low:
        return "PF"
    if "frequency" in low or "hz" in low:
        return "Hz"
    if "thd" in low:
        return "THD"
    if "kwh" in low or "kvah" in low or "kvarh" in low:
        return "ENERGY"
    # Power
    if "kw" in low or "kva" in low or "kvar" in low:
        return "POWER"
    # Electrical
    if "(v" in low or " voltage" in low or low.endswith("v") or "vavg" in low:
        return "V"
    if "(a" in low or " current" in low or low.endswith("a") or "iavg" in low:
        return "A"
    if "run hour" in low or "hours" in low:
        return "HOURS"
    return ""

def _ylim_from_data(y_vals, pad_frac: float = 0.06):
    try:
        y_min = min(y_vals)
        y_max = max(y_vals)
    except Exception:
        return None
    if y_min == y_max:
        # give a small window
        span = abs(y_min) if y_min else 1.0
        return (y_min - 0.1 * span, y_max + 0.1 * span)
    span = (y_max - y_min)
    pad = span * float(pad_frac or 0.0)
    return (y_min - pad, y_max + pad)

def apply_chart_theme(fig, ax):
    """Apply dark theme to a matplotlib Figure/Axes (safe no-op if None)."""
    if fig is None or ax is None:
        return
    try:
        fig.patch.set_facecolor(CHART_BG)
        ax.set_facecolor(CHART_AX)
        ax.title.set_color(CHART_TEXT)
        ax.tick_params(colors=CHART_TEXT)
        # axis labels
        ax.xaxis.label.set_color(CHART_TEXT)
        ax.yaxis.label.set_color(CHART_TEXT)
        # spine colors
        for sp in ax.spines.values():
            sp.set_color(CHART_GRID)
        ax.grid(True, linestyle=':', color=CHART_GRID, alpha=0.6)
    except Exception:
        pass


class ParameterPickerPopup(tk.Toplevel):
    """Searchable parameter picker used by Trend charts."""

    def __init__(self, parent, available_keys, on_select, initial="", title="Select Parameter"):
        super().__init__(parent)
        self.transient(parent)
        # Use caller-provided title (e.g. "Add Overlay")
        try:
            self.title(str(title))
        except Exception:
            self.title("Select Parameter")
        self.resizable(False, False)
        self.on_select = on_select
        self.available = sorted(set(available_keys))
        self.filtered = list(self.available)
        self.search_var = tk.StringVar(value=initial)
        self._display_keys: List[str] = []

        ttk.Label(self, text="Search").pack(anchor="w", padx=8, pady=(8, 0))
        entry = ttk.Entry(self, textvariable=self.search_var, width=32)
        entry.pack(fill="x", padx=8, pady=(0, 8))

        self.listbox = tk.Listbox(self, height=10, activestyle="dotbox")
        self.listbox.pack(fill="both", expand=True, padx=8, pady=(0, 8))
        self.listbox.bind("<Double-Button-1>", lambda _: self._choose())
        self.listbox.bind("<Return>", lambda _: self._choose())

        self.search_var.trace_add("write", lambda *_: self._filter())
        entry.bind("<Down>", lambda _: self._focus_list())
        self.bind("<Escape>", lambda _: self.destroy())

        self._refresh_list()
        self.grab_set()
        self.after(0, entry.focus_set)

    def _filter(self):
        query = self.search_var.get().lower()
        self.filtered = [k for k in self.available if query in k.lower() or query in key_label(k).lower()]
        self._refresh_list()

    def _refresh_list(self):
        self.listbox.delete(0, tk.END)
        self._display_keys = []
        for key in self.filtered[:200]:
            display = f"{key} — {key_label(key)}"
            self.listbox.insert(tk.END, display)
            self._display_keys.append(key)
        if self._display_keys:
            self.listbox.selection_set(0)

    def _choose(self):
        sel = self.listbox.curselection()
        if not sel:
            return
        key = self._display_keys[sel[0]]
        self.on_select(key)
        self.destroy()

    def _focus_list(self):
        if self._display_keys:
            self.listbox.focus_set()
            self.listbox.selection_set(0)

try:
    from matplotlib.figure import Figure
    from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg, NavigationToolbar2Tk
    import matplotlib.dates as mdates
    CHARTS_AVAILABLE = True
except ImportError:  # pragma: no cover
    Figure = None
    FigureCanvasTkAgg = None
    NavigationToolbar2Tk = None
    mdates = None
    CHARTS_AVAILABLE = False


if CHARTS_AVAILABLE:
    class TrendChart(ttk.Frame):
        """Basic trend chart using matplotlib."""

        def __init__(self, parent, title="kW Trend"):
            super().__init__(parent)
            self.fig = Figure(figsize=(6, 3), dpi=100)
            self.ax = self.fig.add_subplot(111)
            apply_chart_theme(self.fig, self.ax)
            self.ax.set_title(title)

            # Pre-create a few lines to support overlays without re-allocations.
            self._lines = []
            for i in range(4):
                ln, = self.ax.plot([], [], color=CHART_LINE)
                ln.set_visible(i == 0)
                self._lines.append(ln)
            self._line = self._lines[0]
            self._last_version = -1

            self.canvas = FigureCanvasTkAgg(self.fig, master=self)
            self.canvas.get_tk_widget().pack(fill="both", expand=True)

        def update_from_buffer(self, trend_buffer):
            if trend_buffer is None:
                return
            if trend_buffer.version == self._last_version:
                return
            self._last_version = trend_buffer.version

            x = list(trend_buffer.t)
            y = list(trend_buffer.y)
            if not x:
                self._line.set_data([], [])
                self.canvas.draw_idle()
                return

            self._line.set_data(x, y)
            self.ax.relim()
            self.ax.autoscale_view()
            self.canvas.draw_idle()
else:
    class TrendChart(ttk.Frame):
        """Placeholder when matplotlib is not installed."""

        def __init__(self, parent, title="kW Trend"):
            super().__init__(parent)
            ttk.Label(
                self,
                text=CHARTS_DISABLED_MESSAGE,
                justify="center",
            ).pack(fill="both", expand=True, padx=14, pady=14)


class TrendChartFrame(ttk.Frame):
    """
    Trend chart with:
    - Title showing the selected key + current window
    - Real-time timestamp axis
    - Right-click parameter + window selection
    - Snapshot export + alarm-highlight support
    """

    def __init__(self, parent, title: str = "Trend",
                 get_available_keys_fn: Optional[Callable[[], List[str]]] = None,
                 on_config_change: Optional[Callable[[], None]] = None,
                 max_plot_points: int = 1000,
                 show_alarm_shading: bool = True,
                 alarm_shading_max_intervals: int = 60):
        super().__init__(parent)
        self.get_available_keys_fn = get_available_keys_fn or (lambda: COMMON_CHART_KEYS)
        self.on_config_change = on_config_change
        self.selected_key = "kW"
        # Graph+ overlays (up to 3 extra traces on same axes)
        self.overlay_keys: List[str] = []
        self._legend_on: bool = True
        self._source_label = ""
        self._last_quality = ""
        self.window_minutes = 60
        self.max_plot_points = int(max_plot_points or 1000)
        self._base_max_plot_points = int(self.max_plot_points)
        self.show_alarm_shading = bool(show_alarm_shading)
        try:
            self.alarm_shading_max_intervals = max(0, int(alarm_shading_max_intervals or 0))
        except Exception:
            self.alarm_shading_max_intervals = 60
        self.hold_var = tk.BooleanVar(value=False)
        self.view_offset_minutes = 0  # how far back we are viewing (minutes)
        self._hold_anchor_epoch = None  # epoch seconds when HOLD engaged

        # Alarm shading performance:
        # Redrawing patches is expensive; only redraw when ranges actually change.
        self._alarm_dirty = True
        self._alarm_signature = None
        self._force_redraw = False  # allow redraw even when HOLD is active
        self._nav_hold_forced = False  # HOLD forced by navigation buttons
        self._quick_btns = {}  # minutes->button
        self._last_version = -1
        self._alarm_ranges: List[tuple[float, float, str]] = []
        self._alarm_patches = []
        # Event markers (vertical lines): alarm raise/clear, protection pickup/clear, trip/block.
        # Format: [(ts_epoch, color, linestyle), ...]
        self._markers: List[tuple[float, str, str]] = []
        self._marker_lines = []

        # Threshold lines: horizontal dashed lines at protection pickup levels.
        # Format: [(level, color, label), ...]  — only active (WARNING/ALARM) rules.
        self._threshold_lines_data: List[tuple] = []
        self._threshold_line_artists: List = []
        self._quality_state: Optional[str] = None
        self._quality_text = None
        self._charts_enabled = CHARTS_AVAILABLE and mdates is not None
        # Interaction state (hover + zoom)
        self._dragging = False
        self._drag_start_x = None
        self._drag_start_offset = 0
        self._t_cache = []  # epoch seconds
        self._y_cache = []
        self._latest_epoch = None

        self._xdata = []
        self._ydata = []
        self._hover_annot = None
        self._hover_dot = None
        self._manual_ylim = False

        # Multi-trace hover cache: one entry per active trace (primary + overlays).
        # Stored as matplotlib datenum so _on_motion can bisect directly.
        self._all_series_labels: List[str] = []
        self._all_series_xnum: List[List[float]] = []
        self._all_series_y: List[List[float]] = []
        # Debounced redraw (prevents UI starvation when changing overlays/legend)
        self._redraw_after_id = None
        # Draw throttling (SCADA-grade): coalesce redraws to avoid UI starvation
        self._draw_after_id = None
        self._last_draw_ts = 0.0
        self._min_draw_interval_s = 0.12  # ~8 FPS max
        self._legend_sig = None
        self._legend_state = None
        self._auto_snapshot_job: Optional[str] = None
        self._auto_snapshot_minutes: Optional[int] = None
        self._build()

    def _build(self):
        if not self._charts_enabled:
            self._build_disabled()
            return

        header = ttk.Frame(self, style="ChartHeader.TFrame")
        header.pack(fill="x", padx=2, pady=(2, 0))

        self.param_label = ttk.Label(
            header,
            text=key_label(self.selected_key),
            style="TileTitle.TLabel"
        )
        self.param_label.pack(side="left", padx=4)

        self.window_label = ttk.Label(
            header,
            text=self._window_text(),
            style="TileTitle.TLabel"
        )
        self.window_label.pack(side="left", padx=6)

        # Right-side operator controls (visible, no right-click needed)
        # HOLD: freeze chart updates (data collection continues).
        self.hold_btn = ttk.Checkbutton(
            header,
            text="HOLD",
            variable=self.hold_var,
            command=self._on_hold_toggle,
            style="Hold.TCheckbutton",
        )
        self.hold_btn.pack(side="right", padx=4)

        # Quick time windows (kept compact). 5m/15m/1h/6h/24h
        quick = ttk.Frame(header, style="ChartHeader.TFrame")
        quick.pack(side="right", padx=(0, 6))
        self._quick_btns = {}
        for minutes, label in QUICK_WINDOWS:
            btn = ttk.Button(
                quick,
                text=label,
                width=4,
                style="Interval.TButton",
                command=lambda m=minutes: self.set_window(m),
            )
            btn.pack(side="left", padx=1)
            self._quick_btns[int(minutes)] = btn
        # Stats strip: compact min / avg / max / last for the primary trace.
        # Updated on each redraw; hidden when no data is present.
        stats_row = ttk.Frame(self, style="ChartHeader.TFrame")
        stats_row.pack(fill="x", padx=2, pady=0)
        self._stats_label = ttk.Label(
            stats_row, text="",
            style="TileTitle.TLabel",
            font=("Consolas", 8),
        )
        self._stats_label.pack(side="left", padx=6)
        self._cursor_label = ttk.Label(
            stats_row, text="",
            font=("Consolas", 8),
        )
        self._cursor_label.pack(side="right", padx=6)

        chart_frame = ttk.Frame(self)
        chart_frame.pack(fill="both", expand=True)

        self.fig = Figure(figsize=(6, 3), dpi=100)
        self.fig.subplots_adjust(left=0.08, right=0.97, top=0.92, bottom=0.15)
        self.ax = self.fig.add_subplot(111)
        apply_chart_theme(self.fig, self.ax)
        # Right axis for unlike-unit overlays (Graph+ style). Keep grid only on left.
        try:
            self.ax_r = self.ax.twinx()
            apply_chart_theme(self.fig, self.ax_r)
            try:
                self.ax_r.grid(False)
            except Exception:
                pass
            # Make sure both y-axes are readable on dark theme
            self.ax_r.tick_params(labelsize=8, colors=CHART_TEXT)
        except Exception:
            self.ax_r = None
        # Quality watermark (STALE/OFFLINE). Hidden when GOOD.
        self._quality_text = self.ax.text(
            0.02, 0.95, "",
            transform=self.ax.transAxes,
            ha="left", va="top",
            fontsize=14, fontweight="bold",
            color=QUALITY_STALE,
            alpha=0.22,
        )
        self._quality_text.set_visible(False)
        self.ax.set_title(key_label(self.selected_key), fontsize=11, color=CHART_TEXT)
        self.ax.tick_params(labelsize=8, colors=CHART_TEXT)
        self.ax.xaxis_date()
        self.ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))
        # grid handled by apply_chart_theme
        self._waiting_text = self.ax.text(
            0.5, 0.5, "Waiting for data…",
            transform=self.ax.transAxes,
            ha="center", va="center",
            fontsize=9,
            color=CHART_MUTED,
            alpha=0.7,
        )

        # HOLD overlay (visible when operator freezes chart)
        self._hold_text = self.ax.text(
            0.5, 0.56, "HOLD ACTIVE",
            transform=self.ax.transAxes,
            ha="center", va="center",
            fontsize=9,
            color=CHART_MUTED,
            alpha=0.85,
        )
        self._hold_text.set_visible(False)

        # Pre-create line pools (left + right). We only call set_data()/set_visible()
        # at runtime (no artist churn). This is the key to "no freeze" overlays.
        self._lines_l = []
        self._lines_r = []
        try:
            ln0, = self.ax.plot([], [], linewidth=1.6, color=CHART_LINE)
        except Exception:
            ln0, = self.ax.plot([], [], linewidth=1.6)
        self._lines_l.append(ln0)
        for _ in range(3):
            ln, = self.ax.plot([], [], linewidth=1.2)
            ln.set_visible(False)
            self._lines_l.append(ln)
        self._line = self._lines_l[0]

        if self.ax_r is not None:
            for _ in range(4):
                ln, = self.ax_r.plot([], [], linewidth=1.15)
                ln.set_visible(False)
                self._lines_r.append(ln)

        # Backwards-compat alias for older code paths
        self._lines = self._lines_l

        self.canvas = FigureCanvasTkAgg(self.fig, master=chart_frame)
        canvas_widget = self.canvas.get_tk_widget()
        try:
            canvas_widget.configure(bg=CHART_BG)
        except Exception:
            pass

        # Navigation toolbar (zoom/pan/save). Packed at BOTTOM before canvas so
        # canvas gets remaining space via expand=True.
        self.toolbar = None
        if NavigationToolbar2Tk is not None:
            try:
                self.toolbar = NavigationToolbar2Tk(
                    self.canvas, chart_frame, pack_toolbar=False
                )
                self.toolbar.update()
                # Theme toolbar background to match dark chart area
                try:
                    self.toolbar.configure(background=CHART_BG)
                    for child in self.toolbar.winfo_children():
                        try:
                            child.configure(background=CHART_BG, foreground=CHART_TEXT)
                        except Exception:
                            pass
                except Exception:
                    pass
                self.toolbar.pack(side="bottom", fill="x")
            except Exception:
                self.toolbar = None

        canvas_widget.pack(fill="both", expand=True)
        # legend toggle state (tk var for menu)
        self._legend_var = tk.BooleanVar(value=True)
        canvas_widget.bind("<Button-3>", self._show_context_menu)

        # Interactions: hover value/time readout + wheel zoom
        self._init_hover_artists()
        try:
            self.canvas.mpl_connect("motion_notify_event", self._on_motion)
            self.canvas.mpl_connect("figure_leave_event", self._on_leave)
            self.canvas.mpl_connect("scroll_event", self._on_scroll)
            self.canvas.mpl_connect("button_press_event", self._on_press)
            self.canvas.mpl_connect("button_release_event", self._on_release)
        except Exception:
            pass

    def _init_hover_artists(self):
        """Create hover marker + crosshair + annotation (safe no-op if charts disabled).

        IMPORTANT: Matplotlib artists (Line2D/Annotation) can evaluate False when empty,
        so we MUST test against None, not truthiness.
        """
        if not self._charts_enabled:
            return
        self._hover_last_i = None
        self._hover_dot = None
        self._hover_annot = None
        self._hover_vline = None
        self._hover_hline = None
        try:
            # Crosshair lines — light but clearly visible on dark background
            self._hover_vline = self.ax.axvline(
                x=0.0, color=CHART_TEXT, linewidth=1.0, alpha=0.70,
                linestyle="--", zorder=4,
            )
            self._hover_hline = self.ax.axhline(
                y=0.0, color=CHART_TEXT, linewidth=0.8, alpha=0.45,
                linestyle=":", zorder=4,
            )
            self._hover_vline.set_visible(False)
            self._hover_hline.set_visible(False)

            # A small dot that snaps to the nearest real sample point
            (self._hover_dot,) = self.ax.plot(
                [], [], marker="o", markersize=4, linestyle="None",
                color=CHART_LINE, zorder=5
            )
            self._hover_dot.set_visible(False)

            # Annotation box
            self._hover_annot = self.ax.annotate(
                "",
                xy=(0, 0),
                xytext=(12, 12),
                textcoords="offset points",
                fontsize=8,
                bbox=dict(boxstyle="round,pad=0.25", fc=CHART_AX, ec=CHART_GRID, alpha=0.95),
                color=CHART_TEXT,
            )
            self._hover_annot.set_visible(False)
        except Exception:
            self._hover_dot = None
            self._hover_annot = None
            self._hover_vline = None
            self._hover_hline = None

    def _build_disabled(self):
        ttk.Label(
            self,
            text=CHARTS_DISABLED_MESSAGE,
            justify="center",
        ).pack(fill="both", expand=True, padx=10, pady=10)

    def _window_text(self) -> str:
        label = None
        for minutes, lbl in TIME_WINDOWS:
            if int(minutes) == int(self.window_minutes):
                label = lbl
                break
        base = f"Window: {label}" if label else f"Window: {self.window_minutes} min"
        try:
            off = int(getattr(self, "view_offset_minutes", 0) or 0)
        except Exception:
            off = 0
        if off > 0:
            base += f"  |  View: -{off}m"
        return base

    def _show_context_menu(self, event):
        if not self._charts_enabled:
            return
        menu = tk.Menu(self, tearoff=0)
        menu.add_command(label="Select Parameter…", command=self._choose_parameter)
        menu.add_command(label="Add Overlay…", command=self._add_overlay)
        menu.add_command(label="Clear All Overlays", command=self._clear_overlays)
        # "Remove one overlay" submenu — only shown when overlays exist
        if self.overlay_keys:
            rm_menu = tk.Menu(menu, tearoff=0)
            for ok in self.overlay_keys:
                rm_menu.add_command(
                    label=key_label(ok) or ok,
                    command=lambda k=ok: self._remove_overlay(k),
                )
            menu.add_cascade(label="Remove Overlay ▸", menu=rm_menu)
        menu.add_separator()
        menu.add_command(label="Copy Cursor Value", command=self._copy_cursor_value)
        menu.add_command(label="Export Visible CSV…", command=self._export_visible_csv)
        menu.add_separator()
        menu.add_checkbutton(label="Show Legend", onvalue=True, offvalue=False,
                             variable=self._legend_var, command=self._on_legend_toggle)

        window_menu = tk.Menu(menu, tearoff=0)
        for minutes, label in TIME_WINDOWS:
            window_menu.add_command(
                label=label,
                command=lambda m=minutes: self.set_window(m)
            )
        menu.add_cascade(label="Window", menu=window_menu)
        auto_menu = tk.Menu(menu, tearoff=0)
        for minutes, label in TIME_WINDOWS:
            auto_menu.add_command(
                label=label,
                command=lambda m=minutes: self._start_auto_snapshot(m)
            )
        menu.add_cascade(label="Auto Snapshot", menu=auto_menu)
        menu.add_separator()
        menu.add_command(label="Save Snapshot…", command=self._save_snapshot)
        menu.add_command(label="Reset Zoom", command=self._reset_zoom)
        stop_state = "normal" if self._auto_snapshot_minutes else "disabled"
        menu.add_command(label="Stop Auto Snapshot", command=self._stop_auto_snapshot, state=stop_state)
        menu.tk_popup(event.x_root, event.y_root)

    def _reset_zoom(self):
        """Reset operator zoom/pan and return to live view."""
        self._manual_ylim = False
        self.view_offset_minutes = 0
        self._hold_anchor_epoch = None
        self._nav_hold_forced = False
        # Force one redraw even if version hasn't changed
        self._force_redraw = True
        if not self._charts_enabled:
            return
        try:
            self.ax.relim()
            self.ax.autoscale_view()
        except Exception:
            pass
        try:
            self.canvas.draw_idle()
        except Exception:
            pass

    def _on_hold_toggle(self):
        """Operator HOLD: freeze/unfreeze chart redraws (data collection continues)."""
        if not self._charts_enabled:
            return
        try:
            import time
            on = bool(self.hold_var.get())

            if on:
                # Freeze time reference at the moment HOLD is engaged.
                if self._hold_anchor_epoch is None:
                    self._hold_anchor_epoch = time.time()
                # If operator pressed HOLD manually (not via nav), keep offset at 0.
                if not self._nav_hold_forced:
                    self.view_offset_minutes = 0
            else:
                # Release HOLD -> go live.
                self.view_offset_minutes = 0
                self._hold_anchor_epoch = None
                self._nav_hold_forced = False

            if hasattr(self, "_hold_text") and self._hold_text is not None:
                self._hold_text.set_visible(on)

            if hasattr(self, "hold_btn"):
                self.hold_btn.configure(text="HOLD ✓" if on else "HOLD")

            self._update_header()
            self._mark_force_redraw()
            self._request_draw()
        except Exception:
            pass


    def set_quality(self, quality: str):
        """Show/clear quality overlay on chart area. quality: OK/STALE/OFFLINE."""
        try:
            q = (quality or "").upper().strip()
            if q in ("", "OK"):
                q = ""
            # Gate redraws: only change overlay if state changed
            if q == str(getattr(self, "_last_quality", "")):
                return
            self._last_quality = q
            if hasattr(self, "_quality_text") and self._quality_text is not None:
                if q == "":
                    self._quality_text.set_visible(False)
                else:
                    self._quality_text.set_text(q)
                    self._quality_text.set_visible(True)
            self._request_draw()
        except Exception:
            pass

    def _on_leave(self, _event):
        # Hide hover info when pointer leaves the figure
        try:
            if getattr(self, "_hover_dot", None) is not None:
                self._hover_dot.set_visible(False)
            if getattr(self, "_hover_annot", None) is not None:
                self._hover_annot.set_visible(False)
            if getattr(self, "_hover_vline", None) is not None:
                self._hover_vline.set_visible(False)
            if getattr(self, "_hover_hline", None) is not None:
                self._hover_hline.set_visible(False)
            self._hover_last_i = None
            # Clear cursor readout strip
            try:
                cl = getattr(self, "_cursor_label", None)
                if cl is not None:
                    cl.config(text="")
            except Exception:
                pass
            if self._charts_enabled:
                self._request_draw()
        except Exception:
            pass

    def _cursor_y_left(self, event):
        """Return cursor Y in left-axis units (handles twinx)."""
        if event is None or getattr(event, "ydata", None) is None:
            return None
        try:
            y = float(event.ydata)
        except Exception:
            return None
        if event.inaxes == self.ax:
            return y
        ax_r = getattr(self, "ax_r", None)
        if ax_r is not None and event.inaxes == ax_r:
            try:
                # Map right-axis data -> display -> left-axis data
                _, y_disp = ax_r.transData.transform((0, y))
                _, y_left = self.ax.transData.inverted().transform((0, y_disp))
                return float(y_left)
            except Exception:
                return y
        return y

    def _on_motion(self, event):
        if not self._charts_enabled:
            return
        ax_r = getattr(self, "ax_r", None)
        if event is None or event.inaxes not in (self.ax, ax_r):
            return

        # Drag-pan (X-axis) for investigation. While dragging we suppress hover updates.
        if getattr(self, "_dragging", False):
            self._on_drag(event)
            return
        if not self._xdata or not self._ydata:
            return
        if event.xdata is None:
            return

        # Find nearest point by X distance (datenum axis)
        try:
            x = float(event.xdata)
            idx = bisect.bisect_left(self._xdata, x)
            if idx <= 0:
                i = 0
            elif idx >= len(self._xdata):
                i = len(self._xdata) - 1
            else:
                i = idx if abs(self._xdata[idx] - x) < abs(x - self._xdata[idx - 1]) else (idx - 1)

            # Throttle: only update when snapped sample changes (prevents redraw spam)
            if getattr(self, "_hover_last_i", None) == i:
                return
            self._hover_last_i = i

            px = self._xdata[i]
            py = self._ydata[i]
        except Exception:
            return

        # Skip discontinuity markers (NaN breaks) so hover stays clean.
        try:
            if py != py:  # NaN check
                return
        except Exception:
            return

        try:
            # Crosshair
            if getattr(self, "_hover_vline", None) is not None:
                self._hover_vline.set_xdata([px, px])
                self._hover_vline.set_visible(True)
            if getattr(self, "_hover_hline", None) is not None:
                # If operator wants "cursor Y", use mouse ydata when available,
                # otherwise snap to point.
                cy = self._cursor_y_left(event)
                if cy is None:
                    cy = py
                self._hover_hline.set_ydata([cy, cy])
                self._hover_hline.set_visible(True)

            # Snap dot
            if getattr(self, "_hover_dot", None) is not None:
                self._hover_dot.set_data([px], [py])
                self._hover_dot.set_visible(True)

            # Multi-trace tooltip: show time + value for every active series
            if getattr(self, "_hover_annot", None) is not None:
                dt = mdates.num2date(px)
                ttxt = dt.strftime("%Y-%m-%d %H:%M:%S")
                self._hover_annot.xy = (px, py)

                lines_out = [ttxt]
                cursor_parts: List[str] = []
                sxnums = getattr(self, "_all_series_xnum", [])
                sys_   = getattr(self, "_all_series_y",    [])
                slbls  = getattr(self, "_all_series_labels", [])
                for slbl, sxnum, syy in zip(slbls, sxnums, sys_):
                    if not sxnum or not syy:
                        continue
                    try:
                        si = bisect.bisect_left(sxnum, x)
                        if si >= len(sxnum):
                            si = len(sxnum) - 1
                        elif si > 0 and abs(sxnum[si] - x) > abs(x - sxnum[si - 1]):
                            si -= 1
                        sv = syy[si]
                        if sv == sv:   # not NaN
                            lines_out.append(f"{slbl}: {sv:,.3f}")
                            cursor_parts.append(f"{slbl}={sv:,.3f}")
                    except Exception:
                        pass
                if len(lines_out) == 1:
                    # Fallback: primary value only (before first multi-series update)
                    lines_out.append(f"{key_label(self.selected_key)}: {py:,.3f}")
                    cursor_parts.append(f"{key_label(self.selected_key)}={py:,.3f}")

                self._hover_annot.set_text("\n".join(lines_out))
                self._hover_annot.set_visible(True)

                # Update cursor readout in stats strip (non-intrusive, outside axes)
                try:
                    cl = getattr(self, "_cursor_label", None)
                    if cl is not None:
                        cl.config(text="  " + "  |  ".join(cursor_parts) if cursor_parts else "")
                except Exception:
                    pass

            self._request_draw()
        except Exception:
            pass

    def _toolbar_active(self) -> bool:
        """Return True when the matplotlib toolbar has an active mode (pan/zoom).

        When the toolbar is in pan or zoom mode the user is interacting via the
        toolbar, so we skip our own scroll-zoom and drag-pan handlers to avoid
        fighting with it.
        """
        tb = getattr(self, "toolbar", None)
        return tb is not None and bool(tb.mode)

    def _on_scroll(self, event):
        """Mouse wheel zoom (Y-axis). Skipped when toolbar pan/zoom is active."""
        if not self._charts_enabled:
            return
        if self._toolbar_active():
            return
        ax_r = getattr(self, "ax_r", None)
        if event is None or event.inaxes not in (self.ax, ax_r):
            return

        # Drag-pan (X-axis) for investigation. While dragging we suppress hover updates.
        if getattr(self, "_dragging", False):
            self._on_drag(event)
            return
        if not self._ydata:
            return

        try:
            # Matplotlib: button is 'up' / 'down'
            direction = getattr(event, "button", None)
            if direction == "up":
                factor = 0.85  # zoom in
            elif direction == "down":
                factor = 1.18  # zoom out
            else:
                return

            ymid = self._cursor_y_left(event)
            if ymid is None:
                ymid = float(self._ydata[-1])

            ymin, ymax = self.ax.get_ylim()
            span = max(1e-6, (ymax - ymin) * factor)
            new_ymin = ymid - span / 2.0
            new_ymax = ymid + span / 2.0

            # Guard against inverted limits
            if new_ymax <= new_ymin:
                return

            self.ax.set_ylim(new_ymin, new_ymax)
            self._manual_ylim = True
            self._request_draw()
        except Exception:
            pass


    def _redraw_from_cache(self):
        """Force a redraw from the last cached buffers (safe, no IO)."""
        if not self._charts_enabled:
            return
        if not getattr(self, "_t_cache", None):
            return
        try:
            self._force_redraw = True
            self.update_series(self._t_cache, self._y_cache, int(self._last_version) + 1)
        except Exception:
            pass
        finally:
            self._force_redraw = False

    def _on_press(self, event):
        """Left-click press starts drag-pan. Skipped when toolbar pan/zoom is active."""
        if not self._charts_enabled:
            return
        if self._toolbar_active():
            return
        ax_r = getattr(self, "ax_r", None)
        if event is None or event.inaxes not in (self.ax, ax_r):
            return
        try:
            if getattr(event, "button", None) != 1:
                return
        except Exception:
            return
        if getattr(event, "xdata", None) is None:
            return

        self._dragging = True
        self._drag_start_x = float(event.xdata)
        try:
            self._drag_start_offset = float(getattr(self, "view_offset_minutes", 0) or 0)
        except Exception:
            self._drag_start_offset = 0.0

        # Pan implies investigation mode: engage HOLD to stop the view drifting.
        try:
            if not self.hold_var.get():
                self.hold_var.set(True)
                self._nav_hold_forced = True
        except Exception:
            pass

        # Anchor HOLD at latest known sample time (stable reference).
        try:
            if self._hold_anchor_epoch is None:
                self._hold_anchor_epoch = float(self._latest_epoch) if self._latest_epoch is not None else None
        except Exception:
            pass

    def _on_release(self, event):
        """Release ends drag-pan."""
        self._dragging = False
        self._drag_start_x = None

    def _on_drag(self, event):
        """During drag: update view_offset_minutes and redraw."""
        if not self._charts_enabled:
            return
        if not getattr(self, "_dragging", False):
            return
        ax_r = getattr(self, "ax_r", None)
        if event is None or event.inaxes not in (self.ax, ax_r):
            return
        if getattr(event, "xdata", None) is None or self._drag_start_x is None:
            return

        # Convert matplotlib date numbers delta -> minutes
        try:
            delta_days = float(event.xdata) - float(self._drag_start_x)
        except Exception:
            return
        delta_min = delta_days * 24.0 * 60.0

        new_offset = float(self._drag_start_offset) + float(delta_min)

        # Clamp to available cached data range
        try:
            t = self._t_cache or []
            if t:
                oldest = float(t[0])
                # reference end is HOLD anchor if active, else latest
                ref_end = float(self._hold_anchor_epoch) if self._hold_anchor_epoch is not None else float(t[-1])
                max_offset = ((ref_end - oldest) / 60.0) - float(self.window_minutes)
                if max_offset < 0:
                    max_offset = 0.0
                if new_offset < 0:
                    new_offset = 0.0
                if new_offset > max_offset:
                    new_offset = max_offset
        except Exception:
            # still enforce non-negative
            if new_offset < 0:
                new_offset = 0.0

        try:
            self.view_offset_minutes = new_offset
        except Exception:
            return

        self._redraw_from_cache()

    def _choose_parameter(self):
        available = self.get_available_keys_fn()
        ParameterPickerPopup(
            self,
            available,
            on_select=self._select_key,
            initial=self.selected_key,
        )


    def _add_overlay(self):
        if not self._charts_enabled:
            return
        # Limit overlays to keep UI readable + CPU bounded
        if len(self.overlay_keys) >= 3:
            try:
                log_once("overlay_limit", "Max overlays reached (3).")
            except Exception:
                pass
            return

        keys = []
        try:
            keys = list(self.get_available_keys_fn() or [])
        except Exception:
            keys = COMMON_CHART_KEYS

        # Remove primary + existing overlays
        taken = set([canonical_key(self.selected_key)] + [canonical_key(k) for k in (self.overlay_keys or [])])
        avail = [k for k in keys if canonical_key(k) not in taken]

        def on_pick(k: str):
            if not k:
                return
            kc = canonical_key(k)
            if kc in taken:
                return
            self.overlay_keys.append(kc)
            # Debounced refresh to avoid UI stalls on overlay selection
            self._request_config_refresh()

        ParameterPickerPopup(self, avail, on_pick, title="Add Overlay")

    def _clear_overlays(self):
        if self.overlay_keys:
            self.overlay_keys = []
            self._request_config_refresh()

    def _remove_overlay(self, key: str):
        """Remove a single overlay by key."""
        kc = canonical_key(key)
        removed = False
        if kc and kc in self.overlay_keys:
            self.overlay_keys.remove(kc)
            removed = True
        elif key in self.overlay_keys:
            self.overlay_keys.remove(key)
            removed = True
        if removed:
            self._request_config_refresh()

    def _copy_cursor_value(self):
        """Copy the most-recently hovered value to the system clipboard."""
        try:
            if not self._ydata:
                return
            # Use last valid (non-NaN) value in the hover data
            val = None
            for v in reversed(self._ydata):
                try:
                    fv = float(v)
                    if fv == fv:   # not NaN
                        val = fv
                        break
                except Exception:
                    continue
            if val is None:
                return
            self.clipboard_clear()
            self.clipboard_append(f"{val:.4f}")
        except Exception:
            pass

    def _update_stats_strip(self, y_values, label: str = ""):
        """Recompute min / avg / max / last for *y_values* and display in stats strip."""
        try:
            lbl = getattr(self, "_stats_label", None)
            if lbl is None:
                return
            finite = [v for v in (y_values or []) if isinstance(v, (int, float)) and v == v]
            if not finite:
                lbl.config(text="")
                return
            vmin  = min(finite)
            vmax  = max(finite)
            vavg  = sum(finite) / len(finite)
            vlast = finite[-1]
            unit  = f"  [{label}]" if label else ""
            lbl.config(text=(
                f"  min {vmin:,.3f}  "
                f"avg {vavg:,.3f}  "
                f"max {vmax:,.3f}  "
                f"last {vlast:,.3f}{unit}"
            ))
        except Exception:
            pass

    def _on_legend_toggle(self):
        try:
            self._legend_on = bool(self._legend_var.get())
        except Exception:
            self._legend_on = True
        self._request_config_refresh()

    def _save_snapshot(self):
        if not self._charts_enabled:
            return
        default_name = f"{self.selected_key}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
        path = filedialog.asksaveasfilename(
            defaultextension=".png",
            filetypes=[("PNG image", "*.png")],
            initialfile=default_name
        )
        if path:
            self._export_snapshot(path)

    def _start_auto_snapshot(self, minutes: int):
        if not self._charts_enabled or minutes <= 0:
            return
        self._cancel_auto_snapshot()
        self._auto_snapshot_minutes = minutes
        self._schedule_auto_snapshot()

    def _stop_auto_snapshot(self):
        self._auto_snapshot_minutes = None
        self._cancel_auto_snapshot()

    def _cancel_auto_snapshot(self):
        if self._auto_snapshot_job:
            try:
                self.after_cancel(self._auto_snapshot_job)
            except Exception:
                pass
        self._auto_snapshot_job = None

    def _schedule_auto_snapshot(self):
        if not self._auto_snapshot_minutes:
            return
        delay_ms = int(self._auto_snapshot_minutes * 60 * 1000)
        self._auto_snapshot_job = self.after(delay_ms, self._auto_snapshot_tick)

    def _auto_snapshot_tick(self):
        self._auto_snapshot_job = None
        if not self._auto_snapshot_minutes:
            return
        path = self._auto_snapshot_path()
        self._export_snapshot(path)
        self._schedule_auto_snapshot()

    def _auto_snapshot_path(self) -> str:
        date_folder = datetime.now().strftime("%Y-%m-%d")
        day_dir = os.path.join(logs_dir(), date_folder)
        charts_subdir = os.path.join(day_dir, "charts")
        os.makedirs(charts_subdir, exist_ok=True)
        safe_key = "".join(ch if ch.isalnum() else "_" for ch in self.selected_key)
        window_label = f"{self.window_minutes}m"
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        return os.path.join(charts_subdir, f"{safe_key}_{window_label}_{ts}.png")

    def _export_snapshot(self, path: str):
        try:
            self.fig.savefig(path)
        except Exception:
            log_once(logger, "snapshot_export", "warning", f"Failed to export chart snapshot: {path}")

    def _select_key(self, key: str):
        canon = canonical_key(key)
        if not canon:
            return
        self.selected_key = canon
        self._update_header()
        self._line.set_data([], [])
        self._last_version = -1
        self._mark_force_redraw()
        try:
            self._request_draw()
        except Exception:
            pass
        self._request_config_refresh()

    def _update_header(self):
        base = key_label(self.selected_key)
        src = (self._source_label or "").strip()
        label = f"{src} • {base}" if src else base
        self.param_label.config(text=label)
        if hasattr(self, "window_label") and self.window_label:
            self.window_label.config(text=self._window_text())
        if self._charts_enabled:
            self.ax.set_title(label, fontsize=11)
        self._update_quick_window_styles()
    def set_source(self, source: str):
        """Set source label (e.g., TOTAL (4/6), M3)."""
        try:
            s = str(source or "")
            if s == str(getattr(self, "_source_label", "")):
                return
            self._source_label = s
            self._update_header()
        except Exception:
            pass


    def set_key(self, key: str):
        canon = canonical_key(key)
        if not canon:
            return
        self.selected_key = canon
        if self._charts_enabled:
            self._update_header()

    def set_window(self, minutes: int):
        """Set the visible time window (minutes).

        IoTaWatt/Graph+ behavior:
        - time window also drives decimation density (max_plot_points)
        - window/param changes must redraw immediately (even if version unchanged)
        """
        try:
            m = int(minutes)
        except Exception:
            m = 60
        self.window_minutes = max(1, m)

        # Dynamic decimation budget: more points for short windows, fewer for long.
        # Keep hard-capped to protect low-end control-room PCs.
        try:
            base_cap = int(getattr(self, "_base_max_plot_points", self.max_plot_points) or self.max_plot_points)
            base_cap = max(600, base_cap)  # never too low
            # recommended points based on window length
            recommended = int(max(400, min(1200, self.window_minutes * 40)))
            self.max_plot_points = int(min(recommended, base_cap))
        except Exception:
            pass

        if self._charts_enabled:
            self._update_header()
        self._update_quick_window_styles()
        self._mark_force_redraw()
        self._request_config_refresh()

    
    def _mark_force_redraw(self):
        # Allow one redraw even when HOLD is active
        self._force_redraw = True

    def _request_config_refresh(self, delay_ms: int = 120):
        """Debounced config refresh.

        Overlay/menu actions can fire multiple UI events quickly.
        We debounce to avoid blocking the UI thread with repeated redraw work.
        """
        self._force_redraw = True
        try:
            if self._redraw_after_id:
                self.after_cancel(self._redraw_after_id)
        except Exception:
            pass

        def _do():
            self._redraw_after_id = None
            if self.on_config_change:
                try:
                    self.on_config_change()
                except Exception:
                    pass
            # Ensure a visual response even if version unchanged
            try:
                self._request_draw()
            except Exception:
                pass

        try:
            self._redraw_after_id = self.after(max(0, int(delay_ms)), _do)
        except Exception:
            _do()

    def _request_draw(self):
        """Coalesce matplotlib redraws (prevents UI freeze on weak PCs)."""
        if not self._charts_enabled:
            return
        # If a draw is already scheduled, do nothing
        if getattr(self, "_draw_after_id", None):
            return

        import time as _time
        now = _time.monotonic()
        wait_s = max(0.0, float(getattr(self, "_min_draw_interval_s", 0.12)) - (now - float(getattr(self, "_last_draw_ts", 0.0))))
        wait_ms = int(wait_s * 1000)

        def _do():
            self._draw_after_id = None
            self._last_draw_ts = _time.monotonic()
            try:
                self.canvas.draw_idle()
            except Exception:
                pass

        try:
            self._draw_after_id = self.after(max(0, wait_ms), _do)
        except Exception:
            _do()

    def _update_quick_window_styles(self):
        # Highlight selected time window button
        try:
            for m, btn in (self._quick_btns or {}).items():
                btn.configure(style=("IntervalActive.TButton" if int(m) == int(self.window_minutes) else "Interval.TButton"))
        except Exception:
            pass

    def go_live(self):
        """Jump back to LIVE view (no offset) and release HOLD."""
        self.view_offset_minutes = 0
        self._hold_anchor_epoch = None
        self._nav_hold_forced = False
        try:
            self.hold_var.set(False)
        except Exception:
            pass
        self._update_header()
        self._mark_force_redraw()
    def slide_back(self):
        """View older data. Enables HOLD automatically."""
        step = max(1, int(round(self.window_minutes * 0.25)))
        self.view_offset_minutes = int(self.view_offset_minutes) + step
        # Force HOLD so the window doesn't drift while viewing the past
        if not bool(self.hold_var.get()):
            self._nav_hold_forced = True
            try:
                self.hold_var.set(True)
            except Exception:
                pass
            if self._hold_anchor_epoch is None:
                import time
                self._hold_anchor_epoch = time.time()
        self._update_header()
        self._mark_force_redraw()

    def slide_forward(self):
        """View newer data (toward LIVE)."""
        step = max(1, int(round(self.window_minutes * 0.25)))
        self.view_offset_minutes = max(0, int(self.view_offset_minutes) - step)
        if self.view_offset_minutes == 0 and self._nav_hold_forced:
            # If HOLD was forced only for navigation, return to LIVE automatically
            self.go_live()
            return
        self._update_header()
        self._mark_force_redraw()


    def set_threshold_lines(self, thresholds: Optional[list]) -> None:
        """
        Draw dashed horizontal lines at protection pickup threshold levels.

        ``thresholds`` format: [(level, color, label), ...]
          level : float — threshold in parameter engineering units
          color : str   — '#ef4444' (ALARM) | '#fbbf24' (WARNING)
          label : str   — annotation text, e.g. "Vavg >= 253.0"

        Lines are only drawn when the rule is in WARNING or ALARM state
        (supplied by ProtectionEngine.get_threshold_lines_by_scope()).
        Calling with [] or None removes all existing threshold lines.
        """
        new_data = []
        for t in (thresholds or []):
            try:
                if len(t) >= 3:
                    new_data.append((float(t[0]), str(t[1]), str(t[2])))
            except Exception:
                pass
        if new_data == self._threshold_lines_data:
            return
        self._threshold_lines_data = new_data
        self._draw_threshold_lines()
        if self._charts_enabled:
            self._request_draw()

    def _draw_threshold_lines(self) -> None:
        """Remove stale artists and re-draw threshold lines + text labels."""
        for artist in self._threshold_line_artists:
            try:
                artist.remove()
            except Exception:
                pass
        self._threshold_line_artists = []

        if not self._charts_enabled:
            return

        for level, color, label in self._threshold_lines_data:
            try:
                line = self.ax.axhline(
                    y=level,
                    color=color,
                    linewidth=1.0,
                    linestyle="--",
                    alpha=0.70,
                    zorder=3,
                )
                # Label at the right edge, aligned vertically with the line.
                # ax.get_yaxis_transform(): x in axes [0..1], y in data units.
                txt = self.ax.text(
                    0.985,
                    level,
                    f" {label} ",
                    transform=self.ax.get_yaxis_transform(),
                    ha="right",
                    va="center",
                    fontsize=7,
                    color=color,
                    alpha=0.85,
                    clip_on=True,
                )
                self._threshold_line_artists.extend([line, txt])
            except Exception:
                pass

    def _export_visible_csv(self) -> None:
        """Export the chart's visible window data to a CSV file."""
        if not self._charts_enabled:
            return
        t_data = list(getattr(self, "_t_cache", []) or [])
        y_data = list(getattr(self, "_y_cache", []) or [])
        if not t_data or not y_data:
            try:
                from tkinter import messagebox
                messagebox.showinfo("Export CSV", "No data to export.")
            except Exception:
                pass
            return

        # Filter to the visible X window
        try:
            x0_num, x1_num = self.ax.get_xlim()
            x0_ep = mdates.num2date(x0_num).timestamp()
            x1_ep = mdates.num2date(x1_num).timestamp()
        except Exception:
            x0_ep = t_data[0]
            x1_ep = t_data[-1]

        visible_rows = [
            (t, y) for t, y in zip(t_data, y_data)
            if x0_ep <= t <= x1_ep and y == y  # y == y drops NaN
        ]

        if not visible_rows:
            try:
                from tkinter import messagebox
                messagebox.showinfo("Export CSV", "No data in visible window.")
            except Exception:
                pass
            return

        default_name = (
            f"{self.selected_key}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        )
        path = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV file", "*.csv"), ("All files", "*.*")],
            initialfile=default_name,
        )
        if not path:
            return

        import csv as _csv
        try:
            with open(path, "w", newline="", encoding="utf-8") as f:
                writer = _csv.writer(f)
                writer.writerow(["Timestamp", key_label(self.selected_key)])
                for t_ep, y_val in visible_rows:
                    dt_str = datetime.fromtimestamp(t_ep).strftime("%Y-%m-%d %H:%M:%S")
                    writer.writerow([dt_str, f"{y_val:.6g}"])
            logger.info("[Chart] exported %d rows to %s", len(visible_rows), path)
        except Exception as exc:
            try:
                from tkinter import messagebox
                messagebox.showerror("Export CSV", f"Save failed: {exc}")
            except Exception:
                pass

    def set_alarm_ranges(self, ranges: Optional[List[tuple[float, float, str]]]):
        if not self.show_alarm_shading:
            self._alarm_ranges = []
            self._alarm_dirty = True
            return

        raw = ranges or []

        # Normalize, clamp bad entries, and merge overlaps.
        cleaned: List[tuple[float, float, str]] = []
        for start, end, color in raw:
            try:
                s = float(start)
                e = float(end)
            except Exception:
                continue
            if e <= s:
                continue
            cleaned.append((s, e, str(color or "#fbbf24")))
        cleaned.sort(key=lambda r: (r[0], r[1]))

        merged: List[tuple[float, float, str]] = []
        for s, e, c in cleaned:
            if not merged:
                merged.append((s, e, c))
                continue
            ps, pe, pc = merged[-1]
            # Merge same-color overlaps/adjacent spans (cuts patch count drastically)
            # Treat near-adjacent spans as continuous (prevents "barcode" shading)
            # 2.5s is safe: it merges jittery rule chatter without hiding real incidents.
            if pc == c and s <= pe + 2.5:
                merged[-1] = (ps, max(pe, e), pc)
            else:
                merged.append((s, e, c))

        # Hard cap to prevent "yellow barcode" + lag.
        cap = int(self.alarm_shading_max_intervals or 0)
        if cap > 0 and len(merged) > cap:
            merged = merged[-cap:]

        # Change detection signature (cheap)
        sig = (len(merged), merged[-1][0] if merged else 0.0, merged[-1][1] if merged else 0.0)
        if sig == self._alarm_signature:
            return
        self._alarm_signature = sig
        self._alarm_ranges = merged
        self._alarm_dirty = True

    def set_markers(self, markers: Optional[List[tuple[float, str, str]]]):
        """Set vertical event markers.

        markers format: [(ts_epoch, color, linestyle), ...]
        linestyle examples: '-', '--', ':'
        """
        self._markers = markers or []
        self._draw_markers()

    def _draw_alarm_ranges(self):
        if not self._charts_enabled:
            return
        if not getattr(self, "_alarm_dirty", True):
            return
        for patch in self._alarm_patches:
            patch.remove()
        self._alarm_patches.clear()
        if not self._alarm_ranges:
            self._alarm_dirty = False
            return

        # Only draw spans that intersect the current x-window.
        try:
            x0, x1 = self.ax.get_xlim()
        except Exception:
            x0, x1 = None, None
        for start, end, color in self._alarm_ranges:
            if end <= start:
                continue
            start_num = mdates.date2num(datetime.fromtimestamp(start))
            end_num = mdates.date2num(datetime.fromtimestamp(end))
            if x0 is not None and x1 is not None:
                if end_num < x0 or start_num > x1:
                    continue
            patch = self.ax.axvspan(start_num, end_num, color=color, alpha=0.25, zorder=0)
            self._alarm_patches.append(patch)
        self._alarm_dirty = False
    def _draw_markers(self):
        if not self._charts_enabled:
            return

        # Remove old marker lines
        for ln in self._marker_lines:
            try:
                ln.remove()
            except Exception:
                pass
        self._marker_lines.clear()

        if not self._markers:
            return

        # Limit to keep CPU + clutter low
        markers = self._markers[-40:]
        for ts, color, ls in markers:
            try:
                x = mdates.date2num(datetime.fromtimestamp(float(ts)))
                ln = self.ax.axvline(
                    x,
                    color=color,
                    linestyle=(ls or "-"),
                    linewidth=1.0,
                    alpha=0.65,
                    zorder=2,
                )
                self._marker_lines.append(ln)
            except Exception:
                continue

    def set_quality_state(self, state: Optional[str]):
        """Set chart watermark: STALE / OFFLINE (None/GOOD hides it)."""
        try:
            self._quality_state = state
            if not self._charts_enabled or not self._quality_text:
                return

            if state is None:
                self._quality_text.set_visible(False)
                return

            s = str(state).upper().strip()
            if s in ("GOOD", "OK", ""):
                self._quality_text.set_visible(False)
                return

            if s in ("COMM_LOST", "OFFLINE", "DISABLED"):
                self._quality_text.set_text("OFFLINE")
                self._quality_text.set_color(QUALITY_OFFLINE)
                self._quality_text.set_alpha(0.26)
                self._quality_text.set_visible(True)
                return

            # Default: show as-is (e.g. STALE)
            self._quality_text.set_text(s)
            self._quality_text.set_color(QUALITY_STALE if s == "STALE" else CHART_TEXT)
            self._quality_text.set_alpha(0.22)
            self._quality_text.set_visible(True)
        except Exception:
            return


    def can_draw_now(self) -> bool:
        """True only when this chart is actually visible on screen.

        Used to skip expensive series extraction + redraw work when this frame
        is hidden inside Notebook tabs or not mapped.
        """
        if not getattr(self, "_charts_enabled", True):
            return False
        try:
            # viewable is stricter than ismapped; it respects hidden notebook tabs
            return bool(self.winfo_viewable())
        except Exception:
            try:
                return bool(self.winfo_ismapped())
            except Exception:
                return True

    def _compute_window_bounds(self, end_epoch_hint: Optional[float] = None) -> tuple[float, float]:
        """Return (start_epoch, end_epoch) for the current window.

        Kept identical in spirit to update_series() so overlays behave the same.
        """
        import time
        try:
            off_min = int(getattr(self, "view_offset_minutes", 0) or 0)
        except Exception:
            off_min = 0

        hold_on = bool(getattr(self, "hold_var", None).get()) if hasattr(self, "hold_var") else False
        if hold_on:
            if self._hold_anchor_epoch is None:
                self._hold_anchor_epoch = time.time()
            end_epoch_target = float(self._hold_anchor_epoch) - (off_min * 60.0)
        else:
            end_epoch_target = float(end_epoch_hint) if end_epoch_hint is not None else time.time()
            end_epoch_target = end_epoch_target - (off_min * 60.0)

        end_epoch = float(end_epoch_target)
        start_epoch = end_epoch - (float(self.window_minutes) * 60.0)
        return start_epoch, end_epoch

    def _apply_time_window(self, end_epoch_hint: Optional[float] = None) -> None:
        """Apply current x-limits to both left and right axes."""
        if not self._charts_enabled:
            return
        try:
            start_epoch, end_epoch = self._compute_window_bounds(end_epoch_hint)
            start_num = mdates.date2num(datetime.fromtimestamp(start_epoch))
            end_num = mdates.date2num(datetime.fromtimestamp(end_epoch))
            self.ax.set_xlim(start_num, end_num)
            if getattr(self, "ax_r", None) is not None:
                try:
                    self.ax_r.set_xlim(start_num, end_num)
                except Exception:
                    pass
        except Exception:
            pass

    def update_multi_series(self, series: List[tuple], version: int):
        """Update plot with 1..4 traces.

        series: [(label, t_list, y_list), ...] where label is a short legend label.
        """
        if not self._charts_enabled:
            return

        # Visibility gating (SCADA CPU rule)
        try:
            if not self.can_draw_now():
                return
        except Exception:
            pass

        hold_on = bool(getattr(self, "hold_var", None).get()) if hasattr(self, "hold_var") else False
        if hold_on and not self._force_redraw:
            return
        if (not hold_on) and version == self._last_version and not getattr(self, "_force_redraw", False):
            return

        if not hold_on:
            self._last_version = version

        # No data: keep axes alive
        has_any = False
        for _, t_list, y_list in (series or []):
            if t_list and y_list:
                has_any = True
                break

        if not has_any:
            # Keep stable x-range even before first data arrives
            try:
                self._apply_time_window()
            except Exception:
                pass
            for ln in getattr(self, "_lines", []):
                ln.set_data([], [])
                ln.set_visible(False)
            if hasattr(self, "_waiting_text"):
                self._waiting_text.set_visible(True)
            # keep a sane y-range
            self.ax.set_ylim(0, 1)
            try:
                if self.ax.get_legend() is not None:
                    self.ax.get_legend().remove()
            except Exception:
                pass
            self._request_draw()
            self._force_redraw = False
            return

        if hasattr(self, "_waiting_text"):
            self._waiting_text.set_visible(False)

        # Cache primary series for hover readout
        try:
            if series and series[0][1] and series[0][2]:
                self._t_cache = list(series[0][1] or [])
                self._y_cache = list(series[0][2] or [])
                self._latest_epoch = float(series[0][1][-1]) if series[0][1] else None
        except Exception:
            pass

        # Set live window (respect view_offset if used)
        try:
            self._apply_time_window()
        except Exception:
            pass

        # Plot each series (decimated) into pre-created line pools.
        # IMPORTANT SCADA RULE: never mix unlike units on one y-axis.
        # This prevents autoscale thrash and UI "freeze" on overlay actions.
        primary_group = _infer_unit_group(series[0][0]) if series else ""
        lines_l = getattr(self, "_lines_l", getattr(self, "_lines", []))
        lines_r = getattr(self, "_lines_r", [])
        used_l = 0
        used_r = 0
        y_l_acc = []
        y_r_acc = []
        _hover_traces: list = []  # (label, x_num_list, y_list) — built during loop

        # Disable autoscale; we manage y-limits cheaply.
        try:
            self.ax.set_autoscale_on(False)
        except Exception:
            pass
        if getattr(self, "ax_r", None) is not None:
            try:
                self.ax_r.set_autoscale_on(False)
            except Exception:
                pass

        for idx, (label, t_list, y_list) in enumerate(series or []):
            if not t_list or not y_list:
                continue

            # Decide axis
            g = _infer_unit_group(label)
            use_right = bool(g) and bool(primary_group) and g != primary_group
            ax = self.ax_r if (use_right and getattr(self, "ax_r", None) is not None) else self.ax
            pool = lines_r if (ax is not self.ax and lines_r) else lines_l
            pool_idx = used_r if (ax is not self.ax and lines_r) else used_l
            if pool_idx >= len(pool):
                continue

            ln = pool[pool_idx]

            # Decimate FIRST (epoch floats) => convert only reduced timestamps
            try:
                y = list(y_list)
            except Exception:
                y = []
            t_s = t_list
            if self.max_plot_points and len(t_s) > self.max_plot_points:
                k = max(1, len(t_s) // self.max_plot_points)
                t_s = t_s[::k]
                y = y[::k] if len(y) >= len(t_list) else y

            # Use matplotlib epoch2num for speed (keeps consistent date units).
            x_num = None
            try:
                x_num = mdates.epoch2num([float(ts) for ts in t_s])
                x = x_num
            except Exception:
                try:
                    x = [datetime.fromtimestamp(float(ts)) for ts in t_s]
                except Exception:
                    x = []

            ln.set_data(x, y)
            try:
                ln.set_label(str(label))
            except Exception:
                pass
            ln.set_visible(True)

            # Keep hover caches aligned with the plotted primary series.
            if idx == 0:
                try:
                    if x_num is None:
                        try:
                            x_num = mdates.date2num(x)
                        except Exception:
                            x_num = None
                    if x_num is not None:
                        self._xdata = list(x_num)
                        self._ydata = list(y[:len(x_num)])
                except Exception:
                    pass

            # Accumulate per-trace hover data (all traces, not just primary)
            try:
                if x_num is not None and y:
                    _hover_traces.append((str(label), list(x_num), list(y)))
            except Exception:
                pass

            if ax is self.ax:
                used_l += 1
                try:
                    y_l_acc.extend(y)
                except Exception:
                    pass
            else:
                used_r += 1
                try:
                    y_r_acc.extend(y)
                except Exception:
                    pass

        # Publish multi-trace hover caches + update stats strip
        try:
            self._all_series_labels = [t[0] for t in _hover_traces]
            self._all_series_xnum   = [t[1] for t in _hover_traces]
            self._all_series_y      = [t[2] for t in _hover_traces]
        except Exception:
            pass
        try:
            primary_y = _hover_traces[0][2] if _hover_traces else []
            primary_lbl = _hover_traces[0][0] if _hover_traces else ""
            self._update_stats_strip(
                [v for v in primary_y if isinstance(v, (int, float)) and v == v],
                primary_lbl,
            )
        except Exception:
            pass

        # Hide unused lines (left)
        for j in range(used_l, len(lines_l)):
            try:
                lines_l[j].set_data([], [])
                lines_l[j].set_visible(False)
            except Exception:
                pass

        # Hide unused lines (right)
        for j in range(used_r, len(lines_r)):
            try:
                lines_r[j].set_data([], [])
                lines_r[j].set_visible(False)
            except Exception:
                pass

        # Right-axis visibility and range (fixed for PF/THD)
        if getattr(self, "ax_r", None) is not None:
            try:
                show_r = used_r > 0
                self.ax_r.tick_params(labelleft=False, labelright=show_r)
            except Exception:
                pass

            # Fixed ranges when possible (PF/THD)
            if used_r > 0:
                # pick the first right-series group
                rg = ""
                for (lbl, _, _) in (series or []):
                    g = _infer_unit_group(lbl)
                    if g and primary_group and g != primary_group:
                        rg = g
                        break
                if rg in _UNIT_FIXED_RANGES:
                    y0, y1 = _UNIT_FIXED_RANGES[rg]
                    try:
                        self.ax_r.set_ylim(y0, y1)
                    except Exception:
                        pass
                else:
                    yl = _ylim_from_data(y_r_acc)
                    if yl is not None:
                        try:
                            self.ax_r.set_ylim(yl[0], yl[1])
                        except Exception:
                            pass

        # Left y-range (cheap)
        try:
            if not getattr(self, "_manual_ylim", False):
                yl = _ylim_from_data(y_l_acc)
                if yl is not None:
                    self.ax.set_ylim(yl[0], yl[1])
        except Exception:
            pass

        # Legend: combine left+right handles (cached; building legend is expensive)
        try:
            want_leg = (used_l + used_r) > 1 and bool(getattr(self, "_legend_var", None).get()) and self._legend_on
            sig = None
            if want_leg:
                labels = []
                handles = []
                for ln in list(lines_l) + list(lines_r):
                    try:
                        if ln.get_visible():
                            handles.append(ln)
                            labels.append(str(ln.get_label()))
                    except Exception:
                        pass
                sig = tuple(labels)
                # Only rebuild legend if it actually changed (prevents "soggy" feel)
                if self._legend_state != True or self._legend_sig != sig:
                    self._legend_state = True
                    self._legend_sig = sig
                    if handles:
                        self.ax.legend(handles, labels, loc="upper left", fontsize=8, framealpha=0.25)
            else:
                if self._legend_state != False:
                    self._legend_state = False
                    self._legend_sig = None
                    lg = self.ax.get_legend()
                    if lg is not None:
                        lg.remove()
        except Exception:
            pass

        # IMPORTANT: avoid fig.tight_layout() in live updates (it is expensive and
        # can cause visible freezes on low-end PCs). We already set subplots_adjust.
        self._request_draw()
        self._force_redraw = False




    def update_series(self, t_list: List[float], y_list: List[float], version: int):
        if not self._charts_enabled:
            return

        # If chart is not currently visible (tab not selected), skip drawing to keep CPU low.
        # We do NOT advance _last_version here so it redraws immediately when visible again.
        try:
            if not self.can_draw_now():
                return
        except Exception:
            pass

        hold_on = bool(getattr(self, "hold_var", None).get()) if hasattr(self, "hold_var") else False

        # HOLD: freeze automatic redraws; allow one redraw when operator changes view/window/parameter.
        if hold_on and not self._force_redraw:
            return

        if (not hold_on) and version == self._last_version and not getattr(self, "_force_redraw", False):
            return

        # Auto-hold on hard comm loss: keep last visible trend when fully unplugged.
        # Do NOT advance _last_version here, so when comm recovers we redraw immediately.
        try:
            s = str(getattr(self, "_quality_state", "") or "").upper().strip()
            if (not hold_on) and s in ("COMM_LOST", "OFFLINE", "DISABLED"):
                if hasattr(self, "_line") and len(self._line.get_xdata() or []) > 0:
                    return
        except Exception:
            pass

        # Only advance version when we are LIVE. When HOLD is active we keep _last_version unchanged
        # so that releasing HOLD triggers an immediate redraw.
        if not hold_on:
            self._last_version = version

        # No data yet: show an empty chart with a "Waiting" watermark.
        if not t_list or not y_list:
            now = datetime.now()
            window_delta = timedelta(minutes=self.window_minutes)
            start = now - window_delta
            end = now
            self.ax.set_xlim(start, end)
            self._line.set_data([], [])
            self.ax.set_ylim(0, 1)
            if hasattr(self, "_waiting_text"):
                self._waiting_text.set_visible(True)
            self._request_draw()
            return

        # Cache latest raw buffers for interaction (pan/zoom). Keep as epoch seconds.
        try:
            self._t_cache = list(t_list or [])
            self._y_cache = list(y_list or [])
            self._latest_epoch = float(t_list[-1]) if t_list else None
        except Exception:
            pass


        # Apply view window + navigation offset (in epoch seconds)
        import time
        try:
            off_min = int(getattr(self, "view_offset_minutes", 0) or 0)
        except Exception:
            off_min = 0

        if hold_on:
            if self._hold_anchor_epoch is None:
                self._hold_anchor_epoch = time.time()
            end_epoch_target = float(self._hold_anchor_epoch) - (off_min * 60.0)
        else:
            end_epoch_target = float(t_list[-1]) - (off_min * 60.0)

        # Clamp to available data range
        end_epoch = min(end_epoch_target, float(t_list[-1]))
        start_epoch = end_epoch - (float(self.window_minutes) * 60.0)

        # Filter to window (O(logN) slice using bisect; avoids per-point loops on long histories)
        try:
            i0 = bisect.bisect_left(t_list, start_epoch)
            i1 = bisect.bisect_right(t_list, end_epoch)
            win_t = t_list[i0:i1]
            win_y = y_list[i0:i1]
        except Exception:
            win_t: List[float] = []
            win_y: List[float] = []
            for ts, yy in zip(t_list, y_list):
                if ts < start_epoch or ts > end_epoch:
                    continue
                win_t.append(ts)
                win_y.append(yy)

        # If no points in the requested window, show empty frame but keep the window axis stable.
        if not win_t:
            start_dt = datetime.fromtimestamp(start_epoch)
            end_dt = datetime.fromtimestamp(end_epoch)
            self.ax.set_xlim(start_dt, end_dt)
            self._line.set_data([], [])
            if not self._manual_ylim:
                self.ax.set_ylim(0, 1)
            if hasattr(self, "_waiting_text"):
                self._waiting_text.set_visible(True)
            self._xdata = []
            self._ydata = []
            self._force_redraw = False
            self._request_draw()
            return

        # Convert epoch seconds -> matplotlib date numbers
        dates = [datetime.fromtimestamp(ts) for ts in win_t]
        datenum = mdates.date2num(dates)

        # Break long gaps so matplotlib does NOT draw a straight line across them.
        gap_sec = 15.0
        x_clean2: List[float] = []
        y_clean2: List[float] = []
        prev_ts: Optional[float] = None
        for ts, x, y in zip(win_t, datenum, win_y):
            if prev_ts is not None and (ts - prev_ts) > gap_sec:
                x_clean2.append(x)
                y_clean2.append(float("nan"))
            x_clean2.append(x)
            try:
                y_clean2.append(float(y))
            except Exception:
                y_clean2.append(float("nan"))
            prev_ts = ts

        # Decimate for performance on weak PCs (preserve spikes).
        if self.max_plot_points and len(x_clean2) > self.max_plot_points:
            x_clean2, y_clean2 = _decimate_minmax(list(x_clean2), list(y_clean2), self.max_plot_points)

        # Cache for hover/zoom helpers
        self._xdata = list(x_clean2)
        self._ydata = list(y_clean2)

        # Multi-trace hover cache (single trace here)
        try:
            pk = key_label(self.selected_key) or self.selected_key
            self._all_series_labels = [pk]
            self._all_series_xnum   = [list(self._xdata)]
            self._all_series_y      = [list(self._ydata)]
        except Exception:
            pass

        # Stats strip (min/avg/max/last)
        self._update_stats_strip(
            [v for v in self._ydata if v == v],
            key_label(self.selected_key) or self.selected_key,
        )

        self._line.set_data(self._xdata, self._ydata)

        # Keep a stable rolling window (based on requested end_epoch)
        start_num = mdates.date2num(datetime.fromtimestamp(start_epoch))
        end_num = mdates.date2num(datetime.fromtimestamp(end_epoch))
        self.ax.set_xlim(start_num, end_num)

        # Fast Y management (avoid relim/autoscale_view in live updates).
        # relim() walks artists and gets expensive with overlays/markers.
        if not self._manual_ylim:
            try:
                y_numeric = [v for v in self._ydata if v == v]  # drop NaNs
                yl = _ylim_from_data(y_numeric)
                if yl is not None:
                    self.ax.set_ylim(yl[0], yl[1])
            except Exception:
                pass

        self._force_redraw = False

        self.ax.set_xlabel("Time", fontsize=8)
        # Alarm shading redraw is expensive; only redraw when ranges changed.
        self._draw_alarm_ranges()
        # Threshold lines (pickup levels for active protection rules)
        self._draw_threshold_lines()
        if hasattr(self, "_waiting_text"):
            self._waiting_text.set_visible(False)
        self._request_draw()

# =========================
# PATCH #5 ADDITIONS
# =========================
    def set_bar_mode(self, enabled: bool):
        """Switch chart to bar mode (used for daily kWh)."""
        try:
            self._bar_mode = bool(enabled)
        except Exception:
            self._bar_mode = False
