"""
Dashboard with 16 configurable tiles and 2 trend charts.
Supports double-click/right-click parameter selection and time window control.
"""
from __future__ import annotations
import math
import time
import tkinter as tk
import tkinter.font as tkfont
from tkinter import ttk
from collections import deque
from typing import Dict, List, Optional, Any, Tuple


# NOTE: Parameter selection popup is provided by ui.charts.ParameterPickerPopup (single source of truth).

from ui.charts import TrendChartFrame, CHARTS_AVAILABLE, CHARTS_DISABLED_MESSAGE, ParameterPickerPopup
from core.alarm_engine import AlarmEngine
from core.snapshot_bus import MeterQuality
from ui.key_registry import (
    COMMON_CHART_KEYS,
    COMMON_KEYS,
    DEFAULT_TILE_KEYS,
    key_label,
    key_unit,
    canonical_key,
    canonical_keys,
)
from ui.tooltips import attach_tooltip
from utils.health import meter_is_fresh, stale_seconds
from utils.logger import setup_logger
from utils.log_once import log_once

logger = setup_logger("dashboard")


QUALITY_COLORS = {
    "GOOD": "#2ecc71",
    "STALE": "#f1c40f",
    "COMM_LOST": "#ff4d4d",
    "DISABLED": "#6b7280",
}

# Tile fault-stripe colours (protection engine alarm/warning state).
# Stripe is a 3-px bar at the top of each tile — independent of quality colour.
_FAULT_STRIPE_COLOR = {
    "ALARM":   "#ef4444",   # IEC-62682 red
    "WARNING": "#f59e0b",   # IEC-62682 amber
}
_FAULT_STRIPE_NORMAL = "#0b1220"  # matches control-room dark background → visually hidden

# Tab badge suffixes (appended to meter name in notebook tab)
_TAB_BADGE_NONE  = ""
_TAB_BADGE_WARN  = "  WARN"
_TAB_BADGE_ALARM = "  ALARM"

DASHBOARD_PRESETS = {
    "overview": {
        "label": "Overview",
        "tip": "Balanced operator view for normal monitoring.",
        "tiles": [
            "kW", "kVA", "kVAr", "PFavg", "Frequency", "Vavg", "Iavg", "Import_kWh",
            "Export_kWh", "Net_kWh", "V1N", "V2N", "V3N", "I1", "I2", "I3",
        ],
        "chart1": ["kW", "kVA", "kVAr", "PFavg"],
        "chart2": ["Vavg", "Iavg", "Frequency"],
        "chart1_window": 15,
        "chart2_window": 60,
    },
    "power": {
        "label": "Power",
        "tip": "Active, apparent, reactive power and power factor analysis.",
        "tiles": [
            "kW", "kVA", "kVAr", "PFavg", "kW1", "kW2", "kW3", "kVA1",
            "kVA2", "kVA3", "kVAr1", "kVAr2", "kVAr3", "PF1", "PF2", "PF3",
        ],
        "chart1": ["kW", "kVA", "kVAr"],
        "chart2": ["PFavg", "PF1", "PF2", "PF3"],
        "chart1_window": 15,
        "chart2_window": 60,
    },
    "voltage": {
        "label": "Voltage",
        "tip": "Voltage balance, frequency and voltage distortion.",
        "tiles": [
            "Vavg", "Average Voltage LL", "V1N", "V2N", "V3N", "V12", "V23", "V31",
            "Frequency", "Vavg_ref", "Freq_ref", "THD_V_worst", "THD Voltage V1N",
            "THD Voltage V2N", "THD Voltage V3N", "Harmonic Voltage Worst",
        ],
        "chart1": ["Vavg", "V1N", "V2N", "V3N"],
        "chart2": ["Frequency", "THD_V_worst", "Harmonic Voltage Worst"],
        "chart1_window": 15,
        "chart2_window": 60,
    },
    "current": {
        "label": "Current",
        "tip": "Phase current balance and current distortion.",
        "tiles": [
            "Iavg", "I1", "I2", "I3", "I1_total", "I2_total", "I3_total", "kW",
            "PFavg", "THD_I_worst", "THD Current I1", "THD Current I2", "THD Current I3",
            "Harmonic Current Worst", "RunHour", "Frequency",
        ],
        "chart1": ["Iavg", "I1", "I2", "I3"],
        "chart2": ["THD_I_worst", "THD Current I1", "THD Current I2", "THD Current I3"],
        "chart1_window": 15,
        "chart2_window": 60,
    },
    "quality": {
        "label": "Quality",
        "tip": "Power quality indicators for fast fault diagnosis.",
        "tiles": [
            "PFavg", "Frequency", "Vavg", "Iavg", "THD_V_worst", "THD_I_worst",
            "Harmonic Voltage Worst", "Harmonic Current Worst", "THD Voltage V1N",
            "THD Voltage V2N", "THD Voltage V3N", "THD Current I1", "THD Current I2",
            "THD Current I3", "V1N", "I1",
        ],
        "chart1": ["Vavg", "Frequency", "PFavg"],
        "chart2": ["THD_V_worst", "THD_I_worst", "Harmonic Voltage Worst", "Harmonic Current Worst"],
        "chart1_window": 60,
        "chart2_window": 60,
    },
    "energy": {
        "label": "Energy",
        "tip": "Energy counters and production/consumption review.",
        "tiles": [
            "Import_kWh", "Export_kWh", "Net_kWh", "Today_kWh", "Lifetime_kWh",
            "Total Net kVAh", "Total Net kVArh", "kW", "kVA", "kVAr", "PFavg",
            "RunHour", "Vavg", "Iavg", "Frequency", "THD_V_worst",
        ],
        "chart1": ["Import_kWh", "Export_kWh", "Net_kWh"],
        "chart2": ["kW", "kVA", "kVAr"],
        "chart1_window": 60,
        "chart2_window": 15,
    },
}


class FaultBannerFrame(tk.Frame):
    """
    Compact fault indicator strip rendered between the tiles and charts.

    • Hidden (zero height) when no alarms are active.
    • Shows coloured pill badges, one per active alarm code.
    • ALARM severity → red pills + dark-red strip background.
    • WARN  severity → amber pills + dark-amber strip background.
    • PROT_ prefix is stripped from pill labels for brevity.

    Returns a tab badge suffix string from update_alarms():
        ""       — no active faults
        "  ⚠"   — at least one WARN (no ALARM)
        "  ●"   — at least one ALARM
    """

    _PILL_BG   = {"ALARM": "#dc2626", "WARN": "#d97706"}
    _STRIP_BG  = {"ALARM": "#450a0a", "WARN": "#431407"}
    _STRIP_FG  = "#e2e8f0"
    _FALLBACK_BG = "#0b1220"

    def __init__(self, parent):
        super().__init__(parent, height=0)
        try:
            self.pack_propagate(False)
            self.grid_propagate(False)
        except Exception:
            pass
        self._pills: list = []

    # ------------------------------------------------------------------
    def update_alarms(self, events: list) -> str:
        """Render a single compact active-fault summary. Returns tab badge."""
        for p in self._pills:
            try:
                p.destroy()
            except Exception:
                pass
        self._pills.clear()

        if not events:
            try:
                parent_bg = str(self.master.cget("background"))
            except Exception:
                parent_bg = self._FALLBACK_BG
            self.configure(bg=parent_bg, height=0)
            try:
                self.grid_remove()
            except Exception:
                pass
            return _TAB_BADGE_NONE

        worst = "ALARM" if any(getattr(e, "severity", "") == "ALARM" for e in events) else "WARN"
        strip_bg = self._STRIP_BG.get(worst, self._FALLBACK_BG)
        try:
            self.grid()
        except Exception:
            pass
        self.configure(bg=strip_bg, height=12)

        alarm_count = sum(1 for e in events if getattr(e, "severity", "") == "ALARM")
        warn_count = max(0, len(events) - alarm_count)
        summary = f"FAULTS: {len(events)} active"
        if alarm_count:
            summary += f" | {alarm_count} alarm"
        if warn_count:
            summary += f" | {warn_count} warning"
        lbl = tk.Label(
            self, text=f"  {summary}",
            fg=self._STRIP_FG, bg=strip_bg,
            font=("Segoe UI", 7, "bold"),
        )
        lbl.pack(side="left", padx=(4, 5), pady=0)
        self._pills.append(lbl)

        sorted_evs = sorted(
            events,
            key=lambda e: (0 if getattr(e, "severity", "") == "ALARM" else 1,
                           getattr(e, "code", "")),
        )
        max_codes = 4
        for ev in sorted_evs[:max_codes]:
            pill_bg = self._PILL_BG.get(getattr(ev, "severity", "WARN"), "#6b7280")
            short = str(getattr(ev, "code", "?")).replace("PROT_", "").strip()
            if len(short) > 18:
                short = short[:15].rstrip() + "..."
            pill = tk.Label(
                self, text=f" {short} ",
                fg="white", bg=pill_bg,
                font=("Consolas", 7, "bold"),
            )
            pill.pack(side="left", padx=1, pady=0)
            self._pills.append(pill)
        extra = len(sorted_evs) - max_codes
        if extra > 0:
            more = tk.Label(
                self, text=f" +{extra} ",
                fg=self._STRIP_FG, bg=strip_bg,
                font=("Consolas", 7, "bold"),
            )
            more.pack(side="left", padx=1, pady=0)
            self._pills.append(more)

        return _TAB_BADGE_ALARM if worst == "ALARM" else _TAB_BADGE_WARN


class TrendManager:
    """
    Manages trend buffers for multiple parameters per meter.

    SCADA-grade rule:
      - Never "draw lies" across invalid data.
      - When a signal transitions OK <-> invalid, we insert a NaN marker to
        break the line (prevents the classic straight-line artifact).

    Implementation:
      - ring buffer per (meter_id, key)
      - validity transition markers (NaN) are inserted sparingly
    """

    def __init__(self, maxlen: int = 7200):  # 2 hours at 1 sample/sec
        self.maxlen = maxlen
        # {meter_id: {key: SimpleTrendBuffer}}
        self._buffers: Dict[Any, Dict[str, 'SimpleTrendBuffer']] = {}

    def get_or_create_buffer(self, meter_id: Any, key: str) -> 'SimpleTrendBuffer':
        if meter_id not in self._buffers:
            self._buffers[meter_id] = {}
        if key not in self._buffers[meter_id]:
            self._buffers[meter_id][key] = SimpleTrendBuffer(self.maxlen)
        return self._buffers[meter_id][key]

    def push(self, meter_id: Any, key: str, ts: float, value: float):
        """Backwards-compatible push (assumes valid)."""
        self.push_sample(meter_id, key, ts, value=value, valid=True)

    def push_sample(self, meter_id: Any, key: str, ts: float, *, value: Optional[float], valid: bool):
        """Push a new sample with explicit validity.

        If valid is False, we do NOT push the numeric value. We only insert a
        single NaN marker on transitions to break the line.
        """
        buf = self.get_or_create_buffer(meter_id, key)
        buf.push(ts, value=value, valid=bool(valid))

    def get_series(self, meter_id: Any, key: str, window_minutes: int = 60) -> Tuple[List[float], List[float]]:
        if meter_id not in self._buffers or key not in self._buffers[meter_id]:
            return [], []

        buf = self._buffers[meter_id][key]
        now = time.time()
        cutoff = now - (window_minutes * 60)

        t_list: List[float] = []
        y_list: List[float] = []
        for ts, val in zip(buf.t, buf.y):
            if ts >= cutoff:
                t_list.append(ts)
                y_list.append(val)

        return t_list, y_list

    def get_version(self, meter_id: Any, key: str) -> int:
        if meter_id not in self._buffers or key not in self._buffers[meter_id]:
            return 0
        return self._buffers[meter_id][key].version

    def set_series(self, meter_id: Any, key: str,
                   t_list: List[float], y_list: List[float]) -> None:
        """Pre-seed a trend buffer from historian data.

        Only seeds when the buffer has fewer points than the historian series
        (i.e. the ring buffer is thin after startup).  Does nothing if live
        data has already filled the buffer past the historian snapshot.

        A NaN gap marker is inserted between the historian tail and any
        existing live samples so the chart does not draw a straight line
        across the seam.
        """
        if not t_list or not y_list:
            return
        buf = self.get_or_create_buffer(meter_id, key)
        # Don't overwrite a buffer that is already well-populated with live data.
        if len(buf.t) >= len(t_list):
            return

        # Build a new buffer with historian data first, then re-add existing live points.
        live_t = list(buf.t)
        live_y = list(buf.y)

        # Replace buffer contents: clear and re-fill from historian
        buf.t.clear()
        buf.y.clear()
        buf._version += 1
        buf._last_valid = None
        buf._last_ts = None

        for ts, val in zip(t_list, y_list):
            buf.push(ts, value=val, valid=True)

        # Re-append existing live points (if any, and newer than historian tail)
        if live_t and live_t[-1] > t_list[-1]:
            # Insert gap-break marker at the historian/live seam
            buf._push_raw(t_list[-1] + 0.001, float("nan"))
            for ts, val in zip(live_t, live_y):
                if ts > t_list[-1]:
                    import math
                    try:
                        valid = math.isfinite(float(val))
                    except Exception:
                        valid = False
                    buf.push(ts, value=float(val) if valid else None, valid=valid)


class SimpleTrendBuffer:
    """Simple ring buffer for trend data + validity transition markers."""

    def __init__(self, maxlen: int = 7200):
        self.t = deque(maxlen=maxlen)
        self.y = deque(maxlen=maxlen)
        self._version = 0
        self._last_valid: Optional[bool] = None
        # Used to break the drawn line across missing samples.
        # If we resume after a long gap (e.g., Dashboard tab not visible),
        # we inject a NaN marker before the next valid point so matplotlib
        # will not connect a straight line across the gap.
        self._last_ts: Optional[float] = None
        # IMPORTANT: don't set this too low.
        # TOTAL (and sometimes meters) can be sampled at slightly irregular cadence
        # depending on UI load, polling jitter, or Windows scheduler.
        # If too small, we will inject NaN too often and you'll see "dots".
        self._gap_break_sec: float = 15.0  # break only on real gaps (tab switch / comm loss)

    def _marker_ts(self, ts: float) -> float:
        """Return a timestamp suitable for inserting a NaN marker.

        Matplotlib draws vertical lines if the x-value is identical for two consecutive
        points (NaN marker then the real value). So we offset markers slightly.
        """
        try:
            last = self._last_ts
            if last is None:
                return float(ts)
            # Prefer a marker just AFTER the last real timestamp, but still <= ts.
            # If ts is very close, clamp to last + epsilon.
            eps = 1e-3
            candidate = max(float(last) + eps, float(ts) - eps)
            # Never go backwards.
            return max(candidate, float(last) + eps)
        except Exception:
            return float(ts)

    def _push_raw(self, ts: float, val: float):
        self.t.append(ts)
        self.y.append(val)
        self._version += 1
        self._last_ts = ts

    def push(self, ts: float, *, value: Optional[float], valid: bool):
        # First sample: if invalid, do nothing (no line yet). If valid, push it.
        if self._last_valid is None:
            self._last_valid = bool(valid)
            if valid and value is not None:
                self._push_raw(ts, float(value))
            return

        valid = bool(valid)

        # Gap handling: if we resumed after a long silence and we are still in
        # a valid state, break the line so the chart doesn't draw a lie.
        if (
            valid
            and self._last_valid is True
            and self._last_ts is not None
            and (ts - float(self._last_ts)) > float(self._gap_break_sec)
        ):
            self._push_raw(self._marker_ts(ts), float("nan"))

        # Transition handling: insert a NaN marker exactly once to break the line.
        if self._last_valid != valid:
            self._push_raw(self._marker_ts(ts), float("nan"))
            self._last_valid = valid

        if valid and value is not None:
            try:
                self._push_raw(ts, float(value))
            except Exception:
                # Never crash UI for one bad value
                self._push_raw(ts, float("nan"))

    @property
    def version(self) -> int:
        return self._version


class _SparklineBuffer:
    """
    Ring buffer holding the last N numeric samples for a single tile sparkline.

    Values are stored as floats.  math.nan is used as a sentinel for missing /
    invalid readings so the polyline renderer can break the line at gaps rather
    than drawing a misleading straight segment across the discontinuity.

    Only finite values are used to compute the min/max scaling range.
    """

    __slots__ = ("_buf", "_capacity", "_pos", "_count")

    def __init__(self, capacity: int = 20) -> None:
        import math
        self._capacity = max(4, int(capacity))
        self._buf = [math.nan] * self._capacity
        self._pos = 0
        self._count = 0

    def push(self, value) -> None:
        import math
        try:
            v = float(value)
            if not math.isfinite(v):
                v = math.nan
        except (TypeError, ValueError):
            v = math.nan
        self._buf[self._pos] = v
        self._pos = (self._pos + 1) % self._capacity
        self._count = min(self._count + 1, self._capacity)

    def push_invalid(self) -> None:
        import math
        self.push(math.nan)

    def values(self, n: int = 10):
        """Return the last *n* values in chronological order (oldest first)."""
        n = min(n, self._capacity, self._count)
        if n <= 0:
            return []
        start = (self._pos - n) % self._capacity
        result = []
        for i in range(n):
            result.append(self._buf[(start + i) % self._capacity])
        return result

    def has_data(self) -> bool:
        import math
        return any(math.isfinite(v) for v in self._buf[:self._count])


def _draw_sparkline(canvas, values, color: str, w: int, h: int) -> None:
    """
    Render a sparkline polyline onto *canvas*.

    • Scales the finite values to fill [2, h-2] vertically.
    • Draws connected line segments; gaps (nan) break the line.
    • Clears the canvas first on every call.

    Parameters
    ----------
    canvas : tk.Canvas
    values : list[float]   — chronological, may contain nan
    color  : str           — line colour (hex)
    w, h   : int           — canvas pixel dimensions
    """
    import math
    canvas.delete("all")
    if not values:
        return

    finite = [v for v in values if math.isfinite(v)]
    if not finite:
        return

    vmin = min(finite)
    vmax = max(finite)
    span = vmax - vmin
    # Flat-line guard: if all values identical, draw a centred horizontal line
    if span < 1e-9:
        y_mid = h // 2
        canvas.create_line(2, y_mid, w - 2, y_mid, fill=color, width=1, smooth=False)
        return

    n = len(values)
    x_step = (w - 4) / max(n - 1, 1)
    pad_top = 2
    pad_bot = 2
    y_range = h - pad_top - pad_bot

    # Build segments: collect consecutive finite points, draw each run
    seg: list = []
    for i, v in enumerate(values):
        x = 2 + i * x_step
        if math.isfinite(v):
            y = pad_top + y_range * (1.0 - (v - vmin) / span)
            seg.append((x, y))
        else:
            if len(seg) >= 2:
                canvas.create_line(seg, fill=color, width=1, smooth=True)
            seg = []
    if len(seg) >= 2:
        canvas.create_line(seg, fill=color, width=1, smooth=True)
    elif len(seg) == 1:
        # Single point — draw a tiny dot
        x0, y0 = seg[0]
        canvas.create_oval(x0 - 1, y0 - 1, x0 + 1, y0 + 1, fill=color, outline="")


class TilesPanel(ttk.Frame):
    """
    Panel with 16 configurable tiles (2 rows x 8 columns).
    Each tile can be right-clicked to select parameter.
    """
    MAX_TILES = 16
    
    def __init__(self, parent, cfg: dict, view_key: str, get_available_keys_fn, on_config_change=None):
        super().__init__(parent)
        self.cfg = cfg
        self.view_key = view_key  # "TOTAL" or meter_id
        self.get_available_keys_fn = get_available_keys_fn
        self.on_config_change = on_config_change
        self._tile_count = self.MAX_TILES
        self._display_style = "tiles"
        self._raw_numbers = False
        
        self.tiles: List[Dict] = []  # List of tile info dicts
        # Per-tile sparkline ring buffers — keyed by tile index.
        # Populated lazily in update() so key changes don't require a rebuild.
        self._spark_bufs: Dict[int, _SparklineBuffer] = {}
        self._build()
        self._load_config()

    @staticmethod
    def _equivalent_key(key: str) -> str:
        """Collapse aliases that are the same operator-facing parameter."""
        canon = canonical_key(key)
        return {
            "PF": "PFavg",
            "PF_total": "PFavg",
            "Total_kW": "kW",
            "Total_kVA": "kVA",
            "Total_kVAr": "kVAr",
            "I1_total": "I1",
            "I2_total": "I2",
            "I3_total": "I3",
            "RunHour_total": "RunHour",
        }.get(canon, canon)

    @classmethod
    def _tile_identity(cls, key: str) -> Tuple[str, str, str]:
        canon = canonical_key(key)
        label = " ".join(str(key_label(canon) or canon).lower().split())
        unit = str(key_unit(canon) or "").lower().strip()
        return (cls._equivalent_key(canon), label, unit)

    @staticmethod
    def _title_text(key: str) -> str:
        label = key_label(key)
        if len(label) <= 24:
            return label
        return label[:21].rstrip() + "..."

    def _unique_tile_keys(self, preferred: List[str]) -> List[str]:
        """Return 16 unique, useful tile keys, preserving saved order first."""
        try:
            available = list(self.get_available_keys_fn() or [])
        except Exception:
            available = []
        if self.view_key == "TOTAL":
            candidates = list(preferred or []) + available + DEFAULT_TILE_KEYS + COMMON_KEYS
        else:
            candidates = list(preferred or []) + DEFAULT_TILE_KEYS + available + COMMON_KEYS

        out: List[str] = []
        seen = set()
        for raw in candidates:
            canon = canonical_key(raw)
            if not canon:
                continue
            ident = self._tile_identity(canon)
            if ident in seen:
                continue
            seen.add(ident)
            out.append(canon)
            if len(out) >= self.MAX_TILES:
                break
        return out[:self.MAX_TILES]
    
    def _get_tile_keys(self) -> List[str]:
        """Get de-duplicated tile keys from config or use defaults."""
        dash_cfg = self.cfg.setdefault("dashboard", {})
        tiles_cfg = dash_cfg.setdefault("tiles", {})
        key = str(self.view_key)
        saved = tiles_cfg.get(key)
        normalized = canonical_keys(saved or [])
        base = normalized if normalized else canonical_keys(DEFAULT_TILE_KEYS)
        unique = self._unique_tile_keys(base)
        tiles_cfg[key] = unique
        return unique
    
    def _save_tile_keys(self, keys: List[str]):
        """Save tile keys to config."""
        dash_cfg = self.cfg.setdefault("dashboard", {})
        tiles_cfg = dash_cfg.setdefault("tiles", {})
        tiles_cfg[str(self.view_key)] = self._unique_tile_keys(canonical_keys(keys))
        self._notify_config_change()

    def _notify_config_change(self) -> None:
        if callable(self.on_config_change):
            try:
                self.on_config_change()
            except Exception:
                pass

    def apply_keys(self, keys: List[str]) -> None:
        """Apply a full tile layout and save it."""
        selected = self._unique_tile_keys(canonical_keys(keys or []))
        for i, tile in enumerate(self.tiles):
            if i < len(selected):
                self._apply_tile_key(tile, selected[i])
                self._clear_sparkline(i)
        self._save_tile_keys([t["key"] for t in self.tiles])

    def _apply_tile_key(self, tile: Dict, key: str) -> None:
        canon = canonical_key(key)
        tile["key"] = canon
        tile["title_label"].config(text=self._title_text(canon))
        try:
            tile["unit_label"].config(text=key_unit(canon))
        except Exception:
            pass

    def _clear_sparkline(self, tile_index: int) -> None:
        self._spark_bufs.pop(tile_index, None)
        try:
            self.tiles[tile_index]["sparkline"].delete("all")
        except Exception:
            pass

    def set_display_options(self, tile_count: int = 16, style: str = "tiles", raw_numbers: bool = False) -> None:
        try:
            count = int(tile_count)
        except Exception:
            count = self.MAX_TILES
        self._tile_count = max(1, min(self.MAX_TILES, count))
        self._display_style = str(style or "tiles").strip().lower()
        if self._display_style not in {"tiles", "gauges"}:
            self._display_style = "tiles"
        self._raw_numbers = bool(raw_numbers)
        self._arrange_tiles()
        self._apply_style_visibility()

    def _arrange_tiles(self) -> None:
        visible = max(1, min(self.MAX_TILES, int(self._tile_count or self.MAX_TILES)))
        if visible <= 8:
            cols = 8
        elif visible <= 12:
            cols = 6
        else:
            cols = 8
        rows = max(1, int(math.ceil(visible / float(cols))))

        for col in range(8):
            self.columnconfigure(col, weight=1 if col < cols else 0)
        for row in range(2):
            self.rowconfigure(row, weight=1 if row < rows else 0)

        for idx, tile in enumerate(self.tiles):
            frame = tile.get("frame")
            if frame is None:
                continue
            if idx >= visible:
                try:
                    frame.grid_remove()
                except Exception:
                    pass
                continue
            row = idx // cols
            col = idx % cols
            try:
                frame.grid(row=row, column=col, padx=3, pady=3, sticky="nsew")
            except Exception:
                pass

    def _apply_style_visibility(self) -> None:
        gauge_mode = self._display_style == "gauges"
        for idx, tile in enumerate(self.tiles):
            if idx >= self._tile_count:
                continue
            gauge = tile.get("gauge_canvas")
            value_lbl = tile.get("value_label")
            unit_lbl = tile.get("unit_label")
            spark = tile.get("sparkline")
            try:
                if gauge_mode:
                    if value_lbl is not None:
                        value_lbl.pack_forget()
                    if unit_lbl is not None:
                        unit_lbl.pack_forget()
                    if spark is not None:
                        spark.pack_forget()
                    if gauge is not None:
                        gauge.pack(fill="both", expand=True, padx=4, pady=(0, 4))
                else:
                    if gauge is not None:
                        gauge.pack_forget()
                    if value_lbl is not None:
                        value_lbl.pack(padx=4, pady=(2, 0))
                    if unit_lbl is not None:
                        unit_lbl.pack(padx=4, pady=(0, 2))
                    if spark is not None:
                        spark.pack(fill="x", padx=4, pady=(0, 4))
            except Exception:
                pass

    @staticmethod
    def _format_raw_value(value) -> str:
        try:
            return f"{float(value):,.4f}"
        except Exception:
            return str(value)

    def _draw_gauge(self, tile: Dict, key: str, value, color: str) -> None:
        canvas = tile.get("gauge_canvas")
        if canvas is None:
            return
        try:
            canvas.delete("all")
            w = max(88, int(canvas.winfo_width() or 0))
            h = max(88, int(canvas.winfo_height() or 0))
            size = min(w - 8, h - 8)
            left = (w - size) / 2.0
            top = (h - size) / 2.0
            right = left + size
            bottom = top + size
            canvas.create_oval(left, top, right, bottom, outline="#1f2937", width=6)
            value_f = float(value) if isinstance(value, (int, float)) else None
            frac = 0.0
            if value_f is not None:
                grp = _infer_unit_group(key_label(key) or key)
                if grp == "PF":
                    frac = max(0.0, min(1.0, value_f / 1.0))
                elif grp == "Hz":
                    frac = max(0.0, min(1.0, (value_f - 45.0) / 10.0))
                elif grp == "THD":
                    frac = max(0.0, min(1.0, value_f / 20.0))
                else:
                    buf = self._spark_bufs.get(tile.get("index"))
                    samples = []
                    try:
                        samples = [abs(float(v)) for v in (buf.values(20) if buf else []) if isinstance(v, (int, float)) and float(v) == float(v)]
                    except Exception:
                        samples = []
                    peak = max(samples) if samples else max(abs(value_f), 1.0)
                    frac = max(0.0, min(1.0, abs(value_f) / max(peak, 1e-9)))
            canvas.create_arc(
                left, top, right, bottom,
                start=220, extent=-260,
                style="arc", outline=color, width=7,
            )
            canvas.create_arc(
                left, top, right, bottom,
                start=220, extent=-(260 * frac),
                style="arc", outline="#38bdf8" if color == "#4da6ff" else color, width=7,
            )
            if value_f is None:
                text = "--"
            elif self._raw_numbers:
                text = self._format_raw_value(value_f)
            else:
                text = self._format_compact_value(value_f)
            canvas.create_text(w / 2.0, h / 2.0 - 6, text=text, fill="#e5e7eb", font=("Segoe UI", 11, "bold"))
            unit = key_unit(key)
            if unit:
                canvas.create_text(w / 2.0, h / 2.0 + 14, text=unit, fill="#9ca3af", font=("Segoe UI", 8))
        except Exception:
            pass

    @staticmethod
    def _format_compact_value(fv: float) -> str:
        if abs(fv) >= 10000:
            return f"{fv:,.0f}"
        if abs(fv) >= 1000:
            return f"{fv:,.1f}"
        if abs(fv) >= 100:
            return f"{fv:.1f}"
        if abs(fv) >= 10:
            return f"{fv:.2f}"
        return f"{fv:.3f}"

    @staticmethod
    def _value_font_size(text: str) -> int:
        t = str(text or "").strip()
        n = len(t)
        if n <= 7:
            return 22
        if n <= 10:
            return 18
        if n <= 13:
            return 16
        return 14

    def _fit_value_label(self, label_widget, text: str) -> None:
        size = self._value_font_size(text)
        try:
            if int(getattr(label_widget, "_font_size", 0) or 0) == size:
                return
            current = label_widget.cget("font")
            try:
                base = tkfont.nametofont(current).actual()
                family = str(base.get("family") or "Segoe UI")
                weight = str(base.get("weight") or "bold")
            except Exception:
                family = "Segoe UI"
                weight = "bold"
            label_widget.configure(font=(family, size, weight))
            label_widget._font_size = size
        except Exception:
            pass
    
    def _load_config(self):
        """Load tile configuration."""
        saved_keys = self._get_tile_keys()
        for i, tile in enumerate(self.tiles):
            if i < len(saved_keys):
                self._apply_tile_key(tile, saved_keys[i])
    
    def _build(self):
        """Build 16 tiles in 2 rows x 8 columns."""
        # Configure grid weights
        for col in range(8):
            self.columnconfigure(col, weight=1)
        for row in range(2):
            self.rowconfigure(row, weight=1)

        try:
            t = __import__("ui.styles", fromlist=["get_theme"]).get_theme()
            _card_bg = t.card
        except Exception:
            _card_bg = "#242426"

        for i in range(16):
            row = i // 8
            col = i % 8
            key_name = DEFAULT_TILE_KEYS[i] if i < len(DEFAULT_TILE_KEYS) else "kW"
            key_name = canonical_key(key_name)

            # Outer frame — zero internal padding so fault stripe touches the border
            tile_frame = ttk.LabelFrame(self, style="Tile.TLabelframe", padding=0)
            tile_frame.grid(row=row, column=col, padx=3, pady=3, sticky="nsew")

            # ── Fault indicator stripe (top, 3 px) ───────────────────────────────
            fault_stripe = tk.Frame(tile_frame, height=3, bg=_FAULT_STRIPE_NORMAL)
            fault_stripe.pack(fill="x", side="top")

            # ── Header row: title left, quality dot right ─────────────────────────
            hdr = tk.Frame(tile_frame, bg=_card_bg)
            hdr.pack(fill="x", padx=4, pady=(2, 0))

            title_lbl = ttk.Label(hdr, text=self._title_text(key_name), style="TileTitle.TLabel")
            title_lbl.pack(side="left", anchor="w")

            # Quality dot — tiny colored canvas circle, top-right
            dot_canvas = tk.Canvas(
                hdr, width=10, height=10,
                bg=_card_bg, highlightthickness=0,
            )
            dot_canvas.pack(side="right", anchor="e", padx=(2, 0))
            dot_id = dot_canvas.create_oval(1, 1, 9, 9, fill="#5e5a56", outline="")

            # ── Value (large) ─────────────────────────────────────────────────────
            value_lbl = ttk.Label(tile_frame, text="--", style="TileValue.TLabel")
            value_lbl.pack(padx=4, pady=(2, 0))

            # ── Unit label (below value) ──────────────────────────────────────────
            unit_lbl = ttk.Label(
                tile_frame,
                text=key_unit(key_name),
                style="TileUnit.TLabel",
            )
            unit_lbl.pack(padx=4, pady=(0, 2))

            # ── Sparkline canvas (10-point mini-trend) ────────────────────────────
            # 56 × 18 px — no axes, no labels, just a polyline.
            # Rendered by _draw_sparkline() on every update() call.
            spark_canvas = tk.Canvas(
                tile_frame,
                width=64, height=18,
                bg=_card_bg,
                highlightthickness=0,
                bd=0,
            )
            spark_canvas.pack(fill="x", padx=4, pady=(0, 4))

            gauge_canvas = tk.Canvas(
                tile_frame,
                width=96, height=92,
                bg=_card_bg,
                highlightthickness=0,
                bd=0,
            )
            gauge_canvas.pack_forget()

            tile_info = {
                "index": i,
                "key": key_name,
                "frame": tile_frame,
                "title_label": title_lbl,
                "value_label": value_lbl,
                "unit_label": unit_lbl,
                "fault_stripe": fault_stripe,
                "dot_canvas": dot_canvas,
                "dot_id": dot_id,
                "sparkline": spark_canvas,
                "gauge_canvas": gauge_canvas,
                "_card_bg": _card_bg,
            }
            self.tiles.append(tile_info)

            # Let operators discover parameter changes without reading documentation.
            _hint = "Double-click or right-click to change this tile parameter."
            for widget in [tile_frame, hdr, title_lbl, value_lbl, unit_lbl]:
                widget.bind("<Button-3>", lambda e, idx=i: self._show_context_menu(e, idx))
                widget.bind("<Double-Button-1>", lambda e, idx=i: self._show_context_menu(e, idx))
                attach_tooltip(widget, _hint)
                try:
                    widget.bind("<Enter>", lambda e, w=tile_frame: w.configure(cursor="hand2"))
                    widget.bind("<Leave>", lambda e, w=tile_frame: w.configure(cursor=""))
                except Exception:
                    pass
    
    def _show_context_menu(self, event, tile_index: int):
        """Show searchable Picker for tile parameter selection."""
        available = self.get_available_keys_fn()
        current_key = self.tiles[tile_index]["key"]

        def on_select(key):
            self._select_tile_key(tile_index, key)

        popup = ParameterPickerPopup(self, available, on_select, initial=current_key)
        try:
            popup.geometry(f"+{event.x_root}+{event.y_root}")
        except Exception:
            pass
    
    def _select_tile_key(self, tile_index: int, key: str):
        """Select a new key for a tile."""
        tile = self.tiles[tile_index]
        canon = canonical_key(key)
        if not canon:
            return
        old_key = canonical_key(tile.get("key", ""))
        new_ident = self._tile_identity(canon)

        duplicate_idx = None
        for idx, other in enumerate(self.tiles):
            if idx == tile_index:
                continue
            if self._tile_identity(other.get("key", "")) == new_ident:
                duplicate_idx = idx
                break

        if duplicate_idx is not None and old_key:
            self._apply_tile_key(self.tiles[duplicate_idx], old_key)
            self._clear_sparkline(duplicate_idx)

        self._apply_tile_key(tile, canon)

        # Reset sparkline buffer so stale history from the old key doesn't show.
        self._clear_sparkline(tile_index)

        # Save to config
        keys = [t["key"] for t in self.tiles]
        self._save_tile_keys(keys)
    
    def update(
        self,
        values: Optional[Dict[str, Any]],
        data_valid: bool = True,
        quality: str = "GOOD",
        **_ignored,
    ):
        """Update tile values from data dict.

        Note: callers in other modules may pass extra keyword args (e.g. quality_state)
        depending on build version. We intentionally ignore unknown kwargs to keep the
        Dashboard operator-safe (no crashes, no silent freezes).
        """
        quality_name = (
            quality.value if hasattr(quality, "value") else str(quality or "")
        ).upper()
        dot_color = QUALITY_COLORS.get(quality_name, "#5e5a56")
        # Value text color: accent (blue) when GOOD, otherwise quality color (amber/red/grey)
        value_color = "#4da6ff" if (data_valid and quality_name == "GOOD") else dot_color

        for tile in self.tiles:
            key = tile["key"]
            key_canon = canonical_key(key)
            value_lbl = tile["value_label"]

            # Update quality dot
            try:
                tile["dot_canvas"].itemconfig(tile["dot_id"], fill=dot_color)
            except Exception:
                pass

            # Get or create sparkline buffer for this tile (lazy, keyed by tile index).
            t_idx = tile["index"]
            spark_buf = self._spark_bufs.get(t_idx)
            if spark_buf is None:
                spark_buf = _SparklineBuffer(capacity=20)
                self._spark_bufs[t_idx] = spark_buf

            if not data_valid or not values:
                # Show quality state text, muted; push invalid sample so sparkline shows a gap.
                value_lbl.config(text="--", foreground=dot_color)
                self._fit_value_label(value_lbl, "--")
                spark_buf.push_invalid()
                try:
                    sw = max(56, int(tile["sparkline"].winfo_width() or 0))
                    sh = max(18, int(tile["sparkline"].winfo_height() or 0))
                    _draw_sparkline(tile["sparkline"], spark_buf.values(10), dot_color, sw, sh)
                except Exception:
                    pass
                if self._display_style == "gauges":
                    self._draw_gauge(tile, key_canon, None, dot_color)
                continue

            v = None
            try:
                v = values.get(key_canon)
                if v is None and key_canon != key:
                    v = values.get(key)
            except Exception:
                v = None

            if isinstance(v, (int, float)):
                # Format based on magnitude — keep it compact for tiles
                try:
                    fv = float(v)
                    text = self._format_raw_value(fv) if self._raw_numbers else self._format_compact_value(fv)
                except Exception:
                    text = str(v)
                value_lbl.config(text=text, foreground=value_color)
                self._fit_value_label(value_lbl, text)
                # Push valid sample and redraw sparkline.
                spark_buf.push(fv)
                try:
                    sw = max(56, int(tile["sparkline"].winfo_width() or 0))
                    sh = max(18, int(tile["sparkline"].winfo_height() or 0))
                    _draw_sparkline(tile["sparkline"], spark_buf.values(10), value_color, sw, sh)
                except Exception:
                    pass
                if self._display_style == "gauges":
                    self._draw_gauge(tile, key_canon, fv, value_color)
            else:
                if values and key_canon not in values:
                    log_once(logger, f"tile_missing_{key_canon}", "warning",
                             f"Tile key missing: '{key_canon}'. Available keys sample: {list(values.keys())[:12]}")
                value_lbl.config(text="--", foreground=dot_color)
                self._fit_value_label(value_lbl, "--")
                spark_buf.push_invalid()
                try:
                    sw = max(56, int(tile["sparkline"].winfo_width() or 0))
                    sh = max(18, int(tile["sparkline"].winfo_height() or 0))
                    _draw_sparkline(tile["sparkline"], spark_buf.values(10), dot_color, sw, sh)
                except Exception:
                    pass
                if self._display_style == "gauges":
                    self._draw_gauge(tile, key_canon, None, dot_color)

    def set_fault_states(self, states: Dict[str, str]) -> None:
        """
        Colour the fault indicator stripe on tiles whose parameter has an
        active protection rule in WARNING or ALARM state.

        ``states`` maps canonical parameter key → "ALARM" | "WARNING".
        Keys absent from ``states`` (or mapped to "NORMAL") reset to hidden.

        Visual convention (IEC 62682 / ISA-101):
          ALARM   → red  (#ef4444)  — requires immediate operator action
          WARNING → amber (#f59e0b) — operator awareness required
          NORMAL  → hidden          — no action needed
        """
        for tile in self.tiles:
            stripe = tile.get("fault_stripe")
            if stripe is None:
                continue
            key_c = canonical_key(tile["key"])
            state = (states.get(key_c) or states.get(tile["key"]) or "").upper().strip()
            new_bg = _FAULT_STRIPE_COLOR.get(state, _FAULT_STRIPE_NORMAL)
            new_h  = 4 if state in _FAULT_STRIPE_COLOR else 2
            try:
                stripe.config(bg=new_bg, height=new_h)
            except Exception:
                pass


class TrendChartPair(ttk.Frame):
    """
    Container for two side-by-side trend charts with parameter selection
    and time window control.
    """
    
    def __init__(self, parent, cfg: dict, view_key: str, 
                 get_available_keys_fn, trend_manager: TrendManager, on_config_change=None):
        super().__init__(parent)
        self.cfg = cfg
        self.view_key = view_key
        self.get_available_keys_fn = get_available_keys_fn
        self.trend_manager = trend_manager
        self.on_config_change = on_config_change
        self._normalizing_chart_pair = False
        
        self.charts: List[TrendChartFrame] = []
        self._build()
        self._load_config()
    
    def _get_trend_config(self) -> dict:
        """Get trend config for this view."""
        dash_cfg = self.cfg.setdefault("dashboard", {})
        trends_cfg = dash_cfg.setdefault("trends", {})
        key = str(self.view_key)
        return trends_cfg.setdefault(key, {
            "chart1_key": "kW",
            "chart2_key": "Import_kWh",
            "chart1_window": 60,
            "chart2_window": 60,
        })
    
    def _save_trend_config(self):
        """Save trend config."""
        if self._normalizing_chart_pair:
            return
        self._ensure_distinct_primary_keys()
        dash_cfg = self.cfg.setdefault("dashboard", {})
        trends_cfg = dash_cfg.setdefault("trends", {})
        def _saved_overlays(chart):
            limit = int(getattr(chart, "max_overlay_keys", 4) or 4)
            out = []
            taken = {canonical_key(getattr(chart, "selected_key", "") or "")}
            for raw in list(getattr(chart, "overlay_keys", []) or [])[:limit]:
                canon = canonical_key(raw)
                if canon and canon not in taken:
                    out.append(canon)
                    taken.add(canon)
            return out
        trends_cfg[str(self.view_key)] = {
            "chart1_key": canonical_key(self.charts[0].selected_key),
            "chart2_key": canonical_key(self.charts[1].selected_key),
            "chart1_window": self.charts[0].window_minutes,
            "chart2_window": self.charts[1].window_minutes,
            "chart1_overlays": _saved_overlays(self.charts[0]),
            "chart2_overlays": _saved_overlays(self.charts[1]),
        }
        if callable(self.on_config_change):
            try:
                self.on_config_change()
            except Exception:
                pass

    def _candidate_chart_keys(self) -> List[str]:
        out: List[str] = []
        seen = set()
        try:
            keys = list(self.get_available_keys_fn() or [])
        except Exception:
            keys = []
        for raw in list(keys) + list(COMMON_CHART_KEYS):
            canon = canonical_key(raw)
            if canon and canon not in seen:
                seen.add(canon)
                out.append(canon)
        return out

    def _ensure_distinct_primary_keys(self) -> None:
        """Avoid duplicate primary parameters across the two visible charts."""
        if self._normalizing_chart_pair or len(self.charts) < 2:
            return
        left = canonical_key(getattr(self.charts[0], "selected_key", "") or "")
        right = canonical_key(getattr(self.charts[1], "selected_key", "") or "")
        if not left or not right or left != right:
            return
        replacement = None
        for cand in self._candidate_chart_keys():
            if cand != left:
                replacement = cand
                break
        if not replacement:
            return
        self._normalizing_chart_pair = True
        try:
            try:
                self.charts[1]._select_key(replacement)
            except Exception:
                self.charts[1].set_key(replacement)
                try:
                    self.charts[1]._request_config_refresh(20)
                except Exception:
                    pass
        finally:
            self._normalizing_chart_pair = False
    
    def _load_config(self):
        """Load saved configuration."""
        cfg = self._get_trend_config()
        self.charts[0].set_key(cfg.get("chart1_key", "kW"))
        self.charts[0].set_window(cfg.get("chart1_window", 60))
        self.charts[1].set_key(cfg.get("chart2_key", "Import_kWh"))
        self.charts[1].set_window(cfg.get("chart2_window", 60))
        self._ensure_distinct_primary_keys()
        for chart, cfg_key in ((self.charts[0], "chart1_overlays"), (self.charts[1], "chart2_overlays")):
            limit = int(getattr(chart, "max_overlay_keys", 4) or 4)
            taken = {canonical_key(getattr(chart, "selected_key", "") or "")}
            overlays = []
            for raw in list(cfg.get(cfg_key, []) or []):
                canon = canonical_key(raw)
                if canon and canon not in taken:
                    overlays.append(canon)
                    taken.add(canon)
                if len(overlays) >= limit:
                    break
            try:
                chart.overlay_keys = overlays
            except Exception:
                pass

    @staticmethod
    def _preset_trace_keys(keys: List[str], limit: int = 5) -> List[str]:
        traces = []
        for raw in keys or []:
            canon = canonical_key(raw)
            if canon and canon not in traces:
                traces.append(canon)
            if len(traces) >= limit:
                break
        return traces

    def apply_preset(
        self,
        chart1_keys: List[str],
        chart2_keys: List[str],
        chart1_window: int = 60,
        chart2_window: int = 60,
    ) -> None:
        """Apply chart trace presets and save the trend layout."""
        for chart, keys, window in (
            (self.charts[0], chart1_keys, chart1_window),
            (self.charts[1], chart2_keys, chart2_window),
        ):
            traces = self._preset_trace_keys(keys, limit=1 + int(getattr(chart, "max_overlay_keys", 4) or 4))
            if traces:
                chart.set_key(traces[0])
                chart.overlay_keys = traces[1:]
            try:
                chart.set_window(window)
                chart._update_header()
                chart._request_config_refresh(20)
            except Exception:
                pass
        self._ensure_distinct_primary_keys()
        self._save_trend_config()
    
    def _build(self):
        """Build two side-by-side charts."""
        self.columnconfigure(0, weight=1)
        self.columnconfigure(1, weight=1)
        self.rowconfigure(0, weight=1)
        
        # Chart 1
        chart1 = TrendChartFrame(
            self,
            title="Trend 1",
            get_available_keys_fn=self.get_available_keys_fn,
            on_config_change=self._save_trend_config,
            max_plot_points=int((self.cfg or {}).get("ui", {}).get("max_plot_points", 1000)),
            show_alarm_shading=bool((self.cfg or {}).get("ui", {}).get("show_alarm_shading", True)),
            alarm_shading_max_intervals=int((self.cfg or {}).get("ui", {}).get("alarm_shading_max_intervals", 40)),
        )
        chart1.grid(row=0, column=0, padx=2, pady=2, sticky="nsew")
        self.charts.append(chart1)
        
        # Chart 2
        chart2 = TrendChartFrame(
            self,
            title="Trend 2",
            get_available_keys_fn=self.get_available_keys_fn,
            on_config_change=self._save_trend_config,
            max_plot_points=int((self.cfg or {}).get("ui", {}).get("max_plot_points", 1000)),
            show_alarm_shading=bool((self.cfg or {}).get("ui", {}).get("show_alarm_shading", True)),
            alarm_shading_max_intervals=int((self.cfg or {}).get("ui", {}).get("alarm_shading_max_intervals", 40)),
        )
        chart2.grid(row=0, column=1, padx=2, pady=2, sticky="nsew")
        self.charts.append(chart2)

        if not CHARTS_AVAILABLE:
            notice = ttk.Label(
                self,
                text=CHARTS_DISABLED_MESSAGE,
                justify="center",
            )
            notice.grid(row=1, column=0, columnspan=2, sticky="ew", padx=4, pady=(2, 4))

    def set_layout_mode(self, mode: str = "both") -> None:
        """Switch chart pair layout without adding dashboard controls."""
        mode = str(mode or "both").strip().lower()
        if mode not in {"both", "chart1", "chart2"}:
            mode = "both"
        try:
            for ch in self.charts:
                ch.grid_remove()
            if mode == "both":
                # Reset both columns to equal weight + no minimum size constraint.
                # columnspan must be explicitly 1 to clear any prior columnspan=2.
                self.columnconfigure(0, weight=1, minsize=0)
                self.columnconfigure(1, weight=1, minsize=0)
                self.charts[0].grid(row=0, column=0, columnspan=1, padx=2, pady=2, sticky="nsew")
                self.charts[1].grid(row=0, column=1, columnspan=1, padx=2, pady=2, sticky="nsew")
            elif mode == "chart1":
                self.columnconfigure(0, weight=1, minsize=0)
                self.columnconfigure(1, weight=0, minsize=0)
                self.charts[0].grid(row=0, column=0, columnspan=2, padx=2, pady=2, sticky="nsew")
            else:  # chart2
                self.columnconfigure(0, weight=1, minsize=0)
                self.columnconfigure(1, weight=0, minsize=0)
                self.charts[1].grid(row=0, column=0, columnspan=2, padx=2, pady=2, sticky="nsew")
            # Force geometry recalculation before matplotlib sizes its canvas.
            # Without this, the chart that renders first can grab all the space
            # and the second one appears cropped on the next redraw cycle.
            try:
                self.update_idletasks()
            except Exception:
                pass
        except Exception:
            pass

    def save_visible_snapshots(self) -> List[str]:
        """Save snapshots for charts that are currently mapped."""
        paths: List[str] = []
        for chart in self.charts:
            try:
                if not bool(chart.winfo_ismapped()):
                    continue
                if hasattr(chart, "save_snapshot_auto"):
                    path = chart.save_snapshot_auto()
                    if path:
                        paths.append(path)
            except Exception:
                pass
        return paths
    
    def update_charts(
        self,
        meter_id: Any,
        values: Optional[Dict[str, Any]],
        data_valid: bool = True,
        quality_state: Optional[str] = None,
        alarm_ranges: Optional[List[tuple[float, float, str]]] = None,
        alarm_ranges_map: Optional[Dict[str, List[tuple[float, float, str]]]] = None,
        markers: Optional[List[tuple[float, str, str]]] = None,
        markers_map: Optional[Dict[str, List[tuple[float, str, str]]]] = None,
        force_push: bool = False,
        threshold_lines_map: Optional[Dict[str, list]] = None,
    ):
        """Update both charts with new data."""
        now = time.time()
        
        for chart in self.charts:
            key = chart.selected_key
            key_canon = canonical_key(key)
            window = chart.window_minutes
            trace_keys = [key_canon] if key_canon else []
            try:
                for ok in getattr(chart, "overlay_keys", []) or []:
                    okc = canonical_key(ok)
                    if okc and okc not in trace_keys:
                        trace_keys.append(okc)
            except Exception:
                pass

            # Push new data point (SCADA-grade):
            # - only push numeric values when data is valid (OK)
            # - on OK <-> invalid transitions, TrendManager inserts a NaN marker to break the line
            if values:
                for push_key in trace_keys:
                    v = None
                    try:
                        v = values.get(push_key)
                        if v is None and push_key == key_canon and key_canon != key:
                            v = values.get(key)
                    except Exception:
                        v = None

                    if data_valid and isinstance(v, (int, float)):
                        try:
                            self.trend_manager.push_sample(meter_id, push_key, now, value=float(v), valid=True)
                        except Exception:
                            log_once(logger, f"trend_push_fail_{meter_id}_{push_key}", "warning",
                                     f"Trend push failed for meter {meter_id} key {push_key}")
                    else:
                        # Invalid/stale/offline: do not push numeric values (prevents flat lies).
                        # We still notify the buffer about invalid state so it can break the line once.
                        try:
                            self.trend_manager.push_sample(meter_id, push_key, now, value=None, valid=False)
                        except Exception:
                            pass

                        # Only log missing keys when we are in valid mode; otherwise it creates noise during comm loss.
                        if data_valid and values and push_key not in values and v is None:
                            log_once(logger, f"trend_missing_{meter_id}_{push_key}", "warning",
                                     f"Trend key missing: meter {meter_id} key '{push_key}'. Available keys sample: {list(values.keys())[:12]}")
                        elif data_valid and v is not None and not isinstance(v, (int, float)):
                            log_once(logger, f"trend_non_numeric_{meter_id}_{push_key}", "warning",
                                     f"Trend value not numeric: meter {meter_id} key '{push_key}' value={v}")

            else:
                # No values dict at all (e.g., meter hasn't produced any values yet or is offline).
                # We still notify invalid state so long gaps / comm loss won't be drawn as a straight line.
                if (not data_valid) or force_push:
                    for push_key in trace_keys:
                        try:
                            self.trend_manager.push_sample(meter_id, push_key, now, value=None, valid=False)
                        except Exception:
                            pass

            # Always set quality state (cheap; keeps overlays correct when you come back)
            chart.set_quality_state(quality_state)

            # ✅ Visible-only redraw:
            # Skip expensive series extraction + matplotlib redraw when this chart isn't actually visible.
            try:
                can_draw = chart.can_draw_now()
            except Exception:
                try:
                    can_draw = bool(chart.winfo_viewable())
                except Exception:
                    can_draw = True

            if not can_draw:
                continue

            # Get series and update chart (only when drawable)
            # Primary + overlays (Graph+ feel) — keep CPU bounded.
            keys = trace_keys
            if not keys:
                continue

            series = []
            v_list = []
            for kx in keys:
                try:
                    tt, yy = self.trend_manager.get_series(meter_id, kx, window)
                    vv = self.trend_manager.get_version(meter_id, kx)
                except Exception:
                    tt, yy, vv = [], [], 0
                # Legend label: Source / Param
                try:
                    lbl = f"{meter_id} / {key_label(kx)}"
                except Exception:
                    lbl = str(kx)
                series.append((lbl, tt, yy))
                v_list.append(int(vv or 0))

            version = max(v_list) if v_list else 0
            if len(series) > 1 and hasattr(chart, "update_multi_series"):
                chart.update_multi_series(series, version)
            else:
                t_list, y_list = series[0][1], series[0][2]
                chart.update_series(t_list, y_list, version)

            # Alarm highlighting:
            # - alarm_ranges (legacy): applies to both charts
            # - alarm_ranges_map: per-parameter ranges (preferred)
            per_key = []
            if alarm_ranges_map:
                per_key = alarm_ranges_map.get(key_canon, []) or alarm_ranges_map.get(key, []) or []
            merged = (alarm_ranges or []) + (per_key or [])
            chart.set_alarm_ranges(merged)

            # Event markers (vertical lines): alarms + protection transitions.
            m_per_key = []
            if markers_map:
                m_per_key = markers_map.get(key_canon, []) or markers_map.get(key, []) or []
            m_merged = (markers or []) + (m_per_key or [])
            chart.set_markers(m_merged)

            # Threshold lines: dashed horizontal lines at protection pickup levels.
            # Only drawn for rules currently in WARNING or ALARM state.
            thresholds = []
            if threshold_lines_map:
                thresholds = (
                    threshold_lines_map.get(key_canon)
                    or threshold_lines_map.get(key)
                    or []
                )
            if hasattr(chart, "set_threshold_lines"):
                chart.set_threshold_lines(thresholds)


class SystemHealthWidget(tk.Frame):
    """
    Compact system health strip shown at the bottom of the TOTAL dashboard tab.

    Cells (left to right):
      Comm  |  Last Read  |  Historian  |  Data Quality  |  Email Queue  |  Restarts  |  Uptime
    """

    _CELLS = [
        ("comm",     "COMM",       "—"),
        ("last_rx",  "LAST READ",  "—"),
        ("historian","HISTORIAN",  "—"),
        ("data_q",   "DATA QUALITY","—"),
        ("email_q",  "EMAIL QUEUE","—"),
        ("restarts", "RESTARTS",   "—"),
        ("uptime",   "UPTIME",     "—"),
    ]

    def __init__(self, parent):
        try:
            from ui.styles import get_theme
            t = get_theme()
            bg   = t.panel
            fg   = t.text
            muted = t.text_muted
            good  = t.good
            warn  = t.warn
            alarm = t.alarm
        except Exception:
            bg, fg, muted, good, warn, alarm = "#1c1c1e", "#e2e8f0", "#9a9490", "#2ecc71", "#f1c40f", "#ef4444"

        super().__init__(parent, bg=bg, relief="flat")
        self._bg = bg; self._fg = fg; self._muted = muted
        self._good = good; self._warn = warn; self._alarm = alarm

        # separator
        tk.Frame(self, bg=muted, height=1).pack(fill="x", side="top")

        inner = tk.Frame(self, bg=bg)
        inner.pack(fill="x", padx=6, pady=4)

        self._val_labels: Dict[str, tk.Label] = {}
        for i, (key, title, init) in enumerate(self._CELLS):
            cell = tk.Frame(inner, bg=bg)
            cell.pack(side="left", padx=(0, 18))
            tk.Label(cell, text=title, font=("Segoe UI", 7), fg=muted, bg=bg).pack(anchor="w")
            lbl = tk.Label(cell, text=init, font=("Segoe UI", 9, "bold"), fg=fg, bg=bg)
            lbl.pack(anchor="w")
            self._val_labels[key] = lbl

    def _set(self, key: str, text: str, color: Optional[str] = None):
        lbl = self._val_labels.get(key)
        if lbl is None:
            return
        lbl.config(text=text)
        if color:
            lbl.config(fg=color)
        else:
            lbl.config(fg=self._fg)

    def update_health(self, data: dict) -> None:
        """
        data keys (all optional):
          comm_state    str   "LIVE" / "RECONNECTING" / "OFFLINE"
          comm_detail   str   operator-facing bus message
          last_rx_age   float seconds since last good read (None = never)
          hist_size_mb  float historian DB size in MB
          hist_last_s   float seconds since last historian write (None = never)
          data_quality_issues int rejected/invalid meter values
          snapshot_drops int snapshots dropped by overloaded UI queues
          email_pending int   pending + retry jobs in queue
          email_dead    int   permanently failed jobs
          restart_count int   worker restarts today
          uptime_s      float app uptime in seconds
        """
        # COMM
        cs = str(data.get("comm_state", "—") or "—")
        cs_u = cs.upper()
        if "LIVE" in cs_u or "ONLINE" in cs_u:
            self._set("comm", "LIVE", self._good)
        elif "RECONNECT" in cs_u or "RECOVER" in cs_u:
            self._set("comm", cs, self._warn)
        elif "OFFLINE" in cs_u or "NO DATA" in cs_u:
            self._set("comm", "OFFLINE", self._alarm)
        else:
            self._set("comm", cs_u)

        # LAST READ
        age = data.get("last_rx_age")
        comm_detail = str(data.get("comm_detail", "") or "").strip()
        if age is None and comm_detail:
            self._set("last_rx", comm_detail, self._muted)
        elif age is None:
            self._set("last_rx", "never", self._muted)
        elif age < 10:
            self._set("last_rx", f"{age:.1f}s ago", self._good)
        elif age < 60:
            self._set("last_rx", f"{age:.0f}s ago", self._warn)
        else:
            self._set("last_rx", f"{age/60:.0f}m ago", self._alarm)

        # HISTORIAN
        hist_mb = data.get("hist_size_mb")
        hist_age = data.get("hist_last_s")
        if hist_mb is None:
            self._set("historian", "off", self._muted)
        else:
            age_txt = ""
            if hist_age is not None:
                age_txt = f"  {hist_age:.0f}s" if hist_age < 120 else f"  {hist_age/60:.0f}m"
            self._set("historian", f"{hist_mb:.1f} MB{age_txt}")

        # DATA QUALITY
        dq = int(data.get("data_quality_issues", 0) or 0)
        drops = int(data.get("snapshot_drops", 0) or 0)
        if dq > 0 or drops > 0:
            self._set("data_q", f"{dq} bad  {drops} drops", self._warn)
        else:
            self._set("data_q", "clean", self._good)

        # EMAIL QUEUE
        pending = int(data.get("email_pending", 0) or 0)
        dead    = int(data.get("email_dead",    0) or 0)
        if dead > 0:
            self._set("email_q", f"{pending} pending  {dead} failed", self._alarm)
        elif pending > 0:
            self._set("email_q", f"{pending} pending", self._warn)
        else:
            self._set("email_q", "idle", self._good)

        # RESTARTS
        rc = int(data.get("restart_count", 0) or 0)
        self._set("restarts", str(rc), self._alarm if rc > 0 else self._good)

        # UPTIME
        uptime = data.get("uptime_s")
        if uptime is None:
            self._set("uptime", "—")
        else:
            h = int(uptime) // 3600
            m = (int(uptime) % 3600) // 60
            self._set("uptime", f"{h}h {m:02d}m")


class DashboardLayoutDialog(tk.Toplevel):
    """Compact dialog for display-only dashboard layout controls."""

    _CHART_LAYOUT_CHOICES = (
        ("Both charts", "both"),
        ("Left chart only", "chart1"),
        ("Right chart only", "chart2"),
    )

    def __init__(self, parent, *, presets: Dict[str, dict], active_preset: str,
                 settings: dict, on_apply, on_apply_preset, on_save_visible):
        super().__init__(parent)
        self.transient(parent)
        self.title("Display Layout")
        self.resizable(False, False)
        self._presets = dict(presets or {})
        self._preset_names = {k: str(v.get("label", k.title())) for k, v in self._presets.items()}
        self._preset_labels = {v: k for k, v in self._preset_names.items()}
        self._on_apply = on_apply
        self._on_apply_preset = on_apply_preset
        self._on_save_visible = on_save_visible

        active_label = self._preset_names.get(active_preset, "")
        self.preset_var = tk.StringVar(value=active_label)
        self.surface_var = tk.StringVar(value=str(settings.get("surface", "tiles")))
        self.tile_count_var = tk.IntVar(value=int(settings.get("tile_count", 12) or 12))
        self.raw_numbers_var = tk.BooleanVar(value=bool(settings.get("raw_numbers", False)))
        self._layout_label_to_value = {label: value for label, value in self._CHART_LAYOUT_CHOICES}
        self._layout_value_to_label = {value: label for label, value in self._CHART_LAYOUT_CHOICES}
        chart_layout = str(settings.get("chart_layout", "both") or "both").strip().lower()
        self.chart_layout_var = tk.StringVar(value=self._layout_value_to_label.get(chart_layout, "Both charts"))
        self.chart_share_var = tk.IntVar(value=int(settings.get("chart_share_pct", 65) or 65))
        self.chart_share_text = tk.StringVar(value=f"{self.chart_share_var.get()}% chart area")

        self._build()
        self._sync_surface_state()
        self.grab_set()
        self.after(0, self.focus_force)

    def _build(self):
        body = ttk.Frame(self, padding=12)
        body.pack(fill="both", expand=True)

        ttk.Label(body, text="Preset", style="TileTitle.TLabel").grid(row=0, column=0, sticky="w")
        preset_values = [""] + [self._preset_names[k] for k in self._presets]
        self.preset_combo = ttk.Combobox(body, state="readonly", values=preset_values, textvariable=self.preset_var, width=18)
        self.preset_combo.grid(row=0, column=1, sticky="ew", padx=(8, 8))
        ttk.Button(body, text="Apply Preset", command=self._apply_preset, style="Small.TButton").grid(row=0, column=2, sticky="ew")

        ttk.Label(body, text="Display", style="TileTitle.TLabel").grid(row=1, column=0, sticky="w", pady=(12, 0))
        display_row = ttk.Frame(body)
        display_row.grid(row=1, column=1, columnspan=2, sticky="w", pady=(10, 0))
        for value, text in (("tiles", "Tiles"), ("gauges", "Round Gauge"), ("charts", "Charts Only")):
            ttk.Radiobutton(display_row, text=text, value=value, variable=self.surface_var).pack(side="left", padx=(0, 10))

        ttk.Label(body, text="Tile Count", style="TileTitle.TLabel").grid(row=2, column=0, sticky="w", pady=(12, 0))
        self.tile_count_combo = ttk.Combobox(
            body,
            state="readonly",
            values=[4, 6, 8, 10, 12, 14, 16],
            textvariable=self.tile_count_var,
            width=8,
        )
        self.tile_count_combo.grid(row=2, column=1, sticky="w", padx=(8, 0), pady=(10, 0))
        self.raw_numbers_check = ttk.Checkbutton(body, text="Raw numbers", variable=self.raw_numbers_var)
        self.raw_numbers_check.grid(row=2, column=2, sticky="w", pady=(10, 0))

        ttk.Label(body, text="Charts", style="TileTitle.TLabel").grid(row=3, column=0, sticky="w", pady=(12, 0))
        self.chart_layout_combo = ttk.Combobox(
            body,
            state="readonly",
            values=[label for label, _value in self._CHART_LAYOUT_CHOICES],
            textvariable=self.chart_layout_var,
            width=18,
        )
        self.chart_layout_combo.grid(row=3, column=1, sticky="w", padx=(8, 0), pady=(10, 0))
        ttk.Label(body, text="Choose 1 or 2 trend charts.", style="TileUnit.TLabel").grid(row=3, column=2, sticky="w", pady=(10, 0))

        ttk.Label(body, text="Chart Share", style="TileTitle.TLabel").grid(row=4, column=0, sticky="w", pady=(12, 0))
        self.chart_share_scale = tk.Scale(
            body,
            from_=20,
            to=80,
            orient="horizontal",
            variable=self.chart_share_var,
            showvalue=True,
            resolution=5,
            length=220,
            command=lambda _v: self.chart_share_text.set(f"{self.chart_share_var.get()}% chart area"),
        )
        self.chart_share_scale.grid(row=4, column=1, sticky="ew", padx=(8, 0), pady=(8, 0))
        ttk.Label(body, textvariable=self.chart_share_text, style="TileUnit.TLabel").grid(row=4, column=2, sticky="w", pady=(10, 0))

        btns = ttk.Frame(body)
        btns.grid(row=5, column=0, columnspan=3, sticky="ew", pady=(16, 0))
        ttk.Button(btns, text="Save Visible Charts", command=self._save_visible, style="Small.TButton").pack(side="left")
        ttk.Button(btns, text="Close", command=self.destroy).pack(side="right")
        ttk.Button(btns, text="Apply", command=self._apply, style="Primary.TButton").pack(side="right", padx=(0, 8))

        body.columnconfigure(1, weight=1)

        self.surface_var.trace_add("write", lambda *_args: self._sync_surface_state())

    def _set_state(self, widget, enabled: bool) -> None:
        try:
            widget.configure(state="normal" if enabled else "disabled")
        except Exception:
            pass

    def _sync_surface_state(self) -> None:
        surface = str(self.surface_var.get() or "tiles").strip().lower()
        has_tiles = surface in {"tiles", "gauges"}
        self._set_state(self.tile_count_combo, has_tiles)
        self._set_state(self.raw_numbers_check, has_tiles)
        self._set_state(self.chart_layout_combo, True)
        self._set_state(self.chart_share_scale, True)

    def _apply(self):
        payload = {
            "surface": self.surface_var.get(),
            "tile_count": int(self.tile_count_var.get() or 12),
            "raw_numbers": bool(self.raw_numbers_var.get()),
            "chart_layout": self._layout_label_to_value.get(self.chart_layout_var.get(), "both"),
            "chart_share_pct": int(self.chart_share_var.get() or 65),
        }
        try:
            self._on_apply(payload)
        except Exception:
            pass

    def _apply_preset(self):
        key = self._preset_labels.get(str(self.preset_var.get() or ""))
        if not key:
            return
        try:
            self._on_apply_preset(str(key))
        except Exception:
            pass

    def _save_visible(self):
        try:
            self._on_save_visible()
        except Exception:
            pass


class DashboardTab(ttk.Frame):
    """
    Main dashboard tab with sub-tabs for TOTAL and individual meters.
    Each sub-tab has 16 tiles and 2 trend charts.
    """
    
    def __init__(
        self,
        parent,
        meters,
        aggregator,
        selector,
        cfg: dict,
        alarm_engine: Optional[AlarmEngine] = None,
        event_journal=None,
        protection_engine=None,
        on_save_config=None,
        on_notify=None,
    ):
        super().__init__(parent, padding=8)
        self.meters = meters
        self.aggregator = aggregator
        self.selector = selector
        self.cfg = cfg
        self.on_save_config = on_save_config
        self.on_notify = on_notify
        self._save_after_id = None
        self.alarm_engine = alarm_engine
        self.event_journal = event_journal
        self.protection_engine = protection_engine

        # External (Protection supervision) chart highlights, injected by MainWindow.
        # Format: {'TOTAL': {'Vavg': [(start,end,color), ...]}, 'M1': {'kW': ...}}
        self._protection_alarm_ranges_by_scope: Dict[str, Dict[str, List[tuple[float, float, str]]]] = {}

        # Marker cache (avoid sqlite queries per chart)
        # key: (meter_id, window_min) -> (built_at_ts, markers_list, prot_markers_map)
        self._marker_cache = {}

        # Fault banner + tab badge tracking
        self._fault_banners: Dict[Any, FaultBannerFrame] = {}
        self._tab_frames: Dict[Any, tk.Frame] = {}    # view_key -> ttk.Frame
        self._tab_names: Dict[Any, str] = {}           # view_key -> original display name
        # Status strip labels (quality + age)
        self._status_quality_lbls: Dict[Any, tk.Label] = {}
        self._status_age_lbls: Dict[Any, tk.Label] = {}
        self._preset_buttons: Dict[Any, dict] = {}
        self._focus_buttons: Dict[Any, tk.Widget] = {}
        self._restore_surface_by_view: Dict[Any, str] = {}
        
        # Shared trend manager for all views
        max_win_min = 0
        try:
            dash_cfg = (self.cfg.get('dashboard') or {})
            trends_cfg = (dash_cfg.get('trends') or {})
            for _vk, tc in trends_cfg.items():
                max_win_min = max(max_win_min, int((tc or {}).get('chart1_window', 0) or 0))
                max_win_min = max(max_win_min, int((tc or {}).get('chart2_window', 0) or 0))
        except Exception:
            max_win_min = 0
        # Store up to the maximum configured window (min), at ~1 sample/sec. Hard cap: 24h.
        maxlen = max(7200, min(86400, max_win_min * 60)) if max_win_min else 28800
        self.trend_manager = TrendManager(maxlen=maxlen)
        
        
        self.sub = ttk.Notebook(self)
        self.sub.pack(fill="both", expand=True, padx=4, pady=4)

        self._health_widget: Optional['SystemHealthWidget'] = None

        self._views: Dict[Any, Tuple[TilesPanel, TrendChartPair]] = {}
        self._last_selection = None
        # Performance: matplotlib redraws are expensive. We throttle chart updates to ~1 Hz.
        self._last_chart_update_ts = 0.0
        self._chart_interval_sec = float((self.cfg.get("ui") or {}).get("chart_interval_sec", 1.0))
        self._build_tabs()
    
    def _get_total_output_keys(self) -> List[str]:
        slots = (self.cfg.get("total_custom") or {}).get("slots", []) or []
        keys = [(s.get("output_key") or "").strip() for s in slots if s.get("output_key")]
        if keys:
            return keys
        total = self.aggregator.compute(self.meters, cfg=self.cfg)
        if total:
            return list(total.keys())
        return COMMON_KEYS.copy()
    
    def _get_available_keys_meter(self, meter_id: int) -> List[str]:
        """Get available keys for a specific meter."""
        for m in self.meters:
            if m.meter_id == meter_id and m.values:
                return list(m.values.keys())
        return COMMON_KEYS.copy()

    def _dashboard_cfg(self) -> dict:
        return self.cfg.setdefault("dashboard", {})

    def _display_cfg(self, view_key) -> dict:
        dash_cfg = self._dashboard_cfg()
        cfg = dash_cfg.setdefault("display", {})
        base = cfg.setdefault(str(view_key), {})
        legacy_focus = bool((dash_cfg.get("charts_focus") or {}).get(str(view_key), False))
        legacy_layout = str((dash_cfg.get("chart_layout") or {}).get(str(view_key), "both") or "both").strip().lower()
        surface = str(base.get("surface", "tiles") or "tiles").strip().lower()
        chart_layout = str(base.get("chart_layout", "both") or "both").strip().lower()
        return {
            "surface": (
                surface if surface in {"tiles", "gauges", "charts"}
                else ("charts" if legacy_focus else "tiles")
            ),
            "tile_count": max(1, min(16, int(base.get("tile_count", 12) or 12))),
            "raw_numbers": bool(base.get("raw_numbers", False)),
            "chart_layout": (
                chart_layout if chart_layout in {"both", "chart1", "chart2"}
                else (legacy_layout if legacy_layout in {"both", "chart1", "chart2"} else "both")
            ),
            "chart_share_pct": max(20, min(80, int(base.get("chart_share_pct", 65) or 65))),
        }

    def _set_display_cfg(self, view_key, patch: dict) -> None:
        store = self._dashboard_cfg().setdefault("display", {}).setdefault(str(view_key), {})
        for key, value in dict(patch or {}).items():
            store[str(key)] = value

    def _open_layout_dialog(self, view_key) -> None:
        current = self._display_cfg(view_key)
        active_preset = str((self._dashboard_cfg().get("active_preset") or {}).get(str(view_key), "") or "")
        try:
            existing = getattr(self, "_layout_dialog", None)
            if existing is not None and existing.winfo_exists():
                existing.lift()
                existing.focus_force()
                return
        except Exception:
            pass

        def _apply_settings(payload: dict):
            self._set_display_cfg(view_key, payload)
            self._apply_view_layout(view_key)
            self._schedule_config_save()

        self._layout_dialog = DashboardLayoutDialog(
            self,
            presets=DASHBOARD_PRESETS,
            active_preset=active_preset,
            settings=current,
            on_apply=_apply_settings,
            on_apply_preset=lambda pk: self._apply_dashboard_preset(view_key, pk),
            on_save_visible=lambda: self._save_visible_charts(view_key),
        )

    @staticmethod
    def _toolbar_palette() -> dict:
        try:
            from ui.styles import get_theme
            t = get_theme()
            return {
                "bg": t.card,
                "fg": t.text,
                "muted": t.text_muted,
                "border": t.border,
                "accent": t.accent,
                "input_bg": t.input_bg,
            }
        except Exception:
            return {
                "bg": "#242426",
                "fg": "#e8e4dc",
                "muted": "#9a9490",
                "border": "#38383a",
                "accent": "#4da6ff",
                "input_bg": "#1a1a1c",
            }

    def _schedule_config_save(self, delay_ms: int = 450) -> None:
        """Debounce dashboard config writes from fast UI interactions."""
        if not callable(self.on_save_config):
            return
        try:
            if self._save_after_id:
                self.after_cancel(self._save_after_id)
        except Exception:
            pass

        def _do_save():
            self._save_after_id = None
            try:
                self.on_save_config()
            except Exception:
                pass

        try:
            self._save_after_id = self.after(max(0, int(delay_ms)), _do_save)
        except Exception:
            _do_save()

    def _notify(self, message: str) -> None:
        if callable(self.on_notify):
            try:
                self.on_notify(str(message or ""))
            except Exception:
                pass

    def _make_preset_toolbar(self, parent, view_key):
        pal = self._toolbar_palette()
        fg = pal["fg"]
        border = pal["border"]
        accent = pal["accent"]
        input_bg = pal["input_bg"]

        bar = tk.Frame(parent, bg=border, padx=1, pady=1)
        # Floating controls: zero layout height, so the chart grid keeps the space.
        bar.place(relx=1.0, x=-8, y=5, anchor="ne")

        menu_btn = tk.Label(
            bar,
            text="Layout",
            bg=input_bg,
            fg=fg,
            font=("Segoe UI", 8, "bold"),
            padx=7,
            pady=2,
            cursor="hand2",
        )
        menu_btn.pack(side="left")
        menu_btn.bind("<Button-1>", lambda _e, vk=view_key: self._open_layout_dialog(vk))
        menu_btn.bind("<Enter>", lambda _e, w=menu_btn, a=accent: w.configure(bg=a, fg="#ffffff"))
        menu_btn.bind("<Leave>", lambda _e, w=menu_btn, b=input_bg, f=fg: w.configure(bg=b, fg=f))
        attach_tooltip(menu_btn, "Open all display layout options for this dashboard view.")
        self._preset_buttons[view_key] = {"button": menu_btn}
        try:
            parent.bind("<Configure>", lambda _e, w=bar: w.lift(), add="+")
        except Exception:
            pass
        return bar

    def _refresh_preset_menu(self, view_key) -> None:
        controls = self._preset_buttons.get(view_key) or {}
        btn = controls.get("button") if isinstance(controls, dict) else None
        if btn is None:
            return
        try:
            active = str((self._dashboard_cfg().get("active_preset") or {}).get(str(view_key), "") or "")
            label = "Layout"
            if active:
                label = f"Layout: {str(DASHBOARD_PRESETS.get(active, {}).get('label', active)).strip()[:10]}"
            btn.configure(text=label)
        except Exception:
            pass

    def _set_preset_active(self, view_key, preset_key: str) -> None:
        key = str(view_key)
        dash_cfg = self._dashboard_cfg()
        active = dash_cfg.setdefault("active_preset", {})
        active[key] = str(preset_key or "")
        self._refresh_preset_menu(view_key)

    def _apply_dashboard_preset(self, view_key, preset_key: str) -> None:
        spec = DASHBOARD_PRESETS.get(str(preset_key or ""))
        if not spec:
            return
        view = self._views.get(view_key)
        if not view:
            return
        tiles, charts = view
        try:
            tiles.apply_keys(list(spec.get("tiles") or []))
        except Exception:
            pass
        try:
            charts.apply_preset(
                list(spec.get("chart1") or []),
                list(spec.get("chart2") or []),
                int(spec.get("chart1_window", 60) or 60),
                int(spec.get("chart2_window", 60) or 60),
            )
        except Exception:
            pass
        self._set_preset_active(view_key, str(preset_key))
        self._schedule_config_save()

    def _chart_focus_enabled(self, view_key) -> bool:
        return self._display_cfg(view_key).get("surface") == "charts"

    def _sync_chart_focus_button(self, view_key) -> None:
        btn = self._focus_buttons.get(view_key)
        if btn is None:
            return
        self._refresh_preset_menu(view_key)

    def _apply_chart_focus(self, view_key) -> None:
        view = self._views.get(view_key)
        if not view:
            return
        tiles, _charts = view
        surface = self._display_cfg(view_key).get("surface", "tiles")
        try:
            if surface == "charts":
                tiles.grid_remove()
            else:
                tiles.grid()
        except Exception:
            pass
        self._sync_chart_focus_button(view_key)

    def _toggle_chart_focus(self, view_key) -> None:
        try:
            current = self._display_cfg(view_key).get("surface", "tiles")
            if current == "charts":
                restored = self._restore_surface_by_view.get(view_key, "tiles")
                self._set_display_cfg(view_key, {"surface": restored})
            else:
                self._restore_surface_by_view[view_key] = current
                self._set_display_cfg(view_key, {"surface": "charts"})
        except Exception:
            pass
        self._apply_view_layout(view_key)
        self._schedule_config_save()

    def _chart_layout_mode(self, view_key) -> str:
        mode = str(self._display_cfg(view_key).get("chart_layout", "both") or "both").strip().lower()
        return mode if mode in {"both", "chart1", "chart2"} else "both"

    def _apply_chart_layout(self, view_key) -> None:
        self._apply_view_layout(view_key)

    def _set_chart_layout(self, view_key, mode: str) -> None:
        try:
            clean = str(mode or "both").strip().lower()
            self._set_display_cfg(view_key, {"chart_layout": clean if clean in {"both", "chart1", "chart2"} else "both"})
        except Exception:
            pass
        self._apply_view_layout(view_key)
        self._schedule_config_save()

    def _apply_view_layout(self, view_key) -> None:
        view = self._views.get(view_key)
        frame = self._tab_frames.get(view_key)
        if not view or frame is None:
            return
        tiles, pair = view
        display = self._display_cfg(view_key)
        try:
            tiles.set_display_options(
                tile_count=int(display.get("tile_count", 12) or 12),
                style="gauges" if display.get("surface") == "gauges" else "tiles",
                raw_numbers=bool(display.get("raw_numbers", False)),
            )
        except Exception:
            pass
        try:
            pair.set_layout_mode(self._chart_layout_mode(view_key))
        except Exception:
            pass
        try:
            if display.get("surface") == "charts":
                frame.rowconfigure(0, weight=0)
                frame.rowconfigure(2, weight=100)
            else:
                chart_share = int(display.get("chart_share_pct", 65) or 65)
                top_share = max(12, 100 - chart_share)
                frame.rowconfigure(0, weight=top_share)
                frame.rowconfigure(2, weight=chart_share)
        except Exception:
            pass
        self._apply_chart_focus(view_key)
        self._refresh_preset_menu(view_key)

    def _save_visible_charts(self, view_key) -> None:
        view = self._views.get(view_key)
        if not view:
            return
        _tiles, pair = view
        paths = []
        try:
            paths = pair.save_visible_snapshots()
        except Exception:
            paths = []
        if paths:
            self._notify(f"Saved {len(paths)} chart snapshot(s).")
        else:
            self._notify("No visible charts to save.")

    def save_current_visible_charts(self) -> None:
        key = self.current_view_key()
        if key is not None:
            self._save_visible_charts(key)

    def current_view_key(self):
        try:
            selected = self.sub.select()
            for key, frame in (self._tab_frames or {}).items():
                if str(frame) == str(selected):
                    return key
        except Exception:
            pass
        return "TOTAL" if "TOTAL" in self._views else next(iter(self._views), None)

    def set_current_chart_layout(self, mode: str) -> None:
        key = self.current_view_key()
        if key is not None:
            self._set_chart_layout(key, mode)

    def toggle_current_chart_focus(self) -> None:
        key = self.current_view_key()
        if key is not None:
            self._toggle_chart_focus(key)
    
    def _build_tabs(self):
        """Build or rebuild sub-tabs."""
        for t in self.sub.tabs():
            self.sub.forget(t)
        self._views.clear()
        self._fault_banners.clear()
        self._tab_frames.clear()
        self._tab_names.clear()
        self._status_quality_lbls.clear()
        self._status_age_lbls.clear()
        self._preset_buttons.clear()
        self._focus_buttons.clear()

        # ---- helper: build one sub-tab ----------------------------------------
        def _make_tab(view_key, tab_text, tiles_cfg_key, get_keys_fn):
            frm = ttk.Frame(self.sub)
            self.sub.add(frm, text=tab_text)
            frm.rowconfigure(0, weight=35)
            frm.rowconfigure(2, weight=65)   # row 2 = charts (expands)
            frm.columnconfigure(0, weight=1)

            tiles = TilesPanel(frm, self.cfg, tiles_cfg_key, get_keys_fn, on_config_change=self._schedule_config_save)
            tiles.grid(row=0, column=0, sticky="ew", padx=4, pady=(4, 1))

            # ── Status strip (row=1): quality badge + data age + right-click hint ──
            try:
                from ui.styles import get_theme
                _panel_bg = get_theme().panel
                _muted_fg = get_theme().text_muted
                _dim_fg   = get_theme().text_dim
            except Exception:
                _panel_bg, _muted_fg, _dim_fg = "#1c1c1e", "#9a9490", "#5e5a56"

            status_strip = tk.Frame(frm, bg=_panel_bg, height=0)
            status_strip.grid(row=1, column=0, sticky="ew", padx=4, pady=0)
            status_strip.grid_remove()
            status_strip.grid_propagate(False)

            status_quality_lbl = tk.Label(
                status_strip, text="● GOOD", bg=_panel_bg,
                fg=QUALITY_COLORS.get("GOOD", "#34c768"),
                font=("Segoe UI", 7, "bold"),
            )
            status_quality_lbl.pack(side="left", padx=(8, 10), pady=1)
            try:
                status_quality_lbl.config(text="● GOOD")
            except Exception:
                pass

            status_age_lbl = tk.Label(
                status_strip, text="", bg=_panel_bg,
                fg=_muted_fg, font=("Segoe UI", 7),
            )
            status_age_lbl.pack(side="left", padx=0, pady=1)

            status_hint = tk.Label(
                status_strip, text="Right-click tile: parameter",
                bg=_panel_bg, fg=_dim_fg, font=("Segoe UI", 7),
            )
            status_hint.pack(side="right", padx=(0, 8), pady=1)

            # Fault banner row (row=1; hidden until alarms fire)
            banner = FaultBannerFrame(frm)
            banner.grid(row=1, column=0, sticky="ew", padx=4, pady=0)
            banner.grid_remove()

            charts = TrendChartPair(
                frm, self.cfg, tiles_cfg_key, get_keys_fn, self.trend_manager,
                on_config_change=self._schedule_config_save,
            )
            charts.grid(row=2, column=0, sticky="nsew", padx=4, pady=(0, 1))

            self._views[view_key]              = (tiles, charts)
            self._fault_banners[view_key]      = banner
            self._tab_frames[view_key]         = frm
            self._tab_names[view_key]          = tab_text
            self._status_quality_lbls[view_key] = status_quality_lbl
            self._status_age_lbls[view_key]    = status_age_lbl
            self._make_preset_toolbar(frm, view_key)
            self._apply_view_layout(view_key)
        # -----------------------------------------------------------------------

        # TOTAL tab
        if self.selector.show_total():
            _make_tab("TOTAL", "TOTAL", "TOTAL", self._get_total_output_keys)
            # The bottom app StatusBar already carries COMM/LOG/uptime health.
            # Keeping the dashboard chart area free gives operators more trend visibility.
            self._health_widget = None
        else:
            self._health_widget = None

        # Meter tabs
        for mid in self.selector.selected_meters():
            meter_name = f"Meter {mid}"
            for m in self.meters:
                if m.meter_id == mid:
                    meter_name = m.name or f"Meter {mid}"
                    break
            _make_tab(mid, meter_name, f"meter_{mid}",
                      lambda m=mid: self._get_available_keys_meter(m))

    # ------------------------------------------------------------------
    # Fault banner + tab badge helpers
    # ------------------------------------------------------------------

    def _active_alarms_for(self, meter_id) -> list:
        """Return list of active AlarmEvents for *meter_id* from AlarmEngine."""
        if not self.alarm_engine:
            return []
        result = []
        try:
            for (mid, _code), ev in list(self.alarm_engine.active.items()):
                if mid == meter_id:
                    result.append(ev)
        except Exception:
            pass
        return result

    def _update_tab_badge(self, view_key, badge_suffix: str):
        """Update the notebook tab text for *view_key* with *badge_suffix*."""
        base = self._tab_names.get(view_key, str(view_key))
        new_text = base + badge_suffix
        try:
            frm = self._tab_frames.get(view_key)
            if frm is not None:
                idx = self.sub.index(frm)
                self.sub.tab(idx, text=new_text)
        except Exception:
            pass

    def _update_status_strip(self, view_key: Any, quality: str, data_valid: bool, last_ts=None) -> None:
        """Update the quality label and data-age label in the status strip."""
        q_lbl = self._status_quality_lbls.get(view_key)
        age_lbl = self._status_age_lbls.get(view_key)
        if q_lbl is None:
            return
        quality_name = (quality.value if hasattr(quality, "value") else str(quality or "GOOD")).upper()
        color = QUALITY_COLORS.get(quality_name, "#5e5a56")
        dot = "●"
        dot = "●"
        try:
            q_lbl.config(text=f"{dot} {quality_name}", fg=color)
        except Exception:
            pass
        if age_lbl is None:
            return
        try:
            if last_ts and isinstance(last_ts, (int, float)) and last_ts > 0:
                age_s = int(time.time() - float(last_ts))
                if age_s < 5:
                    age_text = "Live"
                elif age_s < 60:
                    age_text = f"Updated {age_s}s ago"
                elif age_s < 3600:
                    age_text = f"Updated {age_s // 60}m ago"
                else:
                    age_text = f"Updated {age_s // 3600}h ago"
            else:
                age_text = ""
            age_lbl.config(text=age_text)
        except Exception:
            pass

    def _alarm_ranges_for(self, meter_id: Any) -> List[tuple[float, float, str]]:
        if not self.alarm_engine:
            return []
        ranges: List[tuple[float, float, str]] = []
        cutoff = time.time() - 3600
        recent = list(reversed(self.alarm_engine.history[-200:]))
        for ev in recent:
            if ev.meter_id != meter_id:
                continue
            if ev.ts < cutoff:
                break
            start = ev.ts
            end = start + 10
            color = "#ef4444" if ev.severity == "ALARM" else "#fbbf24"
            ranges.append((start, end, color))
        return ranges

    def _build_alarm_ranges_and_markers(self, meter_id: Any, since_ts: float) -> tuple[List[tuple[float, float, str]], List[tuple[float, str, str]]]:
        """Build shaded intervals + vertical markers for AlarmEngine events.

        Prefer EventJournal (has RAISE+CLEAR), fall back to AlarmEngine.history.
        """
        now = time.time()
        since_ts = float(since_ts or 0.0)

        # --- Prefer EventJournal (accurate durations) ---
        if self.event_journal:
            try:
                # Keep this bounded: sqlite queries + shading/markers can get heavy on weak PCs.
                rows = self.event_journal.query(since_ts=since_ts, meter_id=str(meter_id), kind="ALARM", limit=600)
                # query returns newest-first; process oldest-first
                rows = list(reversed(rows))

                active: Dict[str, tuple[float, str]] = {}  # code -> (start_ts, color)
                ranges: List[tuple[float, float, str]] = []
                markers: List[tuple[float, str, str]] = []

                for r in rows:
                    try:
                        ts = float(r["ts"])
                    except Exception:
                        continue
                    code_full = str(r["code"] or "")
                    sev = str(r["severity"] or "").upper()
                    color = "#ef4444" if sev == "ALARM" else "#fbbf24"

                    action = None
                    base = code_full
                    if ":" in code_full:
                        base, action = code_full.rsplit(":", 1)
                        action = action.upper().strip()
                    else:
                        action = "RAISE"

                    if action == "RAISE":
                        active[base] = (ts, color)
                        markers.append((ts, color, "-"))
                    elif action == "CLEAR":
                        markers.append((ts, "#94a3b8", "--"))
                        st = active.pop(base, None)
                        if st:
                            start_ts, c = st
                            if ts > start_ts:
                                ranges.append((start_ts, ts, c))

                # Any still active => up to now
                for start_ts, c in active.values():
                    if now > start_ts:
                        ranges.append((start_ts, now, c))

                return ranges, markers
            except Exception:
                pass

        # --- Fallback (approx): use AlarmEngine.history raise timestamps only ---
        ranges: List[tuple[float, float, str]] = []
        markers: List[tuple[float, str, str]] = []
        if self.alarm_engine:
            cutoff = since_ts
            recent = list(reversed(self.alarm_engine.history[-400:]))
            for ev in recent:
                if ev.meter_id != meter_id:
                    continue
                if ev.ts < cutoff:
                    break
                start = float(ev.ts)
                end = start + 10.0
                color = "#ef4444" if ev.severity == "ALARM" else "#fbbf24"
                ranges.append((start, end, color))
                markers.append((start, color, "-"))
        return ranges, markers

    def _build_protection_markers_map(self, meter_id: Any, since_ts: float) -> Dict[str, List[tuple[float, str, str]]]:
        """Per-parameter markers from ProtectionEngine state transition events."""
        if not self.protection_engine:
            return {}

        try:
            now = time.time()
            since_ts = float(since_ts or 0.0)
            if meter_id == "TOTAL":
                scope = "TOTAL"
            else:
                scope = f"M{int(meter_id)}"
        except Exception:
            scope = str(meter_id)

        out: Dict[str, List[tuple[float, str, str]]] = {}
        try:
            evs = self.protection_engine.get_recent_events(since_ts, include_advisory=True)
        except Exception:
            evs = []

        # newest-first -> draw oldest-first by reversing later at chart
        for ev in evs:
            try:
                if str(ev.get("scope") or "").upper().strip() != scope:
                    continue
                st = str(ev.get("state") or "").upper().strip()
                if st not in ("WARNING", "ALARM", "NORMAL"):
                    continue
                ts = float(ev.get("ts") or 0.0)
                if ts < since_ts:
                    continue
                param = canonical_key(str(ev.get("param") or "").strip())
                if not param:
                    continue
                if st == "WARNING":
                    out.setdefault(param, []).append((ts, "#fbbf24", "-"))
                elif st == "ALARM":
                    out.setdefault(param, []).append((ts, "#ef4444", "-"))
                else:  # NORMAL
                    out.setdefault(param, []).append((ts, "#94a3b8", "--"))
            except Exception:
                continue

        return out

    def _get_marker_bundle(self, meter_id: Any, window_minutes: int):
        """Cached marker+range builder (keeps CPU low on weak PCs)."""
        try:
            w = int(window_minutes)
        except Exception:
            w = 60
        now = time.time()
        cache_key = (str(meter_id), w)
        cached = self._marker_cache.get(cache_key)
        if cached and (now - float(cached[0] or 0.0)) < 1.0:
            return cached[1], cached[2], cached[3]

        since_ts = now - float(w) * 60.0
        alarm_ranges, alarm_markers = self._build_alarm_ranges_and_markers(meter_id, since_ts)
        prot_markers_map = self._build_protection_markers_map(meter_id, since_ts)

        self._marker_cache[cache_key] = (now, alarm_ranges, alarm_markers, prot_markers_map)
        return alarm_ranges, alarm_markers, prot_markers_map

    # ---- Protection supervision highlights (from Protection tab rules) ----
    def set_protection_alarm_ranges(self, ranges_by_scope: dict):
        """Inject active Protection supervision highlight ranges.

        Expected format (from ProtectionEngine.get_active_alarm_ranges):
            {'TOTAL': {'Vavg': [(start,end,color), ...]}, 'M1': {...}}
        """
        self._protection_alarm_ranges_by_scope = ranges_by_scope or {}
        self._update_tile_fault_states()

    def _update_tile_fault_states(self) -> None:
        """
        Push per-key fault states derived from active protection ranges to
        each TilesPanel so tile stripes reflect current protection state.

        Colour mapping:
          '#ef4444' in ranges → "ALARM"   (at least one active ALARM rule)
          '#fbbf24' in ranges → "WARNING" (WARNING rules only)
          absent from ranges  → "NORMAL"  (stripe hidden)
        """
        ranges = self._protection_alarm_ranges_by_scope or {}
        scope_states: Dict[str, Dict[str, str]] = {}

        for scope, params in ranges.items():
            key_states: Dict[str, str] = {}
            for param_key, span_list in (params or {}).items():
                worst = ""
                for _s, _e, color in (span_list or []):
                    if str(color) == "#ef4444":
                        worst = "ALARM"
                        break
                    worst = "WARNING"
                if worst:
                    key_states[canonical_key(param_key)] = worst
            if key_states:
                scope_states[str(scope).upper()] = key_states

        for view_key, (tiles_panel, _) in self._views.items():
            scope = "TOTAL" if view_key == "TOTAL" else f"M{int(view_key)}"
            states = scope_states.get(scope, {})
            try:
                tiles_panel.set_fault_states(states)
            except Exception:
                pass

    def _protection_ranges_map_for(self, meter_id: Any) -> Dict[str, List[tuple[float, float, str]]]:
        if not getattr(self, "_protection_alarm_ranges_by_scope", None):
            return {}
        if meter_id == "TOTAL":
            scope = "TOTAL"
        else:
            try:
                scope = f"M{int(meter_id)}"
            except Exception:
                scope = str(meter_id)
        return (self._protection_alarm_ranges_by_scope.get(scope) or {})

    def _threshold_lines_map_for(self, meter_id: Any) -> Dict[str, list]:
        """Return {param_key: [(level, color, label), ...]} for the given scope.

        Calls protection_engine.get_threshold_lines_by_scope() — a cheap pure-Python
        iteration over the rules list — so no caching is required.
        Scope mapping:  meter_id=="TOTAL" → "TOTAL",  integer → "M{n}"
        """
        if not getattr(self, "protection_engine", None):
            return {}
        try:
            by_scope = self.protection_engine.get_threshold_lines_by_scope()
        except Exception:
            return {}
        if meter_id == "TOTAL":
            scope = "TOTAL"
        else:
            try:
                scope = f"M{int(meter_id)}"
            except Exception:
                scope = str(meter_id)
        return by_scope.get(scope) or {}

    def on_show(self):
        """Called by MainWindow when the Dashboard page becomes visible."""
        try:
            # Force an immediate refresh so tiles show live values instantly
            # and charts redraw from buffered trends.
            self._last_chart_update_ts = 0.0
            self.update_view()
        except Exception:
            return

    def update_view(self):
        """Update dashboard with latest data."""
        # Rebuild tabs if selection changed
        current_selection = (self.selector.show_total(), tuple(self.selector.selected_meters()))
        if current_selection != self._last_selection:
            self._build_tabs()
            self._last_selection = current_selection
        stale_sec = stale_seconds(self.cfg)
        now = time.time()
        do_charts = (now - self._last_chart_update_ts) >= self._chart_interval_sec
        
        # Update TOTAL view
        if "TOTAL" in self._views:
            total = self.aggregator.compute(self.meters, cfg=self.cfg)
            # NOTE (SCADA-grade): TotalAggregator's "data_valid" is derived from a
            # quality roll-up that can be influenced by meters that are NOT
            # selected for TOTAL (e.g. disabled/offline meters shown in Display).
            # If we use that flag for chart ingestion we inject NaN breaks and
            # the TOTAL trend looks "dotted" even while at least one valid meter
            # is contributing.
            total_valid = bool(total.get("data_valid", False))
            trend_valid = int(total.get("_valid_meter_count", 0) or 0) > 0
            tiles, charts = self._views["TOTAL"]
            total_quality = total.get("quality", "GOOD")
            tiles.update(total, data_valid=total_valid,
                    quality_state=(None if total_valid else "STALE"), quality=total_quality)

            # Status strip update for TOTAL — use newest meter timestamp
            _total_last_ts = None
            try:
                _ts_vals = [float(m.last_good_ts) for m in self.meters if getattr(m, "last_good_ts", None)]
                if _ts_vals:
                    _total_last_ts = max(_ts_vals)
            except Exception:
                pass
            self._update_status_strip("TOTAL", total_quality, total_valid, _total_last_ts)

            # Fault banner + tab badge for TOTAL
            total_alarms = self._active_alarms_for("TOTAL")
            banner = self._fault_banners.get("TOTAL")
            badge = banner.update_alarms(total_alarms) if banner is not None else _TAB_BADGE_NONE
            self._update_tab_badge("TOTAL", badge)
            total_has_values = any(
                isinstance(total.get(k), (int, float)) for k in total if not k.startswith("_")
            )
            total_force_push = total_has_values and not total_valid
            if do_charts:
                # Marker + alarm shading bundle (use max window across the 2 charts)
                try:
                    wmax = max(int(charts.charts[0].window_minutes), int(charts.charts[1].window_minutes))
                except Exception:
                    wmax = 60
                a_ranges, a_markers, p_markers_map = self._get_marker_bundle("TOTAL", wmax)
                charts.update_charts(
                    "TOTAL",
                    total,
                    # Use trend_valid (selected meters contributing) for chart ingestion.
                    data_valid=trend_valid,
                    quality_state=total_quality,
                    alarm_ranges=a_ranges,
                    alarm_ranges_map=self._protection_ranges_map_for("TOTAL"),
                    markers=a_markers,
                    markers_map=p_markers_map,
                    force_push=total_force_push,
                    threshold_lines_map=self._threshold_lines_map_for("TOTAL"),
                )
        
        # Update meter views
        for m in self.meters:
            if m.meter_id in self._views:
                tiles, charts = self._views[m.meter_id]
                meter_valid = meter_is_fresh(m, stale_sec)
                meter_values = getattr(m, "values", {}) or {}
                meter_has_data = bool(meter_values)
                first_data = bool(getattr(m, "first_data_received", False))
                values = meter_values if meter_has_data else None
                quality_value = getattr(m, "quality", MeterQuality.DISABLED)
                quality_name = (
                    quality_value.value if isinstance(quality_value, MeterQuality) else str(quality_value)
                )
                tiles.update(values, data_valid=meter_valid,
                        quality_state=(None if quality_name == "GOOD" else ("STALE" if quality_name == "STALE" else "OFFLINE")), quality=quality_name)

                # Status strip update for this meter
                last_ts = getattr(m, "last_good_ts", None)
                self._update_status_strip(m.meter_id, quality_name, meter_valid, last_ts)

                # Fault banner + tab badge for this meter
                meter_alarms = self._active_alarms_for(m.meter_id)
                banner = self._fault_banners.get(m.meter_id)
                badge = banner.update_alarms(meter_alarms) if banner is not None else _TAB_BADGE_NONE
                self._update_tab_badge(m.meter_id, badge)
                force_push = meter_has_data and not meter_valid and first_data
                if do_charts:
                    try:
                        wmax = max(int(charts.charts[0].window_minutes), int(charts.charts[1].window_minutes))
                    except Exception:
                        wmax = 60
                    a_ranges, a_markers, p_markers_map = self._get_marker_bundle(m.meter_id, wmax)
                    charts.update_charts(
                        m.meter_id,
                        values,
                        data_valid=meter_valid,
                        alarm_ranges=a_ranges,
                        alarm_ranges_map=self._protection_ranges_map_for(m.meter_id),
                        markers=a_markers,
                        markers_map=p_markers_map,
                        force_push=force_push,
                        threshold_lines_map=self._threshold_lines_map_for(m.meter_id),
                    )

        if do_charts:
            self._last_chart_update_ts = now

    # ------------------------------------------------------------------
    # Background trend ingestion (important for continuity)
    # ------------------------------------------------------------------

    def _collect_needed_trend_keys(self) -> Dict[Any, set]:
        """Return {meter_id: {keys}} needed for trend buffers.

        This is the single biggest performance lever:
        we only push samples for keys that are actually plotted on any dashboard view.

        Keys are canonicalized (Modbus mapping remains untouched).
        """
        needed: Dict[Any, set] = {}
        # _views: {view_key: (tiles_panel, trend_pair)}
        for view_key, (_tiles, pair) in (self._views or {}).items():
            ks = set()
            try:
                for ch in getattr(pair, "charts", []) or []:
                    k = canonical_key(getattr(ch, "selected_key", "") or "")
                    if k:
                        ks.add(k)
                    # overlays (Graph+ style)
                    try:
                        ovs = getattr(ch, "overlay_keys", []) or []
                        for ok in ovs:
                            okc = canonical_key(ok)
                            if okc:
                                ks.add(okc)
                    except Exception:
                        pass
            except Exception:
                pass
            if ks:
                needed[view_key] = ks
        return needed

    def feed_trends(self, meters, total_dict: Optional[dict] = None, extra_needed: Optional[Dict[Any, set]] = None):
        """Feed trend buffers even when Dashboard tab is not visible.

        SCADA-grade behavior:
        - Push numeric samples only when data is valid (GOOD).
        - On GOOD <-> invalid transitions, insert a single NaN marker to break the line.
        - Throttled to ~1 Hz to protect weak PCs.
        """
        try:
            now = time.time()
            last = float(getattr(self, "_last_trend_feed_ts", 0.0) or 0.0)
            if (now - last) < 1.0:
                return
            self._last_trend_feed_ts = now

            stale_sec = stale_seconds(self.cfg)


            # Compute which keys are actually needed (charts only).


            needed = self._collect_needed_trend_keys()
            # Merge extra keys requested by other screens (Data/Graphs, etc.)
            try:
                if isinstance(extra_needed, dict):
                    for mid, ks in (extra_needed or {}).items():
                        if not ks:
                            continue
                        needed.setdefault(mid, set()).update(set(ks))
            except Exception:
                pass


            total_needed = set(needed.get("TOTAL", set()) or set())



            # TOTAL samples — keep TOTAL trends continuous even when Dashboard tab is not visible.


            if isinstance(total_dict, dict) and total_needed:


                # IMPORTANT:


                # Don't gate TOTAL trends on total_dict['data_valid'].


                # Instead: consider TOTAL valid if at least one meter is valid and contributing.


                # TOTAL validity for trending:
                # Prefer TotalAggregator metadata, but fall back to real-time meter freshness.
                # (If cfg was not passed to TotalAggregator somewhere upstream, _valid_meter_count
                # can be 0 even when meters are good. We must never let that blank TOTAL trends.)
                try:
                    total_valid = int(total_dict.get("_valid_meter_count", 0) or 0) > 0
                except Exception:
                    total_valid = False
                if not total_valid:
                    try:
                        total_valid = any(meter_is_fresh(mm, stale_sec) for mm in (meters or []))
                    except Exception:
                        total_valid = False


                for key in total_needed:


                    v = total_dict.get(key)


                    if total_valid and isinstance(v, (int, float)):


                        self.trend_manager.push_sample("TOTAL", key, now, value=float(v), valid=True)


                    else:


                        self.trend_manager.push_sample("TOTAL", key, now, value=None, valid=False)



            # Per-meter samples
            for m in meters or []:
                mid = getattr(m, "meter_id", None)
                if mid is None:
                    continue
                meter_valid = bool(meter_is_fresh(m, stale_sec))
                vals = getattr(m, "values", None) or {}
                keys_needed = set((needed.get(mid, set()) or set()))
                if not keys_needed:
                    continue
                for key in keys_needed:
                    v = vals.get(key)
                    if meter_valid and isinstance(v, (int, float)):
                        self.trend_manager.push_sample(mid, key, now, value=float(v), valid=True)
                    else:
                        self.trend_manager.push_sample(mid, key, now, value=None, valid=False)
        except Exception:
            return

    def update_system_health(self, data: dict) -> None:
        """Push health data to the SystemHealthWidget (TOTAL tab bottom strip)."""
        try:
            if self._health_widget is not None:
                self._health_widget.update_health(data)
        except Exception:
            pass
