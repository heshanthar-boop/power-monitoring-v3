"""
Small Hydro Plant KPI Dashboard Tab
=====================================
Presents IEC 60041 / CBIP performance indicators computed by core.hydro_kpi.

Layout
------
  ┌─ KPI Tiles (2 rows × 3 cols) ────────────────────────────────────────────┐
  │  Today kWh   │  PLF          │  Availability                             │
  │  Efficiency  │  Specific Energy │  CO₂ Avoided                          │
  ├─ Performance vs Target ──────────────────────────────────────────────────┤
  │  Progress bar  +  status badge  +  design target line                    │
  ├─ Plant Configuration ────────────────────────────────────────────────────┤
  │  Rated kW  Head  Flow  Turbine type  PLF design  Optional live inputs    │
  └──────────────────────────────────────────────────────────────────────────┘

Availability tracking
---------------------
The tab maintains a rolling per-session counter: seconds when P_AC > threshold.
This is reset at midnight and used for "run hours today" passed to the engine.
The MFM384 RunHour register gives accumulated lifetime hours — the midnight
delta is also tracked when that key is available.
"""
from __future__ import annotations

import time
import tkinter as tk
from tkinter import ttk
from typing import Any, Dict, Optional

from core.hydro_kpi import (
    HydroKPIEngine,
    HydroPlantConfig,
    CEA_CO2_KG_PER_KWH,
    RHO_FRESH_WATER,
    _TURBINE_EFF_RANGE,
)
from ui.styles import get_theme
from utils.logger import setup_logger

logger = setup_logger("hydro_kpi_view")

# Colour constants used throughout this module
_MUTED  = "#9a9490"
_AMBER  = "#d97706"
_STATUS_COLORS = {
    "ON_TARGET":    "#16a34a",
    "BELOW_TARGET": "#d97706",
    "POOR":         "#dc2626",
}


def _t():
    return get_theme()


def _status_colors():
    t = _t()
    return {
        "ON_TARGET":    t.good,
        "BELOW_TARGET": t.warn,
        "POOR":         t.alarm,
        "UNCONFIGURED": t.stale,
    }

_ENGINE = HydroKPIEngine()

TURBINE_TYPES = list(_TURBINE_EFF_RANGE.keys())


# ─────────────────────────────────────────────────────────────────────────────
# KPI tile widget  (shared pattern with solar_kpi_view)
# ─────────────────────────────────────────────────────────────────────────────

class _KpiTile(ttk.Frame):
    def __init__(self, parent, title: str, unit: str, info: str = "",
                 good: float = 0.0, fair: float = 0.0,
                 higher_is_better: bool = True):
        super().__init__(parent, style="Tile.TLabelframe", padding=(12, 8))
        self._good   = good
        self._fair   = fair
        self._higher = higher_is_better

        ttk.Label(self, text=title, style="TileTitle.TLabel").pack(anchor="w")
        self._val = ttk.Label(self, text="—", style="TileValue.TLabel")
        self._val.pack(anchor="center", pady=(4, 0))
        ttk.Label(self, text=unit, style="TileUnit.TLabel").pack(anchor="center")
        if info:
            ttk.Label(self, text=info, style="TileInfo.TLabel",
                      wraplength=170).pack(anchor="w", pady=(6, 0))

    def update(self, value: Optional[float], override_color: str = ""):
        t = _t()
        if value is None:
            self._val.config(text="—", foreground=t.stale)
            return
        txt = (f"{value:,.1f}" if abs(value) >= 1000
               else f"{value:.2f}" if abs(value) >= 10
               else f"{value:.4f}" if abs(value) < 1
               else f"{value:.3f}")
        color = override_color or self._threshold_color(value)
        self._val.config(text=txt, foreground=color)

    def _threshold_color(self, v: float) -> str:
        t = _t()
        if self._good == 0 and self._fair == 0:
            return t.text
        if self._higher:
            return t.good if v >= self._good else (t.warn if v >= self._fair else t.alarm)
        return t.good if v <= self._good else (t.warn if v <= self._fair else t.alarm)


# ─────────────────────────────────────────────────────────────────────────────
# Main tab
# ─────────────────────────────────────────────────────────────────────────────

class HydroKPITab(ttk.Frame):
    """
    Hydro Plant KPI tab.

    Call ``update_view(values_dict)`` every UI tick (1 Hz) with the TOTAL
    or single-meter values dict.  The tab internally tracks run-hours by
    counting seconds when P_AC > min_operating threshold.
    """

    def __init__(self, parent, cfg: dict):
        super().__init__(parent, padding=8)
        self.cfg = cfg
        self._last_kpi: Dict[str, Any] = {}

        # --- Run-hours tracking ---
        # _run_seconds: seconds P_AC > threshold, accumulated today
        # _tracking_date: calendar date string when tracking started (for midnight reset)
        # _last_tick_ts: wall-clock time of last update_view call
        self._run_seconds: float = 0.0
        self._tracking_date: str = ""
        self._last_tick_ts: float = 0.0

        # --- RunHour register midnight baseline ---
        # If the meter exposes accumulated RunHour, track the value at session start
        # to compute "run hours since app started" as a minimum availability estimate.
        self._run_hour_baseline: Optional[float] = None

        self._build_scroll_shell()
        self._build_tiles()
        self._build_target_bar()
        self._build_config_panel()
        self._build_warnings_row()

    def _build_scroll_shell(self):
        self.columnconfigure(0, weight=1)
        self.rowconfigure(0, weight=1)
        self._canvas = tk.Canvas(self, highlightthickness=0, borderwidth=0)
        self._vbar = ttk.Scrollbar(self, orient="vertical", command=self._canvas.yview)
        self._canvas.configure(yscrollcommand=self._vbar.set)
        self._canvas.grid(row=0, column=0, sticky="nsew")
        self._vbar.grid(row=0, column=1, sticky="ns")
        self._content = ttk.Frame(self, padding=8)
        self._content.columnconfigure(0, weight=1)
        self._content_window = self._canvas.create_window((0, 0), window=self._content, anchor="nw")
        self._content.bind("<Configure>", self._on_content_configure)
        self._canvas.bind("<Configure>", self._on_canvas_configure)
        self._bind_mousewheel(self._canvas)
        self._bind_mousewheel(self._content)

    def _on_content_configure(self, _event=None):
        self._canvas.configure(scrollregion=self._canvas.bbox("all"))

    def _on_canvas_configure(self, event):
        self._canvas.itemconfigure(self._content_window, width=event.width)

    def _bind_mousewheel(self, widget):
        widget.bind("<Enter>", lambda _e: self._set_mousewheel_active(True), add="+")
        widget.bind("<Leave>", lambda _e: self._set_mousewheel_active(False), add="+")

    def _set_mousewheel_active(self, active: bool):
        if active:
            self.bind_all("<MouseWheel>", self._on_mousewheel, add="+")
            self.bind_all("<Button-4>", self._on_mousewheel_linux, add="+")
            self.bind_all("<Button-5>", self._on_mousewheel_linux, add="+")
        else:
            self.unbind_all("<MouseWheel>")
            self.unbind_all("<Button-4>")
            self.unbind_all("<Button-5>")

    def _on_mousewheel(self, event):
        delta = -1 if event.delta > 0 else 1
        self._canvas.yview_scroll(delta, "units")

    def _on_mousewheel_linux(self, event):
        delta = -1 if getattr(event, "num", 0) == 4 else 1
        self._canvas.yview_scroll(delta, "units")

    # ─────────────────────── KPI tiles ───────────────────────────────────────

    def _build_tiles(self):
        frm = ttk.LabelFrame(self._content, text="Today's Performance  (IEC 60041 / CBIP)")
        frm.grid(row=0, column=0, sticky="ew", padx=4, pady=(4, 2))
        for c in range(3):
            frm.columnconfigure(c, weight=1)

        self._tile_energy = _KpiTile(
            frm, "Today's Generation", "kWh",
            info="AC energy metered since midnight",
        )
        self._tile_energy.grid(row=0, column=0, padx=4, pady=4, sticky="nsew")

        self._tile_plf = _KpiTile(
            frm, "Plant Load Factor  (PLF)", "%",
            info="CBIP: E_AC / (P_rated × 24 h) × 100",
            good=50.0, fair=30.0,
        )
        self._tile_plf.grid(row=0, column=1, padx=4, pady=4, sticky="nsew")

        self._tile_avail = _KpiTile(
            frm, "Availability", "%",
            info="Hours online / hours elapsed × 100",
            good=90.0, fair=70.0,
        )
        self._tile_avail.grid(row=0, column=2, padx=4, pady=4, sticky="nsew")

        self._tile_eff = _KpiTile(
            frm, "Overall Efficiency  η", "%",
            info="IEC 60041: P_AC / (ρ·g·Q·H_net/1000) × 100",
            good=80.0, fair=65.0,
        )
        self._tile_eff.grid(row=1, column=0, padx=4, pady=4, sticky="nsew")

        self._tile_spec_e = _KpiTile(
            frm, "Specific Energy", "kWh / m³",
            info="E_AC / (Q × run_hrs × 3600 s)",
            good=0.0, fair=0.0,   # no universal benchmark; shown for trend
        )
        self._tile_spec_e.grid(row=1, column=1, padx=4, pady=4, sticky="nsew")

        self._tile_co2 = _KpiTile(
            frm, "CO₂ Avoided", "kg  today",
            info=f"E_AC × {CEA_CO2_KG_PER_KWH} kg/kWh  (CEA India FY2022-23)",
        )
        self._tile_co2.grid(row=1, column=2, padx=4, pady=4, sticky="nsew")

        self._eff_source_lbl = ttk.Label(frm, text="", style="TileInfo.TLabel")
        self._eff_source_lbl.grid(
            row=2, column=0, columnspan=3, sticky="w", padx=8, pady=(0, 6)
        )

    # ─────────────────────── Target progress bar ─────────────────────────────

    def _build_target_bar(self):
        frm = ttk.LabelFrame(self._content, text="  vs Design Target")
        frm.grid(row=1, column=0, sticky="ew", padx=4, pady=2)
        frm.columnconfigure(1, weight=1)

        ttk.Label(frm, text="Today:", style="FieldLabel.TLabel").grid(
            row=0, column=0, padx=(0, 6), pady=8, sticky="w")
        self._bar_var = tk.DoubleVar(value=0.0)
        ttk.Progressbar(
            frm, variable=self._bar_var, maximum=100.0,
            length=300, mode="determinate",
        ).grid(row=0, column=1, padx=4, pady=8, sticky="ew")

        self._pct_lbl = ttk.Label(frm, text="—", style="MonoBold.TLabel", width=8)
        self._pct_lbl.grid(row=0, column=2, padx=(6, 4))
        self._status_lbl = ttk.Label(frm, text="", style="SectionTitle.TLabel")
        self._status_lbl.grid(row=0, column=3, padx=(0, 8))

        self._detail_lbl = ttk.Label(frm, text="", style="Caption.TLabel")
        self._detail_lbl.grid(
            row=1, column=0, columnspan=4, sticky="w", padx=0, pady=(0, 6)
        )

    # ─────────────────────── Configuration panel ─────────────────────────────

    def _build_config_panel(self):
        outer = ttk.LabelFrame(self._content, text="  Plant Configuration  (IEC 60041 / CBIP)",
                               style="Form.TLabelframe")
        outer.grid(row=2, column=0, sticky="ew", padx=4, pady=2)

        # Row 0: capacity, turbine type
        r0 = ttk.Frame(outer)
        r0.pack(fill="x", padx=8, pady=(6, 2))

        ttk.Label(r0, text="Rated capacity (kW):").pack(side="left")
        self._ent_rated = ttk.Entry(r0, width=10)
        self._ent_rated.insert(0, str(self._hv("rated_capacity_kw", 0.0)))
        self._ent_rated.pack(side="left", padx=(4, 16))

        ttk.Label(r0, text="Turbine type:").pack(side="left")
        self._cmb_turbine = ttk.Combobox(
            r0, values=TURBINE_TYPES, state="readonly", width=12,
        )
        self._cmb_turbine.set(self._hv("turbine_type", "FRANCIS"))
        self._cmb_turbine.pack(side="left", padx=(4, 16))

        ttk.Label(r0, text="Design PLF (%):").pack(side="left")
        self._ent_plf = ttk.Entry(r0, width=8)
        self._ent_plf.insert(0, str(self._hv("design_plf_pct", 50.0)))
        self._ent_plf.pack(side="left", padx=(4, 0))
        ttk.Label(r0, text="  typical India: 35–65%", style="Dim.TLabel").pack(side="left")

        # Row 1: design head, design flow
        r1 = ttk.Frame(outer)
        r1.pack(fill="x", padx=8, pady=2)

        ttk.Label(r1, text="Design net head (m):").pack(side="left")
        self._ent_head_d = ttk.Entry(r1, width=10)
        self._ent_head_d.insert(0, str(self._hv("design_head_m", 0.0)))
        self._ent_head_d.pack(side="left", padx=(4, 16))

        ttk.Label(r1, text="Design flow Q (m³/s):").pack(side="left")
        self._ent_flow_d = ttk.Entry(r1, width=10)
        self._ent_flow_d.insert(0, str(self._hv("design_flow_m3_s", 0.0)))
        self._ent_flow_d.pack(side="left", padx=(4, 16))

        ttk.Label(r1, text="Annual target (kWh):").pack(side="left")
        self._ent_annual = ttk.Entry(r1, width=12)
        self._ent_annual.insert(0, str(self._hv("design_annual_kwh", 0.0)))
        self._ent_annual.pack(side="left", padx=(4, 0))
        ttk.Label(r1, text="  (0 = auto from PLF)", style="Dim.TLabel").pack(side="left")

        # Row 2: today's live inputs (flow, head)
        r2 = ttk.Frame(outer)
        r2.pack(fill="x", padx=8, pady=2)

        ttk.Label(r2, text="Today's avg Q (m³/s):").pack(side="left")
        self._ent_flow_live = ttk.Entry(r2, width=10)
        self._ent_flow_live.insert(0, str(self._hv("flow_m3_s", 0.0)))
        self._ent_flow_live.pack(side="left", padx=(4, 16))

        ttk.Label(r2, text="Today's avg H_net (m):").pack(side="left")
        self._ent_head_live = ttk.Entry(r2, width=10)
        self._ent_head_live.insert(0, str(self._hv("head_net_m", 0.0)))
        self._ent_head_live.pack(side="left", padx=(4, 16))

        ttk.Label(r2, text="Water density (kg/m³):").pack(side="left")
        self._ent_rho = ttk.Entry(r2, width=10)
        self._ent_rho.insert(0, str(self._hv("water_density_kg_m3", RHO_FRESH_WATER)))
        self._ent_rho.pack(side="left", padx=(4, 0))
        ttk.Label(r2, text="  1000 = fresh; 1005–1050 for silt-laden", style="Dim.TLabel").pack(side="left")

        # Row 3: min operating threshold, energy/power keys
        r3 = ttk.Frame(outer)
        r3.pack(fill="x", padx=8, pady=2)

        ttk.Label(r3, text="Min operating kW:").pack(side="left")
        self._ent_min_kw = ttk.Entry(r3, width=10)
        self._ent_min_kw.insert(0, str(self._hv("min_operating_kw", 0.0)))
        self._ent_min_kw.pack(side="left", padx=(4, 4))
        ttk.Label(r3, text="(0=auto 3%)", style="Dim.TLabel").pack(side="left", padx=(0, 16))

        ttk.Label(r3, text="Energy register:").pack(side="left")
        self._cmb_ekey = ttk.Combobox(
            r3, values=["Today_kWh", "Import_kWh", "Net_kWh"],
            state="readonly", width=14,
        )
        self._cmb_ekey.set(self._hv("energy_key", "Today_kWh"))
        self._cmb_ekey.pack(side="left", padx=(4, 16))

        ttk.Label(r3, text="Power register:").pack(side="left")
        self._cmb_pkey = ttk.Combobox(
            r3, values=["kW", "Total_kW"], state="readonly", width=12,
        )
        self._cmb_pkey.set(self._hv("power_key", "kW"))
        self._cmb_pkey.pack(side="left", padx=(4, 0))

        # Row 4: apply + design preview
        r4 = ttk.Frame(outer)
        r4.pack(fill="x", padx=8, pady=(4, 8))

        ttk.Button(r4, text="✔  Apply & Save",
                   command=self._apply_config, style="Primary.TButton").pack(side="left")
        self._preview_lbl = ttk.Label(r4, text="", style="Mono.TLabel")
        self._preview_lbl.pack(side="left", padx=(16, 0))
        self._refresh_preview()

    # ─────────────────────── Warnings row ────────────────────────────────────

    def _build_warnings_row(self):
        self._warn_lbl = ttk.Label(self._content, text="", style="Warn.TLabel", wraplength=800)
        self._warn_lbl.grid(row=3, column=0, sticky="w", padx=8, pady=(4, 6))

    # ─────────────────────── Update cycle ────────────────────────────────────

    def update_view(self, values: Optional[Dict[str, Any]]):
        """
        Called every UI tick (~1 Hz) with TOTAL/meter values dict.
        Maintains internal run-hours counter and feeds KPI engine.
        """
        config = HydroKPIEngine.cfg_to_config(self.cfg)

        # ------ Midnight reset ------
        from datetime import datetime
        today_str = datetime.now().strftime("%Y-%m-%d")
        if today_str != self._tracking_date:
            self._run_seconds = 0.0
            self._tracking_date = today_str
            self._run_hour_baseline = None
            self._last_tick_ts = 0.0

        # ------ Run-hours tracking ------
        now_ts = time.monotonic()
        if self._last_tick_ts > 0:
            dt = now_ts - self._last_tick_ts
        else:
            dt = 0.0
        self._last_tick_ts = now_ts

        current_kw = 0.0
        today_kwh  = 0.0
        if values:
            try:
                pv = values.get(config.power_key)
                current_kw = float(pv) if isinstance(pv, (int, float)) else 0.0
            except Exception:
                pass
            try:
                ev = values.get(config.energy_key)
                today_kwh = float(ev) if isinstance(ev, (int, float)) else 0.0
            except Exception:
                pass

        # Count seconds when unit is running (P > threshold)
        if current_kw >= config.min_operating_kw_effective and dt > 0:
            self._run_seconds += dt
        run_hours_today = self._run_seconds / 3600.0

        # Try to use RunHour register for better accuracy when available
        if values:
            try:
                rh_reg = values.get("RunHour")
                if rh_reg is not None:
                    rh_val = float(rh_reg)
                    if self._run_hour_baseline is None:
                        self._run_hour_baseline = rh_val
                    run_hours_today = max(run_hours_today,
                                         rh_val - self._run_hour_baseline)
            except Exception:
                pass

        # Hours elapsed today (up to current wall-clock time from midnight)
        from datetime import datetime as _dt
        now = _dt.now()
        hours_elapsed = now.hour + now.minute / 60.0 + now.second / 3600.0
        hours_elapsed = max(0.001, hours_elapsed)

        kpi = _ENGINE.compute(
            today_kwh=today_kwh,
            current_kw=current_kw,
            run_hours_today=run_hours_today,
            config=config,
            hours_elapsed=hours_elapsed,
        )
        self._last_kpi = kpi
        self._refresh_tiles(kpi)
        self._refresh_bar(kpi)
        self._refresh_warnings(kpi)

    # ─────────────────────── Refresh helpers ─────────────────────────────────

    def _refresh_tiles(self, kpi: dict):
        self._tile_energy.update(kpi.get("energy_kwh"))
        self._tile_plf.update(kpi.get("plf_pct"))
        self._tile_avail.update(kpi.get("availability_pct"))
        self._tile_co2.update(kpi.get("co2_avoided_kg"))

        # Specific energy — no colour benchmark (system-specific)
        se = kpi.get("specific_energy")
        self._tile_spec_e.update(se, override_color=_TEXT if se is not None else "")

        # Efficiency
        eff = kpi.get("efficiency_pct")
        src = kpi.get("efficiency_source", "none")
        if src == "none":
            self._tile_eff.update(None)
            self._eff_source_lbl.config(
                text="Efficiency: Enter today's flow Q and design head to enable IEC 60041 calculation",
                foreground=_MUTED,
            )
        else:
            color = "" if src == "measured_head_flow" else _AMBER
            self._tile_eff.update(eff, override_color=color)
            p_hyd = kpi.get("hydraulic_power_kw")
            src_txt = (
                f"η computed from measured H_net + flow (IEC 60041)   "
                f"P_hydraulic = {p_hyd:.1f} kW"
            ) if src == "measured_head_flow" else (
                f"η estimated using design H_net (not measured) — amber   "
                f"P_hydraulic = {p_hyd:.1f} kW"
            )
            self._eff_source_lbl.config(text=src_txt, foreground=_MUTED)

    def _refresh_bar(self, kpi: dict):
        vs     = kpi.get("vs_target_pct")
        status = kpi.get("vs_target_status", "UNCONFIGURED")
        color  = _STATUS_COLORS.get(status, _MUTED)
        target = kpi.get("target_daily_kwh", 0.0)
        actual = kpi.get("energy_kwh", 0.0)
        rated  = kpi.get("rated_capacity_kw", 0.0)
        plf_d  = kpi.get("plf_design_pct", 50.0)

        if vs is not None:
            self._bar_var.set(min(100.0, vs))
            self._pct_lbl.config(text=f"{vs:.1f}%", foreground=color)
            status_map = {
                "ON_TARGET":    "✓ On Target",
                "BELOW_TARGET": "⚠ Below Target",
                "POOR":         "✗ Poor",
            }
            self._status_lbl.config(
                text=status_map.get(status, status), foreground=color
            )
            self._detail_lbl.config(
                text=(
                    f"Actual: {actual:.1f} kWh   "
                    f"Target: {target:.1f} kWh/day   "
                    f"(P_rated × PLF_design/100 × 24 = "
                    f"{rated:.0f} kW × {plf_d:.0f}% × 24 h)"
                )
            )
        else:
            self._bar_var.set(0.0)
            self._pct_lbl.config(text="—", foreground=_MUTED)
            self._status_lbl.config(text="Configure plant", foreground=_MUTED)
            self._detail_lbl.config(text="Set rated_capacity_kw and design_plf_pct")

    def _refresh_warnings(self, kpi: dict):
        warns = kpi.get("warnings") or []
        self._warn_lbl.config(
            text="  ⚠  " + "   |   ".join(warns) if warns else ""
        )

    def _refresh_preview(self):
        try:
            config = HydroKPIEngine.cfg_to_config(self.cfg)
            daily  = config.design_daily_kwh
            annual = config.annual_target_effective
            p_hyd  = config.hydraulic_power_kw
            eta_d  = config.design_efficiency_pct
            parts  = []
            if daily > 0:
                parts.append(f"Design daily: {daily:.1f} kWh")
                parts.append(f"Design annual: {annual:,.0f} kWh")
            if p_hyd is not None:
                parts.append(f"P_hydraulic (design): {p_hyd:.1f} kW")
            if eta_d is not None:
                parts.append(f"Design η: {eta_d:.1f}%")
            self._preview_lbl.config(
                text="   ".join(parts) if parts else "Enter rated capacity to compute targets"
            )
        except Exception:
            pass

    # ─────────────────────── Config handler ──────────────────────────────────

    def _apply_config(self):
        hc = self.cfg.setdefault("hydro_plant", {})
        errors: list = []

        def _float(ent: ttk.Entry, key: str, lo: float, hi: float, label: str):
            try:
                v = float(ent.get().strip())
            except ValueError:
                errors.append(f"{label}: not a number")
                return
            if not (lo <= v <= hi):
                errors.append(f"{label}: must be {lo}–{hi} (got {v})")
                return
            hc[key] = v

        _float(self._ent_rated,     "rated_capacity_kw",    0.0,  500_000.0, "Rated capacity")
        _float(self._ent_plf,       "design_plf_pct",       0.1,  100.0,    "Design PLF")
        _float(self._ent_head_d,    "design_head_m",        0.0, 2_000.0,   "Design head")
        _float(self._ent_flow_d,    "design_flow_m3_s",     0.0, 10_000.0,  "Design flow")
        _float(self._ent_annual,    "design_annual_kwh",    0.0, 1e9,       "Annual target")
        _float(self._ent_flow_live, "flow_m3_s",            0.0, 10_000.0,  "Today's flow")
        _float(self._ent_head_live, "head_net_m",           0.0, 2_000.0,   "Today's head")
        _float(self._ent_rho,       "water_density_kg_m3",  990.0, 1_100.0, "Water density")
        _float(self._ent_min_kw,    "min_operating_kw",     0.0, 500_000.0, "Min operating kW")

        hc["turbine_type"] = self._cmb_turbine.get()
        hc["energy_key"]   = self._cmb_ekey.get()
        hc["power_key"]    = self._cmb_pkey.get()

        if errors:
            self._warn_lbl.config(
                text="Config errors: " + " | ".join(errors), foreground=_RED
            )
            return

        self._warn_lbl.config(text="", foreground=_AMBER)
        self._refresh_preview()
        logger.info("[HydroKPI] config saved: %s", hc)

    # ─────────────────────── Helper ──────────────────────────────────────────

    def _hv(self, key: str, default):
        return self.cfg.get("hydro_plant", {}).get(key, default)
