"""
Industrial Load KPI Dashboard Tab
===================================
Presents demand analysis, power factor, load factor, and power quality KPIs
computed by core.load_kpi.LoadKPIEngine.

Layout
------
  ┌─ KPI Tiles (2 rows × 3 cols) ──────────────────────────────────────────────┐
  │  Max Demand kVA  │  Load Factor %   │  Power Factor                        │
  │  Reactive Load   │  Power Quality   │  Billing Estimate                    │
  ├─ Demand Interval Bar ──────────────────────────────────────────────────────┤
  │  Rolling N-min average vs today's peak (visual demand profile)             │
  ├─ Plant / Tariff Configuration ─────────────────────────────────────────────┤
  │  Contract demand  Interval  PF thresholds  Tariff rates  Register keys     │
  └────────────────────────────────────────────────────────────────────────────┘

Standards referenced
--------------------
IEEE 1459:2010   Power quantity definitions (kW, kVA, kVAR, PF)
IEEE 519:2014    Harmonic control limits (THD_V < 5 %, THD_I < 8 % indicative)
BEE India        Load factor benchmarks (≥ 70 % excellent, 50–70 % good)
CERC/SERC        PF penalty < 0.85, incentive ≥ 0.95 (default; configurable)
CEA Metering     15 / 30-min demand interval
"""
from __future__ import annotations

import time
import tkinter as tk
from datetime import datetime
from tkinter import ttk
from typing import Any, Dict, Optional

from core.load_kpi import (
    LoadKPIEngine,
    LoadPlantConfig,
    CEA_CO2_KG_PER_KWH,
    LF_EXCELLENT_PCT,
    LF_GOOD_PCT,
    PF_INCENTIVE_DEFAULT,
    PF_PENALTY_DEFAULT,
    THD_V_LIMIT_PCT,
    THD_I_LIMIT_PCT,
)
from ui.styles import get_theme
from utils.logger import setup_logger

logger = setup_logger("load_kpi_view")


def _t():
    return get_theme()


def _pf_status_color():
    t = _t()
    return {"INCENTIVE": t.good, "NORMAL": t.text, "PENALTY": t.alarm}


def _lf_status_color():
    t = _t()
    return {"excellent": t.good, "good": t.warn, "poor": t.alarm, "unknown": t.stale}


def _muted():
    return _t().text_muted


def _text():
    return _t().text


def _good():
    return _t().good


def _alarm():
    return _t().alarm


# ─────────────────────────────────────────────────────────────────────────────
# Reusable KPI tile widget
# ─────────────────────────────────────────────────────────────────────────────

class _KpiTile(ttk.Frame):
    """
    Single KPI tile: title / large primary value / unit / secondary line.

    Threshold colouring (when good_threshold / fair_threshold are provided):
      higher_is_better=True  →  ≥ good = green, ≥ fair = amber, < fair = red
      higher_is_better=False →  ≤ good = green, ≤ fair = amber, > fair = red
      Both zero              →  always theme text colour (no colouring)
    """

    def __init__(self, parent, title: str, unit: str, info: str = "",
                 good_threshold: float = 0.0, fair_threshold: float = 0.0,
                 higher_is_better: bool = True):
        super().__init__(parent, style="Tile.TLabelframe", padding=(12, 8))
        self._good   = good_threshold
        self._fair   = fair_threshold
        self._higher = higher_is_better

        ttk.Label(self, text=title, style="TileTitle.TLabel").pack(anchor="w")
        self._val_lbl = ttk.Label(self, text="—", style="TileValue.TLabel")
        self._val_lbl.pack(anchor="center", pady=(4, 0))
        self._unit_lbl = ttk.Label(self, text=unit, style="TileUnit.TLabel")
        self._unit_lbl.pack(anchor="center")
        self._info_lbl = ttk.Label(self, text=info or "", style="TileInfo.TLabel", wraplength=190)
        self._info_lbl.pack(anchor="w", pady=(6, 0))

    def update_value(self, value: Optional[float],
                     override_color: str = "",
                     info: str = "") -> None:
        t = _t()
        if value is None:
            self._val_lbl.config(text="—", foreground=t.stale)
        else:
            txt = (f"{value:,.1f}" if abs(value) >= 1000
                   else f"{value:.2f}" if abs(value) >= 10
                   else f"{value:.3f}")
            color = override_color or self._color(value)
            self._val_lbl.config(text=txt, foreground=color)
        if info:
            self._info_lbl.config(text=info)

    def _color(self, v: float) -> str:
        t = _t()
        if self._good == 0.0 and self._fair == 0.0:
            return t.text
        if self._higher:
            return t.good if v >= self._good else (t.warn if v >= self._fair else t.alarm)
        else:
            return t.good if v <= self._good else (t.warn if v <= self._fair else t.alarm)


# ─────────────────────────────────────────────────────────────────────────────
# Main tab
# ─────────────────────────────────────────────────────────────────────────────

class LoadKPITab(ttk.Frame):
    """
    Industrial Load KPI tab.

    Call ``update_view(values_dict)`` on every UI tick with the TOTAL
    aggregator values dict (or a single-meter dict).

    The engine is stateful: MD peaks, PF averages, and the rolling demand
    window are accumulated across calls.  A midnight reset is performed
    automatically inside ``update_view()``.
    """

    def __init__(self, parent, cfg: dict):
        super().__init__(parent, padding=8)
        self.cfg = cfg
        self._last_kpi: Dict[str, Any] = {}

        # Stateful engine — created with current cfg; replaced on reconfigure
        self._engine = LoadKPIEngine(LoadKPIEngine.cfg_to_config(cfg))

        # Midnight-reset tracking
        self._tracking_date: str = ""

        self._build_scroll_shell()

        self._build_kpi_tiles()
        self._build_demand_bar()
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

    # ─────────────────────────── KPI tile grid ───────────────────────────────

    def _build_kpi_tiles(self):
        frm = ttk.LabelFrame(
            self._content,
            text="Today's Load Analysis  (IEEE 1459 · IEEE 519 · BEE India · CERC/SERC)",
        )
        frm.grid(row=0, column=0, sticky="ew", padx=4, pady=(4, 2))
        for c in range(3):
            frm.columnconfigure(c, weight=1)

        # ── Row 0 ──────────────────────────────────────────────────────────
        self._tile_md = _KpiTile(
            frm,
            title="Max Demand  (MD)",
            unit="kVA",
            info="Today's peak of 15/30-min rolling average  (CEA Metering Regs)",
            good_threshold=0.0,
            fair_threshold=0.0,
        )
        self._tile_md.grid(row=0, column=0, padx=4, pady=4, sticky="nsew")

        self._tile_lf = _KpiTile(
            frm,
            title="Load Factor",
            unit="%",
            info="LF = kWh / (MD_kW × elapsed h) × 100  (BEE India)  "
                 "≥70% excellent · 50–70% good · <50% poor",
            good_threshold=LF_EXCELLENT_PCT,
            fair_threshold=LF_GOOD_PCT,
        )
        self._tile_lf.grid(row=0, column=1, padx=4, pady=4, sticky="nsew")

        self._tile_pf = _KpiTile(
            frm,
            title="Power Factor",
            unit="",
            info="Displacement PF from meter  (IEEE 1459 §3)  "
                 "CERC/SERC: < 0.85 penalty · ≥ 0.95 incentive",
            good_threshold=PF_INCENTIVE_DEFAULT,
            fair_threshold=PF_PENALTY_DEFAULT,
        )
        self._tile_pf.grid(row=0, column=2, padx=4, pady=4, sticky="nsew")

        # ── Row 1 ──────────────────────────────────────────────────────────
        self._tile_kvar = _KpiTile(
            frm,
            title="Reactive Load",
            unit="kVAR",
            info="Q = sqrt(S²−P²)  (IEEE 1459 §3.1)",
            good_threshold=0.0,
            fair_threshold=0.0,
        )
        self._tile_kvar.grid(row=1, column=0, padx=4, pady=4, sticky="nsew")

        self._tile_thd = _KpiTile(
            frm,
            title="Voltage THD",
            unit="%",
            info=f"Avg of THD_V1/V2/V3  (IEEE 519:2014 limit: {THD_V_LIMIT_PCT:.0f}%)",
            good_threshold=THD_V_LIMIT_PCT,
            fair_threshold=THD_V_LIMIT_PCT * 1.5,
            higher_is_better=False,
        )
        self._tile_thd.grid(row=1, column=1, padx=4, pady=4, sticky="nsew")

        self._tile_billing = _KpiTile(
            frm,
            title="Billing Estimate",
            unit="LKR  today",
            info="Demand + energy charges (configure tariff rates to activate)",
            good_threshold=0.0,
            fair_threshold=0.0,
        )
        self._tile_billing.grid(row=1, column=2, padx=4, pady=4, sticky="nsew")

        self._pf_detail_lbl = ttk.Label(frm, text="", style="TileInfo.TLabel")
        self._pf_detail_lbl.grid(
            row=2, column=0, columnspan=3, sticky="w", padx=8, pady=(0, 6),
        )

    # ─────────────────────────── Demand bar ──────────────────────────────────

    def _build_demand_bar(self):
        frm = ttk.LabelFrame(self._content, text="  Rolling Demand Interval")
        frm.grid(row=1, column=0, sticky="ew", padx=4, pady=2)
        frm.columnconfigure(1, weight=1)

        self._di_label_var = tk.StringVar(value="Current interval avg:")
        ttk.Label(frm, textvariable=self._di_label_var,
                  style="FieldLabel.TLabel").grid(row=0, column=0, padx=(0, 6), pady=8, sticky="w")

        self._di_bar_var = tk.DoubleVar(value=0.0)
        self._di_bar = ttk.Progressbar(
            frm, variable=self._di_bar_var, maximum=100.0,
            length=300, mode="determinate",
        )
        self._di_bar.grid(row=0, column=1, padx=4, pady=8, sticky="ew")

        self._di_pct_lbl = ttk.Label(frm, text="—", style="MonoBold.TLabel", width=8)
        self._di_pct_lbl.grid(row=0, column=2, padx=(6, 4))

        self._di_detail_lbl = ttk.Label(frm, text="", style="Caption.TLabel")
        self._di_detail_lbl.grid(
            row=1, column=0, columnspan=3, sticky="w", padx=0, pady=(0, 6),
        )

    # ─────────────────────────── Config panel ────────────────────────────────

    def _build_config_panel(self):
        outer = ttk.LabelFrame(self._content, text="  Plant / Tariff Configuration  (IEEE 1459 · CERC/SERC · CEA)",
                               style="Form.TLabelframe")
        outer.grid(row=2, column=0, sticky="ew", padx=4, pady=2)

        # Row 0: demand settings
        r0 = ttk.Frame(outer)
        r0.pack(fill="x", padx=8, pady=(6, 2))

        ttk.Label(r0, text="Contract Demand (kVA):").pack(side="left")
        self._ent_cd = ttk.Entry(r0, width=10)
        self._ent_cd.pack(side="left", padx=(4, 16))
        self._ent_cd.insert(0, str(self._lp("contract_demand_kva", 0.0)))

        ttk.Label(r0, text="Demand Interval:").pack(side="left")
        self._cmb_di = ttk.Combobox(r0, values=["15", "30"], state="readonly", width=5)
        self._cmb_di.set(str(self._lp("demand_interval_min", 15)))
        self._cmb_di.pack(side="left", padx=(4, 4))
        ttk.Label(r0, text="min  (CEA Metering)", foreground=_muted(),
                  font=("Segoe UI", 8)).pack(side="left", padx=(0, 16))

        # Row 1: PF thresholds
        r1 = ttk.Frame(outer)
        r1.pack(fill="x", padx=8, pady=2)

        ttk.Label(r1, text="PF Penalty below:").pack(side="left")
        self._ent_pf_pen = ttk.Entry(r1, width=8)
        self._ent_pf_pen.pack(side="left", padx=(4, 4))
        self._ent_pf_pen.insert(0, str(self._lp("pf_penalty_threshold", PF_PENALTY_DEFAULT)))
        ttk.Label(r1, text="  (CERC/SERC default 0.85)", foreground=_muted(),
                  font=("Segoe UI", 8)).pack(side="left", padx=(0, 16))

        ttk.Label(r1, text="PF Incentive at/above:").pack(side="left")
        self._ent_pf_inc = ttk.Entry(r1, width=8)
        self._ent_pf_inc.pack(side="left", padx=(4, 4))
        self._ent_pf_inc.insert(0, str(self._lp("pf_incentive_threshold", PF_INCENTIVE_DEFAULT)))
        ttk.Label(r1, text="  CERC/SERC default 0.95", style="Dim.TLabel").pack(side="left")

        # Row 2: tariff rates
        r2 = ttk.Frame(outer)
        r2.pack(fill="x", padx=8, pady=2)

        ttk.Label(r2, text="Demand rate (LKR/kVA/mo):").pack(side="left")
        self._ent_dr = ttk.Entry(r2, width=10)
        self._ent_dr.pack(side="left", padx=(4, 16))
        self._ent_dr.insert(0, str(self._lp("tariff_demand_rate", 0.0)))

        ttk.Label(r2, text="Energy rate (LKR/kWh):").pack(side="left")
        self._ent_er = ttk.Entry(r2, width=10)
        self._ent_er.pack(side="left", padx=(4, 16))
        self._ent_er.insert(0, str(self._lp("tariff_energy_rate", 0.0)))

        ttk.Label(r2, text="  0 = disable estimate", style="Dim.TLabel").pack(side="left")

        # Row 3: register keys
        r3 = ttk.Frame(outer)
        r3.pack(fill="x", padx=8, pady=2)

        ttk.Label(r3, text="kW register:").pack(side="left")
        self._cmb_kw = ttk.Combobox(r3, values=["kW", "Total_kW"],
                                    state="readonly", width=12)
        self._cmb_kw.set(self._lp("power_key", "kW"))
        self._cmb_kw.pack(side="left", padx=(4, 16))

        ttk.Label(r3, text="kVA register:").pack(side="left")
        self._cmb_kva = ttk.Combobox(r3, values=["kVA", "Total_kVA"],
                                     state="readonly", width=12)
        self._cmb_kva.set(self._lp("apparent_power_key", "kVA"))
        self._cmb_kva.pack(side="left", padx=(4, 16))

        ttk.Label(r3, text="PF register:").pack(side="left")
        self._cmb_pf = ttk.Combobox(r3, values=["PF", "PF_avg"],
                                    state="readonly", width=10)
        self._cmb_pf.set(self._lp("pf_key", "PF"))
        self._cmb_pf.pack(side="left", padx=(4, 16))

        ttk.Label(r3, text="Energy register:").pack(side="left")
        self._cmb_ek = ttk.Combobox(
            r3, values=["Today_kWh", "Import_kWh", "Net_kWh", "Lifetime_kWh"],
            state="readonly", width=14,
        )
        self._cmb_ek.set(self._lp("energy_key", "Today_kWh"))
        self._cmb_ek.pack(side="left", padx=(4, 0))

        # Row 4: TOU / evening peak schedule
        r4 = ttk.Frame(outer)
        r4.pack(fill="x", padx=8, pady=2)

        self._var_tou = tk.BooleanVar(value=bool(self._lp("tou_enabled", False)))
        ttk.Checkbutton(
            r4, text="Enable TOU / Evening Peak (CEB)", variable=self._var_tou,
            command=self._on_tou_toggle,
        ).pack(side="left")

        ttk.Label(r4, text="  Peak window:", style="FieldLabel.TLabel").pack(side="left")
        self._ent_tou_start = ttk.Entry(r4, width=7)
        self._ent_tou_start.insert(0, str(self._lp("tou_peak_start", "18:30")))
        self._ent_tou_start.pack(side="left", padx=(4, 2))
        ttk.Label(r4, text="–", style="FieldLabel.TLabel").pack(side="left")
        self._ent_tou_end = ttk.Entry(r4, width=7)
        self._ent_tou_end.insert(0, str(self._lp("tou_peak_end", "22:30")))
        self._ent_tou_end.pack(side="left", padx=(2, 4))
        ttk.Label(r4, text="(HH:MM, 24-h)", foreground=_muted(),
                  font=("Segoe UI", 8)).pack(side="left")

        # Row 5: TOU rates
        self._r5 = ttk.Frame(outer)
        self._r5.pack(fill="x", padx=8, pady=2)

        ttk.Label(self._r5, text="Peak rate (LKR/kWh):").pack(side="left")
        self._ent_tou_pr = ttk.Entry(self._r5, width=10)
        self._ent_tou_pr.insert(0, str(self._lp("tou_peak_rate_lkr_kwh", 0.0)))
        self._ent_tou_pr.pack(side="left", padx=(4, 16))

        ttk.Label(self._r5, text="Off-peak rate (LKR/kWh):").pack(side="left")
        self._ent_tou_opr = ttk.Entry(self._r5, width=10)
        self._ent_tou_opr.insert(0, str(self._lp("tou_offpeak_rate_lkr_kwh", 0.0)))
        self._ent_tou_opr.pack(side="left", padx=(4, 16))

        ttk.Label(self._r5, text="Peak MD rate (LKR/kVA/mo):").pack(side="left")
        self._ent_tou_pdr = ttk.Entry(self._r5, width=10)
        self._ent_tou_pdr.insert(0, str(self._lp("tou_peak_demand_rate", 0.0)))
        self._ent_tou_pdr.pack(side="left", padx=(4, 0))

        # Apply initial TOU visibility
        self._on_tou_toggle()

        # Row 6: TOU live status label (updated by update_view)
        self._tou_status_lbl = ttk.Label(outer, text="", style="Caption.TLabel")
        self._tou_status_lbl.pack(anchor="w", padx=8, pady=(0, 4))

        # Row 7: save button + validation label
        r7 = ttk.Frame(outer)
        r7.pack(fill="x", padx=8, pady=(8, 4))

        ttk.Button(r7, text="✔  Apply Configuration",
                   command=self._apply_config, style="Primary.TButton").pack(side="left")
        self._cfg_status_lbl = ttk.Label(r7, text="", style="Good.TLabel")
        self._cfg_status_lbl.pack(side="left", padx=(12, 0))

    # ─────────────────────────── Warnings row ────────────────────────────────

    def _build_warnings_row(self):
        self._warn_lbl = ttk.Label(self._content, text="", style="Warn.TLabel", wraplength=900)
        self._warn_lbl.grid(row=3, column=0, sticky="w", padx=8, pady=(4, 6))

    # ─────────────────────────── update_view ─────────────────────────────────

    def update_view(self, values: Optional[Dict[str, Any]] = None) -> None:
        """
        Refresh all KPI widgets.

        Parameters
        ----------
        values : dict or None
            Latest TOTAL aggregator (or single-meter) values dict.
            Pass None when comms are lost; tiles will show "—".
        """
        # ── Midnight reset ────────────────────────────────────────────────────
        today_str = datetime.now().strftime("%Y-%m-%d")
        if today_str != self._tracking_date:
            self._engine.reset_daily()
            self._tracking_date = today_str

        # ── Run engine ────────────────────────────────────────────────────────
        if not values:
            # No data — clear all tiles but leave config panel intact
            for tile in (self._tile_md, self._tile_lf, self._tile_pf,
                         self._tile_kvar, self._tile_thd, self._tile_billing):
                tile.update_value(None)
            self._pf_detail_lbl.config(text="No data")
            self._di_bar_var.set(0.0)
            self._di_pct_lbl.config(text="—")
            self._di_detail_lbl.config(text="")
            self._warn_lbl.config(text="")
            return

        try:
            kpi = self._engine.update(values)
        except Exception as exc:
            logger.exception("LoadKPIEngine.update failed: %s", exc)
            return

        self._last_kpi = kpi

        # ── Tile 1: Max Demand ────────────────────────────────────────────────
        md_kva = kpi.get("md_kva_today")
        md_util = kpi.get("md_utilisation_pct")
        md_info = ""
        t = _t()
        if md_util is not None:
            contract = kpi.get("contract_demand_kva", 0.0)
            md_color = (t.alarm if md_util > 100.0 else t.warn if md_util > 85.0 else t.good)
            md_info = f"MD Utilisation: {md_util:.1f}%  (contract {contract:.0f} kVA)"
        else:
            md_color = t.text
            md_info = f"kW peak: {kpi.get('md_kw_today', 0.0):.1f} kW  (contract demand not set)"
        self._tile_md.update_value(md_kva, override_color=md_color, info=md_info)

        # ── Tile 2: Load Factor ───────────────────────────────────────────────
        lf = kpi.get("load_factor_pct")
        lf_status = kpi.get("load_factor_status", "unknown")
        lf_color = _lf_status_color().get(lf_status, t.stale)
        lf_info = (f"Status: {lf_status.upper()}"
                   if lf_status not in ("unknown", "") else
                   "Need MD_kW > 0 to compute")
        self._tile_lf.update_value(lf, override_color=lf_color, info=lf_info)

        # ── Tile 3: Power Factor ──────────────────────────────────────────────
        pf_now    = kpi.get("pf_now")
        pf_status = kpi.get("pf_status", "NORMAL")
        pf_color  = _pf_status_color().get(pf_status, t.text)
        pf_avg    = kpi.get("pf_avg_today")
        pf_min    = kpi.get("pf_min_today")
        pf_pen    = kpi.get("pf_penalty_threshold", PF_PENALTY_DEFAULT)
        pf_inc    = kpi.get("pf_incentive_threshold", PF_INCENTIVE_DEFAULT)
        pf_info   = f"Status: {pf_status}"
        self._tile_pf.update_value(pf_now, override_color=pf_color, info=pf_info)

        detail_parts = []
        if pf_avg is not None:
            detail_parts.append(f"Avg today: {pf_avg:.3f}")
        if pf_min is not None:
            detail_parts.append(f"Min today: {pf_min:.3f}")
        detail_parts.append(f"Penalty < {pf_pen:.2f}  |  Incentive ≥ {pf_inc:.2f}")
        self._pf_detail_lbl.config(text="   ".join(detail_parts))

        # ── Tile 4: Reactive Load ─────────────────────────────────────────────
        kvar_now = kpi.get("kvar_now")
        cap_rec  = kpi.get("capacitor_kvar_rec")
        if cap_rec is not None and cap_rec > 0.5:
            kvar_info = f"Add ~{cap_rec:.1f} kVAR capacitor bank to reach PF {pf_inc:.2f}"
            kvar_color = t.warn
        else:
            kvar_info = "Reactive compensation not required" if pf_now else ""
            kvar_color = t.text
        self._tile_kvar.update_value(kvar_now, override_color=kvar_color, info=kvar_info)

        # ── Tile 5: Voltage THD ───────────────────────────────────────────────
        thd_v = kpi.get("thd_v_avg_pct")
        thd_i = kpi.get("thd_i_avg_pct")
        if thd_v is not None:
            thd_lim = kpi.get("thd_v_limit_pct", THD_V_LIMIT_PCT)
            thd_color = t.alarm if thd_v > thd_lim else t.good
            thd_info = (
                f"THD_I: {thd_i:.1f}%  (limit {kpi.get('thd_i_limit_pct',THD_I_LIMIT_PCT):.0f}%)"
                if thd_i is not None
                else f"Limit {thd_lim:.0f}% (IEEE 519)  —  THD_I not available"
            )
        else:
            thd_color = t.stale
            thd_info = "THD registers not available (THD_V1/V2/V3)"
        self._tile_thd.update_value(thd_v, override_color=thd_color, info=thd_info)

        # ── Tile 6: Billing Estimate ──────────────────────────────────────────
        dem_chg  = kpi.get("demand_charge_est")
        enrg_chg = kpi.get("energy_charge_est")
        tou_pk_chg  = kpi.get("tou_peak_charge")
        tou_opk_chg = kpi.get("tou_offpeak_charge")
        tou_pd_chg  = kpi.get("tou_peak_demand_charge")
        tou_enabled = kpi.get("tou_enabled", False)

        total_est: Optional[float] = None
        billing_info = ""

        if tou_enabled:
            parts = [c for c in (dem_chg, tou_pk_chg, tou_opk_chg, tou_pd_chg)
                     if c is not None]
            if parts:
                total_est = round(sum(parts), 2)
                lines = []
                if dem_chg   is not None: lines.append(f"Demand: LKR {dem_chg:,.0f}")
                if tou_pk_chg  is not None: lines.append(f"Peak energy: LKR {tou_pk_chg:,.0f}")
                if tou_opk_chg is not None: lines.append(f"Off-peak: LKR {tou_opk_chg:,.0f}")
                if tou_pd_chg  is not None: lines.append(f"Peak MD charge: LKR {tou_pd_chg:,.0f}")
                billing_info = "  |  ".join(lines)
            else:
                billing_info = "Configure TOU rates to enable billing estimate"
        else:
            if dem_chg is not None or enrg_chg is not None:
                total_est = (dem_chg or 0.0) + (enrg_chg or 0.0)
                billing_info = (
                    f"Demand: LKR {dem_chg:,.2f}  Energy: LKR {enrg_chg:,.2f}"
                    if dem_chg is not None and enrg_chg is not None
                    else (f"Demand: LKR {dem_chg:,.2f}" if dem_chg is not None
                          else f"Energy: LKR {enrg_chg:,.2f}")
                )
            else:
                billing_info = "Configure tariff rates to enable billing estimate"
        self._tile_billing.update_value(total_est, info=billing_info)

        # ── TOU live status label ─────────────────────────────────────────────
        try:
            if tou_enabled:
                in_peak  = kpi.get("tou_in_peak", False)
                pk_start = kpi.get("tou_peak_start", "")
                pk_end   = kpi.get("tou_peak_end", "")
                pk_kwh   = kpi.get("tou_peak_kwh")
                op_kwh   = kpi.get("tou_offpeak_kwh")
                status   = "🔴 PEAK WINDOW active" if in_peak else "🟢 Off-peak"
                split_txt = ""
                if pk_kwh is not None and op_kwh is not None:
                    split_txt = (f"   Peak: {pk_kwh:.1f} kWh  "
                                 f"Off-peak: {op_kwh:.1f} kWh  "
                                 f"(estimated split)")
                self._tou_status_lbl.config(
                    text=f"TOU {pk_start}–{pk_end}  ·  {status}{split_txt}",
                    foreground=t.alarm if in_peak else t.good,
                )
            else:
                self._tou_status_lbl.config(text="TOU disabled", foreground=_muted())
        except Exception:
            pass

        # ── Demand interval bar ───────────────────────────────────────────────
        di_kva = kpi.get("demand_interval_kva", 0.0) or 0.0
        di_min = kpi.get("demand_interval_min", 15)
        md_now = kpi.get("md_kva_today", 0.0) or 0.0
        self._di_label_var.set(f"Current {di_min}-min avg:")

        if md_now > 0.0:
            pct = min(100.0, (di_kva / md_now) * 100.0)
            self._di_bar_var.set(pct)
            self._di_pct_lbl.config(
                text=f"{pct:.1f}%",
                foreground=(t.alarm if pct > 85.0 else t.warn if pct > 70.0 else t.good),
            )
            self._di_detail_lbl.config(
                text=f"Interval avg: {di_kva:.1f} kVA   Today's MD peak: {md_now:.1f} kVA"
                     + (f"   MD util: {md_util:.1f}%" if md_util is not None else ""),
            )
        elif kpi.get("contract_demand_kva", 0.0) > 0.0:
            contract = kpi["contract_demand_kva"]
            pct = min(100.0, (di_kva / contract) * 100.0)
            self._di_bar_var.set(pct)
            self._di_pct_lbl.config(
                text=f"{pct:.1f}%",
                foreground=(t.alarm if pct > 85.0 else t.warn if pct > 70.0 else t.good),
            )
            self._di_detail_lbl.config(
                text=f"Interval avg: {di_kva:.1f} kVA vs contract {contract:.0f} kVA",
            )
        else:
            self._di_bar_var.set(0.0)
            self._di_pct_lbl.config(text="—", foreground=t.stale)
            self._di_detail_lbl.config(text=f"Interval avg: {di_kva:.1f} kVA")

        # ── Warnings ─────────────────────────────────────────────────────────
        warns = kpi.get("warnings") or []
        self._warn_lbl.config(text="  ⚠  " + "   |   ".join(warns) if warns else "")

    # ─────────────────────────── Config handlers ─────────────────────────────

    def _on_tou_toggle(self) -> None:
        """Show/hide TOU rate row based on checkbox state."""
        try:
            if self._var_tou.get():
                self._r5.pack(fill="x", padx=8, pady=2)
            else:
                self._r5.pack_forget()
        except Exception:
            pass

    def _apply_config(self) -> None:
        """Validate fields, save to cfg dict, rebuild engine."""
        errors = []

        def _f(widget, name: str, lo: float = 0.0, hi: float = 1e9) -> Optional[float]:
            try:
                v = float(widget.get().strip())
                if not (lo <= v <= hi):
                    raise ValueError(f"out of range [{lo}, {hi}]")
                return v
            except (ValueError, AttributeError) as exc:
                errors.append(f"{name}: {exc}")
                return None

        contract  = _f(self._ent_cd,     "Contract demand",   0.0, 100_000.0)
        pf_pen    = _f(self._ent_pf_pen, "PF penalty",        0.5, 0.99)
        pf_inc    = _f(self._ent_pf_inc, "PF incentive",      0.5, 1.00)
        dem_rate  = _f(self._ent_dr,     "Demand rate",        0.0, 1e6)
        enrg_rate = _f(self._ent_er,     "Energy rate",        0.0, 1e6)

        try:
            di_min = int(self._cmb_di.get())
            if di_min not in (15, 30):
                raise ValueError("must be 15 or 30")
        except (ValueError, AttributeError) as exc:
            errors.append(f"Demand interval: {exc}")
            di_min = None

        if pf_pen is not None and pf_inc is not None and pf_inc <= pf_pen:
            errors.append("PF incentive threshold must be > penalty threshold")

        # TOU fields
        import re as _re
        _hhmm_pat = _re.compile(r"^\d{1,2}:\d{2}$")
        tou_on = bool(self._var_tou.get())
        tou_start = self._ent_tou_start.get().strip()
        tou_end   = self._ent_tou_end.get().strip()
        tou_pr  = _f(self._ent_tou_pr,  "TOU peak rate",        0.0, 1e6) if tou_on else 0.0
        tou_opr = _f(self._ent_tou_opr, "TOU off-peak rate",    0.0, 1e6) if tou_on else 0.0
        tou_pdr = _f(self._ent_tou_pdr, "TOU peak demand rate", 0.0, 1e6) if tou_on else 0.0
        if tou_on:
            if not _hhmm_pat.match(tou_start):
                errors.append(f"TOU peak start invalid (expected HH:MM): {tou_start!r}")
            if not _hhmm_pat.match(tou_end):
                errors.append(f"TOU peak end invalid (expected HH:MM): {tou_end!r}")

        if errors:
            self._cfg_status_lbl.config(
                text="Errors: " + "; ".join(errors), foreground=_alarm(),
            )
            return

        # Persist to cfg dict
        lp = self.cfg.setdefault("load_plant", {})
        lp["contract_demand_kva"]    = contract
        lp["demand_interval_min"]    = di_min
        lp["pf_penalty_threshold"]   = pf_pen
        lp["pf_incentive_threshold"] = pf_inc
        lp["tariff_demand_rate"]     = dem_rate
        lp["tariff_energy_rate"]     = enrg_rate
        lp["power_key"]              = self._cmb_kw.get()
        lp["apparent_power_key"]     = self._cmb_kva.get()
        lp["pf_key"]                 = self._cmb_pf.get()
        lp["energy_key"]             = self._cmb_ek.get()
        # TOU
        lp["tou_enabled"]              = tou_on
        lp["tou_peak_start"]           = tou_start
        lp["tou_peak_end"]             = tou_end
        lp["tou_peak_rate_lkr_kwh"]    = tou_pr  or 0.0
        lp["tou_offpeak_rate_lkr_kwh"] = tou_opr or 0.0
        lp["tou_peak_demand_rate"]     = tou_pdr or 0.0

        # Rebuild engine with new config (preserves daily accumulators)
        new_cfg = LoadKPIEngine.cfg_to_config(self.cfg)
        self._engine.reconfigure(new_cfg)

        # Update tile thresholds to match new PF settings
        self._tile_pf._good = pf_inc
        self._tile_pf._fair = pf_pen

        # Save via config store if available
        try:
            from config.store import save_config
            save_config(self.cfg)
        except Exception:
            pass

        self._cfg_status_lbl.config(
            text=f"Saved  {datetime.now().strftime('%H:%M:%S')}",
            foreground=_good(),
        )
        logger.info("Load KPI config updated: contract=%.1f kVA  interval=%d min  "
                    "pf_pen=%.2f  pf_inc=%.2f",
                    contract, di_min, pf_pen, pf_inc)

    # ─────────────────────────── Helpers ─────────────────────────────────────

    def _lp(self, key: str, default):
        """Read a value from cfg['load_plant'] with a fallback default."""
        return (self.cfg.get("load_plant") or {}).get(key, default)
