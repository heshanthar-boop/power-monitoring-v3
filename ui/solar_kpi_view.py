"""
Solar Plant KPI Dashboard Tab
==============================
Presents IEC 61724-1 performance indicators computed by core.solar_kpi.

Layout
------
  ┌─ KPI Tiles (2 rows × 3 cols) ──────────────────────────────────┐
  │  Today kWh  │  Specific Yield  │  CUF        │
  │  PR         │  Loading Now     │  CO₂ Avoided │
  ├─ Performance vs Target ────────────────────────────────────────┤
  │  Progress bar  +  status badge  +  design target line          │
  ├─ Plant Configuration ──────────────────────────────────────────┤
  │  DC kWp  AC kW  PSH  PR_design  CO2 factor  Irradiance entry  │
  └────────────────────────────────────────────────────────────────┘
"""
from __future__ import annotations

import tkinter as tk
from tkinter import ttk
from typing import Any, Dict, Optional

from core.solar_kpi import (
    SolarKPIEngine,
    SolarPlantConfig,
    CEA_CO2_KG_PER_KWH,
    PR_SANITY_CEILING,
)
from ui.styles import get_theme
from utils.logger import setup_logger

logger = setup_logger("solar_kpi_view")


def _t():
    """Shortcut — returns the active Theme at call time."""
    return get_theme()


def _status_colors():
    t = _t()
    return {
        "ON_TARGET":    t.good,
        "BELOW_TARGET": t.warn,
        "POOR":         t.alarm,
        "UNCONFIGURED": t.stale,
    }

# ── Engineering benchmarks (informational, not enforced) ──────────────────────
# Source: IRENA Renewable Power Generation Costs 2023, IEA PVPS reports
_BENCHMARKS = {
    "specific_yield": {"good": 4.5, "fair": 3.5, "unit": "kWh/kWp"},
    "cuf_24h_pct":    {"good": 20.0, "fair": 15.0, "unit": "%"},
    "pr":             {"good": 78.0, "fair": 70.0, "unit": "%"},
}

_ENGINE = SolarKPIEngine()


# ─────────────────────────────────────────────────────────────────────────────
# KPI tile widget
# ─────────────────────────────────────────────────────────────────────────────

class _KpiTile(ttk.Frame):
    """
    One KPI tile: title / large value / unit / benchmark indicator / info line.

    Benchmark colouring:
      ≥ good threshold  →  green
      ≥ fair threshold  →  amber
      < fair threshold  →  red
      N/A / unconfigured → muted grey
    """

    def __init__(self, parent, title: str, unit: str, info: str = "",
                 good_threshold: float = 0.0, fair_threshold: float = 0.0,
                 higher_is_better: bool = True):
        super().__init__(parent, style="Tile.TLabelframe", padding=(12, 8))
        self._good = good_threshold
        self._fair = fair_threshold
        self._higher = higher_is_better

        ttk.Label(self, text=title, style="TileTitle.TLabel").pack(anchor="w")
        self._val_lbl = ttk.Label(self, text="—", style="TileValue.TLabel")
        self._val_lbl.pack(anchor="center", pady=(4, 0))
        self._unit_lbl = ttk.Label(self, text=unit, style="TileUnit.TLabel")
        self._unit_lbl.pack(anchor="center")
        if info:
            ttk.Label(self, text=info, style="TileInfo.TLabel",
                      wraplength=170).pack(anchor="w", pady=(6, 0))

    def update_value(self, value: Optional[float], override_color: str = ""):
        t = _t()
        if value is None:
            self._val_lbl.config(text="—", foreground=t.stale)
            return
        if abs(value) >= 1000:
            txt = f"{value:,.1f}"
        elif abs(value) >= 10:
            txt = f"{value:.2f}"
        else:
            txt = f"{value:.3f}"
        color = override_color or self._color(value)
        self._val_lbl.config(text=txt, foreground=color)

    def _color(self, v: float) -> str:
        t = _t()
        if self._good == 0 and self._fair == 0:
            return t.text
        if self._higher:
            return t.good if v >= self._good else (t.warn if v >= self._fair else t.alarm)
        else:
            return t.good if v <= self._good else (t.warn if v <= self._fair else t.alarm)


# ─────────────────────────────────────────────────────────────────────────────
# Main tab
# ─────────────────────────────────────────────────────────────────────────────

class SolarKPITab(ttk.Frame):
    """
    Solar Plant KPI tab.

    Call ``update_view(values_dict)`` every UI tick with the TOTAL aggregator
    or a single-meter values dict.  The ``values_dict`` must contain at least
    the keys matching ``config.energy_key`` and ``config.power_key``.
    """

    def __init__(self, parent, cfg: dict):
        super().__init__(parent, padding=8)
        self.cfg = cfg
        self._last_kpi: Dict[str, Any] = {}

        self._build_scroll_shell()

        self._build_kpi_tiles()
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

    # ─────────────────────────── KPI tile grid ───────────────────────────────

    def _build_kpi_tiles(self):
        frm = ttk.LabelFrame(self._content, text="Today's Performance  (IEC 61724-1)")
        frm.grid(row=0, column=0, sticky="ew", padx=4, pady=(4, 2))
        for c in range(3):
            frm.columnconfigure(c, weight=1)

        self._tile_energy = _KpiTile(
            frm,
            title="Today's Generation",
            unit="kWh",
            info="AC energy metered since midnight",
        )
        self._tile_energy.grid(row=0, column=0, padx=4, pady=4, sticky="nsew")

        self._tile_yf = _KpiTile(
            frm,
            title="Specific Yield  Y_F",
            unit="kWh / kWp",
            info="IEC 61724 Final Yield = E_AC / P_dc",
            good_threshold=4.5,
            fair_threshold=3.5,
        )
        self._tile_yf.grid(row=0, column=1, padx=4, pady=4, sticky="nsew")

        self._tile_cuf = _KpiTile(
            frm,
            title="CUF  (24 h)",
            unit="%",
            info="Capacity Utilisation Factor = E_AC/(P_dc×24h) — MNRE definition",
            good_threshold=20.0,
            fair_threshold=15.0,
        )
        self._tile_cuf.grid(row=0, column=2, padx=4, pady=4, sticky="nsew")

        self._tile_pr = _KpiTile(
            frm,
            title="Performance Ratio  PR",
            unit="%",
            info="IEC 61724 PR = Y_F / Y_R  (measured)  or  Y_F / PSH_design (estimated)",
            good_threshold=78.0,
            fair_threshold=70.0,
        )
        self._tile_pr.grid(row=1, column=0, padx=4, pady=4, sticky="nsew")

        self._tile_load = _KpiTile(
            frm,
            title="Loading Now",
            unit="%",
            info="Instantaneous P_AC / P_dc_rated",
            good_threshold=60.0,
            fair_threshold=30.0,
        )
        self._tile_load.grid(row=1, column=1, padx=4, pady=4, sticky="nsew")

        self._tile_co2 = _KpiTile(
            frm,
            title="CO₂ Avoided",
            unit="kg  today",
            info=f"E_AC × {CEA_CO2_KG_PER_KWH} kg/kWh  (CEA India FY2022-23)",
        )
        self._tile_co2.grid(row=1, column=2, padx=4, pady=4, sticky="nsew")

        # PR source label (measured / estimated)
        self._pr_source_lbl = ttk.Label(frm, text="", style="TileInfo.TLabel")
        self._pr_source_lbl.grid(row=2, column=0, columnspan=3, sticky="w", padx=8, pady=(0, 6))

    # ──────────────────────── Target progress bar ────────────────────────────

    def _build_target_bar(self):
        frm = ttk.LabelFrame(self._content, text="  vs Design Target")
        frm.grid(row=1, column=0, sticky="ew", padx=4, pady=2)
        frm.columnconfigure(1, weight=1)

        ttk.Label(frm, text="Today:", style="FieldLabel.TLabel").grid(
            row=0, column=0, padx=(0, 6), pady=8, sticky="w")
        self._target_bar_var = tk.DoubleVar(value=0.0)
        self._target_bar = ttk.Progressbar(
            frm, variable=self._target_bar_var, maximum=100.0,
            length=300, mode="determinate",
        )
        self._target_bar.grid(row=0, column=1, padx=4, pady=8, sticky="ew")

        self._target_pct_lbl = ttk.Label(
            frm, text="—", style="MonoBold.TLabel", width=8,
        )
        self._target_pct_lbl.grid(row=0, column=2, padx=(6, 4))

        self._target_status_lbl = ttk.Label(
            frm, text="", style="SectionTitle.TLabel",
        )
        self._target_status_lbl.grid(row=0, column=3, padx=(0, 8))

        self._target_detail_lbl = ttk.Label(frm, text="", style="Caption.TLabel")
        self._target_detail_lbl.grid(
            row=1, column=0, columnspan=4, sticky="w", padx=0, pady=(0, 6),
        )

    # ──────────────────────── Plant config panel ─────────────────────────────

    def _build_config_panel(self):
        outer = ttk.LabelFrame(self._content, text="  Plant Configuration  (IEC 61724-1)",
                               style="Form.TLabelframe")
        outer.grid(row=2, column=0, sticky="ew", padx=4, pady=2)

        # Row 0: capacity
        r0 = ttk.Frame(outer)
        r0.pack(fill="x", padx=8, pady=(6, 2))

        ttk.Label(r0, text="DC Capacity (kWp):").pack(side="left")
        self._ent_dc = ttk.Entry(r0, width=10)
        self._ent_dc.pack(side="left", padx=(4, 16))
        self._ent_dc.insert(0, str(self._sc_val("dc_capacity_kwp", 0.0)))

        ttk.Label(r0, text="AC Capacity (kW):").pack(side="left")
        self._ent_ac = ttk.Entry(r0, width=10)
        self._ent_ac.pack(side="left", padx=(4, 16))
        self._ent_ac.insert(0, str(self._sc_val("ac_capacity_kw", 0.0)))

        ttk.Label(r0, text="Annual Target (kWh):").pack(side="left")
        self._ent_annual = ttk.Entry(r0, width=12)
        self._ent_annual.pack(side="left", padx=(4, 0))
        self._ent_annual.insert(0, str(self._sc_val("annual_target_kwh", 0.0)))
        ttk.Label(r0, text="  (0 = auto)", style="Dim.TLabel").pack(side="left")

        # Row 1: design parameters
        r1 = ttk.Frame(outer)
        r1.pack(fill="x", padx=8, pady=2)

        ttk.Label(r1, text="Design PSH (h/day):").pack(side="left")
        self._ent_psh = ttk.Entry(r1, width=8)
        self._ent_psh.pack(side="left", padx=(4, 4))
        self._ent_psh.insert(0, str(self._sc_val("psh_design", 5.0)))
        ttk.Label(r1, text="PSH from PVGIS/NASA", style="Dim.TLabel").pack(side="left", padx=(0, 16))

        ttk.Label(r1, text="Design PR (0–1):").pack(side="left")
        self._ent_pr = ttk.Entry(r1, width=8)
        self._ent_pr.pack(side="left", padx=(4, 16))
        self._ent_pr.insert(0, str(self._sc_val("pr_design", 0.78)))

        ttk.Label(r1, text="CO₂ factor (kg/kWh):").pack(side="left")
        self._ent_co2f = ttk.Entry(r1, width=8)
        self._ent_co2f.pack(side="left", padx=(4, 0))
        self._ent_co2f.insert(0, str(self._sc_val("emission_factor_kg_kwh", CEA_CO2_KG_PER_KWH)))
        ttk.Label(r1, text="  CEA India default", style="Dim.TLabel").pack(side="left")

        # Row 2: energy/power key + irradiance
        r2 = ttk.Frame(outer)
        r2.pack(fill="x", padx=8, pady=2)

        ttk.Label(r2, text="Energy register:").pack(side="left")
        self._cmb_ekey = ttk.Combobox(
            r2,
            values=["Today_kWh", "Import_kWh", "Net_kWh", "Lifetime_kWh"],
            state="readonly", width=14,
        )
        self._cmb_ekey.set(self._sc_val("energy_key", "Today_kWh"))
        self._cmb_ekey.pack(side="left", padx=(4, 16))

        ttk.Label(r2, text="Power register:").pack(side="left")
        self._cmb_pkey = ttk.Combobox(
            r2,
            values=["kW", "Total_kW"],
            state="readonly", width=12,
        )
        self._cmb_pkey.set(self._sc_val("power_key", "kW"))
        self._cmb_pkey.pack(side="left", padx=(4, 16))

        # Row 3: irradiance manual entry
        r3 = ttk.Frame(outer)
        r3.pack(fill="x", padx=8, pady=2)

        ttk.Label(r3, text="H_POA today (kWh/m²):").pack(side="left")
        self._ent_irr = ttk.Entry(r3, width=10)
        self._ent_irr.pack(side="left", padx=(4, 4))
        self._ent_irr.insert(0, str(self._sc_val("irradiance_kwh_m2", 0.0)))
        ttk.Label(
            r3,
            text="In-plane irradiation measured today.  0 = not available → PR estimated from PSH.",
            style="Dim.TLabel",
        ).pack(side="left", padx=(4, 0))

        # Row 4: apply button + design preview
        r4 = ttk.Frame(outer)
        r4.pack(fill="x", padx=8, pady=(8, 4))

        ttk.Button(r4, text="✔  Apply & Save",
                   command=self._apply_config, style="Primary.TButton").pack(side="left")
        self._design_preview_lbl = ttk.Label(r4, text="", style="Mono.TLabel")
        self._design_preview_lbl.pack(side="left", padx=(16, 0))

        self._refresh_design_preview()

    # ──────────────────────── Warnings row ───────────────────────────────────

    def _build_warnings_row(self):
        self._warn_lbl = ttk.Label(
            self, text="", style="Warn.TLabel", wraplength=800,
        )
        self._warn_lbl.grid(row=3, column=0, sticky="w", padx=8, pady=(4, 6))

    # ─────────────────────────── Update cycle ────────────────────────────────

    def update_view(self, values: Optional[Dict[str, Any]]):
        """
        Called every UI tick (typically 1 Hz) with the TOTAL/meter values dict.

        Reads energy and power from the configured register keys,
        runs the KPI engine, and refreshes all widgets.
        """
        config = SolarKPIEngine.cfg_to_config(self.cfg)
        sc = self.cfg.get("solar_plant") or {}

        # Pull live irradiance from config (operator may update it mid-day)
        config.irradiance_kwh_m2 = float(sc.get("irradiance_kwh_m2") or 0.0)

        # Extract energy and power from metered values
        energy_kwh = 0.0
        current_kw = 0.0
        if values:
            try:
                ev = values.get(config.energy_key)
                energy_kwh = float(ev) if isinstance(ev, (int, float)) else 0.0
            except Exception:
                pass
            try:
                pv = values.get(config.power_key)
                current_kw = float(pv) if isinstance(pv, (int, float)) else 0.0
            except Exception:
                pass

        kpi = _ENGINE.compute(
            today_kwh=energy_kwh,
            current_kw=current_kw,
            config=config,
        )
        self._last_kpi = kpi
        self._refresh_tiles(kpi)
        self._refresh_target_bar(kpi)
        self._refresh_warnings(kpi)

    # ─────────────────────────── Refresh helpers ─────────────────────────────

    def _refresh_tiles(self, kpi: dict):
        self._tile_energy.update_value(kpi.get("energy_kwh"))
        self._tile_yf.update_value(kpi.get("specific_yield"))
        self._tile_cuf.update_value(kpi.get("cuf_24h_pct"))
        self._tile_load.update_value(kpi.get("loading_pct"))
        self._tile_co2.update_value(kpi.get("co2_avoided_kg"))

        # PR tile: prefer measured, fall back to estimated
        pr_source = kpi.get("pr_source", "none")
        t = _t()
        if pr_source == "measured":
            pr_val = kpi.get("pr_measured_pct")
            pr_tag = "PR (IEC 61724, measured H_POA)"
            color = ""
        elif pr_source == "estimated":
            pr_val = kpi.get("pr_estimated_pct")
            pr_tag = "PR estimated from PSH_design — add pyranometer for measured PR"
            color = t.warn
        else:
            pr_val = None
            pr_tag = "PR unavailable — configure dc_capacity_kwp"
            color = t.stale
        self._tile_pr.update_value(pr_val, override_color=color)
        self._pr_source_lbl.config(text=pr_tag)

    def _refresh_target_bar(self, kpi: dict):
        vs = kpi.get("vs_target_pct")
        status = kpi.get("vs_target_status", "UNCONFIGURED")
        color = _status_colors().get(status, _t().stale)
        target = kpi.get("target_daily_kwh", 0.0)
        actual = kpi.get("energy_kwh", 0.0)

        if vs is not None:
            self._target_bar_var.set(min(100.0, vs))
            self._target_pct_lbl.config(text=f"{vs:.1f}%", foreground=color)
            status_txt = {
                "ON_TARGET":    "✓ On Target",
                "BELOW_TARGET": "⚠ Below Target",
                "POOR":         "✗ Poor",
            }.get(status, status)
            self._target_status_lbl.config(text=status_txt, foreground=color)
            self._target_detail_lbl.config(
                text=(
                    f"Actual: {actual:.1f} kWh   "
                    f"Target: {target:.1f} kWh/day   "
                    f"(P_dc × PSH_design × PR_design = "
                    f"{kpi.get('dc_capacity_kwp', 0):.1f} kWp × "
                    f"{self.cfg.get('solar_plant', {}).get('psh_design', 5.0):.1f} h × "
                    f"{self.cfg.get('solar_plant', {}).get('pr_design', 0.78):.2f})"
                )
            )
        else:
            self._target_bar_var.set(0.0)
            self._target_pct_lbl.config(text="—", foreground=_t().stale)
            self._target_status_lbl.config(text="Configure plant capacity", foreground=_t().stale)
            self._target_detail_lbl.config(text="Set dc_capacity_kwp, psh_design, pr_design below")

    def _refresh_warnings(self, kpi: dict):
        warns = kpi.get("warnings") or []
        self._warn_lbl.config(text="  ⚠  " + "   |   ".join(warns) if warns else "")

    def _refresh_design_preview(self):
        """Update the computed daily / annual design target preview."""
        try:
            config = SolarKPIEngine.cfg_to_config(self.cfg)
            daily = config.design_daily_kwh
            annual = config.annual_target_effective
            cuf_d = config.design_cuf_pct
            if daily > 0:
                self._design_preview_lbl.config(
                    text=(
                        f"Design daily: {daily:.1f} kWh   "
                        f"Design annual: {annual:,.0f} kWh   "
                        f"Design CUF: {cuf_d:.1f}%"
                    )
                )
            else:
                self._design_preview_lbl.config(text="Enter DC capacity to compute design targets")
        except Exception:
            pass

    # ─────────────────────────── Config handlers ─────────────────────────────

    def _apply_config(self):
        """Read UI entries → validate → save to cfg → refresh design preview."""
        sc = self.cfg.setdefault("solar_plant", {})
        errors: list = []

        def _float(entry: ttk.Entry, key: str, lo: float, hi: float, label: str):
            try:
                v = float(entry.get().strip())
            except ValueError:
                errors.append(f"{label}: not a number")
                return
            if not (lo <= v <= hi):
                errors.append(f"{label}: must be {lo}–{hi} (got {v})")
                return
            sc[key] = v

        _float(self._ent_dc,    "dc_capacity_kwp",       0.0,   100_000.0, "DC Capacity")
        _float(self._ent_ac,    "ac_capacity_kw",         0.0,   100_000.0, "AC Capacity")
        _float(self._ent_annual,"annual_target_kwh",      0.0, 1_000_000.0, "Annual Target")
        _float(self._ent_psh,   "psh_design",             0.1,   24.0,      "PSH Design")
        _float(self._ent_pr,    "pr_design",              0.1,    1.0,      "Design PR")
        _float(self._ent_co2f,  "emission_factor_kg_kwh", 0.0,    5.0,      "CO₂ Factor")
        _float(self._ent_irr,   "irradiance_kwh_m2",      0.0,   50.0,      "H_POA")

        sc["energy_key"] = self._cmb_ekey.get()
        sc["power_key"]  = self._cmb_pkey.get()

        if errors:
            self._warn_lbl.config(
                text="⚠  Config errors: " + " | ".join(errors),
                foreground=_t().alarm,
            )
            return

        self._warn_lbl.config(text="", foreground=_t().warn)
        self._refresh_design_preview()
        logger.info("[SolarKPI] config saved: %s", sc)

    # ─────────────────────────── Helpers ─────────────────────────────────────

    def _sc_val(self, key: str, default):
        """Read a value from cfg['solar_plant'] with a fallback."""
        return self.cfg.get("solar_plant", {}).get(key, default)
