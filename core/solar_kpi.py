"""
Solar PV Plant KPI Engine
=========================
Computes IEC 61724-1:2017-compliant performance indicators from AC-metered
data (Selec MFM384).  All calculations are stateless pure functions —
no Tkinter dependency, fully unit-testable.

Standards & References
----------------------
IEC 61724-1:2017   Photovoltaic system performance — Part 1: Monitoring
IEC 61724-3:2016   Photovoltaic system performance — Part 3: Energy evaluation
MNRE (India)       Guidelines for grid-connected solar rooftop systems (CUF)
CEA (India)        CO2 Baseline Database for the Indian Power Sector v17
                   FY 2022-23 national emission factor: 0.716 kg CO2/kWh

Terminology (IEC 61724-1 §3)
-----------------------------
Y_F  Final Yield         = E_AC / P_dc   [kWh/kWp]
Y_R  Reference Yield     = H_POA / G_STC [h, "peak sun hours" equivalent]
Y_A  Array Yield         = E_DC / P_dc   [kWh/kWp]  (not used here: no DC meter)
PR   Performance Ratio   = Y_F / Y_R     [dimensionless, 0–1]
CUF  Capacity Utilisation Factor         = E_AC/(P_dc×T) [%] — MNRE definition
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

# ---------------------------------------------------------------------------
# Physical / regulatory constants
# ---------------------------------------------------------------------------

#: IEC 61724 STC reference irradiance [kW/m²]
G_STC: float = 1.0

#: CEA India FY 2022-23 national grid CO2 emission factor [kg CO2/kWh]
#: Source: CEA "CO2 Baseline Database" v17 (published 2024-02)
#: Update when CEA releases a new annual figure.
CEA_CO2_KG_PER_KWH: float = 0.716

#: Physically plausible upper bound for PR [%].
#: Values above this indicate a config error (wrong capacity or irradiance).
PR_SANITY_CEILING: float = 100.0


# ---------------------------------------------------------------------------
# Configuration dataclass
# ---------------------------------------------------------------------------

@dataclass
class SolarPlantConfig:
    """
    Site-specific parameters required for IEC 61724 KPI computation.

    Attributes
    ----------
    dc_capacity_kwp : float
        Installed DC peak capacity [kWp].
        Sum of all PV module nameplate ratings at STC.
        Example: 200 modules × 400 Wp = 80.0 kWp.

    ac_capacity_kw : float
        Inverter(s) rated AC output [kW].
        Used for inverter loading factor.
        If 0, engine uses dc_capacity_kwp as a proxy.

    psh_design : float
        Design Peak Sun Hours [h/day].
        Definition: annual average daily in-plane irradiation (kWh/m²)
        divided by G_STC (1.0 kW/m²).
        Source: PVGIS, NASA SSE, or on-site pyranometer data.
        Typical India range: 4.5 – 6.5 h/day.

    pr_design : float
        Design Performance Ratio [dimensionless].
        Accounts for temperature losses (γ·ΔT), soiling, wiring, inverter,
        mismatch, shading, and availability.
        Typical well-maintained crystalline-Si system: 0.75 – 0.82.

    irradiance_kwh_m2 : float
        Today's measured in-plane irradiation H_POA [kWh/m²].
        Set to 0.0 when no pyranometer is available.
        When > 0, enables IEC 61724 PR_measured calculation.

    emission_factor_kg_kwh : float
        Grid CO2 emission displacement factor [kg CO2/kWh].
        Default = CEA India FY 2022-23 (0.716 kg CO2/kWh).

    annual_target_kwh : float
        Annual energy generation target [kWh/year].
        If 0, auto-computed as P_dc × PSH_design × PR_design × 365.

    energy_key : str
        Meter register key used for today's energy reading.
        Options: "Today_kWh", "Import_kWh", "Net_kWh".

    power_key : str
        Meter register key used for instantaneous power.
        Options: "kW", "Total_kW".
    """

    dc_capacity_kwp: float = 0.0
    ac_capacity_kw: float = 0.0
    psh_design: float = 5.0
    pr_design: float = 0.78
    irradiance_kwh_m2: float = 0.0
    emission_factor_kg_kwh: float = CEA_CO2_KG_PER_KWH
    annual_target_kwh: float = 0.0
    energy_key: str = "Today_kWh"
    power_key: str = "kW"

    # ------------------------------------------------------------------
    # Derived properties
    # ------------------------------------------------------------------

    @property
    def ac_cap_effective(self) -> float:
        """Effective AC capacity for loading [kW].  Falls back to dc_capacity_kwp."""
        return self.ac_capacity_kw if self.ac_capacity_kw > 0 else self.dc_capacity_kwp

    @property
    def design_daily_kwh(self) -> float:
        """
        Design daily AC energy target [kWh/day].

        Formula (IEC 61724 sizing method):
            E_design = P_dc_kWp × PSH_design × PR_design

        This is the standard calculation used by EPC contractors and auditors.
        """
        if self.dc_capacity_kwp <= 0 or self.psh_design <= 0:
            return 0.0
        return self.dc_capacity_kwp * self.psh_design * self.pr_design

    @property
    def annual_target_effective(self) -> float:
        """Annual energy target [kWh/year]: explicit override, else daily × 365."""
        if self.annual_target_kwh > 0:
            return self.annual_target_kwh
        return self.design_daily_kwh * 365.0

    @property
    def design_cuf_pct(self) -> float:
        """
        Design CUF over 24 h [%].
        CUF_design = E_design_daily / (P_dc_kWp × 24) × 100
        """
        if self.dc_capacity_kwp <= 0:
            return 0.0
        return (self.design_daily_kwh / (self.dc_capacity_kwp * 24.0)) * 100.0


# ---------------------------------------------------------------------------
# KPI Engine
# ---------------------------------------------------------------------------

class SolarKPIEngine:
    """
    Stateless solar PV KPI calculator.

    All methods are class-level (no instance state).  Pass metered values
    and a SolarPlantConfig; receive a fully-annotated result dict.
    """

    # -----------------------------------------------------------------------
    # Main entry point
    # -----------------------------------------------------------------------

    def compute(
        self,
        *,
        today_kwh: float,
        current_kw: float,
        config: SolarPlantConfig,
        hours_elapsed: float = 24.0,
    ) -> Dict[str, Any]:
        """
        Compute all IEC 61724 KPIs for the current period.

        Parameters
        ----------
        today_kwh : float
            AC energy generated since midnight [kWh].
            Read from meter key ``config.energy_key`` (e.g. Today_kWh).

        current_kw : float
            Current AC active power [kW], instantaneous sample.

        config : SolarPlantConfig
            Site configuration.

        hours_elapsed : float
            Hours elapsed in current day (0–24).
            Used for CUF over the actual measurement period.
            Pass 24.0 for end-of-day or annualised comparisons.

        Returns
        -------
        dict
            Keys:
            ``energy_kwh``          : measured AC energy [kWh]
            ``current_kw``          : current AC power [kW]
            ``dc_capacity_kwp``     : installed DC capacity [kWp]
            ``specific_yield``      : Y_F [kWh/kWp] — IEC 61724 Final Yield
            ``cuf_pct``             : CUF over hours_elapsed [%] — MNRE
            ``cuf_24h_pct``         : CUF normalised to 24 h [%]
            ``pr_estimated_pct``    : PR estimated from design PSH [%]
            ``pr_measured_pct``     : PR from measured irradiance [%] or None
            ``pr_source``           : "measured" | "estimated" | "none"
            ``loading_pct``         : instantaneous P_AC / P_dc [%]
            ``co2_avoided_kg``      : CO2 displacement today [kg]
            ``target_daily_kwh``    : design daily energy target [kWh]
            ``annual_target_kwh``   : annual energy target [kWh/year]
            ``vs_target_pct``       : actual / target × 100 [%]
            ``vs_target_status``    : "ON_TARGET" | "BELOW_TARGET" | "POOR" | "UNCONFIGURED"
            ``irradiance_available``: True if irradiance was provided
            ``warnings``            : list[str] of sanity-check messages
        """
        e = float(today_kwh)
        p = float(current_kw)
        T = max(0.001, float(hours_elapsed))
        kWp = float(config.dc_capacity_kwp)

        result: Dict[str, Any] = {
            "energy_kwh":           round(e, 3),
            "current_kw":           round(p, 3),
            "dc_capacity_kwp":      kWp,
            "ac_capacity_kw":       config.ac_capacity_kw,
            "irradiance_available": config.irradiance_kwh_m2 > 0,
            "pr_source":            "none",
            "pr_estimated_pct":     None,
            "pr_measured_pct":      None,
            "specific_yield":       None,
            "cuf_pct":              None,
            "cuf_24h_pct":          None,
            "loading_pct":          None,
            "co2_avoided_kg":       round(e * config.emission_factor_kg_kwh, 2),
            "target_daily_kwh":     round(config.design_daily_kwh, 1),
            "annual_target_kwh":    round(config.annual_target_effective, 0),
            "vs_target_pct":        None,
            "vs_target_status":     "UNCONFIGURED",
            "design_cuf_pct":       round(config.design_cuf_pct, 2),
            "design_pr_pct":        round(config.pr_design * 100.0, 1),
            "warnings":             [],
        }

        if kWp <= 0:
            result["warnings"].append(
                "DC capacity not configured — set dc_capacity_kwp in Solar Plant settings"
            )
            return result

        # ------------------------------------------------------------------
        # 1. Final Yield  Y_F  (IEC 61724-1 §7.3.3)
        #    Y_F = E_AC / P_dc   [kWh/kWp]
        # ------------------------------------------------------------------
        yf = e / kWp
        result["specific_yield"] = round(yf, 3)

        # ------------------------------------------------------------------
        # 2. Capacity Utilisation Factor  (MNRE definition)
        #    CUF = E_AC / (P_dc × T) × 100  [%]
        # ------------------------------------------------------------------
        result["cuf_pct"]    = round((e / (kWp * T))    * 100.0, 2)
        result["cuf_24h_pct"] = round((e / (kWp * 24.0)) * 100.0, 2)

        # ------------------------------------------------------------------
        # 3. Performance Ratio  PR  (IEC 61724-1 §7.3.5)
        #
        #    With irradiance (preferred):
        #       Y_R = H_POA / G_STC   [h]  Reference Yield
        #       PR  = Y_F / Y_R        [dimensionless → % here]
        #
        #    Without irradiance (indicative only):
        #       PR_est = Y_F / PSH_design
        #       Interpretation: how well did we use the design sun budget?
        # ------------------------------------------------------------------
        if config.irradiance_kwh_m2 > 0:
            yr = config.irradiance_kwh_m2 / G_STC      # Reference Yield [h]
            if yr > 0:
                pr_meas = (yf / yr) * 100.0
                result["pr_measured_pct"] = round(pr_meas, 2)
                result["pr_source"] = "measured"
                if pr_meas > PR_SANITY_CEILING:
                    result["warnings"].append(
                        f"PR_measured = {pr_meas:.1f}% exceeds physical limit. "
                        "Verify H_POA input and dc_capacity_kwp."
                    )

        if config.psh_design > 0:
            pr_est = (yf / config.psh_design) * 100.0
            result["pr_estimated_pct"] = round(pr_est, 2)
            if result["pr_source"] == "none":
                result["pr_source"] = "estimated"

        # ------------------------------------------------------------------
        # 4. Instantaneous Loading Factor
        #    = P_AC_now / P_dc × 100  [%]
        #    Informal metric: fraction of installed capacity currently active.
        # ------------------------------------------------------------------
        result["loading_pct"] = round((max(0.0, p) / kWp) * 100.0, 1)

        # ------------------------------------------------------------------
        # 5. vs Design Target
        # ------------------------------------------------------------------
        target = config.design_daily_kwh
        if target > 0:
            vs = (e / target) * 100.0
            result["vs_target_pct"] = round(vs, 1)
            if vs >= 95.0:
                result["vs_target_status"] = "ON_TARGET"     # ≥ 95% of target
            elif vs >= 75.0:
                result["vs_target_status"] = "BELOW_TARGET"  # 75–95% of target
            else:
                result["vs_target_status"] = "POOR"          # < 75% of target

        # ------------------------------------------------------------------
        # 6. Sanity warnings
        # ------------------------------------------------------------------
        if e < 0:
            result["warnings"].append(
                "Energy reading is negative. "
                "Check if this is an export/net meter — use Import_kWh key for generation."
            )
        if p > kWp * 1.15:
            result["warnings"].append(
                f"Instantaneous power ({p:.1f} kW) > 115% of DC capacity "
                f"({kWp:.1f} kWp). Verify dc_capacity_kwp setting."
            )

        return result

    # -----------------------------------------------------------------------
    # Config helper
    # -----------------------------------------------------------------------

    @staticmethod
    def cfg_to_config(cfg: dict) -> SolarPlantConfig:
        """
        Build SolarPlantConfig from the application config dict.
        Reads ``cfg["solar_plant"]`` section.
        """
        sc = cfg.get("solar_plant") or {}
        return SolarPlantConfig(
            dc_capacity_kwp=float(sc.get("dc_capacity_kwp") or 0.0),
            ac_capacity_kw=float(sc.get("ac_capacity_kw") or 0.0),
            psh_design=float(sc.get("psh_design") or 5.0),
            pr_design=float(sc.get("pr_design") or 0.78),
            irradiance_kwh_m2=float(sc.get("irradiance_kwh_m2") or 0.0),
            emission_factor_kg_kwh=float(
                sc.get("emission_factor_kg_kwh") or CEA_CO2_KG_PER_KWH
            ),
            annual_target_kwh=float(sc.get("annual_target_kwh") or 0.0),
            energy_key=str(sc.get("energy_key") or "Today_kWh"),
            power_key=str(sc.get("power_key") or "kW"),
        )
