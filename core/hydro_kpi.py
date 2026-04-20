"""
Small Hydro Plant KPI Engine
=============================
Computes engineering-grade performance indicators from AC-metered data
(Selec MFM384).  All calculations follow named standards.

Standards & References
----------------------
IEC 60041:1991  Field acceptance tests — hydraulic turbines / pumps
                Defines specific hydraulic energy, efficiency, unit values
IEC 60193:1999  Model acceptance tests — Hydraulic turbines / pumps
IS 12837:1989   Hydraulic turbines for medium and large power plants (India)
CBIP Manual     Planning and Design of Small Hydro Projects (India)
                Defines Plant Load Factor (PLF) and Availability Factor
CEA (India)     CO2 Baseline Database v17, FY 2022-23: 0.716 kg CO2/kWh

Physical Constants (IEC 60041 §3.1)
------------------------------------
ρ_water   = 1000.0 kg/m³  (fresh water at 15 °C)
g         = 9.81  m/s²    (standard gravitational acceleration, IEC 60041)

Turbine Types Supported
-----------------------
PELTON    : Impulse, high head (>100 m),   design η: 85–92%
FRANCIS   : Reaction, medium head (10–400 m), design η: 85–93%
KAPLAN    : Axial reaction, low head (<20 m),  design η: 88–92%
CROSS_FLOW: Banki-Michell, small schemes, η: 60–82%
TURGO     : Impulse, medium head,          design η: 78–90%
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

# ---------------------------------------------------------------------------
# Physical constants  (IEC 60041 §3.1)
# ---------------------------------------------------------------------------

#: Standard gravitational acceleration [m/s²]
G_STANDARD: float = 9.81

#: Fresh water density at 15 °C [kg/m³]
RHO_FRESH_WATER: float = 1000.0

#: CEA India FY 2022-23 CO2 emission factor [kg CO2/kWh]
CEA_CO2_KG_PER_KWH: float = 0.716

# ---------------------------------------------------------------------------
# Turbine efficiency reference bands (from IEC 60193, manufacturer data)
# Used for sanity-checking measured efficiency against technology norms.
# Format: {type: (η_min, η_max)} at full load design point.
# ---------------------------------------------------------------------------
_TURBINE_EFF_RANGE: Dict[str, Tuple[float, float]] = {
    "PELTON":     (0.82, 0.92),
    "FRANCIS":    (0.82, 0.93),
    "KAPLAN":     (0.85, 0.92),
    "CROSS_FLOW": (0.60, 0.82),
    "TURGO":      (0.78, 0.90),
}

# ---------------------------------------------------------------------------
# Configuration dataclass
# ---------------------------------------------------------------------------

@dataclass
class HydroPlantConfig:
    """
    Site-specific parameters for hydro KPI computation.

    Attributes
    ----------
    rated_capacity_kw : float
        Generator rated output at the generator terminals [kW].
        This is the nameplate electrical capacity — NOT hydraulic power.

    design_head_m : float
        Net design head [m].
        H_net = H_gross - Σ(hydraulic losses in penstock, valves, draft tube).
        Source: project DPR / model test report.

    design_flow_m3_s : float
        Design discharge (flow rate) at rated conditions [m³/s].
        Used to compute hydraulic power at design point.

    turbine_type : str
        One of: PELTON, FRANCIS, KAPLAN, CROSS_FLOW, TURGO.
        Used for efficiency range validation and context labelling.

    design_plf_pct : float
        Design Plant Load Factor [%].
        PLF = E_annual / (P_rated × 8760) × 100.
        Source: project DPR.  Typical small hydro India: 40–65%.
        If 0, daily target is computed as P_rated × 24h only.

    design_annual_kwh : float
        Design annual energy generation [kWh/year].
        If 0, auto-computed from rated_capacity_kw × design_plf_pct × 8760.

    min_operating_kw : float
        Minimum power threshold for "unit is running" determination [kW].
        Used for Availability Factor computation.
        Typically 3–5% of rated capacity.

    water_density_kg_m3 : float
        Water density [kg/m³].
        Use 1000 kg/m³ (fresh water IEC 60041).
        For silt-laden rivers: 1005–1050 kg/m³ (operator-measured).

    flow_m3_s : float
        Today's average / current measured flow [m³/s].
        0.0 = not available (no flow meter). Efficiency and specific energy
        computations will return None with an appropriate message.

    head_net_m : float
        Current / average net head [m].
        0.0 = not available.  If 0, design_head_m is used as estimate
        (labelled "design head — not measured").

    energy_key : str
        Meter register key for today's energy.
        Options: "Today_kWh", "Import_kWh".

    power_key : str
        Meter register key for instantaneous power.
        Options: "kW", "Total_kW".

    emission_factor_kg_kwh : float
        Grid CO2 emission factor [kg CO2/kWh].
        Hydro is a zero-emission source; this factor is used to compute
        the carbon displacement credit vs grid equivalent.
    """

    rated_capacity_kw: float = 0.0
    design_head_m: float = 0.0
    design_flow_m3_s: float = 0.0
    turbine_type: str = "FRANCIS"
    design_plf_pct: float = 50.0
    design_annual_kwh: float = 0.0
    min_operating_kw: float = 0.0
    water_density_kg_m3: float = RHO_FRESH_WATER
    flow_m3_s: float = 0.0
    head_net_m: float = 0.0
    energy_key: str = "Today_kWh"
    power_key: str = "kW"
    emission_factor_kg_kwh: float = CEA_CO2_KG_PER_KWH

    # -----------------------------------------------------------------------
    # Derived properties
    # -----------------------------------------------------------------------

    @property
    def min_operating_kw_effective(self) -> float:
        """Availability threshold [kW].  Default = 3% of rated capacity."""
        if self.min_operating_kw > 0:
            return self.min_operating_kw
        return max(1.0, self.rated_capacity_kw * 0.03)

    @property
    def design_daily_kwh(self) -> float:
        """
        Design daily generation target [kWh/day].

        Derived from design PLF:
            E_daily = P_rated × (PLF/100) × 24

        Source: CBIP Manual on Small Hydro Projects.
        """
        if self.rated_capacity_kw <= 0:
            return 0.0
        plf = self.design_plf_pct if self.design_plf_pct > 0 else 50.0
        return self.rated_capacity_kw * (plf / 100.0) * 24.0

    @property
    def annual_target_effective(self) -> float:
        """Annual generation target [kWh/year]: explicit override or daily × 365."""
        if self.design_annual_kwh > 0:
            return self.design_annual_kwh
        return self.design_daily_kwh * 365.0

    @property
    def hydraulic_power_kw(self) -> Optional[float]:
        """
        Hydraulic power at design point [kW].

        Formula (IEC 60041 §3.2):
            P_hyd = ρ × g × Q × H_net / 1000   [kW]

        Returns None if design_head_m or design_flow_m3_s is not set.
        """
        if self.design_head_m <= 0 or self.design_flow_m3_s <= 0:
            return None
        return (self.water_density_kg_m3 * G_STANDARD *
                self.design_flow_m3_s * self.design_head_m) / 1000.0

    @property
    def design_efficiency_pct(self) -> Optional[float]:
        """
        Design-point overall efficiency [%].
        η = P_rated_kW / P_hydraulic_kW × 100

        Represents combined turbine + generator efficiency at design point.
        """
        p_hyd = self.hydraulic_power_kw
        if p_hyd is None or p_hyd <= 0:
            return None
        return (self.rated_capacity_kw / p_hyd) * 100.0

    @property
    def turbine_eff_range(self) -> Tuple[float, float]:
        """Expected full-load efficiency range for turbine type [fraction]."""
        return _TURBINE_EFF_RANGE.get(self.turbine_type.upper(), (0.70, 0.92))


# ---------------------------------------------------------------------------
# KPI Engine
# ---------------------------------------------------------------------------

class HydroKPIEngine:
    """
    Stateless hydro plant KPI calculator.

    Requires only AC electrical measurements (MFM384).
    Flow/head are optional — efficiency and specific energy are only
    computed when they are provided, and clearly labelled otherwise.
    """

    def compute(
        self,
        *,
        today_kwh: float,
        current_kw: float,
        run_hours_today: float,
        config: HydroPlantConfig,
        hours_elapsed: float = 24.0,
    ) -> Dict[str, Any]:
        """
        Compute all hydro KPIs for the current period.

        Parameters
        ----------
        today_kwh : float
            AC energy generated since midnight [kWh].

        current_kw : float
            Current AC active power [kW], instantaneous.

        run_hours_today : float
            Hours the unit was online today (power > min_operating threshold).
            Computed externally by comparing P_AC to min_operating_kw.
            Range: 0–hours_elapsed.

        config : HydroPlantConfig
            Site configuration.

        hours_elapsed : float
            Hours elapsed in the current day (0–24).

        Returns
        -------
        dict with keys:
            energy_kwh          : today's AC energy [kWh]
            current_kw          : current power [kW]
            rated_capacity_kw   : nameplate [kW]
            plf_pct             : Plant Load Factor [%]
            plf_design_pct      : Design PLF [%]
            availability_pct    : availability factor [%]
            loading_pct         : P_AC / P_rated [%] instantaneous
            efficiency_pct      : overall η [%] or None
            efficiency_source   : "measured_head_flow" | "design_head_flow" | "none"
            specific_energy     : kWh/m³ or None
            hydraulic_power_kw  : P_hyd at current Q+H or None
            co2_avoided_kg      : carbon displacement credit [kg]
            target_daily_kwh    : design daily target [kWh]
            annual_target_kwh   : annual target [kWh/year]
            vs_target_pct       : actual / target × 100 [%]
            vs_target_status    : "ON_TARGET" | "BELOW_TARGET" | "POOR" | "UNCONFIGURED"
            warnings            : list[str]
        """
        e = float(today_kwh)
        p = float(current_kw)
        rh = max(0.0, float(run_hours_today))
        T = max(0.001, float(hours_elapsed))
        kW_rated = float(config.rated_capacity_kw)

        result: Dict[str, Any] = {
            "energy_kwh":           round(e, 3),
            "current_kw":           round(p, 3),
            "rated_capacity_kw":    kW_rated,
            "turbine_type":         config.turbine_type,
            "run_hours_today":      round(rh, 2),
            "plf_pct":              None,
            "plf_design_pct":       round(config.design_plf_pct, 1),
            "availability_pct":     None,
            "loading_pct":          None,
            "efficiency_pct":       None,
            "efficiency_source":    "none",
            "specific_energy":      None,
            "hydraulic_power_kw":   None,
            "co2_avoided_kg":       round(e * config.emission_factor_kg_kwh, 2),
            "target_daily_kwh":     round(config.design_daily_kwh, 1),
            "annual_target_kwh":    round(config.annual_target_effective, 0),
            "vs_target_pct":        None,
            "vs_target_status":     "UNCONFIGURED",
            "warnings":             [],
        }

        if kW_rated <= 0:
            result["warnings"].append(
                "Rated capacity not configured — set rated_capacity_kw in Hydro Plant settings"
            )
            return result

        # ------------------------------------------------------------------
        # 1. Plant Load Factor  (PLF)  — CBIP / CEA India definition
        #    PLF = E_AC / (P_rated × T) × 100   [%]
        #    T = hours in period (24h for daily)
        # ------------------------------------------------------------------
        plf = (e / (kW_rated * T)) * 100.0
        result["plf_pct"] = round(plf, 2)

        # ------------------------------------------------------------------
        # 2. Availability Factor
        #    A = run_hours_today / hours_elapsed × 100   [%]
        #    "Unit available" means P_AC > min_operating_kw.
        # ------------------------------------------------------------------
        availability = (rh / T) * 100.0
        result["availability_pct"] = round(availability, 1)

        # ------------------------------------------------------------------
        # 3. Instantaneous Loading Factor
        #    = P_AC_now / P_rated × 100   [%]
        # ------------------------------------------------------------------
        result["loading_pct"] = round((max(0.0, p) / kW_rated) * 100.0, 1)

        # ------------------------------------------------------------------
        # 4. Overall Efficiency  η  (IEC 60041 §3.2)
        #    η = P_output / P_hydraulic × 100
        #    P_hydraulic = ρ × g × Q × H_net / 1000   [kW]
        #
        #    Preference order for H_net:
        #      (a) config.head_net_m  — measured today  → "measured_head_flow"
        #      (b) config.design_head_m — design value  → "design_head_flow"
        #    Q must be > 0 for computation.
        # ------------------------------------------------------------------
        Q = config.flow_m3_s
        H_meas = config.head_net_m
        H_design = config.design_head_m

        if Q > 0 and p > 0:
            if H_meas > 0:
                H_used = H_meas
                eff_src = "measured_head_flow"
            elif H_design > 0:
                H_used = H_design
                eff_src = "design_head_flow"
            else:
                H_used = 0.0
                eff_src = "none"

            if H_used > 0:
                p_hyd = (config.water_density_kg_m3 * G_STANDARD * Q * H_used) / 1000.0
                if p_hyd > 0:
                    eta = (p / p_hyd) * 100.0
                    result["efficiency_pct"]   = round(eta, 2)
                    result["efficiency_source"] = eff_src
                    result["hydraulic_power_kw"] = round(p_hyd, 2)

                    # Sanity: compare against turbine-type bounds
                    lo, hi = config.turbine_eff_range
                    if eta > hi * 100.0 + 5.0:
                        result["warnings"].append(
                            f"η = {eta:.1f}% is above the expected range for "
                            f"{config.turbine_type} ({lo*100:.0f}–{hi*100:.0f}%). "
                            "Check flow / head input or rated capacity."
                        )
                    elif eta < lo * 100.0 - 10.0:
                        result["warnings"].append(
                            f"η = {eta:.1f}% is well below the expected range for "
                            f"{config.turbine_type} ({lo*100:.0f}–{hi*100:.0f}%). "
                            "Possible degradation, cavitation, or flow measurement error."
                        )
        elif Q <= 0:
            result["warnings"].append(
                "Flow rate not entered — efficiency and specific energy not computed. "
                "Enter today's average discharge in Plant Configuration."
            )

        # ------------------------------------------------------------------
        # 5. Specific Energy  e_w  [kWh/m³]
        #    e_w = E_AC / V_total
        #    V_total = Q [m³/s] × run_hours × 3600 [s/h]
        #    Reference: IEC 60041 unit energy concept; CBIP water accounting
        # ------------------------------------------------------------------
        if Q > 0 and rh > 0 and e > 0:
            v_total_m3 = Q * rh * 3600.0
            result["specific_energy"] = round(e / v_total_m3, 4)

        # ------------------------------------------------------------------
        # 6. vs Design Target
        # ------------------------------------------------------------------
        target = config.design_daily_kwh
        if target > 0:
            vs = (e / target) * 100.0
            result["vs_target_pct"] = round(vs, 1)
            if vs >= 95.0:
                result["vs_target_status"] = "ON_TARGET"
            elif vs >= 75.0:
                result["vs_target_status"] = "BELOW_TARGET"
            else:
                result["vs_target_status"] = "POOR"

        # ------------------------------------------------------------------
        # 7. Sanity warnings
        # ------------------------------------------------------------------
        if e < 0:
            result["warnings"].append(
                "Energy reading is negative — verify meter polarity."
            )
        if p > kW_rated * 1.10:
            result["warnings"].append(
                f"P_AC ({p:.1f} kW) > 110% of rated capacity ({kW_rated:.1f} kW). "
                "Verify rated_capacity_kw or check for over-generation condition."
            )
        if rh > T + 0.1:
            result["warnings"].append(
                f"Run hours ({rh:.1f} h) > elapsed time ({T:.1f} h) — check availability tracking."
            )

        return result

    # -----------------------------------------------------------------------
    # Config helper
    # -----------------------------------------------------------------------

    @staticmethod
    def cfg_to_config(cfg: dict) -> HydroPlantConfig:
        """Build HydroPlantConfig from the application config dict."""
        hc = cfg.get("hydro_plant") or {}
        return HydroPlantConfig(
            rated_capacity_kw=float(hc.get("rated_capacity_kw") or 0.0),
            design_head_m=float(hc.get("design_head_m") or 0.0),
            design_flow_m3_s=float(hc.get("design_flow_m3_s") or 0.0),
            turbine_type=str(hc.get("turbine_type") or "FRANCIS"),
            design_plf_pct=float(hc.get("design_plf_pct") or 50.0),
            design_annual_kwh=float(hc.get("design_annual_kwh") or 0.0),
            min_operating_kw=float(hc.get("min_operating_kw") or 0.0),
            water_density_kg_m3=float(hc.get("water_density_kg_m3") or RHO_FRESH_WATER),
            flow_m3_s=float(hc.get("flow_m3_s") or 0.0),
            head_net_m=float(hc.get("head_net_m") or 0.0),
            energy_key=str(hc.get("energy_key") or "Today_kWh"),
            power_key=str(hc.get("power_key") or "kW"),
            emission_factor_kg_kwh=float(hc.get("emission_factor_kg_kwh") or CEA_CO2_KG_PER_KWH),
        )
