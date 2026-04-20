"""
Industrial Load KPI Engine
==========================
Computes demand analysis, power factor, load factor, and power quality KPIs
from metered data (Selec MFM384 via Modbus).

Standards & References
----------------------
IEEE 1459:2010   Definitions for the measurement of electric power quantities
                 under sinusoidal, nonsinusoidal, balanced, or unbalanced conditions.
IEEE 519:2014    Recommended Practice and Requirements for Harmonic Control
                 in Electric Power Systems.
                 THD_V limit: < 5 % at PCC (≤ 69 kV bus, Table 1).
                 THD_I limit: < 8 % (I_SC/I_L = 20–50, LV, Table 2 indicative).
BEE India        Bureau of Energy Efficiency — Load Factor and PF improvement
                 guidelines under Energy Conservation Act 2001.
CERC/SERC        Central/State Electricity Regulatory Commission tariff orders.
                 Typical PF penalty slab: PF < 0.85 → surcharge.
                 Typical PF incentive slab: PF ≥ 0.95 → rebate.
CEA Metering     Central Electricity Authority (Metering) Regulations 2006:
                 Demand measurement interval = 15 min (or 30 min by agreement).
CEA CO2          CEA CO2 Baseline Database for the Indian Power Sector v17,
                 FY 2022-23 national emission factor: 0.716 kg CO2/kWh.

Terminology
-----------
MD   Maximum Demand  — peak of the demand-interval averages [kVA or kW]
LF   Load Factor     = Today_kWh / (MD_kW × elapsed_hours) × 100  [%]
PF   Power Factor    = kW / kVA  (displacement PF from meter registers)
THD  Total Harmonic Distortion  [%]  (from meter THD registers if available)
"""
from __future__ import annotations

import math
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple


# ── Physical / regulatory constants ──────────────────────────────────────────

#: IEEE 519:2014 Table 1 — voltage THD limit at the PCC (bus ≤ 69 kV) [%]
THD_V_LIMIT_PCT: float = 5.0

#: IEEE 519:2014 Table 2 — current THD limit (I_SC/I_L = 20–50, LV) [%]
#: This is indicative; exact limit depends on the short-circuit ratio at PCC.
THD_I_LIMIT_PCT: float = 8.0

#: BEE India — Load Factor benchmarks [%]
LF_EXCELLENT_PCT: float = 70.0
LF_GOOD_PCT:      float = 50.0

#: Default PF thresholds (CERC/SERC typical; operator-configurable in cfg)
PF_PENALTY_DEFAULT:   float = 0.85
PF_INCENTIVE_DEFAULT: float = 0.95

#: CEA India FY 2022-23 grid emission factor [kg CO2/kWh]
CEA_CO2_KG_PER_KWH: float = 0.716


# ── Configuration dataclass ───────────────────────────────────────────────────

@dataclass
class LoadPlantConfig:
    """
    Site-specific parameters for industrial load KPI computation.

    Attributes
    ----------
    contract_demand_kva : float
        Sanctioned / contracted demand [kVA] as per utility agreement.
        0 = not configured (MD utilisation will not be computed).
    demand_interval_min : int
        Demand measurement window [min].  Must be 15 or 30 per CEA Metering
        Regulations 2006.  Default: 15.
    pf_penalty_threshold : float
        Power factor below which utility applies a surcharge [0–1].
        Default 0.85 (CERC/SERC); PUCSL Sri Lanka also 0.85.
    pf_incentive_threshold : float
        Power factor at or above which utility grants a rebate [0–1].
        Default 0.95.
    tariff_demand_rate : float
        Demand charge rate [LKR/kVA/month].  0 = not configured.
    tariff_energy_rate : float
        Flat energy charge rate [LKR/kWh].  Used when TOU is disabled.
        0 = not configured.
    power_key : str
        Meter register key for active power [kW].
    apparent_power_key : str
        Meter register key for apparent power [kVA].
    pf_key : str
        Meter register key for power factor.
    energy_key : str
        Meter register key for today's energy [kWh].

    TOU (Time-of-Use) fields — CEB evening peak schedule
    -----------------------------------------------------
    tou_enabled : bool
        Enable TOU energy billing split.  When True, tariff_energy_rate
        is ignored and tou_peak_rate / tou_offpeak_rate are used instead.
    tou_peak_start : str
        Peak window start as "HH:MM" (24-h, local time).
        CEB General Purpose default: "18:30".
    tou_peak_end : str
        Peak window end as "HH:MM" (24-h, local time).
        CEB General Purpose default: "22:30".
        Overnight wrap supported (end < start means next day).
    tou_peak_rate_lkr_kwh : float
        Energy charge during peak window [LKR/kWh].
    tou_offpeak_rate_lkr_kwh : float
        Energy charge outside peak window [LKR/kWh].
    tou_peak_demand_rate : float
        Additional demand charge for peak-period MD [LKR/kVA/month].
        0 = not applicable.
    """
    contract_demand_kva:      float = 0.0
    demand_interval_min:      int   = 15
    pf_penalty_threshold:     float = PF_PENALTY_DEFAULT
    pf_incentive_threshold:   float = PF_INCENTIVE_DEFAULT
    tariff_demand_rate:       float = 0.0
    tariff_energy_rate:       float = 0.0
    power_key:                str   = "kW"
    apparent_power_key:       str   = "kVA"
    pf_key:                   str   = "PF"
    energy_key:               str   = "Today_kWh"
    # TOU fields
    tou_enabled:              bool  = False
    tou_peak_start:           str   = "18:30"
    tou_peak_end:             str   = "22:30"
    tou_peak_rate_lkr_kwh:    float = 0.0
    tou_offpeak_rate_lkr_kwh: float = 0.0
    tou_peak_demand_rate:     float = 0.0


# ── KPI Engine ────────────────────────────────────────────────────────────────

class LoadKPIEngine:
    """
    Stateful industrial load KPI engine.

    Instantiate once per tab session.  Call:
      • ``update(values, ts)``   — on each UI tick (returns KPI dict)
      • ``reset_daily()``        — at midnight to reset daily accumulators
      • ``reconfigure(cfg)``     — when operator saves new plant config
    """

    def __init__(self, config: LoadPlantConfig):
        self.config = config
        self._reset_state()

    # ── Public API ────────────────────────────────────────────────────────────

    def update(self, values: Dict[str, Any], ts: Optional[float] = None) -> dict:
        """
        Ingest one scan-cycle snapshot.

        Parameters
        ----------
        values : dict
            Latest meter/aggregator values dict.
        ts : float, optional
            Epoch timestamp of this sample.  Defaults to ``time.time()``.

        Returns
        -------
        dict
            KPI result dict.  All keys are always present; value is ``None``
            when the register is unavailable or the KPI cannot be computed.
        """
        if ts is None:
            ts = time.time()

        cfg    = self.config
        warns: List[str] = []

        # ── 1. Extract live readings ──────────────────────────────────────────
        kw  = self._fval(values, cfg.power_key)
        kva = self._fval(values, cfg.apparent_power_key)
        pf  = self._fval(values, cfg.pf_key)
        kwh = self._fval(values, cfg.energy_key)

        # Reactive power: Q = sqrt(S² − P²)  (IEEE 1459 §3.1)
        kvar: Optional[float] = None
        if kw is not None and kva is not None and kva >= 0.0:
            q2 = kva ** 2 - kw ** 2
            kvar = round(math.sqrt(max(0.0, q2)), 2)

        # THD — optional registers; average across available phases
        thd_v = self._thd_avg(values, ["THD_V1", "THD_V2", "THD_V3"])
        thd_i = self._thd_avg(values, ["THD_I1", "THD_I2", "THD_I3"])

        # ── 2. Rolling demand window (CEA 15-/30-min sliding average) ─────────
        interval_s = float(cfg.demand_interval_min) * 60.0

        if kva is not None and kva >= 0.0:
            self._win_kva.append((ts, kva))
        self._expire(self._win_kva, ts, interval_s)

        if kw is not None and kw >= 0.0:
            self._win_kw.append((ts, kw))
        self._expire(self._win_kw, ts, interval_s)

        di_kva = (sum(v for _, v in self._win_kva) / len(self._win_kva)
                  if self._win_kva else (kva or 0.0))
        di_kw  = (sum(v for _, v in self._win_kw)  / len(self._win_kw)
                  if self._win_kw  else (kw  or 0.0))

        # Update today's MD peaks
        if di_kva > self._md_kva:
            self._md_kva = di_kva
        if di_kw > self._md_kw:
            self._md_kw = di_kw

        # ── 3. Load Factor (BEE India definition) ─────────────────────────────
        # LF = E_consumed / (MD_kW × elapsed_hours) × 100
        # A high load factor means the load is being used efficiently (close to
        # peak demand continuously rather than in short bursts).
        load_factor_pct: Optional[float] = None
        lf_status = "unknown"
        elapsed_h = max(0.001, (ts - self._day_start_ts) / 3600.0)
        if kwh is not None and self._md_kw > 0.0:
            theoretical_max_kwh = self._md_kw * elapsed_h
            if theoretical_max_kwh > 0.0:
                lf = (kwh / theoretical_max_kwh) * 100.0
                load_factor_pct = round(min(lf, 100.0), 1)
                if load_factor_pct >= LF_EXCELLENT_PCT:
                    lf_status = "excellent"
                elif load_factor_pct >= LF_GOOD_PCT:
                    lf_status = "good"
                else:
                    lf_status = "poor"

        # ── 4. Power factor tracking ──────────────────────────────────────────
        pf_now    = None
        pf_status = "NORMAL"

        if pf is not None:
            pf_c = abs(float(pf))
            if pf_c > 1.0:
                pf_c = min(1.0, pf_c)
                warns.append("PF register value > 1 — clamped to 1.0.")
            pf_now = round(pf_c, 3)

            # Track today's minimum
            if pf_now < self._pf_min:
                self._pf_min = pf_now

            # kVA-weighted accumulator for daily average PF
            # (IEEE 1459 §5: weight by apparent power so idle periods have
            #  negligible effect on the reported average)
            w = kva if (kva is not None and kva > 0.0) else 1.0
            self._pf_wsum   += pf_now * w
            self._pf_wcount += w

            # Determine penalty / incentive status (CERC/SERC slab)
            if pf_now >= cfg.pf_incentive_threshold:
                pf_status = "INCENTIVE"
            elif pf_now < cfg.pf_penalty_threshold:
                pf_status = "PENALTY"
                warns.append(
                    f"PF {pf_now:.3f} below penalty threshold "
                    f"{cfg.pf_penalty_threshold:.2f} (CERC/SERC)."
                )
        else:
            warns.append("PF register not available in values dict.")

        pf_avg = (round(self._pf_wsum / self._pf_wcount, 3)
                  if self._pf_wcount > 0.0 else None)
        pf_min = self._pf_min if self._pf_min <= 1.0 else None

        # ── 5. Capacitor bank recommendation ──────────────────────────────────
        # Required kVAR to raise PF from pf_now to pf_incentive_threshold:
        # Q_C = P × (tan φ₁ − tan φ₂)   (standard reactive compensation formula)
        cap_rec: Optional[float] = None
        if (kw is not None and kw > 0.5
                and pf_now is not None
                and pf_now < cfg.pf_incentive_threshold):
            try:
                phi1 = math.acos(min(1.0, max(0.0, pf_now)))
                phi2 = math.acos(min(1.0, max(0.0, cfg.pf_incentive_threshold)))
                cap_rec = round(kw * (math.tan(phi1) - math.tan(phi2)), 1)
                if cap_rec < 0.0:
                    cap_rec = 0.0
            except (ValueError, ZeroDivisionError):
                cap_rec = None

        # ── 6. MD utilisation ─────────────────────────────────────────────────
        md_util_pct: Optional[float] = None
        if cfg.contract_demand_kva > 0.0:
            md_util_pct = round((self._md_kva / cfg.contract_demand_kva) * 100.0, 1)
            if md_util_pct > 100.0:
                warns.append(
                    f"MD {self._md_kva:.1f} kVA exceeds contract demand "
                    f"{cfg.contract_demand_kva:.0f} kVA — excess demand charges may apply."
                )

        # ── 7. THD checks (IEEE 519:2014) ─────────────────────────────────────
        if thd_v is not None and thd_v > THD_V_LIMIT_PCT:
            warns.append(
                f"THD_V {thd_v:.1f}% exceeds IEEE 519 limit of "
                f"{THD_V_LIMIT_PCT:.0f}% at PCC."
            )
        if thd_i is not None and thd_i > THD_I_LIMIT_PCT:
            warns.append(
                f"THD_I {thd_i:.1f}% exceeds IEEE 519 indicative limit of "
                f"{THD_I_LIMIT_PCT:.0f}% (verify SCR at PCC)."
            )

        # ── 8. TOU peak-window accumulation ──────────────────────────────────
        # We cannot get separate peak/off-peak kWh from the MFM384 (single
        # Today_kWh register).  Instead we integrate kW samples into two
        # buckets and use the split ratio to apportion Today_kWh for billing.
        in_peak = self._is_peak(cfg, ts)
        if kw is not None and kw >= 0.0:
            if in_peak:
                self._tou_peak_kw_sum    += kw
                self._tou_peak_kw_samples += 1
                # Track peak-period MD for separate demand charge
                if di_kva > self._tou_peak_md_kva:
                    self._tou_peak_md_kva = di_kva
            else:
                self._tou_offpeak_kw_sum    += kw
                self._tou_offpeak_kw_samples += 1

        # Compute apportioned kWh for TOU billing
        tou_total_samples = self._tou_peak_kw_samples + self._tou_offpeak_kw_samples
        if cfg.tou_enabled and tou_total_samples > 0 and kwh is not None and kwh > 0.0:
            peak_ratio = self._tou_peak_kw_sum / max(
                self._tou_peak_kw_sum + self._tou_offpeak_kw_sum, 1e-9
            )
            tou_peak_kwh    = round(kwh * peak_ratio, 2)
            tou_offpeak_kwh = round(kwh * (1.0 - peak_ratio), 2)
        else:
            tou_peak_kwh    = None
            tou_offpeak_kwh = None

        # ── 9. Billing estimates ──────────────────────────────────────────────
        # Demand charge: pro-rated over elapsed billing fraction (days / 30).
        demand_charge_est: Optional[float] = None
        energy_charge_est: Optional[float] = None
        tou_peak_charge:   Optional[float] = None
        tou_offpeak_charge: Optional[float] = None
        tou_peak_demand_charge: Optional[float] = None

        elapsed_days = max(0.001, (ts - self._day_start_ts) / 86400.0)
        if cfg.tariff_demand_rate > 0.0 and self._md_kva > 0.0:
            demand_charge_est = round(
                self._md_kva * cfg.tariff_demand_rate * (elapsed_days / 30.0), 2
            )

        if cfg.tou_enabled:
            # TOU energy billing: split today_kWh into peak / off-peak
            if cfg.tou_peak_rate_lkr_kwh > 0.0 and tou_peak_kwh is not None:
                tou_peak_charge = round(tou_peak_kwh * cfg.tou_peak_rate_lkr_kwh, 2)
            if cfg.tou_offpeak_rate_lkr_kwh > 0.0 and tou_offpeak_kwh is not None:
                tou_offpeak_charge = round(tou_offpeak_kwh * cfg.tou_offpeak_rate_lkr_kwh, 2)
            # Sum available charges for energy_charge_est (used by billing tile total)
            parts = [c for c in (tou_peak_charge, tou_offpeak_charge) if c is not None]
            energy_charge_est = round(sum(parts), 2) if parts else None
            # Optional separate demand charge for peak-period MD
            if cfg.tou_peak_demand_rate > 0.0 and self._tou_peak_md_kva > 0.0:
                tou_peak_demand_charge = round(
                    self._tou_peak_md_kva * cfg.tou_peak_demand_rate * (elapsed_days / 30.0), 2
                )
        else:
            if cfg.tariff_energy_rate > 0.0 and kwh is not None and kwh > 0.0:
                energy_charge_est = round(kwh * cfg.tariff_energy_rate, 2)

        # ── 10. Assemble result ───────────────────────────────────────────────
        return {
            # ---- Live readings ----
            "demand_kva":              round(kva, 2)  if kva  is not None else None,
            "demand_kw":               round(kw, 2)   if kw   is not None else None,
            "demand_kvar":             round(kvar, 2) if kvar is not None else None,
            # ---- Rolling interval demand (CEA 15/30-min window) ----
            "demand_interval_kva":     round(di_kva, 2),
            "demand_interval_kw":      round(di_kw,  2),
            "demand_interval_min":     cfg.demand_interval_min,
            # ---- Today's MD peaks ----
            "md_kva_today":            round(self._md_kva, 2),
            "md_kw_today":             round(self._md_kw,  2),
            "md_utilisation_pct":      md_util_pct,
            "contract_demand_kva":     cfg.contract_demand_kva,
            # ---- Load factor (BEE India) ----
            "load_factor_pct":         load_factor_pct,
            "load_factor_status":      lf_status,
            # ---- Power factor (CERC/SERC / PUCSL) ----
            "pf_now":                  pf_now,
            "pf_avg_today":            pf_avg,
            "pf_min_today":            pf_min,
            "pf_status":               pf_status,
            "pf_penalty_threshold":    cfg.pf_penalty_threshold,
            "pf_incentive_threshold":  cfg.pf_incentive_threshold,
            # ---- Reactive power & compensation ----
            "kvar_now":                round(kvar, 2) if kvar is not None else None,
            "capacitor_kvar_rec":      cap_rec,
            # ---- Power quality (IEEE 519) ----
            "thd_v_avg_pct":           thd_v,
            "thd_i_avg_pct":           thd_i,
            "thd_v_limit_pct":         THD_V_LIMIT_PCT,
            "thd_i_limit_pct":         THD_I_LIMIT_PCT,
            # ---- TOU / peak-schedule ----
            "tou_enabled":             cfg.tou_enabled,
            "tou_in_peak":             in_peak,
            "tou_peak_start":          cfg.tou_peak_start,
            "tou_peak_end":            cfg.tou_peak_end,
            "tou_peak_kwh":            tou_peak_kwh,
            "tou_offpeak_kwh":         tou_offpeak_kwh,
            "tou_peak_md_kva":         round(self._tou_peak_md_kva, 2),
            # ---- Billing estimates ----
            "demand_charge_est":       demand_charge_est,
            "energy_charge_est":       energy_charge_est,
            "tou_peak_charge":         tou_peak_charge,
            "tou_offpeak_charge":      tou_offpeak_charge,
            "tou_peak_demand_charge":  tou_peak_demand_charge,
            "tariff_demand_rate":      cfg.tariff_demand_rate,
            "tariff_energy_rate":      cfg.tariff_energy_rate,
            # ---- Meta ----
            "warnings":                warns,
        }

    def reset_daily(self) -> None:
        """Reset all daily accumulators.  Call at midnight."""
        self._reset_state()

    def reconfigure(self, config: LoadPlantConfig) -> None:
        """Apply new plant configuration without resetting daily accumulators."""
        self.config = config

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _reset_state(self) -> None:
        self._day_start_ts: float = time.time()
        self._win_kva: deque = deque()   # (ts, kva) rolling demand window
        self._win_kw:  deque = deque()   # (ts, kw)  rolling demand window
        self._md_kva:  float = 0.0       # today's peak MD [kVA]
        self._md_kw:   float = 0.0       # today's peak MD [kW]
        self._pf_wsum:   float = 0.0     # kVA-weighted PF sum
        self._pf_wcount: float = 0.0     # kVA weight accumulator
        self._pf_min:    float = 1.1     # today's minimum PF (sentinel > 1)
        # TOU accumulators — kWh estimated from live kW integration
        # These are approximations (trapezoidal) since the meter only gives
        # Today_kWh as a single register.  They track the *proportion* of
        # energy consumed during the peak / off-peak windows and split the
        # meter's Today_kWh reading proportionally for billing estimate.
        self._tou_peak_kw_samples:    int   = 0   # sample count inside peak
        self._tou_offpeak_kw_samples: int   = 0   # sample count outside peak
        self._tou_peak_kw_sum:        float = 0.0  # kW sum inside peak
        self._tou_offpeak_kw_sum:     float = 0.0  # kW sum outside peak
        self._tou_peak_md_kva:        float = 0.0  # peak-period MD [kVA]

    @staticmethod
    def _expire(window: deque, now: float, max_age_s: float) -> None:
        """Remove samples older than max_age_s from the left of the deque."""
        while window and (now - window[0][0]) > max_age_s:
            window.popleft()

    @staticmethod
    def _parse_hhmm(s: str) -> Tuple[int, int]:
        """
        Parse a "HH:MM" string to (hour, minute).
        Returns (0, 0) on any parse error — safe fallback.
        """
        try:
            parts = str(s).strip().split(":")
            return int(parts[0]) % 24, int(parts[1]) % 60
        except (IndexError, ValueError):
            return 0, 0

    @classmethod
    def _is_peak(cls, cfg: "LoadPlantConfig", ts: float) -> bool:
        """
        Return True if *ts* (epoch) falls within the configured TOU peak window.

        Handles overnight wrap (e.g. peak_start="22:00", peak_end="06:00").
        """
        if not cfg.tou_enabled:
            return False
        dt = datetime.fromtimestamp(ts)
        now_min = dt.hour * 60 + dt.minute
        sh, sm = cls._parse_hhmm(cfg.tou_peak_start)
        eh, em = cls._parse_hhmm(cfg.tou_peak_end)
        start_min = sh * 60 + sm
        end_min   = eh * 60 + em
        if start_min <= end_min:
            # Normal window (e.g. 18:30 – 22:30)
            return start_min <= now_min < end_min
        else:
            # Overnight wrap (e.g. 22:00 – 06:00)
            return now_min >= start_min or now_min < end_min

    @staticmethod
    def _fval(values: Dict[str, Any], key: str) -> Optional[float]:
        """Safe float extraction from values dict."""
        if not key or not values:
            return None
        v = values.get(key)
        if v is None:
            return None
        try:
            f = float(v)
            return f if math.isfinite(f) else None
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _thd_avg(values: Dict[str, Any], keys: List[str]) -> Optional[float]:
        """Average of THD registers that are present and finite."""
        vals = []
        for k in keys:
            v = values.get(k)
            if v is None:
                continue
            try:
                f = float(v)
                if math.isfinite(f) and f >= 0.0:
                    vals.append(f)
            except (TypeError, ValueError):
                pass
        return round(sum(vals) / len(vals), 2) if vals else None

    @classmethod
    def cfg_to_config(cls, cfg: dict) -> "LoadPlantConfig":
        """Build a LoadPlantConfig from the application cfg dict."""
        lp = cfg.get("load_plant") or {}
        return LoadPlantConfig(
            contract_demand_kva      = float(lp.get("contract_demand_kva",      0.0)),
            demand_interval_min      = int(  lp.get("demand_interval_min",      15)),
            pf_penalty_threshold     = float(lp.get("pf_penalty_threshold",     PF_PENALTY_DEFAULT)),
            pf_incentive_threshold   = float(lp.get("pf_incentive_threshold",   PF_INCENTIVE_DEFAULT)),
            tariff_demand_rate       = float(lp.get("tariff_demand_rate",        0.0)),
            tariff_energy_rate       = float(lp.get("tariff_energy_rate",        0.0)),
            power_key                = str(  lp.get("power_key",                "kW")),
            apparent_power_key       = str(  lp.get("apparent_power_key",       "kVA")),
            pf_key                   = str(  lp.get("pf_key",                   "PF")),
            energy_key               = str(  lp.get("energy_key",               "Today_kWh")),
            # TOU
            tou_enabled              = bool( lp.get("tou_enabled",              False)),
            tou_peak_start           = str(  lp.get("tou_peak_start",           "18:30")),
            tou_peak_end             = str(  lp.get("tou_peak_end",             "22:30")),
            tou_peak_rate_lkr_kwh    = float(lp.get("tou_peak_rate_lkr_kwh",    0.0)),
            tou_offpeak_rate_lkr_kwh = float(lp.get("tou_offpeak_rate_lkr_kwh", 0.0)),
            tou_peak_demand_rate     = float(lp.get("tou_peak_demand_rate",      0.0)),
        )
