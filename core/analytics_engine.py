from __future__ import annotations

import math
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from core.snapshot_bus import MeterQuality
from utils.health import meter_is_fresh, stale_seconds


@dataclass
class AnalyticsIssue:
    severity: str
    area: str
    title: str
    detail: str
    action: str
    page: str = ""


def _f(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        out = float(value)
        if math.isnan(out) or math.isinf(out):
            return None
        return out
    except Exception:
        return None


def _abs(value: Any) -> Optional[float]:
    val = _f(value)
    return abs(val) if val is not None else None


def _quality_text(value: Any) -> str:
    try:
        if isinstance(value, MeterQuality):
            return value.value
        return str(value or "").upper()
    except Exception:
        return ""


class PlantAnalyticsEngine:
    """Fast, UI-safe analytics for live SCADA snapshots.

    The engine reads only in-memory meter values. It never calls Modbus or disk.
    """

    def __init__(self, cfg: Optional[Dict[str, Any]] = None):
        self.cfg = cfg or {}

    def analyze(self, meters: List[Any], total: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        meters = list(meters or [])
        cfg = self.cfg or {}
        acfg = cfg.get("analytics", {}) or {}
        stale_sec = stale_seconds(cfg)
        now = time.time()

        enabled = [m for m in meters if bool(getattr(m, "enabled", True))]
        live = [m for m in enabled if self._meter_live(m, stale_sec)]
        issues: List[AnalyticsIssue] = []

        if not enabled:
            issues.append(AnalyticsIssue(
                "CRITICAL", "Data", "No active meters configured",
                "Analytics cannot evaluate the plant because every meter is disabled or missing.",
                "Open Communication and enable the site meters.", "comm",
            ))
        elif not live:
            issues.append(AnalyticsIssue(
                "CRITICAL", "Data", "No live meter data",
                "All enabled meters are waiting, stale, or offline.",
                "Use Communication > Auto Setup or check RS-485 wiring and COM port.", "comm",
            ))
        elif len(live) < len(enabled):
            issues.append(AnalyticsIssue(
                "WARN", "Data", "Partial meter data",
                f"{len(live)} of {len(enabled)} enabled meters are live.",
                "Open Meter Status to identify the missing meter before trusting totals.", "status",
            ))

        total = total or {}
        plant_kw = _f(total.get("Total_kW", total.get("kW")))
        plant_kva = _abs(total.get("Total_kVA", total.get("kVA")))
        pf = _f(total.get("PF_total", total.get("PFavg", total.get("PF"))))
        freq = _f(total.get("Freq_ref", total.get("Frequency")))
        vavg = _f(total.get("Vavg_ref", total.get("Vavg")))
        thd_v = _f(total.get("THD_V_worst", total.get("THD_V")))
        thd_i = _f(total.get("THD_I_worst", total.get("THD_I")))

        signed = self._signed_power_split(live)
        contributions = self._meter_contributions(live)
        demand = self._forecast_demand(live, horizon_min=float(acfg.get("forecast_horizon_min", 15.0) or 15.0))

        self._check_frequency(freq, issues, acfg)
        self._check_voltage(vavg, total, issues, acfg)
        self._check_power_factor(pf, issues, acfg)
        self._check_thd(thd_v, thd_i, issues, acfg)
        self._check_add_deduct_balance(signed, issues, acfg)
        self._check_contribution(contributions, issues, acfg)

        if demand.get("forecast_kw") is None:
            issues.append(AnalyticsIssue(
                "INFO", "Forecast", "Demand forecast warming up",
                "More live samples are needed before a reliable 15-minute trend can be shown.",
                "Keep polling and logging enabled; forecast improves after a few minutes.", "logging",
            ))

        score = self._score(issues, live_count=len(live), enabled_count=len(enabled))
        return {
            "ts": now,
            "score": score,
            "state": self._score_state(score),
            "plant_kw": plant_kw,
            "plant_kva": plant_kva,
            "pf": pf,
            "frequency": freq,
            "vavg": vavg,
            "thd_v": thd_v,
            "thd_i": thd_i,
            "enabled_meter_count": len(enabled),
            "live_meter_count": len(live),
            "data_confidence_pct": round((len(live) / len(enabled) * 100.0), 1) if enabled else 0.0,
            "signed_power": signed,
            "contributions": contributions,
            "demand": demand,
            "issues": [issue.__dict__ for issue in issues],
        }

    def _meter_live(self, meter: Any, stale_sec: float) -> bool:
        if not bool(getattr(meter, "enabled", True)):
            return False
        if not bool(getattr(meter, "data_valid", False)):
            return False
        if not meter_is_fresh(meter, stale_sec):
            return False
        q = _quality_text(getattr(meter, "quality", ""))
        return q in ("GOOD", "OK", "ONLINE", "LIVE", "")

    def _signed_power_split(self, meters: List[Any]) -> Dict[str, float]:
        add_kw = 0.0
        deduct_kw = 0.0
        net_kw = 0.0
        for m in meters:
            val = _f((getattr(m, "values", {}) or {}).get("kW"))
            if val is None:
                continue
            sign = -1.0 if float(getattr(m, "total_sign", 1.0) or 1.0) < 0 else 1.0
            if sign < 0:
                deduct_kw += abs(val)
            else:
                add_kw += abs(val)
            net_kw += sign * val
        ratio = (deduct_kw / add_kw * 100.0) if add_kw > 0.001 else 0.0
        return {
            "add_kw": add_kw,
            "deduct_kw": deduct_kw,
            "net_kw": net_kw,
            "deduct_pct_of_add": ratio,
        }

    def _meter_contributions(self, meters: List[Any]) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        values = []
        for m in meters:
            kw = _f((getattr(m, "values", {}) or {}).get("kW"))
            if kw is None:
                continue
            values.append(abs(kw))
        total_abs = sum(values)
        for m in meters:
            kw = _f((getattr(m, "values", {}) or {}).get("kW"))
            if kw is None:
                continue
            sign = -1.0 if float(getattr(m, "total_sign", 1.0) or 1.0) < 0 else 1.0
            pct = (abs(kw) / total_abs * 100.0) if total_abs > 0.001 else 0.0
            rows.append({
                "meter_id": int(getattr(m, "meter_id", 0) or 0),
                "name": str(getattr(m, "name", "") or f"Meter {getattr(m, 'meter_id', '?')}"),
                "role": "DEDUCT" if sign < 0 else "ADD",
                "kw": kw,
                "signed_kw": sign * kw,
                "contribution_pct": pct,
                "pf": _f((getattr(m, "values", {}) or {}).get("PFavg", (getattr(m, "values", {}) or {}).get("PF"))),
                "vavg": _f((getattr(m, "values", {}) or {}).get("Vavg")),
            })
        rows.sort(key=lambda r: abs(float(r.get("signed_kw", 0.0) or 0.0)), reverse=True)
        return rows

    def _forecast_demand(self, meters: List[Any], horizon_min: float = 15.0) -> Dict[str, Any]:
        horizon_sec = max(60.0, float(horizon_min) * 60.0)
        now = time.time()
        sample_count = 0
        widest_span = 0.0
        current_kw = 0.0
        forecast_kw = 0.0
        usable_meters = 0
        for m in meters:
            sign = -1.0 if float(getattr(m, "total_sign", 1.0) or 1.0) < 0 else 1.0
            tb = getattr(m, "trend_kw", None)
            ts_list = list(getattr(tb, "t", []) or [])
            y_list = list(getattr(tb, "y", []) or [])
            samples: List[tuple[float, float]] = []
            for ts, val in zip(ts_list, y_list):
                fts = _f(ts)
                fval = _f(val)
                if fts is None or fval is None:
                    continue
                if now - fts <= 30 * 60:
                    samples.append((fts, sign * fval))

            if len(samples) < 2:
                continue

            samples.sort(key=lambda item: item[0])
            first_ts, first_kw = samples[0]
            last_ts, last_kw = samples[-1]
            span = max(1.0, float(last_ts - first_ts))
            slope_kw_per_sec = (last_kw - first_kw) / span
            current_kw += last_kw
            forecast_kw += last_kw + slope_kw_per_sec * horizon_sec
            sample_count += len(samples)
            widest_span = max(widest_span, span)
            usable_meters += 1

        if usable_meters <= 0 or sample_count < 4:
            return {"forecast_kw": None, "trend_kw_per_min": None, "confidence": "LOW", "sample_count": sample_count}

        trend_per_min = (forecast_kw - current_kw) / (horizon_sec / 60.0)
        confidence = "HIGH" if sample_count >= 20 and widest_span >= 5 * 60 else "MEDIUM" if widest_span >= 2 * 60 else "LOW"
        return {
            "current_kw": current_kw,
            "forecast_kw": forecast_kw,
            "trend_kw_per_min": trend_per_min,
            "confidence": confidence,
            "sample_count": sample_count,
            "sample_span_sec": widest_span,
            "usable_meter_count": usable_meters,
            "horizon_min": float(horizon_sec / 60.0),
        }

    def _check_frequency(self, freq: Optional[float], issues: List[AnalyticsIssue], acfg: Dict[str, Any]) -> None:
        if freq is None:
            return
        low = float(acfg.get("freq_min_hz", 49.5) or 49.5)
        high = float(acfg.get("freq_max_hz", 50.5) or 50.5)
        if freq < low or freq > high:
            issues.append(AnalyticsIssue(
                "WARN", "Power Quality", "Frequency outside normal band",
                f"Measured frequency is {freq:.2f} Hz. Expected band is {low:.2f}-{high:.2f} Hz.",
                "Check CEB/grid stability, generator synchronization, or inverter grid settings.", "status",
            ))

    def _check_voltage(self, vavg: Optional[float], total: Dict[str, Any], issues: List[AnalyticsIssue], acfg: Dict[str, Any]) -> None:
        if vavg is not None:
            if vavg >= 300.0:
                low = float(acfg.get("voltage_ll_min_v", 360.0) or 360.0)
                high = float(acfg.get("voltage_ll_max_v", 440.0) or 440.0)
                label = "L-L"
            else:
                low = float(acfg.get("voltage_ln_min_v", 207.0) or 207.0)
                high = float(acfg.get("voltage_ln_max_v", 253.0) or 253.0)
                label = "L-N"
            if vavg < low or vavg > high:
                issues.append(AnalyticsIssue(
                    "WARN", "Power Quality", "Voltage outside normal band",
                    f"Average {label} voltage is {vavg:.1f} V. Expected band is {low:.0f}-{high:.0f} V.",
                    "Check grid voltage, transformer tap, loose neutral, and meter PT settings.", "status",
                ))

        phases = [_f(total.get(k)) for k in ("V1N", "V2N", "V3N")]
        phases = [v for v in phases if v is not None and v > 1.0]
        if len(phases) == 3:
            avg = sum(phases) / 3.0
            unbalance = (max(abs(v - avg) for v in phases) / avg * 100.0) if avg else 0.0
            warn = float(acfg.get("voltage_unbalance_warn_pct", 2.0) or 2.0)
            if unbalance > warn:
                issues.append(AnalyticsIssue(
                    "WARN", "Power Quality", "Voltage phase unbalance",
                    f"Voltage unbalance is {unbalance:.1f}%. Recommended limit is {warn:.1f}%.",
                    "Check phase loading, loose terminals, CT/PT mapping, and upstream supply imbalance.", "status",
                ))

    def _check_power_factor(self, pf: Optional[float], issues: List[AnalyticsIssue], acfg: Dict[str, Any]) -> None:
        if pf is None:
            return
        warn = float(acfg.get("pf_warn", 0.90) or 0.90)
        target = float(acfg.get("pf_target", 0.95) or 0.95)
        if abs(pf) < warn:
            issues.append(AnalyticsIssue(
                "WARN", "Efficiency", "Low power factor",
                f"Power factor is {pf:.3f}. Target is {target:.2f}+.",
                "Check capacitor bank/APFC, reactive load, CT direction, and tariff penalty risk.", "analytics",
            ))

    def _check_thd(self, thd_v: Optional[float], thd_i: Optional[float], issues: List[AnalyticsIssue], acfg: Dict[str, Any]) -> None:
        v_warn = float(acfg.get("thd_v_warn_pct", 5.0) or 5.0)
        i_warn = float(acfg.get("thd_i_warn_pct", 8.0) or 8.0)
        if thd_v is not None and thd_v > v_warn:
            issues.append(AnalyticsIssue(
                "WARN", "Power Quality", "High voltage THD",
                f"Voltage THD is {thd_v:.1f}%. Recommended warning limit is {v_warn:.1f}%.",
                "Inspect non-linear loads, inverter harmonics, filters, and meter harmonic setup.", "analytics",
            ))
        if thd_i is not None and thd_i > i_warn:
            issues.append(AnalyticsIssue(
                "WARN", "Power Quality", "High current THD",
                f"Current THD is {thd_i:.1f}%. Recommended warning limit is {i_warn:.1f}%.",
                "Check VFD/inverter loads, harmonic filters, neutral heating, and CT sizing.", "analytics",
            ))

    def _check_add_deduct_balance(self, signed: Dict[str, float], issues: List[AnalyticsIssue], acfg: Dict[str, Any]) -> None:
        ratio = float(signed.get("deduct_pct_of_add", 0.0) or 0.0)
        warn = float(acfg.get("deduct_ratio_warn_pct", 15.0) or 15.0)
        if ratio > warn:
            issues.append(AnalyticsIssue(
                "INFO", "Energy Balance", "Deduct meter is a large share of total",
                f"Deducted load is {ratio:.1f}% of added power.",
                "Confirm this is expected. If not, check meter add/deduct role in Communication.", "comm",
            ))

    def _check_contribution(self, rows: List[Dict[str, Any]], issues: List[AnalyticsIssue], acfg: Dict[str, Any]) -> None:
        warn = float(acfg.get("single_meter_share_warn_pct", 85.0) or 85.0)
        for row in rows[:1]:
            pct = float(row.get("contribution_pct", 0.0) or 0.0)
            if pct > warn and len(rows) > 1:
                issues.append(AnalyticsIssue(
                    "INFO", "Energy Balance", "Single meter dominates total",
                    f"{row.get('name')} contributes {pct:.1f}% of live kW.",
                    "Confirm meter selection and CT scaling if this is not expected.", "comm",
                ))

    def _score(self, issues: List[AnalyticsIssue], live_count: int, enabled_count: int) -> int:
        score = 100
        for issue in issues:
            sev = str(issue.severity).upper()
            if sev == "CRITICAL":
                score -= 35
            elif sev == "WARN":
                score -= 15
            else:
                score -= 5
        if enabled_count:
            missing = max(0, enabled_count - live_count)
            score -= min(30, missing * 10)
        return max(0, min(100, int(score)))

    def _score_state(self, score: int) -> str:
        if score >= 85:
            return "Healthy"
        if score >= 65:
            return "Watch"
        if score >= 40:
            return "At Risk"
        return "Critical"
