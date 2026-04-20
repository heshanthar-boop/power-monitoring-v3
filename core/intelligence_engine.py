from __future__ import annotations

import math
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from core.analytics_engine import PlantAnalyticsEngine
from utils.health import meter_is_fresh, stale_seconds


@dataclass
class IntelligenceFinding:
    severity: str
    asset: str
    prediction: str
    evidence: str
    likely_cause: str
    action: str
    page: str = ""
    confidence: str = "MEDIUM"


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


def _first_value(values: Dict[str, Any], *keys: str) -> Optional[float]:
    for key in keys:
        val = _f((values or {}).get(key))
        if val is not None:
            return val
    return None


def _pct_unbalance(values: List[float]) -> Optional[float]:
    vals = [float(v) for v in values if _f(v) is not None and abs(float(v)) > 0.001]
    if len(vals) < 3:
        return None
    avg = sum(vals) / len(vals)
    if abs(avg) <= 0.001:
        return None
    return max(abs(v - avg) for v in vals) / abs(avg) * 100.0


def _quality_text(value: Any) -> str:
    try:
        if hasattr(value, "value"):
            return str(value.value or "").upper()
        return str(value or "").upper()
    except Exception:
        return ""


def _severity_rank(severity: str) -> int:
    sev = str(severity or "").upper()
    return {"CRITICAL": 0, "WARN": 1, "INFO": 2}.get(sev, 3)


class MaintenanceIntelligenceEngine:
    """Rule-based predictive maintenance intelligence for field reliability.

    This engine intentionally avoids fake black-box AI. It makes explainable
    predictions from live readings, Modbus health counters, historian health,
    remote-sync status, and existing alarm state.
    """

    def __init__(self, cfg: Optional[Dict[str, Any]] = None) -> None:
        self.cfg = cfg or {}
        self.analytics = PlantAnalyticsEngine(self.cfg)

    def analyze(
        self,
        meters: List[Any],
        *,
        total: Optional[Dict[str, Any]] = None,
        worker: Any = None,
        alarm_engine: Any = None,
        logging_engine: Any = None,
        remote_sync: Any = None,
    ) -> Dict[str, Any]:
        cfg = self.cfg or {}
        icfg = cfg.get("intelligence", {}) or {}
        meters = list(meters or [])
        enabled = [m for m in meters if bool(getattr(m, "enabled", True))]
        stale_sec = stale_seconds(cfg)
        live = [m for m in enabled if self._meter_live(m, stale_sec)]
        findings: List[IntelligenceFinding] = []

        total = total if isinstance(total, dict) else {}
        analytics = self.analytics.analyze(meters, total)
        bus = self._safe_bus_health(worker)
        meter_comm = self._safe_meter_comm_health(worker)
        log_health = self._safe_logging_health(logging_engine)
        sync_health = self._safe_remote_health(remote_sync)

        self._check_data_availability(findings, enabled, live)
        self._check_bus_health(findings, bus, icfg)
        self._check_meter_comm(findings, enabled, meter_comm, icfg)
        self._check_electrical_stress(findings, live, total, icfg)
        self._check_demand_trend(findings, analytics, icfg)
        self._check_alarms(findings, alarm_engine)
        self._check_historian(findings, log_health)
        self._check_remote_sync(findings, sync_health, icfg)

        asset_rows = self._asset_rows(enabled, live, meter_comm, icfg)
        score = self._readiness_score(findings, live_count=len(live), enabled_count=len(enabled))
        findings.sort(key=lambda item: (_severity_rank(item.severity), item.asset, item.prediction))

        return {
            "ts": time.time(),
            "readiness_score": score,
            "state": self._score_state(score),
            "enabled_meter_count": len(enabled),
            "live_meter_count": len(live),
            "data_confidence_pct": round((len(live) / len(enabled) * 100.0), 1) if enabled else 0.0,
            "analytics_score": analytics.get("score"),
            "bus": bus,
            "log_health": log_health,
            "remote_sync": sync_health,
            "top_action": self._top_action(findings),
            "findings": [finding.__dict__ for finding in findings],
            "assets": asset_rows,
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

    def _safe_bus_health(self, worker: Any) -> Dict[str, Any]:
        if worker is None or not hasattr(worker, "get_bus_health"):
            return {}
        try:
            return worker.get_bus_health() or {}
        except Exception:
            return {}

    def _safe_meter_comm_health(self, worker: Any) -> Dict[int, Dict[str, Any]]:
        if worker is None or not hasattr(worker, "get_meter_comm_health"):
            return {}
        try:
            raw = worker.get_meter_comm_health() or {}
            return {int(k): dict(v or {}) for k, v in raw.items()}
        except Exception:
            return {}

    def _safe_logging_health(self, logging_engine: Any) -> Dict[str, Any]:
        if logging_engine is None:
            return {}
        try:
            wh = getattr(logging_engine, "write_health", None)
            if callable(wh):
                return dict(wh() or {})
            if isinstance(wh, dict):
                return dict(wh)
        except Exception:
            return {}
        return {}

    def _safe_remote_health(self, remote_sync: Any) -> Dict[str, Any]:
        if remote_sync is None or not hasattr(remote_sync, "health"):
            return {}
        try:
            return dict(remote_sync.health() or {})
        except Exception:
            return {}

    def _check_data_availability(
        self,
        findings: List[IntelligenceFinding],
        enabled: List[Any],
        live: List[Any],
    ) -> None:
        if not enabled:
            findings.append(IntelligenceFinding(
                "CRITICAL", "Plant", "Predictive maintenance unavailable",
                "No enabled meters are configured.",
                "Site setup is incomplete.",
                "Open Communication and configure the three site meters.",
                "comm", "HIGH",
            ))
            return
        if not live:
            findings.append(IntelligenceFinding(
                "CRITICAL", "Plant", "No live data for intelligence",
                "All enabled meters are stale, waiting, or offline.",
                "RS-485 adapter, COM selection, meter address, or wiring issue.",
                "Open Communication and run Auto Setup; then check A/B wiring and slave IDs.",
                "comm", "HIGH",
            ))
            return
        if len(live) < len(enabled):
            findings.append(IntelligenceFinding(
                "WARN", "Plant", "Partial monitoring coverage",
                f"{len(live)} of {len(enabled)} enabled meters are live.",
                "One panel may be offline or responding too slowly.",
                "Open Meter Status and inspect the missing meter before trusting total kW.",
                "status", "HIGH",
            ))

    def _check_bus_health(self, findings: List[IntelligenceFinding], bus: Dict[str, Any], icfg: Dict[str, Any]) -> None:
        if not bus:
            return
        state = str(bus.get("state", "") or "").upper()
        fail_count = int(bus.get("bus_fail_count", 0) or 0)
        poll_ok = int(bus.get("poll_ok", 0) or 0)
        poll_fail = int(bus.get("poll_fail", 0) or 0)
        latency = _f(bus.get("latency_ms"))
        latency_warn = float(icfg.get("comm_latency_warn_ms", 900.0) or 900.0)

        if state in {"RECOVERING", "STALE", "OFFLINE"}:
            findings.append(IntelligenceFinding(
                "WARN" if state != "OFFLINE" else "CRITICAL",
                "RS-485 Bus",
                "Communication reliability degradation",
                f"Bus state={state}, consecutive bus failures={fail_count}, last error={bus.get('last_error', '')}",
                "Loose RS-485 terminals, missing adapter, wrong COM port, poor grounding, or long cable noise.",
                "Open Communication and use Auto Setup; check shield/termination if this repeats.",
                "comm", "HIGH",
            ))
        elif fail_count > 0:
            findings.append(IntelligenceFinding(
                "INFO", "RS-485 Bus", "Intermittent bus failures observed",
                f"Current consecutive bus failure counter is {fail_count}.",
                "Short noise bursts or a slow/slave meter retry may be starting.",
                "Watch the Communication page; if failures increase, inspect A/B wiring and termination.",
                "comm", "MEDIUM",
            ))

        if latency is not None and latency > latency_warn:
            findings.append(IntelligenceFinding(
                "WARN", "RS-485 Bus", "Slow Modbus responses trending risky",
                f"EWMA latency is {latency:.0f} ms; warning threshold is {latency_warn:.0f} ms.",
                "Weak USB-RS485 adapter, noisy cable, wrong baud/timeout, or overloaded slave segment.",
                "Lower scan rate or inspect adapter/cable; use one 120 ohm termination at the line end.",
                "comm", "MEDIUM",
            ))

        total_polls = poll_ok + poll_fail
        if total_polls >= 20:
            fail_rate = poll_fail / max(1, total_polls) * 100.0
            fail_warn = float(icfg.get("poll_fail_rate_warn_pct", 8.0) or 8.0)
            if fail_rate > fail_warn:
                findings.append(IntelligenceFinding(
                    "WARN", "RS-485 Bus", "Poll failure rate is too high",
                    f"Poll fail rate is {fail_rate:.1f}% ({poll_fail}/{total_polls}).",
                    "The link may work now but is not reliable enough for unattended logging.",
                    "Check RS-485 polarity, cable route near VFD/inverter wiring, and retry/timeout settings.",
                    "comm", "HIGH",
                ))

    def _check_meter_comm(
        self,
        findings: List[IntelligenceFinding],
        enabled: List[Any],
        meter_comm: Dict[int, Dict[str, Any]],
        icfg: Dict[str, Any],
    ) -> None:
        consec_warn = int(icfg.get("meter_consecutive_fail_warn", 3) or 3)
        fail_rate_warn = float(icfg.get("meter_fail_rate_warn_pct", 12.0) or 12.0)
        latency_warn = float(icfg.get("comm_latency_warn_ms", 900.0) or 900.0)

        for meter in enabled:
            mid = int(getattr(meter, "meter_id", 0) or 0)
            info = meter_comm.get(mid, {})
            if not info:
                continue
            name = str(info.get("name") or getattr(meter, "name", f"Meter {mid}"))
            consecutive = int(info.get("consecutive_fail", 0) or 0)
            fail_count = int(info.get("fail_count", 0) or 0)
            ok_count = int(info.get("ok_count", 0) or 0)
            latency = _f(info.get("latency_ms"))
            if consecutive >= consec_warn:
                findings.append(IntelligenceFinding(
                    "WARN", name, "Meter may drop offline soon",
                    f"{consecutive} consecutive failures; last error={info.get('last_error', '')}",
                    "Slave address collision, terminal looseness, meter power dip, or cable noise.",
                    "Inspect this meter first; verify slave ID, RS-485 A/B, and panel supply.",
                    "status", "HIGH",
                ))
            total = ok_count + fail_count
            if total >= 10:
                fail_rate = fail_count / max(1, total) * 100.0
                if fail_rate > fail_rate_warn:
                    findings.append(IntelligenceFinding(
                        "WARN", name, "Meter communication is unreliable",
                        f"Failure rate is {fail_rate:.1f}% ({fail_count}/{total}).",
                        "This meter is likely reducing total data confidence and future reports.",
                        "Check meter wiring and consider increasing timeout only after physical checks.",
                        "status", "HIGH",
                    ))
            if latency is not None and latency > latency_warn:
                findings.append(IntelligenceFinding(
                    "INFO", name, "Meter response is slow",
                    f"Meter latency is {latency:.0f} ms.",
                    "Slow response can precede timeouts on weak RS-485 lines.",
                    "Check scan interval, baud rate, and cable quality for this meter.",
                    "status", "MEDIUM",
                ))

    def _check_electrical_stress(
        self,
        findings: List[IntelligenceFinding],
        live: List[Any],
        total: Dict[str, Any],
        icfg: Dict[str, Any],
    ) -> None:
        current_imb_warn = float(icfg.get("current_unbalance_warn_pct", 20.0) or 20.0)
        voltage_imb_warn = float(icfg.get("voltage_unbalance_warn_pct", 2.5) or 2.5)
        pf_warn = float(icfg.get("pf_warn", 0.90) or 0.90)
        thd_v_warn = float(icfg.get("thd_v_warn_pct", 5.0) or 5.0)
        thd_i_warn = float(icfg.get("thd_i_warn_pct", 10.0) or 10.0)

        total_v_unbalance = _pct_unbalance([
            _first_value(total, "V1N", "VL1N"),
            _first_value(total, "V2N", "VL2N"),
            _first_value(total, "V3N", "VL3N"),
        ])
        if total_v_unbalance is not None and total_v_unbalance > voltage_imb_warn:
            findings.append(IntelligenceFinding(
                "WARN", "Plant", "Voltage imbalance can stress equipment",
                f"Voltage phase unbalance is {total_v_unbalance:.1f}%; warning threshold is {voltage_imb_warn:.1f}%.",
                "Uneven phase loading, loose neutral, upstream supply imbalance, or PT/meter mapping issue.",
                "Check phase voltages at the panel and compare against meter wiring.",
                "status", "MEDIUM",
            ))

        for meter in live:
            values = getattr(meter, "values", {}) or {}
            name = str(getattr(meter, "name", f"Meter {getattr(meter, 'meter_id', '?')}") or "")
            current_unbalance = _pct_unbalance([
                _first_value(values, "I1", "I_L1", "Current_L1"),
                _first_value(values, "I2", "I_L2", "Current_L2"),
                _first_value(values, "I3", "I_L3", "Current_L3"),
            ])
            if current_unbalance is not None and current_unbalance > current_imb_warn:
                findings.append(IntelligenceFinding(
                    "WARN", name, "Current imbalance can overheat wiring",
                    f"Current phase unbalance is {current_unbalance:.1f}%; warning threshold is {current_imb_warn:.1f}%.",
                    "Single-phase load imbalance, loose terminal, CT issue, or failing load branch.",
                    "Inspect phase currents and panel terminals; redistribute loads if imbalance is real.",
                    "status", "MEDIUM",
                ))

            pf = _first_value(values, "PFavg", "PF", "PowerFactor")
            kw = abs(_first_value(values, "kW", "Total_kW", "ActivePower_Total") or 0.0)
            if pf is not None and abs(pf) < pf_warn and kw > 0.5:
                findings.append(IntelligenceFinding(
                    "INFO", name, "Power factor may cause energy penalty",
                    f"PF={pf:.3f} while load is {kw:.2f} kW.",
                    "Reactive load, APFC/capacitor issue, or CT direction problem.",
                    "Check capacitor/APFC status and confirm CT direction before adjusting settings.",
                    "analytics", "MEDIUM",
                ))

            thd_v = _first_value(values, "THD_V", "THD_V_worst", "THD_V1", "THD Voltage L1")
            thd_i = _first_value(values, "THD_I", "THD_I_worst", "THD_I1", "THD Current L1")
            if thd_v is not None and thd_v > thd_v_warn:
                findings.append(IntelligenceFinding(
                    "WARN", name, "Voltage harmonics may reduce equipment life",
                    f"Voltage THD={thd_v:.1f}%; warning threshold is {thd_v_warn:.1f}%.",
                    "Inverter/VFD harmonic injection or weak filtering.",
                    "Inspect harmonic filters and compare readings during inverter/load changes.",
                    "analytics", "MEDIUM",
                ))
            if thd_i is not None and thd_i > thd_i_warn:
                findings.append(IntelligenceFinding(
                    "WARN", name, "Current harmonics can heat cables/transformers",
                    f"Current THD={thd_i:.1f}%; warning threshold is {thd_i_warn:.1f}%.",
                    "Non-linear loads, VFD/inverter switching, or undersized neutral/filtering.",
                    "Check harmonic source and cable/neutral temperature if available.",
                    "analytics", "MEDIUM",
                ))

    def _check_demand_trend(
        self,
        findings: List[IntelligenceFinding],
        analytics: Dict[str, Any],
        icfg: Dict[str, Any],
    ) -> None:
        demand = analytics.get("demand", {}) or {}
        forecast = _f(demand.get("forecast_kw"))
        current = _f(demand.get("current_kw"))
        trend = _f(demand.get("trend_kw_per_min"))
        if forecast is None or current is None:
            return
        rise_pct = ((forecast - current) / abs(current) * 100.0) if abs(current) > 0.5 else 0.0
        rise_warn = float(icfg.get("forecast_rise_warn_pct", 20.0) or 20.0)
        trend_warn = float(icfg.get("kw_ramp_warn_per_min", 5.0) or 5.0)
        if rise_pct > rise_warn or (trend is not None and trend > trend_warn):
            findings.append(IntelligenceFinding(
                "INFO", "Demand", "Load is rising faster than normal",
                f"Current={current:.2f} kW, forecast={forecast:.2f} kW, trend={trend or 0.0:.2f} kW/min.",
                "Process load increase, motor start sequence, or abnormal auxiliary load.",
                "Watch demand for the next 15 minutes; compare Control Panel and Auxiliary contribution.",
                "analytics", "MEDIUM",
            ))

    def _check_alarms(self, findings: List[IntelligenceFinding], alarm_engine: Any) -> None:
        if alarm_engine is None:
            return
        try:
            active = getattr(alarm_engine, "active", {}) or {}
            active_count = len(active)
        except Exception:
            active_count = 0
        try:
            unacked = int(getattr(alarm_engine, "unacknowledged_count", 0) or 0)
        except Exception:
            unacked = 0
        if active_count > 0:
            findings.append(IntelligenceFinding(
                "WARN", "Alarms", "Active alarms increase failure risk",
                f"{active_count} active alarm(s), {unacked} unacknowledged.",
                "Operator response may be delayed or root cause is not cleared.",
                "Open Active Alarms, acknowledge, and resolve the highest severity alarm first.",
                "alarms", "HIGH",
            ))

    def _check_historian(self, findings: List[IntelligenceFinding], log_health: Dict[str, Any]) -> None:
        if not log_health:
            return
        queue_depth = int(log_health.get("queue_depth", 0) or 0)
        dropped = int(log_health.get("dropped_write_count", 0) or 0)
        if dropped > 0:
            findings.append(IntelligenceFinding(
                "WARN", "Historian", "Data loss has already occurred",
                f"Historian dropped write count is {dropped}.",
                "Disk, SQLite, permissions, or write interval pressure.",
                "Open Data Logger and verify the storage path, interval, and disk free space.",
                "logging", "HIGH",
            ))
        elif queue_depth > 25:
            findings.append(IntelligenceFinding(
                "INFO", "Historian", "Historian write queue is building",
                f"Current historian queue depth is {queue_depth}.",
                "Disk writes may be slower than sampling rate.",
                "Watch queue depth; increase logging interval if it keeps rising.",
                "logging", "MEDIUM",
            ))

    def _check_remote_sync(
        self,
        findings: List[IntelligenceFinding],
        sync_health: Dict[str, Any],
        icfg: Dict[str, Any],
    ) -> None:
        if not sync_health:
            return
        queue_depth = int(sync_health.get("queue_depth", 0) or 0)
        max_queue = int(sync_health.get("max_queue", 0) or 0)
        dropped = int(sync_health.get("dropped_count", 0) or 0)
        mqtt_state = str(sync_health.get("mqtt_state", "") or "").upper()
        warn_pct = float(icfg.get("remote_queue_warn_pct", 50.0) or 50.0)
        queue_pct = (queue_depth / max_queue * 100.0) if max_queue > 0 else 0.0
        if dropped > 0:
            findings.append(IntelligenceFinding(
                "WARN", "Remote Sync", "Remote monitoring data was dropped",
                f"Dropped={dropped}, queue={queue_depth}/{max_queue}, MQTT={mqtt_state}.",
                "Internet/broker outage lasted longer than the configured offline queue capacity.",
                "Open Connectivity, restore MQTT/internet, and increase remote_sync.max_queue if needed.",
                "connectivity", "HIGH",
            ))
        elif queue_pct >= warn_pct and queue_depth > 0:
            findings.append(IntelligenceFinding(
                "INFO", "Remote Sync", "Remote queue is filling",
                f"Queue={queue_depth}/{max_queue} ({queue_pct:.0f}%), MQTT={mqtt_state}.",
                "Internet or MQTT broker is unavailable or slow.",
                "Open Connectivity and check broker/network before the queue reaches capacity.",
                "connectivity", "MEDIUM",
            ))

    def _asset_rows(
        self,
        enabled: List[Any],
        live: List[Any],
        meter_comm: Dict[int, Dict[str, Any]],
        icfg: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        live_ids = {int(getattr(m, "meter_id", 0) or 0) for m in live}
        rows: List[Dict[str, Any]] = []
        latency_warn = float(icfg.get("comm_latency_warn_ms", 900.0) or 900.0)
        for meter in enabled:
            mid = int(getattr(meter, "meter_id", 0) or 0)
            info = meter_comm.get(mid, {})
            consecutive = int(info.get("consecutive_fail", getattr(meter, "consecutive_fail", 0)) or 0)
            fail_count = int(info.get("fail_count", getattr(meter, "fail_count", 0)) or 0)
            ok_count = int(info.get("ok_count", getattr(meter, "poll_count", 0)) or 0)
            latency = _f(info.get("latency_ms"))
            risk = 0
            reasons: List[str] = []
            if mid not in live_ids:
                risk += 50
                reasons.append("no live data")
            if consecutive:
                risk += min(30, consecutive * 8)
                reasons.append(f"{consecutive} fail streak")
            total = ok_count + fail_count
            if total >= 10:
                fail_rate = fail_count / max(1, total) * 100.0
                if fail_rate > 12.0:
                    risk += min(25, int(fail_rate / 2))
                    reasons.append(f"{fail_rate:.0f}% fail rate")
            if latency is not None and latency > latency_warn:
                risk += 10
                reasons.append(f"{latency:.0f}ms latency")
            risk = max(0, min(100, risk))
            rows.append({
                "meter_id": mid,
                "name": str(getattr(meter, "name", f"Meter {mid}") or f"Meter {mid}"),
                "status": "Live" if mid in live_ids else "No Data",
                "risk_score": risk,
                "risk": self._asset_risk_label(risk),
                "reason": ", ".join(reasons) or "normal",
                "action": "Inspect meter wiring/settings" if risk >= 50 else "Watch" if risk >= 20 else "No action",
            })
        rows.sort(key=lambda row: int(row.get("risk_score", 0) or 0), reverse=True)
        return rows

    def _readiness_score(self, findings: List[IntelligenceFinding], live_count: int, enabled_count: int) -> int:
        score = 100
        for finding in findings:
            sev = str(finding.severity).upper()
            if sev == "CRITICAL":
                score -= 35
            elif sev == "WARN":
                score -= 15
            else:
                score -= 5
        if enabled_count:
            score -= min(25, max(0, enabled_count - live_count) * 10)
        return max(0, min(100, int(score)))

    def _score_state(self, score: int) -> str:
        if score >= 85:
            return "Reliable"
        if score >= 65:
            return "Watch"
        if score >= 40:
            return "Maintenance Needed"
        return "Critical"

    def _asset_risk_label(self, score: int) -> str:
        if score >= 70:
            return "High"
        if score >= 35:
            return "Medium"
        if score >= 10:
            return "Low"
        return "Normal"

    def _top_action(self, findings: List[IntelligenceFinding]) -> str:
        if not findings:
            return "No immediate maintenance action. Continue normal monitoring."
        first = sorted(findings, key=lambda item: _severity_rank(item.severity))[0]
        return first.action
