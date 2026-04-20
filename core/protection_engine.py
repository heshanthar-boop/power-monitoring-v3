"""
Protection Monitoring Engine (SCADA supervision; NOT relay-grade tripping)

Evaluates slow supervision using meter snapshots:
- OV/UV (from Vavg)
- OF/UF (from Frequency)
- ROCOF (approx, smoothed)
- Voltage Unbalance % (if V1N/V2N/V3N exists)
- THD-V, THD-I (worst-case)
- PF low
- Optional: kVAr limit, Overcurrent warn, Reverse power

Industrial rules:
- Stale data suppression ON by default (no false alarms from comm loss)
- 3 states: NORMAL / WARNING / ALARM (+ SUPPRESSED / UNKNOWN)
- Ack allowed, manual clear only
"""

import time
import re
import threading
from functools import wraps
import json
import csv
import os
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, List, Tuple

from utils.paths import logs_dir


def _locked(method):
    @wraps(method)
    def wrapper(self, *args, **kwargs):
        lock = getattr(self, "_lock", None)
        if lock is None:
            return method(self, *args, **kwargs)
        with lock:
            return method(self, *args, **kwargs)
    return wrapper


DEFAULTS = {
    # Site baselines
    "nominal_f": 50.0,
    "nominal_vln": 230.0,

    # Engine behavior
    "stale_suppression": True,

    # Sri Lanka-friendly starting points (voltage/frequency only)
    "UV_pickup_pu": 0.90,
    "UV_delay": 5.0,

    "OV_pickup_pu": 1.10,
    "OV_delay": 2.0,

    # Phase loss / low phase (Vmin)
    "PHASELOSS_pickup_pu": 0.70,
    "PHASELOSS_delay": 1.5,

    # Voltage unbalance %
    "UNBAL_pickup": 4.0,
    "UNBAL_delay": 5.0,

    # Frequency
    "UF_pickup": 47.5,
    "UF_delay": 5.0,
    "OF_pickup": 52.5,
    "OF_delay": 5.0,

    # Phase asymmetry (Vmax - Vmin) in volts
    "ASYM_pickup_v": 40.0,
    "ASYM_delay": 5.0,

    # Voltage instability: |ΔVavg| over 10 seconds (alarm-grade)
    "VSTEP10_pickup_v": 20.0,
    "VSTEP10_delay": 10.0,

    # Sustained deviation band (maintenance-grade)
    "SUST_band_low_pu": 0.95,
    "SUST_band_high_pu": 1.05,
    "SUST_delay": 600.0,
}


# Parameters that can be used by Protection rules (for full UI customization)
# Parameters allowed for voltage-only protection rules.
# NOTE: Derived params (Vmin/Vmax/etc.) are computed by this engine from V1N/V2N/V3N.
PARAM_SPECS = {
    'Vavg': {'label':'Vavg','unit':'V','keys':['Vavg','Average Voltage L-N','Average Voltage (V)','Average Voltage LN','Average Voltage']},
    'V1N': {'label':'V1N','unit':'V','keys':['V1N','Voltage V1N','Voltage V1','V1']},
    'V2N': {'label':'V2N','unit':'V','keys':['V2N','Voltage V2N','Voltage V2','V2']},
    'V3N': {'label':'V3N','unit':'V','keys':['V3N','Voltage V3N','Voltage V3','V3']},
    'Frequency': {'label':'Frequency','unit':'Hz','keys':['Frequency','Freq','Hz']},

    'Iavg': {'label':'Iavg','unit':'A','keys':['Iavg','Average Current','Average Current (A)','Avg Current']},
    'I1': {'label':'I1','unit':'A','keys':['I1','Current I1','Current 1']},
    'I2': {'label':'I2','unit':'A','keys':['I2','Current I2','Current 2']},
    'I3': {'label':'I3','unit':'A','keys':['I3','Current I3','Current 3']},

    'kW': {'label':'kW','unit':'kW','keys':['kW','Active Power (kW)','Active Power']},
    'kVA': {'label':'kVA','unit':'kVA','keys':['kVA','Apparent Power (kVA)','Apparent Power']},
    'kVAr': {'label':'kVAr','unit':'kVAr','keys':['kVAr','Reactive Power (kVAr)','Reactive Power','kvar']},
    'PF': {'label':'PF','unit':'','keys':['PF','PFavg','Power Factor']},

    'Vmin': {'label':'Vmin','unit':'V','keys':None},
    'Vmax': {'label':'Vmax','unit':'V','keys':None},
    'VmaxVmin': {'label':'Vmax−Vmin','unit':'V','keys':None},
    'Unbalance': {'label':'Voltage Unbalance %','unit':'%','keys':None},
    'Vstep10s': {'label':'|ΔVavg| (10s)','unit':'V','keys':None},

    'Imin': {'label':'Imin','unit':'A','keys':None},
    'Imax': {'label':'Imax','unit':'A','keys':None},
    'ImaxImin': {'label':'Imax−Imin','unit':'A','keys':None},
    'Iunbalance': {'label':'Current Unbalance %','unit':'%','keys':None},

    'Fpkpk10s': {'label':'Freq pk-pk (10s)','unit':'Hz','keys':None},
    'Fstd10s': {'label':'Freq std-dev (10s)','unit':'Hz','keys':None},

    'KWstep10s': {'label':'|ΔkW| (10s)','unit':'kW','keys':None},
    'KVAstep10s': {'label':'|ΔkVA| (10s)','unit':'kVA','keys':None},
    'PFstep10s': {'label':'|ΔPF| (10s)','unit':'','keys':None},

    'SagCount60s': {'label':'Sag count (60s)','unit':'','keys':None},
    'SwellCount60s': {'label':'Swell count (60s)','unit':'','keys':None},
    'VstepCount60s': {'label':'V step count (60s)','unit':'','keys':None},

    'DataValid': {'label':'Data Valid','unit':'','keys':None},
}

def _f(x) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def _get_any(values: Dict[str, Any], keys: List[str]) -> Optional[float]:
    for k in keys:
        if k in values:
            v = _f(values.get(k))
            if v is not None:
                return v
    return None


@dataclass
class RuleRuntime:
    state: str = "NORMAL"  # NORMAL/WARNING/ALARM/SUPPRESSED/UNKNOWN
    start_violation_ts: Optional[float] = None
    last_event_ts: Optional[float] = None
    last_value: Optional[float] = None
    time_in_violation: float = 0.0
    acked: bool = False
    cleared: bool = False


@dataclass
class ProtectionRule:
    rid: str
    function_code: str
    display_name: str
    # Scope: TOTAL or M1..M6 (meter id). Persisted via protection_rule_flags[RID]['scope']
    scope: str
    # Which parameter (live value) this rule supervises. Persisted via protection_rule_flags[RID]['param']
    param_key: str

    # Classification (for UI filtering)
    group: str = 'Voltage'
    grade: str = 'Protection'
    tier: int = 1

    # Compare mode: 'HIGH' means alarm when value >= pickup_high; 'LOW' means alarm when value <= pickup_low
    compare_mode: str = 'HIGH'
    pickup_high: Optional[float] = None
    pickup_low: Optional[float] = None
    delay_sec: float = 1.0
    enabled: bool = True

    # IMPORTANT: Python 3.13 requires default_factory for mutable defaults
    rt: RuleRuntime = field(default_factory=RuleRuntime)


class ProtectionEngine:
    def __init__(self, cfg: dict):
        self.cfg = cfg
        self._lock = threading.RLock()
        self.rules: List[ProtectionRule] = []

        # In-memory event history (latest first).
        # Keep Protection and Smart Advisory separate so each tab can have its
        # own history window and separate CSV output.
        self._event_history_by_stream: Dict[str, deque] = {
            "protection": deque(maxlen=2000),
            "advisory": deque(maxlen=2000),
        }
        # ROCOF history per scope to support per-meter ROCOF supervision
        self._rocof_state: Dict[str, dict] = {}

        # Cache of latest evaluation results.
        # MainWindow ticks the engine even when the Protection/Smart tabs are not visible.
        # Tabs must render from this cache (avoid calling tick() again) to prevent double CPU work.
        self._last_tick_ts: float = 0.0
        self._last_rows: List[dict] = []
        self._last_summary: dict = {}
        self._last_events: List[dict] = []

        # AlarmEngine bridge — wired via set_alarm_bridge() after construction.
        self._alarm_engine = None

        self.reload_from_cfg()

    @_locked
    def set_alarm_bridge(self, alarm_engine) -> None:
        """Wire the AlarmEngine so protection state transitions appear in the Alarms tab.

        Call this once after construction (see main_window.py).  Safe to call
        with None to disable bridging.  AlarmEngine already handles its own
        EventJournal writes, so no separate journal wiring is needed here.
        """
        self._alarm_engine = alarm_engine

    @_locked
    def get_last_result(self) -> Tuple[List[dict], List[dict], dict]:
        """Return the most recent tick() result.

        Tabs use this to render without re-running tick() (which is executed in MainWindow._ui_tick).
        """
        return list(self._last_rows), list(self._last_events), dict(self._last_summary)

    @_locked
    def get_recent_events(self, since_ts: float, *, include_advisory: bool = True) -> List[dict]:
        """Return recent protection/advisory events (newest-first).

        Safe for UI callers (no mutation). This lets charts place markers
        without re-running tick() or touching private deques directly.
        """
        out: List[dict] = []
        try:
            s = float(since_ts)
        except Exception:
            s = 0.0

        streams = ["protection"]
        if include_advisory:
            streams.append("advisory")

        for stream in streams:
            dq = self._event_history_by_stream.get(stream)
            if not dq:
                continue
            # deque is newest-first
            for ev in dq:
                try:
                    if float(ev.get("ts") or 0.0) < s:
                        break
                except Exception:
                    continue
                out.append(dict(ev))

        # keep newest-first overall
        out.sort(key=lambda e: float(e.get("ts") or 0.0), reverse=True)
        return out

    # -------- Event logging (per stream) --------
    def _events_cfg(self, stream: str = "protection") -> dict:
        """Return (and lazily create) event logging config for a stream.

        Streams:
          - 'protection' => Tier-1 (Protection tab)
          - 'advisory'   => Tier-2 (Smart Advisory tab)
        """
        stream = (stream or "protection").strip().lower()
        key = "protection_events" if stream != "advisory" else "advisory_events"
        default_name = "protection_events.csv" if stream != "advisory" else "advisory_events.csv"

        ecfg = self.cfg.setdefault(key, {})
        if "enabled" not in ecfg:
            ecfg["enabled"] = True
        if "csv_path" not in ecfg or not str(ecfg.get("csv_path") or "").strip():
            ecfg["csv_path"] = os.path.join(logs_dir(), default_name)
        return ecfg

    def get_events_cfg(self, stream: str = "protection") -> dict:
        return dict(self._events_cfg(stream))

    def set_events_cfg(self, stream: str = "protection", *, enabled: Optional[bool] = None, csv_path: Optional[str] = None) -> None:
        ecfg = self._events_cfg(stream)
        if enabled is not None:
            ecfg["enabled"] = bool(enabled)
        if csv_path is not None:
            csv_path = str(csv_path).strip()
            if csv_path:
                ecfg["csv_path"] = csv_path

    @_locked
    def get_event_history(self, stream: str = "protection") -> List[dict]:
        """Latest events in memory (newest first) for a stream."""
        stream = (stream or "protection").strip().lower()
        stream = "advisory" if stream == "advisory" else "protection"
        return list(self._event_history_by_stream.get(stream, deque()))

    @_locked
    def clear_event_history(self, stream: str = "protection") -> None:
        stream = (stream or "protection").strip().lower()
        stream = "advisory" if stream == "advisory" else "protection"
        dq = self._event_history_by_stream.get(stream)
        if dq is not None:
            dq.clear()

    def _should_log_event(self, rid: str, stream: str) -> bool:
        ecfg = self._events_cfg(stream)
        if not bool(ecfg.get("enabled", True)):
            return False
        flags = self.get_rule_flags(rid)
        return bool(flags.get("log_to_history", True))

    def _append_event_csv(self, ev: dict, stream: str) -> None:
        ecfg = self._events_cfg(stream)
        path = str(ecfg.get("csv_path") or "").strip()
        if not path:
            return

        # Ensure folder exists.
        folder = os.path.dirname(path)
        if folder:
            os.makedirs(folder, exist_ok=True)

        header = [
            "ts_iso",
            "ts_epoch",
            "rid",
            "code",
            "param",
            "scope",
            "compare",
            "state",
            "value",
            "pickup",
            "delay_s",
        ]
        write_header = (not os.path.exists(path)) or (os.path.getsize(path) == 0)
        with open(path, "a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            if write_header:
                w.writerow(header)
            ts = float(ev.get("ts") or time.time())
            w.writerow([
                str(ev.get("ts_iso") or time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))),
                f"{ts:.3f}",
                ev.get("rid", ""),
                ev.get("code", ""),
                ev.get("param", ""),
                ev.get("scope", ""),
                ev.get("compare", ""),
                ev.get("state", ""),
                "" if ev.get("value") is None else f"{float(ev.get('value')):.6g}",
                "" if (ev.get("pickup_high") in (None, "") and ev.get("pickup_low") in (None, "")) else (
                    f"{float(ev.get('pickup_high') if ev.get('pickup_high') not in (None, '') else ev.get('pickup_low')):.6g}"
                ),
                "" if ev.get("delay_s") in (None, "") else f"{float(ev.get('delay_s')):.6g}",
            ])

    def _scope_to_meter_id(self, scope: str):
        """Map a protection scope string to an AlarmEngine meter_id.

        'TOTAL' -> 'TOTAL'
        'M1'    -> 1
        'M2'    -> 2  ... etc.
        Anything else is returned as-is.
        """
        s = str(scope or "TOTAL").strip().upper()
        if s == "TOTAL":
            return "TOTAL"
        if s.startswith("M") and s[1:].isdigit():
            return int(s[1:])
        return s

    def _bridge_to_alarm_engine(self, ev: dict) -> None:
        """Forward a protection state-transition event to AlarmEngine.

        RAISE on WARNING/ALARM; CLEAR on NORMAL.
        The AlarmEngine handles deduplication and EventJournal writes internally.
        """
        if self._alarm_engine is None:
            return

        state = str(ev.get("state") or "NORMAL").upper()
        rid = str(ev.get("rid") or "")
        if not rid:
            return

        meter_id = self._scope_to_meter_id(ev.get("scope", "TOTAL"))
        # Namespace the code so protection alarms are distinguishable from
        # comm/quality alarms in the Alarms tab (e.g. PROT_UV_TOTAL).
        alarm_code = f"PROT_{rid}"

        if state in ("WARNING", "ALARM"):
            severity = "ALARM" if state == "ALARM" else "WARN"
            value = ev.get("value")
            val_str = f" = {value:.4g}" if isinstance(value, (int, float)) else ""
            message = f"[Protection] {ev.get('name', rid)}{val_str}"
            snapshot = {"param": ev.get("param"), "value": value, "scope": ev.get("scope")}
            try:
                self._alarm_engine._raise(meter_id, alarm_code, message, severity, snapshot=snapshot)
            except Exception:
                pass

        elif state == "NORMAL":
            try:
                self._alarm_engine._clear(meter_id, alarm_code)
            except Exception:
                pass

    def _record_events(self, events: List[dict]) -> None:
        # newest first in memory
        for ev in reversed(events):
            rid = str(ev.get("rid") or "")
            if not rid:
                continue

            # Route to stream based on tier (1=Protection, 2=Smart Advisory)
            tier = int(ev.get("tier") or 1)
            stream = "advisory" if tier == 2 else "protection"

            if not self._should_log_event(rid, stream):
                continue

            dq = self._event_history_by_stream.get(stream)
            if dq is not None:
                dq.appendleft(dict(ev))

            try:
                self._append_event_csv(ev, stream)
            except Exception:
                # CSV failures must never break supervision.
                pass

            # Bridge state transition to AlarmEngine so it appears in Alarms tab.
            self._bridge_to_alarm_engine(ev)

    @_locked
    def get_active_alarm_ranges(self) -> Dict[str, Dict[str, List[tuple[float, float, str]]]]:
        """Return active alarm ranges for chart highlighting.

        Output format:
        {
          'TOTAL': {'Vavg': [(start,end,'#ef4444')], ...},
          'M1':    {'kW':   [(start,end,'#fbbf24')], ...},
        }
        Only includes rules with highlight_chart=True and state in WARNING/ALARM.
        """
        now = time.time()
        out: Dict[str, Dict[str, List[tuple[float, float, str]]]] = {}
        for r in self.rules:
            f = self.get_rule_flags(r.rid)
            if not bool(f.get('highlight_chart', True)):
                continue
            st = str(r.rt.state or 'UNKNOWN')
            if st not in ('WARNING', 'ALARM'):
                continue
            start = r.rt.start_violation_ts or r.rt.last_event_ts or now
            scope = str(r.scope or 'TOTAL').strip().upper()
            key = str(r.param_key or '').strip()
            if not key:
                continue
            color = '#ef4444' if st == 'ALARM' else '#fbbf24'
            out.setdefault(scope, {}).setdefault(key, []).append((float(start), float(now), color))
        return out

    @_locked
    def get_threshold_lines_by_scope(self) -> Dict[str, Dict[str, list]]:
        """Return pickup threshold levels for horizontal chart overlay lines.

        Only rules currently in WARNING or ALARM state are included, so
        the chart only shows threshold lines that are actively violated.

        Format:
            {scope: {param_key: [(level, color, label), ...]}}

        where:
            level : float  — pickup threshold in parameter engineering units
            color : str    — '#ef4444' (ALARM) | '#fbbf24' (WARNING)
            label : str    — human-readable annotation, e.g. "Vavg >= 253.0"
        """
        out: Dict[str, Dict[str, list]] = {}
        for r in self.rules:
            st = str(r.rt.state or '').upper()
            if st not in ('WARNING', 'ALARM'):
                continue
            scope = str(r.scope or 'TOTAL').strip().upper()
            key = str(r.param_key or '').strip()
            if not key:
                continue
            color = '#ef4444' if st == 'ALARM' else '#fbbf24'
            # Emit one threshold line per non-None pickup value
            for level, compare_op in ((r.pickup_high, '>='), (r.pickup_low, '<=')):
                if level is None:
                    continue
                try:
                    level_f = float(level)
                except (TypeError, ValueError):
                    continue
                label = f"{key} {compare_op} {level_f:.4g}"
                out.setdefault(scope, {}).setdefault(key, []).append(
                    (level_f, color, label)
                )
        return out

    # -------- Public controls --------
    @_locked
    def reload_from_cfg(self):
        '''Build Tier-1 (Protection) + Tier-2 (Smart Advisory) rules.

        Tier-1 lives in Protection tab.
        Tier-2 lives in Smart Advisory tab.

        All rules are SCADA-grade supervision (Alarm / Trip-request), not relay-grade tripping.
        '''
        self.rules.clear()
        prot = self.cfg.setdefault('protection', {})
        site = self.cfg.setdefault('site', {})

        # Apply defaults (only if missing)
        for k, v in DEFAULTS.items():
            if k not in prot and k.endswith(('_pickup', '_delay', '_warn', '_alarm', '_pickup_pu', '_limit', '_pu', '_v')):
                prot[k] = v
        if 'stale_suppression' not in prot:
            prot['stale_suppression'] = DEFAULTS['stale_suppression']

        site.setdefault('nominal_f', DEFAULTS['nominal_f'])
        site.setdefault('nominal_vln', DEFAULTS['nominal_vln'])

        nominal_vln = float(site.get('nominal_vln', DEFAULTS['nominal_vln']))

        # --- Voltage thresholds (pu -> V) ---
        uv_v = float(prot.get('UV_pickup_pu', 0.90)) * nominal_vln
        suv_v = float(prot.get('SUV_pickup_pu', 0.85)) * nominal_vln
        ov_v = float(prot.get('OV_pickup_pu', 1.10)) * nominal_vln
        sov_v = float(prot.get('SOV_pickup_pu', 1.15)) * nominal_vln
        pl_v = float(prot.get('PHASELOSS_pickup_pu', 0.70)) * nominal_vln

        band_low_v = float(prot.get('SUST_band_low_pu', 0.95)) * nominal_vln
        band_high_v = float(prot.get('SUST_band_high_pu', 1.05)) * nominal_vln

        # --- Advisory dynamics defaults ---
        prot.setdefault('SAG_pickup_pu', 0.90)
        prot.setdefault('SWELL_pickup_pu', 1.10)
        prot.setdefault('SAGSWELL_window_s', 60.0)
        prot.setdefault('VSTEP_pickup_v', 10.0)
        prot.setdefault('VSTEP_window_s', 60.0)

        # --- Power step windows ---
        prot.setdefault('STEP_window_s', 10.0)

        # Convenience builders
        def R(rid, code, name, scope, param, cmp_mode, hi, lo, delay, group, grade, tier, enabled=True):
            return ProtectionRule(
                rid, code, name,
                group=group, grade=grade, tier=tier,
                scope=scope, param_key=param,
                compare_mode=cmp_mode, pickup_high=hi, pickup_low=lo,
                delay_sec=float(delay), enabled=bool(enabled)
            )

        # -----------------
        # TIER-1 RULES (Protection tab)
        # -----------------
        t1 = []

        # Voltage (Vavg + per-phase)
        t1 += [
            R('UV_TOTAL', 'UV', 'Under Voltage (Vavg)', 'TOTAL', 'Vavg', 'LOW', None, uv_v, prot.get('UV_delay', 5.0), 'Voltage', 'Protection', 1, True),
            R('SUV_TOTAL', 'SUV', 'Severe Under Voltage (Vavg)', 'TOTAL', 'Vavg', 'LOW', None, suv_v, prot.get('SUV_delay', 2.0), 'Voltage', 'Protection', 1, True),
            R('OV_TOTAL', 'OV', 'Over Voltage (Vavg)', 'TOTAL', 'Vavg', 'HIGH', ov_v, None, prot.get('OV_delay', 2.0), 'Voltage', 'Protection', 1, True),
            R('SOV_TOTAL', 'SOV', 'Severe Over Voltage (Vavg)', 'TOTAL', 'Vavg', 'HIGH', sov_v, None, prot.get('SOV_delay', 1.0), 'Voltage', 'Protection', 1, True),

            R('UV_V1_TOTAL', 'UVPH', 'Phase Under Voltage (V1)', 'TOTAL', 'V1N', 'LOW', None, uv_v, prot.get('UV_delay', 5.0), 'Voltage', 'Protection', 1, True),
            R('UV_V2_TOTAL', 'UVPH', 'Phase Under Voltage (V2)', 'TOTAL', 'V2N', 'LOW', None, uv_v, prot.get('UV_delay', 5.0), 'Voltage', 'Protection', 1, True),
            R('UV_V3_TOTAL', 'UVPH', 'Phase Under Voltage (V3)', 'TOTAL', 'V3N', 'LOW', None, uv_v, prot.get('UV_delay', 5.0), 'Voltage', 'Protection', 1, True),

            R('OV_V1_TOTAL', 'OVPH', 'Phase Over Voltage (V1)', 'TOTAL', 'V1N', 'HIGH', ov_v, None, prot.get('OV_delay', 2.0), 'Voltage', 'Protection', 1, True),
            R('OV_V2_TOTAL', 'OVPH', 'Phase Over Voltage (V2)', 'TOTAL', 'V2N', 'HIGH', ov_v, None, prot.get('OV_delay', 2.0), 'Voltage', 'Protection', 1, True),
            R('OV_V3_TOTAL', 'OVPH', 'Phase Over Voltage (V3)', 'TOTAL', 'V3N', 'HIGH', ov_v, None, prot.get('OV_delay', 2.0), 'Voltage', 'Protection', 1, True),

            R('PHASELOSS_TOTAL', 'PHLOSS', 'Phase Loss / Low Phase (Vmin)', 'TOTAL', 'Vmin', 'LOW', None, pl_v, prot.get('PHASELOSS_delay', 1.5), 'Voltage', 'Protection', 1, True),
            R('V_UNBAL_TOTAL', 'VUNB', 'Voltage Unbalance %', 'TOTAL', 'Unbalance', 'HIGH', float(prot.get('UNBAL_pickup', 4.0)), None, prot.get('UNBAL_delay', 5.0), 'Voltage', 'Protection', 1, True),
            R('V_ASYM_TOTAL', 'VASYM', 'Voltage Asymmetry (Vmax−Vmin)', 'TOTAL', 'VmaxVmin', 'HIGH', float(prot.get('ASYM_pickup_v', 40.0)), None, prot.get('ASYM_delay', 5.0), 'Voltage', 'Protection', 1, True),
            R('V_SUST_UV_TOTAL', 'SUSTUV', 'Sustained Low Voltage (Vavg)', 'TOTAL', 'Vavg', 'LOW', None, band_low_v, prot.get('SUST_delay', 600.0), 'Voltage', 'Maintenance', 1, True),
            R('V_SUST_OV_TOTAL', 'SUSTOV', 'Sustained High Voltage (Vavg)', 'TOTAL', 'Vavg', 'HIGH', band_high_v, None, prot.get('SUST_delay', 600.0), 'Voltage', 'Maintenance', 1, True),
        ]

        # Frequency
        t1 += [
            R('UF_TOTAL', 'UF', 'Under Frequency', 'TOTAL', 'Frequency', 'LOW', None, float(prot.get('UF_pickup', 47.5)), prot.get('UF_delay', 5.0), 'Frequency', 'Protection', 1, True),
            R('SUF_TOTAL', 'SUF', 'Severe Under Frequency', 'TOTAL', 'Frequency', 'LOW', None, float(prot.get('SUF_pickup', 47.0)), prot.get('SUF_delay', 2.0), 'Frequency', 'Protection', 1, True),
            R('OF_TOTAL', 'OF', 'Over Frequency', 'TOTAL', 'Frequency', 'HIGH', float(prot.get('OF_pickup', 52.5)), None, prot.get('OF_delay', 5.0), 'Frequency', 'Protection', 1, True),
            R('SOF_TOTAL', 'SOF', 'Severe Over Frequency', 'TOTAL', 'Frequency', 'HIGH', float(prot.get('SOF_pickup', 53.0)), None, prot.get('SOF_delay', 2.0), 'Frequency', 'Protection', 1, True),
            R('F_SUST_TOTAL', 'FSUST', 'Frequency Out-of-Band Sustained', 'TOTAL', 'Frequency', 'HIGH', float(prot.get('FSUST_hi', 51.0)), None, prot.get('FSUST_delay', 600.0), 'Frequency', 'Maintenance', 1, True),
        ]

        # Current (defaults disabled until user config)
        prot.setdefault('OC_pickup', None)
        prot.setdefault('SOC_pickup', None)
        prot.setdefault('IUNB_pickup', 20.0)
        prot.setdefault('IASYM_pickup', None)
        prot.setdefault('I_SUST_pickup', None)
        prot.setdefault('I_SUST_delay', 300.0)
        prot.setdefault('I_LOW_pickup', None)

        t1 += [
            R('OC_TOTAL', 'OC', 'Over Current (Imax)', 'TOTAL', 'Imax', 'HIGH', prot.get('OC_pickup'), None, prot.get('OC_delay', 5.0), 'Current', 'Protection', 1, prot.get('OC_pickup') is not None),
            R('SOC_TOTAL', 'SOC', 'Severe Over Current (Imax)', 'TOTAL', 'Imax', 'HIGH', prot.get('SOC_pickup'), None, prot.get('SOC_delay', 2.0), 'Current', 'Protection', 1, prot.get('SOC_pickup') is not None),
            R('OC_I1_TOTAL', 'OC', 'Phase Over Current (I1)', 'TOTAL', 'I1', 'HIGH', prot.get('OC_pickup'), None, prot.get('OC_delay', 5.0), 'Current', 'Protection', 1, prot.get('OC_pickup') is not None),
            R('OC_I2_TOTAL', 'OC', 'Phase Over Current (I2)', 'TOTAL', 'I2', 'HIGH', prot.get('OC_pickup'), None, prot.get('OC_delay', 5.0), 'Current', 'Protection', 1, prot.get('OC_pickup') is not None),
            R('OC_I3_TOTAL', 'OC', 'Phase Over Current (I3)', 'TOTAL', 'I3', 'HIGH', prot.get('OC_pickup'), None, prot.get('OC_delay', 5.0), 'Current', 'Protection', 1, prot.get('OC_pickup') is not None),
            R('I_UNBAL_TOTAL', 'IUNB', 'Current Unbalance %', 'TOTAL', 'Iunbalance', 'HIGH', float(prot.get('IUNB_pickup', 20.0)), None, prot.get('IUNB_delay', 5.0), 'Current', 'Protection', 1, True),
            R('I_ASYM_TOTAL', 'IASYM', 'Current Asymmetry (Imax−Imin)', 'TOTAL', 'ImaxImin', 'HIGH', prot.get('IASYM_pickup'), None, prot.get('IASYM_delay', 5.0), 'Current', 'Protection', 1, prot.get('IASYM_pickup') is not None),
            R('I_SUST_TOTAL', 'ISUST', 'Sustained High Current (Iavg)', 'TOTAL', 'Iavg', 'HIGH', prot.get('I_SUST_pickup'), None, prot.get('I_SUST_delay', 300.0), 'Current', 'Maintenance', 1, prot.get('I_SUST_pickup') is not None),
        ]

        # Power (defaults disabled until user config)
        prot.setdefault('KW_pickup', None)
        prot.setdefault('SKW_pickup', None)
        prot.setdefault('KVA_pickup', None)
        prot.setdefault('REVKW_pickup', 0.1)

        t1 += [
            R('OP_KW_TOTAL', 'OPKW', 'Over Active Power (kW)', 'TOTAL', 'kW', 'HIGH', prot.get('KW_pickup'), None, prot.get('KW_delay', 5.0), 'Power', 'Protection', 1, prot.get('KW_pickup') is not None),
            R('SOP_KW_TOTAL', 'SOPKW', 'Severe Over kW', 'TOTAL', 'kW', 'HIGH', prot.get('SKW_pickup'), None, prot.get('SKW_delay', 2.0), 'Power', 'Protection', 1, prot.get('SKW_pickup') is not None),
            R('OP_KVA_TOTAL', 'OPKVA', 'Over Apparent Power (kVA)', 'TOTAL', 'kVA', 'HIGH', prot.get('KVA_pickup'), None, prot.get('KVA_delay', 5.0), 'Power', 'Protection', 1, prot.get('KVA_pickup') is not None),
            R('REV_KW_TOTAL', 'REVKW', 'Reverse Active Power (kW<0)', 'TOTAL', 'kW', 'LOW', None, -abs(float(prot.get('REVKW_pickup',0.1))), prot.get('REVKW_delay', 2.0), 'Power', 'Protection', 1, True),
        ]

        # PF (tier1 includes advisory/contract but still in Protection tab as Tier-1 catalog)
        prot.setdefault('PF_low', 0.85)
        prot.setdefault('PF_vlow', 0.75)
        prot.setdefault('PF_lead', -0.95)
        prot.setdefault('PF_pen_delay', 600.0)

        t1 += [
            R('PF_LOW_TOTAL', 'PFLOW', 'Low Power Factor', 'TOTAL', 'PF', 'LOW', None, float(prot.get('PF_low',0.85)), prot.get('PF_delay', 30.0), 'PF', 'Advisory', 1, True),
            R('PF_VLOW_TOTAL', 'PFVLOW', 'Very Low Power Factor', 'TOTAL', 'PF', 'LOW', None, float(prot.get('PF_vlow',0.75)), prot.get('PFv_delay', 30.0), 'PF', 'Advisory', 1, True),
            R('PF_LEAD_TOTAL', 'PFLEAD', 'Leading PF Beyond Limit', 'TOTAL', 'PF', 'LOW', None, float(prot.get('PF_lead', -0.95)), prot.get('PFlead_delay', 30.0), 'PF', 'Advisory', 1, True),
            R('PF_PEN_TOTAL', 'PFPEN', 'PF Penalty Risk (sustained)', 'TOTAL', 'PF', 'LOW', None, float(prot.get('PF_low',0.85)), prot.get('PF_pen_delay', 600.0), 'PF', 'Contract', 1, True),
        ]

        # Data quality
        t1 += [
            R('DATA_VALID_TOTAL', 'DATA', 'Data Valid (TOTAL)', 'TOTAL', 'DataValid', 'LOW', None, 1.0, 1.0, 'Data', 'Protection', 1, True),
        ]

        # -----------------
        # TIER-2 RULES (Smart Advisory tab)
        # -----------------
        t2 = []

        # Voltage dynamics counts (SCADA estimates)
        t2 += [
            R('SAGCNT_TOTAL', 'SAGCNT', 'Fast Voltage Dip Count (60s)', 'TOTAL', 'SagCount60s', 'HIGH', float(prot.get('SAGCNT_pickup', 3.0)), None, 0.0, 'Voltage', 'Advisory', 2, True),
            R('SWELLCNT_TOTAL', 'SWLCNT', 'Fast Voltage Swell Count (60s)', 'TOTAL', 'SwellCount60s', 'HIGH', float(prot.get('SWELLCNT_pickup', 3.0)), None, 0.0, 'Voltage', 'Advisory', 2, True),
            R('VSTEPCNT_TOTAL', 'VSTPCNT', 'Voltage Step Changes (60s)', 'TOTAL', 'VstepCount60s', 'HIGH', float(prot.get('VSTEPCNT_pickup', 6.0)), None, 0.0, 'Voltage', 'Advisory', 2, True),
            R('VSTEP10_TOTAL', 'VSTEP', 'Voltage Instability |ΔVavg| (10s)', 'TOTAL', 'Vstep10s', 'HIGH', float(prot.get('VSTEP10_pickup_v', 20.0)), None, float(prot.get('VSTEP10_delay', 10.0)), 'Voltage', 'Advisory', 2, True),
        ]

        # Frequency instability
        t2 += [
            R('F_PKPK_TOTAL', 'FPKPK', 'Frequency Peak-Peak (10s)', 'TOTAL', 'Fpkpk10s', 'HIGH', float(prot.get('FPKPK_pickup', 0.3)), None, 10.0, 'Frequency', 'Advisory', 2, True),
            R('F_STD_TOTAL', 'FSTD', 'Frequency Std-Dev (10s)', 'TOTAL', 'Fstd10s', 'HIGH', float(prot.get('FSTD_pickup', 0.1)), None, 10.0, 'Frequency', 'Advisory', 2, True),
        ]

        # Power/PF steps
        t2 += [
            R('KW_STEP_TOTAL', 'KWSTEP', 'kW Step Change (10s)', 'TOTAL', 'KWstep10s', 'HIGH', float(prot.get('KWSTEP_pickup', 10.0)), None, 0.0, 'Power', 'Advisory', 2, True),
            R('KVA_STEP_TOTAL', 'KVASTEP', 'kVA Step Change (10s)', 'TOTAL', 'KVAstep10s', 'HIGH', float(prot.get('KVASTEP_pickup', 10.0)), None, 0.0, 'Power', 'Advisory', 2, True),
            R('PF_STEP_TOTAL', 'PFSTEP', 'PF Instability (10s)', 'TOTAL', 'PFstep10s', 'HIGH', float(prot.get('PFSTEP_pickup', 0.10)), None, 0.0, 'PF', 'Advisory', 2, True),
        ]

        # NOTE: Smart combined rules are added later once you confirm thresholds per site.

        self.rules.extend(t1 + t2)

        self._apply_rule_flags()

    def _apply_rule_flags(self):
        flags = self.cfg.setdefault("protection_rule_flags", {}) or {}
        overrides = self.cfg.setdefault("protection_rule_overrides", {}) or {}
        for r in self.rules:
            f = flags.get(r.rid) or {}
            o = overrides.get(r.rid) or {}

            # enabled
            if "enabled" in f:
                r.enabled = bool(f.get("enabled"))

            # scope + parameter
            if "scope" in f and str(f.get("scope") or "").strip():
                r.scope = str(f.get("scope")).strip()
            if "param" in f and str(f.get("param") or "").strip():
                r.param_key = str(f.get("param")).strip()

            # compare mode
            if "compare" in f and str(f.get("compare") or "").strip():
                r.compare_mode = str(f.get("compare")).strip().upper() in ("LOW", "L") and "LOW" or "HIGH"

            # per-rule pickup/delay overrides
            # meta overrides
            if "display_name" in o and str(o.get("display_name") or "").strip():
                r.display_name = str(o.get("display_name")).strip()
            if "code" in o and str(o.get("code") or "").strip():
                r.function_code = str(o.get("code")).strip()

            if "delay_sec" in o and o.get("delay_sec") is not None:
                try:
                    r.delay_sec = float(o.get("delay_sec"))
                except Exception:
                    pass
            # pickup overrides: accept pickup_high/pickup_low or generic pickup + compare
            if "pickup_high" in o and o.get("pickup_high") is not None:
                try:
                    r.pickup_high = float(o.get("pickup_high"))
                    r.pickup_low = None
                    r.compare_mode = "HIGH"
                except Exception:
                    pass
            if "pickup_low" in o and o.get("pickup_low") is not None:
                try:
                    r.pickup_low = float(o.get("pickup_low"))
                    r.pickup_high = None
                    r.compare_mode = "LOW"
                except Exception:
                    pass

    @_locked
    def get_rule_flags(self, rid: str) -> dict:
        flags = self.cfg.setdefault("protection_rule_flags", {})
        f = flags.get(rid) or {}
        # defaults
        if "enabled" not in f:
            # Default to the rule object's current enabled value (some defaults are intentionally disabled).
            r = self._find(rid)
            f["enabled"] = bool(r.enabled) if r else True
        if "log_to_history" not in f:
            f["log_to_history"] = True
        if "highlight_chart" not in f:
            f["highlight_chart"] = True
        # scope/param/compare default are taken from the current rule object in UI via helper
        return dict(f)

    @_locked
    def set_rule_flags(self, rid: str, *, enabled=None, log_to_history=None, highlight_chart=None, scope=None, param=None, compare=None):
        flags = self.cfg.setdefault("protection_rule_flags", {})
        f = flags.get(rid) or {}
        if enabled is not None:
            f["enabled"] = bool(enabled)
        if log_to_history is not None:
            f["log_to_history"] = bool(log_to_history)
        if highlight_chart is not None:
            f["highlight_chart"] = bool(highlight_chart)
        if scope is not None:
            f["scope"] = str(scope).strip()
        if param is not None:
            f["param"] = str(param).strip()
        if compare is not None:
            c = str(compare).strip().upper()
            f["compare"] = "LOW" if c in ("LOW","L","<") else "HIGH"
        flags[rid] = f
        self._apply_rule_flags()

    @_locked
    def update_rule_setpoints(self, rid: str, *, pickup: float = None, delay_sec: float = None, compare: str = None):
        """Per-rule pickup/delay overrides.

        These are stored under cfg['protection_rule_overrides'][RID] so each tile can be tuned independently.
        """
        r = self._find(rid)
        if not r:
            return
        overrides = self.cfg.setdefault("protection_rule_overrides", {})
        o = overrides.get(rid) or {}

        if delay_sec is not None:
            o["delay_sec"] = float(delay_sec)

        if pickup is not None:
            p = float(pickup)
            c = (compare or r.compare_mode or "HIGH").upper()
            if c in ("LOW","L","<"):
                o["pickup_low"] = p
                o.pop("pickup_high", None)
            else:
                o["pickup_high"] = p
                o.pop("pickup_low", None)

        overrides[rid] = o
        self.reload_from_cfg()

    @_locked
    def update_rule_meta(self, rid: str, *, display_name: str | None = None, code: str | None = None):
        """Override the rule's label (and optional code) shown in UI/event history."""
        r = self._find(rid)
        if not r:
            return
        overrides = self.cfg.setdefault("protection_rule_overrides", {})
        o = overrides.get(rid) or {}
        if display_name is not None:
            o["display_name"] = str(display_name).strip()
        if code is not None:
            o["code"] = str(code).strip()
        overrides[rid] = o
        self.reload_from_cfg()

    @_locked
    def ack_rule(self, rid: str):
        r = self._find(rid)
        if r:
            r.rt.acked = True

    @_locked
    def clear_rule(self, rid: str):
        r = self._find(rid)
        if not r:
            return
        if r.rt.state in ("NORMAL", "UNKNOWN", "SUPPRESSED"):
            r.rt.cleared = True

    @_locked
    def save_profile(self, path: str):
        data = {
            'protection': self.cfg.get('protection', {}),
            'protection_rule_flags': self.cfg.get('protection_rule_flags', {}),
            'protection_rule_overrides': self.cfg.get('protection_rule_overrides', {}),
            'protection_events': self.cfg.get('protection_events', {}),
        }
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2)

    @_locked
    def load_profile(self, path: str):
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        if isinstance(data, dict) and 'protection' in data:
            self.cfg['protection'] = data.get('protection', {})
            self.cfg['protection_rule_flags'] = data.get('protection_rule_flags', {})
            self.cfg['protection_rule_overrides'] = data.get('protection_rule_overrides', {})
            self.cfg['protection_events'] = data.get('protection_events', self.cfg.get('protection_events', {}))
        else:
            # backward compatible: profile was just protection dict
            self.cfg['protection'] = data
        self.reload_from_cfg()

    # -------- Tick --------
    # -------- Tick --------
    @_locked
    def tick(self, snapshot: dict) -> Tuple[List[dict], List[dict], dict]:
        """Evaluate voltage-only supervision rules.

        Snapshot format expected (from MainWindow._get_snapshot):
            {
              "total": {"values": {...}, "data_valid": bool, "ts": ...},
              "meters": {1: {"values": {...}, "data_valid": bool, "enabled": bool, "ts": ...}, ...}
            }

        Returns:
            rows:   list[dict] per rule (for optional UI / reports)
            events: list[dict] only on state changes
            summary: dict counts + latest computed params
        """
        now = time.time()
        prot = self.cfg.get("protection", {}) or {}
        site = self.cfg.get("site", {}) or {}

        stale_supp = bool(prot.get("stale_suppression", DEFAULTS["stale_suppression"]))
        nominal_vln = float(site.get("nominal_vln", DEFAULTS["nominal_vln"]))

        total = snapshot.get("total", {}) or {}
        meters = snapshot.get("meters", {}) or {}

        total_vals = (total.get("values", {}) or {})
        total_valid = bool(total.get("data_valid", False))

        # History for 10s voltage step
        if not hasattr(self, "_vavg_hist"):
            self._vavg_hist = {}

        def _phase_voltages(vals: dict):
            v1 = _get_any(vals, ["Voltage V1N", "V1N", "V1"]) 
            v2 = _get_any(vals, ["Voltage V2N", "V2N", "V2"]) 
            v3 = _get_any(vals, ["Voltage V3N", "V3N", "V3"]) 
            if v1 is None or v2 is None or v3 is None:
                return None
            return float(v1), float(v2), float(v3)

        def _compute_vavg(vals: dict):
            vavg = _get_any(vals, PARAM_SPECS["Vavg"]["keys"])
            if vavg is not None:
                return float(vavg)
            ph = _phase_voltages(vals)
            if not ph:
                return None
            v1, v2, v3 = ph
            return (v1 + v2 + v3) / 3.0

        def _compute_phase_derived(vals: dict):
            ph = _phase_voltages(vals)
            if not ph:
                return None, None, None, None
            v1, v2, v3 = ph
            vavg = (v1 + v2 + v3) / 3.0
            vmin = min(v1, v2, v3)
            vmax = max(v1, v2, v3)
            vmaxvmin = vmax - vmin
            unb = None
            if vavg > 0.1:
                d = max(abs(v1 - vavg), abs(v2 - vavg), abs(v3 - vavg))
                unb = (d / vavg) * 100.0
            return vmin, vmax, vmaxvmin, unb

        def _vstep10(scope: str, vavg_now: float):
            scope = str(scope or "TOTAL").strip().upper()
            dq = self._vavg_hist.get(scope)
            if dq is None:
                dq = deque(maxlen=300)
                self._vavg_hist[scope] = dq
            dq.append((now, float(vavg_now)))
            # find value ~10s ago (oldest >=10s back)
            target_ts = now - 10.0
            older = None
            for ts, v in dq:
                if ts <= target_ts:
                    older = (ts, v)
                else:
                    break
            if older is None:
                return None
            return abs(float(vavg_now) - float(older[1]))

        def _params_for_scope(scope: str, src_vals: dict, src_valid: bool):
            # Canonical phase voltages
            ph = _phase_voltages(src_vals)
            v1 = v2 = v3 = None
            if ph:
                v1, v2, v3 = ph
            vavg = _compute_vavg(src_vals)
            freq = _get_any(src_vals, PARAM_SPECS['Frequency']['keys'])

            # Currents
            i1 = _get_any(src_vals, PARAM_SPECS['I1']['keys'])
            i2 = _get_any(src_vals, PARAM_SPECS['I2']['keys'])
            i3 = _get_any(src_vals, PARAM_SPECS['I3']['keys'])
            iavg = _get_any(src_vals, PARAM_SPECS['Iavg']['keys'])
            if iavg is None and None not in (i1, i2, i3):
                iavg = (float(i1) + float(i2) + float(i3)) / 3.0

            # Power / PF
            kw = _get_any(src_vals, PARAM_SPECS['kW']['keys'])
            kva = _get_any(src_vals, PARAM_SPECS['kVA']['keys'])
            kvar = _get_any(src_vals, PARAM_SPECS['kVAr']['keys'])
            pf = _get_any(src_vals, PARAM_SPECS['PF']['keys'])

            # Derived voltage metrics
            vmin, vmax, vmaxvmin, vunb = _compute_phase_derived(src_vals)
            vstep = _vstep10(scope, vavg) if (vavg is not None) else None

            # Derived current metrics
            imin = imax = imaximin = iunb = None
            if None not in (i1, i2, i3):
                i1f, i2f, i3f = float(i1), float(i2), float(i3)
                imin, imax = min(i1f, i2f, i3f), max(i1f, i2f, i3f)
                imaximin = imax - imin
                if (iavg is not None) and float(iavg) > 0.05:
                    d = max(abs(i1f - float(iavg)), abs(i2f - float(iavg)), abs(i3f - float(iavg)))
                    iunb = (d / float(iavg)) * 100.0

            # --- rolling histories (10s window by default) ---
            if not hasattr(self, '_hist'):
                self._hist = {}
            h = self._hist.get(scope)
            if h is None:
                h = {
                    'vavg': deque(maxlen=600),
                    'freq': deque(maxlen=600),
                    'kw': deque(maxlen=600),
                    'kva': deque(maxlen=600),
                    'pf': deque(maxlen=600),
                    'sag_events': deque(maxlen=2000),
                    'swell_events': deque(maxlen=2000),
                    'vstep_events': deque(maxlen=2000),
                }
                self._hist[scope] = h

            def _push(name, val):
                if val is None:
                    return
                h[name].append((now, float(val)))

            _push('freq', freq)
            _push('kw', kw)
            _push('kva', kva)
            _push('pf', pf)
            _push('vavg', vavg)

            step_window = float(prot.get('STEP_window_s', 10.0))
            def _delta_abs(series, window_s):
                if not series or len(series) < 2:
                    return None
                target = now - window_s
                older = None
                for ts, vv in series:
                    if ts <= target:
                        older = (ts, vv)
                    else:
                        break
                if older is None:
                    return None
                return abs(float(series[-1][1]) - float(older[1]))

            kwstep = _delta_abs(h['kw'], step_window)
            kvastep = _delta_abs(h['kva'], step_window)
            pfstep = _delta_abs(h['pf'], step_window)

            # Frequency stats (10s)
            win = 10.0
            target = now - win
            fvals = [vv for ts, vv in h['freq'] if ts >= target]
            fpkpk = fstd = None
            if len(fvals) >= 3:
                fpkpk = max(fvals) - min(fvals)
                mean = sum(fvals) / len(fvals)
                var = sum((x-mean)**2 for x in fvals) / len(fvals)
                fstd = var ** 0.5

            # Tier-2 counters (60s)
            sag_thr = float(prot.get('SAG_pickup_pu', 0.90)) * nominal_vln
            swell_thr = float(prot.get('SWELL_pickup_pu', 1.10)) * nominal_vln
            step_thr = float(prot.get('VSTEP_pickup_v', 10.0))
            count_win = float(prot.get('SAGSWELL_window_s', 60.0))

            # detect sag/swell/step events from vavg
            if vavg is not None:
                if float(vavg) < sag_thr:
                    h['sag_events'].append(now)
                if float(vavg) > swell_thr:
                    h['swell_events'].append(now)
                if vstep is not None and float(vstep) >= step_thr:
                    h['vstep_events'].append(now)

            def _count_recent(evq):
                while evq and evq[0] < now - count_win:
                    evq.popleft()
                return len(evq)

            sag_cnt = _count_recent(h['sag_events'])
            swell_cnt = _count_recent(h['swell_events'])
            vstep_cnt = _count_recent(h['vstep_events'])

            return {
                'Vavg': vavg,
                'V1N': (float(v1) if v1 is not None else None),
                'V2N': (float(v2) if v2 is not None else None),
                'V3N': (float(v3) if v3 is not None else None),
                'Frequency': (float(freq) if freq is not None else None),

                'Iavg': (float(iavg) if iavg is not None else None),
                'I1': (float(i1) if i1 is not None else None),
                'I2': (float(i2) if i2 is not None else None),
                'I3': (float(i3) if i3 is not None else None),

                'kW': (float(kw) if kw is not None else None),
                'kVA': (float(kva) if kva is not None else None),
                'kVAr': (float(kvar) if kvar is not None else None),
                'PF': (float(pf) if pf is not None else None),

                'Vmin': vmin,
                'Vmax': vmax,
                'VmaxVmin': vmaxvmin,
                'Unbalance': vunb,
                'Vstep10s': vstep,

                'Imin': imin,
                'Imax': imax,
                'ImaxImin': imaximin,
                'Iunbalance': iunb,

                'Fpkpk10s': fpkpk,
                'Fstd10s': fstd,

                'KWstep10s': kwstep,
                'KVAstep10s': kvastep,
                'PFstep10s': pfstep,

                'SagCount60s': float(sag_cnt),
                'SwellCount60s': float(swell_cnt),
                'VstepCount60s': float(vstep_cnt),

                'DataValid': (1.0 if src_valid else 0.0),
            }

        # TOTAL derived: if total has no phase voltages, borrow from any valid meter for Vmin/Vmax/Unbalance
        total_params = _params_for_scope("TOTAL", total_vals, total_valid)
        if total_params["Vmin"] is None or total_params["Vmax"] is None or total_params["Unbalance"] is None:
            for ms in meters.values():
                if not ms.get("enabled", True) or not ms.get("data_valid", False):
                    continue
                borrowed = _params_for_scope("TOTAL", (ms.get("values", {}) or {}), True)
                # only fill missing derived values; keep Vavg/freq from TOTAL if present
                for k in ("Vmin", "Vmax", "VmaxVmin", "Unbalance"):
                    if total_params.get(k) is None:
                        total_params[k] = borrowed.get(k)
                break

        events: List[dict] = []
        rows: List[dict] = []

        warn_cnt = 0
        alarm_cnt = 0
        supp_cnt = 0
        unk_cnt = 0

        for r in self.rules:
            # IMPORTANT:
            # Always produce a UI row for every rule (even when disabled / suppressed / unknown).
            # Previous versions used early `continue` statements, which resulted in an empty table.

            scope = str(r.scope or "TOTAL").strip().upper()
            param = str(r.param_key or "").strip()

            # Resolve source by scope
            if scope == "TOTAL":
                src_vals = total_vals
                src_valid = bool(total_valid)
                params = total_params
            else:
                mid_s = re.sub(r"[^0-9]", "", scope)
                mid = int(mid_s) if mid_s else None
                ms = meters.get(mid) if mid is not None else None
                src_vals = (ms.get("values", {}) if ms else {}) or {}
                src_valid = bool(ms and ms.get("enabled", True) and ms.get("data_valid", False))
                params = _params_for_scope(scope, src_vals, src_valid)

            # Decide state + live value
            live = None
            new_state = "UNKNOWN"

            if not r.enabled:
                # Disabled rules still show in the UI.
                new_state = "UNKNOWN"
            else:
                # DataValid must always evaluate (no stale suppression)
                if stale_supp and (not src_valid) and param != "DataValid":
                    new_state = "SUPPRESSED"
                    r.rt.start_violation_ts = None
                    r.rt.time_in_violation = 0.0
                    supp_cnt += 1
                else:
                    live = params.get(param)
                    r.rt.last_value = live

                    if live is None:
                        new_state = "UNKNOWN"
                        r.rt.start_violation_ts = None
                        r.rt.time_in_violation = 0.0
                        unk_cnt += 1
                    else:
                        # Compare
                        violation = False
                        if str(r.compare_mode or "HIGH").upper() == "LOW":
                            if r.pickup_low is not None and float(live) < float(r.pickup_low):
                                violation = True
                        else:
                            if r.pickup_high is not None and float(live) > float(r.pickup_high):
                                violation = True

                        if violation:
                            if r.rt.start_violation_ts is None:
                                r.rt.start_violation_ts = now
                            r.rt.time_in_violation = now - r.rt.start_violation_ts
                            new_state = "ALARM" if r.rt.time_in_violation >= float(r.delay_sec) else "WARNING"
                        else:
                            r.rt.start_violation_ts = None
                            r.rt.time_in_violation = 0.0
                            new_state = "NORMAL"

            # State change event (only if we have a numeric live value to log)
            if new_state != r.rt.state and live is not None:
                r.rt.last_event_ts = now
                ev = {
                    "ts": float(now),
                    "ts_iso": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(now)),
                    "rid": r.rid,
                    "tier": int(getattr(r, "tier", 1) or 1),
                    "name": r.display_name,
                    "code": r.function_code,
                    "param": param,
                    "scope": r.scope,
                    "compare": ("LOW" if str(r.compare_mode or "HIGH").upper() == "LOW" else "HIGH"),
                    "state": new_state,
                    "value": float(live),
                    "pickup_high": ("" if r.pickup_high is None else float(r.pickup_high)),
                    "pickup_low": ("" if r.pickup_low is None else float(r.pickup_low)),
                    "delay_s": float(r.delay_sec),
                }
                events.append(ev)
                if new_state in ("WARNING", "ALARM"):
                    r.rt.cleared = False

            r.rt.state = new_state
            if new_state == "WARNING":
                warn_cnt += 1
            elif new_state == "ALARM":
                alarm_cnt += 1
            elif new_state == "SUPPRESSED":
                # already counted above, but keep totals sane if logic changes
                pass

            # UI row schema MUST match ui/protection_view.py expectations.
            # If keys mismatch, the table will appear blank.
            flags = self.get_rule_flags(r.rid)
            rows.append({
                "id": r.rid,
                "rid": r.rid,
                "name": r.display_name,
                "code": r.function_code,
                "scope": r.scope,
                "param": param,
                "compare": ("LOW" if str(r.compare_mode or "HIGH").upper() == "LOW" else "HIGH"),
                "pickup_high": r.pickup_high,
                "pickup_low": r.pickup_low,
                "delay_sec": float(r.delay_sec),
                "time_in_violation": float(r.rt.time_in_violation or 0.0),
                "state": r.rt.state,
                "live_value": (None if live is None else float(live)),
                "last_event_ts": (None if not r.rt.last_event_ts else float(r.rt.last_event_ts)),
                "acked": bool(r.rt.acked),
                "cleared": bool(r.rt.cleared),
                "log_to_history": bool(flags.get("log_to_history", True)),
                "highlight_chart": bool(flags.get("highlight_chart", True)),
                "enabled": bool(r.enabled),
            })

        summary = {
            "ts": float(now),
            "warning_count": int(warn_cnt),
            "alarm_count": int(alarm_cnt),
            "suppressed_count": int(supp_cnt),
            "unknown_count": int(unk_cnt),
            "active_count": int(warn_cnt + alarm_cnt),
        }

        # Minimal tile-like payload (useful for future UI/report usage)
        tiles = {
            "Vavg": {"value": total_params.get("Vavg"), "unit": "V"},
            "Vmin": {"value": total_params.get("Vmin"), "unit": "V"},
            "Vmax": {"value": total_params.get("Vmax"), "unit": "V"},
            "VmaxVmin": {"value": total_params.get("VmaxVmin"), "unit": "V"},
            "Unbalance": {"value": total_params.get("Unbalance"), "unit": "%"},
            "Frequency": {"value": total_params.get("Frequency"), "unit": "Hz"},
            "Vstep10s": {"value": total_params.get("Vstep10s"), "unit": "V"},
            "DataValid": {"value": total_params.get("DataValid"), "unit": ""},
        }
        summary["tiles"] = tiles

        if events:
            self._record_events(events)

        # Cache latest result for UI views (avoid re-ticking from tabs)
        self._last_tick_ts = float(now)
        self._last_rows = list(rows or [])
        self._last_events = list(events or [])
        self._last_summary = dict(summary or {})
        return rows, events, summary

    @_locked
    def get_last(self) -> Tuple[List[dict], dict, float]:
        """Return (rows, summary, ts_epoch) from the most recent tick()."""
        return list(self._last_rows or []), dict(self._last_summary or {}), float(self._last_tick_ts or 0.0)

    # -------- Internals --------
    @_locked
    def _find(self, rid: str) -> Optional[ProtectionRule]:
        for r in self.rules:
            if r.rid == rid:
                return r
        return None

    def _calc_rocof_scoped(self, scope: str, f_now: Optional[float], now: float) -> Optional[float]:
        """Return median-smoothed ROCOF for the given scope (TOTAL / M1..)."""
        if f_now is None:
            return None
        scope = str(scope or 'TOTAL').strip().upper()
        st = self._rocof_state.get(scope)
        if not st:
            st = {'prev_f': None, 'prev_ts': None, 'hist': deque(maxlen=5)}
            self._rocof_state[scope] = st

        prev_f = st.get('prev_f')
        prev_ts = st.get('prev_ts')
        if prev_f is None or prev_ts is None:
            st['prev_f'] = f_now
            st['prev_ts'] = now
            return None

        dt = now - float(prev_ts)
        if dt < 0.2:
            return None

        rocof = (f_now - float(prev_f)) / dt
        if abs(rocof) > 5.0:
            st['prev_f'] = f_now
            st['prev_ts'] = now
            return None

        st['prev_f'] = f_now
        st['prev_ts'] = now
        st['hist'].append(rocof)
        s = sorted(st['hist'])
        return float(s[len(s) // 2])

    def _calc_unbalance(self, values: Dict[str, Any]) -> Optional[float]:
        v1 = _get_any(values, ["Voltage V1N", "V1N", "V1"])
        v2 = _get_any(values, ["Voltage V2N", "V2N", "V2"])
        v3 = _get_any(values, ["Voltage V3N", "V3N", "V3"])
        if v1 is None or v2 is None or v3 is None:
            return None
        vavg = (v1 + v2 + v3) / 3.0
        if vavg <= 0.1:
            return None
        d = max(abs(v1 - vavg), abs(v2 - vavg), abs(v3 - vavg))
        return (d / vavg) * 100.0

    def _worst_thd(self, meters: Dict[int, dict]) -> Tuple[Optional[float], Optional[float]]:
        worst_v = None
        worst_i = None
        for ms in meters.values():
            if not ms.get("enabled", True):
                continue
            if not ms.get("data_valid", False):
                continue
            vals = ms.get("values", {}) or {}

            thdv = _get_any(vals, ["THD of Voltage V1", "THD-V", "THDV"])
            thdi = _get_any(vals, ["THD of Current I1", "THD-I", "THDI"])

            if thdv is not None:
                worst_v = thdv if worst_v is None else max(worst_v, thdv)
            if thdi is not None:
                worst_i = thdi if worst_i is None else max(worst_i, thdi)

        return worst_v, worst_i

    def _value_for_param(
        self,
        param_key: str,
        src_vals: Dict[str, Any],
        tiles: Dict[str, Any],
        kvar: Optional[float],
        export_kw: Optional[float],
    ) -> Optional[float]:
        k = (param_key or "").strip()
        if not k:
            return None
        if k == "Vavg":
            return _get_any(src_vals, ["Vavg", "Average Voltage L-N", "Average Voltage L-L"]) 
        if k == "Frequency":
            return _get_any(src_vals, ["Frequency"]) 
        if k == "ROCOF":
            return _f(tiles.get("ROCOF", {}).get("value"))
        if k == "Unbalance":
            return _f(tiles.get("Unbalance", {}).get("value"))
        if k == "THD_V":
            return _f(tiles.get("THD_V", {}).get("value"))
        if k == "THD_I":
            return _f(tiles.get("THD_I", {}).get("value"))
        if k == "PF":
            return _get_any(src_vals, ["PFavg", "PF"]) 
        if k == "Iavg":
            return _get_any(src_vals, ["Iavg", "Average Current"]) 
        if k == "kVAr":
            return kvar
        if k == "ExportKW":
            return export_kw
        if k == "kVA_pct":
            return _f(tiles.get("kVA_pct", {}).get("value"))
        return _f(src_vals.get(k))

    def _apply_tile_states(self, tiles: Dict[str, Any], nominal_vln: float):
        prot = self.cfg.get("protection", {})

        uv_v = float(prot.get("UV_pickup_pu", DEFAULTS["UV_pickup_pu"])) * nominal_vln
        ov_v = float(prot.get("OV_pickup_pu", DEFAULTS["OV_pickup_pu"])) * nominal_vln
        uf = float(prot.get("UF_pickup", DEFAULTS["UF_pickup"]))
        of = float(prot.get("OF_pickup", DEFAULTS["OF_pickup"]))
        rocof_warn = float(prot.get("ROCOF_warn", DEFAULTS["ROCOF_warn"]))
        rocof_alarm = float(prot.get("ROCOF_alarm", DEFAULTS["ROCOF_alarm"]))
        unb = float(prot.get("UNBAL_pickup", DEFAULTS["UNBAL_pickup"]))
        thdv_warn = float(prot.get("THDV_warn", DEFAULTS["THDV_warn"]))
        thdv_alarm = float(prot.get("THDV_alarm", DEFAULTS["THDV_alarm"]))
        thdi_warn = float(prot.get("THDI_warn", DEFAULTS["THDI_warn"]))
        thdi_alarm = float(prot.get("THDI_alarm", DEFAULTS["THDI_alarm"]))
        pf_warn = float(prot.get("PF_warn", DEFAULTS["PF_warn"]))
        pf_alarm = float(prot.get("PF_alarm", DEFAULTS["PF_alarm"]))

        def band(x, warn_hi=None, alarm_hi=None, warn_lo=None, alarm_lo=None):
            if x is None:
                return "UNKNOWN"
            if alarm_hi is not None and x >= alarm_hi:
                return "ALARM"
            if alarm_lo is not None and x <= alarm_lo:
                return "ALARM"
            if warn_hi is not None and x >= warn_hi:
                return "WARNING"
            if warn_lo is not None and x <= warn_lo:
                return "WARNING"
            return "NORMAL"

        tiles["Vavg"]["state"] = band(tiles["Vavg"]["value"], warn_hi=0.98 * ov_v, alarm_hi=ov_v,
                                      warn_lo=1.02 * uv_v, alarm_lo=uv_v)
        tiles["Frequency"]["state"] = band(tiles["Frequency"]["value"], warn_hi=0.98 * of, alarm_hi=of,
                                           warn_lo=1.02 * uf, alarm_lo=uf)
        tiles["ROCOF"]["state"] = band(tiles["ROCOF"]["value"], warn_hi=rocof_warn, alarm_hi=rocof_alarm)
        tiles["Unbalance"]["state"] = band(tiles["Unbalance"]["value"], warn_hi=0.9 * unb, alarm_hi=unb)
        tiles["THD_V"]["state"] = band(tiles["THD_V"]["value"], warn_hi=thdv_warn, alarm_hi=thdv_alarm)
        tiles["THD_I"]["state"] = band(tiles["THD_I"]["value"], warn_hi=thdi_warn, alarm_hi=thdi_alarm)
        tiles["PF"]["state"] = band(tiles["PF"]["value"], warn_lo=pf_warn, alarm_lo=pf_alarm)

        tiles["Iavg"]["state"] = "NORMAL" if tiles["Iavg"]["value"] is not None else "UNKNOWN"
        tiles["kVA_pct"]["state"] = "NORMAL" if tiles["kVA_pct"]["value"] is not None else "UNKNOWN"
        tiles["ExportKW"]["state"] = "NORMAL" if tiles["ExportKW"]["value"] is not None else "UNKNOWN"
