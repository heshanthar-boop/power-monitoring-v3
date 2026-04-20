from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Tuple


@dataclass(frozen=True)
class DataQualityIssue:
    key: str
    reason: str
    value: Any

    def short(self) -> str:
        return f"{self.key}: {self.reason}"


def _cfg(cfg: dict) -> dict:
    qcfg = (cfg or {}).get("data_quality", {}) or {}
    return {
        "enabled": bool(qcfg.get("enabled", True)),
        "reject_non_finite": bool(qcfg.get("reject_non_finite", True)),
        "max_abs_voltage_v": float(qcfg.get("max_abs_voltage_v", 1_500_000.0)),
        "max_abs_current_a": float(qcfg.get("max_abs_current_a", 200_000.0)),
        "max_abs_power_kw": float(qcfg.get("max_abs_power_kw", 1_000_000.0)),
        "max_abs_energy_kwh": float(qcfg.get("max_abs_energy_kwh", 10_000_000_000.0)),
        "frequency_min_hz": float(qcfg.get("frequency_min_hz", 40.0)),
        "frequency_max_hz": float(qcfg.get("frequency_max_hz", 70.0)),
        "pf_abs_max": float(qcfg.get("pf_abs_max", 1.05)),
    }


def _is_voltage_key(key: str) -> bool:
    k = key.lower()
    return k in {
        "v1n", "v2n", "v3n", "vavg", "v12", "v23", "v31",
        "voltage_l1_n", "voltage_l2_n", "voltage_l3_n",
        "voltage_l1_l2", "voltage_l2_l3", "voltage_l3_l1",
        "vavg_ref",
    }


def _is_current_key(key: str) -> bool:
    k = key.lower()
    return k in {"i1", "i2", "i3", "iavg", "current_l1", "current_l2", "current_l3", "iavg_ref"}


def _is_frequency_key(key: str) -> bool:
    return key.lower() in {"hz", "freq", "frequency", "freq_ref"}


def _is_pf_key(key: str) -> bool:
    k = key.lower()
    return k in {"pf", "pfavg", "pf_ref", "power_factor", "average_power_factor"} or k.startswith("pf_")


def _is_energy_key(key: str) -> bool:
    k = key.lower()
    return "kwh" in k or "kvah" in k or "kvarh" in k or k.endswith("_wh")


def _is_power_key(key: str) -> bool:
    k = key.lower()
    if _is_energy_key(key):
        return False
    return (
        k in {"kw", "kva", "kvar", "total_kw", "total_kva", "total_kvar"}
        or "active_power" in k
        or "apparent_power" in k
        or "reactive_power" in k
        or k.endswith("_kw")
        or k.endswith("_kva")
        or k.endswith("_kvar")
    )


def _range_issue(key: str, value: float, limits: dict) -> str:
    if _is_voltage_key(key) and abs(value) > limits["max_abs_voltage_v"]:
        return f"voltage out of range (+/-{limits['max_abs_voltage_v']:.0f} V)"
    if _is_current_key(key) and abs(value) > limits["max_abs_current_a"]:
        return f"current out of range (+/-{limits['max_abs_current_a']:.0f} A)"
    if _is_power_key(key) and abs(value) > limits["max_abs_power_kw"]:
        return f"power out of range (+/-{limits['max_abs_power_kw']:.0f} kW)"
    if _is_energy_key(key) and abs(value) > limits["max_abs_energy_kwh"]:
        return f"energy out of range (+/-{limits['max_abs_energy_kwh']:.0f} kWh)"
    if _is_frequency_key(key) and not (limits["frequency_min_hz"] <= value <= limits["frequency_max_hz"]):
        return f"frequency out of range ({limits['frequency_min_hz']:.1f}-{limits['frequency_max_hz']:.1f} Hz)"
    if _is_pf_key(key) and abs(value) > limits["pf_abs_max"]:
        return f"power factor out of range (+/-{limits['pf_abs_max']:.2f})"
    return ""


def validate_meter_values(values: Any, cfg: dict, *, meter_id: int | None = None) -> Tuple[Dict[str, Any], List[DataQualityIssue]]:
    """Return a sanitized values dict plus validation issues.

    The limits are intentionally conservative by default so valid CT/PT-scaled
    MV/LV sites are not rejected. The primary protection is non-finite removal
    and physically impossible frequency/PF values.
    """
    if not isinstance(values, dict):
        return {}, [DataQualityIssue("values", "not a mapping", type(values).__name__)]

    limits = _cfg(cfg)
    if not limits["enabled"]:
        return dict(values), []

    clean: Dict[str, Any] = {}
    issues: List[DataQualityIssue] = []

    for key, value in values.items():
        key_s = str(key)
        if isinstance(value, bool):
            clean[key_s] = value
            continue
        if isinstance(value, (int, float)):
            f = float(value)
            if limits["reject_non_finite"] and not math.isfinite(f):
                issues.append(DataQualityIssue(key_s, "non-finite number", value))
                continue
            reason = _range_issue(key_s, f, limits)
            if reason:
                issues.append(DataQualityIssue(key_s, reason, value))
                continue
            clean[key_s] = f
            continue
        clean[key_s] = value

    return clean, issues


def summarize_issues(issues: Iterable[DataQualityIssue], *, limit: int = 4) -> str:
    items = [i.short() for i in list(issues)[:max(1, int(limit))]]
    return "; ".join(items)
