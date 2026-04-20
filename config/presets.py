"""Plant-type preset helpers for first-run and runtime setup flows."""
from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, List, Sequence, Tuple


PLANT_TYPE_CHOICES: Sequence[Tuple[str, str]] = (
    ("solar_ground", "Solar - Ground Mount"),
    ("solar_net", "Solar - Net Accounting (import + export meters)"),
    ("hydro", "Hydro Plant"),
    ("industrial", "Industrial Load Monitoring"),
    ("general", "General / Other"),
)

_DEFAULT_PLANT_TYPE = "solar_ground"

_PRESET_OVERRIDES: Dict[str, Dict[str, Any]] = {
    "solar_ground": {
        "alarms": {"enabled": True, "stale_sec": 12, "startup_inhibit_sec": 20},
        "reports": {
            "template": "solar_ops",
            "include_keys": ["kW", "Today_kWh", "Import_kWh", "PFavg", "Vavg", "Iavg", "Frequency"],
        },
        "logging": {"keys": ["kW", "kVA", "PFavg", "Vavg", "Iavg", "Import_kWh", "Today_kWh"]},
        "solar_plant": {"energy_key": "Today_kWh", "power_key": "kW", "pr_design": 0.78},
    },
    "solar_net": {
        "alarms": {"enabled": True, "stale_sec": 12, "startup_inhibit_sec": 20},
        "reports": {
            "template": "solar_net_ops",
            "include_keys": ["kW", "Import_kWh", "Export_kWh", "Net_kWh", "PFavg", "Vavg", "Iavg", "Frequency"],
        },
        "logging": {"keys": ["kW", "kVA", "PFavg", "Vavg", "Iavg", "Import_kWh", "Export_kWh", "Net_kWh"]},
        "solar_plant": {"energy_key": "Net_kWh", "power_key": "kW", "pr_design": 0.76},
    },
    "hydro": {
        "alarms": {"enabled": True, "stale_sec": 15, "startup_inhibit_sec": 25},
        "reports": {
            "template": "hydro_ops",
            "include_keys": ["kW", "Today_kWh", "Import_kWh", "PFavg", "Vavg", "Iavg", "Frequency"],
        },
        "logging": {"keys": ["kW", "kVA", "PFavg", "Vavg", "Iavg", "Import_kWh", "Today_kWh"]},
        "hydro_plant": {"energy_key": "Today_kWh", "power_key": "kW", "design_plf_pct": 50.0},
    },
    "industrial": {
        "alarms": {"enabled": True, "stale_sec": 10, "startup_inhibit_sec": 10},
        "reports": {
            "template": "industrial_ops",
            "include_keys": ["kW", "kVA", "PFavg", "Import_kWh", "Today_kWh", "Vavg", "Iavg", "Frequency"],
        },
        "logging": {"keys": ["kW", "kVA", "kVAr", "PFavg", "Vavg", "Iavg", "Import_kWh", "Today_kWh"]},
        "load_plant": {
            "power_key": "kW",
            "apparent_power_key": "kVA",
            "pf_key": "PF",
            "energy_key": "Today_kWh",
            "demand_interval_min": 15,
        },
    },
    "general": {
        "alarms": {"enabled": True, "stale_sec": 10, "startup_inhibit_sec": 10},
        "reports": {
            "template": "default",
            "include_keys": ["kW", "kVA", "PFavg", "Import_kWh", "Today_kWh", "Vavg", "Iavg", "Frequency"],
        },
        "logging": {"keys": ["kW", "kVA", "kVAr", "Vavg", "Iavg", "PFavg", "Import_kWh", "Export_kWh"]},
    },
}

_METER_NAME_PRESETS: Dict[str, Sequence[str]] = {
    "solar_ground": ("Plant Main", "Inverter 1", "Inverter 2", "Inverter 3", "Inverter 4", "Aux Meter"),
    "solar_net": ("Grid Import", "Solar Export", "Aux Meter 1", "Aux Meter 2", "Aux Meter 3", "Aux Meter 4"),
    "hydro": ("Generator Main", "Aux Load", "Feeder 3", "Feeder 4", "Feeder 5", "Feeder 6"),
    "industrial": ("Main Incomer", "Production Line 1", "Production Line 2", "HVAC", "Utilities", "Spare"),
    "general": ("Meter 1", "Meter 2", "Meter 3", "Meter 4", "Meter 5", "Meter 6"),
}


def normalize_plant_type(value: str) -> str:
    raw = str(value or "").strip().lower()
    allowed = {k for k, _ in PLANT_TYPE_CHOICES}
    return raw if raw in allowed else _DEFAULT_PLANT_TYPE


def plant_type_label(value: str) -> str:
    key = normalize_plant_type(value)
    for k, label in PLANT_TYPE_CHOICES:
        if k == key:
            return label
    return key


def preset_overrides(plant_type: str) -> Dict[str, Any]:
    key = normalize_plant_type(plant_type)
    return deepcopy(_PRESET_OVERRIDES.get(key, {}))


def _deep_update(dst: Dict[str, Any], src: Dict[str, Any]) -> Dict[str, Any]:
    for key, value in src.items():
        if isinstance(value, dict):
            node = dst.get(key)
            if not isinstance(node, dict):
                node = {}
                dst[key] = node
            _deep_update(node, value)
        else:
            dst[key] = deepcopy(value)
    return dst


def apply_plant_preset(cfg: Dict[str, Any], plant_type: str) -> str:
    """Apply selected plant defaults directly into cfg. Returns normalized key."""
    key = normalize_plant_type(plant_type)
    site = cfg.setdefault("site", {})
    site["plant_type"] = key
    _deep_update(cfg, preset_overrides(key))
    return key


def suggested_total_signs(plant_type: str, meter_count: int) -> List[str]:
    count = max(0, int(meter_count))
    signs = ["+"] * count
    if normalize_plant_type(plant_type) == "solar_net" and count >= 2:
        # Typical net-accounting topology:
        # meter 1 = grid import (+), meter 2 = export/solar feeder (-).
        signs[1] = "-"
    return signs


def suggested_meter_names(plant_type: str, meter_count: int) -> List[str]:
    count = max(0, int(meter_count))
    names = list(_METER_NAME_PRESETS.get(normalize_plant_type(plant_type), _METER_NAME_PRESETS["general"]))
    if len(names) < count:
        names.extend(f"Meter {i + 1}" for i in range(len(names), count))
    return names[:count]


def is_generic_meter_name(value: str) -> bool:
    name = str(value or "").strip().lower()
    return (not name) or name.startswith("meter ")

