"""Registry of canonical keys + labels shared across UI, logging, and aggregation."""

from __future__ import annotations

from typing import Any, Dict, Iterable, List

from core.register_map import MFM384RegisterMap

CANONICAL_KEY_LABELS: Dict[str, str] = {
    "kW": "Active Power (kW)",
    "kVA": "Apparent Power (kVA)",
    "kVAr": "Reactive Power (kVAr)",
    "Vavg": "Average Voltage (V)",
    "Iavg": "Average Current (A)",
    "PFavg": "Power Factor",
    "PF": "Power Factor",
    "Total_kW": "Total Active Power (kW)",
    "Total_kVA": "Total Apparent Power (kVA)",
    "Total_kVAr": "Total Reactive Power (kVAr)",
    "Frequency": "Frequency (Hz)",
    "RunHour": "Run Hours",
    "Import_kWh": "Import kWh",
    "Export_kWh": "Export kWh",
    "Net_kWh": "Total Net kWh",
    "Lifetime_kWh": "Lifetime kWh",
    "Today_kWh": "Today kWh",
    "V1N": "Voltage V1N",
    "V2N": "Voltage V2N",
    "V3N": "Voltage V3N",
    "V12": "Voltage V12",
    "V23": "Voltage V23",
    "V31": "Voltage V31",
    "I1": "Current I1",
    "I2": "Current I2",
    "I3": "Current I3",
    "THD Voltage V1N": "THD of Voltage V1",
    "THD Voltage V2N": "THD of Voltage V2",
    "THD Voltage V3N": "THD of Voltage V3",
    "THD Voltage V12": "THD of Voltage V12",
    "THD Voltage V23": "THD of Voltage V23",
    "THD Voltage V31": "THD of Voltage V31",
    "THD Current I1": "THD of Current I1",
    "THD Current I2": "THD of Current I2",
    "THD Current I3": "THD of Current I3",
    "I1_total": "Total Current I1",
    "I2_total": "Total Current I2",
    "I3_total": "Total Current I3",
    "Vavg_ref": "Reference Voltage (V)",
    "Freq_ref": "Reference Frequency (Hz)",
    "THD_V_worst": "THD Voltage (Worst %)",
    "THD_I_worst": "THD Current (Worst %)",
    "PF_total": "Total Power Factor",
    "RunHour_total": "Total Run Hours",

    # --- Per-phase power & PF (from register_map.py) ---
    "Average Voltage LL": "Vavg (L-L)",
    "kW1": "kW L1",
    "kW2": "kW L2",
    "kW3": "kW L3",
    "kVA1": "kVA L1",
    "kVA2": "kVA L2",
    "kVA3": "kVA L3",
    "kVAr1": "kVAr L1",
    "kVAr2": "kVAr L2",
    "kVAr3": "kVAr L3",
    "PF1": "PF L1",
    "PF2": "PF L2",
    "PF3": "PF L3",
    "Total Net kVAh": "Total Net kVAh",
    "Total Net kVArh": "Total Net kVArh",
}

HARMONIC_SUMMARY_KEYS: List[str] = [
    "Harmonic Voltage Worst",
    "Harmonic Current Worst",
    "Harmonics_ok",
    "Harmonics_read_failed_count",
]

HARMONIC_LOG_KEYS: List[str] = []
for harmonic_no in range(MFM384RegisterMap.HARMONIC_MIN, MFM384RegisterMap.HARMONIC_MAX + 1):
    for harmonic_key, (_, harmonic_label) in MFM384RegisterMap.HARMONIC_PARAMETERS.items():
        key = f"H{harmonic_no:02d}_{harmonic_key}"
        suffix = "th"
        if harmonic_no % 100 not in (11, 12, 13):
            suffix = {1: "st", 2: "nd", 3: "rd"}.get(harmonic_no % 10, "th")
        CANONICAL_KEY_LABELS[key] = f"{harmonic_no}{suffix} Harmonic {harmonic_label} (%)"
        HARMONIC_LOG_KEYS.append(key)

CANONICAL_KEY_LABELS.update({
    "Harmonic Voltage Worst": "Worst Individual Voltage Harmonic (%)",
    "Harmonic Current Worst": "Worst Individual Current Harmonic (%)",
    "Harmonics_ok": "Individual Harmonics Read OK",
    "Harmonics_read_failed_count": "Individual Harmonic Blocks Failed",
})

DEFAULT_TILE_KEYS: List[str] = [
    "kW", "kVA", "kVAr", "Vavg", "Iavg", "PFavg", "Frequency", "RunHour",
    "Import_kWh", "Export_kWh", "Net_kWh", 
    "V12", "V23", "THD Voltage V1N", "THD Current I1",
]

COMMON_KEYS: List[str] = [
    "kW", "kVA", "kVAr", "Import_kWh", "Export_kWh", "Net_kWh", "Lifetime_kWh",
    "Today_kWh", "Vavg", "Iavg", "PFavg", "Frequency", "RunHour",
    "V1N", "V2N", "V3N", "V12", "V23", "V31",
    "I1", "I2", "I3",
    "THD Voltage V1N", "THD Voltage V2N", "THD Voltage V3N",
    "THD Voltage V12", "THD Voltage V23", "THD Voltage V31",
    "THD Current I1", "THD Current I2", "THD Current I3",
    "Harmonic Voltage Worst", "Harmonic Current Worst",
]

COMMON_CHART_KEYS: List[str] = [
    "kW",
    "Import_kWh", "Export_kWh", "Net_kWh",
    "Vavg", "Iavg", "PFavg", "Frequency",
]


def key_label(key: str) -> str:
    """Return the user-facing label for a canonical key."""
    if not key:
        return ""
    return CANONICAL_KEY_LABELS.get(key, key)


def _normalize_key_text(text: str) -> str:
    if not text:
        return ""
    normalized = text.replace("_", " ").replace("-", " ").replace(".", " ")
    normalized = normalized.replace("(", " ").replace(")", " ")
    normalized = " ".join(normalized.split())
    return normalized.lower()


_ALIAS_TO_CANONICAL: Dict[str, str] = {}


def _register_alias(alias: str, canonical: str) -> None:
    normalized = _normalize_key_text(alias)
    if normalized:
        _ALIAS_TO_CANONICAL[normalized] = canonical


for canonical in CANONICAL_KEY_LABELS:
    _register_alias(canonical, canonical)
    label = CANONICAL_KEY_LABELS.get(canonical)
    if label:
        _register_alias(label, canonical)

REGISTER_ALIASES = {
    "total kw": "kW",
    "total kva": "kVA",
    "total kvar": "kVAr",
    "total kwh import": "Import_kWh",
    "total kwh export": "Export_kWh",
    "total net kwh": "Net_kWh",
    "today kwh": "Today_kWh",
    "average voltage ln": "Vavg",
    "average current": "Iavg",
    "average pf": "PFavg",
    "run hour": "RunHour",
    "run hours": "RunHour",
    "frequency hz": "Frequency",
    "frequency (hz)": "Frequency",
    "freq": "Frequency",
    "freq.": "Frequency",
    "freq hz": "Frequency",
    "freqhz": "Frequency",
    # Some meters / older builds emit plain 'Hz'
    "hz": "Frequency",
    "hertz": "Frequency",
    "power factor": "PFavg",
    "active power": "kW",
    "apparent power": "kVA",
    "reactive power": "kVAr",
    "import kwh": "Import_kWh",
    "export kwh": "Export_kWh",
    "net kwh": "Net_kWh",
}

for alias, canonical in REGISTER_ALIASES.items():
    _register_alias(alias, canonical)


_KEY_UNITS: Dict[str, str] = {
    "kW": "kW",
    "kVA": "kVA",
    "kVAr": "kVAr",
    "Total_kW": "kW",
    "Total_kVA": "kVA",
    "Total_kVAr": "kVAr",
    "kW1": "kW", "kW2": "kW", "kW3": "kW",
    "kVA1": "kVA", "kVA2": "kVA", "kVA3": "kVA",
    "kVAr1": "kVAr", "kVAr2": "kVAr", "kVAr3": "kVAr",
    "Vavg": "V",
    "Vavg_ref": "V",
    "V1N": "V", "V2N": "V", "V3N": "V",
    "V12": "V", "V23": "V", "V31": "V",
    "Average Voltage LL": "V",
    "Iavg": "A",
    "I1": "A", "I2": "A", "I3": "A",
    "I1_total": "A", "I2_total": "A", "I3_total": "A",
    "PFavg": "", "PF": "", "PF_total": "",
    "PF1": "", "PF2": "", "PF3": "",
    "Frequency": "Hz",
    "Freq_ref": "Hz",
    "Import_kWh": "kWh",
    "Export_kWh": "kWh",
    "Net_kWh": "kWh",
    "Lifetime_kWh": "kWh",
    "Today_kWh": "kWh",
    "Total Net kVAh": "kVAh",
    "Total Net kVArh": "kVArh",
    "RunHour": "h",
    "RunHour_total": "h",
    "THD Voltage V1N": "%", "THD Voltage V2N": "%", "THD Voltage V3N": "%",
    "THD Voltage V12": "%", "THD Voltage V23": "%", "THD Voltage V31": "%",
    "THD Current I1": "%", "THD Current I2": "%", "THD Current I3": "%",
    "THD_V_worst": "%", "THD_I_worst": "%",
    "Harmonic Voltage Worst": "%",
    "Harmonic Current Worst": "%",
    "Harmonics_ok": "",
    "Harmonics_read_failed_count": "",
}

for key in HARMONIC_LOG_KEYS:
    _KEY_UNITS[key] = "%"


def key_unit(key: str) -> str:
    """Return the SI unit string for a canonical key (e.g. 'kW', 'V', 'A', 'Hz', '%').

    Returns an empty string for dimensionless quantities (e.g. power factor, run hours
    are returned as 'h') or for unknown keys.
    """
    if not key:
        return ""
    return _KEY_UNITS.get(key, "")


def canonical_key(key: Any) -> str:
    """Return the canonical key for any user-facing string."""
    if key is None:
        return ""
    key_str = str(key).strip()
    if not key_str:
        return ""
    if key_str in CANONICAL_KEY_LABELS:
        return key_str
    normalized = _normalize_key_text(key_str)
    return _ALIAS_TO_CANONICAL.get(normalized, key_str)


def canonical_keys(keys: Iterable[Any]) -> List[str]:
    seen: List[str] = []
    for key in keys:
        canon = canonical_key(key)
        if canon and canon not in seen:
            seen.append(canon)
    return seen


__all__ = [
    "CANONICAL_KEY_LABELS",
    "DEFAULT_TILE_KEYS",
    "COMMON_KEYS",
    "COMMON_CHART_KEYS",
    "HARMONIC_SUMMARY_KEYS",
    "HARMONIC_LOG_KEYS",
    "key_label",
    "key_unit",
    "canonical_key",
    "canonical_keys",
]
