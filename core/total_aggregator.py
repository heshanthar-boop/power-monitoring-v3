from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import math
import time

from utils.health import meter_is_fresh, stale_seconds
from core.snapshot_bus import MeterQuality
from core.key_registry import canonical_key


def _safe_float(x: Any) -> Optional[float]:
    if isinstance(x, (int, float)):
        return float(x)
    if x is None:
        return None
    try:
        return float(x)
    except Exception:
        return None


def _inc_to_bool(x: Any) -> bool:
    if isinstance(x, bool):
        return x
    if x is None:
        return False
    if isinstance(x, (int, float)):
        return float(x) != 0.0
    s = str(x).strip().lower()
    return s in ("y", "yes", "true", "1", "on")


def _op_to_sign(op: Any) -> float:
    if op in ("-", "sub", "subtract", "minus"):
        return -1.0
    return 1.0


@dataclass
class CustomSlot:
    slot: int
    output_key: str
    source_key: str
    label: str = ""
    unit: str = ""
    mode: str = "SUM_SIGNED"
    ref_meter_id: Optional[int] = None
    meter_settings: Dict[int, Tuple[bool, float]] = None


class TotalAggregator:
    """
    Aggregates multiple meter readings into a unified TOTAL/PLANT meter and
    optionally allows custom TOTAL math configuration via cfg["total_custom"].
    """
    SUM_KEYS = [
        "kW", "kVA", "kVAr",
        "Import_kWh", "Export_kWh", "Net_kWh",
        "Today_kWh",
        "Iavg",
        # Per-phase currents (requested for plant monitoring)
        "I1", "I2", "I3",
    ]

    AVG_KEYS = [
        "Vavg", "Frequency",
        "V1N", "V2N", "V3N",
        "V12", "V23", "V31",
    ]

    MAX_KEYS = [
        "THD Voltage V1N", "THD Voltage V2N", "THD Voltage V3N",
        "THD Voltage V12", "THD Voltage V23", "THD Voltage V31",
        "THD Current I1", "THD Current I2", "THD Current I3",
        "kW Active Power Max DMD", "kW Active Power Min DMD",
        "kVAr Reactive Power Max DMD", "kVAr Reactive Power Min DMD",
        "kVA Apparent Power Max DMD",
        "RunHour",
    ]

    def __init__(self, cfg: Optional[Dict[str, Any]] = None):
        self.cfg = cfg or {}
        self.last_compute_ts: Optional[float] = None
        self._cached_result: Optional[Dict[str, Any]] = None
        self.QUALITY_PRIORITY = {
            MeterQuality.DISABLED: 0,
            MeterQuality.GOOD: 1,
            MeterQuality.STALE: 2,
            MeterQuality.COMM_LOST: 3,
        }

    def set_cfg(self, cfg: Dict[str, Any]) -> None:
        self.cfg = cfg or {}

    def compute(self, meters: List[Any], cfg: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        used_cfg = cfg if cfg is not None else self.cfg
        self.last_compute_ts = time.time()
        stale_sec = stale_seconds(used_cfg)
        # Respect UI Display selection (Total should follow the same selected meters
        # as the operator checkboxes), not just "enabled".
        selected_ids = self._selected_meter_ids_from_cfg(used_cfg)
        valid_meters = self._get_valid_meters(meters, stale_sec, selected_ids)

        if not valid_meters:
            result = self._empty_result()
            # Additive metadata (does not affect existing business logic)
            # Used by UI trend feeding to avoid GOOD/STALE flapping artifacts.
            result["_valid_meter_count"] = 0
            result["_selected_meter_count"] = int(len(selected_ids or []))
            self._cached_result = result
            return result

        result = self._compute_base_totals(valid_meters)
        # Additive metadata (safe): how many meters actually contributed.
        result["_valid_meter_count"] = int(len(valid_meters))
        result["_selected_meter_count"] = int(len(selected_ids or []))

        slots = self._parse_custom_slots(used_cfg)
        slot_meta_map: Dict[int, Dict[str, Any]] = {}
        seen_output_keys: set[str] = set()
        if slots:
            meters_by_id = self._meters_by_id(meters)
            for slot in slots:
                value, meta = self._calc_slot(
                    slot,
                    meters_by_id,
                    stale_sec,
                    seen_output_keys,
                    selected_ids,
                )
                result[slot.output_key] = value
                try:
                    slot_index = int(slot.slot)
                except Exception:
                    slot_index = len(slot_meta_map) + 1
                slot_meta_map[slot_index] = meta
        if slot_meta_map:
            result["_custom_slot_meta"] = slot_meta_map

        total_quality = self._determine_total_quality(meters)
        result["quality"] = total_quality.value
        result["data_valid"] = total_quality == MeterQuality.GOOD
        result["quality_priority"] = self.QUALITY_PRIORITY.get(total_quality, 0)

        self._cached_result = result
        return result

    def _compute_base_totals(self, valid_meters: List[Any]) -> Dict[str, Any]:
        result: Dict[str, Any] = {}

        for key in self.SUM_KEYS:
            result[key] = self._safe_sum(valid_meters, key)

        for key in self.AVG_KEYS:
            result[key] = self._safe_average(valid_meters, key)

        for key in self.MAX_KEYS:
            result[key] = self._safe_max(valid_meters, key)

        total_kW = result.get("kW")
        total_kVA = result.get("kVA")
        if total_kVA is not None and total_kVA > 0.001:
            pf = total_kW / total_kVA
            result["PFavg"] = max(-1.0, min(1.0, pf))
        else:
            result["PFavg"] = None
        result["PF"] = result["PFavg"]

        import_kwh = result.get("Import_kWh")
        export_kwh = result.get("Export_kWh")
        if import_kwh is not None and export_kwh is not None:
            result["Lifetime_kWh"] = import_kwh - export_kwh
        else:
            result["Lifetime_kWh"] = result.get("Net_kWh")

        thd_v_values = [
            result.get("THD Voltage V1N"),
            result.get("THD Voltage V2N"),
            result.get("THD Voltage V3N"),
        ]
        valid_thd_v = [v for v in thd_v_values if v is not None]
        result["THD_V"] = max(valid_thd_v) if valid_thd_v else None

        thd_i_values = [
            result.get("THD Current I1"),
            result.get("THD Current I2"),
            result.get("THD Current I3"),
        ]
        valid_thd_i = [v for v in thd_i_values if v is not None]
        result["THD_I"] = max(valid_thd_i) if valid_thd_i else None

        result["Max_DMD_kW"] = result.get("kW Active Power Max DMD")
        result["valid_meter_count"] = len(valid_meters)

        # Convenience aliases for dashboard tiles & charts (common operator wording)
        result["Total_kW"] = result.get("kW")
        result["Total_kVA"] = result.get("kVA")
        result["Total_kVAr"] = result.get("kVAr")
        result["I1_total"] = result.get("I1")
        result["I2_total"] = result.get("I2")
        result["I3_total"] = result.get("I3")

        return result

    def _parse_custom_slots(self, cfg: Dict[str, Any]) -> List[CustomSlot]:
        tc = (cfg or {}).get("total_custom", {}) or {}
        if not tc.get("enabled", False):
            return []

        raw_slots = tc.get("slots", []) or []
        slots: List[CustomSlot] = []

        for i, rs in enumerate(raw_slots[:16], start=1):
            try:
                slot_num = int(rs.get("slot", i))
            except Exception:
                slot_num = i

            out_key = str(rs.get("output_key", "") or "").strip()
            src_key = str(rs.get("source_key", "") or "").strip()
            out_key = canonical_key(out_key)
            src_key = canonical_key(src_key)
            mode = str(rs.get("mode", "SUM_SIGNED") or "SUM_SIGNED").strip().upper()
            label = str(rs.get("label", "") or "").strip()
            unit = str(rs.get("unit", "") or "").strip()

            ref_mid = rs.get("ref_meter_id")
            try:
                ref_mid = int(ref_mid) if ref_mid not in (None, "") else None
            except Exception:
                ref_mid = None

            ms: Dict[int, Tuple[bool, float]] = {}
            for mid in range(1, 7):
                inc = _inc_to_bool(rs.get(f"m{mid}_include", False))
                sign = _op_to_sign(rs.get(f"m{mid}_op", "+"))
                ms[mid] = (inc, sign)

            if not out_key or not src_key:
                continue

            slots.append(CustomSlot(
                slot=slot_num,
                output_key=out_key,
                source_key=src_key,
                label=label,
                unit=unit,
                mode=mode,
                ref_meter_id=ref_mid,
                meter_settings=ms,
            ))

        return slots

    def _choose_ref_meter(
        self,
        meters_by_id: Dict[int, Any],
        slot: CustomSlot,
        stale_sec: float,
    ) -> Optional[Any]:
        if slot.ref_meter_id and slot.ref_meter_id in meters_by_id:
            candidate = meters_by_id[slot.ref_meter_id]
            if self._valid_meter(candidate, stale_sec):
                return candidate

        for mid, (inc, _sign) in (slot.meter_settings or {}).items():
            if not inc:
                continue
            candidate = meters_by_id.get(mid)
            if candidate is None:
                continue
            if self._valid_meter(candidate, stale_sec):
                return candidate

        for candidate in meters_by_id.values():
            if self._valid_meter(candidate, stale_sec):
                return candidate

        return None

    def _valid_meter(self, m: Any, stale_sec: float) -> bool:
        return bool(
            getattr(m, "enabled", True)
            and getattr(m, "include_in_total", True)
            and meter_is_fresh(m, stale_sec)
        )

    def _max_quality(self, current: MeterQuality, candidate: MeterQuality) -> MeterQuality:
        if current is None:
            return candidate
        if candidate is None:
            return current
        current_priority = self.QUALITY_PRIORITY.get(current, 0)
        candidate_priority = self.QUALITY_PRIORITY.get(candidate, 0)
        return candidate if candidate_priority > current_priority else current

    def _calc_slot(
        self,
        slot: CustomSlot,
        meters_by_id: Dict[int, Any],
        stale_sec: float,
        seen_output_keys: set[str],
        selected_ids: List[int],
    ) -> Tuple[Optional[float], Dict[str, Any]]:
        warnings: List[str] = []
        requested_meters: List[int] = []
        contributions: List[Tuple[Any, float, float]] = []
        valid_entries: List[Tuple[Any, float]] = []
        active_meter_ids: List[int] = []
        slot_quality = MeterQuality.GOOD
        source_present = False

        # For most parameters, TOTAL should automatically include the meters
        # selected by the Display checkboxes.
        # Only kW/kVA/kVAr allow signed (+/-) per-meter configuration.
        editable_signed = slot.source_key in ("KW", "KVA", "KVAR")
        meter_settings = slot.meter_settings or {}
        if not editable_signed:
            meter_settings = {mid: (mid in selected_ids, 1.0) for mid in range(1, 6)}
        else:
            # If user didn't select any meters for a signed slot, default to UI selection.
            if not any(bool(inc) for (inc, _sign) in meter_settings.values()):
                meter_settings = {mid: (mid in selected_ids, 1.0) for mid in range(1, 6)}

        for mid, (inc, sign) in meter_settings.items():
            if not inc:
                continue
            requested_meters.append(mid)
            m = meters_by_id.get(mid)
            if m is None:
                warnings.append(f"Meter {mid} not configured")
                continue
            values = getattr(m, "values", {}) or {}
            if slot.source_key and slot.source_key in values:
                source_present = True
            meter_quality = getattr(m, "quality", MeterQuality.DISABLED)
            slot_quality = self._max_quality(slot_quality, meter_quality)

            if not getattr(m, "enabled", True):
                warnings.append(f"Meter {mid} disabled")
                continue
            if not getattr(m, "include_in_total", True):
                warnings.append(f"Meter {mid} excluded from TOTALs")

            if not self._valid_meter(m, stale_sec):
                warnings.append(f"Meter {mid} data unavailable ({meter_quality.value})")
                continue

            valid_entries.append((m, sign))
            val = _safe_float(values.get(slot.source_key))
            if val is not None:
                contributions.append((m, val, sign))
                active_meter_ids.append(mid)

        mode = slot.mode.strip().upper() if slot.mode else "SUM_SIGNED"

        if not requested_meters:
            warnings.append("No meters selected for this TOTAL slot")
            slot_quality = MeterQuality.DISABLED

        if slot.output_key:
            if slot.output_key in seen_output_keys:
                warnings.append(f"Duplicate output key '{slot.output_key}' (last slot wins)")
            else:
                seen_output_keys.add(slot.output_key)

        if slot.source_key and not source_present:
            warnings.append(f"Source key '{slot.source_key}' not found on selected meters")

        if not contributions and mode != "REF_METER":
            warnings.append("No valid values available from selected meters")
            slot_quality = self._max_quality(slot_quality, MeterQuality.STALE)

        if mode == "REF_METER":
            mref = self._choose_ref_meter(meters_by_id, slot, stale_sec)
            value = None
            if mref is not None:
                value = _safe_float((mref.values or {}).get(slot.source_key))
            elif requested_meters:
                warnings.append("Reference meter not available")
                slot_quality = self._max_quality(slot_quality, MeterQuality.STALE)
        else:
            value = None
            if contributions:
                if mode == "SUM_SIGNED":
                    total = 0.0
                    for _, v, sign in contributions:
                        total += v * sign
                    value = total
                else:
                    vals = [v for (_, v, _) in contributions]
                    if mode == "AVG":
                        value = sum(vals) / len(vals)
                    elif mode == "MIN":
                        value = min(vals)
                    elif mode == "MAX":
                        value = max(vals)
                    elif mode == "PF_WEIGHTED":
                        kw_sum = 0.0
                        kva_sum = 0.0
                        for m, _sign in valid_entries:
                            kw = _safe_float((m.values or {}).get("kW"))
                            kva = _safe_float((m.values or {}).get("kVA"))
                            if kw is not None:
                                kw_sum += kw
                            if kva is not None:
                                kva_sum += abs(kva)
                        if kva_sum > 0.0:
                            value = kw_sum / kva_sum
        if value is None and mode == "PF_WEIGHTED":
            warnings.append("PF calculation missing kW/kVA data")

        meta = {
            "slot": slot.slot,
            "output_key": slot.output_key,
            "source_key": slot.source_key,
            "mode": mode,
            "quality": slot_quality.value,
            "warning": "; ".join(warnings) if warnings else "",
            "requested_meters": requested_meters,
            "active_meters": active_meter_ids,
            "value": value,
        }
        return value, meta

    def _meters_by_id(self, meters: List[Any]) -> Dict[int, Any]:
        return {
            int(getattr(m, "meter_id", 0)): m
            for m in meters
            if getattr(m, "meter_id", None) is not None
        }

    def _selected_meter_ids_from_cfg(self, cfg: Dict[str, Any]) -> List[int]:
        disp = (cfg or {}).get("display", {}) or {}
        meters_map = disp.get("meters", {}) or {}
        ids = []
        for i in range(1, 7):
            if bool(meters_map.get(str(i), False)):
                ids.append(i)
        if not ids:
            ids = [1]
        return ids

    def _get_valid_meters(self, meters: List[Any], stale_sec: float, selected_ids: List[int]) -> List[Any]:
        valid = []
        for m in meters:
            mid = getattr(m, "meter_id", None)
            if mid is not None and int(mid) not in selected_ids:
                continue
            if not getattr(m, "enabled", False):
                continue
            if not getattr(m, "include_in_total", True):
                continue
            if not meter_is_fresh(m, stale_sec):
                continue
            valid.append(m)
        return valid

    def _get_value(self, meter: Any, key: str) -> Optional[float]:
        try:
            values = getattr(meter, "values", {})
            if not isinstance(values, dict):
                return None
            val = values.get(key)
            if val is None:
                return None
            if not isinstance(val, (int, float)):
                return None
            if math.isnan(val) or math.isinf(val):
                return None
            return float(val)
        except Exception:
            return None

    def _meter_sign_for_key(self, meter: Any, key: str) -> float:
        """Return sign (+1/-1) for a given meter when computing TOTAL for a key.

        Priority:
        1) cfg['total_math']['per_key'][<key>][<meter_id>]  (explicit per-key)
        2) meter.total_sign (overall meter role)

        Anything invalid falls back to +1.
        """
        try:
            mid = int(getattr(meter, "meter_id", 0) or 0)
        except Exception:
            mid = 0

        # Per-key override
        try:
            tm = (self.cfg or {}).get("total_math", {}) or {}
            if bool(tm.get("enabled", False)):
                per_key = tm.get("per_key", {}) or {}
                kcfg = per_key.get(canonical_key(key), {}) or {}
                if isinstance(kcfg, dict):
                    raw = kcfg.get(str(mid), None)
                    if raw in ("+", "ADD", "add"):
                        return 1.0
                    if raw in ("-", "SUB", "SUBTRACT", "sub", "subtract"):
                        return -1.0
                    if isinstance(raw, (int, float)):
                        return -1.0 if float(raw) < 0 else 1.0
        except Exception:
            pass

        # Meter-level role
        try:
            s = float(getattr(meter, "total_sign", 1.0) or 1.0)
            return -1.0 if s < 0 else 1.0
        except Exception:
            return 1.0

    def _safe_sum(self, meters: List[Any], key: str) -> Optional[float]:
        """Signed sum.

        Default behavior: sum selected meters.
        If meter.total_sign == -1, subtract that meter's value.
        If cfg['total_math'] enabled and per_key override exists, that takes priority.
        """
        total = 0.0
        have = False
        for m in meters:
            val = self._get_value(m, key)
            if val is None:
                continue
            have = True
            total += self._meter_sign_for_key(m, key) * float(val)
        if not have:
            return None
        return total

    def _safe_average(self, meters: List[Any], key: str) -> Optional[float]:
        values = []
        for m in meters:
            val = self._get_value(m, key)
            if val is not None:
                values.append(val)
        if not values:
            return None
        return sum(values) / len(values)

    def _safe_max(self, meters: List[Any], key: str) -> Optional[float]:
        values = []
        for m in meters:
            val = self._get_value(m, key)
            if val is not None:
                values.append(val)
        if not values:
            return None
        return max(values)

    def _empty_result(self) -> Dict[str, Any]:
        return {
            "kW": None,
            "kVA": None,
            "kVAr": None,
            "PF": None,
            "PFavg": None,
            "Frequency": None,
            "Vavg": None,
            "Iavg": None,
            "I1": None,
            "I2": None,
            "I3": None,
            "I1_total": None,
            "I2_total": None,
            "I3_total": None,
            "Total_kW": None,
            "Total_kVA": None,
            "Total_kVAr": None,
            "Import_kWh": None,
            "Export_kWh": None,
            "Net_kWh": None,
            "Today_kWh": None,
            "Lifetime_kWh": None,
            "Max_DMD_kW": None,
            "THD_V": None,
            "THD_I": None,
            "RunHour": None,
            "quality": MeterQuality.DISABLED.value,
            "data_valid": False,
            "valid_meter_count": 0,
            "quality_priority": self.QUALITY_PRIORITY.get(MeterQuality.DISABLED, 0),
        }

    def _determine_total_quality(self, meters: List[Any]) -> MeterQuality:
        # Only consider enabled meters that contribute to totals.
        # Disabled/ghost meters must not drag quality down to COMM_LOST.
        candidates = [
            getattr(m, "quality", MeterQuality.DISABLED)
            for m in meters
            if getattr(m, "enabled", True) and getattr(m, "include_in_total", True)
        ]
        if not candidates:
            return MeterQuality.DISABLED
        return max(candidates, key=lambda q: self.QUALITY_PRIORITY.get(q, 0))

    def get_cached_result(self) -> Optional[Dict[str, Any]]:
        return self._cached_result


def compute_total_values(meters: List[Any]) -> Optional[Dict[str, Any]]:
    """
    Convenience wrapper for a stateless TOTAL computation.
    """
    aggregator = TotalAggregator()
    return aggregator.compute(meters)
