"""
core/device_drivers.py
======================
Per-device Modbus read drivers.

Each driver is a callable:
    read_fn(client, slave_id, word_order, base_address) -> Dict[str, float]

All drivers return values in canonical app keys (same as MFM384 normalized output):
    kW, kVA, kVAr, Vavg, Iavg, PFavg, PF, Frequency,
    Import_kWh, Export_kWh, Net_kWh,
    V1N, V2N, V3N, V12, V23, V31,
    I1, I2, I3,
    THD Voltage V1N, THD Voltage V2N, THD Voltage V3N,
    THD Current I1, THD Current I2, THD Current I3,
    kW1, kW2, kW3, PF1, PF2, PF3,
    kW Active Power Max DMD, kVA Apparent Power Max DMD, RunHour, ...

Register address notes use 0-based Modbus offsets (same as pymodbus).
All FLOAT32 values use IEEE 754 two-register encoding.
"""
from __future__ import annotations

import math
import struct
from typing import Any, Callable, Dict, List, Optional, Tuple


# â”€â”€ Float helpers (self-contained â€” no import from modbus_codec to avoid circular) â”€â”€â”€â”€

def _regs_to_f32(r0: int, r1: int, word_order: str = "CDAB") -> float:
    a = (r0 >> 8) & 0xFF; b = r0 & 0xFF
    c = (r1 >> 8) & 0xFF; d = r1 & 0xFF
    src = {"A": a, "B": b, "C": c, "D": d}
    order = word_order.upper().strip()
    if order not in ("ABCD", "CDAB", "BADC", "DCBA"):
        order = "ABCD"
    data = bytes([src[ch] for ch in order])
    val = struct.unpack(">f", data)[0]
    if not math.isfinite(val):
        return 0.0
    return val


def _fc04(client, addr: int, count: int, slave_id: int):
    try:
        fn = client.read_input_registers
        try:
            return fn(address=addr, count=count, slave=slave_id)
        except TypeError:
            return fn(address=addr, count=count, unit=slave_id)
    except Exception:
        return None


def _fc03(client, addr: int, count: int, slave_id: int):
    try:
        fn = client.read_holding_registers
        try:
            return fn(address=addr, count=count, slave=slave_id)
        except TypeError:
            return fn(address=addr, count=count, unit=slave_id)
    except Exception:
        return None


def _is_err(rr) -> bool:
    return rr is None or getattr(rr, "isError", lambda: True)()


def _read_block(client, addr: int, count: int, slave_id: int, fc: int = 4) -> List[int]:
    """Read a register block, return list of raw int register values (empty on error)."""
    MAX_CHUNK = 60
    regs: List[int] = []
    remaining = count
    cur = addr
    chunk = MAX_CHUNK
    while remaining > 0:
        step = min(remaining, chunk)
        rr = _fc04(client, cur, step, slave_id) if fc == 4 else _fc03(client, cur, step, slave_id)
        if _is_err(rr):
            if chunk > 8:
                chunk = max(8, chunk // 2)
                continue
            raise RuntimeError(f"FC0{fc} read error @ {cur} count={step}: {rr}")
        regs.extend(rr.registers)
        cur += step
        remaining -= step
    return regs


def _f(regs: List[int], idx: int, word_order: str = "ABCD") -> Optional[float]:
    """Safely decode FLOAT32 at register index idx (2 consecutive registers)."""
    if idx + 1 >= len(regs):
        return None
    return _regs_to_f32(regs[idx], regs[idx + 1], word_order)


def _safe(v: Optional[float], scale: float = 1.0) -> Optional[float]:
    if v is None:
        return None
    r = v * scale
    return r if math.isfinite(r) else None


# â”€â”€ Model registry â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

# Tuples of (model_id, display_label)
DEVICE_MODELS: Tuple[Tuple[str, str], ...] = (
    # â”€â”€ Selec â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    ("selec_mfm384",          "Selec MFM384-C / MFM384-R (default)"),
    ("selec_mfm384_harmonics", "Selec MFM384-C / R + individual harmonics (2nd-31st)"),
    ("selec_mfm383",          "Selec MFM383A / MFM376 / MFM284"),
    # â”€â”€ Schneider Electric â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    ("schneider_pm2120",      "Schneider PM2120 / PM2220 / EasyLogic PM2100"),
    ("schneider_pm5000",      "Schneider PM5000 / PM7000 series"),
    # â”€â”€ Circutor â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    ("circutor_cvm_c10",      "Circutor CVM-C10 / CVM-B100 / CVM-C11"),
    ("circutor_cvm_b150",     "Circutor CVM-B150 / CEM-C21"),
    # â”€â”€ Accuenergy â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    ("accuenergy_acuvim2",    "Accuenergy Acuvim II / Acuvim-L"),
    ("accuenergy_acurev1310", "Accuenergy AcuRev 1310 (DIN Rail)"),
    # â”€â”€ Secure â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    ("secure_elite440",       "Secure Elite 440 / 441 / 442"),
    # â”€â”€ Siemens â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    ("siemens_7kt0310",       "Siemens 7KT0310 / 7KT0311"),
    ("siemens_7kt0320",       "Siemens 7KT0320 (with THD)"),
    # â”€â”€ Multispan â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    ("multispan_avh16",       "Multispan AVH-16-E3 / AVH-14-E3"),
    ("multispan_avh12",       "Multispan AVH-12-E3 (72x72mm)"),
    # â”€â”€ GFUVE â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    ("gfuve_fu2200",          "GFUVE FU2200B / FU2200A"),
    # â”€â”€ Saia Burgess â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    ("saia_ale3",             "Saia ALE3D5FD10C2A00 / ALD1D5FD"),
    # â”€â”€ Acrel â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    ("acrel_adw210",          "Acrel ADW210-D16 / AMC96L-E4"),
    # Rishabh Instruments / Soar Engineering Sri Lanka
    ("rish_em1320",           "Rishabh RISH EM 1320"),
    ("rish_em1330",           "Rishabh RISH EM 1330"),
    ("rish_em1340",           "Rishabh RISH EM 1340"),
    ("rish_em2340ds",         "Rishabh RISH EM 2340DS"),
    ("rish_em3490ds",         "Rishabh RISH EM 3490DS"),
    ("rish_em3490dsi",        "Rishabh RISH EM 3490DSi"),
    ("rish_emdc6000",         "Rishabh RISH EMDC 6000 DC"),
    ("rish_emdc6001",         "Rishabh RISH EMDC 6001 DC"),
    ("rish_emdc6002",         "Rishabh RISH EMDC 6002 DC"),
    ("rish_delta_energy",     "Rishabh RISH Delta Energy"),
    ("rish_delta_energy_nx",  "Rishabh RISH Delta Energy NX"),
    ("rish_delta_power",      "Rishabh RISH Delta Power"),
    ("rish_delta_power_nx",   "Rishabh RISH Delta Power NX"),
    ("rish_delta_vaf",        "Rishabh RISH Delta VAF"),
    ("rish_delta_vaf_nx",     "Rishabh RISH Delta VAF NX"),
    ("rish_master3430",       "Rishabh RISH Master 3430"),
    ("rish_master3430i",      "Rishabh RISH Master 3430i"),
    ("rish_master3440",       "Rishabh RISH Master 3440"),
    ("rish_master3440i",      "Rishabh RISH Master 3440i"),
    ("rish_master3440idl",    "Rishabh RISH Master 3440iDL"),
    ("rish_master3480",       "Rishabh RISH Master 3480"),
    ("rish_lm1340",           "Rishabh RISH LM 1340"),
    ("rish_lm1360",           "Rishabh RISH LM 1360"),
    ("rish_lm1360_rj12",      "Rishabh RISH LM 1360 RJ12"),
    ("rish_md1340",           "Rishabh RISH MD1340"),
    ("rish_ed1100",           "Rishabh RISH ED1100"),
    ("rish_ed1111",           "Rishabh RISH ED1111"),
    ("rish_ed11x1",           "Rishabh RISH ED11x1"),
    ("rish_ed21x1",           "Rishabh RISH ED21X1"),
    ("rish_ed43xx",           "Rishabh RISH ED43XX"),
    ("rish_ducer_m01",        "Rishabh RISH Ducer M01"),
    ("rish_ducer_m30",        "Rishabh RISH Ducer M30"),
    ("rish_ducer_m40",        "Rishabh RISH Ducer M40"),
    ("rish_ducer_mxx",        "Rishabh RISH Ducer MXX"),
    ("rish_cr12",             "Rishabh RISH CR12"),
    ("rish_cd30",             "Rishabh RISH CD30"),
    ("rish_cd60",             "Rishabh RISH CD60"),
    ("rish_optima_vaf",       "Rishabh RISH OPTIMA VAF"),
    ("rish_ml1400",           "Rishabh RISH ML 1400"),
    ("rish_ml1410",           "Rishabh RISH ML 1410"),
)

DEVICE_MODEL_IDS: Tuple[str, ...] = tuple(m[0] for m in DEVICE_MODELS)
DEVICE_MODEL_LABELS: Dict[str, str] = {k: v for k, v in DEVICE_MODELS}

DEFAULT_MODEL = "selec_mfm384"

RISHABH_SUPPLIER_INFO: Dict[str, Dict[str, str]] = {
    "soar_technology": {
        "name": "Soar Technology (Pvt) Ltd",
        "phone": "+94-11-2 232 601",
        "email": "soartech@soar.lk",
        "website": "www.soar.lk",
        "location": "Colombo, Sri Lanka",
    },
    "rishabh_instruments": {
        "name": "Rishabh Instruments (India)",
        "phone": "+91 253 2202099",
        "email": "marketing@rishabh.co.in",
        "website": "www.rishabh.co.in",
    },
}


def _rishabh_meta(category: str, mounting: str, size: str, thd: str,
                  accuracy: str, stock: str, profile: str,
                  *, rs485: str = "yes", certification: str = "CE, IEC",
                  extra: str = "") -> Dict[str, str]:
    return {
        "supplier": "Soar Technology (Pvt) Ltd / Rishabh Instruments",
        "category": category,
        "mounting": mounting,
        "size": size,
        "rs485": rs485,
        "thd": thd,
        "accuracy": accuracy,
        "certification": certification,
        "stock_status": stock,
        "driver_profile": profile,
        "extra": extra,
    }


RISHABH_MODEL_CATALOG: Dict[str, Dict[str, str]] = {
    "rish_em1320": _rishabh_meta("RISH EM Series", "Panel", "96x96mm", "15th", "Class 1", "order", "rishabh_ac_basic", rs485="optional"),
    "rish_em1330": _rishabh_meta("RISH EM Series", "Panel", "96x96mm", "15th", "Class 1", "order", "rishabh_ac_basic", rs485="optional"),
    "rish_em1340": _rishabh_meta("RISH EM Series", "Panel", "96x96mm", "15th", "Class 1", "order", "rishabh_ac_basic", rs485="optional"),
    "rish_em2340ds": _rishabh_meta("RISH EM Series", "Panel", "96x96mm", "15th", "Class 1", "order", "rishabh_ac_basic", rs485="optional"),
    "rish_em3490ds": _rishabh_meta("RISH EM Series", "Panel", "96x96mm", "15th", "Class 1", "order", "rishabh_ac_basic", rs485="optional"),
    "rish_em3490dsi": _rishabh_meta("RISH EM Series", "Touch Panel", "96x96mm", "31st", "Class 0.2S", "order", "rishabh_ac_basic"),
    "rish_emdc6000": _rishabh_meta("RISH EM Series", "Panel", "96x96mm", "N/A (DC)", "Class 1", "order", "rishabh_dc"),
    "rish_emdc6001": _rishabh_meta("RISH EM Series", "Panel", "96x96mm", "N/A (DC)", "Class 1", "order", "rishabh_dc"),
    "rish_emdc6002": _rishabh_meta("RISH EM Series", "Panel", "96x96mm", "N/A (DC)", "Class 1", "order", "rishabh_dc"),
    "rish_delta_energy": _rishabh_meta("RISH Delta Series", "Panel", "96x96mm", "15th", "Class 1", "in_stock", "rishabh_ac_basic", rs485="optional"),
    "rish_delta_energy_nx": _rishabh_meta("RISH Delta Series", "Panel", "96x96mm", "15th", "Class 1", "in_stock", "rishabh_ac_basic"),
    "rish_delta_power": _rishabh_meta("RISH Delta Series", "Panel", "96x96mm", "15th", "Class 1", "in_stock", "rishabh_ac_basic", rs485="optional"),
    "rish_delta_power_nx": _rishabh_meta("RISH Delta Series", "Panel", "96x96mm", "15th", "Class 1", "in_stock", "rishabh_ac_basic"),
    "rish_delta_vaf": _rishabh_meta("RISH Delta Series", "Panel", "96x96mm", "15th", "Class 1", "in_stock", "rishabh_ac_basic", rs485="optional"),
    "rish_delta_vaf_nx": _rishabh_meta("RISH Delta Series", "Panel", "96x96mm", "15th", "Class 1", "in_stock", "rishabh_ac_basic"),
    "rish_master3430": _rishabh_meta("RISH Master Series", "Panel", "96x96mm", "15th", "Class 1", "order", "rishabh_ac_basic", rs485="optional"),
    "rish_master3430i": _rishabh_meta("RISH Master Series", "Touch Panel", "96x96mm", "15th", "Class 1", "order", "rishabh_ac_basic"),
    "rish_master3440": _rishabh_meta("RISH Master Series", "Panel", "96x96mm", "31st", "Class 0.2S", "order", "rishabh_master"),
    "rish_master3440i": _rishabh_meta("RISH Master Series", "Touch Panel", "96x96mm", "31st", "Class 0.2S", "order", "rishabh_master"),
    "rish_master3440idl": _rishabh_meta("RISH Master Series", "Touch Panel", "96x96mm", "31st", "Class 0.2S", "order", "rishabh_master"),
    "rish_master3480": _rishabh_meta("RISH Master Series", "Touch Panel", "96x96mm", "31st", "Class 0.5S", "order", "rishabh_master"),
    "rish_lm1340": _rishabh_meta("RISH LM Series", "Panel", "96x96mm", "15th", "Class 1", "order", "rishabh_ac_basic", rs485="optional"),
    "rish_lm1360": _rishabh_meta("RISH LM Series", "Panel", "96x96mm", "31st", "Class 0.2S/0.5", "order", "rishabh_master", rs485="optional"),
    "rish_lm1360_rj12": _rishabh_meta("RISH LM Series", "Panel", "96x96mm", "31st", "Class 0.2S/0.5", "order", "rishabh_master"),
    "rish_md1340": _rishabh_meta("RISH LM Series", "DIN Rail", "70mm", "15th", "Class 1", "order", "rishabh_ac_basic", rs485="optional"),
    "rish_ed1100": _rishabh_meta("RISH ED Series", "Panel", "96x96mm", "N/A", "Class 1", "order", "rishabh_ac_basic", rs485="optional", extra="63A/100A direct connection"),
    "rish_ed1111": _rishabh_meta("RISH ED Series", "Panel", "96x96mm", "N/A", "Class 1", "order", "rishabh_ac_basic", extra="45A direct connection"),
    "rish_ed11x1": _rishabh_meta("RISH ED Series", "Panel", "96x96mm", "N/A", "Class 1", "order", "rishabh_ac_basic", extra="45A direct connection"),
    "rish_ed21x1": _rishabh_meta("RISH ED Series", "Panel", "96x96mm", "N/A", "Class 1", "order", "rishabh_ac_basic", extra="100A direct connection"),
    "rish_ed43xx": _rishabh_meta("RISH ED Series", "Panel", "96x96mm", "N/A", "Class 1", "order", "rishabh_ac_basic", extra="100A 3-phase direct connection"),
    "rish_ducer_m01": _rishabh_meta("RISH Ducer Series", "DIN Rail", "70mm", "N/A", "Class 0.2", "eol", "rishabh_ac_basic", extra="transducer, no analog output"),
    "rish_ducer_m30": _rishabh_meta("RISH Ducer Series", "DIN Rail", "70mm", "N/A", "Class 0.2", "eol", "rishabh_ac_basic", extra="transducer, 3 analog outputs"),
    "rish_ducer_m40": _rishabh_meta("RISH Ducer Series", "DIN Rail", "70mm", "N/A", "Class 0.2", "eol", "rishabh_ac_basic", extra="transducer, 4 analog outputs"),
    "rish_ducer_mxx": _rishabh_meta("RISH Ducer Series", "DIN Rail", "70mm", "N/A", "Class 0.2", "eol", "rishabh_ac_basic", extra="transducer, 2-4 analog outputs"),
    "rish_cr12": _rishabh_meta("RISH Compact Series", "Panel", "72x72mm", "15th", "Class 1", "order", "rishabh_ac_basic", rs485="optional"),
    "rish_cd30": _rishabh_meta("RISH Compact Series", "Panel", "72x72mm", "No", "Class 1", "order", "rishabh_ac_basic", rs485="optional"),
    "rish_cd60": _rishabh_meta("RISH Compact Series", "Panel", "72x72mm", "No", "Class 1", "order", "rishabh_ac_basic", rs485="optional"),
    "rish_optima_vaf": _rishabh_meta("RISH Compact Series", "Panel", "96x96mm", "No", "Class 1", "order", "rishabh_ac_basic", rs485="optional"),
    "rish_ml1400": _rishabh_meta("RISH MULTILoad Series", "Panel", "96x96mm", "N/A", "Class 1", "order", "rishabh_ac_basic", extra="4x3Ph channels"),
    "rish_ml1410": _rishabh_meta("RISH MULTILoad Series", "Panel", "96x96mm", "N/A", "Class 1", "order", "rishabh_ac_basic", extra="4x3Ph channels"),
}


# â”€â”€ 1. Selec MFM384 â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def _read_selec_mfm384(client, slave_id: int, word_order: str, base_address: int) -> Dict[str, Any]:
    from core.modbus_codec import read_mfm384_fc04
    return read_mfm384_fc04(
        client=client,
        slave_id=slave_id,
        word_order=word_order,
        base_address=base_address,
    )


def _read_selec_mfm384_harmonics(client, slave_id: int, word_order: str, base_address: int) -> Dict[str, Any]:
    from core.modbus_codec import read_mfm384_fc04
    return read_mfm384_fc04(
        client=client,
        slave_id=slave_id,
        word_order=word_order,
        base_address=base_address,
        include_harmonics=True,
    )


# â”€â”€ 2. Schneider PM2120 (EasyLogic PM2100 series) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# FC03 Holding Registers, addresses start at 3000 (0-based: 2999 ... but
# Schneider uses 1-based in manual â†’ subtract 1 for pymodbus 0-based).
# Key addresses (0-based): 3000-3001=V1N, 3002-3003=V2N, 3004-3005=V3N ...
# All FLOAT32, word order typically ABCD (Big Endian).
# Power in W, Energy in Wh â†’ convert to kW, kWh.

def _read_schneider_pm2120(client, slave_id: int, word_order: str, base_address: int) -> Dict[str, Any]:
    wo = word_order if word_order in ("ABCD", "CDAB", "BADC", "DCBA") else "ABCD"
    BASE = base_address + 3000  # 0-based; manual shows 3001 = 3001-1
    regs = _read_block(client, BASE, 38, slave_id, fc=3)
    if len(regs) < 38:
        raise RuntimeError(f"Schneider PM2120: short read ({len(regs)}/38)")

    v1n   = _f(regs,  0, wo)
    v2n   = _f(regs,  2, wo)
    v3n   = _f(regs,  4, wo)
    i1    = _f(regs,  6, wo)
    i2    = _f(regs,  8, wo)
    i3    = _f(regs, 10, wo)
    kw    = _safe(_f(regs, 12, wo), 0.001)   # W â†’ kW
    kvar  = _safe(_f(regs, 14, wo), 0.001)   # VAr â†’ kVAr
    kva   = _safe(_f(regs, 16, wo), 0.001)   # VA â†’ kVA
    pf    = _f(regs, 18, wo)
    freq  = _f(regs, 20, wo)
    kwh   = _safe(_f(regs, 22, wo), 0.001)   # Wh â†’ kWh
    kvarh = _safe(_f(regs, 24, wo), 0.001)   # VArh â†’ kVArh
    thd_v1 = _f(regs, 26, wo)
    thd_v2 = _f(regs, 28, wo)
    thd_v3 = _f(regs, 30, wo)
    thd_i1 = _f(regs, 32, wo)
    thd_i2 = _f(regs, 34, wo)
    thd_i3 = _f(regs, 36, wo)

    vavg = None
    if v1n is not None and v2n is not None and v3n is not None:
        vavg = (v1n + v2n + v3n) / 3.0
    iavg = None
    if i1 is not None and i2 is not None and i3 is not None:
        iavg = (i1 + i2 + i3) / 3.0

    return {
        "V1N": v1n, "V2N": v2n, "V3N": v3n, "Vavg": vavg,
        "I1": i1, "I2": i2, "I3": i3, "Iavg": iavg,
        "kW": kw, "kVAr": kvar, "kVA": kva,
        "PF": pf, "PFavg": pf,
        "Frequency": freq,
        "Import_kWh": kwh, "Net_kWh": kwh,
        "THD Voltage V1N": thd_v1, "THD Voltage V2N": thd_v2, "THD Voltage V3N": thd_v3,
        "THD Current I1": thd_i1, "THD Current I2": thd_i2, "THD Current I3": thd_i3,
    }


# â”€â”€ 3. Circutor CVM-C10 / CVM-B100 â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# FC03, 0-based addresses. Power in W, Energy in Wh.

def _read_circutor_cvm(client, slave_id: int, word_order: str, base_address: int) -> Dict[str, Any]:
    wo = word_order if word_order in ("ABCD", "CDAB", "BADC", "DCBA") else "ABCD"
    BASE = base_address
    regs = _read_block(client, BASE, 34, slave_id, fc=3)
    if len(regs) < 34:
        raise RuntimeError(f"Circutor CVM: short read ({len(regs)}/34)")

    v1n   = _f(regs,  0, wo)
    v2n   = _f(regs,  2, wo)
    v3n   = _f(regs,  4, wo)
    i1    = _f(regs,  6, wo)
    i2    = _f(regs,  8, wo)
    i3    = _f(regs, 10, wo)
    kw    = _safe(_f(regs, 12, wo), 0.001)
    kvar  = _safe(_f(regs, 14, wo), 0.001)
    kva   = _safe(_f(regs, 16, wo), 0.001)
    pf    = _f(regs, 18, wo)
    freq  = _f(regs, 20, wo)
    kwh_i = _safe(_f(regs, 22, wo), 0.001)
    kwh_e = _safe(_f(regs, 24, wo), 0.001)
    kvarh_i = _safe(_f(regs, 26, wo), 0.001)
    kvarh_e = _safe(_f(regs, 28, wo), 0.001)
    thd_v = _f(regs, 30, wo)
    thd_i = _f(regs, 32, wo)

    vavg = None
    if v1n is not None and v2n is not None and v3n is not None:
        vavg = (v1n + v2n + v3n) / 3.0
    iavg = None
    if i1 is not None and i2 is not None and i3 is not None:
        iavg = (i1 + i2 + i3) / 3.0
    net = None
    if kwh_i is not None and kwh_e is not None:
        net = kwh_i - kwh_e

    return {
        "V1N": v1n, "V2N": v2n, "V3N": v3n, "Vavg": vavg,
        "I1": i1, "I2": i2, "I3": i3, "Iavg": iavg,
        "kW": kw, "kVAr": kvar, "kVA": kva,
        "PF": pf, "PFavg": pf,
        "Frequency": freq,
        "Import_kWh": kwh_i, "Export_kWh": kwh_e, "Net_kWh": net,
        "THD Voltage V1N": thd_v,
        "THD Current I1": thd_i,
    }


# â”€â”€ 4. Accuenergy Acuvim II / Acuvim-L â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# FC03, base address 1000 (0-based). Power in W, Energy in Wh.

def _read_accuenergy_acuvim(client, slave_id: int, word_order: str, base_address: int) -> Dict[str, Any]:
    wo = word_order if word_order in ("ABCD", "CDAB", "BADC", "DCBA") else "ABCD"
    BASE = base_address + 1000
    regs = _read_block(client, BASE, 30, slave_id, fc=3)
    if len(regs) < 30:
        raise RuntimeError(f"Acuvim: short read ({len(regs)}/30)")

    v1n   = _f(regs,  0, wo)
    v2n   = _f(regs,  2, wo)
    v3n   = _f(regs,  4, wo)
    i1    = _f(regs,  6, wo)
    i2    = _f(regs,  8, wo)
    i3    = _f(regs, 10, wo)
    kw    = _safe(_f(regs, 12, wo), 0.001)
    kvar  = _safe(_f(regs, 14, wo), 0.001)
    kva   = _safe(_f(regs, 16, wo), 0.001)
    pf    = _f(regs, 18, wo)
    freq  = _f(regs, 20, wo)
    kwh_i = _safe(_f(regs, 22, wo), 0.001)
    kwh_e = _safe(_f(regs, 24, wo), 0.001)
    thd_v1 = _f(regs, 26, wo)
    thd_i1 = _f(regs, 28, wo)

    vavg = None
    if v1n is not None and v2n is not None and v3n is not None:
        vavg = (v1n + v2n + v3n) / 3.0
    iavg = None
    if i1 is not None and i2 is not None and i3 is not None:
        iavg = (i1 + i2 + i3) / 3.0
    net = None
    if kwh_i is not None and kwh_e is not None:
        net = kwh_i - kwh_e

    return {
        "V1N": v1n, "V2N": v2n, "V3N": v3n, "Vavg": vavg,
        "I1": i1, "I2": i2, "I3": i3, "Iavg": iavg,
        "kW": kw, "kVAr": kvar, "kVA": kva,
        "PF": pf, "PFavg": pf,
        "Frequency": freq,
        "Import_kWh": kwh_i, "Export_kWh": kwh_e, "Net_kWh": net,
        "THD Voltage V1N": thd_v1, "THD Current I1": thd_i1,
    }


# â”€â”€ 5. Secure Elite 440 / 441 â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# FC03, 0-based. Power in W, Energy in Wh.

def _read_secure_elite440(client, slave_id: int, word_order: str, base_address: int) -> Dict[str, Any]:
    wo = word_order if word_order in ("ABCD", "CDAB", "BADC", "DCBA") else "ABCD"
    BASE = base_address
    regs = _read_block(client, BASE, 28, slave_id, fc=3)
    if len(regs) < 28:
        raise RuntimeError(f"Secure Elite: short read ({len(regs)}/28)")

    vavg  = _f(regs,  0, wo)
    _vll  = _f(regs,  2, wo)   # L-L average (not needed for canonical keys)
    i1    = _f(regs,  4, wo)
    i2    = _f(regs,  6, wo)
    i3    = _f(regs,  8, wo)
    kw    = _safe(_f(regs, 10, wo), 0.001)
    kvar  = _safe(_f(regs, 12, wo), 0.001)
    kva   = _safe(_f(regs, 14, wo), 0.001)
    pf    = _f(regs, 16, wo)
    freq  = _f(regs, 18, wo)
    kwh_i = _safe(_f(regs, 20, wo), 0.001)
    kwh_e = _safe(_f(regs, 22, wo), 0.001)
    thd_v = _f(regs, 24, wo)
    thd_i = _f(regs, 26, wo)

    iavg = None
    if i1 is not None and i2 is not None and i3 is not None:
        iavg = (i1 + i2 + i3) / 3.0
    net = None
    if kwh_i is not None and kwh_e is not None:
        net = kwh_i - kwh_e

    return {
        "Vavg": vavg,
        "I1": i1, "I2": i2, "I3": i3, "Iavg": iavg,
        "kW": kw, "kVAr": kvar, "kVA": kva,
        "PF": pf, "PFavg": pf,
        "Frequency": freq,
        "Import_kWh": kwh_i, "Export_kWh": kwh_e, "Net_kWh": net,
        "THD Voltage V1N": thd_v, "THD Current I1": thd_i,
    }


# â”€â”€ 6. Siemens 7KT0310 / 7KT0311 â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# FC03, 0-based. Power in W, Energy in Wh.

def _read_siemens_7kt(client, slave_id: int, word_order: str, base_address: int) -> Dict[str, Any]:
    wo = word_order if word_order in ("ABCD", "CDAB", "BADC", "DCBA") else "ABCD"
    BASE = base_address
    regs = _read_block(client, BASE, 20, slave_id, fc=3)
    if len(regs) < 20:
        raise RuntimeError(f"Siemens 7KT: short read ({len(regs)}/20)")

    v1n  = _f(regs,  0, wo)
    v2n  = _f(regs,  2, wo)
    v3n  = _f(regs,  4, wo)
    i1   = _f(regs,  6, wo)
    i2   = _f(regs,  8, wo)
    i3   = _f(regs, 10, wo)
    kw   = _safe(_f(regs, 12, wo), 0.001)
    pf   = _f(regs, 14, wo)
    freq = _f(regs, 16, wo)
    kwh  = _safe(_f(regs, 18, wo), 0.001)

    vavg = None
    if v1n is not None and v2n is not None and v3n is not None:
        vavg = (v1n + v2n + v3n) / 3.0
    iavg = None
    if i1 is not None and i2 is not None and i3 is not None:
        iavg = (i1 + i2 + i3) / 3.0

    return {
        "V1N": v1n, "V2N": v2n, "V3N": v3n, "Vavg": vavg,
        "I1": i1, "I2": i2, "I3": i3, "Iavg": iavg,
        "kW": kw, "PF": pf, "PFavg": pf,
        "Frequency": freq,
        "Import_kWh": kwh, "Net_kWh": kwh,
    }


# â”€â”€ 7. Multispan AVH-16-E3 / AVH-14 â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# FC03, 0-based. Power in W, Energy in Wh.

def _read_multispan_avh(client, slave_id: int, word_order: str, base_address: int) -> Dict[str, Any]:
    wo = word_order if word_order in ("ABCD", "CDAB", "BADC", "DCBA") else "ABCD"
    BASE = base_address
    regs = _read_block(client, BASE, 20, slave_id, fc=3)
    if len(regs) < 20:
        raise RuntimeError(f"Multispan AVH: short read ({len(regs)}/20)")

    vavg  = _f(regs,  0, wo)
    iavg  = _f(regs,  2, wo)
    kw    = _safe(_f(regs,  4, wo), 0.001)
    kvar  = _safe(_f(regs,  6, wo), 0.001)
    pf    = _f(regs,  8, wo)
    freq  = _f(regs, 10, wo)
    kwh_i = _safe(_f(regs, 12, wo), 0.001)
    kwh_e = _safe(_f(regs, 14, wo), 0.001)
    thd_v = _f(regs, 16, wo)
    thd_i = _f(regs, 18, wo)

    net = None
    if kwh_i is not None and kwh_e is not None:
        net = kwh_i - kwh_e

    # Multispan only reports averages â€” derive kVA from kW/PF
    kva = None
    if kw is not None and pf is not None and abs(pf) > 0.001:
        kva = abs(kw / pf)

    return {
        "Vavg": vavg, "Iavg": iavg,
        "kW": kw, "kVAr": kvar, "kVA": kva,
        "PF": pf, "PFavg": pf,
        "Frequency": freq,
        "Import_kWh": kwh_i, "Export_kWh": kwh_e, "Net_kWh": net,
        "THD Voltage V1N": thd_v, "THD Current I1": thd_i,
    }


# â”€â”€ 8. GFUVE FU2200B / FU2200A â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# FC03 or FC04 (model dependent). Using FC04 here (common default).
# Power in W, Energy in Wh.

def _read_gfuve_fu2200(client, slave_id: int, word_order: str, base_address: int) -> Dict[str, Any]:
    wo = word_order if word_order in ("ABCD", "CDAB", "BADC", "DCBA") else "ABCD"
    BASE = base_address
    regs = _read_block(client, BASE, 28, slave_id, fc=4)
    if len(regs) < 28:
        raise RuntimeError(f"GFUVE FU2200: short read ({len(regs)}/28)")

    vln   = _f(regs,  0, wo)   # L-N average
    _vll  = _f(regs,  2, wo)   # L-L (not stored in canonical)
    i1    = _f(regs,  4, wo)
    i2    = _f(regs,  6, wo)
    i3    = _f(regs,  8, wo)
    kw    = _safe(_f(regs, 10, wo), 0.001)
    kvar  = _safe(_f(regs, 12, wo), 0.001)
    kva   = _safe(_f(regs, 14, wo), 0.001)
    pf    = _f(regs, 16, wo)
    freq  = _f(regs, 18, wo)
    kwh_i = _safe(_f(regs, 20, wo), 0.001)
    kwh_e = _safe(_f(regs, 22, wo), 0.001)
    thd_v = _f(regs, 24, wo)
    thd_i = _f(regs, 26, wo)

    iavg = None
    if i1 is not None and i2 is not None and i3 is not None:
        iavg = (i1 + i2 + i3) / 3.0
    net = None
    if kwh_i is not None and kwh_e is not None:
        net = kwh_i - kwh_e

    return {
        "Vavg": vln,
        "I1": i1, "I2": i2, "I3": i3, "Iavg": iavg,
        "kW": kw, "kVAr": kvar, "kVA": kva,
        "PF": pf, "PFavg": pf,
        "Frequency": freq,
        "Import_kWh": kwh_i, "Export_kWh": kwh_e, "Net_kWh": net,
        "THD Voltage V1N": thd_v, "THD Current I1": thd_i,
    }


# â”€â”€ 9. Saia ALE3D5FD10C2A00 / ALD1D5 â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# FC03, 0-based. Power in W, Energy in Wh.

def _read_saia_ale3(client, slave_id: int, word_order: str, base_address: int) -> Dict[str, Any]:
    wo = word_order if word_order in ("ABCD", "CDAB", "BADC", "DCBA") else "ABCD"
    BASE = base_address
    regs = _read_block(client, BASE, 14, slave_id, fc=3)
    if len(regs) < 14:
        raise RuntimeError(f"Saia ALE3: short read ({len(regs)}/14)")

    vln   = _f(regs,  0, wo)   # L-N (single phase or average)
    i1    = _f(regs,  2, wo)
    i2    = _f(regs,  4, wo)
    i3    = _f(regs,  6, wo)
    kw    = _safe(_f(regs,  8, wo), 0.001)
    kwh_i = _safe(_f(regs, 10, wo), 0.001)
    kwh_e = _safe(_f(regs, 12, wo), 0.001)

    iavg = None
    if i1 is not None and i2 is not None and i3 is not None:
        iavg = (i1 + i2 + i3) / 3.0
    net = None
    if kwh_i is not None and kwh_e is not None:
        net = kwh_i - kwh_e

    return {
        "Vavg": vln,
        "I1": i1, "I2": i2, "I3": i3, "Iavg": iavg,
        "kW": kw,
        "Import_kWh": kwh_i, "Export_kWh": kwh_e, "Net_kWh": net,
    }


# â”€â”€ 10. Selec MFM383A / MFM376 / MFM284 â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# Same Modbus codec as MFM384 but fewer registers available.
# Safe to use the full MFM384 read â€” missing registers return NaN â†’ filtered downstream.

_read_selec_mfm383 = _read_selec_mfm384   # same codec, subset of registers


# â”€â”€ 11. Acrel ADW210-D16 / AMC96L-E4 â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# FC03 holding registers, 0-based.
# Power in kW (direct), Energy in kWh (direct) â€” no Wâ†’kW conversion needed.
# Typical CDAB word order.
# ADW210: 3-phase DIN rail multi-circuit. AMC96L: 96x96mm panel.
# Both share the same base register layout for V/I/P/Q/S/PF/Hz/kWh.

def _read_acrel_adw210(client, slave_id: int, word_order: str, base_address: int) -> Dict[str, Any]:
    wo = word_order if word_order in ("ABCD", "CDAB", "BADC", "DCBA") else "CDAB"
    BASE = base_address
    regs = _read_block(client, BASE, 30, slave_id, fc=3)
    if len(regs) < 30:
        raise RuntimeError(f"Acrel ADW210: short read ({len(regs)}/30)")

    v1n  = _f(regs,  0, wo)
    v2n  = _f(regs,  2, wo)
    v3n  = _f(regs,  4, wo)
    i1   = _f(regs,  6, wo)
    i2   = _f(regs,  8, wo)
    i3   = _f(regs, 10, wo)
    kw   = _f(regs, 12, wo)    # direct kW
    kvar = _f(regs, 14, wo)    # direct kVAr
    kva  = _f(regs, 16, wo)    # direct kVA
    pf   = _f(regs, 18, wo)
    freq = _f(regs, 20, wo)
    kwh  = _f(regs, 22, wo)    # direct kWh

    vavg = None
    if v1n is not None and v2n is not None and v3n is not None:
        vavg = (v1n + v2n + v3n) / 3.0
    iavg = None
    if i1 is not None and i2 is not None and i3 is not None:
        iavg = (i1 + i2 + i3) / 3.0

    return {
        "V1N": v1n, "V2N": v2n, "V3N": v3n, "Vavg": vavg,
        "I1": i1, "I2": i2, "I3": i3, "Iavg": iavg,
        "kW": kw, "kVAr": kvar, "kVA": kva,
        "PF": pf, "PFavg": pf,
        "Frequency": freq,
        "Import_kWh": kwh, "Net_kWh": kwh,
    }


# â”€â”€ 12. Schneider PM5000 / PM7000 â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# Same register layout as PM2120 but with additional harmonics available.
# Use PM2120 driver â€” captures all core parameters.

_read_schneider_pm5000 = _read_schneider_pm2120


# â”€â”€ 13. Circutor CVM-B150 / CEM-C21 â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# Same base layout as CVM-C10 â€” use CVM driver.

_read_circutor_cvm_b150 = _read_circutor_cvm


# â”€â”€ 14. Siemens 7KT0320 â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# Extended version of 7KT0310 with THD. Same base registers â€” use 7KT driver.

_read_siemens_7kt0320 = _read_siemens_7kt


# â”€â”€ 15. Multispan AVH-12-E3 â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# Compact 72x72 version â€” fewer parameters but same base layout.

_read_multispan_avh12 = _read_multispan_avh


# â”€â”€ 16. Accuenergy AcuRev 1310 â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# DIN rail version of Acuvim-L. Same register layout.

_read_accuenergy_acurev = _read_accuenergy_acuvim



# Rishabh AC meters: EM 13xx / Delta / LM / ED / Compact / ML.
# FC04 input registers, 30001 -> 0-based offset 0. FLOAT32 direct units.
def _read_rishabh_ac_basic(client, slave_id: int, word_order: str, base_address: int) -> Dict[str, Any]:
    wo = word_order if word_order in ("ABCD", "CDAB", "BADC", "DCBA") else "ABCD"
    regs: List[int] = []
    last_exc: Optional[Exception] = None
    for count in (78, 62, 52):
        try:
            regs = _read_block(client, base_address, count, slave_id, fc=4)
            break
        except Exception as exc:
            last_exc = exc
            regs = []
    if len(regs) < 52:
        raise RuntimeError(f"Rishabh AC: short read ({len(regs)}/52): {last_exc}")

    v1n = _f(regs, 0, wo);   v2n = _f(regs, 2, wo);   v3n = _f(regs, 4, wo)
    v12 = _f(regs, 6, wo);   v23 = _f(regs, 8, wo);   v31 = _f(regs, 10, wo)
    i1 = _f(regs, 12, wo);   i2 = _f(regs, 14, wo);   i3 = _f(regs, 16, wo)
    kw1 = _f(regs, 18, wo);  kw2 = _f(regs, 20, wo);  kw3 = _f(regs, 22, wo);  kw = _f(regs, 24, wo)
    kvar1 = _f(regs, 26, wo); kvar2 = _f(regs, 28, wo); kvar3 = _f(regs, 30, wo); kvar = _f(regs, 32, wo)
    kva1 = _f(regs, 34, wo); kva2 = _f(regs, 36, wo); kva3 = _f(regs, 38, wo); kva = _f(regs, 40, wo)
    pf1 = _f(regs, 42, wo);  pf2 = _f(regs, 44, wo);  pf3 = _f(regs, 46, wo);  pfavg = _f(regs, 48, wo)
    freq = _f(regs, 50, wo)
    kwh_i = _f(regs, 52, wo); kwh_e = _f(regs, 54, wo)
    kvarh_i = _f(regs, 56, wo); kvarh_e = _f(regs, 58, wo); kvah = _f(regs, 60, wo)
    thd_v1 = _f(regs, 62, wo); thd_v2 = _f(regs, 64, wo); thd_v3 = _f(regs, 66, wo)
    thd_i1 = _f(regs, 68, wo); thd_i2 = _f(regs, 70, wo); thd_i3 = _f(regs, 72, wo)
    run_hour = _f(regs, 74, wo); on_hour = _f(regs, 76, wo)

    vavg = None if None in (v1n, v2n, v3n) else (float(v1n) + float(v2n) + float(v3n)) / 3.0
    iavg = None if None in (i1, i2, i3) else (float(i1) + float(i2) + float(i3)) / 3.0
    net = None if None in (kwh_i, kwh_e) else float(kwh_i) - float(kwh_e)

    return {
        "Voltage L1-N": v1n, "Voltage L2-N": v2n, "Voltage L3-N": v3n,
        "Voltage L1-L2": v12, "Voltage L2-L3": v23, "Voltage L3-L1": v31,
        "V1N": v1n, "V2N": v2n, "V3N": v3n, "V12": v12, "V23": v23, "V31": v31, "Vavg": vavg,
        "Current L1": i1, "Current L2": i2, "Current L3": i3,
        "I1": i1, "I2": i2, "I3": i3, "Iavg": iavg,
        "Active Power L1": kw1, "Active Power L2": kw2, "Active Power L3": kw3,
        "kW1": kw1, "kW2": kw2, "kW3": kw3, "kW": kw,
        "Reactive Power L1": kvar1, "Reactive Power L2": kvar2, "Reactive Power L3": kvar3,
        "kVAr1": kvar1, "kVAr2": kvar2, "kVAr3": kvar3, "kVAr": kvar,
        "Apparent Power L1": kva1, "Apparent Power L2": kva2, "Apparent Power L3": kva3,
        "kVA1": kva1, "kVA2": kva2, "kVA3": kva3, "kVA": kva,
        "PF1": pf1, "PF2": pf2, "PF3": pf3, "PF": pfavg, "PFavg": pfavg,
        "Frequency": freq,
        "Import_kWh": kwh_i, "Export_kWh": kwh_e, "Net_kWh": net,
        "Import_kVArh": kvarh_i, "Export_kVArh": kvarh_e, "Apparent_kVAh": kvah,
        "THD Voltage V1N": thd_v1, "THD Voltage V2N": thd_v2, "THD Voltage V3N": thd_v3,
        "THD Current I1": thd_i1, "THD Current I2": thd_i2, "THD Current I3": thd_i3,
        "RunHour": run_hour, "OnHour": on_hour,
    }


def _read_rishabh_master(client, slave_id: int, word_order: str, base_address: int) -> Dict[str, Any]:
    # Master/LM advanced models share the same first live-value block for core SCADA values.
    return _read_rishabh_ac_basic(client, slave_id, word_order, base_address)


def _read_rishabh_emdc(client, slave_id: int, word_order: str, base_address: int) -> Dict[str, Any]:
    wo = word_order if word_order in ("ABCD", "CDAB", "BADC", "DCBA") else "ABCD"
    regs: List[int] = []
    last_exc: Optional[Exception] = None
    for count in (42, 24):
        try:
            regs = _read_block(client, base_address, count, slave_id, fc=4)
            break
        except Exception as exc:
            last_exc = exc
            regs = []
    if len(regs) < 24:
        raise RuntimeError(f"Rishabh EMDC: short read ({len(regs)}/24): {last_exc}")

    vdc = _f(regs, 0, wo)
    i1 = _f(regs, 2, wo); i2 = _f(regs, 4, wo); i3 = _f(regs, 6, wo); i4 = _f(regs, 8, wo)
    p1 = _safe(_f(regs, 10, wo), 0.001); p2 = _safe(_f(regs, 12, wo), 0.001)
    p3 = _safe(_f(regs, 14, wo), 0.001); p4 = _safe(_f(regs, 16, wo), 0.001)
    p_i = _safe(_f(regs, 18, wo), 0.001); p_e = _safe(_f(regs, 20, wo), 0.001)
    e_i = _safe(_f(regs, 38, wo), 0.001); e_e = _safe(_f(regs, 40, wo), 0.001)

    currents = [x for x in (i1, i2, i3, i4) if x is not None]
    iavg = (sum(float(x) for x in currents) / len(currents)) if currents else None
    net = None if None in (e_i, e_e) else float(e_i) - float(e_e)

    return {
        "Voltage DC": vdc, "Vavg": vdc,
        "Current Ch1": i1, "Current Ch2": i2, "Current Ch3": i3, "Current Ch4": i4,
        "I1": i1, "I2": i2, "I3": i3, "Iavg": iavg,
        "Power Ch1": p1, "Power Ch2": p2, "Power Ch3": p3, "Power Ch4": p4,
        "kW": p_i, "Export_kW": p_e,
        "Import_kWh": e_i, "Export_kWh": e_e, "Net_kWh": net,
        "PF": 1.0, "PFavg": 1.0,
    }

# â”€â”€ Driver dispatch table â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

_DRIVERS: Dict[str, Callable] = {
    "selec_mfm384":            _read_selec_mfm384,
    "selec_mfm384_harmonics":  _read_selec_mfm384_harmonics,
    "selec_mfm383":            _read_selec_mfm383,
    "schneider_pm2120":        _read_schneider_pm2120,
    "schneider_pm5000":        _read_schneider_pm5000,
    "circutor_cvm_c10":        _read_circutor_cvm,
    "circutor_cvm_b150":       _read_circutor_cvm_b150,
    "accuenergy_acuvim2":      _read_accuenergy_acuvim,
    "accuenergy_acurev1310":   _read_accuenergy_acurev,
    "secure_elite440":         _read_secure_elite440,
    "siemens_7kt0310":         _read_siemens_7kt,
    "siemens_7kt0320":         _read_siemens_7kt0320,
    "multispan_avh16":         _read_multispan_avh,
    "multispan_avh12":         _read_multispan_avh12,
    "gfuve_fu2200":            _read_gfuve_fu2200,
    "saia_ale3":               _read_saia_ale3,
    "acrel_adw210":            _read_acrel_adw210,
}

_RISHABH_AC_BASIC_IDS = (
    "rish_em1320", "rish_em1330", "rish_em1340", "rish_em2340ds",
    "rish_em3490ds", "rish_em3490dsi",
    "rish_delta_energy", "rish_delta_energy_nx", "rish_delta_power",
    "rish_delta_power_nx", "rish_delta_vaf", "rish_delta_vaf_nx",
    "rish_master3430", "rish_master3430i",
    "rish_lm1340", "rish_md1340",
    "rish_ed1100", "rish_ed1111", "rish_ed11x1", "rish_ed21x1", "rish_ed43xx",
    "rish_ducer_m01", "rish_ducer_m30", "rish_ducer_m40", "rish_ducer_mxx",
    "rish_cr12", "rish_cd30", "rish_cd60", "rish_optima_vaf",
    "rish_ml1400", "rish_ml1410",
)
_RISHABH_MASTER_IDS = (
    "rish_master3440", "rish_master3440i", "rish_master3440idl", "rish_master3480",
    "rish_lm1360", "rish_lm1360_rj12",
)
_RISHABH_DC_IDS = ("rish_emdc6000", "rish_emdc6001", "rish_emdc6002")

_DRIVERS.update({mid: _read_rishabh_ac_basic for mid in _RISHABH_AC_BASIC_IDS})
_DRIVERS.update({mid: _read_rishabh_master for mid in _RISHABH_MASTER_IDS})
_DRIVERS.update({mid: _read_rishabh_emdc for mid in _RISHABH_DC_IDS})


def get_driver(model_id: str) -> Callable:
    """Return the read function for a given model ID.

    Falls back to Selec MFM384 if model_id is unknown â€” safe default.
    """
    return _DRIVERS.get(str(model_id or "").strip().lower(), _read_selec_mfm384)


def read_meter(model_id: str, client, slave_id: int,
               word_order: str = "CDAB", base_address: int = 0) -> Dict[str, Any]:
    """Convenience wrapper: look up driver and call it."""
    driver = get_driver(model_id)
    return driver(client, slave_id, word_order, base_address)
