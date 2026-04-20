# core/modbus_codec.py
"""
Selec MFM384-C Modbus Codec
- FC04 Input Register reading and decoding (Float32)
- FC03 Holding Register reading/writing for setup parameters
- Thread-safe operations via worker lock
"""
from __future__ import annotations
import struct
import math
from typing import Dict, List, Tuple, Any, Optional, Sequence
from core.register_map import (
    MFM384RegisterMap, RegisterDefinition,
    SETUP_400XX, SETUP_400XX_DEFS, SETUP_400XX_BY_OFFSET, SETUP_400XX_BY_NAME,
    SetupRegisterDef, ENDIANNESS_TO_CODE
)

# =============================================================================
# Pymodbus Compatibility Helpers
# =============================================================================

def _rr_is_error(rr) -> bool:
    return (rr is None) or getattr(rr, "isError", lambda: True)()

def _call_with_slave(fn, slave_id: int, **kwargs):
    """Call a pymodbus client method with the correct slave ID parameter.

    pymodbus 3.x (3.0+) uses ``slave=``.
    pymodbus 2.x used ``unit=``.
    Some intermediate builds accepted ``device_id=`` but silently ignored it
    (the actual slave used was 0x00, causing 'no response' errors).
    We probe ``slave=`` first (works on 3.x), then fall back to ``unit=`` (2.x).
    """
    try:
        return fn(**kwargs, slave=slave_id)
    except TypeError:
        return fn(**kwargs, unit=slave_id)

def _read_input_registers(client, address: int, count: int, slave_id: int):
    return _call_with_slave(client.read_input_registers, slave_id, address=address, count=count)

def _read_holding_registers(client, address: int, count: int, slave_id: int):
    return _call_with_slave(client.read_holding_registers, slave_id, address=address, count=count)

def _write_register(client, address: int, value: int, slave_id: int):
    return _call_with_slave(client.write_register, slave_id, address=address, value=value)

def _write_registers(client, address: int, values, slave_id: int):
    return _call_with_slave(client.write_registers, slave_id, address=address, values=values)

# =============================================================================
# Float32 Decoding Helpers
# =============================================================================

def regs_to_float32(reg0: int, reg1: int, word_order: str = "CDAB") -> float:
    """
    Convert two 16-bit registers to IEEE 754 Float32.
    
    Word order mapping (assuming reg0 bytes = A B, reg1 bytes = C D):
    - CDAB: Mid Little Endian (Selec default)
    - ABCD: Big Endian
    - BADC: Byte-swapped Big Endian
    - DCBA: Little Endian
    """
    a = (reg0 >> 8) & 0xFF
    b = reg0 & 0xFF
    c = (reg1 >> 8) & 0xFF
    d = reg1 & 0xFF
    src = {"A": a, "B": b, "C": c, "D": d}
    order = word_order.upper().strip()
    if order not in ("ABCD", "CDAB", "BADC", "DCBA"):
        order = "CDAB"
    data = bytes([src[ch] for ch in order])
    return struct.unpack(">f", data)[0]


def regs_to_u32(reg_hi: int, reg_lo: int) -> int:
    """Convert two 16-bit registers to a 32-bit unsigned integer.
    
    Modbus sends high register first, then low register.
    So regs[0] is high word, regs[1] is low word.
    """
    return (reg_hi << 16) | reg_lo


def u32_to_regs(value: int) -> Tuple[int, int]:
    """Convert a 32-bit unsigned integer to two 16-bit registers.
    
    Returns (high_word, low_word) for Selec MFM384 big-endian register order.
    """
    lo = value & 0xFFFF
    hi = (value >> 16) & 0xFFFF
    return (hi, lo)


# =============================================================================
# FC04 Input Register Functions
# =============================================================================

# Many meters *claim* to support up to 125 registers per request (Modbus limit),
# but in practice some firmwares / RS485 adapters start failing above ~60-80.
# Default to a safer chunk size and add an adaptive fallback.
DEFAULT_MAX_REGS_PER_READ = 60


def _adaptive_read_fc04(client, start_addr: int, count: int, slave_id: int, max_chunk: int) -> List[int]:
    """Read FC04 input registers with adaptive chunk sizing.

    If a chunk read fails (timeout/illegal data value/etc.), reduce the chunk
    size and retry. This prevents a full "STALE" UI when the device cannot
    handle large Modbus frames.
    """
    regs: List[int] = []
    remaining = int(count)
    addr = int(start_addr)
    chunk = max(1, int(max_chunk))

    while remaining > 0:
        step = min(remaining, chunk)
        rr = _read_input_registers(client, address=addr, count=step, slave_id=slave_id)
        if _rr_is_error(rr):
            # reduce chunk and retry the *same* address
            if chunk <= 8:
                raise RuntimeError(f"FC04 read error at {addr} (count={step}): {rr}")
            chunk = max(8, chunk // 2)
            continue

        regs.extend(rr.registers)
        addr += step
        remaining -= step

    return regs


def calc_input_block_span(defs: Tuple[RegisterDefinition, ...]) -> Tuple[int, int]:
    """Calculate the register span needed to read all defined input registers."""
    min_off = min(d.offset for d in defs)
    max_off = max(d.offset for d in defs)
    start = min_off
    count = (max_off - min_off) + 2  # +2 because each float uses 2 regs
    return start, count


def decode_fc04_registers(registers: List[int], start_offset: int, word_order: str = "CDAB") -> Dict[str, float]:
    """Decode raw FC04 registers into named float values."""
    out: Dict[str, float] = {}
    for d in MFM384RegisterMap.INPUT_REGISTERS:
        i = d.offset - start_offset
        if i < 0 or i + 1 >= len(registers):
            continue
        out[d.name] = regs_to_float32(registers[i], registers[i + 1], word_order=word_order)
    return out


def normalize_values(decoded: Dict[str, float]) -> Dict[str, float]:
    """
    Map MFM384 register names -> normalized app keys expected by tiles/logging.
    Also keeps full named values for Real Values tab.
    """
    norm = dict(decoded)

    # Normalized tile keys
    if "Total kW" in decoded:
        norm["kW"] = decoded["Total kW"]
    if "Total kVA" in decoded:
        norm["kVA"] = decoded["Total kVA"]
    if "Total kVAr" in decoded:
        norm["kVAr"] = decoded["Total kVAr"]
    if "Total kWh Import" in decoded:
        norm["Import_kWh"] = decoded["Total kWh Import"]
    if "Total kWh Export" in decoded:
        norm["Export_kWh"] = decoded["Total kWh Export"]
    if "Average Voltage LN" in decoded:
        norm["Vavg"] = decoded["Average Voltage LN"]
    if "Average Current" in decoded:
        norm["Iavg"] = decoded["Average Current"]
    if "Average PF" in decoded:
        norm["PFavg"] = decoded["Average PF"]
    if "Frequency" in decoded:
        norm["Frequency"] = decoded["Frequency"]
    if "Total Net kWh" in decoded:
        norm["Net_kWh"] = decoded["Total Net kWh"]
    if "Run hour" in decoded:
        norm["RunHour"] = decoded["Run hour"]
    if "Current I1" in decoded:
        norm["I1"] = decoded["Current I1"]
    if "Current I2" in decoded:
        norm["I2"] = decoded["Current I2"]
    if "Current I3" in decoded:
        norm["I3"] = decoded["Current I3"]
    if "Voltage V1N" in decoded:
        norm["V1N"] = decoded["Voltage V1N"]
    if "Voltage V2N" in decoded:
        norm["V2N"] = decoded["Voltage V2N"]
    if "Voltage V3N" in decoded:
        norm["V3N"] = decoded["Voltage V3N"]
    if "Voltage V12" in decoded:
        norm["V12"] = decoded["Voltage V12"]
    if "Voltage V23" in decoded:
        norm["V23"] = decoded["Voltage V23"]
    if "Voltage V31" in decoded:
        norm["V31"] = decoded["Voltage V31"]

    return norm


DEFAULT_HARMONIC_PARAMETERS: Tuple[str, ...] = ("V1N", "V2N", "V3N", "V12", "V23", "V31", "I1", "I2", "I3")


def read_mfm384_harmonics_fc04(
    client,
    slave_id: int,
    word_order: str = "CDAB",
    base_address: int = 0,
    harmonics: Optional[Sequence[int]] = None,
    parameters: Optional[Sequence[str]] = None,
) -> Dict[str, Any]:
    """
    Read Selec MFM384 individual harmonics using the manual formula:

        offset = 143 + ((harmonic_no - 2) * 2) + (60 * constant_parameter)

    Each selected parameter is read as one compact contiguous block. The normal
    Selec model does not call this by default because all 2nd-31st harmonics
    across 9 channels add 540 extra registers per poll cycle.
    """
    harmonic_numbers = list(harmonics or range(MFM384RegisterMap.HARMONIC_MIN, MFM384RegisterMap.HARMONIC_MAX + 1))
    if not harmonic_numbers:
        return {}

    min_h = min(int(h) for h in harmonic_numbers)
    max_h = max(int(h) for h in harmonic_numbers)
    # Validate the requested range once before touching the Modbus bus.
    MFM384RegisterMap.harmonic_offset(min_h, "V1N")
    MFM384RegisterMap.harmonic_offset(max_h, "V1N")

    param_keys = [
        MFM384RegisterMap.harmonic_parameter(p)[0]
        for p in (parameters or DEFAULT_HARMONIC_PARAMETERS)
    ]

    max_chunk = int(getattr(client, "_mfm384_harmonic_max_regs", DEFAULT_MAX_REGS_PER_READ) or DEFAULT_MAX_REGS_PER_READ)
    if max_chunk <= 0:
        max_chunk = DEFAULT_MAX_REGS_PER_READ

    out: Dict[str, Any] = {}
    failed_count = 0
    voltage_values: List[float] = []
    current_values: List[float] = []

    for param in param_keys:
        key, _, _label = MFM384RegisterMap.harmonic_parameter(param)
        start_off = MFM384RegisterMap.harmonic_offset(min_h, key)
        end_off = MFM384RegisterMap.harmonic_offset(max_h, key)
        register_count = (end_off - start_off) + 2

        try:
            regs = _adaptive_read_fc04(
                client,
                start_addr=base_address + start_off,
                count=register_count,
                slave_id=slave_id,
                max_chunk=max_chunk,
            )
        except Exception:
            failed_count += 1
            continue

        for harmonic in harmonic_numbers:
            h = int(harmonic)
            idx = MFM384RegisterMap.harmonic_offset(h, key) - start_off
            if idx < 0 or idx + 1 >= len(regs):
                continue
            value = regs_to_float32(regs[idx], regs[idx + 1], word_order=word_order)
            out[f"H{h:02d}_{key}"] = value
            if isinstance(value, (int, float)) and math.isfinite(float(value)):
                if key.startswith("V"):
                    voltage_values.append(value)
                else:
                    current_values.append(value)

    if voltage_values:
        out["Harmonic Voltage Worst"] = max(voltage_values)
    if current_values:
        out["Harmonic Current Worst"] = max(current_values)
    out["Harmonics_read_failed_count"] = float(failed_count)
    out["Harmonics_ok"] = 1.0 if failed_count == 0 else 0.0
    return out


def read_mfm384_fc04(
    client,
    slave_id: int,
    word_order: str = "CDAB",
    base_address: int = 0,
    include_harmonics: bool = False,
) -> Dict[str, Any]:
    """
    Read all FC04 input registers (in chunks if needed) and decode to float values.
    Uses device_id parameter for pymodbus 3.x compatibility.
    """
    start_off, total_count = calc_input_block_span(MFM384RegisterMap.INPUT_REGISTERS)
    
    # Adaptive chunk read (prevents failures on devices that can't handle large reads)
    max_chunk = int(getattr(client, "_mfm384_max_regs", DEFAULT_MAX_REGS_PER_READ) or DEFAULT_MAX_REGS_PER_READ)
    if max_chunk <= 0:
        max_chunk = DEFAULT_MAX_REGS_PER_READ
    all_regs = _adaptive_read_fc04(
        client,
        start_addr=base_address + start_off,
        count=total_count,
        slave_id=slave_id,
        max_chunk=max_chunk,
    )

    decoded = decode_fc04_registers(all_regs, start_offset=start_off, word_order=word_order)
    values = normalize_values(decoded)
    if include_harmonics:
        try:
            values.update(
                read_mfm384_harmonics_fc04(
                    client=client,
                    slave_id=slave_id,
                    word_order=word_order,
                    base_address=base_address,
                )
            )
        except Exception:
            values["Harmonics_read_failed_count"] = 1.0
            values["Harmonics_ok"] = 0.0
    return values


def read_raw_fc04(client, slave_id: int, count: int = 160, base_address: int = 0) -> List[int]:
    """Read raw FC04 registers (for debugging/diagnostics)."""
    max_chunk = DEFAULT_MAX_REGS_PER_READ
    return _adaptive_read_fc04(
        client,
        start_addr=base_address,
        count=count,
        slave_id=slave_id,
        max_chunk=max_chunk,
    )


def read_meter_serial(client, slave_id: int, base_address: int = 0) -> str:
    """
    Read meter serial number from FC04 registers 30684-30685 (offset 684-685).
    
    The serial number is stored as a 32-bit value across 2 registers.
    Register order: [high_word, low_word] (big-endian).
    Returns the serial as a decimal string.
    """
    try:
        # Serial number is at offset 684-685 from base 30000
        serial_offset = 684
        rr = _read_input_registers(client, address=base_address + serial_offset, count=2, slave_id=slave_id)
        if _rr_is_error(rr):
            return f"Error: {rr}"
        
        # Combine as 32-bit value: [high_word, low_word]
        serial_value = (rr.registers[0] << 16) | rr.registers[1]
        return str(serial_value)
    except Exception as e:
        return f"Error: {e}"


# =============================================================================
# FC03 Holding Register Functions (Setup 400xx)
# =============================================================================

def read_setup_400xx(client, slave_id: int, base_address: int = 0) -> Dict[str, Any]:
    """
    Read all defined 400xx setup parameters and return decoded values.
    
    Returns dict with:
    - Enum fields: {"code": int, "label": str}
    - Scaled fields: float (e.g., 0.5 sec for pulse duration)
    - Write-only fields: None (not readable)
    - Other fields: int
    """
    out: Dict[str, Any] = {}
    
    for reg_def in SETUP_400XX_DEFS:
        # Skip write-only registers (can't read them)
        if reg_def.write_only:
            out[reg_def.name] = None
            continue
            
        try:
            rr = _read_holding_registers(client, address=base_address + reg_def.offset, count=reg_def.nregs, slave_id=slave_id)
            if _rr_is_error(rr):
                out[reg_def.name] = f"Error: {rr}"
                continue
            
            regs = rr.registers
            
            # Decode based on dtype
            if reg_def.nregs == 2:
                # 32-bit value (PT Primary)
                # Selec MFM384 uses big-endian register order: [high_word, low_word]
                raw_value = regs_to_u32(regs[0], regs[1])
            else:
                raw_value = regs[0]
            
            if reg_def.dtype == "enum" and reg_def.enum_map:
                label = reg_def.enum_map.get(raw_value, f"Unknown({raw_value})")
                out[reg_def.name] = {"code": raw_value, "label": label}
            elif reg_def.dtype == "scaled" and reg_def.scale:
                out[reg_def.name] = raw_value / reg_def.scale
            else:
                out[reg_def.name] = raw_value
                
        except Exception as e:
            out[reg_def.name] = f"Error: {str(e)}"
    
    return out


def validate_setup_value(name: str, value: Any, current_setup: Optional[Dict[str, Any]] = None) -> Tuple[bool, str, int]:
    """
    Validate a setup value before writing.
    
    Returns: (valid: bool, error_msg: str, register_value: int)
    
    Special validations:
    - CT Primary depends on CT Secondary value
    - Scaled values need conversion
    - Enum values need code lookup
    """
    reg_def = SETUP_400XX_BY_NAME.get(name)
    if not reg_def:
        return False, f"Unknown parameter: {name}", 0
    
    # Handle enum values (accept either code or label)
    if reg_def.dtype == "enum" and reg_def.enum_map:
        if isinstance(value, str):
            # Find code from label
            for code, label in reg_def.enum_map.items():
                if label == value or label.startswith(value):
                    value = code
                    break
            else:
                return False, f"Invalid enum value: {value}", 0
        
        if not isinstance(value, int) or value not in reg_def.enum_map:
            return False, f"Invalid enum code: {value}. Valid: {list(reg_def.enum_map.keys())}", 0
        
        return True, "", value
    
    # Handle scaled values
    if reg_def.dtype == "scaled" and reg_def.scale:
        try:
            float_val = float(value)
            int_val = int(round(float_val * reg_def.scale))
        except (ValueError, TypeError):
            return False, f"Invalid number: {value}", 0
        
        if reg_def.min_val is not None and int_val < reg_def.min_val:
            return False, f"Value {float_val} too low (min {reg_def.min_val / reg_def.scale})", 0
        if reg_def.max_val is not None and int_val > reg_def.max_val:
            return False, f"Value {float_val} too high (max {reg_def.max_val / reg_def.scale})", 0
        
        return True, "", int_val
    
    # Handle integers
    try:
        int_val = int(value)
    except (ValueError, TypeError):
        return False, f"Invalid integer: {value}", 0
    
    # Special validation: CT Primary depends on CT Secondary
    if name == "CT Primary" and current_setup:
        ct_sec = current_setup.get("CT Secondary", 5)
        if isinstance(ct_sec, dict):
            ct_sec = ct_sec.get("code", 5)
        if ct_sec == 1:
            min_val, max_val = 1, 10000
        else:
            min_val, max_val = 5, 10000
        if int_val < min_val or int_val > max_val:
            return False, f"CT Primary must be {min_val}-{max_val} when CT Secondary={ct_sec}", 0
        return True, "", int_val
    
    # Standard range validation
    if reg_def.min_val is not None and int_val < reg_def.min_val:
        return False, f"Value {int_val} too low (min {reg_def.min_val})", 0
    if reg_def.max_val is not None and int_val > reg_def.max_val:
        return False, f"Value {int_val} too high (max {reg_def.max_val})", 0
    
    return True, "", int_val


def write_setup_400xx(client, slave_id: int, name: str, value: Any, 
                      base_address: int = 0, current_setup: Optional[Dict[str, Any]] = None) -> Tuple[bool, str]:
    """
    Write a single setup parameter.
    
    Args:
        client: Modbus client
        slave_id: Device slave ID
        name: Parameter name from SETUP_400XX_DEFS
        value: Value to write (will be validated and converted)
        base_address: Base address offset (usually 0)
        current_setup: Current setup values for validation dependencies
    
    Returns: (success: bool, message: str)
    """
    reg_def = SETUP_400XX_BY_NAME.get(name)
    if not reg_def:
        return False, f"Unknown parameter: {name}"
    
    # Validate the value
    valid, error_msg, reg_value = validate_setup_value(name, value, current_setup)
    if not valid:
        return False, error_msg
    
    try:
        if reg_def.nregs == 2:
            # Write 32-bit value as two registers
            hi, lo = u32_to_regs(reg_value)
            rr = _write_registers(client, address=base_address + reg_def.offset, values=[hi, lo], slave_id=slave_id)
        else:
            # Write single register
            rr = _write_register(client, address=base_address + reg_def.offset, value=reg_value, slave_id=slave_id)
        
        if _rr_is_error(rr):
            return False, f"Write error: {rr}"
        
        return True, f"Successfully wrote {name} = {value}"
        
    except Exception as e:
        return False, f"Exception: {str(e)}"


def write_reset_command(client, slave_id: int, name: str, base_address: int = 0) -> Tuple[bool, str]:
    """
    Execute a write-only reset command (writes value 1 to the register).
    
    Valid reset commands:
    - Factory Default, Reset Active Energy, Reset Apparent Energy, etc.
    """
    reg_def = SETUP_400XX_BY_NAME.get(name)
    if not reg_def:
        return False, f"Unknown command: {name}"
    
    if not reg_def.write_only:
        return False, f"{name} is not a reset command"
    
    try:
        rr = _write_register(client, address=base_address + reg_def.offset, value=1, slave_id=slave_id)
        
        if _rr_is_error(rr):
            return False, f"Reset command error: {rr}"
        
        return True, f"Successfully executed: {name}"
        
    except Exception as e:
        return False, f"Exception: {str(e)}"


def format_setup_value(name: str, raw_value: Any) -> str:
    """Format a setup value for display."""
    if raw_value is None:
        return "(write-only)"
    if isinstance(raw_value, str) and raw_value.startswith("Error"):
        return raw_value
    if isinstance(raw_value, dict):
        return f"{raw_value.get('label', '?')} ({raw_value.get('code', '?')})"
    if isinstance(raw_value, float):
        return f"{raw_value:.2f}"
    return str(raw_value)


def get_setup_range_desc(name: str) -> str:
    """Get a human-readable range description for a setup parameter."""
    reg_def = SETUP_400XX_BY_NAME.get(name)
    if not reg_def:
        return ""
    
    if reg_def.write_only:
        return "Write 1 to execute"
    
    if reg_def.dtype == "enum" and reg_def.enum_map:
        options = ", ".join(f"{k}={v}" for k, v in reg_def.enum_map.items())
        return f"Options: {options}"
    
    if reg_def.dtype == "scaled" and reg_def.scale:
        min_f = reg_def.min_val / reg_def.scale if reg_def.min_val else 0
        max_f = reg_def.max_val / reg_def.scale if reg_def.max_val else 0
        return f"Range: {min_f:.2f} - {max_f:.2f}"
    
    if reg_def.min_val is not None and reg_def.max_val is not None:
        return f"Range: {reg_def.min_val} - {reg_def.max_val}"
    
    return reg_def.desc
