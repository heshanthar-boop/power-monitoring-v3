"""
Modbus TCP Slave Server
=======================
Exposes live meter readings so external SCADA systems (Ignition, WinCC,
Node-RED, custom PLCs) can poll this app using standard Modbus TCP FC03/FC04.

Register map (all INPUT REGISTERS — FC04, starting at address 0):
  Each meter occupies a block of 40 registers (80 bytes).
  Meter index: 0 = TOTAL, 1 = Meter 1, 2 = Meter 2, … 6 = Meter 6.

  Offset   Description            Unit       Scale     Type
  ------   -------------------    ------     -----     ----
  0        V1N (L1-N voltage)     V          ×10       uint16
  1        V2N                    V          ×10       uint16
  2        V3N                    V          ×10       uint16
  3        Vavg                   V          ×10       uint16
  4        V12                    V          ×10       uint16
  5        V23                    V          ×10       uint16
  6        V31                    V          ×10       uint16
  7        I1 (L1 current)        A          ×100      uint16
  8        I2                     A          ×100      uint16
  9        I3                     A          ×100      uint16
  10       Iavg                   A          ×100      uint16
  11       Freq                   Hz         ×100      uint16
  12       PF1                    —          ×1000     int16  (signed)
  13       PF2                    —          ×1000     int16  (signed)
  14       PF3                    —          ×1000     int16  (signed)
  15       PFavg                  —          ×1000     int16  (signed)
  16-17    kW  (32-bit float)     kW         IEEE754   float32 BE
  18-19    kVA                    kVA        IEEE754   float32 BE
  20-21    kVAr                   kVAr       IEEE754   float32 BE
  22-23    Import_kWh             kWh        IEEE754   float32 BE
  24-25    Export_kWh             kWh        IEEE754   float32 BE
  26       Quality (0=GOOD, 1=STALE, 2=COMM_LOST, 3=DISABLED, 255=OFFLINE)
  27-39    (reserved, = 0)

  Address formula: base_address + meter_index * 40 + offset
  Default base_address = 0, so Meter 1 kW is at registers 40-41.

TOTAL block occupies registers 0-39.
"""
from __future__ import annotations

import logging
import socket
import struct
import threading
import time
from typing import Any, Dict, List, Optional

log = logging.getLogger("modbus_slave")


# ── Register map ──────────────────────────────────────────────────────────────

REGS_PER_METER = 40   # register slots per meter/TOTAL block

# Quality code mapping
_QUALITY_CODE = {
    "GOOD":      0,
    "STALE":     1,
    "COMM_LOST": 2,
    "DISABLED":  3,
}
_QUALITY_OFFLINE = 255


def _f32_to_2regs(value: float) -> tuple[int, int]:
    """Pack a float into two big-endian 16-bit registers (hi, lo)."""
    try:
        raw = struct.pack(">f", float(value))
        hi, lo = struct.unpack(">HH", raw)
        return hi, lo
    except Exception:
        return 0, 0


def _clamp_u16(v: float) -> int:
    return max(0, min(65535, int(round(v))))


def _clamp_i16(v: float) -> int:
    raw = int(round(v))
    if raw < -32768:
        raw = -32768
    if raw > 32767:
        raw = 32767
    # Store as two's complement in uint16 range
    return raw & 0xFFFF


def _values_to_registers(values: dict, quality: str) -> List[int]:
    """Build a 40-register block from a canonical values dict."""
    r = [0] * REGS_PER_METER

    def _g(key, default=0.0):
        v = values.get(key)
        return float(v) if isinstance(v, (int, float)) else float(default)

    r[0]  = _clamp_u16(_g("V1N")   * 10)
    r[1]  = _clamp_u16(_g("V2N")   * 10)
    r[2]  = _clamp_u16(_g("V3N")   * 10)
    r[3]  = _clamp_u16(_g("Vavg")  * 10)
    r[4]  = _clamp_u16(_g("V12")   * 10)
    r[5]  = _clamp_u16(_g("V23")   * 10)
    r[6]  = _clamp_u16(_g("V31")   * 10)
    r[7]  = _clamp_u16(_g("I1")    * 100)
    r[8]  = _clamp_u16(_g("I2")    * 100)
    r[9]  = _clamp_u16(_g("I3")    * 100)
    r[10] = _clamp_u16(_g("Iavg")  * 100)
    r[11] = _clamp_u16(_g("Freq")  * 100)
    r[12] = _clamp_i16(_g("PF1")   * 1000)
    r[13] = _clamp_i16(_g("PF2")   * 1000)
    r[14] = _clamp_i16(_g("PF3")   * 1000)
    r[15] = _clamp_i16(_g("PFavg") * 1000)

    hi, lo = _f32_to_2regs(_g("kW"))
    r[16], r[17] = hi, lo
    hi, lo = _f32_to_2regs(_g("kVA"))
    r[18], r[19] = hi, lo
    hi, lo = _f32_to_2regs(_g("kVAr"))
    r[20], r[21] = hi, lo
    hi, lo = _f32_to_2regs(_g("Import_kWh"))
    r[22], r[23] = hi, lo
    hi, lo = _f32_to_2regs(_g("Export_kWh"))
    r[24], r[25] = hi, lo

    r[26] = _QUALITY_CODE.get(str(quality).upper(), _QUALITY_OFFLINE)

    return r


# ── Modbus TCP framing ─────────────────────────────────────────────────────────

MODBUS_EXCEPTION_ILLEGAL_FUNCTION  = 0x01
MODBUS_EXCEPTION_ILLEGAL_ADDRESS   = 0x02
MODBUS_EXCEPTION_ILLEGAL_DATA      = 0x03


def _build_exception(tid: int, unit: int, fc: int, exc_code: int) -> bytes:
    pdu = bytes([unit, fc | 0x80, exc_code])
    mbap = struct.pack(">HHHB", tid, 0, 3, unit)
    # Actually: length field = byte count after unit field = len(pdu) - 1 + 1
    length = len(pdu)
    return struct.pack(">HHH", tid, 0, length) + pdu


def _build_read_response(tid: int, unit: int, fc: int, data: bytes) -> bytes:
    byte_count = len(data)
    pdu = bytes([unit, fc, byte_count]) + data
    return struct.pack(">HHH", tid, 0, len(pdu)) + pdu


def _parse_mbap(raw: bytes) -> Optional[tuple]:
    """Parse 7-byte MBAP header + at least 1 byte PDU.

    Returns (tid, protocol_id, length, unit_id) or None.
    """
    if len(raw) < 8:
        return None
    tid, proto, length, unit = struct.unpack(">HHHB", raw[:7])
    if proto != 0:
        return None
    return tid, proto, length, unit


# ── Slave server ──────────────────────────────────────────────────────────────

class ModbusSlave:
    """
    Modbus TCP slave server.

    Usage:
        slave = ModbusSlave(cfg=cfg)
        slave.start()
        # on each data tick:
        slave.update_registers(total_values, total_quality,
                               meters_by_id={1: (values, quality), ...})
        # shutdown:
        slave.stop()
    """

    def __init__(self, cfg: Dict[str, Any]) -> None:
        self._cfg = cfg
        self._lock = threading.Lock()
        self._registers: List[int] = [0] * (REGS_PER_METER * 7)  # TOTAL + 6 meters
        self._thread: Optional[threading.Thread] = None
        self._sock: Optional[socket.socket] = None
        self._running = False
        self._base_address = 0
        self._last_error: str = ""

    # ── Public API ─────────────────────────────────────────────────────────────

    def start(self) -> bool:
        """Start the TCP listener. Returns True if successfully bound."""
        if self._running:
            return True
        scfg = (self._cfg.get("modbus_slave") or {})
        host = str(scfg.get("host", "0.0.0.0") or "0.0.0.0").strip()
        port = int(scfg.get("port", 502) or 502)
        self._base_address = int(scfg.get("base_address", 0) or 0)

        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.settimeout(1.0)
            sock.bind((host, port))
            sock.listen(8)
            self._sock = sock
            self._running = True
            self._last_error = ""
            t = threading.Thread(target=self._serve_forever, daemon=True, name="modbus-slave")
            self._thread = t
            t.start()
            log.info("Modbus slave started on %s:%d (base_addr=%d)", host, port, self._base_address)
            return True
        except OSError as e:
            self._last_error = str(e)
            log.warning("Modbus slave bind failed (%s:%d): %s", host, port, e)
            return False

    def stop(self) -> None:
        self._running = False
        try:
            if self._sock:
                self._sock.close()
        except Exception:
            pass
        self._sock = None
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=3.0)
        self._thread = None
        log.info("Modbus slave stopped")

    @property
    def is_running(self) -> bool:
        return self._running and (self._thread is not None) and self._thread.is_alive()

    @property
    def last_error(self) -> str:
        return self._last_error

    def update_registers(
        self,
        total_values: dict,
        total_quality: str,
        meters_by_id: Dict[int, tuple],  # {meter_id: (values_dict, quality_str)}
    ) -> None:
        """Push new data into the register bank (thread-safe)."""
        new_regs = [0] * (REGS_PER_METER * 7)

        # Block 0 = TOTAL
        block = _values_to_registers(total_values or {}, total_quality or "OFFLINE")
        new_regs[0:REGS_PER_METER] = block

        # Blocks 1-6 = individual meters
        for meter_id in range(1, 7):
            info = (meters_by_id or {}).get(meter_id)
            if info:
                vals, qual = info
            else:
                vals, qual = {}, "DISABLED"
            block = _values_to_registers(vals or {}, qual or "DISABLED")
            offset = meter_id * REGS_PER_METER
            new_regs[offset:offset + REGS_PER_METER] = block

        with self._lock:
            self._registers = new_regs

    # ── Internal server ────────────────────────────────────────────────────────

    def _serve_forever(self) -> None:
        while self._running:
            try:
                conn, addr = self._sock.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            t = threading.Thread(
                target=self._handle_client,
                args=(conn, addr),
                daemon=True,
                name=f"modbus-client-{addr[0]}",
            )
            t.start()

    def _handle_client(self, conn: socket.socket, addr) -> None:
        log.debug("Modbus slave: client connected %s", addr)
        conn.settimeout(30.0)
        buf = b""
        try:
            while self._running:
                try:
                    chunk = conn.recv(256)
                except socket.timeout:
                    break
                if not chunk:
                    break
                buf += chunk
                while len(buf) >= 8:
                    hdr = _parse_mbap(buf)
                    if hdr is None:
                        buf = b""
                        break
                    tid, _, length, unit = hdr
                    # total expected = 6 (MBAP without unit) + length
                    total = 6 + length
                    if len(buf) < total:
                        break   # need more data
                    pdu = buf[7:total]
                    buf = buf[total:]
                    resp = self._process_pdu(tid, unit, pdu)
                    if resp:
                        conn.sendall(resp)
        except Exception as e:
            log.debug("Modbus slave client %s error: %s", addr, e)
        finally:
            try:
                conn.close()
            except Exception:
                pass
        log.debug("Modbus slave: client disconnected %s", addr)

    def _process_pdu(self, tid: int, unit: int, pdu: bytes) -> Optional[bytes]:
        if not pdu:
            return _build_exception(tid, unit, 0, MODBUS_EXCEPTION_ILLEGAL_FUNCTION)

        fc = pdu[0]
        if fc not in (0x03, 0x04):  # FC03 = holding regs, FC04 = input regs
            return _build_exception(tid, unit, fc, MODBUS_EXCEPTION_ILLEGAL_FUNCTION)

        if len(pdu) < 5:
            return _build_exception(tid, unit, fc, MODBUS_EXCEPTION_ILLEGAL_DATA)

        start_addr = struct.unpack(">H", pdu[1:3])[0]
        count      = struct.unpack(">H", pdu[3:5])[0]

        if count < 1 or count > 125:
            return _build_exception(tid, unit, fc, MODBUS_EXCEPTION_ILLEGAL_DATA)

        # Translate absolute address to register index
        base = self._base_address
        reg_idx = start_addr - base
        total_regs = REGS_PER_METER * 7

        if reg_idx < 0 or (reg_idx + count) > total_regs:
            return _build_exception(tid, unit, fc, MODBUS_EXCEPTION_ILLEGAL_ADDRESS)

        with self._lock:
            regs = self._registers[reg_idx:reg_idx + count]

        data = struct.pack(f">{count}H", *regs)
        return _build_read_response(tid, unit, fc, data)

    # ── Config reload ─────────────────────────────────────────────────────────

    def reconfigure(self, cfg: Dict[str, Any]) -> None:
        """Restart with updated config if settings changed."""
        old_scfg = (self._cfg.get("modbus_slave") or {})
        new_scfg = (cfg.get("modbus_slave") or {})
        self._cfg = cfg

        changed = (
            old_scfg.get("host") != new_scfg.get("host")
            or old_scfg.get("port") != new_scfg.get("port")
            or old_scfg.get("enabled") != new_scfg.get("enabled")
        )
        if not changed:
            return
        self.stop()
        if bool(new_scfg.get("enabled", False)):
            self.start()
