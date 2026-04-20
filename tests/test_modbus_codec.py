"""
Unit tests — core/modbus_codec.py

Coverage:
  regs_to_float32     — all four word orders, NaN/Inf passthrough
  regs_to_u32         — boundary values
  u32_to_regs         — round-trip with regs_to_u32
  normalize_values    — canonical key mapping
  validate_setup_value — enum, scaled, integer, CT Primary cross-dep
"""
import math
import struct
import sys
import os
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from core.modbus_codec import (
    regs_to_float32,
    regs_to_u32,
    u32_to_regs,
    normalize_values,
    validate_setup_value,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _float_to_regs(value: float, word_order: str = "CDAB"):
    """Encode a Python float back to two 16-bit register ints (inverse of regs_to_float32)."""
    raw = struct.pack(">f", value)   # 4 bytes big-endian (ABCD)
    a, b, c, d = raw
    src = {"A": a, "B": b, "C": c, "D": d}
    # word_order describes how bytes are stored: position 0=reg0_hi, 1=reg0_lo, 2=reg1_hi, 3=reg1_lo
    order = word_order.upper()
    b0, b1, b2, b3 = src[order[0]], src[order[1]], src[order[2]], src[order[3]]
    reg0 = (b0 << 8) | b1
    reg1 = (b2 << 8) | b3
    return reg0, reg1


class TestRegsToFloat32(unittest.TestCase):
    """regs_to_float32 — verify all four word orders and edge cases."""

    def _roundtrip(self, value: float, word_order: str):
        reg0, reg1 = _float_to_regs(value, word_order)
        got = regs_to_float32(reg0, reg1, word_order)
        if math.isnan(value):
            self.assertTrue(math.isnan(got), f"Expected NaN for word_order={word_order}")
        elif math.isinf(value):
            self.assertTrue(math.isinf(got))
            self.assertEqual(math.copysign(1.0, got), math.copysign(1.0, value))
        else:
            self.assertAlmostEqual(got, value, places=4,
                                   msg=f"word_order={word_order}, value={value}")

    def test_cdab_known_voltage(self):
        """230 V — typical phase-to-neutral voltage, Selec default word order."""
        self._roundtrip(230.0, "CDAB")

    def test_abcd(self):
        self._roundtrip(50.0, "ABCD")   # 50 Hz frequency

    def test_badc(self):
        self._roundtrip(0.95, "BADC")   # PF near 1

    def test_dcba(self):
        self._roundtrip(100.5, "DCBA")

    def test_negative_value(self):
        """Negative kW is valid (export)."""
        self._roundtrip(-12.5, "CDAB")

    def test_zero(self):
        self._roundtrip(0.0, "CDAB")

    def test_nan_passthrough(self):
        """NaN in register data must survive decode as NaN."""
        # IEEE 754 quiet NaN — any exponent all-ones mantissa nonzero
        reg0, reg1 = _float_to_regs(float("nan"), "CDAB")
        result = regs_to_float32(reg0, reg1, "CDAB")
        self.assertTrue(math.isnan(result))

    def test_positive_inf(self):
        self._roundtrip(float("inf"), "CDAB")

    def test_invalid_word_order_defaults_to_cdab(self):
        """Unknown word order should fall back to CDAB silently."""
        reg0, reg1 = _float_to_regs(240.0, "CDAB")
        got = regs_to_float32(reg0, reg1, "INVALID")
        self.assertAlmostEqual(got, 240.0, places=4)

    def test_large_kw(self):
        """Large industrial load — 1500 kW."""
        self._roundtrip(1500.0, "CDAB")

    def test_small_pf(self):
        """Low PF value with precision."""
        self._roundtrip(0.707, "CDAB")


class TestRegsU32(unittest.TestCase):
    """regs_to_u32 and u32_to_regs — 32-bit integer codec."""

    def test_zero(self):
        self.assertEqual(regs_to_u32(0, 0), 0)

    def test_max_u16_lo(self):
        self.assertEqual(regs_to_u32(0, 0xFFFF), 0xFFFF)

    def test_max_u16_hi(self):
        self.assertEqual(regs_to_u32(0xFFFF, 0), 0xFFFF0000)

    def test_max_u32(self):
        self.assertEqual(regs_to_u32(0xFFFF, 0xFFFF), 0xFFFFFFFF)

    def test_serial_example(self):
        """Simulate a meter serial: hi=0x0001, lo=0x86A0 → 100000"""
        self.assertEqual(regs_to_u32(0x0001, 0x86A0), 100000)

    def test_u32_to_regs_zero(self):
        self.assertEqual(u32_to_regs(0), (0, 0))

    def test_u32_to_regs_max(self):
        hi, lo = u32_to_regs(0xFFFFFFFF)
        self.assertEqual(hi, 0xFFFF)
        self.assertEqual(lo, 0xFFFF)

    def test_roundtrip(self):
        for value in (0, 1, 65535, 65536, 100000, 0xDEADBEEF & 0xFFFFFFFF):
            hi, lo = u32_to_regs(value)
            self.assertEqual(regs_to_u32(hi, lo), value, msg=f"roundtrip failed for {value}")


class TestNormalizeValues(unittest.TestCase):
    """normalize_values — canonical key mapping from register names."""

    def test_voltage_mapped(self):
        raw = {"Average Voltage LN": 231.5}
        out = normalize_values(raw)
        self.assertAlmostEqual(out["Vavg"], 231.5, places=3)
        # Original key preserved
        self.assertAlmostEqual(out["Average Voltage LN"], 231.5, places=3)

    def test_power_mapped(self):
        raw = {"Total kW": 42.0, "Total kVA": 50.0}
        out = normalize_values(raw)
        self.assertEqual(out["kW"], 42.0)
        self.assertEqual(out["kVA"], 50.0)

    def test_current_mapped(self):
        raw = {"Average Current": 15.0}
        out = normalize_values(raw)
        self.assertEqual(out["Iavg"], 15.0)

    def test_pf_mapped(self):
        raw = {"Average PF": 0.92}
        out = normalize_values(raw)
        self.assertAlmostEqual(out["PFavg"], 0.92, places=4)

    def test_energy_import_export(self):
        raw = {"Total kWh Import": 1234.5, "Total kWh Export": 0.0}
        out = normalize_values(raw)
        self.assertEqual(out["Import_kWh"], 1234.5)
        self.assertEqual(out["Export_kWh"], 0.0)

    def test_phase_voltages(self):
        raw = {"Voltage V1N": 228.0, "Voltage V2N": 231.0, "Voltage V3N": 230.0}
        out = normalize_values(raw)
        self.assertEqual(out["V1N"], 228.0)
        self.assertEqual(out["V2N"], 231.0)
        self.assertEqual(out["V3N"], 230.0)

    def test_extra_keys_preserved(self):
        """Keys not in the mapping must pass through unchanged."""
        raw = {"SomeUnknownKey": 99.0}
        out = normalize_values(raw)
        self.assertEqual(out["SomeUnknownKey"], 99.0)

    def test_empty_dict(self):
        out = normalize_values({})
        self.assertEqual(out, {})


class TestValidateSetupValue(unittest.TestCase):
    """validate_setup_value — covers CT Primary, enum, scaled, range validation."""

    def test_unknown_parameter(self):
        valid, msg, _ = validate_setup_value("Not A Real Param", 5)
        self.assertFalse(valid)
        self.assertIn("Unknown", msg)

    def test_ct_primary_valid(self):
        # CT Secondary code 5 → min 5
        setup = {"CT Secondary": {"code": 5, "label": "5A"}}
        valid, msg, val = validate_setup_value("CT Primary", 200, setup)
        self.assertTrue(valid, msg)
        self.assertEqual(val, 200)

    def test_ct_primary_too_low(self):
        setup = {"CT Secondary": {"code": 5, "label": "5A"}}
        valid, msg, _ = validate_setup_value("CT Primary", 4, setup)
        self.assertFalse(valid)

    def test_ct_primary_ct1_accepts_1(self):
        """CT Secondary = 1A allows CT Primary down to 1."""
        setup = {"CT Secondary": {"code": 1, "label": "1A"}}
        valid, msg, val = validate_setup_value("CT Primary", 1, setup)
        self.assertTrue(valid, msg)
        self.assertEqual(val, 1)


if __name__ == "__main__":
    unittest.main()
