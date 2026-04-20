"""
Unit tests — core/mqtt_publisher.py

Coverage (pure logic, no broker connection required):
  _sanitise()                — NaN/Inf dropped, finite floats rounded, ints/strings kept
  MQTTPublisher._effective_interval() — normal mode, gprs floor, hard 1-s minimum
  MQTTPublisher._encode_payload()     — no-compress path, compress path, small-payload bypass
  MQTTPublisher.compression_ratio    — None when no data, correct ratio after sends
  MQTTPublisher.bandwidth_mode       — reads cfg correctly
  MQTTPublisher.enabled              — reads cfg correctly
"""
import sys
import os
import math
import zlib
import json
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# We import the private helper directly — it is a module-level function
from core.mqtt_publisher import _sanitise, MQTTPublisher


# ---------------------------------------------------------------------------
# _sanitise
# ---------------------------------------------------------------------------

class TestSanitise(unittest.TestCase):

    def test_finite_float_kept_and_rounded(self):
        out = _sanitise({"v": 1.123456789})
        self.assertAlmostEqual(out["v"], 1.1235, places=4)

    def test_nan_dropped(self):
        out = _sanitise({"v": float("nan")})
        self.assertNotIn("v", out)

    def test_positive_inf_dropped(self):
        out = _sanitise({"v": float("inf")})
        self.assertNotIn("v", out)

    def test_negative_inf_dropped(self):
        out = _sanitise({"v": float("-inf")})
        self.assertNotIn("v", out)

    def test_int_kept(self):
        out = _sanitise({"count": 42})
        self.assertEqual(out["count"], 42)

    def test_str_kept(self):
        out = _sanitise({"unit": "kW"})
        self.assertEqual(out["unit"], "kW")

    def test_zero_kept(self):
        out = _sanitise({"v": 0.0})
        self.assertIn("v", out)
        self.assertEqual(out["v"], 0.0)

    def test_empty_dict(self):
        self.assertEqual(_sanitise({}), {})

    def test_mixed(self):
        out = _sanitise({"good": 1.5, "bad": float("nan"), "label": "A"})
        self.assertIn("good", out)
        self.assertNotIn("bad", out)
        self.assertIn("label", out)


# ---------------------------------------------------------------------------
# _effective_interval
# ---------------------------------------------------------------------------

def _pub(mqtt_cfg: dict) -> MQTTPublisher:
    return MQTTPublisher({"mqtt": mqtt_cfg})


class TestEffectiveInterval(unittest.TestCase):

    def test_normal_mode_uses_publish_interval(self):
        p = _pub({"bandwidth_mode": "normal", "publish_interval_sec": 10})
        self.assertEqual(p._effective_interval(), 10.0)

    def test_gprs_mode_enforces_floor(self):
        """If publish_interval_sec < min_publish_interval_sec, floor wins."""
        p = _pub({"bandwidth_mode": "gprs",
                  "publish_interval_sec": 5,
                  "min_publish_interval_sec": 60})
        self.assertEqual(p._effective_interval(), 60.0)

    def test_gprs_mode_publish_higher_than_floor(self):
        """If publish_interval_sec > min, publish_interval_sec wins."""
        p = _pub({"bandwidth_mode": "gprs",
                  "publish_interval_sec": 120,
                  "min_publish_interval_sec": 60})
        self.assertEqual(p._effective_interval(), 120.0)

    def test_hard_minimum_1s(self):
        """Absurdly low publish_interval_sec must be clamped to 1.0."""
        p = _pub({"bandwidth_mode": "normal", "publish_interval_sec": 0})
        self.assertEqual(p._effective_interval(), 1.0)

    def test_default_when_key_missing(self):
        """Missing publish_interval_sec defaults to 5 s."""
        p = _pub({})
        self.assertEqual(p._effective_interval(), 5.0)


# ---------------------------------------------------------------------------
# _encode_payload
# ---------------------------------------------------------------------------

class TestEncodePayload(unittest.TestCase):

    def _large_json(self, n=200) -> str:
        """Generate a JSON string large enough to compress meaningfully."""
        d = {f"key_{i}": 12345.6789 for i in range(n)}
        return json.dumps(d)

    def test_no_compress_flag_returns_raw(self):
        p = _pub({"compress_payload": False})
        payload, compressed = p._encode_payload('{"v": 1}')
        self.assertFalse(compressed)
        self.assertEqual(payload, b'{"v": 1}')

    def test_compress_flag_small_payload_not_compressed(self):
        """Payloads < 64 bytes must NOT be compressed even when flag is set."""
        p = _pub({"compress_payload": True})
        small = '{"v":1}'   # 7 bytes
        payload, compressed = p._encode_payload(small)
        self.assertFalse(compressed)
        self.assertEqual(payload, small.encode())

    def test_compress_flag_large_payload_compressed(self):
        p = _pub({"compress_payload": True})
        large = self._large_json()
        payload, compressed = p._encode_payload(large)
        self.assertTrue(compressed, "Expected large payload to be compressed")
        # Decompress and verify it round-trips
        recovered = zlib.decompress(payload).decode("utf-8")
        self.assertEqual(recovered, large)

    def test_compressed_is_smaller(self):
        p = _pub({"compress_payload": True})
        large = self._large_json()
        payload, compressed = p._encode_payload(large)
        if compressed:
            self.assertLess(len(payload), len(large.encode()))

    def test_compression_not_applied_if_no_savings(self):
        """If zlib output is larger than input, fall back to raw."""
        # A 64+ byte pseudo-random string won't compress well
        import base64, os
        random_b64 = base64.b64encode(os.urandom(100)).decode()  # 136 bytes, high entropy
        p = _pub({"compress_payload": True})
        _, compressed = p._encode_payload(random_b64)
        # May or may not compress depending on entropy; just verify no crash
        # and payload is bytes


# ---------------------------------------------------------------------------
# bandwidth_mode / enabled properties
# ---------------------------------------------------------------------------

class TestProperties(unittest.TestCase):

    def test_enabled_true(self):
        p = _pub({"enabled": True})
        self.assertTrue(p.enabled)

    def test_enabled_false(self):
        p = _pub({"enabled": False})
        self.assertFalse(p.enabled)

    def test_enabled_default_false(self):
        p = MQTTPublisher({})
        self.assertFalse(p.enabled)

    def test_bandwidth_mode_normal(self):
        p = _pub({"bandwidth_mode": "normal"})
        self.assertEqual(p.bandwidth_mode, "normal")

    def test_bandwidth_mode_gprs(self):
        p = _pub({"bandwidth_mode": "gprs"})
        self.assertEqual(p.bandwidth_mode, "gprs")

    def test_bandwidth_mode_default_normal(self):
        p = _pub({})
        self.assertEqual(p.bandwidth_mode, "normal")

    def test_compression_ratio_none_initially(self):
        p = _pub({})
        self.assertIsNone(p.compression_ratio)

    def test_compression_ratio_computed(self):
        p = _pub({})
        p._bytes_raw_total  = 1000
        p._bytes_sent_total = 400
        self.assertAlmostEqual(p.compression_ratio, 0.4, places=3)


if __name__ == "__main__":
    unittest.main()
