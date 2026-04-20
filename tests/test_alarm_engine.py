"""
Unit tests — core/alarm_engine.py

Coverage:
  AlarmEngine._is_shelved()      — exact, meter-wildcard, code-wildcard, global,
                                   expired auto-cleanup, indefinite (None expiry)
  AlarmEngine.shelve()           — sets expiry, clears active alarm
  AlarmEngine.unshelve()         — removes shelf, returns bool
  AlarmEngine.unshelve_expired() — batch cleanup, returns count
  AlarmEngine.shelved_items()    — list contents and remaining_sec
  AlarmEngine._raise() / _clear()  — happy-path via minimal stubs
  AlarmEngine.evaluate_meter()     — shelved code must NOT raise

Uses a minimal stub AlarmEngine (no DB, no journal, no MQTT) so the tests
run without any infrastructure setup.
"""
import sys
import os
import time
import threading
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# ---------------------------------------------------------------------------
# Minimal AlarmEngine stub (bypasses SQLite and journal)
# ---------------------------------------------------------------------------
from core.alarm_engine import AlarmEngine, AlarmEvent
from core.snapshot_bus import MeterQuality


def _make_engine() -> AlarmEngine:
    """Create an AlarmEngine with no-op DB / journal / MQTT."""
    eng = AlarmEngine.__new__(AlarmEngine)
    # Replicate __init__ minimally
    eng._lock        = threading.RLock()
    eng._shelf_lock  = threading.Lock()
    eng._shelves     = {}
    eng.active       = {}
    eng.history      = []
    eng.event_journal = None
    eng._mqtt_publisher = None
    eng._sms_service = None          # Phase 15: SMS service (None = disabled)
    eng._state_db    = None
    eng._cfg         = {}
    eng.start_ts     = time.time() - 60.0   # past startup inhibit
    eng.startup_inhibit_sec = 5.0
    eng._last_beep_ts = 0.0
    eng._BEEP_COOLDOWN_SEC = 30.0
    eng._first_out_ts = 0.0
    eng._BURST_WINDOW_SEC = 5.0
    return eng


def _make_meter(meter_id="M1", name="Test Meter", quality=MeterQuality.GOOD,
                values=None, enabled=True):
    """Minimal meter stub."""
    class _Meter:
        pass
    m = _Meter()
    m.meter_id = meter_id
    m.name = name
    m.quality = quality
    m.values = values or {"kW": 100.0, "Vavg": 230.0, "PFavg": 0.92, "Frequency": 50.0}
    m.enabled = enabled
    m.first_data_received = True
    def age():
        return 1.0
    m.age = age
    return m


# ---------------------------------------------------------------------------
# Shelving logic
# ---------------------------------------------------------------------------

class TestIsShelved(unittest.TestCase):

    def setUp(self):
        self.eng = _make_engine()

    def test_not_shelved_by_default(self):
        self.assertFalse(self.eng._is_shelved("M1", "OV"))

    def test_exact_shelf_active(self):
        self.eng._shelves[("M1", "OV")] = time.time() + 100
        self.assertTrue(self.eng._is_shelved("M1", "OV"))

    def test_exact_shelf_does_not_bleed_to_other_code(self):
        self.eng._shelves[("M1", "OV")] = time.time() + 100
        self.assertFalse(self.eng._is_shelved("M1", "UV"))

    def test_exact_shelf_does_not_bleed_to_other_meter(self):
        self.eng._shelves[("M1", "OV")] = time.time() + 100
        self.assertFalse(self.eng._is_shelved("M2", "OV"))

    def test_meter_wildcard(self):
        """Shelf on (meter_id, '*') suppresses ALL codes for that meter."""
        self.eng._shelves[("M1", "*")] = time.time() + 100
        self.assertTrue(self.eng._is_shelved("M1", "UV"))
        self.assertTrue(self.eng._is_shelved("M1", "OV"))
        self.assertFalse(self.eng._is_shelved("M2", "OV"))

    def test_code_wildcard(self):
        """Shelf on ('*', code) suppresses that code across ALL meters."""
        self.eng._shelves[("*", "OV")] = time.time() + 100
        self.assertTrue(self.eng._is_shelved("M1", "OV"))
        self.assertTrue(self.eng._is_shelved("M2", "OV"))
        self.assertFalse(self.eng._is_shelved("M1", "UV"))

    def test_global_wildcard(self):
        """Shelf on ('*', '*') — system-wide maintenance mode."""
        self.eng._shelves[("*", "*")] = time.time() + 100
        self.assertTrue(self.eng._is_shelved("M1", "OV"))
        self.assertTrue(self.eng._is_shelved("M99", "ANYTHING"))

    def test_indefinite_shelf(self):
        """Shelf with None expiry (indefinite) must be active."""
        self.eng._shelves[("M1", "OV")] = None
        self.assertTrue(self.eng._is_shelved("M1", "OV"))

    def test_expired_shelf_returns_false(self):
        self.eng._shelves[("M1", "OV")] = time.time() - 1  # already expired
        self.assertFalse(self.eng._is_shelved("M1", "OV"))

    def test_expired_shelf_auto_cleaned(self):
        """Expired shelf must be removed from _shelves dict on check."""
        self.eng._shelves[("M1", "OV")] = time.time() - 1
        self.eng._is_shelved("M1", "OV")
        self.assertNotIn(("M1", "OV"), self.eng._shelves)

    def test_specificity_order(self):
        """Most-specific key takes precedence, but expired specific doesn't
        block the less-specific wildcard match."""
        # Expired exact shelf
        self.eng._shelves[("M1", "OV")] = time.time() - 1
        # Active meter wildcard
        self.eng._shelves[("M1", "*")] = time.time() + 100
        # Should still be shelved via meter wildcard
        self.assertTrue(self.eng._is_shelved("M1", "OV"))


class TestShelveUnshelve(unittest.TestCase):

    def setUp(self):
        self.eng = _make_engine()

    def test_shelve_creates_entry(self):
        self.eng.shelve("M1", "OV", 3600.0)
        self.assertIn(("M1", "OV"), self.eng._shelves)
        expiry = self.eng._shelves[("M1", "OV")]
        self.assertIsNotNone(expiry)
        self.assertGreater(expiry, time.time())

    def test_shelve_indefinite(self):
        self.eng.shelve("M1", "OV", None)
        self.assertIsNone(self.eng._shelves[("M1", "OV")])

    def test_shelve_clears_active_alarm(self):
        """Shelving a code that is currently active must clear it."""
        # Manually inject active alarm
        ev = AlarmEvent("M1", "OV", "Overvoltage", "ALARM")
        self.eng.active[("M1", "OV")] = ev
        self.eng.shelve("M1", "OV", 3600.0)
        self.assertNotIn(("M1", "OV"), self.eng.active)

    def test_unshelve_returns_true(self):
        self.eng._shelves[("M1", "OV")] = time.time() + 100
        result = self.eng.unshelve("M1", "OV")
        self.assertTrue(result)

    def test_unshelve_removes_shelf(self):
        self.eng._shelves[("M1", "OV")] = time.time() + 100
        self.eng.unshelve("M1", "OV")
        self.assertNotIn(("M1", "OV"), self.eng._shelves)

    def test_unshelve_nonexistent_returns_false(self):
        result = self.eng.unshelve("M1", "NONEXISTENT")
        self.assertFalse(result)


class TestUnshelveExpired(unittest.TestCase):

    def setUp(self):
        self.eng = _make_engine()

    def test_removes_expired_returns_count(self):
        self.eng._shelves[("M1", "OV")] = time.time() - 5   # expired
        self.eng._shelves[("M2", "UV")] = time.time() - 5   # expired
        self.eng._shelves[("M3", "OV")] = time.time() + 100 # still active
        count = self.eng.unshelve_expired()
        self.assertEqual(count, 2)
        self.assertNotIn(("M1", "OV"), self.eng._shelves)
        self.assertNotIn(("M2", "UV"), self.eng._shelves)
        self.assertIn(("M3", "OV"), self.eng._shelves)

    def test_indefinite_not_removed(self):
        self.eng._shelves[("M1", "OV")] = None  # indefinite
        count = self.eng.unshelve_expired()
        self.assertEqual(count, 0)
        self.assertIn(("M1", "OV"), self.eng._shelves)

    def test_empty_returns_zero(self):
        self.assertEqual(self.eng.unshelve_expired(), 0)


class TestShelvedItems(unittest.TestCase):

    def setUp(self):
        self.eng = _make_engine()

    def test_empty(self):
        self.assertEqual(self.eng.shelved_items(), [])

    def test_active_shelf_present(self):
        self.eng._shelves[("M1", "OV")] = time.time() + 3600
        items = self.eng.shelved_items()
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["meter_id"], "M1")
        self.assertEqual(items[0]["code"], "OV")
        self.assertGreater(items[0]["remaining_sec"], 0)

    def test_indefinite_shelf_remaining_none(self):
        self.eng._shelves[("M1", "OV")] = None
        items = self.eng.shelved_items()
        self.assertEqual(len(items), 1)
        self.assertIsNone(items[0]["remaining_sec"])

    def test_expired_not_returned(self):
        self.eng._shelves[("M1", "OV")] = time.time() - 1  # expired
        items = self.eng.shelved_items()
        self.assertEqual(items, [])


# ---------------------------------------------------------------------------
# _raise / _clear integration
# ---------------------------------------------------------------------------

class TestRaiseClear(unittest.TestCase):

    def setUp(self):
        self.eng = _make_engine()

    def test_raise_adds_to_active(self):
        self.eng._raise("M1", "OV", "Overvoltage", "ALARM")
        self.assertIn(("M1", "OV"), self.eng.active)

    def test_raise_adds_to_history(self):
        self.eng._raise("M1", "OV", "Overvoltage", "ALARM")
        self.assertEqual(len(self.eng.history), 1)

    def test_raise_idempotent(self):
        """Re-raising the same alarm must not add duplicate."""
        self.eng._raise("M1", "OV", "Overvoltage", "ALARM")
        self.eng._raise("M1", "OV", "Overvoltage", "ALARM")
        self.assertEqual(len(self.eng.history), 1)

    def test_clear_removes_from_active(self):
        self.eng._raise("M1", "OV", "Overvoltage", "ALARM")
        self.eng._clear("M1", "OV")
        self.assertNotIn(("M1", "OV"), self.eng.active)

    def test_clear_nonexistent_no_crash(self):
        self.eng._clear("M1", "NONEXISTENT")  # must not raise

    def test_shelved_alarm_not_raised(self):
        """_raise() must be a no-op when the alarm is shelved."""
        self.eng._shelves[("M1", "OV")] = time.time() + 3600
        self.eng._raise("M1", "OV", "Overvoltage", "ALARM")
        self.assertNotIn(("M1", "OV"), self.eng.active)

    def test_first_out_flag_on_first_alarm(self):
        self.eng._raise("M1", "OV", "Overvoltage", "ALARM")
        ev = self.eng.active.get(("M1", "OV"))
        self.assertIsNotNone(ev)
        self.assertTrue(ev.is_first_out)

    def test_second_alarm_within_burst_window_also_flagged(self):
        self.eng._raise("M1", "OV", "Overvoltage", "ALARM")
        self.eng._raise("M1", "UV", "Undervoltage", "ALARM")
        ev2 = self.eng.active.get(("M1", "UV"))
        self.assertIsNotNone(ev2)
        self.assertTrue(ev2.is_first_out)


# ---------------------------------------------------------------------------
# evaluate_meter — shelved code must not appear
# ---------------------------------------------------------------------------

class TestEvaluateMeterShelved(unittest.TestCase):

    def setUp(self):
        self.eng = _make_engine()
        self.eng._cfg = {
            "analytics": {
                "voltage_ln_min_v": 210.0,
                "voltage_ln_max_v": 250.0,
                "freq_min_hz": 49.0,
                "freq_max_hz": 51.0,
                "pf_warn": 0.85,
            }
        }

    def test_ov_alarm_raised_when_not_shelved(self):
        meter = _make_meter(values={"kW": 100.0, "Vavg": 260.0, "PFavg": 0.92,
                                    "Frequency": 50.0})
        self.eng.evaluate_meter(meter)
        self.assertIn(("M1", "OV"), self.eng.active)

    def test_ov_alarm_suppressed_when_shelved(self):
        self.eng._shelves[("M1", "OV")] = time.time() + 3600
        meter = _make_meter(values={"kW": 100.0, "Vavg": 260.0, "PFavg": 0.92,
                                    "Frequency": 50.0})
        self.eng.evaluate_meter(meter)
        self.assertNotIn(("M1", "OV"), self.eng.active)

    def test_uv_alarm_raised_when_low_voltage(self):
        meter = _make_meter(values={"kW": 100.0, "Vavg": 200.0, "PFavg": 0.92,
                                    "Frequency": 50.0})
        self.eng.evaluate_meter(meter)
        self.assertIn(("M1", "UV"), self.eng.active)

    def test_freq_high_alarm(self):
        meter = _make_meter(values={"kW": 100.0, "Vavg": 230.0, "PFavg": 0.92,
                                    "Frequency": 52.0})
        self.eng.evaluate_meter(meter)
        self.assertIn(("M1", "FREQ_HIGH"), self.eng.active)

    def test_pf_low_alarm(self):
        meter = _make_meter(values={"kW": 100.0, "Vavg": 230.0, "PFavg": 0.75,
                                    "Frequency": 50.0})
        self.eng.evaluate_meter(meter)
        self.assertIn(("M1", "PF_LOW"), self.eng.active)

    def test_global_shelf_suppresses_all_alarms(self):
        """System-wide maintenance mode must suppress every alarm."""
        self.eng._shelves[("*", "*")] = time.time() + 3600
        meter = _make_meter(values={"kW": 100.0, "Vavg": 270.0, "PFavg": 0.60,
                                    "Frequency": 53.0})
        self.eng.evaluate_meter(meter)
        self.assertEqual(len(self.eng.active), 0)

    def test_comm_loss_quality_triggers_conn_loss(self):
        meter = _make_meter(quality=MeterQuality.COMM_LOST)
        self.eng.evaluate_meter(meter)
        self.assertIn(("M1", "CONN_LOSS"), self.eng.active)

    def test_stale_quality_triggers_stale(self):
        meter = _make_meter(quality=MeterQuality.STALE)
        self.eng.evaluate_meter(meter)
        self.assertIn(("M1", "STALE"), self.eng.active)

    def test_startup_inhibit_blocks_evaluation(self):
        """During startup inhibit window, evaluate_meter must be a no-op."""
        self.eng.start_ts = time.time()  # just started
        self.eng.startup_inhibit_sec = 30.0
        meter = _make_meter(values={"kW": 100.0, "Vavg": 270.0})
        self.eng.evaluate_meter(meter)
        self.assertEqual(len(self.eng.active), 0)


if __name__ == "__main__":
    unittest.main()
