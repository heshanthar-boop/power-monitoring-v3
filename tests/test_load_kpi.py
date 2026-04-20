"""
Unit tests — core/load_kpi.py

Coverage:
  LoadKPIEngine.update()      — MD tracking, load factor, PF status, kvar, billing
  LoadKPIEngine._is_peak()    — TOU window: normal, overnight-wrap, edge, disabled
  LoadKPIEngine._parse_hhmm() — valid and malformed strings
  LoadKPIEngine.reset_daily() — accumulator reset
  LoadKPIEngine.reconfigure() — config swap
  TOU billing split           — peak/off-peak kWh apportionment, charge calculation
"""
import sys
import os
import time
import unittest
from datetime import datetime, date

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from core.load_kpi import LoadKPIEngine, LoadPlantConfig


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ts_at(hour: int, minute: int = 0, day_offset: int = 0) -> float:
    """Return epoch for today (+ day_offset days) at HH:MM local time."""
    today = date.today()
    from datetime import timedelta
    d = today + timedelta(days=day_offset)
    return datetime(d.year, d.month, d.day, hour, minute).timestamp()


def _cfg(**kwargs) -> LoadPlantConfig:
    defaults = dict(
        contract_demand_kva=500.0,
        demand_interval_min=15,
        pf_penalty_threshold=0.85,
        pf_incentive_threshold=0.95,
        tariff_demand_rate=0.0,
        tariff_energy_rate=0.0,
        power_key="kW",
        apparent_power_key="kVA",
        pf_key="PFavg",
        energy_key="Today_kWh",
    )
    defaults.update(kwargs)
    return LoadPlantConfig(**defaults)


def _vals(kw=100.0, kva=110.0, pf=0.91, kwh=50.0, **extra):
    d = {"kW": kw, "kVA": kva, "PFavg": pf, "Today_kWh": kwh}
    d.update(extra)
    return d


# ---------------------------------------------------------------------------
# _parse_hhmm
# ---------------------------------------------------------------------------

class TestParseHHMM(unittest.TestCase):
    def test_normal(self):
        self.assertEqual(LoadKPIEngine._parse_hhmm("18:30"), (18, 30))

    def test_single_digit_hour(self):
        self.assertEqual(LoadKPIEngine._parse_hhmm("6:05"), (6, 5))

    def test_midnight(self):
        self.assertEqual(LoadKPIEngine._parse_hhmm("00:00"), (0, 0))

    def test_malformed_empty(self):
        self.assertEqual(LoadKPIEngine._parse_hhmm(""), (0, 0))

    def test_malformed_no_colon(self):
        self.assertEqual(LoadKPIEngine._parse_hhmm("1830"), (0, 0))

    def test_malformed_letters(self):
        self.assertEqual(LoadKPIEngine._parse_hhmm("AB:CD"), (0, 0))

    def test_wrap_24h(self):
        # 25 % 24 = 1, 65 % 60 = 5
        self.assertEqual(LoadKPIEngine._parse_hhmm("25:65"), (1, 5))


# ---------------------------------------------------------------------------
# _is_peak  (TOU window detection)
# ---------------------------------------------------------------------------

class TestIsPeak(unittest.TestCase):
    """Test all TOU window boundary cases without relying on wall time."""

    def _cfg_tou(self, start="18:30", end="22:30", enabled=True):
        return LoadPlantConfig(
            tou_enabled=enabled,
            tou_peak_start=start,
            tou_peak_end=end,
        )

    def test_disabled_always_false(self):
        cfg = self._cfg_tou(enabled=False)
        self.assertFalse(LoadKPIEngine._is_peak(cfg, _ts_at(20, 0)))

    def test_normal_window_inside(self):
        cfg = self._cfg_tou("18:30", "22:30")
        self.assertTrue(LoadKPIEngine._is_peak(cfg, _ts_at(19, 0)))
        self.assertTrue(LoadKPIEngine._is_peak(cfg, _ts_at(22, 29)))

    def test_normal_window_at_start(self):
        cfg = self._cfg_tou("18:30", "22:30")
        self.assertTrue(LoadKPIEngine._is_peak(cfg, _ts_at(18, 30)))

    def test_normal_window_at_end_is_outside(self):
        """End boundary is exclusive."""
        cfg = self._cfg_tou("18:30", "22:30")
        self.assertFalse(LoadKPIEngine._is_peak(cfg, _ts_at(22, 30)))

    def test_normal_window_outside_before(self):
        cfg = self._cfg_tou("18:30", "22:30")
        self.assertFalse(LoadKPIEngine._is_peak(cfg, _ts_at(8, 0)))

    def test_normal_window_outside_after(self):
        cfg = self._cfg_tou("18:30", "22:30")
        self.assertFalse(LoadKPIEngine._is_peak(cfg, _ts_at(23, 0)))

    def test_overnight_wrap_inside_after_midnight(self):
        """22:00-06:00 window — 02:00 should be peak."""
        cfg = self._cfg_tou("22:00", "06:00")
        self.assertTrue(LoadKPIEngine._is_peak(cfg, _ts_at(2, 0)))

    def test_overnight_wrap_inside_before_midnight(self):
        """22:00-06:00 window — 23:30 should be peak."""
        cfg = self._cfg_tou("22:00", "06:00")
        self.assertTrue(LoadKPIEngine._is_peak(cfg, _ts_at(23, 30)))

    def test_overnight_wrap_outside(self):
        """22:00-06:00 window — 12:00 should NOT be peak."""
        cfg = self._cfg_tou("22:00", "06:00")
        self.assertFalse(LoadKPIEngine._is_peak(cfg, _ts_at(12, 0)))

    def test_overnight_wrap_at_end_is_outside(self):
        cfg = self._cfg_tou("22:00", "06:00")
        self.assertFalse(LoadKPIEngine._is_peak(cfg, _ts_at(6, 0)))


# ---------------------------------------------------------------------------
# LoadKPIEngine.update()
# ---------------------------------------------------------------------------

class TestLoadKPIUpdate(unittest.TestCase):
    """Core update() logic — demand, load factor, PF, kvar."""

    def setUp(self):
        self.cfg = _cfg(contract_demand_kva=500.0, tariff_energy_rate=0.0)
        self.eng = LoadKPIEngine(self.cfg)

    def test_result_has_all_expected_keys(self):
        ts = time.time()
        result = self.eng.update(_vals(), ts=ts)
        for k in ("demand_kva", "demand_kw", "md_kva_today", "md_kw_today",
                  "load_factor_pct", "pf_now", "pf_status", "pf_avg_today",
                  "demand_charge_est", "energy_charge_est", "warnings",
                  "tou_enabled", "tou_in_peak"):
            self.assertIn(k, result, f"Missing key: {k}")

    def test_md_tracks_peak(self):
        base_ts = time.time()
        self.eng.update(_vals(kva=100.0), ts=base_ts)
        self.eng.update(_vals(kva=200.0), ts=base_ts + 10)
        self.eng.update(_vals(kva=150.0), ts=base_ts + 20)
        result = self.eng.update(_vals(kva=180.0), ts=base_ts + 30)
        # MD should be 200 — the rolling average of [100,200,150,180] ≈ 157.5
        # but the MD peak recorded is the highest interval average seen
        self.assertGreater(result["md_kva_today"], 0.0)
        self.assertLessEqual(result["md_kva_today"], 200.0)

    def test_md_never_decreases(self):
        ts = time.time()
        self.eng.update(_vals(kva=300.0), ts=ts)
        r1 = self.eng.update(_vals(kva=50.0), ts=ts + 10)
        # MD should stay at 300 even when current demand drops
        self.assertGreaterEqual(r1["md_kva_today"], 300.0 * 0.99)

    def test_pf_penalty_status(self):
        result = self.eng.update(_vals(pf=0.80), ts=time.time())
        self.assertEqual(result["pf_status"], "PENALTY")

    def test_pf_normal_status(self):
        result = self.eng.update(_vals(pf=0.90), ts=time.time())
        self.assertEqual(result["pf_status"], "NORMAL")

    def test_pf_incentive_status(self):
        result = self.eng.update(_vals(pf=0.97), ts=time.time())
        self.assertEqual(result["pf_status"], "INCENTIVE")

    def test_pf_clamped_above_1(self):
        """PF register reporting 1.05 should be clamped to 1.0 with a warning."""
        result = self.eng.update(_vals(pf=1.05), ts=time.time())
        self.assertEqual(result["pf_now"], 1.0)
        self.assertTrue(any("clamped" in w.lower() for w in result["warnings"]))

    def test_kvar_computed(self):
        # kvar = sqrt(kva^2 - kw^2) = sqrt(100^2 - 80^2) = 60
        result = self.eng.update(_vals(kw=80.0, kva=100.0, pf=0.8), ts=time.time())
        self.assertAlmostEqual(result["demand_kvar"], 60.0, places=0)

    def test_kvar_none_when_no_kva(self):
        v = {"kW": 100.0, "PFavg": 0.9, "Today_kWh": 50.0}  # no kVA
        result = self.eng.update(v, ts=time.time())
        self.assertIsNone(result["demand_kvar"])

    def test_load_factor_computed(self):
        base_ts = time.time()
        # Simulate 1 hour of 100 kW → ~100 kWh, MD ~100 kW → LF near 100%
        result = self.eng.update(
            _vals(kw=100.0, kva=110.0, kwh=100.0),
            ts=base_ts + 3600.0
        )
        lf = result["load_factor_pct"]
        self.assertIsNotNone(lf)
        # LF cannot exceed 100%
        self.assertLessEqual(lf, 100.0)
        self.assertGreater(lf, 0.0)

    def test_md_utilisation_computed(self):
        ts = time.time()
        result = self.eng.update(_vals(kva=250.0), ts=ts)
        # Contract demand = 500 kVA, current ~250 kVA → utilisation ≈ 50%
        util = result["md_utilisation_pct"]
        self.assertIsNotNone(util)
        self.assertAlmostEqual(util, 50.0, delta=5.0)

    def test_md_utilisation_none_without_contract(self):
        eng = LoadKPIEngine(_cfg(contract_demand_kva=0.0))
        result = eng.update(_vals(), ts=time.time())
        self.assertIsNone(result["md_utilisation_pct"])

    def test_missing_pf_key_generates_warning(self):
        v = {"kW": 100.0, "kVA": 110.0, "Today_kWh": 50.0}  # no PFavg
        result = self.eng.update(v, ts=time.time())
        self.assertIsNotNone(result["warnings"])
        self.assertTrue(any("PF" in w for w in result["warnings"]))

    def test_flat_energy_billing(self):
        eng = LoadKPIEngine(_cfg(tariff_energy_rate=30.0, tou_enabled=False))
        result = eng.update(_vals(kwh=100.0), ts=time.time())
        # 100 kWh × 30 LKR/kWh = 3000 LKR
        self.assertAlmostEqual(result["energy_charge_est"], 3000.0, places=1)

    def test_reset_daily_clears_md(self):
        ts = time.time()
        self.eng.update(_vals(kva=400.0), ts=ts)
        self.eng.reset_daily()
        result = self.eng.update(_vals(kva=50.0), ts=ts + 100)
        # After reset, MD should be near 50
        self.assertLess(result["md_kva_today"], 100.0)

    def test_reconfigure_takes_effect(self):
        ts = time.time()
        result_before = self.eng.update(_vals(pf=0.80), ts=ts)
        self.assertEqual(result_before["pf_status"], "PENALTY")

        new_cfg = _cfg(pf_penalty_threshold=0.70)  # higher tolerance
        self.eng.reconfigure(new_cfg)
        result_after = self.eng.update(_vals(pf=0.80), ts=ts + 10)
        self.assertEqual(result_after["pf_status"], "NORMAL")

    def test_nan_values_skipped(self):
        """NaN values in the dict must not crash the engine."""
        import math
        v = _vals(kw=float("nan"), kva=float("nan"), pf=float("nan"))
        result = self.eng.update(v, ts=time.time())
        self.assertIsNone(result["demand_kva"])
        self.assertIsNone(result["demand_kw"])

    def test_negative_kw_no_crash(self):
        """Negative kW (export / CT reversal) should produce a finite result."""
        result = self.eng.update(_vals(kw=-20.0), ts=time.time())
        self.assertIsNotNone(result)


# ---------------------------------------------------------------------------
# TOU billing split
# ---------------------------------------------------------------------------

class TestTOUBilling(unittest.TestCase):
    """TOU kWh split and charge calculation."""

    def _make_engine(self, peak_rate=50.0, offpeak_rate=20.0):
        cfg = LoadPlantConfig(
            tou_enabled=True,
            tou_peak_start="18:30",
            tou_peak_end="22:30",
            tou_peak_rate_lkr_kwh=peak_rate,
            tou_offpeak_rate_lkr_kwh=offpeak_rate,
            power_key="kW",
            apparent_power_key="kVA",
            pf_key="PFavg",
            energy_key="Today_kWh",
        )
        return LoadKPIEngine(cfg)

    def test_all_samples_in_peak(self):
        eng = self._make_engine()
        ts_peak = _ts_at(20, 0)  # 20:00 — inside 18:30-22:30
        for i in range(5):
            result = eng.update(_vals(kw=100.0, kwh=100.0), ts=ts_peak + i)
        self.assertIsNotNone(result["tou_peak_kwh"])
        # All samples in peak → peak_ratio ≈ 1.0 → tou_peak_kwh ≈ 100
        self.assertAlmostEqual(result["tou_peak_kwh"], 100.0, delta=1.0)
        self.assertAlmostEqual(result["tou_offpeak_kwh"], 0.0, delta=1.0)

    def test_all_samples_off_peak(self):
        eng = self._make_engine()
        ts_offpeak = _ts_at(10, 0)  # 10:00 — outside 18:30-22:30
        for i in range(5):
            result = eng.update(_vals(kw=100.0, kwh=80.0), ts=ts_offpeak + i)
        self.assertIsNotNone(result["tou_offpeak_kwh"])
        self.assertAlmostEqual(result["tou_offpeak_kwh"], 80.0, delta=1.0)

    def test_charges_computed(self):
        eng = self._make_engine(peak_rate=50.0, offpeak_rate=20.0)
        ts_peak = _ts_at(20, 0)
        for i in range(5):
            result = eng.update(_vals(kw=100.0, kwh=100.0), ts=ts_peak + i)
        # All peak: charge = 100 kWh × 50 LKR = 5000
        self.assertIsNotNone(result["tou_peak_charge"])
        self.assertAlmostEqual(result["tou_peak_charge"], 5000.0, delta=50.0)

    def test_tou_disabled_uses_flat_rate(self):
        cfg = LoadPlantConfig(
            tou_enabled=False,
            tariff_energy_rate=30.0,
            power_key="kW",
            apparent_power_key="kVA",
            pf_key="PFavg",
            energy_key="Today_kWh",
        )
        eng = LoadKPIEngine(cfg)
        result = eng.update(_vals(kwh=100.0), ts=time.time())
        self.assertIsNone(result["tou_peak_kwh"])
        self.assertAlmostEqual(result["energy_charge_est"], 3000.0, places=1)

    def test_tou_peak_kwh_none_when_no_kwh(self):
        """Without kWh in values, split cannot be computed."""
        eng = self._make_engine()
        v = {"kW": 100.0, "kVA": 110.0, "PFavg": 0.91}  # no Today_kWh
        result = eng.update(v, ts=_ts_at(20, 0))
        self.assertIsNone(result["tou_peak_kwh"])


# ---------------------------------------------------------------------------
# Capacitor bank recommendation
# ---------------------------------------------------------------------------

class TestCapacitorRec(unittest.TestCase):

    def test_cap_rec_positive_when_low_pf(self):
        cfg = _cfg(pf_incentive_threshold=0.95)
        eng = LoadKPIEngine(cfg)
        result = eng.update(_vals(kw=100.0, kva=125.0, pf=0.80), ts=time.time())
        # Q_C = P × (tan(acos(0.80)) − tan(acos(0.95))) > 0
        cap = result["capacitor_kvar_rec"]
        self.assertIsNotNone(cap)
        self.assertGreater(cap, 0.0)

    def test_cap_rec_zero_when_pf_at_target(self):
        cfg = _cfg(pf_incentive_threshold=0.95)
        eng = LoadKPIEngine(cfg)
        result = eng.update(_vals(pf=0.97), ts=time.time())
        # pf ≥ incentive → no compensation needed
        self.assertIsNone(result["capacitor_kvar_rec"])

    def test_cap_rec_none_without_kw(self):
        cfg = _cfg(pf_incentive_threshold=0.95)
        eng = LoadKPIEngine(cfg)
        v = {"kVA": 110.0, "PFavg": 0.80, "Today_kWh": 50.0}  # no kW
        result = eng.update(v, ts=time.time())
        self.assertIsNone(result["capacitor_kvar_rec"])


if __name__ == "__main__":
    unittest.main()
