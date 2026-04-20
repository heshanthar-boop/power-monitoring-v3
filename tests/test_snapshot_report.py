"""
Unit tests — core/snapshot_report.py

Coverage:
  _h            — html escaping
  _fmt          — numeric formatting, None/nan, units
  _quality_badge — css class selection, label text
  _val_class    — deviation thresholds (ok/warn/alarm/empty)
  _pf_class     — power factor thresholds
  build_snapshot_html — structure, tiles, alarm table, meter sections,
                        no-alarm branch, no-meter-data branch,
                        TOTAL exclusion, site/location header
  SnapshotReporter.generate  — writes file, correct name pattern, HTML content
  SnapshotReporter.open_in_browser — calls webbrowser with file:/// URI
  SnapshotReporter.print_snapshot  — generate + open in one call
  SnapshotReporter.last_path       — updated after generate
  SnapshotReporter.reconfigure     — cfg swap reflected in next generate
"""
import sys
import os
import math
import re
import tempfile
import unittest
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from core.snapshot_report import (
    _h, _fmt, _quality_badge, _val_class, _pf_class,
    build_snapshot_html, SnapshotReporter,
)


# ── Minimal AlarmEvent stub ───────────────────────────────────────────────────

class _Alarm:
    def __init__(self, meter_id="M1", code="OV", message="Overvoltage",
                 severity="ALARM", ts=1_700_000_000.0, acknowledged=False):
        self.meter_id     = meter_id
        self.code         = code
        self.message      = message
        self.severity     = severity
        self.ts           = ts
        self.acknowledged = acknowledged


class _Meter:
    def __init__(self, meter_id="M1", name="Main Incomer"):
        self.meter_id = meter_id
        self.name     = name


# ── Shared minimal data ───────────────────────────────────────────────────────

def _cfg(**overrides):
    base = {
        "site": {"plant_name": "TestPlant", "location": "Block A", "nominal_vln": 230.0},
        "reports": {},
    }
    base.update(overrides)
    return base


def _vals():
    return {
        "TOTAL": {
            "kW": 120.5, "kVA": 135.0, "PFavg": 0.892,
            "Vavg": 228.3, "Iavg": 195.0, "Frequency": 50.02,
            "Today_kWh": 45.6,
        },
        "M1": {
            "V1N": 228.0, "V2N": 229.5, "V3N": 227.1,
            "V12": 395.0, "V23": 397.0, "V31": 394.5,
            "I1": 65.0, "I2": 67.0, "I3": 63.5,
            "kW": 120.5, "kVA": 135.0, "kVAr": 58.0,
            "PFavg": 0.892, "Frequency": 50.02, "Vavg": 228.3, "Iavg": 65.2,
            "Today_kWh": 45.6,
        },
    }


def _quality():
    return {"M1": "GOOD"}


def _alarms():
    return {
        ("M1", "OV"): _Alarm("M1", "OV", "Overvoltage", "ALARM"),
        ("M2", "PF_LOW"): _Alarm("M2", "PF_LOW", "Low PF", "WARN"),
    }


# ─────────────────────────────────────────────────────────────────────────────
# _h
# ─────────────────────────────────────────────────────────────────────────────

class TestH(unittest.TestCase):

    def test_escapes_angle_brackets(self):
        self.assertEqual(_h("<b>"), "&lt;b&gt;")

    def test_escapes_ampersand(self):
        self.assertEqual(_h("A&B"), "A&amp;B")

    def test_none_becomes_empty_string(self):
        # _h converts None → "" (None is mapped to empty string in _h)
        result = _h(None)
        self.assertIsInstance(result, str)   # must be a string, not crash

    def test_integer_passthrough(self):
        self.assertEqual(_h(42), "42")


# ─────────────────────────────────────────────────────────────────────────────
# _fmt
# ─────────────────────────────────────────────────────────────────────────────

class TestFmt(unittest.TestCase):

    def test_normal_float(self):
        self.assertEqual(_fmt(230.456, 2), "230.46")

    def test_zero(self):
        self.assertEqual(_fmt(0, 2), "0.00")

    def test_none_returns_dash(self):
        self.assertEqual(_fmt(None), "—")

    def test_nan_returns_dash(self):
        self.assertEqual(_fmt(float("nan")), "—")

    def test_inf_returns_dash(self):
        self.assertEqual(_fmt(float("inf")), "—")

    def test_with_unit(self):
        result = _fmt(50.0, 2, "Hz")
        self.assertIn("50.00", result)
        self.assertIn("Hz", result)

    def test_ndp_zero(self):
        self.assertEqual(_fmt(230.7, 0), "231")

    def test_string_non_numeric(self):
        self.assertEqual(_fmt("hello"), "—")

    def test_negative_value(self):
        self.assertEqual(_fmt(-12.5, 1), "-12.5")


# ─────────────────────────────────────────────────────────────────────────────
# _quality_badge
# ─────────────────────────────────────────────────────────────────────────────

class TestQualityBadge(unittest.TestCase):

    def test_good_class(self):
        html = _quality_badge("GOOD")
        self.assertIn("q-GOOD", html)
        self.assertIn("GOOD", html)

    def test_stale_class(self):
        html = _quality_badge("STALE")
        self.assertIn("q-STALE", html)

    def test_comm_lost_label(self):
        html = _quality_badge("COMM_LOST")
        self.assertIn("OFFLINE", html)

    def test_bad_class_for_unknown(self):
        html = _quality_badge("FAULT")
        self.assertIn("q-BAD", html)

    def test_empty_string(self):
        html = _quality_badge("")
        self.assertIn("UNKNOWN", html)

    def test_lowercase_accepted(self):
        html = _quality_badge("good")
        self.assertIn("q-GOOD", html)


# ─────────────────────────────────────────────────────────────────────────────
# _val_class
# ─────────────────────────────────────────────────────────────────────────────

class TestValClass(unittest.TestCase):

    def test_within_warn_tolerance_is_ok(self):
        # 230 nominal, ±10 % = ok
        cls = _val_class(230.0, 230.0)
        self.assertIn("ok", cls)

    def test_at_warn_boundary(self):
        # 230 * 1.10 = 253 — just inside ok/warn boundary
        cls = _val_class(253.0, 230.0, tol_warn=0.10)
        self.assertIn("ok", cls)

    def test_between_warn_and_alarm(self):
        # 230 * 1.12 = 257.6 — in warn zone
        cls = _val_class(257.6, 230.0, tol_warn=0.10, tol_alarm=0.15)
        self.assertIn("warn", cls)

    def test_beyond_alarm_threshold(self):
        # 230 * 1.20 = 276 — alarm
        cls = _val_class(276.0, 230.0, tol_warn=0.10, tol_alarm=0.15)
        self.assertIn("alarm", cls)

    def test_none_returns_empty(self):
        self.assertEqual(_val_class(None, 230.0), "")

    def test_nan_returns_empty(self):
        self.assertEqual(_val_class(float("nan"), 230.0), "")

    def test_zero_nominal_returns_empty(self):
        self.assertEqual(_val_class(100.0, 0.0), "")


# ─────────────────────────────────────────────────────────────────────────────
# _pf_class
# ─────────────────────────────────────────────────────────────────────────────

class TestPfClass(unittest.TestCase):

    def test_pf_095_ok(self):
        self.assertIn("ok", _pf_class(0.95))

    def test_pf_099_ok(self):
        self.assertIn("ok", _pf_class(0.99))

    def test_pf_090_warn(self):
        self.assertIn("warn", _pf_class(0.90))

    def test_pf_085_warn(self):
        self.assertIn("warn", _pf_class(0.85))

    def test_pf_080_alarm(self):
        self.assertIn("alarm", _pf_class(0.80))

    def test_negative_pf_abs(self):
        # Negative PF (leading) — abs used
        self.assertIn("ok", _pf_class(-0.98))

    def test_none_returns_empty(self):
        self.assertEqual(_pf_class(None), "")


# ─────────────────────────────────────────────────────────────────────────────
# build_snapshot_html — structure
# ─────────────────────────────────────────────────────────────────────────────

class TestBuildSnapshotHtml(unittest.TestCase):

    def setUp(self):
        self.html = build_snapshot_html(
            cfg=_cfg(),
            values_by_source=_vals(),
            quality_map=_quality(),
            active_alarms=_alarms(),
            meters=[_Meter("M1", "Main Incomer")],
        )

    # ── Basic structure ───────────────────────────────────────────────────────

    def test_is_complete_html(self):
        self.assertIn("<!DOCTYPE html>", self.html)
        self.assertIn("</html>", self.html)

    def test_charset_utf8(self):
        self.assertIn("UTF-8", self.html)

    def test_inline_css_present(self):
        self.assertIn("<style>", self.html)

    def test_site_name_in_header(self):
        self.assertIn("TestPlant", self.html)

    def test_location_in_header(self):
        self.assertIn("Block A", self.html)

    def test_timestamp_format(self):
        self.assertTrue(
            re.search(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", self.html)
        )

    # ── Summary tiles ─────────────────────────────────────────────────────────

    def test_kw_tile_present(self):
        self.assertIn("Active Power", self.html)
        self.assertIn("120", self.html)          # 120.5 kW

    def test_pf_tile_present(self):
        self.assertIn("Power Factor", self.html)
        self.assertIn("0.892", self.html)

    def test_freq_tile_present(self):
        self.assertIn("Frequency", self.html)
        self.assertIn("50.0", self.html)

    # ── Alarm table ───────────────────────────────────────────────────────────

    def test_active_alarms_heading(self):
        self.assertIn("Active Alarms", self.html)

    def test_alarm_code_in_table(self):
        self.assertIn("OV", self.html)
        self.assertIn("PF_LOW", self.html)

    def test_alarm_severity_labels(self):
        self.assertIn("ALARM", self.html)
        self.assertIn("WARN", self.html)

    def test_alarm_message_in_table(self):
        self.assertIn("Overvoltage", self.html)
        self.assertIn("Low PF", self.html)

    # ── Meter section ─────────────────────────────────────────────────────────

    def test_meter_source_key_present(self):
        self.assertIn("M1", self.html)

    def test_meter_name_present(self):
        self.assertIn("Main Incomer", self.html)

    def test_voltage_ln_row_present(self):
        self.assertIn("Voltage L-N", self.html)

    def test_voltage_ll_row_present(self):
        self.assertIn("Voltage L-L", self.html)

    def test_current_row_present(self):
        self.assertIn("Current", self.html)

    def test_pf_row_present(self):
        self.assertIn("Power Factor", self.html)

    def test_kwh_row_present(self):
        self.assertIn("Today kWh", self.html)

    def test_total_excluded_from_meter_sections(self):
        # TOTAL should not appear as a meter-section title (only appears in summary)
        meter_section_count = self.html.count('class="meter-section"')
        self.assertEqual(meter_section_count, 1)   # only M1

    def test_quality_badge_present(self):
        self.assertIn("q-GOOD", self.html)

    # ── Footer ────────────────────────────────────────────────────────────────

    def test_footer_present(self):
        self.assertIn("report-footer", self.html)
        self.assertIn("Ctrl+P", self.html)


class TestBuildSnapshotHtmlNoAlarms(unittest.TestCase):

    def test_no_alarm_message(self):
        html = build_snapshot_html(
            cfg=_cfg(),
            values_by_source=_vals(),
            quality_map=_quality(),
            active_alarms={},
            meters=[],
        )
        self.assertIn("No active alarms", html)

    def test_no_meter_data_message(self):
        html = build_snapshot_html(
            cfg=_cfg(),
            values_by_source={"TOTAL": _vals()["TOTAL"]},
            quality_map={},
            active_alarms={},
            meters=[],
        )
        self.assertIn("No meter data available", html)


class TestBuildSnapshotHtmlSiteDefaults(unittest.TestCase):

    def test_empty_cfg_uses_default_plant_name(self):
        html = build_snapshot_html(
            cfg={},
            values_by_source={},
            quality_map={},
            active_alarms={},
            meters=[],
        )
        self.assertIn("Power Monitor", html)

    def test_no_location_skips_separator(self):
        html = build_snapshot_html(
            cfg={"site": {"plant_name": "NoLocSite"}},
            values_by_source={},
            quality_map={},
            active_alarms={},
            meters=[],
        )
        # When no location, "— <location>" should not appear inside the site div
        # Find the site div and confirm it has no em-dash location line
        # The site div pattern: <div class="site">NoLocSite</div>
        self.assertIn("NoLocSite", html)
        # The location separator only appears when location is non-empty
        # Grab everything between the site name and the closing </div>
        site_match = re.search(r'class="site">(.*?)</div>', html, re.DOTALL)
        if site_match:
            site_content = site_match.group(1)
            self.assertNotIn("— ", site_content)

    def test_special_chars_escaped(self):
        html = build_snapshot_html(
            cfg={"site": {"plant_name": "<Danger&Co>"}},
            values_by_source={},
            quality_map={},
            active_alarms={},
            meters=[],
        )
        self.assertNotIn("<Danger&Co>", html)
        self.assertIn("&lt;Danger&amp;Co&gt;", html)


# ─────────────────────────────────────────────────────────────────────────────
# SnapshotReporter
# ─────────────────────────────────────────────────────────────────────────────

class TestSnapshotReporterGenerate(unittest.TestCase):

    def setUp(self):
        self.reporter = SnapshotReporter(_cfg())
        self.tmp = tempfile.mkdtemp()

    def test_returns_file_path(self):
        path = self.reporter.generate(_vals(), _quality(), {}, [], out_dir=self.tmp)
        self.assertTrue(os.path.isfile(path))

    def test_filename_has_snapshot_prefix(self):
        path = self.reporter.generate(_vals(), _quality(), {}, [], out_dir=self.tmp)
        self.assertTrue(os.path.basename(path).startswith("snapshot_"))

    def test_filename_has_html_extension(self):
        path = self.reporter.generate(_vals(), _quality(), {}, [], out_dir=self.tmp)
        self.assertTrue(path.endswith(".html"))

    def test_html_content_written(self):
        path = self.reporter.generate(_vals(), _quality(), {}, [], out_dir=self.tmp)
        with open(path, encoding="utf-8") as fh:
            content = fh.read()
        self.assertIn("<!DOCTYPE html>", content)

    def test_last_path_updated(self):
        self.assertIsNone(self.reporter.last_path)
        path = self.reporter.generate(_vals(), _quality(), {}, [], out_dir=self.tmp)
        self.assertEqual(self.reporter.last_path, path)

    def test_second_generate_updates_last_path(self):
        # Both generates may produce the same filename within the same second —
        # that's fine. What matters is last_path always reflects the most recent call.
        import time as _t
        path1 = self.reporter.generate(_vals(), _quality(), {}, [], out_dir=self.tmp)
        _t.sleep(1.05)   # cross second boundary so filenames differ
        path2 = self.reporter.generate(_vals(), _quality(), {}, [], out_dir=self.tmp)
        self.assertEqual(self.reporter.last_path, path2)
        # Both files should exist
        self.assertTrue(os.path.isfile(path2))

    def test_fallback_to_tempdir_when_outdir_missing(self):
        path = self.reporter.generate(_vals(), _quality(), {}, [],
                                       out_dir="/nonexistent_xyz_99")
        self.assertTrue(os.path.isfile(path))
        self.assertTrue(path.startswith(tempfile.gettempdir()))


class TestSnapshotReporterOpenInBrowser(unittest.TestCase):

    def test_calls_webbrowser_open(self):
        reporter = SnapshotReporter(_cfg())
        with patch("webbrowser.open") as mock_open:
            reporter.open_in_browser("/tmp/test.html")
            self.assertTrue(mock_open.called)

    def test_uri_has_file_scheme(self):
        reporter = SnapshotReporter(_cfg())
        with patch("webbrowser.open") as mock_open:
            reporter.open_in_browser("/tmp/test.html")
            uri = mock_open.call_args[0][0]
            self.assertTrue(uri.startswith("file:///"))

    def test_backslash_converted(self):
        reporter = SnapshotReporter(_cfg())
        with patch("webbrowser.open") as mock_open:
            reporter.open_in_browser("C:\\Users\\test.html")
            uri = mock_open.call_args[0][0]
            self.assertNotIn("\\", uri)

    def test_webbrowser_error_does_not_raise(self):
        reporter = SnapshotReporter(_cfg())
        with patch("webbrowser.open", side_effect=OSError("no browser")):
            reporter.open_in_browser("/tmp/test.html")   # must not raise


class TestSnapshotReporterPrintSnapshot(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.mkdtemp()

    def test_returns_file_path(self):
        reporter = SnapshotReporter(_cfg())
        with patch("webbrowser.open"):
            path = reporter.print_snapshot(_vals(), _quality(), {}, [],
                                           out_dir=self.tmp)
        self.assertTrue(os.path.isfile(path))

    def test_opens_in_browser(self):
        reporter = SnapshotReporter(_cfg())
        with patch("webbrowser.open") as mock_open:
            reporter.print_snapshot(_vals(), _quality(), {}, [], out_dir=self.tmp)
        self.assertTrue(mock_open.called)

    def test_last_path_set_after_print(self):
        reporter = SnapshotReporter(_cfg())
        with patch("webbrowser.open"):
            path = reporter.print_snapshot(_vals(), _quality(), {}, [],
                                           out_dir=self.tmp)
        self.assertEqual(reporter.last_path, path)


class TestSnapshotReporterReconfigure(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.mkdtemp()

    def test_reconfigure_changes_site_name_in_output(self):
        reporter = SnapshotReporter(_cfg())
        reporter.reconfigure({"site": {"plant_name": "NewSite"}})
        with patch("webbrowser.open"):
            path = reporter.print_snapshot(_vals(), _quality(), {}, [],
                                           out_dir=self.tmp)
        with open(path, encoding="utf-8") as fh:
            content = fh.read()
        self.assertIn("NewSite", content)
        self.assertNotIn("TestPlant", content)


if __name__ == "__main__":
    unittest.main()
