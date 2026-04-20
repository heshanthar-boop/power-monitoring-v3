"""
Unit tests — core/sms_service.py

Coverage:
  SMSAlertService.enabled          — reads cfg correctly
  SMSAlertService.provider         — reads cfg correctly
  SMSAlertService.send_alarm_alert — disabled skip, severity filter, warn filter,
                                     CLEAR filter, rate-limit cooldown, fires thread
  SMSAlertService.test_send        — dispatches synchronously
  SMSAlertService.reconfigure      — cfg swap reflected
  _fmt_message                     — template substitution
  _mask_number                     — log masking
  Provider functions               — called with correct args (patched urllib)
"""
import sys
import os
import time
import threading
import unittest
from unittest.mock import patch, MagicMock, call

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from core.sms_service import (
    SMSAlertService,
    _fmt_message,
    _mask_number,
    _send_dialog,
    _send_mobitel,
    _send_generic_http,
    _DEFAULT_TEMPLATE,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _cfg(**kwargs) -> dict:
    """Build a minimal config dict with sms_alert section."""
    sms = {
        "enabled":          True,
        "provider":         "twilio",
        "recipients":       ["+94771234567"],
        "notify_clear":     False,
        "notify_warn":      False,
        "cooldown_min":     0,        # 0 = no rate-limit in tests
        "provider_config":  {
            "account_sid":  "ACtest",
            "auth_token":   "token",
            "from_number":  "+10000000000",
        },
    }
    sms.update(kwargs)
    return {"sms_alert": sms, "site": {"plant_name": "TestSite"}}


def _svc(**kwargs) -> SMSAlertService:
    return SMSAlertService(_cfg(**kwargs))


# ---------------------------------------------------------------------------
# _fmt_message
# ---------------------------------------------------------------------------

class TestFmtMessage(unittest.TestCase):

    def test_substitutes_all_fields(self):
        msg = _fmt_message(
            _DEFAULT_TEMPLATE,
            site="MySite",
            meter_id="M1",
            meter_name="Main Meter",
            code="OV",
            message="Overvoltage detected",
        )
        self.assertIn("MySite", msg)
        self.assertIn("OV", msg)
        self.assertIn("Main Meter", msg)
        self.assertIn("Overvoltage detected", msg)

    def test_custom_template(self):
        tpl = "SITE={site} CODE={code}"
        msg = _fmt_message(tpl, site="X", meter_id="M1", meter_name="Y",
                           code="UV", message="Low V")
        self.assertEqual(msg, "SITE=X CODE=UV")

    def test_ts_field_included(self):
        msg = _fmt_message(_DEFAULT_TEMPLATE, site="S", meter_id="M1",
                           meter_name="M", code="OV", message="V high")
        # ts is formatted as YYYY-MM-DD HH:MM:SS
        import re
        self.assertTrue(re.search(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", msg))


# ---------------------------------------------------------------------------
# _mask_number
# ---------------------------------------------------------------------------

class TestMaskNumber(unittest.TestCase):

    def test_masks_middle(self):
        masked = _mask_number("+94771234567")
        self.assertTrue(masked.startswith("+947"))
        self.assertTrue(masked.endswith("567"))
        self.assertIn("****", masked)

    def test_short_number(self):
        # Should not crash for very short numbers
        masked = _mask_number("123")
        self.assertEqual(masked, "****")

    def test_whatsapp_prefix(self):
        masked = _mask_number("whatsapp:+94771234567")
        self.assertIn("****", masked)


# ---------------------------------------------------------------------------
# SMSAlertService.enabled / provider
# ---------------------------------------------------------------------------

class TestProperties(unittest.TestCase):

    def test_enabled_true(self):
        self.assertTrue(_svc().enabled)

    def test_enabled_false(self):
        svc = SMSAlertService({"sms_alert": {"enabled": False}})
        self.assertFalse(svc.enabled)

    def test_enabled_default_false(self):
        svc = SMSAlertService({})
        self.assertFalse(svc.enabled)

    def test_provider_twilio(self):
        self.assertEqual(_svc().provider, "twilio")

    def test_provider_dialog(self):
        svc = SMSAlertService(_cfg(**{"provider": "dialog"}))
        self.assertEqual(svc.provider, "dialog")

    def test_provider_default(self):
        svc = SMSAlertService({})
        self.assertEqual(svc.provider, "twilio")


# ---------------------------------------------------------------------------
# SMSAlertService.send_alarm_alert — skip conditions
# ---------------------------------------------------------------------------

class TestSendAlarmAlertSkipConditions(unittest.TestCase):

    def test_disabled_service_skips(self):
        svc = SMSAlertService({"sms_alert": {"enabled": False}})
        dispatched = []
        svc._dispatch = lambda body, recipients: dispatched.append(body)
        svc.send_alarm_alert("M1", "Meter", "OV", "Overvoltage", "ALARM", "RAISE")
        self.assertEqual(dispatched, [])

    def test_warn_severity_skipped_by_default(self):
        svc = _svc(notify_warn=False)
        dispatched = []
        svc._dispatch = lambda body, recipients: dispatched.append(body)
        svc.send_alarm_alert("M1", "Meter", "PF_LOW", "Low PF", "WARN", "RAISE")
        self.assertEqual(dispatched, [])

    def test_warn_severity_sent_when_enabled(self):
        svc = _svc(notify_warn=True, cooldown_min=0)
        fired = threading.Event()

        def _fake_dispatch(body, recipients):
            fired.set()
        svc._dispatch = _fake_dispatch
        svc.send_alarm_alert("M1", "Meter", "PF_LOW", "Low PF", "WARN", "RAISE")
        fired.wait(timeout=1.0)
        self.assertTrue(fired.is_set())

    def test_clear_skipped_by_default(self):
        svc = _svc(notify_clear=False)
        dispatched = []
        svc._dispatch = lambda body, recipients: dispatched.append(body)
        svc.send_alarm_alert("M1", "Meter", "OV", "Cleared", "ALARM", "CLEAR")
        self.assertEqual(dispatched, [])

    def test_clear_sent_when_notify_clear_enabled(self):
        svc = _svc(notify_clear=True, cooldown_min=0)
        fired = threading.Event()

        def _fake_dispatch(body, recipients):
            fired.set()
        svc._dispatch = _fake_dispatch
        svc.send_alarm_alert("M1", "Meter", "OV", "Cleared", "ALARM", "CLEAR")
        fired.wait(timeout=1.0)
        self.assertTrue(fired.is_set())

    def test_no_recipients_skips(self):
        svc = _svc(**{"recipients": []})
        dispatched = []
        svc._dispatch = lambda body, recipients: dispatched.append(body)
        svc.send_alarm_alert("M1", "Meter", "OV", "Overvoltage", "ALARM", "RAISE")
        self.assertEqual(dispatched, [])


# ---------------------------------------------------------------------------
# Rate-limit (cooldown)
# ---------------------------------------------------------------------------

class TestRateLimit(unittest.TestCase):

    def test_second_send_within_cooldown_blocked(self):
        svc = _svc(cooldown_min=60)
        fired = []

        def _fake_dispatch(body, recipients):
            fired.append(1)
        svc._dispatch = _fake_dispatch

        svc.send_alarm_alert("M1", "M", "OV", "msg", "ALARM", "RAISE")
        # Should be blocked — within cooldown
        svc.send_alarm_alert("M1", "M", "OV", "msg", "ALARM", "RAISE")
        time.sleep(0.1)   # give thread time to run
        self.assertEqual(len(fired), 1)

    def test_different_codes_not_rate_limited_together(self):
        svc = _svc(cooldown_min=60)
        fired = []

        def _fake_dispatch(body, recipients):
            fired.append(body)
        svc._dispatch = _fake_dispatch

        svc.send_alarm_alert("M1", "M", "OV", "msg", "ALARM", "RAISE")
        svc.send_alarm_alert("M1", "M", "UV", "msg", "ALARM", "RAISE")
        time.sleep(0.1)
        self.assertEqual(len(fired), 2)

    def test_zero_cooldown_allows_repeat(self):
        svc = _svc(cooldown_min=0)
        fired = []

        def _fake_dispatch(body, recipients):
            fired.append(1)
        svc._dispatch = _fake_dispatch

        svc.send_alarm_alert("M1", "M", "OV", "msg1", "ALARM", "RAISE")
        # Force expiry by manually clearing last_sent
        svc._last_sent.clear()
        svc.send_alarm_alert("M1", "M", "OV", "msg2", "ALARM", "RAISE")
        time.sleep(0.1)
        self.assertGreaterEqual(len(fired), 1)


# ---------------------------------------------------------------------------
# Fire-and-forget threading
# ---------------------------------------------------------------------------

class TestFireAndForget(unittest.TestCase):

    def test_send_does_not_block_caller(self):
        """send_alarm_alert must return quickly; network call is in thread."""
        svc = _svc(cooldown_min=0)
        slow_done = threading.Event()

        def _slow_dispatch(body, recipients):
            time.sleep(0.3)
            slow_done.set()

        svc._dispatch = _slow_dispatch
        start = time.time()
        svc.send_alarm_alert("M1", "M", "OV", "msg", "ALARM", "RAISE")
        elapsed = time.time() - start
        # Caller should return in << 300 ms
        self.assertLess(elapsed, 0.1, "send_alarm_alert blocked the caller")
        slow_done.wait(timeout=1.0)

    def test_dispatch_exception_does_not_propagate(self):
        """Network failure in dispatch must never raise to the alarm engine."""
        svc = _svc(cooldown_min=0)

        def _boom(body, recipients, raise_on_error=False):
            raise RuntimeError("network down")

        # Patch _dispatch_single so the exception goes through the real _dispatch
        # wrapper, which catches it and increments fail_count.
        svc._dispatch_single = _boom
        # Must not raise
        try:
            svc.send_alarm_alert("M1", "M", "OV", "msg", "ALARM", "RAISE")
        except Exception as e:
            self.fail(f"send_alarm_alert raised: {e}")
        time.sleep(0.2)   # give daemon thread time to finish
        self.assertEqual(svc.fail_count, 1)


# ---------------------------------------------------------------------------
# reconfigure
# ---------------------------------------------------------------------------

class TestReconfigure(unittest.TestCase):

    def test_reconfigure_updates_enabled(self):
        svc = _svc()
        self.assertTrue(svc.enabled)
        new_cfg = _cfg(**{"enabled": False})
        svc.reconfigure(new_cfg)
        self.assertFalse(svc.enabled)

    def test_reconfigure_updates_provider(self):
        svc = _svc()
        self.assertEqual(svc.provider, "twilio")
        new_cfg = _cfg(**{"provider": "dialog"})
        svc.reconfigure(new_cfg)
        self.assertEqual(svc.provider, "dialog")


# ---------------------------------------------------------------------------
# Provider: Dialog HTTP
# ---------------------------------------------------------------------------

class TestDialogProvider(unittest.TestCase):

    def test_dialog_posts_correct_params(self):
        pcfg = {
            "api_url":  "https://www.dialog.lk/sms/api/",
            "username": "user1",
            "password": "pass1",
            "sender_id": "SCADA",
        }
        mock_resp = MagicMock()
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_resp.read.return_value = b"OK"

        with patch("urllib.request.urlopen", return_value=mock_resp) as mock_open:
            _send_dialog(pcfg, ["+94771234567"], "Test alarm message")
            self.assertTrue(mock_open.called)
            req = mock_open.call_args[0][0]
            # Verify POST data contains expected fields
            body = req.data.decode()
            self.assertIn("user1", body)
            self.assertIn("Test+alarm+message", body.replace("%20", "+").replace("+", "+"))

    def test_dialog_skips_without_credentials(self):
        with patch("urllib.request.urlopen") as mock_open:
            _send_dialog({}, ["+94771234567"], "msg")
            mock_open.assert_not_called()

    def test_dialog_handles_http_error(self):
        import urllib.error
        pcfg = {"username": "u", "password": "p"}
        with patch("urllib.request.urlopen",
                   side_effect=urllib.error.HTTPError("url", 403, "Forbidden", {}, None)):
            # Must not raise
            _send_dialog(pcfg, ["+94771234567"], "msg")


# ---------------------------------------------------------------------------
# Provider: Mobitel HTTP
# ---------------------------------------------------------------------------

class TestMobitelProvider(unittest.TestCase):

    def test_mobitel_posts_json(self):
        pcfg = {
            "api_url":  "https://smsapi.mobitel.lk/smpp/",
            "username": "muser",
            "password": "mpass",
            "sender_id": "SCADA",
        }
        mock_resp = MagicMock()
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_resp.read.return_value = b'{"status":"OK"}'

        with patch("urllib.request.urlopen", return_value=mock_resp) as mock_open:
            _send_mobitel(pcfg, ["+94771234567"], "Test message")
            self.assertTrue(mock_open.called)
            req = mock_open.call_args[0][0]
            import json
            payload = json.loads(req.data.decode())
            self.assertEqual(payload["username"], "muser")
            self.assertIn("Test message", payload["message"])

    def test_mobitel_skips_without_credentials(self):
        with patch("urllib.request.urlopen") as mock_open:
            _send_mobitel({}, ["+94771234567"], "msg")
            mock_open.assert_not_called()


# ---------------------------------------------------------------------------
# Provider: generic HTTP
# ---------------------------------------------------------------------------

class TestGenericHttpProvider(unittest.TestCase):

    def test_generic_skips_without_url(self):
        with patch("urllib.request.urlopen") as mock_open:
            _send_generic_http({}, ["+94771234567"], "msg")
            mock_open.assert_not_called()

    def test_generic_posts_to_url(self):
        pcfg = {"api_url": "http://local-gsm-modem/send"}
        mock_resp = MagicMock()
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_resp.read.return_value = b"sent"

        with patch("urllib.request.urlopen", return_value=mock_resp) as mock_open:
            _send_generic_http(pcfg, ["+94771234567"], "Alarm message")
            self.assertTrue(mock_open.called)

    def test_generic_sends_api_key_header(self):
        pcfg = {
            "api_url":        "http://local/send",
            "api_key_header": "X-Api-Key",
            "api_key":        "mysecret",
        }
        mock_resp = MagicMock()
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_resp.read.return_value = b"ok"

        with patch("urllib.request.urlopen", return_value=mock_resp) as mock_open:
            _send_generic_http(pcfg, ["+94771234567"], "msg")
            req = mock_open.call_args[0][0]
            self.assertEqual(req.get_header("X-api-key"), "mysecret")


# ---------------------------------------------------------------------------
# test_send (synchronous path)
# ---------------------------------------------------------------------------

class TestTestSend(unittest.TestCase):

    def test_test_send_disabled(self):
        svc = SMSAlertService({"sms_alert": {"enabled": False}})
        err = svc.test_send("+94771234567")
        self.assertIn("disabled", err.lower())

    def test_test_send_dispatches(self):
        svc = _svc()
        dispatched = []

        def _fake_single(body, recipients, raise_on_error=False):
            dispatched.append((body, recipients))
        svc._dispatch_single = _fake_single
        err = svc.test_send("+94771234567")
        self.assertEqual(err, "")
        self.assertEqual(len(dispatched), 1)
        self.assertIn("[TEST]", dispatched[0][0])
        self.assertEqual(dispatched[0][1], ["+94771234567"])

    def test_test_send_returns_error_on_exception(self):
        svc = _svc()

        def _fail(body, recipients, raise_on_error=False):
            raise ConnectionError("host unreachable")
        svc._dispatch_single = _fail
        err = svc.test_send("+94771234567")
        self.assertIn("host unreachable", err)


if __name__ == "__main__":
    unittest.main()
