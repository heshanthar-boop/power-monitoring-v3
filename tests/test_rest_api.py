"""
Unit tests — core/rest_api.py

Tests the pure-logic components that don't require a live HTTP server
or FastAPI/uvicorn:

  _sanitise()          — NaN/Inf/bool handling
  _ApiState.update()   — thread-safe snapshot write + read
  _ApiState.snapshot() — correct read-out
  RESTApiServer.is_running / bind_address — properties before start()
  RESTApiServer.backend — 'none' before start
  FastAPI app routes   — tested via TestClient if fastapi available
"""
import sys
import os
import math
import time
import threading
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from core.rest_api import _sanitise, _ApiState, RESTApiServer


# ---------------------------------------------------------------------------
# _sanitise
# ---------------------------------------------------------------------------

class TestSanitiseRestApi(unittest.TestCase):
    """rest_api._sanitise — slightly different from mqtt version (includes bool)."""

    def test_finite_float_rounded(self):
        out = _sanitise({"v": 1.123456789})
        self.assertAlmostEqual(out["v"], 1.1235, places=4)

    def test_nan_dropped(self):
        self.assertNotIn("v", _sanitise({"v": float("nan")}))

    def test_inf_dropped(self):
        self.assertNotIn("v", _sanitise({"v": float("inf")}))

    def test_int_kept(self):
        out = _sanitise({"n": 42})
        self.assertEqual(out["n"], 42)

    def test_str_kept(self):
        out = _sanitise({"s": "GOOD"})
        self.assertEqual(out["s"], "GOOD")

    def test_bool_kept(self):
        out = _sanitise({"flag": True})
        self.assertTrue(out["flag"])

    def test_none_dict_returns_empty(self):
        self.assertEqual(_sanitise(None), {})

    def test_empty_dict_returns_empty(self):
        self.assertEqual(_sanitise({}), {})

    def test_zero_float_kept(self):
        out = _sanitise({"v": 0.0})
        self.assertIn("v", out)


# ---------------------------------------------------------------------------
# _ApiState
# ---------------------------------------------------------------------------

class TestApiState(unittest.TestCase):

    def setUp(self):
        self.state = _ApiState()

    def test_initial_ts_zero(self):
        snap = self.state.snapshot()
        self.assertEqual(snap["ts"], 0.0)

    def test_update_stores_readings(self):
        self.state.update({"M1": {"kW": 100.0}}, {"M1": "GOOD"}, ts=1000.0)
        snap = self.state.snapshot()
        self.assertIn("M1", snap["readings"])
        self.assertEqual(snap["readings"]["M1"]["kW"], 100.0)
        self.assertEqual(snap["quality"]["M1"], "GOOD")
        self.assertEqual(snap["ts"], 1000.0)

    def test_nan_sanitised_on_update(self):
        self.state.update({"M1": {"kW": float("nan"), "Vavg": 230.0}},
                          {"M1": "GOOD"}, ts=1.0)
        snap = self.state.snapshot()
        self.assertNotIn("kW", snap["readings"]["M1"])
        self.assertIn("Vavg", snap["readings"]["M1"])

    def test_update_alarms(self):
        alarms = [{"code": "OV", "meter_id": "M1", "severity": "ALARM"}]
        self.state.update_alarms(alarms)
        snap = self.state.snapshot()
        self.assertEqual(len(snap["alarms"]), 1)
        self.assertEqual(snap["alarms"][0]["code"], "OV")

    def test_set_app_status(self):
        self.state.set_app_status("RUNNING", 2)
        snap = self.state.snapshot()
        self.assertEqual(snap["app_status"], "RUNNING")
        self.assertEqual(snap["meter_count"], 2)

    def test_thread_safety(self):
        """Multiple threads updating simultaneously must not crash."""
        errors = []

        def writer(n):
            try:
                for _ in range(100):
                    self.state.update({f"M{n}": {"kW": float(n)}},
                                      {f"M{n}": "GOOD"}, ts=time.time())
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=writer, args=(i,)) for i in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(errors, [], f"Thread-safety errors: {errors}")


# ---------------------------------------------------------------------------
# RESTApiServer properties (before start)
# ---------------------------------------------------------------------------

class TestRESTApiServerProperties(unittest.TestCase):

    def _make(self, enabled=False, port=18080):
        cfg = {"rest_api": {"enabled": enabled, "port": port,
                            "host": "127.0.0.1", "api_key": ""}}
        return RESTApiServer(cfg)

    def test_not_running_before_start(self):
        srv = self._make()
        self.assertFalse(srv.is_running)

    def test_backend_none_before_start(self):
        srv = self._make()
        self.assertEqual(srv.backend, "none")

    def test_bind_address_format(self):
        srv = self._make(port=18080)
        addr = srv.bind_address
        self.assertIn("18080", addr)

    def test_disabled_server_not_running(self):
        srv = self._make(enabled=False)
        srv.start()   # should be a no-op when disabled
        self.assertFalse(srv.is_running)
        srv.stop()

    def test_update_snapshot_no_crash_when_not_started(self):
        srv = self._make()
        srv.update_snapshot({"M1": {"kW": 100.0}}, {"M1": "GOOD"}, ts=time.time())

    def test_update_alarms_no_crash_when_not_started(self):
        srv = self._make()
        # alarm_engine stub
        class FakeEngine:
            active = {}
            history = []
        srv.update_alarms(FakeEngine())

    def test_reconfigure_no_crash(self):
        srv = self._make(enabled=False)
        new_cfg = {"rest_api": {"enabled": False, "port": 18080,
                                "host": "127.0.0.1", "api_key": ""}}
        srv.reconfigure(new_cfg)  # must not raise


# ---------------------------------------------------------------------------
# FastAPI routes (only if fastapi is available)
# ---------------------------------------------------------------------------

try:
    from fastapi.testclient import TestClient
    # Import the private factory to test routes without binding to a port
    from core.rest_api import _make_fastapi_app, _ApiState as _St
    _FASTAPI_TEST_AVAILABLE = True
except (ImportError, AttributeError):
    _FASTAPI_TEST_AVAILABLE = False


@unittest.skipUnless(_FASTAPI_TEST_AVAILABLE, "fastapi not installed")
class TestFastAPIRoutes(unittest.TestCase):

    def setUp(self):
        state = _St()
        state.update(
            {"M1": {"kW": 100.0, "Vavg": 230.0}, "TOTAL": {"kW": 100.0}},
            {"M1": "GOOD"},
            ts=time.time(),
        )
        state.set_app_status("RUNNING", 1)
        state.update_alarms([])
        self.app = _make_fastapi_app(state, api_key="", cors_origin="*")
        self.client = TestClient(self.app, raise_server_exceptions=True)

    def test_health_200(self):
        r = self.client.get("/health")
        self.assertEqual(r.status_code, 200)
        self.assertIn("ok", r.json().get("status", ""))

    def test_readings_200(self):
        r = self.client.get("/api/v1/readings")
        self.assertEqual(r.status_code, 200)
        data = r.json()
        # Response uses 'sources' key (not 'data') — verify live readings present
        self.assertIn("sources", data)
        self.assertIn("M1", data["sources"])

    def test_status_200(self):
        r = self.client.get("/api/v1/status")
        self.assertEqual(r.status_code, 200)

    def test_alarms_200(self):
        r = self.client.get("/api/v1/alarms")
        self.assertEqual(r.status_code, 200)

    def test_meters_200(self):
        r = self.client.get("/api/v1/meters")
        self.assertEqual(r.status_code, 200)

    def test_auth_required_when_key_set(self):
        """With an API key configured, missing header must return 401."""
        state = _St()
        app_locked = _make_fastapi_app(state, api_key="secret-key", cors_origin="*")
        client = TestClient(app_locked, raise_server_exceptions=False)
        r = client.get("/api/v1/readings")
        self.assertEqual(r.status_code, 401)

    def test_auth_accepted_with_correct_key(self):
        state = _St()
        state.update({"M1": {"kW": 10.0}}, {"M1": "GOOD"}, ts=time.time())
        app_locked = _make_fastapi_app(state, api_key="secret-key", cors_origin="*")
        client = TestClient(app_locked, raise_server_exceptions=True)
        r = client.get("/api/v1/readings", headers={"X-API-Key": "secret-key"})
        self.assertEqual(r.status_code, 200)

    def test_auth_rejected_with_wrong_key(self):
        state = _St()
        app_locked = _make_fastapi_app(state, api_key="secret-key", cors_origin="*")
        client = TestClient(app_locked, raise_server_exceptions=False)
        r = client.get("/api/v1/readings", headers={"X-API-Key": "wrong"})
        self.assertEqual(r.status_code, 401)

    def test_dashboard_no_auth_needed(self):
        state = _St()
        app_locked = _make_fastapi_app(state, api_key="secret-key", cors_origin="*")
        client = TestClient(app_locked, raise_server_exceptions=True)
        r = client.get("/dashboard")
        self.assertEqual(r.status_code, 200)


if __name__ == "__main__":
    unittest.main()
