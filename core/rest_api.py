"""
REST API for SCADA Power Monitor
=================================
Exposes the latest meter readings over HTTP/JSON so external systems
(Node-RED, Grafana, custom scripts, mobile apps) can pull data without
accessing the SCADA host's Modbus bus directly.

Backend
-------
FastAPI + uvicorn (ASGI, multi-threaded worker).  Falls back to the
stdlib HTTPServer if fastapi/uvicorn are not installed, so existing
deployments without the packages are not broken.

Install: pip install fastapi uvicorn

Why FastAPI over stdlib HTTPServer
----------------------------------
The stdlib HTTPServer is single-threaded.  One slow HTTP client (mobile
browser on 3G, a large /history query, a Node-RED flow with a 5 s
timeout) blocks ALL other requests including the alarm-panel poll.
FastAPI + uvicorn uses an async event loop per worker; slow clients
yield the thread — concurrent requests proceed independently.

Endpoints
---------
GET  /api/v1/readings      — latest aggregated snapshot for all sources
GET  /api/v1/meters        — per-meter live values + quality
GET  /api/v1/status        — application state summary
GET  /api/v1/alarms        — active alarm list
GET  /api/v1/history       — last N hot-tier historian rows
GET  /health               — liveness probe (200 OK)
GET  /dashboard  (or /)    — inline web dashboard (no API key needed)

Authentication (optional)
--------------------------
Set rest_api.api_key in config.  If set, all /api/ requests must carry:
  Header:  X-API-Key: <key>

Query-string key passing is intentionally NOT supported — query params
appear in proxy logs, browser history, and server access logs.

Public interface (unchanged from stdlib version)
------------------------------------------------
    api = RESTApiServer(cfg)
    api.start()
    api.update_snapshot(values_by_source, quality_map, ts)
    api.update_alarms(alarm_engine)
    api.update_history(historian, sources, limit)
    api.update_app_status(status, meter_count)
    api.stop()
    api.reconfigure(cfg)
    api.is_running  →  bool
    api.bind_address  →  "host:port"
"""
from __future__ import annotations

import json
import math
import threading
import time
from typing import Any, Dict, Optional

from utils.logger import setup_logger
from utils.security import resolve_secret

logger = setup_logger("rest_api")

# ── version tag written into every response ───────────────────────────────────
_API_VERSION = "1.0"

# ── optional FastAPI + uvicorn import ─────────────────────────────────────────
try:
    import fastapi as _fastapi
    import uvicorn as _uvicorn
    _FASTAPI_AVAILABLE = True
except ImportError:
    _fastapi = None   # type: ignore
    _uvicorn = None   # type: ignore
    _FASTAPI_AVAILABLE = False

# ── stdlib fallback ───────────────────────────────────────────────────────────
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse


# ── inline web dashboard (served at GET /dashboard) ───────────────────────────
_DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>MFM384 Live Dashboard</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4/dist/chart.umd.min.js"></script>
<style>
  *{box-sizing:border-box;margin:0;padding:0}
  body{background:#0d1117;color:#e6edf3;font-family:'Segoe UI',system-ui,sans-serif;font-size:14px}
  header{background:#161b22;padding:12px 20px;border-bottom:1px solid #30363d;
         display:flex;align-items:center;justify-content:space-between}
  header h1{font-size:18px;font-weight:600;letter-spacing:.5px}
  #conn{font-size:12px;padding:3px 10px;border-radius:12px;background:#21262d;color:#8b949e}
  #conn.ok{background:#1a4731;color:#3fb950}
  #conn.err{background:#4b1c1c;color:#f85149}
  #ts{font-size:11px;color:#8b949e}
  .tiles{display:flex;flex-wrap:wrap;gap:12px;padding:16px 20px}
  .tile{background:#161b22;border:1px solid #30363d;border-radius:8px;
        padding:14px 18px;min-width:160px;flex:1}
  .tile .label{font-size:11px;color:#8b949e;text-transform:uppercase;letter-spacing:.6px}
  .tile .val{font-size:28px;font-weight:700;margin-top:4px;color:#58a6ff}
  .tile .unit{font-size:13px;color:#8b949e;margin-left:4px}
  .tile.alarm .val{color:#f85149}
  .tile.warn .val{color:#d29922}
  .tile.good .val{color:#3fb950}
  .charts{display:flex;flex-wrap:wrap;gap:12px;padding:0 20px 20px}
  .chart-box{background:#161b22;border:1px solid #30363d;border-radius:8px;
             padding:14px;flex:1;min-width:300px}
  .chart-box h2{font-size:13px;color:#8b949e;margin-bottom:10px;font-weight:500}
  .footer{text-align:center;padding:10px;color:#484f58;font-size:11px}
  .alarms-box{background:#161b22;border:1px solid #30363d;border-radius:8px;
              margin:0 20px 16px;padding:14px 18px}
  .alarms-box h2{font-size:13px;color:#8b949e;margin-bottom:8px;font-weight:500}
  .alarm-row{display:flex;align-items:center;gap:10px;padding:5px 0;
             border-bottom:1px solid #21262d;font-size:13px}
  .alarm-row:last-child{border-bottom:none}
  .alarm-sev{font-size:11px;font-weight:700;padding:2px 7px;border-radius:10px;min-width:52px;text-align:center}
  .sev-alarm{background:#4b1c1c;color:#f85149}
  .sev-warn{background:#3d2c0a;color:#d29922}
  .alarm-code{font-family:monospace;color:#8b949e;min-width:90px}
  .alarm-meter{color:#58a6ff;min-width:55px}
  .alarm-msg{color:#c9d1d9;flex:1}
  .alarm-ack{font-size:11px;color:#3fb950}
  #alarm-empty{color:#484f58;font-style:italic;font-size:13px}
</style>
</head>
<body>
<header>
  <h1>&#9889; MFM384 SCADA — Live Dashboard</h1>
  <div style="display:flex;gap:12px;align-items:center">
    <span id="ts">—</span>
    <span id="conn">Connecting…</span>
  </div>
</header>

<div class="tiles" id="tiles">
  <div class="tile" id="tile-vavg"><div class="label">Voltage (avg L-N)</div><div class="val" id="v-vavg">—</div><span class="unit">V</span></div>
  <div class="tile" id="tile-iavg"><div class="label">Current (avg)</div><div class="val" id="v-iavg">—</div><span class="unit">A</span></div>
  <div class="tile" id="tile-kw"><div class="label">Active Power</div><div class="val" id="v-kw">—</div><span class="unit">kW</span></div>
  <div class="tile" id="tile-kvar"><div class="label">Reactive Power</div><div class="val" id="v-kvar">—</div><span class="unit">kVAr</span></div>
  <div class="tile" id="tile-kva"><div class="label">Apparent Power</div><div class="val" id="v-kva">—</div><span class="unit">kVA</span></div>
  <div class="tile" id="tile-pf"><div class="label">Power Factor</div><div class="val" id="v-pf">—</div></div>
  <div class="tile" id="tile-hz"><div class="label">Frequency</div><div class="val" id="v-hz">—</div><span class="unit">Hz</span></div>
  <div class="tile" id="tile-kwh"><div class="label">Energy (import)</div><div class="val" id="v-kwh">—</div><span class="unit">kWh</span></div>
</div>

<div class="alarms-box">
  <h2>&#9888; Active Alarms &nbsp;<span id="alarm-count" style="font-size:11px;color:#484f58"></span></h2>
  <div id="alarm-list"><span id="alarm-empty">No active alarms</span></div>
</div>

<div class="charts">
  <div class="chart-box"><h2>Active Power — kW (last 60 readings)</h2><canvas id="chart-kw" height="100"></canvas></div>
  <div class="chart-box"><h2>Voltage Avg L-N — V (last 60 readings)</h2><canvas id="chart-v" height="100"></canvas></div>
</div>
<div class="footer">Auto-refreshes every 5 s &nbsp;|&nbsp; Data from /api/v1/readings and /api/v1/alarms</div>

<script>
const API = '/api/v1/readings';
const MAX_PTS = 60;
const connEl = document.getElementById('conn');
const tsEl   = document.getElementById('ts');

function mkChart(id, label, color) {
  return new Chart(document.getElementById(id).getContext('2d'), {
    type: 'line',
    data: { labels: [], datasets: [{ label, data: [],
      borderColor: color, backgroundColor: color + '22',
      borderWidth: 2, pointRadius: 0, fill: true, tension: 0.3 }] },
    options: {
      animation: false,
      plugins: { legend: { display: false } },
      scales: {
        x: { ticks: { color: '#8b949e', maxTicksLimit: 6 }, grid: { color: '#21262d' } },
        y: { ticks: { color: '#8b949e' }, grid: { color: '#21262d' } }
      }
    }
  });
}
const chartKw = mkChart('chart-kw', 'kW', '#58a6ff');
const chartV  = mkChart('chart-v',  'V',  '#3fb950');

function pushPoint(chart, label, value) {
  chart.data.labels.push(label);
  chart.data.datasets[0].data.push(value);
  if (chart.data.labels.length > MAX_PTS) {
    chart.data.labels.shift();
    chart.data.datasets[0].data.shift();
  }
  chart.update('none');
}

function fmt(v, dp) {
  if (v === null || v === undefined) return '—';
  return typeof v === 'number' ? v.toFixed(dp) : v;
}
function setPF(v) {
  const el = document.getElementById('v-pf');
  const tile = document.getElementById('tile-pf');
  if (v === null || v === undefined) { el.textContent = '—'; return; }
  el.textContent = v.toFixed(3);
  tile.className = 'tile ' + (v >= 0.95 ? 'good' : v >= 0.85 ? 'warn' : 'alarm');
}

function updateTiles(total) {
  document.getElementById('v-vavg').textContent = fmt(total.Vavg_LN ?? total.V_avg_LN ?? total.Vavg, 1);
  document.getElementById('v-iavg').textContent = fmt(total.Iavg ?? total.I_avg, 2);
  document.getElementById('v-kw'  ).textContent = fmt(total.kW   ?? total.P_total, 2);
  document.getElementById('v-kvar').textContent = fmt(total.kVAr ?? total.Q_total, 2);
  document.getElementById('v-kva' ).textContent = fmt(total.kVA  ?? total.S_total, 2);
  document.getElementById('v-hz'  ).textContent = fmt(total.Hz   ?? total.Freq, 2);
  document.getElementById('v-kwh' ).textContent = fmt(total.kWh_import ?? total.kWh, 1);
  setPF(total.PF_avg ?? total.PF ?? total.pf);
}

async function pollAlarms() {
  try {
    const res = await fetch('/api/v1/alarms');
    if (!res.ok) return;
    const data = await res.json();
    const alarms = data.alarms || [];
    const listEl = document.getElementById('alarm-list');
    const cntEl  = document.getElementById('alarm-count');
    if (!alarms.length) {
      listEl.innerHTML = '<span id="alarm-empty">No active alarms</span>';
      cntEl.textContent = '';
      return;
    }
    alarms.sort((a,b) => {
      const band = x => x.severity==='ALARM' ? (x.acknowledged?1:0) : 2;
      return band(a) - band(b) || b.ts_raised - a.ts_raised;
    });
    cntEl.textContent = `(${alarms.length})`;
    listEl.innerHTML = alarms.map(a => {
      const sevCls = a.severity==='ALARM' ? 'sev-alarm' : 'sev-warn';
      const ack = a.acknowledged
        ? `<span class="alarm-ack">&#10003; ${a.acknowledged_by||'ACK'}</span>` : '';
      const fo = a.is_first_out ? '<span style="color:#d29922;font-size:10px"> &#9733;FIRST-OUT</span>' : '';
      const ts = a.ts_raised ? new Date(a.ts_raised*1000).toLocaleTimeString() : '';
      return `<div class="alarm-row">
        <span class="alarm-sev ${sevCls}">${a.severity}</span>
        <span class="alarm-meter">${a.meter_id}</span>
        <span class="alarm-code">${a.code}</span>
        <span class="alarm-msg">${a.message}${fo}</span>
        ${ack}
        <span style="color:#484f58;font-size:11px">${ts}</span>
      </div>`;
    }).join('');
  } catch(e) {}
}

async function poll() {
  try {
    const res = await fetch(API);
    if (!res.ok) throw new Error('HTTP ' + res.status);
    const data = await res.json();
    const total = (data.sources || {}).TOTAL || {};
    const ts = data.ts ? new Date(data.ts * 1000) : new Date();
    const label = ts.toLocaleTimeString();
    updateTiles(total);
    pushPoint(chartKw, label, total.kW ?? total.P_total ?? null);
    pushPoint(chartV,  label, total.Vavg_LN ?? total.V_avg_LN ?? total.Vavg ?? null);
    tsEl.textContent = ts.toLocaleString();
    connEl.textContent = 'LIVE';
    connEl.className = 'ok';
  } catch(e) {
    connEl.textContent = 'OFFLINE';
    connEl.className = 'err';
  }
  pollAlarms();
}
poll();
setInterval(poll, 5000);
</script>
</body>
</html>
"""


def _sanitise(d: Dict[str, Any]) -> Dict[str, Any]:
    """Drop non-finite floats; round finite floats to 4 dp."""
    out: Dict[str, Any] = {}
    for k, v in (d or {}).items():
        if isinstance(v, float):
            if math.isfinite(v):
                out[k] = round(v, 4)
        elif isinstance(v, (int, str, bool)):
            out[k] = v
    return out


# ── shared state (written by UI thread, read by HTTP handler) ─────────────────

class _ApiState:
    """Thread-safe container for the latest snapshot."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._readings: Dict[str, Any] = {}
        self._quality:  Dict[str, str] = {}
        self._ts: float = 0.0
        self._app_status: str = "STARTING"
        self._meter_count: int = 0
        self._alarms: list = []
        self._history_rows: list = []

    def update(
        self,
        values_by_source: Dict[str, Dict[str, Any]],
        quality_map: Dict[str, str],
        ts: float,
    ) -> None:
        with self._lock:
            self._readings = {
                src: _sanitise(vals or {})
                for src, vals in (values_by_source or {}).items()
            }
            self._quality  = dict(quality_map or {})
            self._ts       = float(ts or time.time())

    def set_app_status(self, status: str, meter_count: int) -> None:
        with self._lock:
            self._app_status  = str(status)
            self._meter_count = int(meter_count)

    def update_alarms(self, alarm_list: list) -> None:
        with self._lock:
            self._alarms = list(alarm_list or [])

    def update_history(self, rows: list) -> None:
        with self._lock:
            self._history_rows = list(rows or [])

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "readings":     dict(self._readings),
                "quality":      dict(self._quality),
                "ts":           self._ts,
                "app_status":   self._app_status,
                "meter_count":  self._meter_count,
                "alarms":       list(self._alarms),
                "history_rows": list(self._history_rows),
            }


# ── FastAPI app factory ────────────────────────────────────────────────────────

def _make_fastapi_app(state: _ApiState, api_key: str, cors_origin: str):
    """
    Build and return a FastAPI application bound to *state*.

    All route handlers are synchronous (no async needed — state reads are
    in-memory dict copies behind a mutex, sub-millisecond).  uvicorn runs
    them in a thread pool automatically when defined as plain `def`.

    Auth is handled via a Depends() dependency that reads the X-API-Key
    header.  When api_key is blank, the dependency is a no-op and all
    requests pass through unauthenticated.
    """
    from fastapi import Depends, FastAPI, Header, HTTPException
    from fastapi.middleware.cors import CORSMiddleware
    from fastapi.responses import HTMLResponse

    app = FastAPI(
        title="MFM384 SCADA REST API",
        version=_API_VERSION,
        docs_url=None,    # disable Swagger UI — not needed in production
        redoc_url=None,
    )

    # CORS — allow the configured origin (or * for open LANs)
    origins = [cors_origin] if cors_origin != "*" else ["*"]
    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_methods=["GET", "OPTIONS"],
        allow_headers=["X-API-Key", "Content-Type"],
    )

    # ── auth dependency ───────────────────────────────────────────────────────
    # FastAPI injects Header parameters by name — x_api_key maps to the
    # "x-api-key" / "X-API-Key" header (FastAPI normalises underscores).
    # `alias` makes the mapping explicit and case-insensitive.

    def require_auth(
        x_api_key: Optional[str] = Header(default=None, alias="X-API-Key"),
    ) -> None:
        """Raise 401 if api_key is configured and the header is missing/wrong."""
        if not api_key:
            return   # authentication disabled
        if not x_api_key or x_api_key != api_key:
            raise HTTPException(status_code=401, detail="Unauthorised")

    # Shorthand: routes that require auth declare this as a dependency
    _auth = Depends(require_auth)

    # ── liveness (no auth) ────────────────────────────────────────────────────

    @app.get("/health")
    def health():
        return {"status": "ok", "ts": time.time()}

    # ── dashboard (no auth) ───────────────────────────────────────────────────

    @app.get("/dashboard", response_class=HTMLResponse)
    @app.get("/", response_class=HTMLResponse)
    def dashboard():
        return HTMLResponse(content=_DASHBOARD_HTML, status_code=200)

    # ── readings ──────────────────────────────────────────────────────────────

    @app.get("/api/v1/readings", dependencies=[_auth])
    def readings():
        snap = state.snapshot()
        return {
            "api_version": _API_VERSION,
            "ts":          snap["ts"],
            "sources":     snap["readings"],
            "quality":     snap["quality"],
        }

    # ── meters ────────────────────────────────────────────────────────────────

    @app.get("/api/v1/meters", dependencies=[_auth])
    def meters():
        snap     = state.snapshot()
        rdgs     = snap["readings"]
        quality  = snap["quality"]
        meters_out = {
            src: {"values": vals, "quality": quality.get(src, "UNKNOWN")}
            for src, vals in rdgs.items()
            if src != "TOTAL"
        }
        return {
            "api_version":   _API_VERSION,
            "ts":            snap["ts"],
            "meters":        meters_out,
            "total":         rdgs.get("TOTAL", {}),
            "total_quality": quality.get("TOTAL", "UNKNOWN"),
        }

    # ── status ────────────────────────────────────────────────────────────────

    @app.get("/api/v1/status", dependencies=[_auth])
    def status():
        snap = state.snapshot()
        return {
            "api_version": _API_VERSION,
            "ts":          snap["ts"],
            "app_status":  snap["app_status"],
            "meter_count": snap["meter_count"],
            "data_age_s":  round(time.time() - snap["ts"], 1) if snap["ts"] else None,
            "backend":     "fastapi",
        }

    # ── alarms ────────────────────────────────────────────────────────────────

    @app.get("/api/v1/alarms", dependencies=[_auth])
    def alarms():
        snap = state.snapshot()
        return {
            "api_version": _API_VERSION,
            "ts":          time.time(),
            "count":       len(snap["alarms"]),
            "alarms":      snap["alarms"],
        }

    # ── history ───────────────────────────────────────────────────────────────

    @app.get("/api/v1/history", dependencies=[_auth])
    def history():
        snap = state.snapshot()
        return {
            "api_version": _API_VERSION,
            "ts":          time.time(),
            "count":       len(snap["history_rows"]),
            "rows":        snap["history_rows"],
        }

    return app


# ── stdlib fallback (unchanged from original) ─────────────────────────────────

def _make_handler(state: _ApiState, api_key: str, cors_origin: str):
    """Factory: returns a BaseHTTPRequestHandler subclass bound to *state*."""

    class _Handler(BaseHTTPRequestHandler):

        def log_message(self, fmt, *args):  # noqa: ANN001
            pass

        def log_error(self, fmt, *args):  # noqa: ANN001
            pass

        def _send_json(self, code: int, obj: Any) -> None:
            body = json.dumps(obj, separators=(",", ":")).encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Access-Control-Allow-Origin", cors_origin)
            self.send_header("Access-Control-Allow-Headers", "X-API-Key, Content-Type")
            self.end_headers()
            self.wfile.write(body)

        def _send_html(self, code: int, html: str) -> None:
            body = html.encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _auth_ok(self) -> bool:
            if not api_key:
                return True
            hdr = self.headers.get("X-API-Key", "")
            return bool(hdr and hdr == api_key)

        def do_OPTIONS(self) -> None:  # noqa: N802
            self.send_response(204)
            self.send_header("Access-Control-Allow-Origin", cors_origin)
            self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
            self.send_header("Access-Control-Allow-Headers", "X-API-Key, Content-Type")
            self.end_headers()

        def do_GET(self) -> None:  # noqa: N802
            path = urlparse(self.path).path.rstrip("/")
            try:
                if path == "/health":
                    self._send_json(200, {"status": "ok", "ts": time.time()})
                    return
                if path in ("/dashboard", "/"):
                    self._send_html(200, _DASHBOARD_HTML)
                    return
                if not self._auth_ok():
                    self._send_json(401, {"error": "Unauthorised"})
                    return
                snap = state.snapshot()
                if path == "/api/v1/readings":
                    self._send_json(200, {"api_version": _API_VERSION,
                                          "ts": snap["ts"],
                                          "sources": snap["readings"],
                                          "quality": snap["quality"]})
                elif path == "/api/v1/meters":
                    readings = snap["readings"]
                    quality  = snap["quality"]
                    meters_out = {
                        src: {"values": vals, "quality": quality.get(src, "UNKNOWN")}
                        for src, vals in readings.items() if src != "TOTAL"
                    }
                    self._send_json(200, {"api_version": _API_VERSION,
                                          "ts": snap["ts"],
                                          "meters": meters_out,
                                          "total": readings.get("TOTAL", {}),
                                          "total_quality": quality.get("TOTAL", "UNKNOWN")})
                elif path == "/api/v1/status":
                    self._send_json(200, {"api_version": _API_VERSION,
                                          "ts": snap["ts"],
                                          "app_status": snap["app_status"],
                                          "meter_count": snap["meter_count"],
                                          "data_age_s": round(time.time() - snap["ts"], 1)
                                              if snap["ts"] else None,
                                          "backend": "stdlib"})
                elif path == "/api/v1/alarms":
                    self._send_json(200, {"api_version": _API_VERSION,
                                          "ts": time.time(),
                                          "count": len(snap["alarms"]),
                                          "alarms": snap["alarms"]})
                elif path == "/api/v1/history":
                    self._send_json(200, {"api_version": _API_VERSION,
                                          "ts": time.time(),
                                          "count": len(snap["history_rows"]),
                                          "rows": snap["history_rows"]})
                else:
                    self._send_json(404, {"error": "Not found", "path": path})
            except Exception as exc:
                logger.warning("[REST] handler error: %s", exc)
                try:
                    self._send_json(500, {"error": "Internal server error"})
                except Exception:
                    pass

    return _Handler


# ── public API ─────────────────────────────────────────────────────────────────

class RESTApiServer:
    """
    Thread-safe REST API server.

    Uses FastAPI + uvicorn when available; falls back to stdlib HTTPServer.
    The HTTP server runs in a daemon thread.  update_snapshot() is safe to
    call from the UI thread — all shared access goes through _ApiState's lock.

    Public interface is identical regardless of backend.
    """

    def __init__(self, cfg: dict) -> None:
        self._cfg    = cfg
        self._state  = _ApiState()
        # FastAPI backend
        self._uv_server: Optional[Any] = None     # uvicorn.Server instance
        self._uv_thread: Optional[threading.Thread] = None
        # Stdlib fallback backend
        self._http_server: Optional[HTTPServer] = None
        self._http_thread: Optional[threading.Thread] = None
        self._running = False

    # ── Properties ────────────────────────────────────────────────────────────

    @property
    def enabled(self) -> bool:
        return bool((self._cfg.get("rest_api") or {}).get("enabled", False))

    @property
    def is_running(self) -> bool:
        if self._uv_thread is not None:
            return self._running and self._uv_thread.is_alive()
        if self._http_thread is not None:
            return self._running and self._http_thread.is_alive()
        return False

    @property
    def bind_address(self) -> str:
        rc = self._cfg.get("rest_api") or {}
        return f"{rc.get('host', '127.0.0.1')}:{rc.get('port', 8080)}"

    @property
    def backend(self) -> str:
        """'fastapi' | 'stdlib' | 'none'"""
        if self._uv_thread is not None and self._running:
            return "fastapi"
        if self._http_thread is not None and self._running:
            return "stdlib"
        return "none"

    def _rc(self, key: str, default=None):
        return (self._cfg.get("rest_api") or {}).get(key, default)

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def start(self) -> bool:
        """Start the HTTP server in a background thread.  Returns True if started."""
        if not self.enabled:
            logger.info("[REST] disabled in config")
            return False
        if self._running:
            return True

        host        = str(self._rc("host", "127.0.0.1")).strip()
        port        = int(self._rc("port", 8080))
        api_key     = resolve_secret(
            str(self._rc("api_key", "") or "").strip(),
            env_var=str(self._rc("api_key_env", "") or ""),
            default_env_var="SCADA_REST_API_KEY",
            keyring_service=str(self._rc("api_key_keyring_service", "") or ""),
            keyring_username=str(self._rc("api_key_keyring_username", "") or ""),
        ).strip()
        cors_origin = str(self._rc("cors_origin", "http://127.0.0.1") or "http://127.0.0.1").strip()

        if _FASTAPI_AVAILABLE:
            return self._start_fastapi(host, port, api_key, cors_origin)
        else:
            logger.warning(
                "[REST] fastapi/uvicorn not installed — using stdlib HTTPServer. "
                "Install with: pip install fastapi uvicorn"
            )
            return self._start_stdlib(host, port, api_key, cors_origin)

    def stop(self) -> None:
        """Shut down the HTTP server gracefully."""
        self._running = False
        # FastAPI/uvicorn path
        if self._uv_server is not None:
            try:
                self._uv_server.should_exit = True
            except Exception:
                pass
            self._uv_server = None
        self._uv_thread = None
        # Stdlib fallback path
        if self._http_server is not None:
            try:
                self._http_server.shutdown()
            except Exception:
                pass
            self._http_server = None
        self._http_thread = None
        logger.info("[REST] stopped")

    def reconfigure(self, cfg: dict) -> None:
        """Apply updated config; restarts server if bind address changed."""
        old_host = self._rc("host", "127.0.0.1")
        old_port = int(self._rc("port", 8080))
        self._cfg = cfg
        new_host = str((cfg.get("rest_api") or {}).get("host", "127.0.0.1"))
        new_port = int((cfg.get("rest_api") or {}).get("port", 8080))
        if old_host != new_host or old_port != new_port or not self._running:
            self.stop()
            if self.enabled:
                self.start()

    # ── Data feed (UI thread) ─────────────────────────────────────────────────

    def update_snapshot(
        self,
        values_by_source: Dict[str, Dict[str, Any]],
        quality_map: Dict[str, str],
        ts: Optional[float] = None,
    ) -> None:
        """Push the latest readings into the shared state (UI thread safe)."""
        self._state.update(values_by_source, quality_map, ts or time.time())

    def update_app_status(self, status: str, meter_count: int) -> None:
        """Update application-level status visible via /api/v1/status."""
        self._state.set_app_status(status, meter_count)

    def update_alarms(self, alarm_engine) -> None:
        """Push current active alarm list from alarm_engine into the REST state."""
        try:
            active = getattr(alarm_engine, "active", {}) or {}
            alarm_list = []
            for ev in active.values():
                alarm_list.append({
                    "meter_id":        str(getattr(ev, "meter_id", "")),
                    "code":            str(getattr(ev, "code", "")),
                    "severity":        str(getattr(ev, "severity", "WARN")).upper(),
                    "message":         str(getattr(ev, "message", "")),
                    "active":          bool(getattr(ev, "active", True)),
                    "acknowledged":    bool(getattr(ev, "acknowledged", False)),
                    "acknowledged_by": str(getattr(ev, "acknowledged_by", "") or ""),
                    "ts_raised":       float(getattr(ev, "ts", 0.0)),
                    "ts_acked":        float(getattr(ev, "acknowledged_at", 0.0) or 0.0),
                    "is_first_out":    bool(getattr(ev, "is_first_out", False)),
                })
            self._state.update_alarms(alarm_list)
        except Exception:
            pass

    def update_history(self, historian, sources=None, limit: int = 100) -> None:
        """Push last *limit* historian rows to the /api/v1/history endpoint."""
        try:
            if historian is None or not hasattr(historian, "query_recent"):
                return
            rows = historian.query_recent(sources=sources, limit=limit)
            self._state.update_history(rows)
        except Exception:
            pass

    # ── Internal — FastAPI backend ────────────────────────────────────────────

    def _start_fastapi(self, host: str, port: int, api_key: str, cors_origin: str) -> bool:
        """Start uvicorn in a daemon thread with the FastAPI app."""
        app = _make_fastapi_app(self._state, api_key, cors_origin)

        config = _uvicorn.Config(
            app=app,
            host=host,
            port=port,
            log_level="warning",   # suppress uvicorn access logs (we log ourselves)
            access_log=False,
            loop="asyncio",        # explicit — avoids uvloop dependency on Windows
        )
        server = _uvicorn.Server(config=config)
        # Prevent uvicorn from installing its own signal handlers (we're not PID 1)
        server.install_signal_handlers = lambda: None  # type: ignore[method-assign]

        self._uv_server = server
        self._running   = True

        t = threading.Thread(target=server.run, name="rest-api-fastapi", daemon=True)
        t.start()
        self._uv_thread = t

        logger.info(
            "[REST] FastAPI/uvicorn listening on http://%s:%d/  (api_key=%s)",
            host, port, "set" if api_key else "none",
        )
        return True

    # ── Internal — stdlib fallback ────────────────────────────────────────────

    def _start_stdlib(self, host: str, port: int, api_key: str, cors_origin: str) -> bool:
        """Start the stdlib HTTPServer in a daemon thread (single-threaded fallback)."""
        handler_cls = _make_handler(self._state, api_key, cors_origin)
        try:
            server = HTTPServer((host, port), handler_cls)
        except OSError as exc:
            logger.warning("[REST] Could not bind %s:%d — %s", host, port, exc)
            return False

        self._http_server = server
        self._running     = True

        t = threading.Thread(target=self._serve_stdlib, name="rest-api-stdlib", daemon=True)
        t.start()
        self._http_thread = t

        logger.info("[REST] stdlib HTTPServer listening on http://%s:%d/  (api_key=%s)",
                    host, port, "set" if api_key else "none")
        return True

    def _serve_stdlib(self) -> None:
        """Runs in background daemon thread (stdlib fallback)."""
        try:
            assert self._http_server is not None
            self._http_server.serve_forever()
        except Exception as exc:
            if self._running:
                logger.warning("[REST] stdlib server exited unexpectedly: %s", exc)
        finally:
            self._running = False
