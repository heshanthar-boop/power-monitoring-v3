"""
MQTT Publisher for SCADA Power Monitor
========================================
Publishes live meter readings to an MQTT broker so downstream systems
(Node-RED, cloud dashboards, mobile apps, historians) can consume them
without polling the SCADA host directly.

Protocol: MQTT v3.1.1 / v5.0  (OASIS Standard)
Library:  paho-mqtt  (Eclipse Paho, EPL-2.0)

Topic structure
---------------
{prefix}/live/{source}/{key}
    Payload: {"v": 125.5, "ts": 1711526400.0, "q": "GOOD"}

{prefix}/summary
    Payload: {"ts": 1711526400.0,
              "TOTAL": {"kW": 125.5, "kVA": 140.2, ...},
              "M1":    {"kW":  65.0, ...}}

{prefix}/status
    Payload: {"state": "CONNECTED", "meters": 2, "ts": ...}

Usage
-----
    pub = MQTTPublisher(cfg)
    pub.start()
    # each UI tick:
    pub.publish_snapshot({"TOTAL": {...}, "M1": {...}}, quality_map, ts)
    # on exit:
    pub.stop()

Install paho-mqtt:  pip install paho-mqtt
"""
from __future__ import annotations

import json
import math
import socket
import threading
import time
import zlib
from typing import Any, Dict, Optional

from utils.logger import setup_logger
from utils.security import resolve_secret

logger = setup_logger("mqtt")

# ── paho-mqtt optional import ─────────────────────────────────────────────────
try:
    import paho.mqtt.client as _mqtt
    _PAHO_AVAILABLE = True
except ImportError:
    _mqtt = None          # type: ignore
    _PAHO_AVAILABLE = False


def _sanitise(values: Dict[str, Any]) -> Dict[str, Any]:
    """Drop non-finite floats; keep ints and strings intact."""
    out: Dict[str, Any] = {}
    for k, v in values.items():
        if isinstance(v, float):
            if math.isfinite(v):
                out[k] = round(v, 4)
        elif isinstance(v, (int, str)):
            out[k] = v
    return out


class MQTTPublisher:
    """
    Thread-safe MQTT publisher.

    The paho network loop runs in a daemon thread (loop_start).
    publish_snapshot() may be called from the UI thread.

    Gracefully handles:
    • paho-mqtt not installed  → status = "UNAVAILABLE"
    • broker unreachable       → status = "CONNECTING" with exponential backoff
    • mid-flight disconnects   → paho auto-reconnect (on_disconnect → reconnect)
    """

    STATE_DISABLED     = "DISABLED"
    STATE_UNAVAILABLE  = "UNAVAILABLE"   # paho not installed
    STATE_CONNECTING   = "CONNECTING"
    STATE_CONNECTED    = "CONNECTED"
    STATE_ERROR        = "ERROR"

    def __init__(self, cfg: dict) -> None:
        self._cfg = cfg
        self._client: Optional[Any] = None
        self._lock   = threading.Lock()

        self._state       = self.STATE_DISABLED
        self._last_error  = ""
        self._publish_count   = 0
        self._last_pub_ts     = 0.0
        self._next_pub_ts     = 0.0
        # Bandwidth telemetry (bytes uncompressed vs bytes sent)
        self._bytes_raw_total  = 0
        self._bytes_sent_total = 0

        # Backoff for reconnect attempts
        self._backoff_sec  = 2.0
        self._max_backoff  = 60.0
        self._last_connect_attempt = 0.0

    # ── Properties ────────────────────────────────────────────────────────────

    @property
    def enabled(self) -> bool:
        return bool((self._cfg.get("mqtt") or {}).get("enabled", False))

    @property
    def status(self) -> str:
        return self._state

    @property
    def last_error(self) -> str:
        return self._last_error

    @property
    def publish_count(self) -> int:
        return self._publish_count

    @property
    def is_paho_available(self) -> bool:
        return _PAHO_AVAILABLE

    @property
    def bandwidth_mode(self) -> str:
        return str(self._mc("bandwidth_mode", "normal")).lower()

    @property
    def bytes_raw_total(self) -> int:
        """Cumulative uncompressed payload bytes this session."""
        return self._bytes_raw_total

    @property
    def bytes_sent_total(self) -> int:
        """Cumulative compressed/actual bytes sent this session."""
        return self._bytes_sent_total

    @property
    def compression_ratio(self) -> Optional[float]:
        """Compression ratio (0–1, lower = more compressed). None if no data."""
        if self._bytes_raw_total == 0:
            return None
        return round(self._bytes_sent_total / self._bytes_raw_total, 3)

    def _mc(self, key: str, default=None):
        """Read a value from cfg['mqtt']."""
        return (self._cfg.get("mqtt") or {}).get(key, default)

    def _effective_interval(self) -> float:
        """
        Return the publish interval to enforce.

        In "gprs" bandwidth mode, applies a minimum interval floor so a
        misconfigured short publish_interval_sec does not saturate a
        metered mobile data link.
        """
        interval = float(self._mc("publish_interval_sec", 5))
        if self.bandwidth_mode == "gprs":
            floor = float(self._mc("min_publish_interval_sec", 60))
            interval = max(interval, floor)
        return max(interval, 1.0)   # hard minimum 1 s

    def _encode_payload(self, raw_json: str) -> tuple[bytes, bool]:
        """
        Encode a JSON string for publishing.

        Returns (payload_bytes, compressed).
        Compresses with zlib when cfg compress_payload=True AND the string
        is large enough that compression saves space (≥ 64 bytes raw).
        Small payloads are never compressed — overhead exceeds savings.
        """
        raw = raw_json.encode("utf-8")
        if bool(self._mc("compress_payload", False)) and len(raw) >= 64:
            compressed = zlib.compress(raw, level=6)
            # Only use compression if it actually saves bytes
            if len(compressed) < len(raw):
                return compressed, True
        return raw, False

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def start(self) -> None:
        """Connect to broker and start background network loop."""
        if not self.enabled:
            self._state = self.STATE_DISABLED
            return
        if not _PAHO_AVAILABLE:
            self._state = self.STATE_UNAVAILABLE
            self._last_error = "paho-mqtt not installed.  Run: pip install paho-mqtt"
            logger.warning("[MQTT] paho-mqtt not available")
            return
        self._connect()

    def stop(self) -> None:
        """Disconnect gracefully and stop the paho loop."""
        self._state = self.STATE_DISABLED
        with self._lock:
            if self._client is not None:
                try:
                    self._client.loop_stop()
                    self._client.disconnect()
                except Exception:
                    pass
                self._client = None

    def reconfigure(self, cfg: dict) -> None:
        """Apply updated config dict; reconnects if broker settings changed."""
        old_broker = (self._cfg.get("mqtt") or {}).get("broker_host", "")
        old_port   = (self._cfg.get("mqtt") or {}).get("broker_port", 1883)
        self._cfg  = cfg
        new_broker = self._mc("broker_host", "")
        new_port   = self._mc("broker_port", 1883)
        if old_broker != new_broker or old_port != new_port:
            self.stop()
            if self.enabled:
                self.start()

    # ── Publish ───────────────────────────────────────────────────────────────

    def publish_snapshot(
        self,
        values_by_source: Dict[str, Dict[str, Any]],
        quality_map: Dict[str, str],
        ts: Optional[float] = None,
        force: bool = False,
    ) -> bool:
        """
        Publish live readings to the MQTT broker.

        Called from the UI tick.  Returns True if publish was attempted.
        Silently skips when disabled / not yet connected.

        Parameters
        ----------
        values_by_source : dict
            {source_id: {param_key: value, ...}}
        quality_map : dict
            {source_id: "GOOD" | "STALE" | "COMM_LOST"}
        ts : float, optional
            Epoch timestamp.  Defaults to time.time().
        """
        if not self.enabled:
            return False
        if self._state not in (self.STATE_CONNECTED,):
            # Try reconnect with backoff
            self._maybe_reconnect()
            return False

        if ts is None:
            ts = time.time()

        interval = self._effective_interval()
        if not force:
            if ts < self._next_pub_ts:
                return False
            self._next_pub_ts = ts + interval

        prefix           = str(self._mc("topic_prefix", "power_monitor")).strip("/")
        qos              = int(self._mc("qos", 1))
        retain           = bool(self._mc("retain", False))
        summary_only     = bool(self._mc("publish_summary_only", False))

        published  = 0
        raw_bytes  = 0
        sent_bytes = 0

        with self._lock:
            if self._client is None:
                return False
            client = self._client

            try:
                # ── Per-parameter topics (skipped in summary_only / gprs mode) ──
                if not summary_only:
                    for source, vals in (values_by_source or {}).items():
                        q = (quality_map or {}).get(source, "GOOD")
                        clean = _sanitise(vals or {})
                        for key, value in clean.items():
                            topic   = f"{prefix}/live/{source}/{key}"
                            # Per-key payloads are small — always plain JSON
                            raw_json = json.dumps({"v": value, "ts": ts, "q": q},
                                                  separators=(",", ":"))
                            raw_b = raw_json.encode("utf-8")
                            client.publish(topic, raw_b, qos=qos, retain=retain)
                            raw_bytes  += len(raw_b)
                            sent_bytes += len(raw_b)
                            published  += 1

                # ── Summary topic (always published; optionally compressed) ────
                summary_sources = {
                    src: _sanitise(v or {})
                    for src, v in (values_by_source or {}).items()
                }
                summary_json = json.dumps(
                    {"ts": ts, **summary_sources}, separators=(",", ":")
                )
                summary_payload, compressed = self._encode_payload(summary_json)
                raw_bytes  += len(summary_json.encode("utf-8"))
                sent_bytes += len(summary_payload)

                summary_topic = f"{prefix}/summary"
                # paho publish accepts bytes directly; content-type hint via topic suffix
                # Compressed topic: prefix/summary.z so subscribers can detect it.
                if compressed:
                    summary_topic = f"{prefix}/summary.z"
                client.publish(summary_topic, summary_payload, qos=qos, retain=retain)

            except Exception as exc:
                logger.warning("[MQTT] publish failed: %s", exc)
                self._last_error = str(exc)
                return False

        self._publish_count    += max(published, 1)
        self._last_pub_ts       = ts
        self._bytes_raw_total  += raw_bytes
        self._bytes_sent_total += sent_bytes

        if self.bandwidth_mode == "gprs" and raw_bytes > 0:
            saving_pct = (1.0 - sent_bytes / raw_bytes) * 100.0
            logger.debug(
                "[MQTT][gprs] published  raw=%d B  sent=%d B  saving=%.0f%%  interval=%.0fs",
                raw_bytes, sent_bytes, saving_pct, interval,
            )

        return True

    def publish_alarm_event(
        self,
        meter_id,
        code: str,
        event: str,
        severity: str,
        message: str,
        ts: Optional[float] = None,
    ) -> None:
        """
        Publish a single alarm lifecycle event to {prefix}/alarms.

        event: "RAISE" | "CLEAR" | "ACK"

        Payload:
          {"meter_id": "M1", "code": "UV", "event": "RAISE",
           "severity": "ALARM", "message": "...", "ts": 1711526400.0}

        QoS 1, retain=False — alarms are events, not state snapshots.
        A separate /alarms/active topic (retained, updated on raise/clear)
        carries the current active set for subscribers that connect late.
        """
        if self._state != self.STATE_CONNECTED:
            return
        try:
            prefix = str(self._mc("topic_prefix", "power_monitor")).strip("/")
            raw_json = json.dumps({
                "meter_id": str(meter_id),
                "code":     str(code or ""),
                "event":    str(event or "").upper(),
                "severity": str(severity or "").upper(),
                "message":  str(message or ""),
                "ts":       float(ts or time.time()),
            }, separators=(",", ":"))
            payload, compressed = self._encode_payload(raw_json)
            topic = f"{prefix}/alarms" + (".z" if compressed else "")
            with self._lock:
                if self._client:
                    # Event stream (non-retained) — transient alarm transitions
                    self._client.publish(topic, payload, qos=1, retain=False)
        except Exception as exc:
            logger.warning("[MQTT] alarm publish failed: %s", exc)

    def publish_app_status(self, state_str: str, meter_count: int) -> None:
        """Publish application status to the status topic."""
        if self._state != self.STATE_CONNECTED:
            return
        prefix = str(self._mc("topic_prefix", "power_monitor")).strip("/")
        payload = json.dumps({
            "state":  state_str,
            "meters": meter_count,
            "ts":     time.time(),
        }, separators=(",", ":"))
        try:
            with self._lock:
                if self._client:
                    self._client.publish(f"{prefix}/status", payload, qos=0)
        except Exception:
            pass

    # ── Internal ──────────────────────────────────────────────────────────────

    def _connect(self) -> None:
        """Create paho client and initiate connection."""
        broker = str(self._mc("broker_host", "localhost")).strip()
        port   = int(self._mc("broker_port", 1883))

        if not broker:
            self._state      = self.STATE_ERROR
            self._last_error = "No MQTT broker host configured."
            return

        # Unique client ID (avoids "Client identifier not valid" on some brokers)
        client_id = str(self._mc("client_id", "") or "").strip()
        if not client_id:
            try:
                host_part = socket.gethostname()[:8].replace("-", "")
            except Exception:
                host_part = "pm"
            client_id = f"pm_{host_part}_{int(time.time()) % 100000}"

        try:
            client = _mqtt.Client(client_id=client_id, clean_session=True)
        except TypeError:
            # paho-mqtt ≥ 2.0 changed the constructor signature
            client = _mqtt.Client(callback_api_version=_mqtt.CallbackAPIVersion.VERSION1,
                                  client_id=client_id, clean_session=True)  # type: ignore

        username = str(self._mc("username", "") or "")
        password = resolve_secret(
            str(self._mc("password", "") or ""),
            env_var=str(self._mc("password_env", "") or ""),
            default_env_var="SCADA_MQTT_PASSWORD",
            keyring_service=str(self._mc("password_keyring_service", "") or ""),
            keyring_username=str(self._mc("password_keyring_username", "") or ""),
        )
        if username:
            client.username_pw_set(username, password or None)

        # TLS (optional)
        if bool(self._mc("tls_enabled", False)):
            ca_cert = str(self._mc("tls_ca_cert", "") or "").strip() or None
            try:
                client.tls_set(ca_certs=ca_cert)
                logger.info("[MQTT] TLS enabled")
            except Exception as exc:
                logger.warning("[MQTT] TLS setup failed: %s", exc)

        client.on_connect    = self._on_connect
        client.on_disconnect = self._on_disconnect

        with self._lock:
            if self._client is not None:
                try:
                    self._client.loop_stop()
                    self._client.disconnect()
                except Exception:
                    pass
            self._client = client

        self._state = self.STATE_CONNECTING
        self._last_connect_attempt = time.time()

        try:
            client.connect_async(broker, port, keepalive=60)
            client.loop_start()
            logger.info("[MQTT] connecting to %s:%d  client_id=%s", broker, port, client_id)
        except Exception as exc:
            self._state      = self.STATE_ERROR
            self._last_error = f"connect_async failed: {exc}"
            logger.warning("[MQTT] connection failed: %s", exc)

    def _maybe_reconnect(self) -> None:
        """Attempt reconnect with exponential backoff."""
        now = time.time()
        if now - self._last_connect_attempt < self._backoff_sec:
            return
        self._backoff_sec = min(self._backoff_sec * 2, self._max_backoff)
        self._connect()

    def _on_connect(self, client, userdata, flags, rc) -> None:  # noqa: ARG002
        if rc == 0:
            self._state      = self.STATE_CONNECTED
            self._last_error = ""
            self._backoff_sec = 2.0  # reset backoff on success
            logger.info("[MQTT] connected  rc=%d", rc)
        else:
            codes = {
                1: "Incorrect protocol version",
                2: "Invalid client identifier",
                3: "Server unavailable",
                4: "Bad username or password",
                5: "Not authorised",
            }
            msg = codes.get(rc, f"rc={rc}")
            self._state      = self.STATE_ERROR
            self._last_error = f"Broker refused: {msg}"
            logger.warning("[MQTT] connect refused: %s", self._last_error)

    def _on_disconnect(self, client, userdata, rc) -> None:  # noqa: ARG002
        if self._state == self.STATE_DISABLED:
            return  # intentional stop
        self._state      = self.STATE_CONNECTING
        self._last_error = f"Disconnected (rc={rc}) — reconnecting"
        logger.warning("[MQTT] disconnected rc=%d — paho will auto-reconnect", rc)
