"""
SMS / WhatsApp Alert Service
============================
Sends critical alarm notifications via SMS or WhatsApp when a new ALARM-severity
event fires in the alarm engine.

Supported providers
-------------------
twilio      — Twilio REST API (global, also handles WhatsApp via twilio sandbox)
             pip install twilio
             https://www.twilio.com/

dialog      — Dialog Axiata SMS API (LK)
             HTTP POST to https://www.dialog.lk/sms/api/  (Dialog Business)
             No extra pip install needed — uses stdlib urllib.

mobitel     — Mobitel / SLT-Mobitel SMS Push (LK)
             HTTP POST to https://smsapi.mobitel.lk/smpp/  (Mobitel Enterprise)
             No extra pip install needed — uses stdlib urllib.

generic_http — Custom HTTP POST.  Point at any REST SMS gateway.
             Useful for local GSM gateways (Waveshare, Multitech) on the LAN.

Design
------
• All sends are fire-and-forget daemon threads — never block alarm evaluation.
• Rate limiting: one SMS per (meter_id, code) per cooldown_min (default 60 min).
  Prevents SMS flooding during a sustained fault (e.g. comm loss for 8 h).
• ALARM-only by default.  WARN messages are NOT sent to avoid noise.
• Recipients list: plain international format numbers, e.g. "+94771234567".
  WhatsApp: same number but prefixed "whatsapp:" — handled by Twilio.
• Message template is configurable.  Sane default included.
• Sensitive credentials are resolved via utils.security.resolve_secret
  (supports env-var injection so plaintext doesn't sit in config.json).
"""
from __future__ import annotations

import json
import threading
import time
import urllib.request
import urllib.parse
import urllib.error
from typing import Any, Dict, List, Optional

from utils.logger import setup_logger
from utils.security import resolve_secret

logger = setup_logger("sms_service")

# ── Twilio optional import ────────────────────────────────────────────────────
try:
    from twilio.rest import Client as _TwilioClient
    _TWILIO_AVAILABLE = True
except ImportError:
    _TwilioClient = None  # type: ignore
    _TWILIO_AVAILABLE = False


# ── Default message template ─────────────────────────────────────────────────
_DEFAULT_TEMPLATE = (
    "[{site}] ALARM: {code} on {meter}\n"
    "{message}\n"
    "{ts}"
)


def _fmt_message(template: str, site: str, meter_id: str, meter_name: str,
                 code: str, message: str) -> str:
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    return template.format(
        site=site or "SCADA",
        meter=meter_name or meter_id,
        meter_id=meter_id,
        code=code,
        message=message,
        ts=ts,
    )


# ── Provider implementations ──────────────────────────────────────────────────

def _send_twilio(cfg: dict, to_numbers: List[str], body: str) -> None:
    """Send via Twilio REST API.  Handles both SMS and WhatsApp."""
    if not _TWILIO_AVAILABLE:
        logger.error("[SMS] Twilio not installed.  Run: pip install twilio")
        return

    account_sid = resolve_secret(cfg.get("account_sid", ""))
    auth_token  = resolve_secret(cfg.get("auth_token", ""))
    from_number = cfg.get("from_number", "")

    if not (account_sid and auth_token and from_number):
        logger.warning("[SMS] Twilio: account_sid / auth_token / from_number not set")
        return

    try:
        client = _TwilioClient(account_sid, auth_token)
    except Exception as exc:
        logger.error("[SMS] Twilio client init failed: %s", exc)
        return

    for to in to_numbers:
        try:
            client.messages.create(body=body, from_=from_number, to=to)
            logger.info("[SMS] Twilio sent → %s", _mask_number(to))
        except Exception as exc:
            logger.error("[SMS] Twilio send to %s failed: %s", _mask_number(to), exc)


def _send_dialog(cfg: dict, to_numbers: List[str], body: str) -> None:
    """Send via Dialog Axiata SMS HTTP API (Sri Lanka).

    API endpoint: POST https://www.dialog.lk/sms/api/
    Required params: user, password, smsto, smstext, smsfrom
    Dialog Business account required.  Contact: Dialog Enterprise (1678).
    """
    endpoint = cfg.get("api_url", "https://www.dialog.lk/sms/api/")
    user     = resolve_secret(cfg.get("username", ""))
    password = resolve_secret(cfg.get("password", ""))
    sender   = cfg.get("sender_id", "SCADA")

    if not (user and password):
        logger.warning("[SMS] Dialog: username / password not configured")
        return

    for to in to_numbers:
        params = urllib.parse.urlencode({
            "user":     user,
            "password": password,
            "smsto":    to.lstrip("+"),   # Dialog expects local or intl without +
            "smstext":  body[:160],       # SMS length limit
            "smsfrom":  sender,
        }).encode()
        try:
            req = urllib.request.Request(endpoint, data=params, method="POST")
            with urllib.request.urlopen(req, timeout=15) as resp:
                resp_body = resp.read().decode(errors="replace")
            logger.info("[SMS] Dialog sent → %s  resp=%s", _mask_number(to), resp_body[:80])
        except urllib.error.HTTPError as exc:
            logger.error("[SMS] Dialog HTTP %s for %s: %s", exc.code, _mask_number(to), exc)
        except Exception as exc:
            logger.error("[SMS] Dialog send to %s failed: %s", _mask_number(to), exc)


def _send_mobitel(cfg: dict, to_numbers: List[str], body: str) -> None:
    """Send via SLT-Mobitel SMS Push API (Sri Lanka).

    Endpoint: POST https://smsapi.mobitel.lk/smpp/  (Enterprise)
    Required: username, password, sourceAdd, destinationAdd, message
    Mobitel Enterprise account required.  Contact: Mobitel Enterprise (mobitel.lk).
    """
    endpoint = cfg.get("api_url", "https://smsapi.mobitel.lk/smpp/")
    username = resolve_secret(cfg.get("username", ""))
    password = resolve_secret(cfg.get("password", ""))
    source   = cfg.get("sender_id", "SCADA")

    if not (username and password):
        logger.warning("[SMS] Mobitel: username / password not configured")
        return

    for to in to_numbers:
        payload = json.dumps({
            "username":       username,
            "password":       password,
            "sourceAdd":      source,
            "destinationAdd": to.lstrip("+"),
            "message":        body[:160],
        }).encode()
        try:
            req = urllib.request.Request(
                endpoint, data=payload, method="POST",
                headers={"Content-Type": "application/json"},
            )
            with urllib.request.urlopen(req, timeout=15) as resp:
                resp_body = resp.read().decode(errors="replace")
            logger.info("[SMS] Mobitel sent → %s  resp=%s", _mask_number(to), resp_body[:80])
        except urllib.error.HTTPError as exc:
            logger.error("[SMS] Mobitel HTTP %s for %s: %s", exc.code, _mask_number(to), exc)
        except Exception as exc:
            logger.error("[SMS] Mobitel send to %s failed: %s", _mask_number(to), exc)


def _send_generic_http(cfg: dict, to_numbers: List[str], body: str) -> None:
    """Generic HTTP POST gateway.

    Useful for local GSM modems (Waveshare, Multitech) or any REST SMS API.
    Sends one POST per recipient.  JSON body with {to, message} plus any
    extra_params from config.

    Config keys:
        api_url         — required
        method          — GET or POST (default POST)
        body_template   — JSON template string; {to} and {message} substituted
        headers         — dict of extra HTTP headers
        api_key_header  — header name for API key (e.g. "X-Api-Key")
        api_key         — key value
    """
    endpoint = cfg.get("api_url", "")
    if not endpoint:
        logger.warning("[SMS] generic_http: api_url not set")
        return

    method       = str(cfg.get("method", "POST")).upper()
    body_tpl     = cfg.get("body_template",
                           '{{"to": "{to}", "message": "{message}"}}')
    extra_headers: dict = cfg.get("headers", {})
    api_key_hdr  = cfg.get("api_key_header", "")
    api_key_val  = resolve_secret(cfg.get("api_key", ""))

    for to in to_numbers:
        # Build body
        payload_str = body_tpl.replace("{to}", to).replace(
            "{message}", body.replace('"', '\\"')
        )
        try:
            payload_bytes = payload_str.encode()
        except Exception:
            payload_bytes = b""

        try:
            headers = {"Content-Type": "application/json"}
            headers.update(extra_headers)
            if api_key_hdr and api_key_val:
                headers[api_key_hdr] = api_key_val

            req = urllib.request.Request(
                endpoint, data=payload_bytes if method == "POST" else None,
                headers=headers, method=method,
            )
            with urllib.request.urlopen(req, timeout=15) as resp:
                resp_body = resp.read().decode(errors="replace")
            logger.info("[SMS] generic_http sent → %s  resp=%s",
                        _mask_number(to), resp_body[:80])
        except urllib.error.HTTPError as exc:
            logger.error("[SMS] generic_http HTTP %s for %s: %s",
                         exc.code, _mask_number(to), exc)
        except Exception as exc:
            logger.error("[SMS] generic_http send to %s failed: %s",
                         _mask_number(to), exc)


def _mask_number(number: str) -> str:
    """Mask phone number for log output: +94771234567 → +9477****567."""
    n = str(number)
    if len(n) <= 6:
        return "****"
    return n[:4] + "****" + n[-3:]


# ── Public service class ──────────────────────────────────────────────────────

class SMSAlertService:
    """
    Send SMS / WhatsApp notifications for ALARM-severity alarm events.

    Usage:
        svc = SMSAlertService(cfg)
        # In alarm engine:
        svc.send_alarm_alert(meter_id, meter_name, code, message)

    Thread safety: send_alarm_alert() can be called from any thread.
    All network I/O runs in a daemon thread so it never blocks callers.

    Rate limiting:
    Each (meter_id, code) pair is throttled to one SMS per cooldown_min.
    Default 60 min — a sustained comm-loss won't send 720 SMS/day.

    CLEAR events:
    Optionally send a CLEAR notification when an alarm resolves.
    Controlled by cfg['sms_alert']['notify_clear'] (default False —
    most sites only want to know when something breaks, not when it
    recovers — but enable if operators are remote with no dashboard access).
    """

    def __init__(self, cfg: dict) -> None:
        self._cfg            = cfg
        self._lock           = threading.Lock()
        # (meter_id, code) → last_sent_epoch
        self._last_sent: Dict[tuple, float] = {}
        # session counters
        self._sent_count     = 0
        self._fail_count     = 0
        self._last_error     = ""

    # ── Public API ────────────────────────────────────────────────────────────

    def reconfigure(self, cfg: dict) -> None:
        self._cfg = cfg

    @property
    def enabled(self) -> bool:
        return bool(self._sc("enabled", False))

    @property
    def provider(self) -> str:
        return str(self._sc("provider", "twilio")).lower()

    @property
    def sent_count(self) -> int:
        return self._sent_count

    @property
    def fail_count(self) -> int:
        return self._fail_count

    @property
    def last_error(self) -> str:
        return self._last_error

    @property
    def is_twilio_available(self) -> bool:
        return _TWILIO_AVAILABLE

    def send_alarm_alert(
        self,
        meter_id: str,
        meter_name: str,
        code: str,
        message: str,
        severity: str = "ALARM",
        event: str = "RAISE",
    ) -> None:
        """
        Fire-and-forget: build the SMS and send it in a daemon thread.

        Skips if:
          • service not enabled in config
          • severity != ALARM  (WARN messages are suppressed)
          • CLEAR event and notify_clear=False (default)
          • rate-limit cooldown not expired for this (meter_id, code)
        """
        if not self.enabled:
            return

        # Only send CLEAR if operator explicitly enabled it
        if event == "CLEAR" and not bool(self._sc("notify_clear", False)):
            return

        # Only send ALARM severity by default; notify_warn is opt-in
        if str(severity).upper() != "ALARM" and not bool(self._sc("notify_warn", False)):
            return

        # Rate-limit: skip if same alarm was sent within cooldown window
        key = (str(meter_id), str(code))
        cooldown_sec = float(self._sc("cooldown_min", 60)) * 60.0
        now = time.time()
        with self._lock:
            last = self._last_sent.get(key, 0.0)
            if (now - last) < cooldown_sec:
                return   # still in cooldown
            self._last_sent[key] = now

        # Build message
        template = str(self._sc("message_template", _DEFAULT_TEMPLATE))
        site     = str((self._cfg.get("site") or {}).get("plant_name", "SCADA"))
        body     = _fmt_message(template, site, str(meter_id), meter_name,
                                code, message)
        if event == "CLEAR":
            body = f"[CLEARED] {body}"

        recipients: List[str] = list(self._sc("recipients", []))
        if not recipients:
            logger.warning("[SMS] No recipients configured")
            return

        # Spawn and forget
        t = threading.Thread(
            target=self._dispatch,
            args=(body, recipients),
            name=f"sms-alert-{code}",
            daemon=True,
        )
        t.start()

    def test_send(self, to_number: str) -> str:
        """
        Send a test SMS to a single number synchronously (blocks up to 15 s).
        Returns "" on success, or an error string.

        Used by the connectivity settings UI "Send Test" button.
        """
        if not self.enabled:
            return "SMS service is disabled"
        body = f"[TEST] MFM384 SCADA SMS alert test — {time.strftime('%H:%M:%S')}"
        try:
            self._dispatch_single(body, [to_number], raise_on_error=True)
            return ""
        except Exception as exc:
            return str(exc)

    # ── Internal ─────────────────────────────────────────────────────────────

    def _sc(self, key: str, default=None):
        """Read a value from cfg['sms_alert']."""
        return (self._cfg.get("sms_alert") or {}).get(key, default)

    def _dispatch(self, body: str, recipients: List[str]) -> None:
        """Called in daemon thread.  Catches all exceptions."""
        try:
            self._dispatch_single(body, recipients)
        except Exception as exc:
            self._fail_count += 1
            self._last_error = str(exc)
            logger.error("[SMS] dispatch error: %s", exc)

    def _dispatch_single(self, body: str, recipients: List[str],
                         raise_on_error: bool = False) -> None:
        """Route to the correct provider implementation."""
        p = self.provider
        pcfg: dict = self._sc("provider_config", {}) or {}

        try:
            if p == "twilio":
                _send_twilio(pcfg, recipients, body)
            elif p == "dialog":
                _send_dialog(pcfg, recipients, body)
            elif p == "mobitel":
                _send_mobitel(pcfg, recipients, body)
            elif p == "generic_http":
                _send_generic_http(pcfg, recipients, body)
            else:
                raise ValueError(f"Unknown SMS provider: {p!r}")
            self._sent_count += 1
        except Exception as exc:
            self._last_error = str(exc)
            self._fail_count += 1
            logger.error("[SMS] %s send failed: %s", p, exc)
            if raise_on_error:
                raise
