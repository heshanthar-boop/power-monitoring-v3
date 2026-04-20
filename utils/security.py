from __future__ import annotations

import base64
import hashlib
import hmac
import os
from typing import Optional, Tuple


_HASH_ALGO = "pbkdf2_sha256"
_DEFAULT_ITERATIONS = 260_000
_KEYRING_DEFAULT_USERNAME = "default"


def _b64_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("ascii").rstrip("=")


def _b64_decode(data: str) -> bytes:
    s = str(data or "").strip()
    pad = "=" * ((4 - (len(s) % 4)) % 4)
    return base64.urlsafe_b64decode((s + pad).encode("ascii"))


def hash_password(password: str, *, iterations: int = _DEFAULT_ITERATIONS, salt: Optional[bytes] = None) -> str:
    """Return a salted PBKDF2 password hash token.

    Format:
      pbkdf2_sha256$<iterations>$<salt_b64>$<digest_b64>
    """
    raw = str(password or "").encode("utf-8")
    salt_bytes = salt if isinstance(salt, (bytes, bytearray)) else os.urandom(16)
    iters = max(100_000, int(iterations or _DEFAULT_ITERATIONS))
    digest = hashlib.pbkdf2_hmac("sha256", raw, bytes(salt_bytes), iters)
    return f"{_HASH_ALGO}${iters}${_b64_encode(bytes(salt_bytes))}${_b64_encode(digest)}"


def verify_password(password: str, encoded_hash: str) -> bool:
    token = str(encoded_hash or "").strip()
    if not token:
        return False
    parts = token.split("$")
    if len(parts) != 4:
        return False
    algo, iters_s, salt_s, digest_s = parts
    if algo != _HASH_ALGO:
        return False
    try:
        iters = int(iters_s)
        salt = _b64_decode(salt_s)
        expected = _b64_decode(digest_s)
    except Exception:
        return False
    candidate = hashlib.pbkdf2_hmac("sha256", str(password or "").encode("utf-8"), salt, max(1, iters))
    return hmac.compare_digest(candidate, expected)


def migrate_setup_password_hash(cfg: dict) -> bool:
    """Upgrade legacy plaintext setup password to hashed storage.

    Returns True if cfg changed.
    """
    if not isinstance(cfg, dict):
        return False
    changed = False
    hashed = str(cfg.get("setup_write_password_hash", "") or "").strip()
    legacy = str(cfg.get("setup_write_password", "") or "")
    if legacy and not hashed:
        cfg["setup_write_password_hash"] = hash_password(legacy)
        cfg["setup_write_password"] = ""
        changed = True
    return changed


def setup_password_is_configured(cfg: dict) -> bool:
    if not isinstance(cfg, dict):
        return False
    return bool(
        str(cfg.get("setup_write_password_hash", "") or "").strip()
        or str(cfg.get("setup_write_password", "") or "").strip()
    )


def verify_setup_password(cfg: dict, password: str) -> bool:
    """Verify setup write password against hashed (preferred) or legacy plaintext."""
    if not isinstance(cfg, dict):
        return False
    hashed = str(cfg.get("setup_write_password_hash", "") or "").strip()
    if hashed:
        return verify_password(password, hashed)
    legacy = str(cfg.get("setup_write_password", "") or "")
    if not legacy:
        return str(password or "") == ""
    return hmac.compare_digest(str(password or ""), legacy)


def is_secret_reference(value: str) -> bool:
    s = str(value or "").strip()
    return s.startswith("env:") or s.startswith("keyring:")


def _parse_keyring_ref(ref: str) -> Tuple[str, str]:
    # keyring:<service>:<username>
    # keyring:<service>  (username defaults to "default")
    body = str(ref or "").strip()[len("keyring:") :]
    if ":" in body:
        service, username = body.split(":", 1)
    else:
        service, username = body, _KEYRING_DEFAULT_USERNAME
    return service.strip(), (username.strip() or _KEYRING_DEFAULT_USERNAME)


def _read_keyring_secret(service: str, username: str) -> str:
    service_s = str(service or "").strip()
    username_s = str(username or "").strip() or _KEYRING_DEFAULT_USERNAME
    if not service_s:
        return ""
    try:
        import keyring  # type: ignore
    except Exception:
        return ""
    try:
        val = keyring.get_password(service_s, username_s)
        return str(val or "")
    except Exception:
        return ""


def resolve_secret(
    raw_value: str,
    *,
    env_var: str = "",
    default_env_var: str = "",
    keyring_service: str = "",
    keyring_username: str = "",
) -> str:
    """Resolve secret from env/keyring reference with legacy plaintext fallback.

    Resolution order:
      1) raw_value if it is `env:VAR` or `keyring:SVC[:USER]`
      2) env_var (config key, points to environment variable name)
      3) default_env_var
      4) keyring_service/keyring_username (config fields)
      5) raw_value literal (legacy plaintext)
    """
    raw = str(raw_value or "").strip()

    if raw.startswith("env:"):
        name = raw[4:].strip()
        return str(os.environ.get(name, "") or "")

    if raw.startswith("keyring:"):
        svc, usr = _parse_keyring_ref(raw)
        return _read_keyring_secret(svc, usr)

    env_name = str(env_var or "").strip()
    if env_name:
        env_val = str(os.environ.get(env_name, "") or "")
        if env_val:
            return env_val

    fallback_env = str(default_env_var or "").strip()
    if fallback_env:
        env_val = str(os.environ.get(fallback_env, "") or "")
        if env_val:
            return env_val

    svc = str(keyring_service or "").strip()
    if svc:
        usr = str(keyring_username or "").strip() or _KEYRING_DEFAULT_USERNAME
        v = _read_keyring_secret(svc, usr)
        if v:
            return v

    return raw



# ── TOTP (RFC 6238 Time-based One-Time Password) ─────────────────────────────

def generate_totp_secret() -> str:
    """Generate a new random base32-encoded TOTP secret (160-bit / 20 bytes)."""
    import struct
    raw = os.urandom(20)
    # base32 encode without padding — pyotp / standard authenticator apps accept this
    return base64.b32encode(raw).decode("ascii").rstrip("=")


def totp_uri(secret: str, username: str, issuer: str = "MFM384 Monitor") -> str:
    """Return an otpauth:// URI for QR code generation.

    Compatible with Google Authenticator, Authy, and any RFC 6238-compliant app.
    """
    import urllib.parse
    issuer_enc = urllib.parse.quote(str(issuer or "MFM384 Monitor"), safe="")
    label = urllib.parse.quote(f"{issuer}:{username}", safe=":")
    params = urllib.parse.urlencode({
        "secret": str(secret or "").upper().replace("=", ""),
        "issuer": str(issuer or "MFM384 Monitor"),
        "algorithm": "SHA1",
        "digits": "6",
        "period": "30",
    })
    return f"otpauth://totp/{label}?{params}"


def verify_totp(secret: str, code: str, *, window: int = 1) -> bool:
    """Verify a 6-digit TOTP code.

    window=1 accepts ±1 time step (±30 s) to handle clock drift.
    Falls back to a pure-Python HOTP implementation if pyotp is unavailable.
    """
    raw_secret = str(secret or "").upper().strip().rstrip("=")
    raw_code   = str(code or "").strip().replace(" ", "")
    if len(raw_code) != 6 or not raw_code.isdigit():
        return False
    if not raw_secret:
        return False

    try:
        import pyotp  # type: ignore
        # Pad to multiple of 8 for pyotp
        pad = (8 - len(raw_secret) % 8) % 8
        totp = pyotp.TOTP(raw_secret + "=" * pad)
        return totp.verify(raw_code, valid_window=window)
    except ImportError:
        pass

    # Pure-Python fallback (no pyotp)
    import struct
    import time as _time

    pad = (8 - len(raw_secret) % 8) % 8
    try:
        key = base64.b32decode(raw_secret + "=" * pad, casefold=True)
    except Exception:
        return False

    t_now = int(_time.time()) // 30
    for step in range(-window, window + 1):
        counter = t_now + step
        msg = struct.pack(">Q", counter)
        h = hmac.new(key, msg, hashlib.sha1).digest()
        offset = h[-1] & 0x0F
        val = struct.unpack(">I", h[offset:offset + 4])[0] & 0x7FFFFFFF
        candidate = f"{val % 1_000_000:06d}"
        if hmac.compare_digest(candidate, raw_code):
            return True
    return False


def migrate_plaintext_secret_to_env_ref(
    section: dict,
    *,
    value_key: str,
    env_key: str,
    default_env_var: str,
) -> bool:
    """Replace plaintext secret with env reference token.

    Example result:
      section[value_key] = "env:SCADA_SMTP_PASSWORD"
      section[env_key] = "SCADA_SMTP_PASSWORD"
    """
    if not isinstance(section, dict):
        return False
    changed = False
    env_name = str(section.get(env_key, "") or "").strip() or str(default_env_var or "").strip()
    if env_name and str(section.get(env_key, "") or "").strip() != env_name:
        section[env_key] = env_name
        changed = True

    raw = str(section.get(value_key, "") or "").strip()
    if raw and (not is_secret_reference(raw)):
        if env_name:
            section[value_key] = f"env:{env_name}"
        else:
            section[value_key] = ""
        changed = True
    return changed
