from __future__ import annotations

import os
import sqlite3
import tempfile
import time
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

from utils.paths import db_dir
from utils.security import hash_password, verify_password, generate_totp_secret, verify_totp, totp_uri
from config.features import FEATURE_KEYS, DEFAULT_ROLE_PERMISSIONS  # single source of truth

DEFAULT_ROLE_IDLE_SEC = {
    "operator": 7200,
    "owner": 7200,
    "engineer": 5400,
    "admin_master": 3600,
}

DEFAULT_USER_SEEDS = [
    ("Operator", "operator", "0123456789"),
    ("Owner", "owner", "owner12345"),
    ("Engineer", "engineer", "eng12345"),
]

MASTER_USERNAME = "Admin"
MIN_PASSWORD_LEN = 10


class AuthService:
    def __init__(self, cfg: Dict[str, Any], save_cb: Optional[Callable[[], None]] = None):
        self.cfg = cfg
        self._save_cb = save_cb
        self._audit_db_path = self._resolve_audit_db_path()
        self._init_audit_db()
        self._ensure_cfg()
        self._seed_default_users()

    def set_save_callback(self, save_cb: Optional[Callable[[], None]]) -> None:
        self._save_cb = save_cb

    def _save(self) -> None:
        if callable(self._save_cb):
            try:
                self._save_cb()
            except Exception:
                # Never crash auth flow because config storage is read-only/locked.
                pass

    def _auth_cfg(self) -> Dict[str, Any]:
        return self.cfg.setdefault("auth", {})

    def _ensure_cfg(self) -> None:
        a = self._auth_cfg()
        a.setdefault("users", [])
        a.setdefault("role_idle_timeout_sec", {})
        for k, v in DEFAULT_ROLE_IDLE_SEC.items():
            a["role_idle_timeout_sec"].setdefault(k, int(v))
        a.setdefault("audit_max_rows", 20000)
        a.setdefault("lockout_failed_attempts", 5)
        a.setdefault("lockout_sec", 900)
        a.setdefault("totp_required_roles", ["engineer"])

    def _normalize_username(self, username: str) -> str:
        return str(username or "").strip().lower()

    def _normalize_role(self, role: str) -> str:
        r = self._normalize_username(role)
        return r if r in ("operator", "owner", "engineer") else "operator"

    def _role_permissions(self, role: str) -> Dict[str, bool]:
        role_n = self._normalize_role(role)
        allowed = DEFAULT_ROLE_PERMISSIONS.get(role_n, set())
        return {k: bool(k in allowed) for k in FEATURE_KEYS}

    def _normalize_permissions(self, role: str, raw: Any) -> Dict[str, bool]:
        out = self._role_permissions(role)
        if isinstance(raw, dict):
            for k in FEATURE_KEYS:
                if k in raw:
                    out[k] = bool(raw.get(k))
        return out

    def _seed_default_users(self) -> None:
        users = self._auth_cfg().setdefault("users", [])
        existing = {self._normalize_username(u.get("username", "")) for u in users if isinstance(u, dict)}
        changed = False
        for username, role, password in DEFAULT_USER_SEEDS:
            if self._normalize_username(username) in existing:
                continue
            users.append(
                {
                    "username": username,
                    "role": role,
                    "enabled": True,
                    "password_hash": hash_password(password),
                    "must_change_password": True,
                    "default_seed": True,
                    "failed_login_count": 0,
                    "locked_until": 0.0,
                    "permissions": self._role_permissions(role),
                }
            )
            changed = True

        seeded_defaults = {self._normalize_username(u): pw for u, _role, pw in DEFAULT_USER_SEEDS}
        for u in users:
            if not isinstance(u, dict):
                continue
            role = self._normalize_role(u.get("role", "operator"))
            u["role"] = role
            u["enabled"] = bool(u.get("enabled", True))
            if not str(u.get("password_hash", "") or "").strip():
                temp_pw = f"ChangeMe-{int(time.time())}"
                u["password_hash"] = hash_password(temp_pw)
                u["must_change_password"] = True
                u["default_seed"] = True
                changed = True
            default_pw = seeded_defaults.get(self._normalize_username(u.get("username", "")))
            if default_pw and verify_password(default_pw, str(u.get("password_hash", ""))):
                if not bool(u.get("must_change_password", False)):
                    u["must_change_password"] = True
                    changed = True
                if not bool(u.get("default_seed", False)):
                    u["default_seed"] = True
                    changed = True
            perms = self._normalize_permissions(role, u.get("permissions", {}))
            if perms != u.get("permissions"):
                u["permissions"] = perms
                changed = True
            u.setdefault("failed_login_count", 0)
            u.setdefault("locked_until", 0.0)

        if changed:
            self._save()

    def master_password_for_time(self, when: Optional[datetime] = None) -> str:
        dt = when or datetime.now()
        return dt.strftime("%Y%m%d%H")

    def _lockout_cfg(self) -> tuple[int, int]:
        a = self._auth_cfg()
        try:
            attempts = max(1, int(a.get("lockout_failed_attempts", 5)))
        except Exception:
            attempts = 5
        try:
            lockout_sec = max(60, int(a.get("lockout_sec", 900)))
        except Exception:
            lockout_sec = 900
        return attempts, lockout_sec

    def _is_locked(self, user: Dict[str, Any]) -> bool:
        try:
            return float(user.get("locked_until", 0.0) or 0.0) > time.time()
        except Exception:
            return False

    def _register_failed_login(self, user: Dict[str, Any]) -> None:
        attempts, lockout_sec = self._lockout_cfg()
        try:
            count = int(user.get("failed_login_count", 0) or 0) + 1
        except Exception:
            count = 1
        user["failed_login_count"] = count
        user["last_failed_login_ts"] = float(time.time())
        if count >= attempts:
            user["locked_until"] = float(time.time() + lockout_sec)
            user["failed_login_count"] = 0
        self._save()

    def _reset_failed_login(self, user: Dict[str, Any]) -> None:
        changed = False
        if int(user.get("failed_login_count", 0) or 0) != 0:
            user["failed_login_count"] = 0
            changed = True
        if float(user.get("locked_until", 0.0) or 0.0) != 0.0:
            user["locked_until"] = 0.0
            changed = True
        if changed:
            self._save()

    def _validate_new_password(self, password: str) -> str:
        pw = str(password or "")
        if len(pw) < MIN_PASSWORD_LEN:
            return f"Password must be at least {MIN_PASSWORD_LEN} characters."
        lowered = pw.lower()
        weak_tokens = ("password", "123456", "0123456789", "search1234", "changeme", "admin")
        if any(tok in lowered for tok in weak_tokens):
            return "Password is too easy to guess."
        return ""

    def _totp_required_for_role(self, role: str) -> bool:
        raw = self._auth_cfg().get("totp_required_roles", ["engineer"])
        if not isinstance(raw, (list, tuple, set)):
            return False
        role_n = self._normalize_role(role)
        return role_n in {self._normalize_username(x) for x in raw}

    def authenticate_password(self, username: str, password: str) -> Optional[Dict[str, Any]]:
        """Phase-1 password check.

        Returns a session dict on success, or None on failure.
        If TOTP is enabled, the returned dict contains ``totp_required=True``
        and the caller must call ``verify_totp_for_user`` before granting access.
        """
        return self.authenticate(username, password, _totp_check=False)

    def authenticate(self, username: str, password: str, *, _totp_check: bool = True) -> Optional[Dict[str, Any]]:
        u_norm = self._normalize_username(username)
        p = str(password or "")

        if u_norm == self._normalize_username(MASTER_USERNAME):
            ok = p == self.master_password_for_time()
            self.record_audit(
                actor=MASTER_USERNAME,
                action="LOGIN",
                detail="Master admin login",
                success=ok,
            )
            if not ok:
                return None
            return {
                "username": MASTER_USERNAME,
                "role": "admin_master",
                "is_master_admin": True,
                "idle_timeout_sec": int(self._auth_cfg().get("role_idle_timeout_sec", {}).get("admin_master", DEFAULT_ROLE_IDLE_SEC["admin_master"])),
                "permissions": {k: True for k in FEATURE_KEYS},
            }

        user = self._find_user(username)
        if not user or (not bool(user.get("enabled", True))):
            self.record_audit(actor=str(username or ""), action="LOGIN", detail="Unknown/disabled user", success=False)
            return None
        if self._is_locked(user):
            remaining = int(float(user.get("locked_until", 0.0) or 0.0) - time.time())
            self.record_audit(
                actor=str(user.get("username", "")),
                action="LOGIN_LOCKED",
                detail=f"remaining_sec={max(0, remaining)}",
                success=False,
            )
            return None

        ok = verify_password(p, str(user.get("password_hash", "")))
        self.record_audit(
            actor=str(user.get("username", "")),
            action="LOGIN",
            detail=f"Role={user.get('role', 'operator')}",
            success=ok,
        )
        if not ok:
            self._register_failed_login(user)
            return None
        self._reset_failed_login(user)

        role = self._normalize_role(str(user.get("role", "operator")))
        if self._totp_required_for_role(role) and not self.requires_totp(str(user.get("username", ""))):
            self.record_audit(
                actor=str(user.get("username", "")),
                action="LOGIN_TOTP_REQUIRED",
                detail=f"Role={role} not enrolled",
                success=False,
            )
            return None
        perms = self._normalize_permissions(role, user.get("permissions", {}))
        idle = int(self._auth_cfg().get("role_idle_timeout_sec", {}).get(role, DEFAULT_ROLE_IDLE_SEC.get(role, 7200)))
        session: Dict[str, Any] = {
            "username": str(user.get("username", "")),
            "role": role,
            "is_master_admin": False,
            "idle_timeout_sec": idle,
            "permissions": perms,
            "must_change_password": bool(user.get("must_change_password", False)),
        }
        # If TOTP is enabled and caller requested the check, signal it is required.
        # The login dialog handles the second-factor prompt.
        if not _totp_check and self.requires_totp(str(user.get("username", ""))):
            session["totp_required"] = True
        return session

    def _find_user(self, username: str) -> Optional[Dict[str, Any]]:
        u_norm = self._normalize_username(username)
        for u in self._auth_cfg().get("users", []) or []:
            if not isinstance(u, dict):
                continue
            if self._normalize_username(u.get("username", "")) == u_norm:
                return u
        return None

    def list_users(self) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for u in self._auth_cfg().get("users", []) or []:
            if not isinstance(u, dict):
                continue
            out.append(
                {
                    "username": str(u.get("username", "")),
                    "role": self._normalize_role(str(u.get("role", "operator"))),
                    "enabled": bool(u.get("enabled", True)),
                    "permissions": self._normalize_permissions(str(u.get("role", "operator")), u.get("permissions", {})),
                }
            )
        out.sort(key=lambda x: self._normalize_username(x["username"]))
        return out

    def upsert_user(
        self,
        *,
        actor: str,
        username: str,
        role: str,
        enabled: bool,
        password: str = "",
        permissions: Optional[Dict[str, bool]] = None,
    ) -> bool:
        uname = str(username or "").strip()
        if (not uname) or self._normalize_username(uname) == self._normalize_username(MASTER_USERNAME):
            return False
        role_n = self._normalize_role(role)
        user = self._find_user(uname)
        is_new = user is None
        raw_password = str(password or "")
        if is_new and not raw_password:
            self.record_audit(actor=actor, action="USER_UPSERT_DENIED", detail=f"{uname} missing password", success=False)
            return False
        if raw_password:
            reason = self._validate_new_password(raw_password)
            if reason:
                self.record_audit(actor=actor, action="USER_UPSERT_DENIED", detail=f"{uname}: {reason}", success=False)
                return False
        if user is None:
            user = {"username": uname}
            self._auth_cfg().setdefault("users", []).append(user)
        user["username"] = uname
        user["role"] = role_n
        user["enabled"] = bool(enabled)
        if raw_password:
            user["password_hash"] = hash_password(raw_password)
            user["must_change_password"] = True
            user["default_seed"] = False
        if not str(user.get("password_hash", "")).strip():
            self.record_audit(actor=actor, action="USER_UPSERT_DENIED", detail=f"{uname} missing password hash", success=False)
            return False
        user.setdefault("failed_login_count", 0)
        user.setdefault("locked_until", 0.0)
        user["permissions"] = self._normalize_permissions(role_n, permissions if permissions is not None else user.get("permissions", {}))
        self._save()
        self.record_audit(actor=actor, action="USER_UPSERT", detail=f"{uname} role={role_n} enabled={bool(enabled)}", success=True)
        return True

    def change_password(self, *, actor: str, username: str, old_password: str, new_password: str) -> tuple[bool, str]:
        target_norm = self._normalize_username(username)
        if (not target_norm) or target_norm == self._normalize_username(MASTER_USERNAME):
            return False, "Master admin uses the time-based password and cannot be changed here."
        user = self._find_user(username)
        if user is None:
            return False, "User not found."
        if not verify_password(str(old_password or ""), str(user.get("password_hash", ""))):
            self.record_audit(actor=actor, action="PASSWORD_CHANGE_DENIED", detail=str(username or ""), success=False)
            return False, "Current password is incorrect."
        reason = self._validate_new_password(new_password)
        if reason:
            return False, reason
        if verify_password(str(new_password or ""), str(user.get("password_hash", ""))):
            return False, "New password must be different from the current password."
        user["password_hash"] = hash_password(str(new_password or ""))
        user["must_change_password"] = False
        user["default_seed"] = False
        user["failed_login_count"] = 0
        user["locked_until"] = 0.0
        self._save()
        self.record_audit(actor=actor, action="PASSWORD_CHANGED", detail=str(username or ""), success=True)
        return True, ""

    def delete_user(self, *, actor: str, username: str) -> bool:
        target_norm = self._normalize_username(username)
        if (not target_norm) or target_norm == self._normalize_username(MASTER_USERNAME):
            return False
        users = self._auth_cfg().get("users", []) or []
        new_users = [u for u in users if self._normalize_username((u or {}).get("username", "")) != target_norm]
        if len(new_users) == len(users):
            return False
        self._auth_cfg()["users"] = new_users
        self._save()
        self.record_audit(actor=actor, action="USER_DELETE", detail=str(username or ""), success=True)
        return True

    # ── TOTP management ─────────────────────────────────────────────────────

    def totp_status(self, username: str) -> Dict[str, Any]:
        """Return TOTP status for a user: {'enabled': bool, 'secret': str|None}."""
        user = self._find_user(username)
        if user is None:
            return {"enabled": False, "secret": None}
        return {
            "enabled": bool(user.get("totp_enabled", False)),
            "secret":  str(user.get("totp_secret", "") or "") or None,
        }

    def totp_generate_secret(self, username: str) -> Optional[str]:
        """Generate + store a new TOTP secret for the user (NOT yet enabled).

        The caller must show this to the user for enrollment, then call
        totp_enable() after the user verifies the first code.
        Returns the secret string, or None if the user does not exist.
        """
        user = self._find_user(username)
        if user is None:
            return None
        secret = generate_totp_secret()
        user["totp_secret"]  = secret
        user["totp_enabled"] = False   # pending enrollment
        self._save()
        return secret

    def totp_enable(self, *, actor: str, username: str, code: str) -> bool:
        """Verify the first code and enable TOTP for the user.

        The secret must already exist (call totp_generate_secret first).
        Returns True on success.
        """
        user = self._find_user(username)
        if user is None:
            return False
        secret = str(user.get("totp_secret", "") or "")
        if not secret:
            return False
        if not verify_totp(secret, code):
            self.record_audit(actor=actor, action="TOTP_ENABLE_FAIL",
                              detail=username, success=False)
            return False
        user["totp_enabled"] = True
        self._save()
        self.record_audit(actor=actor, action="TOTP_ENABLED", detail=username, success=True)
        return True

    def totp_disable(self, *, actor: str, username: str) -> bool:
        """Disable and remove TOTP for a user."""
        user = self._find_user(username)
        if user is None:
            return False
        user["totp_enabled"] = False
        user.pop("totp_secret", None)
        self._save()
        self.record_audit(actor=actor, action="TOTP_DISABLED", detail=username, success=True)
        return True

    def totp_uri_for_user(self, username: str) -> str:
        """Return an otpauth:// URI for QR code display, or '' if no secret."""
        user = self._find_user(username)
        if user is None:
            return ""
        secret = str(user.get("totp_secret", "") or "")
        if not secret:
            return ""
        return totp_uri(secret, str(user.get("username", username)))

    def requires_totp(self, username: str) -> bool:
        """True if the user has TOTP fully enabled."""
        user = self._find_user(username)
        if user is None:
            return False
        return bool(user.get("totp_enabled", False)) and bool(user.get("totp_secret", ""))

    def verify_totp_for_user(self, username: str, code: str) -> bool:
        """Verify a TOTP code for a user. Returns False if TOTP not enabled."""
        user = self._find_user(username)
        if user is None:
            return False
        if not bool(user.get("totp_enabled", False)):
            return False
        secret = str(user.get("totp_secret", "") or "")
        return verify_totp(secret, code)

    # ─────────────────────────────────────────────────────────────────────────

    def set_permissions(self, *, actor: str, username: str, permissions: Dict[str, bool]) -> bool:
        user = self._find_user(username)
        if user is None:
            return False
        role = self._normalize_role(user.get("role", "operator"))
        user["permissions"] = self._normalize_permissions(role, permissions)
        self._save()
        self.record_audit(actor=actor, action="USER_PERMISSIONS", detail=f"{user.get('username', '')}", success=True)
        return True

    def record_audit(self, *, actor: str, action: str, detail: str = "", success: bool = True) -> None:
        try:
            with self._conn() as con:
                con.execute(
                    "INSERT INTO audit_log(ts,user_name,action,detail,success) VALUES(?,?,?,?,?)",
                    (float(time.time()), str(actor or ""), str(action or ""), str(detail or "")[:800], 1 if success else 0),
                )
            self._prune_audit_rows()
        except Exception:
            pass

    def list_audit(self, limit: int = 500) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        try:
            with self._conn() as con:
                rows = con.execute(
                    "SELECT ts,user_name,action,detail,success FROM audit_log ORDER BY id DESC LIMIT ?",
                    (max(1, int(limit)),),
                ).fetchall()
            for ts, user_name, action, detail, success in rows:
                out.append(
                    {
                        "ts": float(ts or 0.0),
                        "user_name": str(user_name or ""),
                        "action": str(action or ""),
                        "detail": str(detail or ""),
                        "success": bool(int(success or 0)),
                    }
                )
        except Exception:
            pass
        return out

    def clear_audit(self, *, actor: str, allow: bool) -> bool:
        if not allow:
            self.record_audit(actor=actor, action="AUDIT_CLEAR_DENIED", detail="", success=False)
            return False
        try:
            with self._conn() as con:
                con.execute("DELETE FROM audit_log")
            self.record_audit(actor=actor, action="AUDIT_CLEARED", detail="", success=True)
            return True
        except Exception:
            return False

    def _init_audit_db(self) -> None:
        try:
            with self._conn() as con:
                con.execute(
                    """
                    CREATE TABLE IF NOT EXISTS audit_log (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        ts REAL NOT NULL,
                        user_name TEXT NOT NULL,
                        action TEXT NOT NULL,
                        detail TEXT NOT NULL,
                        success INTEGER NOT NULL
                    )
                    """
                )
                con.execute("CREATE INDEX IF NOT EXISTS idx_audit_ts ON audit_log(ts)")
            return
        except Exception:
            pass

        try:
            root = os.path.join(tempfile.gettempdir(), "mfm384_scada")
            os.makedirs(root, exist_ok=True)
            self._audit_db_path = os.path.join(root, "auth_audit.db")
            with self._conn() as con:
                con.execute(
                    """
                    CREATE TABLE IF NOT EXISTS audit_log (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        ts REAL NOT NULL,
                        user_name TEXT NOT NULL,
                        action TEXT NOT NULL,
                        detail TEXT NOT NULL,
                        success INTEGER NOT NULL
                    )
                    """
                )
                con.execute("CREATE INDEX IF NOT EXISTS idx_audit_ts ON audit_log(ts)")
        except Exception:
            self._audit_db_path = ":memory:"

    def _resolve_audit_db_path(self) -> str:
        candidates: List[str] = []
        try:
            candidates.append(str(db_dir() or ""))
        except Exception:
            pass
        candidates.append(os.path.join(os.getcwd(), "data", "db"))
        candidates.append(str(tempfile.gettempdir() or ""))

        for root in candidates:
            root_s = str(root or "").strip()
            if not root_s:
                continue
            try:
                os.makedirs(root_s, exist_ok=True)
                path = os.path.join(root_s, "auth_audit.db")
                con = sqlite3.connect(path, timeout=10)
                con.close()
                return path
            except Exception:
                continue
        return ":memory:"

    def _prune_audit_rows(self) -> None:
        max_rows = int(self._auth_cfg().get("audit_max_rows", 20000) or 20000)
        if max_rows <= 0:
            return
        try:
            with self._conn() as con:
                con.execute(
                    """
                    DELETE FROM audit_log
                    WHERE id NOT IN (
                        SELECT id FROM audit_log ORDER BY id DESC LIMIT ?
                    )
                    """,
                    (max_rows,),
                )
        except Exception:
            pass

    def _conn(self) -> sqlite3.Connection:
        c = sqlite3.connect(self._audit_db_path, timeout=10)
        try:
            c.execute("PRAGMA journal_mode=WAL")
        except sqlite3.OperationalError:
            # Some locked-down Windows folders allow the DB file but not WAL sidecars.
            pass
        try:
            c.execute("PRAGMA synchronous=NORMAL")
        except sqlite3.OperationalError:
            pass
        return c
