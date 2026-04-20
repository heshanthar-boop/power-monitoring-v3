from __future__ import annotations

import csv
import io
import json
import os
import shutil
import sqlite3
import tempfile
import time
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Optional

from utils.atomic import atomic_write_json
from utils.crash_report import crash_reports_dir
from utils.paths import config_path, db_dir, email_db_path, get_base_dir, logs_dir, reports_db_path
from utils.startup_health import StartupHealthCheck, run_startup_health

_SECRET_KEY_PARTS = ("password", "secret", "token", "api_key", "apikey", "private", "credential")
_LOG_LIMIT = 12
_LOG_MAX_BYTES = 5 * 1024 * 1024


def backup_dir() -> str:
    path = os.path.join(get_base_dir(), "backups")
    os.makedirs(path, exist_ok=True)
    return path


def _stamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _safe_name(name: str) -> str:
    return "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in str(name or "scada")).strip("_") or "scada"


def _zip_file(zf: zipfile.ZipFile, path: str, arcname: str) -> bool:
    if not path or path == ":memory:" or not os.path.exists(path) or not os.path.isfile(path):
        return False
    zf.write(path, arcname)
    return True


def _sqlite_backup_to_zip(zf: zipfile.ZipFile, path: str, arcname: str, temp_dir: str) -> bool:
    if not path or path == ":memory:" or not os.path.exists(path) or not os.path.isfile(path):
        return False
    tmp_path = os.path.join(temp_dir, _safe_name(arcname.replace("/", "_")))
    try:
        src = sqlite3.connect(path, timeout=10)
        try:
            dst = sqlite3.connect(tmp_path)
            try:
                src.backup(dst)
            finally:
                dst.close()
        finally:
            src.close()
        zf.write(tmp_path, arcname)
        return True
    except Exception:
        # Fall back to direct copy if SQLite cannot open the file.
        return _zip_file(zf, path, arcname)


def _historian_path(cfg: dict) -> str:
    lcfg = (cfg or {}).get("logging", {}) or {}
    folder = str(lcfg.get("folder", "") or "").strip() or logs_dir()
    return os.path.join(folder, "historian.db")


def redact_config(obj: Any) -> Any:
    if isinstance(obj, dict):
        out: dict[str, Any] = {}
        for key, value in obj.items():
            key_s = str(key)
            if any(part in key_s.lower() for part in _SECRET_KEY_PARTS):
                out[key_s] = "<redacted>" if value not in (None, "") else value
            else:
                out[key_s] = redact_config(value)
        return out
    if isinstance(obj, list):
        return [redact_config(v) for v in obj]
    return obj


def _health_text(checks: Iterable[StartupHealthCheck]) -> str:
    lines = ["MFM384 startup health", f"Generated: {datetime.now().isoformat(timespec='seconds')}", ""]
    for check in checks:
        lines.append(f"[{str(check.level).upper()}] {check.name}: {check.message}")
        action = str(getattr(check, "action", "") or "").strip()
        if action:
            lines.append(f"  Action: {action}")
    return "\n".join(lines) + "\n"


def _audit_csv(auth_service: Any, limit: int = 5000) -> str:
    rows = []
    if auth_service is not None and hasattr(auth_service, "list_audit"):
        try:
            rows = list(auth_service.list_audit(limit=limit) or [])
        except Exception:
            rows = []
    out = io.StringIO()
    writer = csv.DictWriter(out, fieldnames=["ts", "user_name", "action", "detail", "success"])
    writer.writeheader()
    for row in rows:
        writer.writerow({
            "ts": row.get("ts", ""),
            "user_name": row.get("user_name", ""),
            "action": row.get("action", ""),
            "detail": row.get("detail", ""),
            "success": row.get("success", ""),
        })
    return out.getvalue()


def _recent_log_files() -> list[str]:
    candidates: list[str] = []
    for folder in {logs_dir()}:
        try:
            for path in Path(folder).glob("**/*"):
                if path.is_file() and path.stat().st_size <= _LOG_MAX_BYTES:
                    candidates.append(str(path))
        except Exception:
            continue
    candidates.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    return candidates[:_LOG_LIMIT]


def _recent_crash_report_files() -> list[str]:
    candidates: list[str] = []
    try:
        for path in Path(crash_reports_dir()).glob("*.txt"):
            if path.is_file() and path.stat().st_size <= _LOG_MAX_BYTES:
                candidates.append(str(path))
    except Exception:
        return []
    candidates.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    return candidates[:_LOG_LIMIT]


def create_full_backup(cfg: dict, *, auth_service: Any = None, dest_dir: Optional[str] = None) -> str:
    """Create a full operational backup ZIP.

    This includes the live config and local SQLite databases. It may contain
    secrets from config.json, so store it like a credential-bearing backup.
    """
    site = ((cfg or {}).get("site") or {}).get("plant_name", "scada")
    dest = dest_dir or backup_dir()
    os.makedirs(dest, exist_ok=True)
    out_path = os.path.join(dest, f"{_safe_name(site)}_full_backup_{_stamp()}.zip")

    manifest: dict[str, Any] = {
        "type": "full_backup",
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "base_dir": get_base_dir(),
        "files": [],
        "warning": "Full backup may contain credentials from config.json.",
    }

    with tempfile.TemporaryDirectory(prefix="scada_backup_") as td:
        with zipfile.ZipFile(out_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            if _zip_file(zf, config_path(), "config.json"):
                manifest["files"].append("config.json")

            dbs = [
                (getattr(auth_service, "_audit_db_path", "") if auth_service is not None else "", "db/auth_audit.db"),
                (os.path.join(db_dir(), "events.db"), "db/events.db"),
                (email_db_path(), "db/email_queue.db"),
                (reports_db_path(), "db/reports.db"),
                (_historian_path(cfg), "db/historian.db"),
            ]
            for db_path, arcname in dbs:
                if _sqlite_backup_to_zip(zf, str(db_path or ""), arcname, td):
                    manifest["files"].append(arcname)

            zf.writestr("manifest.json", json.dumps(manifest, indent=2, ensure_ascii=False))
    return out_path


def create_support_bundle(
    cfg: dict,
    *,
    auth_service: Any = None,
    logging_engine: Any = None,
    data_store: Any = None,
    snapshot_bus: Any = None,
    worker: Any = None,
    dest_dir: Optional[str] = None,
) -> str:
    """Create a redacted support bundle for field troubleshooting."""
    site = ((cfg or {}).get("site") or {}).get("plant_name", "scada")
    dest = dest_dir or backup_dir()
    os.makedirs(dest, exist_ok=True)
    out_path = os.path.join(dest, f"{_safe_name(site)}_support_bundle_{_stamp()}.zip")
    checks = run_startup_health(cfg)

    manifest = {
        "type": "support_bundle",
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "base_dir": get_base_dir(),
        "redacted": True,
        "log_limit": _LOG_LIMIT,
        "crash_report_limit": _LOG_LIMIT,
    }

    with zipfile.ZipFile(out_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("manifest.json", json.dumps(manifest, indent=2, ensure_ascii=False))
        zf.writestr("config_redacted.json", json.dumps(redact_config(cfg), indent=2, ensure_ascii=False))
        zf.writestr("startup_health.txt", _health_text(checks))
        zf.writestr("audit_export.csv", _audit_csv(auth_service))
        runtime_health = {
            "logging": getattr(logging_engine, "write_health", {}) if logging_engine is not None else {},
            "data_quality": data_store.get_quality_health() if data_store is not None and hasattr(data_store, "get_quality_health") else {},
            "snapshot_bus": snapshot_bus.stats() if snapshot_bus is not None and hasattr(snapshot_bus, "stats") else {},
            "communication": worker.get_comm_diagnostics() if worker is not None and hasattr(worker, "get_comm_diagnostics") else {},
        }
        zf.writestr("runtime_health.json", json.dumps(runtime_health, indent=2, ensure_ascii=False, default=str))

        inventory = [
            f"base_dir={get_base_dir()}",
            f"config_path={config_path()}",
            f"logs_dir={logs_dir()}",
            f"crash_reports_dir={crash_reports_dir()}",
            f"db_dir={db_dir()}",
            f"historian_path={_historian_path(cfg)}",
        ]
        zf.writestr("path_inventory.txt", "\n".join(inventory) + "\n")

        base_logs = logs_dir()
        for log_path in _recent_log_files():
            try:
                rel = os.path.relpath(log_path, base_logs)
            except Exception:
                rel = os.path.basename(log_path)
            _zip_file(zf, log_path, f"logs/{rel}")

        for crash_path in _recent_crash_report_files():
            _zip_file(zf, crash_path, f"crash_reports/{os.path.basename(crash_path)}")
    return out_path


def restore_config_from_backup(zip_path: str) -> str:
    """Restore only config.json from a backup ZIP. Returns the restored path.

    Database restore is intentionally not done while the app is running because
    live SQLite writers may corrupt or overwrite restored files.
    """
    zpath = str(zip_path or "").strip()
    if not zpath or not os.path.exists(zpath):
        raise FileNotFoundError(zpath or "backup ZIP path is empty")

    with zipfile.ZipFile(zpath, "r") as zf:
        names = zf.namelist()
        cfg_name = "config.json" if "config.json" in names else ""
        if not cfg_name:
            matches = [n for n in names if n.endswith("/config.json")]
            cfg_name = matches[0] if matches else ""
        if not cfg_name:
            raise ValueError("Backup ZIP does not contain config.json")
        raw = zf.read(cfg_name).decode("utf-8")
        data = json.loads(raw)
        if not isinstance(data, dict):
            raise ValueError("config.json in backup is not a JSON object")

    target = config_path()
    if os.path.exists(target):
        base, ext = os.path.splitext(target)
        pre = f"{base}.pre_restore.{_stamp()}{ext or '.json'}"
        shutil.copy2(target, pre)
    atomic_write_json(target, data)
    return target
