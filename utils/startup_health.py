from __future__ import annotations

import glob
import os
import shutil
import sqlite3
from dataclasses import dataclass
from typing import Any, Dict, List

from core.comm_config import serial_port_available, validate_serial_config
from utils.paths import config_path, db_dir, get_base_dir, logs_dir


@dataclass(frozen=True)
class StartupHealthCheck:
    name: str
    level: str  # OK / WARN / ERROR
    message: str
    action: str = ""


def _writable_dir(path: str) -> bool:
    try:
        os.makedirs(path, exist_ok=True)
        probe = os.path.join(path, ".write_probe.tmp")
        with open(probe, "w", encoding="utf-8") as f:
            f.write("ok")
        os.remove(probe)
        return True
    except Exception:
        return False


def _check_sqlite_db(path: str) -> StartupHealthCheck:
    if not os.path.exists(path):
        folder = os.path.dirname(path) or "."
        ok = _writable_dir(folder)
        return StartupHealthCheck(
            "historian_db",
            "OK" if ok else "ERROR",
            f"Historian DB will be created at startup: {path}" if ok else f"Historian folder is not writable: {folder}",
            "No action needed." if ok else "Choose a writable logging folder or fix Windows permissions.",
        )

    try:
        con = sqlite3.connect(path, timeout=5)
        try:
            result = str(con.execute("PRAGMA quick_check").fetchone()[0]).strip().lower()
        finally:
            con.close()
        if result == "ok":
            return StartupHealthCheck(
                "historian_db",
                "OK",
                f"SQLite historian integrity OK: {path}",
                "No action needed.",
            )
        return StartupHealthCheck(
            "historian_db",
            "ERROR",
            f"SQLite historian integrity check failed: {result}",
            "Stop logging, back up historian.db, then restore from backup or start a new DB.",
        )
    except Exception as exc:
        return StartupHealthCheck(
            "historian_db",
            "ERROR",
            f"Could not open SQLite historian: {exc}",
            "Check file permissions, antivirus locks, or restore the DB from backup.",
        )


def run_startup_health(cfg: Dict[str, Any]) -> List[StartupHealthCheck]:
    """Return startup diagnostics for logs and the operator health screen."""
    checks: List[StartupHealthCheck] = []

    base = get_base_dir()
    base_ok = _writable_dir(base)
    checks.append(
        StartupHealthCheck(
            "data_dir",
            "OK" if base_ok else "ERROR",
            f"Data directory {'writable' if base_ok else 'not writable'}: {base}",
            "No action needed." if base_ok else "Move the app data folder to a writable drive or fix Windows permissions.",
        )
    )

    for name, path in (("logs_dir", logs_dir()), ("db_dir", db_dir()), ("crash_reports_dir", os.path.join(base, "crash_reports"))):
        ok = _writable_dir(path)
        checks.append(
            StartupHealthCheck(
                name,
                "OK" if ok else "ERROR",
                f"{path} is {'writable' if ok else 'not writable'}",
                "No action needed." if ok else "Choose a writable data/logging folder or fix Windows permissions.",
            )
        )

    try:
        usage = shutil.disk_usage(base)
        free_mb = usage.free / (1024 * 1024)
        if free_mb < 100:
            level = "ERROR"
            action = "Free disk space now or move the data folder before starting continuous logging."
        elif free_mb < 500:
            level = "WARN"
            action = "Free disk space soon or shorten log retention."
        else:
            level = "OK"
            action = "No action needed."
        checks.append(StartupHealthCheck("disk_free", level, f"{free_mb:.0f} MB free at {base}", action))
    except Exception as exc:
        checks.append(StartupHealthCheck("disk_free", "WARN", f"Could not read disk free space: {exc}", "Check drive status in Windows Explorer."))

    cfg_path = config_path()
    invalid_files = sorted(glob.glob(os.path.join(os.path.dirname(cfg_path) or ".", "config.invalid.*.json")))
    if invalid_files:
        latest = invalid_files[-1]
        checks.append(
            StartupHealthCheck(
                "config_recovery",
                "WARN",
                f"Recovered from an invalid config backup: {latest}",
                "Review the backup and re-apply any missing site/communication settings.",
            )
        )
    else:
        checks.append(StartupHealthCheck("config_recovery", "OK", "No recovered invalid config backups found.", "No action needed."))

    checks.append(StartupHealthCheck("config", "OK", f"Config path: {cfg_path}", "No action needed."))

    serial = (cfg or {}).get("serial", {}) or {}
    comm_issues = validate_serial_config(serial)
    if comm_issues:
        checks.append(
            StartupHealthCheck(
                "comm_validation",
                "ERROR" if bool(serial.get("auto_connect", False)) else "WARN",
                "; ".join(comm_issues[:4]),
                "Fix Communication settings before relying on automatic polling.",
            )
        )
    else:
        checks.append(StartupHealthCheck("comm_validation", "OK", "Communication parameters are valid.", "No action needed."))

    transport = str(serial.get("transport", "rtu") or "rtu").strip().lower()
    auto_connect = bool(serial.get("auto_connect", False))
    if transport == "tcp":
        host = str(serial.get("tcp_host", "") or "").strip()
        port = str(serial.get("tcp_port", 502) or "502").strip()
        if not host:
            checks.append(
                StartupHealthCheck(
                    "comm_config",
                    "WARN",
                    "Modbus TCP is selected but gateway host is blank.",
                    "Login as Engineer/Admin, open Communication, and enter the TCP gateway IP address.",
                )
            )
        else:
            checks.append(StartupHealthCheck("comm_config", "OK", f"Transport=tcp host={host}:{port}", "No action needed."))
    else:
        port = str(serial.get("port", "") or "").strip()
        if not port or port.lower() == "none":
            checks.append(
                StartupHealthCheck(
                    "comm_config",
                    "WARN" if not auto_connect else "ERROR",
                    "RS-485 serial mode is selected but no COM port is configured.",
                    "Login as Engineer/Admin, open Communication, then run Auto Setup or select the USB-RS485 COM port.",
                )
            )
        else:
            available = serial_port_available(port)
            if available is False:
                checks.append(
                    StartupHealthCheck(
                        "comm_port_present",
                        "ERROR" if auto_connect else "WARN",
                        f"Configured COM port is not currently present: {port}",
                        "Plug in the USB-RS485 adapter or select the correct COM port.",
                    )
                )
            elif available is None:
                checks.append(StartupHealthCheck("comm_port_present", "WARN", "Could not list Windows COM ports from this environment.", "Verify COM port in Device Manager if connection fails."))
            else:
                checks.append(StartupHealthCheck("comm_port_present", "OK", f"COM port present: {port}", "No action needed."))
            checks.append(StartupHealthCheck("comm_config", "OK", f"Transport=rtu port={port} baud={serial.get('baud', 9600)}", "No action needed."))

    meters = [m for m in ((cfg or {}).get("meters") or []) if isinstance(m, dict)]
    enabled = [m for m in meters if bool(m.get("enabled", True))]
    if not enabled:
        checks.append(
            StartupHealthCheck(
                "meters",
                "WARN",
                "No enabled meters configured.",
                "Login as Engineer/Admin, open Communication, and enable or auto-detect meters.",
            )
        )
    else:
        seen: Dict[int, int] = {}
        duplicates = []
        for m in enabled:
            try:
                sid = int(m.get("slave_id", m.get("id", 0)))
            except Exception:
                sid = 0
            if sid <= 0:
                duplicates.append(sid)
            if sid in seen:
                duplicates.append(sid)
            seen[sid] = seen.get(sid, 0) + 1
        if duplicates:
            checks.append(
                StartupHealthCheck(
                    "meters",
                    "ERROR",
                    f"Invalid or duplicate enabled slave IDs: {sorted(set(duplicates))}",
                    "Fix slave IDs in Communication. Each enabled RS-485 meter must have a unique ID from 1 to 247.",
                )
            )
        else:
            names = ", ".join(str(m.get("name", f"Meter {m.get('id', '')}") or "").strip() for m in enabled[:5])
            checks.append(
                StartupHealthCheck(
                    "meters",
                    "OK",
                    f"{len(enabled)} enabled meter(s) configured: {names}",
                    "No action needed.",
                )
            )

    lcfg = (cfg or {}).get("logging", {}) or {}
    backend = str(lcfg.get("backend", "sqlite") or "sqlite").strip().lower()
    try:
        interval_sec = int(lcfg.get("interval_sec", 10) or 10)
    except Exception:
        interval_sec = 0
    if backend not in ("sqlite", "csv", "both"):
        checks.append(
            StartupHealthCheck(
                "logging_backend",
                "ERROR",
                f"Unsupported logging backend: {backend}",
                "Set logging backend to sqlite, csv, or both.",
            )
        )
    else:
        checks.append(StartupHealthCheck("logging_backend", "OK", f"Logging backend: {backend}", "No action needed."))
    if interval_sec < 1:
        checks.append(
            StartupHealthCheck(
                "logging_interval",
                "ERROR",
                f"Logging interval is invalid: {interval_sec}s",
                "Set logging interval to 1 second or higher.",
            )
        )
    elif interval_sec < 5:
        checks.append(
            StartupHealthCheck(
                "logging_interval",
                "WARN",
                f"Logging interval is very fast: {interval_sec}s",
                "Use 5-10 seconds unless the historian disk and PC have been validated.",
            )
        )
    else:
        checks.append(StartupHealthCheck("logging_interval", "OK", f"Logging interval: {interval_sec}s", "No action needed."))

    qcfg = (cfg or {}).get("data_quality", {}) or {}
    try:
        fmin = float(qcfg.get("frequency_min_hz", 40.0))
        fmax = float(qcfg.get("frequency_max_hz", 70.0))
        pfmax = float(qcfg.get("pf_abs_max", 1.05))
        if fmin >= fmax or pfmax < 1.0:
            checks.append(
                StartupHealthCheck(
                    "data_quality_limits",
                    "ERROR",
                    f"Invalid data-quality limits: frequency={fmin}-{fmax}Hz pf_abs_max={pfmax}",
                    "Fix data_quality limits in config before relying on alarms/reports.",
                )
            )
        else:
            checks.append(StartupHealthCheck("data_quality_limits", "OK", f"Frequency window={fmin:.1f}-{fmax:.1f}Hz PF limit=+/-{pfmax:.2f}", "No action needed."))
    except Exception as exc:
        checks.append(StartupHealthCheck("data_quality_limits", "ERROR", f"Could not read data-quality limits: {exc}", "Fix data_quality config values."))

    log_folder = str(lcfg.get("folder", "") or "").strip() or logs_dir()
    folder_ok = _writable_dir(log_folder)
    checks.append(
        StartupHealthCheck(
            "logging_folder",
            "OK" if folder_ok else "ERROR",
            f"Logging folder {'writable' if folder_ok else 'not writable'}: {log_folder}",
            "No action needed." if folder_ok else "Choose a writable logging folder before enabling continuous logs.",
        )
    )
    if backend in ("sqlite", "both"):
        checks.append(_check_sqlite_db(os.path.join(log_folder, "historian.db")))
    elif backend == "csv":
        checks.append(StartupHealthCheck("historian_db", "OK", f"Historian disabled because logging backend is {backend}.", "No action needed."))
    else:
        checks.append(StartupHealthCheck("historian_db", "WARN", "Historian check skipped because logging backend is invalid.", "Fix logging backend first."))

    sync_cfg = (cfg or {}).get("remote_sync", {}) or {}
    mqtt_cfg = (cfg or {}).get("mqtt", {}) or {}
    rest_cfg = (cfg or {}).get("rest_api", {}) or {}
    sync_enabled = bool(sync_cfg.get("enabled", True))
    try:
        max_queue = int(sync_cfg.get("max_queue", 300) or 300)
    except Exception:
        max_queue = 0
    if max_queue < 10:
        checks.append(
            StartupHealthCheck(
                "remote_sync",
                "WARN",
                f"Remote sync queue is too small: {max_queue}",
                "Set remote_sync.max_queue to at least 10; 300 is recommended for weak internet.",
            )
        )
    elif sync_enabled and bool(mqtt_cfg.get("enabled", False)):
        checks.append(
            StartupHealthCheck(
                "remote_sync",
                "OK",
                f"MQTT offline queue enabled: up to {max_queue} snapshots.",
                "No action needed.",
            )
        )
    elif sync_enabled and bool(rest_cfg.get("enabled", False)):
        checks.append(
            StartupHealthCheck(
                "remote_sync",
                "OK",
                "REST local endpoint enabled; MQTT offline queue idle because MQTT is disabled.",
                "Enable MQTT only if a broker/cloud bridge is required.",
            )
        )
    elif sync_enabled:
        checks.append(
            StartupHealthCheck(
                "remote_sync",
                "WARN",
                "Remote sync queue is enabled but MQTT and REST are both disabled.",
                "Open Connectivity and enable MQTT and/or REST if remote monitoring is required.",
            )
        )
    else:
        checks.append(StartupHealthCheck("remote_sync", "OK", "Remote sync queue disabled by configuration.", "No action needed."))

    return checks
