from __future__ import annotations

from typing import Any, Dict, List


VALID_BAUD_RATES = {1200, 2400, 4800, 9600, 19200, 38400, 57600, 115200}
VALID_PARITY = {"N", "E", "O"}
VALID_STOPBITS = {1, 2}


def normalize_serial_config(serial_cfg: dict | None) -> Dict[str, Any]:
    cfg = dict(serial_cfg or {})
    transport = str(cfg.get("transport", "rtu") or "rtu").strip().lower()
    if transport not in ("rtu", "tcp"):
        transport = "rtu"
    cfg["transport"] = transport

    if "baud" not in cfg and "baud_rate" in cfg:
        cfg["baud"] = cfg.get("baud_rate")
    if "baud_rate" not in cfg and "baud" in cfg:
        cfg["baud_rate"] = cfg.get("baud")
    try:
        cfg["baud"] = int(cfg.get("baud", 9600) or 9600)
    except Exception:
        cfg["baud"] = 9600
    cfg["baud_rate"] = cfg["baud"]

    if "timeout" not in cfg and "timeout_ms" in cfg:
        try:
            cfg["timeout"] = max(0.1, float(cfg.get("timeout_ms", 1000) or 1000) / 1000.0)
        except Exception:
            cfg["timeout"] = 1.0
    try:
        cfg["timeout"] = max(0.1, min(10.0, float(cfg.get("timeout", 1.0) or 1.0)))
    except Exception:
        cfg["timeout"] = 1.0
    cfg["timeout_ms"] = int(cfg["timeout"] * 1000)

    cfg["parity"] = str(cfg.get("parity", "N") or "N").strip().upper()[:1] or "N"
    try:
        cfg["stopbits"] = int(cfg.get("stopbits", 1) or 1)
    except Exception:
        cfg["stopbits"] = 1

    try:
        cfg["tcp_port"] = int(cfg.get("tcp_port", 502) or 502)
    except Exception:
        cfg["tcp_port"] = 502

    for key, default, minimum, maximum in (
        ("meter_retry_count", 2, 1, 10),
        ("bus_reconnect_threshold", 3, 1, 20),
        ("scan_max_slave_id", 3, 1, 247),
        ("max_regs_per_read", 60, 1, 120),
    ):
        try:
            cfg[key] = max(minimum, min(maximum, int(cfg.get(key, default) or default)))
        except Exception:
            cfg[key] = default

    for key, default, minimum, maximum in (
        ("backoff_initial_sec", 1.0, 0.2, 60.0),
        ("backoff_max_sec", 30.0, 1.0, 300.0),
        ("backoff_jitter", 0.15, 0.0, 0.75),
        ("port_check_interval_sec", 2.0, 0.5, 60.0),
        ("latency_warn_ms", 1000.0, 50.0, 30000.0),
    ):
        try:
            cfg[key] = max(minimum, min(maximum, float(cfg.get(key, default) or default)))
        except Exception:
            cfg[key] = default

    return cfg


def validate_serial_config(serial_cfg: dict | None) -> List[str]:
    cfg = normalize_serial_config(serial_cfg)
    issues: List[str] = []
    if cfg["transport"] == "tcp":
        if not str(cfg.get("tcp_host", "") or "").strip():
            if bool(cfg.get("auto_connect", False)):
                issues.append("TCP transport selected but tcp_host is blank")
        if not (1 <= int(cfg.get("tcp_port", 0) or 0) <= 65535):
            issues.append("TCP port must be 1-65535")
    else:
        port = str(cfg.get("port", "") or "").strip()
        if (not port or port.lower() == "none") and bool(cfg.get("auto_connect", False)):
            issues.append("RTU transport selected but COM port is blank")
        if int(cfg.get("baud", 0) or 0) not in VALID_BAUD_RATES:
            issues.append(f"Unsupported baud rate: {cfg.get('baud')}")
        if str(cfg.get("parity", "N") or "N").upper() not in VALID_PARITY:
            issues.append(f"Unsupported parity: {cfg.get('parity')}")
        if int(cfg.get("stopbits", 1) or 1) not in VALID_STOPBITS:
            issues.append(f"Unsupported stop bits: {cfg.get('stopbits')}")
    return issues


def list_serial_ports() -> List[str]:
    try:
        from serial.tools import list_ports

        return sorted(str(p.device) for p in list_ports.comports() if getattr(p, "device", ""))
    except Exception:
        return []


def serial_port_available(port: str) -> bool | None:
    p = str(port or "").strip()
    if not p or p.lower() == "none":
        return False
    ports = list_serial_ports()
    if not ports:
        return None
    return p.upper() in {x.upper() for x in ports}
