"""
Watchdog heartbeat — writes a JSON heartbeat file every INTERVAL seconds.

An external monitor (Windows Task Scheduler or a separate process) can detect
a stale heartbeat and restart the application.

Heartbeat file location: %LOCALAPPDATA%\MFM384Monitor\heartbeat.json
Heartbeat file content:
    {"ts": 1712345678.12, "pid": 12345, "version": "1.0.0"}

Setup (Windows Task Scheduler):
    Create a task that runs every 2 minutes.
    Condition: if the heartbeat file mtime is older than 90 seconds, restart app.
    Action:  taskkill /F /IM power_monitor.exe  (if running)
             start "" "C:\\path\\to\\power_monitor.exe"

    A simple PowerShell trigger script:
        $hb = "$env:LOCALAPPDATA\\MFM384Monitor\\heartbeat.json"
        if ((Get-Date) - (Get-Item $hb -EA SilentlyContinue).LastWriteTime).TotalSeconds -gt 90) {
            Stop-Process -Name power_monitor -Force -EA SilentlyContinue
            Start-Process "C:\\path\\to\\power_monitor.exe"
        }
"""
from __future__ import annotations

import json
import os
import threading
import time

from utils.logger import setup_logger

log = setup_logger("watchdog")

_INTERVAL_SEC = 30
_HEARTBEAT_SUBDIR = "MFM384Monitor"
_HEARTBEAT_FILE = "heartbeat.json"


def _heartbeat_path() -> str:
    base = os.environ.get("LOCALAPPDATA") or os.path.expanduser("~")
    folder = os.path.join(base, _HEARTBEAT_SUBDIR)
    os.makedirs(folder, exist_ok=True)
    return os.path.join(folder, _HEARTBEAT_FILE)


class WatchdogHeartbeat:
    """Background daemon that touches a heartbeat file every INTERVAL seconds.

    Usage:
        wd = WatchdogHeartbeat(version="1.0.0")
        wd.start()
        # ... app runs ...
        wd.stop()
    """

    def __init__(self, version: str = "", interval_sec: int = _INTERVAL_SEC):
        self._version = version
        self._interval = max(5, int(interval_sec))
        self._stop_evt = threading.Event()
        self._thread: threading.Thread | None = None
        self._path = _heartbeat_path()

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop_evt.clear()
        self._thread = threading.Thread(
            target=self._run,
            name="watchdog-heartbeat",
            daemon=True,
        )
        self._thread.start()
        log.info("Watchdog heartbeat started — file: %s  interval: %ds", self._path, self._interval)

    def stop(self) -> None:
        self._stop_evt.set()
        if self._thread:
            self._thread.join(timeout=5.0)
        log.info("Watchdog heartbeat stopped")

    def _run(self) -> None:
        while not self._stop_evt.is_set():
            self._touch()
            self._stop_evt.wait(timeout=self._interval)

    def _touch(self) -> None:
        try:
            payload = {
                "ts": time.time(),
                "pid": os.getpid(),
                "version": self._version,
            }
            tmp = self._path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(payload, f)
            os.replace(tmp, self._path)
        except Exception:
            log.warning("Watchdog heartbeat write failed", exc_info=True)
