"""
World-class CSV logging engine with crash resilience, aggregation, and scheduling.
Runs I/O in a background thread to never freeze the UI.
"""
from __future__ import annotations

import os
import re
import csv
import shutil
import time
import atexit
import threading
import queue
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any, Optional, Callable, Tuple, Set

from core.snapshot_bus import MeterSnapshot, MeterQuality, SnapshotBus
from core.total_aggregator import TotalAggregator
from core.meter_model import MeterState
from core.key_registry import canonical_key as canonicalize_log_key
from utils.paths import logs_dir
from utils.logger import setup_logger
from utils.log_once import log_once

try:
    from core.historian import Historian as _Historian
except Exception:
    _Historian = None  # type: ignore

logger = setup_logger("logging_engine")


# =============================================================================
# Windows Keep-Awake Helper
# =============================================================================
def _set_keep_awake(enabled: bool):
    """Set Windows thread execution state to prevent sleep."""
    try:
        import ctypes
        ES_CONTINUOUS = 0x80000000
        ES_SYSTEM_REQUIRED = 0x00000001
        if enabled:
            ctypes.windll.kernel32.SetThreadExecutionState(
                ES_CONTINUOUS | ES_SYSTEM_REQUIRED
            )
        else:
            ctypes.windll.kernel32.SetThreadExecutionState(ES_CONTINUOUS)
    except Exception:
        pass  # Non-Windows or ctypes not available


# =============================================================================
# Schedule Helpers
# =============================================================================
def is_in_schedule(now: datetime, schedule_cfg: dict) -> bool:
    """
    Check if current time is within the configured schedule.
    
    Schedule modes:
    - "always": Always ON
    - "date_range": Date+time range (start_datetime, end_datetime) format: YYYY-MM-DD HH:MM
    - "one_shot": Start time + duration
    - "disabled": Always OFF
    """
    mode = schedule_cfg.get("mode", "always")
    
    if mode == "always":
        return True
    elif mode == "disabled":
        return False
    elif mode in ("time_range", "date_range"):
        try:
            start_str = schedule_cfg.get("start_datetime", "")
            end_str = schedule_cfg.get("end_datetime", "")
            if not start_str or not end_str:
                return True  # No range set, default to always
            
            # Parse datetime: "YYYY-MM-DD HH:MM" or legacy "HH:MM"
            if len(start_str) > 5:  # Full datetime
                start_dt = datetime.strptime(start_str, "%Y-%m-%d %H:%M")
                end_dt = datetime.strptime(end_str, "%Y-%m-%d %H:%M")
            else:  # Legacy time-only format
                start_parts = [int(x) for x in start_str.split(":")]
                end_parts = [int(x) for x in end_str.split(":")]
                start_dt = now.replace(hour=start_parts[0], minute=start_parts[1], second=0, microsecond=0)
                end_dt = now.replace(hour=end_parts[0], minute=end_parts[1], second=59, microsecond=999999)
            
            return start_dt <= now <= end_dt
        except Exception:
            return True
    elif mode == "one_shot":
        try:
            start_ts = schedule_cfg.get("start_ts", 0)
            duration_sec = schedule_cfg.get("duration_sec", 3600)
            if start_ts == 0:
                return False
            end_ts = start_ts + duration_sec
            return start_ts <= now.timestamp() <= end_ts
        except Exception:
            return False
    
    return True


# =============================================================================
# Aggregation Statistics
# =============================================================================
class AggStats:
    """Rolling statistics for min/avg/max aggregation."""
    
    def __init__(self):
        self.reset()
    
    def reset(self):
        self.min_val = None
        self.max_val = None
        self.sum_val = 0.0
        self.count = 0
        self._last_val = None
    
    def add(self, value: float):
        if self.min_val is None or value < self.min_val:
            self.min_val = value
        if self.max_val is None or value > self.max_val:
            self.max_val = value
        self.sum_val += value
        self.count += 1
        self._last_val = value
    
    def get(self, mode: str) -> Optional[float]:
        if self.count == 0:
            return None
        if mode == "min":
            return self.min_val
        elif mode == "max":
            return self.max_val
        elif mode == "avg":
            return self.sum_val / self.count
        else:  # instant - return last value
            return self._last_val


# =============================================================================
# Logging Engine
# =============================================================================
class LoggingEngine:
    """
    World-class CSV logging engine with:
    - Background thread for crash-resilient I/O
    - Aggregation modes (instant, min, avg, max)
    - Configurable intervals and scheduling
    - Per-source (TOTAL + meters) logging
    - Site details and meter serials in file header
    """
    
    # State constants
    STATE_DISABLED = "DISABLED"
    STATE_SCHEDULED_OFF = "SCHEDULED_OFF"
    STATE_RUNNING = "RUNNING"
    STATE_ERROR = "ERROR"
    
    # Default keys to log
    DEFAULT_KEYS = ["kW", "kVA", "kVAr", "Vavg", "Iavg", "PFavg", "Import_kWh", "Export_kWh"]
    
    def __init__(self, interval_sec: int = 10):
        # Configuration (set via configure())
        self.cfg: dict = {}
        self.meters: list = []  # Set via configure()
        self.get_snapshot_fn: Optional[Callable] = None
        
        # Runtime state
        self._enabled = False
        self.interval_sec = interval_sec
        self._state = self.STATE_DISABLED
        self._last_error = ""
        self._current_file_path = ""
        self._rows_written_today = defaultdict(int)
        self._last_write_time: Optional[datetime] = None
        self._next_write_time: Optional[datetime] = None
        self._keep_awake_active = False
        self._session_started: Dict[str, bool] = {}

        # Thread communication
        self._queue: queue.Queue = queue.Queue(maxsize=20000)
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._runtime_lock = threading.RLock()

        # Snapshot bus helpers
        self.snapshot_bus: Optional[SnapshotBus] = None
        self._snapshot_queue: Optional[queue.Queue] = None
        self._logging_meters: Dict[int, MeterState] = {}
        self._total_aggregator = TotalAggregator(self.cfg)
        self._quality_priority = {
            MeterQuality.DISABLED: 0,
            MeterQuality.GOOD: 1,
            MeterQuality.STALE: 2,
            MeterQuality.COMM_LOST: 3,
        }
        self._source_quality: Dict[str, MeterQuality] = {}
        
        # Aggregation state: {source: {key: AggStats}}
        self._agg_data: Dict[str, Dict[str, AggStats]] = {}
        self._agg_sample_count: Dict[str, int] = {}
        self._agg_data_valid: Dict[str, bool] = {}
        self._agg_keys_map: Dict[str, Tuple[str, ...]] = {}

        # Energy rollover detection: last known energy value per source/key
        # MFM384 uses 32-bit accumulators → rollover at ~429,496 kWh.
        # We detect a negative delta > ROLLOVER_THRESHOLD and journal it.
        self._last_energy_val: Dict[str, Dict[str, float]] = {}
        self._ROLLOVER_THRESHOLD_KWH = 200_000.0  # negative jump bigger than this = rollover
        
        # File handles (per source)
        self._file_handles: Dict[str, Any] = {}
        self._csv_writers: Dict[str, csv.DictWriter] = {}
        self._current_dates: Dict[str, str] = {}
        self._writer_keys: Dict[str, Tuple[str, ...]] = {}
        self._session_separators: Set[str] = set()
        
        # Interval tracking
        self._interval_start: Optional[float] = None
        self._last_tick = 0.0

        # Purge tracking — run once per calendar day
        self._last_purge_day: str = ""

        # SQLite historian (None until configure() is called)
        self._historian: Optional[Any] = None

        # Write-health diagnostics (operator/support visibility)
        self._write_error_count = 0
        self._queue_full_count = 0
        self._clock_jump_count = 0
        self._last_error_time: Optional[datetime] = None
        self._last_success_time: Optional[datetime] = None
        self._last_tick_wall_ts: Optional[float] = None
        self._last_tick_mon_ts: Optional[float] = None

        # Quality flags accumulated between ticks — cleared after each emit.
        # Flags set here are stamped on the next emitted historian row.
        self._pending_quality_flags: set = set()

        # CT/PT sanity-check: track last-warned state per source to avoid log spam.
        self._ctpt_warn_active: Dict[str, bool] = {}

        # Register cleanup
        atexit.register(self._cleanup)
    
    # -------------------------------------------------------------------------
    # Public API
    # -------------------------------------------------------------------------
    
    @property
    def enabled(self) -> bool:
        return self._enabled
    
    @enabled.setter
    def enabled(self, value: bool):
        self._enabled = value
        if not value:
            self._set_state(self.STATE_DISABLED)
            self._set_keep_awake(False)
            self._mark_session_breaks()
        else:
            # When the operator enables logging while a CSV already exists,
            # we still want a clear header/comment block at the start of the
            # new logging session (Excel-friendly). This is critical for
            # clarity when logs are appended across multiple start/stop runs.
            try:
                self._session_separators.update(self._configured_sources())
            except Exception:
                logger.warning("session separator update failed; new session header may be missing", exc_info=True)
            self.start()

    def _configured_sources(self) -> Set[str]:
        """Return the set of sources that are configured/enabled for logging."""
        sources: Set[str] = set()
        lcfg = self.cfg.get("logging", {})
        scfg = lcfg.get("sources", {})

        # TOTAL
        total_cfg = scfg.get("total", {})
        if isinstance(total_cfg, bool):
            total_enabled = bool(total_cfg)
        else:
            total_enabled = bool(total_cfg.get("enabled", True))
        if total_enabled:
            sources.add("TOTAL")

        # Meters
        meters_cfg = scfg.get("meters", {})
        per_meter = {}
        if isinstance(meters_cfg, dict):
            per_meter = meters_cfg.get("per_meter", {}) or {}

        for m in self.meters:
            mid = getattr(m, "meter_id", None)
            if mid is None:
                continue
            entry = per_meter.get(str(mid), {}) if isinstance(per_meter, dict) else {}
            enabled = bool(entry.get("enabled", bool(getattr(m, "enabled", True))))
            if enabled:
                sources.add(f"M{mid}")

        return sources
    
    @property
    def state(self) -> str:
        return self._state
    
    @property
    def last_error(self) -> str:
        return self._last_error
    
    @property
    def current_file_path(self) -> str:
        return self._current_file_path

    @property
    def current_file_name(self) -> str:
        if not self._current_file_path:
            return ""
        return os.path.basename(self._current_file_path)

    @property
    def write_queue_depth(self) -> int:
        return self._queue.qsize()
    
    @property
    def rows_written_today(self) -> int:
        return sum(self._rows_written_today.values())
    
    @property
    def last_write_time(self) -> Optional[datetime]:
        return self._last_write_time

    @property
    def write_error_count(self) -> int:
        return int(self._write_error_count)

    @property
    def dropped_write_count(self) -> int:
        return int(self._queue_full_count)

    @property
    def clock_jump_count(self) -> int:
        return int(self._clock_jump_count)

    @property
    def write_health(self) -> dict:
        return {
            "state": self._state,
            "backend": (self.cfg.get("logging", {}).get("backend") or "sqlite").lower(),
            "queue_depth": self.write_queue_depth,
            "queue_max": int(getattr(self._queue, "maxsize", 0) or 0),
            "write_error_count": int(self._write_error_count),
            "dropped_write_count": int(self._queue_full_count),
            "clock_jump_count": int(self._clock_jump_count),
            "last_error": self._last_error,
            "last_error_time": self._last_error_time.isoformat(timespec="seconds") if self._last_error_time else "",
            "last_success_time": self._last_success_time.isoformat(timespec="seconds") if self._last_success_time else "",
            "current_file_path": self._current_file_path,
            "historian_path": self.db_path,
        }
    
    @property
    def next_write_time(self) -> Optional[datetime]:
        return self._next_write_time
    
    @property
    def mode(self) -> str:
        return self.cfg.get("logging", {}).get("mode", "instant")
    
    @mode.setter
    def mode(self, value: str):
        if "logging" not in self.cfg:
            self.cfg["logging"] = {}
        self.cfg["logging"]["mode"] = value
    
    @property
    def keys(self) -> List[str]:
        meters_cfg = self.cfg.get("logging", {}).get("sources", {}).get("meters", {})
        return meters_cfg.get("default_keys", self.DEFAULT_KEYS)

    @keys.setter
    def keys(self, value: List[str]):
        lcfg = self.cfg.setdefault("logging", {})
        sources = lcfg.setdefault("sources", {})
        meters_cfg = sources.setdefault("meters", {})
        clean = self._canonicalize_key_list(value) or self.DEFAULT_KEYS
        meters_cfg["default_keys"] = clean
        lcfg["keys"] = clean

    @property
    def retain_days(self) -> int:
        try:
            return max(1, int(self.cfg.get("logging", {}).get("retain_days", 90)))
        except Exception:
            return 90

    @retain_days.setter
    def retain_days(self, value: int) -> None:
        self.cfg.setdefault("logging", {})["retain_days"] = max(1, int(value))

    @property
    def folder_size_mb(self) -> float:
        """Total size of the log folder in megabytes (fast walk)."""
        folder = (self.cfg.get("logging", {}).get("folder", "") or "").strip() or logs_dir()
        return self._folder_size_mb(folder)

    @staticmethod
    def _folder_size_mb(folder: str) -> float:
        total = 0
        try:
            for root, _dirs, files in os.walk(folder):
                for f in files:
                    try:
                        total += os.path.getsize(os.path.join(root, f))
                    except OSError:
                        pass
        except Exception:
            pass
        return total / (1024 * 1024)

    @staticmethod
    def _purge_old_logs(folder: str, retain_days: int) -> int:
        """Delete YYYY-MM-DD day-subfolders older than retain_days.

        Returns count of folders deleted.  Never raises.
        """
        if retain_days <= 0 or not folder:
            return 0
        try:
            if not os.path.isdir(folder):
                return 0
        except Exception:
            return 0

        cutoff = datetime.now() - timedelta(days=retain_days)
        deleted = 0
        day_pattern = re.compile(r"^\d{4}-\d{2}-\d{2}$")
        try:
            entries = os.listdir(folder)
        except Exception:
            return 0

        for name in entries:
            if not day_pattern.match(name):
                continue
            full = os.path.join(folder, name)
            if not os.path.isdir(full):
                continue
            try:
                folder_date = datetime.strptime(name, "%Y-%m-%d")
            except ValueError:
                continue
            if folder_date.date() < cutoff.date():
                try:
                    shutil.rmtree(full)
                    deleted += 1
                    logger.info("[LOG-PURGE] deleted old log folder: %s", full)
                except Exception:
                    logger.warning("[LOG-PURGE] could not delete %s", full, exc_info=True)

        return deleted

    def _maybe_purge(self) -> None:
        """Run purge/downsampling once per calendar day (called from writer thread).

        Retention tiers (configurable via cfg["data_quality"]["retention"]):
          hot_days  — full-resolution rows kept.          Default: 30 days.
          warm_days — hourly-average rows kept after hot. Default: 180 days.

        Rows older than hot_days are downsampled to hourly averages (warm tier).
        Warm rows older than warm_days are deleted outright.
        Both CSV day-folders and SQLite rows are purged independently.
        """
        today = datetime.now().strftime("%Y-%m-%d")
        if today == self._last_purge_day:
            return
        self._last_purge_day = today
        now = datetime.now()
        folder = (self.cfg.get("logging", {}).get("folder", "") or "").strip() or logs_dir()

        # Retention config
        ret_cfg = (self.cfg.get("data_quality", {}) or {}).get("retention", {}) or {}
        hot_days  = int(ret_cfg.get("hot_days",  30))
        warm_days = int(ret_cfg.get("warm_days", 180))
        # Total maximum retention for CSV (keep at least warm_days worth)
        csv_retain = max(hot_days, warm_days, self.retain_days)

        # Purge old CSV day-folders (CSV doesn't tier — keep all within csv_retain)
        try:
            n = self._purge_old_logs(folder, csv_retain)
            if n:
                logger.info("[LOG-PURGE] purged %d CSV folder(s) older than %d days", n, csv_retain)
        except Exception:
            logger.warning("[LOG-PURGE] CSV purge failed", exc_info=True)

        # SQLite historian: downsample hot → warm, then purge stale warm
        if self._historian is not None:
            # 1. Downsample hot rows older than hot_days into hourly warm rows
            try:
                hot_cutoff = (now - timedelta(days=hot_days)).timestamp()
                warm_written, hot_deleted = self._historian.downsample_to_warm(
                    hot_older_than=hot_cutoff,
                    warm_interval_s=3600,
                )
                if warm_written or hot_deleted:
                    logger.info(
                        "[LOG-PURGE] retention: compressed %d hot→%d warm rows "
                        "(hot older than %d days)",
                        hot_deleted, warm_written, hot_days,
                    )
            except Exception:
                logger.warning("[LOG-PURGE] historian downsample failed", exc_info=True)

            # 2. Purge warm rows older than warm_days
            try:
                warm_cutoff = (now - timedelta(days=warm_days)).timestamp()
                n_warm = self._historian.purge_warm_before(warm_cutoff)
                if n_warm:
                    logger.info(
                        "[LOG-PURGE] removed %d warm historian row(s) older than %d days",
                        n_warm, warm_days,
                    )
            except Exception:
                logger.warning("[LOG-PURGE] warm historian purge failed", exc_info=True)

            # 3. Legacy: also respect the old retain_days hot purge if set shorter
            try:
                hard_cutoff = (now - timedelta(days=self.retain_days)).timestamp()
                n_rows = self._historian.purge_before(hard_cutoff)
                if n_rows:
                    logger.info(
                        "[LOG-PURGE] removed %d hot historian row(s) older than %d days (hard limit)",
                        n_rows, self.retain_days,
                    )
            except Exception:
                logger.warning("[LOG-PURGE] historian purge failed", exc_info=True)

    def configure(self, cfg: dict, meters: list = None, get_snapshot_fn: Optional[Callable] = None, snapshot_bus: Optional[SnapshotBus] = None):
        """Configure the engine with config dict, meters list, snapshot function, and snapshot bus."""
        with self._runtime_lock:
            self.cfg = cfg
            if meters is not None:
                self.meters = meters
                self._logging_meters = {}
                for m in meters:
                    clone = MeterState(m.meter_id, m.slave_id)
                    clone.enabled = bool(m.enabled)
                    clone.display = bool(getattr(m, "display", False))
                    clone.include_in_total = bool(getattr(m, "include_in_total", True))
                    clone.priority = bool(getattr(m, "priority", False))
                    clone.name = str(getattr(m, "name", f"Meter {m.meter_id}"))
                    clone.serial_no = str(getattr(m, "serial_no", ""))
                    clone.scan_interval_sec = int(getattr(m, "scan_interval_sec", 1))
                    self._logging_meters[m.meter_id] = clone
            if get_snapshot_fn:
                self.get_snapshot_fn = get_snapshot_fn
            if snapshot_bus:
                if self._snapshot_queue and self.snapshot_bus is not snapshot_bus:
                    self.snapshot_bus.unsubscribe(self._snapshot_queue)
                    self._snapshot_queue = None
                self.snapshot_bus = snapshot_bus
                if self._snapshot_queue is None:
                    self._snapshot_queue = snapshot_bus.subscribe()
            else:
                if self._snapshot_queue and self.snapshot_bus:
                    self.snapshot_bus.unsubscribe(self._snapshot_queue)
                self._snapshot_queue = None
                self.snapshot_bus = None

            self._normalize_logging_sources()
            lcfg = self.cfg.setdefault("logging", {})
            self._enabled = lcfg.get("enabled", False)
            self.interval_sec = lcfg.get("interval_sec", 10)
            self._total_aggregator.set_cfg(self.cfg)
            self._source_quality.clear()
            self._init_historian()

    # -------------------------------------------------------------------------
    # SQLite historian lifecycle
    # -------------------------------------------------------------------------

    def _init_historian(self) -> None:
        """Initialise (or re-use) the SQLite historian for the current folder."""
        if _Historian is None:
            return
        folder = (self.cfg.get("logging", {}).get("folder", "") or "").strip() or logs_dir()
        db_path = os.path.join(folder, "historian.db")
        # Re-use existing historian if the path hasn't changed
        if self._historian is not None and getattr(self._historian, "db_path", None) == db_path:
            return
        # Close old historian before opening a new one
        if self._historian is not None:
            try:
                self._historian.close()
            except Exception:
                pass
        try:
            os.makedirs(folder, exist_ok=True)
            self._historian = _Historian(db_path)
            logger.info("[HISTORIAN] opened: %s", db_path)
        except Exception as exc:
            logger.warning("[HISTORIAN] init failed — SQLite logging disabled: %s", exc)
            self._historian = None

    @property
    def historian(self):
        """The Historian instance (None if not yet configured / no SQLite backend)."""
        return self._historian

    @property
    def db_path(self) -> str:
        """Path to the SQLite historian database, or '' if not initialised."""
        return getattr(self._historian, "db_path", "") if self._historian else ""

    @property
    def folder_size_mb(self) -> float:
        """Total storage used: CSV folder + SQLite DB [MB]."""
        folder = (self.cfg.get("logging", {}).get("folder", "") or "").strip() or logs_dir()
        csv_mb = self._folder_size_mb(folder)
        db_mb  = self._historian.db_size_mb() if self._historian else 0.0
        # Avoid double-counting: the DB lives inside the same folder
        return max(csv_mb, db_mb)

    @property
    def db_size_mb(self) -> float:
        """SQLite DB file size in MB (0 if not in use)."""
        return self._historian.db_size_mb() if self._historian else 0.0

    @property
    def db_row_count(self) -> int:
        """Total rows in the SQLite historian (0 if not in use)."""
        try:
            return self._historian.row_count() if self._historian else 0
        except Exception:
            return 0

    def _normalize_logging_sources(self):
        lcfg = self.cfg.setdefault("logging", {})
        sources = lcfg.setdefault("sources", {})
        canonical_defaults = self._canonicalize_key_list(lcfg.get("keys", self.DEFAULT_KEYS)) or self.DEFAULT_KEYS
        lcfg["keys"] = canonical_defaults

        total_cfg = sources.get("total", {})
        if isinstance(total_cfg, bool):
            total_cfg = {"enabled": total_cfg}
        total_cfg.setdefault("enabled", True)
        total_cfg["keys"] = self._canonicalize_key_list(total_cfg.get("keys", [])) or self._default_total_keys()
        sources["total"] = total_cfg

        meters_cfg = sources.get("meters", {})
        if isinstance(meters_cfg, (list, tuple, str)):
            meters_cfg = self._legacy_meter_config(meters_cfg, canonical_defaults)
        elif not isinstance(meters_cfg, dict):
            meters_cfg = {}

        default_keys = self._canonicalize_key_list(meters_cfg.get("default_keys", canonical_defaults)) or canonical_defaults
        meters_cfg["default_keys"] = default_keys
        per_meter = meters_cfg.setdefault("per_meter", {})
        for meter in self.meters:
            meter_id = getattr(meter, "meter_id", None)
            if meter_id is None:
                continue
            key = str(meter_id)
            entry = per_meter.get(key, {})
            if not isinstance(entry, dict):
                entry = {}
            entry.setdefault("enabled", bool(getattr(meter, "enabled", True)))
            entry["keys"] = self._canonicalize_key_list(entry.get("keys", []))
            per_meter[key] = entry
        meters_cfg["per_meter"] = per_meter
        sources["meters"] = meters_cfg

    def _legacy_meter_config(self, legacy_setting, default_keys: List[str]) -> Dict[str, Any]:
        setting = legacy_setting
        per_meter: Dict[str, Any] = {}
        for meter in self.meters:
            meter_id = getattr(meter, "meter_id", None)
            if meter_id is None:
                continue
            enabled = True
            if isinstance(setting, str):
                token = setting.upper()
                if token == "NONE":
                    enabled = False
                elif token == "ENABLED":
                    enabled = bool(getattr(meter, "enabled", True))
                else:
                    enabled = token == "ALL"
            elif isinstance(setting, (list, tuple, set)):
                enabled = meter_id in setting
            per_meter[str(meter_id)] = {"enabled": enabled, "keys": []}
        return {"default_keys": default_keys, "per_meter": per_meter}

    def _default_total_keys(self) -> List[str]:
        slots = (self.cfg.get("total_custom") or {}).get("slots", []) or []
        seen: List[str] = []
        for slot in slots[:16]:
            key = canonicalize_log_key(slot.get("output_key"))
            if key and key not in seen:
                seen.append(key)
        return seen or ["kW", "kVA", "kVAr"]

    def _get_keys_for_source(self, source: str) -> Tuple[str, ...]:
        sources = self.cfg.get("logging", {}).get("sources", {})
        if source == "TOTAL":
            total_keys = sources.get("total", {}).get("keys")
            if total_keys:
                return tuple(total_keys)
            return tuple(self._default_total_keys())
        if source.startswith("M"):
            meter_id = source[1:]
            meters_cfg = sources.get("meters", {})
            per_meter = meters_cfg.get("per_meter", {})
            entry = per_meter.get(meter_id, {})
            keys = entry.get("keys")
            if keys:
                return tuple(keys)
            return tuple(meters_cfg.get("default_keys", self.DEFAULT_KEYS))
        return tuple(self.DEFAULT_KEYS)

    def _canonicalize_key_list(self, keys):
        normalized: List[str] = []
        for key in keys or []:
            canon = canonicalize_log_key(key)
            if canon and canon not in normalized:
                normalized.append(canon)
        return normalized
    
    def start(self):
        """Start the background logging thread."""
        if self._thread and self._thread.is_alive():
            return
        
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._writer_thread, daemon=True)
        self._thread.start()
    
    def stop(self):
        """Stop the logging thread gracefully."""
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=2.0)
        self._close_file()
        if self.snapshot_bus and self._snapshot_queue:
            self.snapshot_bus.unsubscribe(self._snapshot_queue)
            self._snapshot_queue = None
        self._set_keep_awake(False)
    
    def tick(self, meters: list, total_dict: Optional[dict] = None):
        """
        Service tick (normally RuntimeService every 500ms).
        Reads snapshots and triggers writes at interval boundaries.
        """
        with self._runtime_lock:
            if not self._enabled:
                self._set_state(self.STATE_DISABLED)
                return
            
            now = datetime.now()
            now_ts = time.time()
            self._check_clock_jump(now_ts, time.monotonic())
            
            # Check schedule
            schedule_cfg = self.cfg.get("logging", {}).get("schedule", {"mode": "always"})
            if not is_in_schedule(now, schedule_cfg):
                self._set_state(self.STATE_SCHEDULED_OFF)
                self._set_keep_awake(False)
                return
            
            self._set_state(self.STATE_RUNNING)
            
            # Keep PC awake if configured
            keep_awake = self.cfg.get("logging", {}).get("keep_awake", False)
            self._set_keep_awake(keep_awake)

            # Process all pending snapshots from the bus
            snapshots = self._drain_snapshot_queue()
            for snapshot in snapshots:
                self._process_meter_snapshot(snapshot)

            if snapshots:
                self._process_total_snapshot()
            
            # Check if interval boundary reached
            interval_sec = self.interval_sec
            
            if self._interval_start is None:
                # Align to wall clock
                self._interval_start = (now_ts // interval_sec) * interval_sec
            
            next_boundary = self._interval_start + interval_sec
            self._next_write_time = datetime.fromtimestamp(next_boundary)
            
            if now_ts >= next_boundary:
                # Emit rows
                self._emit_rows(now)
                self._interval_start = (now_ts // interval_sec) * interval_sec
    
    def start_one_shot(self, duration_minutes: int):
        """Start one-shot logging for specified duration."""
        lcfg = self.cfg.setdefault("logging", {})
        schedule = lcfg.setdefault("schedule", {})
        schedule["mode"] = "one_shot"
        schedule["start_ts"] = time.time()
        schedule["duration_sec"] = duration_minutes * 60
        self.enabled = True
        lcfg["enabled"] = True
    
    def stop_now(self):
        """Stop logging immediately."""
        lcfg = self.cfg.setdefault("logging", {})
        lcfg["enabled"] = False
        self._enabled = False
        self._set_state(self.STATE_DISABLED)
        self._set_keep_awake(False)
        self._mark_session_breaks()
    
    def get_status_summary(self) -> str:
        """Return a short status string for status bar."""
        if self._state == self.STATE_DISABLED:
            return "LOG: OFF"
        elif self._state == self.STATE_SCHEDULED_OFF:
            return "LOG: SCHEDULED OFF"
        elif self._state == self.STATE_ERROR:
            return f"LOG: ERROR"
        else:
            mode_str = self.mode.upper()[:3]
            interval = self.interval_sec
            file_name = self.current_file_name or "no file"
            last_write = (
                self._last_write_time.strftime("%H:%M:%S") if self._last_write_time else "never"
            )
            queue_depth = self.write_queue_depth
            return (
                f"LOG: ON ({interval}s {mode_str}) | {file_name} | last {last_write} | queue {queue_depth}"
            )
    
    # -------------------------------------------------------------------------
    # Internal Methods
    # -------------------------------------------------------------------------
    
    def _set_state(self, state: str, error: str = ""):
        previous = self._state
        self._state = state
        if error:
            self._last_error = error
        if state == self.STATE_RUNNING:
            if previous != self.STATE_RUNNING:
                self._session_started.clear()
        else:
            self._session_started.clear()
    
    def _set_keep_awake(self, enabled: bool):
        if enabled != self._keep_awake_active:
            _set_keep_awake(enabled)
            self._keep_awake_active = enabled

    def _mark_session_breaks(self):
        for source in list(self._file_handles.keys()):
            if self._rows_written_today.get(source, 0) > 0:
                self._session_separators.add(source)
                self._session_started[source] = False
    
    def _drain_snapshot_queue(self) -> List[MeterSnapshot]:
        """Drain pending snapshots from the bus queue."""
        snapshots: List[MeterSnapshot] = []
        if not self._snapshot_queue:
            return snapshots
        while True:
            try:
                snapshots.append(self._snapshot_queue.get_nowait())
            except queue.Empty:
                break
        return snapshots

    def _process_meter_snapshot(self, snapshot: MeterSnapshot):
        """Add a meter snapshot to the aggregation stats."""
        values = snapshot.values or {}
        meter_clone = self._logging_meters.get(snapshot.meter_id)
        self._update_logging_meter(snapshot)

        if not self._is_meter_logging_enabled(snapshot.meter_id, meter_clone):
            return

        source = f"M{snapshot.meter_id}"

        # Propagate ct_pt_warn quality flag from the snapshot into the pending
        # flags set so it gets stamped on the next emitted historian row.
        quality_reason = str(getattr(snapshot, "quality_reason", "") or "")
        if "ct_pt_warn" in quality_reason:
            self._pending_quality_flags.add("ct_pt_warn")

        self._aggregate_to_source(source, values, snapshot.quality)

    def _is_meter_logging_enabled(self, meter_id: int, meter_clone: Optional[MeterState]) -> bool:
        meters_cfg = self.cfg.get("logging", {}).get("sources", {}).get("meters", {})
        entry = meters_cfg.get("per_meter", {}).get(str(meter_id))
        if isinstance(entry, dict) and "enabled" in entry:
            return bool(entry.get("enabled"))
        if meter_clone is not None:
            return bool(getattr(meter_clone, "enabled", True))
        return True

    def _process_total_snapshot(self):
        """Compute and aggregate the total snapshot based on logged meter clones."""
        total_cfg = self.cfg.get("logging", {}).get("sources", {}).get("total", {})
        if not total_cfg.get("enabled", True):
            return
        if not self._logging_meters:
            return
        meters = list(self._logging_meters.values())
        total = self._total_aggregator.compute(meters, cfg=self.cfg) or {}
        quality = self._parse_quality(total.get("quality"))
        self._aggregate_to_source("TOTAL", total, quality)

    def _aggregate_to_source(self, source: str, values: Dict[str, Any], quality: MeterQuality):
        """Update aggregation stats for a given source and snapshot quality."""
        keys = self._get_keys_for_source(source)
        if not keys:
            keys = self.DEFAULT_KEYS
        if source not in self._agg_data:
            self._agg_data[source] = {k: AggStats() for k in keys}
            self._agg_sample_count[source] = 0
            self._agg_data_valid[source] = True
        self._agg_keys_map[source] = tuple(keys)
        self._source_quality[source] = self._max_quality(
            self._source_quality.get(source, MeterQuality.DISABLED), quality
        )

        if self._is_quality_good(quality):
            for key in keys:
                v = values.get(key)
                if isinstance(v, (int, float)):
                    if key not in self._agg_data[source]:
                        self._agg_data[source][key] = AggStats()
                    self._agg_data[source][key].add(float(v))
            self._agg_sample_count[source] = self._agg_sample_count.get(source, 0) + 1
        else:
            self._agg_data_valid[source] = False

    def _update_logging_meter(self, snapshot: MeterSnapshot):
        meter = self._logging_meters.get(snapshot.meter_id)
        if not meter:
            return
        meter.values = snapshot.values.copy() if isinstance(snapshot.values, dict) else {}
        meter.data_valid = snapshot.quality == MeterQuality.GOOD
        if meter.data_valid:
            meter.last_good_ts = snapshot.ts
            meter.first_data_received = True
            meter.comm_failure = False
        else:
            meter.comm_failure = snapshot.quality == MeterQuality.COMM_LOST
        meter.quality = snapshot.quality

    def _max_quality(self, current: MeterQuality, candidate: MeterQuality) -> MeterQuality:
        if current is None:
            return candidate
        if candidate is None:
            return current
        current_priority = self._quality_priority.get(current, 0)
        candidate_priority = self._quality_priority.get(candidate, 0)
        return candidate if candidate_priority > current_priority else current

    def _is_quality_good(self, quality: MeterQuality) -> bool:
        return quality == MeterQuality.GOOD

    def _parse_quality(self, quality: Any) -> MeterQuality:
        if isinstance(quality, MeterQuality):
            return quality
        if isinstance(quality, str):
            try:
                return MeterQuality(quality)
            except ValueError:
                pass
        return MeterQuality.DISABLED
    
    def _emit_rows(self, now: datetime):
        """Emit aggregated rows to the write queue."""
        mode = self.mode
        interval = self.interval_sec

        timestamp  = now.strftime("%Y-%m-%d %H:%M:%S")
        ts_epoch   = now.timestamp()
        mon_ts     = time.monotonic()  # capture monotonic at emit time

        # Snapshot and clear pending quality flags for this interval
        flags_this_interval = ",".join(sorted(self._pending_quality_flags))
        self._pending_quality_flags.clear()

        for source, stats_dict in self._agg_data.items():
            keys = self._agg_keys_map.get(source) or tuple(self._get_keys_for_source(source))
            sample_count = self._agg_sample_count.get(source, 0)
            data_valid = self._agg_data_valid.get(source, True) and sample_count > 0

            # Per-source quality flags (may add rollover/ct_pt_warn on top of interval flags)
            source_flags = flags_this_interval

            row = {
                "Timestamp":     timestamp,
                "_ts_epoch":     ts_epoch,    # used by historian; ignored by CSV DictWriter
                "_monotonic_ts": mon_ts,       # historian only
                "_quality_flags": source_flags,  # historian only
                "Source":        source,
                "IntervalSec":   interval,
                "Mode":          mode,
                "DataValid":     1 if data_valid else 0,
                "SampleCount":   sample_count,
                "Quality":       self._source_quality.get(source, MeterQuality.DISABLED).value,
            }

            for key in keys:
                if data_valid and key in stats_dict:
                    val = stats_dict[key].get(mode)
                    row[key] = f"{val:.4f}" if val is not None else ""
                else:
                    row[key] = ""

            # Queue for background write
            if source in self._session_separators:
                self._enqueue(("separator", source, keys))
                self._session_separators.discard(source)
            self._enqueue(("row", row, keys))

        # Reset aggregation
        self._agg_data.clear()
        self._agg_sample_count.clear()
        self._agg_data_valid.clear()
        self._source_quality.clear()
        self._agg_keys_map.clear()

    def _check_clock_jump(self, wall_ts: float, mon_ts: float) -> None:
        prev_wall = self._last_tick_wall_ts
        prev_mon = self._last_tick_mon_ts
        self._last_tick_wall_ts = float(wall_ts)
        self._last_tick_mon_ts = float(mon_ts)
        if prev_wall is None or prev_mon is None:
            return

        wall_delta = float(wall_ts) - float(prev_wall)
        mon_delta = float(mon_ts) - float(prev_mon)
        warn_s = float(((self.cfg.get("data_quality", {}) or {}).get("clock_jump_warn_s", 5.0)) or 5.0)
        if abs(wall_delta - mon_delta) <= warn_s:
            return

        self._clock_jump_count += 1
        self._interval_start = None
        self._pending_quality_flags.add("clock_jump")
        log_once(
            logger,
            "logging_clock_jump",
            "warning",
            f"System clock jump detected while logging (wall_delta={wall_delta:.1f}s monotonic_delta={mon_delta:.1f}s); interval alignment reset.",
            cooldown_sec=30,
        )

    def _enqueue(self, item: tuple) -> bool:
        try:
            self._queue.put_nowait(item)
            return True
        except queue.Full:
            self._queue_full_count += 1
            self._last_error = "Logging write queue full; row dropped"
            self._last_error_time = datetime.now()
            self._set_state(self.STATE_ERROR, self._last_error)
            log_once(
                logger,
                "logging_queue_full",
                "error",
                "Logging write queue full: dropping rows to protect application memory.",
                cooldown_sec=10,
            )
            return False
    
    def _writer_thread(self):
        """Background thread for crash-resilient file I/O.

        Batched flush strategy:
        - CSV rows are written immediately but flushed/fsynced at most every
          _FLUSH_INTERVAL_SEC seconds, not per-row.  This eliminates the
          per-row fsync bottleneck on slow USB/network storage while still
          giving a reasonable durability window.
        - SQLite historian uses WAL + commit-per-row (already fast enough).
        """
        _FLUSH_INTERVAL_SEC = 5.0
        _last_flush_ts = 0.0

        while not self._stop_event.is_set():
            try:
                item = self._queue.get(timeout=0.5)
            except queue.Empty:
                # Periodic flush on idle
                now = time.time()
                if (now - _last_flush_ts) >= _FLUSH_INTERVAL_SEC:
                    self._flush_csv_handles()
                    _last_flush_ts = now
                self._maybe_purge()
                continue

            if item is None:
                break

            cmd, payload, keys = item
            if cmd == "row":
                self._write_row(payload, keys)
            elif cmd == "separator":
                self._write_session_separator(payload, keys)

            # Periodic flush (not per-row)
            now = time.time()
            if (now - _last_flush_ts) >= _FLUSH_INTERVAL_SEC:
                self._flush_csv_handles()
                _last_flush_ts = now

        # Final flush on shutdown
        self._flush_csv_handles(fsync=True)
    
    def _write_row(self, row: dict, keys: list):
        """
        Write one aggregated row.

        Backend selection (cfg['logging']['backend']):
          'sqlite' — SQLite only  (default; smallest footprint, fastest queries)
          'csv'    — CSV only     (legacy; YYYY-MM-DD subfolder per day)
          'both'   — SQLite + CSV (transition mode; use for migration)
        """
        now    = datetime.now()
        source = row.get("Source", "UNKNOWN")
        backend = (self.cfg.get("logging", {}).get("backend") or "sqlite").lower()

        try:
            if backend not in ("sqlite", "csv", "both"):
                raise RuntimeError(f"Unsupported logging backend: {backend}")
            if backend == "sqlite" and self._historian is None:
                raise RuntimeError("SQLite historian is not available")

            # ── Energy rollover detection ──────────────────────────────────
            # Energy keys that accumulate (32-bit rollover ~429 k kWh).
            # A negative delta larger than ROLLOVER_THRESHOLD means rollover.
            _ENERGY_KEYS = ("Import_kWh", "Export_kWh", "Net_kWh", "Today_kWh",
                            "kWh", "kWh_import", "kWh_export")
            prev_energy = self._last_energy_val.setdefault(source, {})
            rollover_detected = False
            for ek in _ENERGY_KEYS:
                raw_val = row.get(ek)
                if raw_val is None or raw_val == "":
                    continue
                try:
                    cur_e = float(raw_val)
                except (TypeError, ValueError):
                    continue
                prev_e = prev_energy.get(ek)
                if prev_e is not None:
                    delta = cur_e - prev_e
                    if delta < -self._ROLLOVER_THRESHOLD_KWH:
                        rollover_detected = True
                        log_once(
                            logger,
                            f"rollover_{source}_{ek}",
                            "warning",
                            f"[LOG] Energy rollover detected — source={source} key={ek} "
                            f"prev={prev_e:.1f} cur={cur_e:.1f} delta={delta:.1f} kWh. "
                            f"Data continuity broken; downstream analytics should handle rollover.",
                            cooldown_sec=300,
                        )
                prev_energy[ek] = cur_e

            # Build final quality_flags string (merge interval-level + row-level flags)
            base_flags_str = str(row.get("_quality_flags") or "")
            flag_set = set(f for f in base_flags_str.split(",") if f)
            if rollover_detected:
                flag_set.add("rollover")
            quality_flags_final = ",".join(sorted(flag_set))

            # ── SQLite write ───────────────────────────────────────────────
            if backend in ("sqlite", "both") and self._historian is not None:
                values_dict: Dict[str, Any] = {}
                for k in keys:
                    v = row.get(k)
                    if v is not None and v != "":
                        try:
                            values_dict[k] = float(v)
                        except (TypeError, ValueError):
                            pass
                self._historian.write_row(
                    ts            = row["Timestamp"],
                    ts_epoch      = row.get("_ts_epoch") or now.timestamp(),
                    source        = source,
                    interval_s    = int(row.get("IntervalSec", self.interval_sec)),
                    mode          = str(row.get("Mode", self.mode)),
                    data_valid    = bool(row.get("DataValid", 1)),
                    sample_ct     = int(row.get("SampleCount", 0)),
                    quality       = str(row.get("Quality", "GOOD")),
                    serial_no     = self._get_serial_for_source(source),
                    values        = values_dict,
                    monotonic_ts  = float(row.get("_monotonic_ts") or 0.0),
                    quality_flags = quality_flags_final,
                )
                self._current_file_path = self._historian.db_path

            # ── CSV write ──────────────────────────────────────────────────
            if backend in ("csv", "both"):
                date_str = now.strftime("%Y-%m-%d")
                writer, fh, file_path = self._ensure_writer_for_source(
                    source, keys, date_str
                )
                row["SerialNo"] = self._get_serial_for_source(source)
                writer.writerow(row)
                # NOTE: flush/fsync is handled by _flush_csv_handles() on a
                # 5-second cadence from the writer thread — NOT per-row.
                self._current_file_path = file_path

            self._rows_written_today[source] = \
                self._rows_written_today.get(source, 0) + 1
            self._last_write_time = now
            self._last_success_time = now
            self._last_error      = ""
            self._session_started[source] = True

        except Exception as exc:
            self._write_error_count += 1
            self._last_error_time = now
            self._last_error = str(exc)
            log_once(
                logger,
                f"logging_write_error_{source}",
                "error",
                f"[LOG-WRITE] source={source} backend={backend} failed: {type(exc).__name__}: {exc}",
                cooldown_sec=5,
            )
            self._set_state(self.STATE_ERROR, str(exc))

    def _ensure_writer_for_source(self, source: str, keys: list, date_str: str):
        """Ensure a file writer exists for this source & date."""
        # IMPORTANT: treat blank string as "use default logs_dir()".
        # The UI may clear cfg['logging']['folder'] when "Storage (One Folder)"
        # is enabled. In that case, we must fall back to logs_dir() which
        # respects the base_dir override.
        folder = (self.cfg.get("logging", {}).get("folder", "") or "").strip() or logs_dir()
        day_folder = os.path.join(folder, date_str)
        os.makedirs(day_folder, exist_ok=True)

        serial = self._get_serial_for_source(source) or "unknown"
        safe_serial = "".join(c if c.isalnum() else "_" for c in serial)
        file_name = f"{source}_{safe_serial}_{date_str}.csv"
        file_path = os.path.join(day_folder, file_name)

        keys_tuple = tuple(keys)
        current_date = self._current_dates.get(source)
        needs_new_file = (
            source not in self._csv_writers
            or current_date != date_str
            or self._writer_keys.get(source) != keys_tuple
        )

        if needs_new_file:
            self._close_file(source)
            write_header = not os.path.exists(file_path) or os.path.getsize(file_path) == 0
            fh = open(file_path, "a", newline="", encoding="utf-8")
            fieldnames = ["Timestamp", "Source", "SerialNo", "IntervalSec", "Mode", "DataValid", "SampleCount", "Quality"] + list(keys)
            writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction='ignore')

            if write_header:
                self._write_site_header(source, fh, keys_tuple)
                writer.writeheader()
                fh.flush()  # flush header immediately so file is readable even before first data row

            self._file_handles[source] = fh
            self._csv_writers[source] = writer
            self._current_dates[source] = date_str
            self._writer_keys[source] = keys_tuple
            self._rows_written_today[source] = 0
            self._session_started[source] = False
        else:
            fh = self._file_handles[source]
            writer = self._csv_writers[source]

        return writer, fh, file_path

    def _write_session_separator(self, source: str, keys: Tuple[str, ...]):
        """Insert a clean header block when logging resumes mid-file."""
        try:
            now = datetime.now()
            date_str = now.strftime("%Y-%m-%d")
            writer, fh, _ = self._ensure_writer_for_source(source, list(keys), date_str)
            # Start a new comment block for clarity (operators open CSV in Excel).
            fh.write("\n")
            self._write_site_header(source, fh, keys)
            writer.writeheader()
            fh.flush()  # flush separator header immediately for readability
            self._session_started[source] = False
        except Exception as e:
            self._write_error_count += 1
            self._last_error = str(e)
            self._last_error_time = datetime.now()
            log_once(
                logger,
                f"logging_separator_error_{source}",
                "error",
                f"[LOG-SEPARATOR] source={source} failed: {type(e).__name__}: {e}",
                cooldown_sec=5,
            )
            self._set_state(self.STATE_ERROR, str(e))

    def _write_site_header(self, source: str, fh: Any, keys: Tuple[str, ...]):
        """Write site information + serials as a clean comment block.

        Format is intentionally Excel-friendly and human-readable.
        """
        site_cfg = self.cfg.get("site", {})
        plant_name = site_cfg.get("plant_name", "MFM384 Monitoring")
        location = site_cfg.get("location", "")
        description = site_cfg.get("description", "")

        # Parameter list (tab-separated to stay readable when copied).
        keys_list = list(keys) if keys else []
        if not keys_list:
            keys_list = self.DEFAULT_KEYS

        lines = [
            f"# Site: {plant_name}",
        ]
        if location:
            lines.append(f"# Location: {location}")
        if description:
            lines.append(f"# Description: {description}")

        lines.append(f"# Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append(f"# Source: {source}")
        serial = self._get_serial_for_source(source)
        if serial:
            lines.append(f"# SerialNo: {serial}")
        lines.append(f"# Parameters: {'	'.join(keys_list)}")
        lines.append("#")
        lines.append("# Meter Serial Numbers:")

        # Only show readable/known serials (avoid stale old serials confusing the operator).
        for m in self.meters:
            if not m.enabled:
                continue
            if not getattr(m, 'serial_no', ''):
                continue
            lines.append(f"#   Meter {m.meter_id} ({m.name}): {m.serial_no}")

        lines.append("#")
        lines.append("")  # Blank line before CSV header

        for line in lines:
            fh.write(line + '\n')
        fh.flush()

    def _get_serial_for_source(self, source: str) -> str:
        """Get serial number for a source like 'M1', 'TOTAL', etc."""
        if source == "TOTAL":
            return "TOTAL"
        
        # Extract meter ID from source like "M1", "M2"
        if source.startswith("M") and len(source) > 1:
            try:
                meter_id = int(source[1:])
                for m in self.meters:
                    if m.meter_id == meter_id:
                        return m.serial_no if m.serial_no else ""
            except ValueError:
                pass
        return ""
    
    def _flush_csv_handles(self, fsync: bool = False) -> None:
        """Flush (and optionally fsync) all open CSV file handles.

        Called periodically from the writer thread — NOT per-row.
        fsync=True only on shutdown to ensure final data is on disk.
        """
        for src, fh in list(self._file_handles.items()):
            try:
                fh.flush()
                if fsync:
                    os.fsync(fh.fileno())
            except Exception:
                pass

    def _close_file(self, source: Optional[str] = None):
        """Close current file handle for the given source (or all if None)."""
        if source is None:
            for src in list(self._file_handles.keys()):
                self._close_file(src)
            return

        prev_rows = self._rows_written_today.get(source, 0)
        fh = self._file_handles.pop(source, None)
        if fh:
            try:
                fh.flush()
                os.fsync(fh.fileno())
                fh.close()
            except Exception:
                pass
        self._csv_writers.pop(source, None)
        self._writer_keys.pop(source, None)
        self._current_dates.pop(source, None)
        self._rows_written_today.pop(source, None)
        self._session_started.pop(source, None)
        if prev_rows:
            self._session_separators.add(source)
    
    def _cleanup(self):
        """Cleanup on exit."""
        self.stop()
        if self._historian is not None:
            try:
                self._historian.close()
            except Exception:
                pass
