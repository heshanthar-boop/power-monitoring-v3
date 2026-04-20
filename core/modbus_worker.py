import time
import threading
from collections import defaultdict
from typing import Optional
from utils.threading_utils import StoppableThread
from utils.logger import setup_logger
from core.backoff import BackoffTimer

from core.comm_config import normalize_serial_config, serial_port_available, validate_serial_config
from core.data_quality import summarize_issues, validate_meter_values
from core.snapshot_bus import MeterSnapshot, MeterQuality, SnapshotBus
from core.event_journal import EventJournal
from core.key_registry import canonical_key, CANONICAL_KEY_LABELS
from utils.log_once import log_once

logger = setup_logger("modbus")


def _apply_ct_pt_scaling(values: dict, ct_ratio: float, pt_ratio: float) -> dict:
    """Apply CT and PT ratio multipliers to decoded meter values.

    The MFM384 measures at the instrument transformer secondary.
    If external CTs/PTs are used, the meter reads a scaled-down value.
    Multiply by the ratio to recover the primary (actual system) quantity.

    CT ratio (ct_ratio):  applied to current (I1, I2, I3, Iavg) and all
                          power values derived from them (kW, kVA, kVAr, kWh*).
    PT ratio (pt_ratio):  applied to voltages (V1N, V2N, V3N, Vavg, V12…).

    PF and Frequency are dimensionless — no scaling.

    ct_ratio = pt_ratio = 1.0 means no scaling (pass-through).
    """
    if ct_ratio == 1.0 and pt_ratio == 1.0:
        return values  # fast path: nothing to do

    out = dict(values)

    if ct_ratio != 1.0:
        # Current channels
        for k in ("I1", "I2", "I3", "Iavg"):
            if k in out and out[k] is not None:
                out[k] = out[k] * ct_ratio
        # Power and energy — scale by both CT and PT ratios (P = V * I)
        combined = ct_ratio * pt_ratio
        for k in ("kW", "kVA", "kVAr",
                  "Import_kWh", "Export_kWh", "Net_kWh", "Today_kWh",
                  "kW Active Power Max DMD", "kW Active Power Min DMD",
                  "kVAr Reactive Power Max DMD", "kVAr Reactive Power Min DMD",
                  "kVA Apparent Power Max DMD"):
            if k in out and out[k] is not None:
                out[k] = out[k] * combined

    if pt_ratio != 1.0:
        for k in ("V1N", "V2N", "V3N", "Vavg",
                  "V12", "V23", "V31"):
            if k in out and out[k] is not None:
                out[k] = out[k] * pt_ratio

    return out


def _normalize_values(meter_id: int, values):
    """Normalize mapping keys to canonical keys and log unusual keys once."""
    if not isinstance(values, dict):
        return {}
    out = {}
    unknown = []
    collisions = []
    for k, v in values.items():
        ck = canonical_key(k)
        if ck in out and out.get(ck) != v:
            collisions.append((str(k), ck))
        out[ck] = v
        orig = str(k).strip()
        # Only warn if canonical_key() did not map the key and it is not a known canonical key
        if ck and ck == orig and ck not in CANONICAL_KEY_LABELS:
            unknown.append(str(k))

    if unknown:
        log_once(logger, f"unknown_keys_m{meter_id}", "warning",
                 f"Meter {meter_id}: unknown map keys (showing up to 15): {unknown[:15]}")
    if collisions:
        log_once(logger, f"key_collision_m{meter_id}", "warning",
                 f"Meter {meter_id}: key collisions after canonicalization (showing up to 10): {collisions[:10]}")
    return out

def _check_ctpt_sanity(values: dict, meter, cfg: dict) -> str:
    """Return a short warning string if CT/PT-scaled values look implausible.

    Checks performed (all optional — silently skip if data is missing):
    1. Vavg vs nominal_vln: must be within ±40% of site nominal.
       Catches: wrong PT ratio, dead PT fuse, voltage wiring error.
    2. Iavg vs CT primary rating: must not exceed CT_ratio × CT_secondary_A × 1.2.
       Catches: CT ratio entered backwards (e.g. 5/200 instead of 200/5).
    3. kW vs apparent limits: kW should not exceed Vavg × Iavg × 3 × 1.05 (3-phase).
       Catches: combined CT+PT ratio error that slips past individual checks.

    Returns "" if everything looks OK, or a brief description of the first issue.
    Not a hard reject — it's a non-blocking flag written into quality_flags.

    Disabled entirely if cfg["data_quality"]["ctpt_sanity_enabled"] == False.
    """
    dq_cfg = (cfg or {}).get("data_quality", {}) or {}
    if not bool(dq_cfg.get("ctpt_sanity_enabled", True)):
        return ""

    site_cfg = (cfg or {}).get("site", {}) or {}
    nominal_vln = float(site_cfg.get("nominal_vln", 230.0) or 230.0)

    ct = float(getattr(meter, "ct_ratio", 1.0) or 1.0)
    pt = float(getattr(meter, "pt_ratio", 1.0) or 1.0)

    vavg = values.get("Vavg")
    iavg = values.get("Iavg")
    kw   = values.get("kW")

    # Effective primary voltage after PT scaling
    nominal_primary_v = nominal_vln * pt

    # 1. Voltage sanity: within ±40% of expected primary voltage
    if isinstance(vavg, (int, float)) and vavg > 0 and nominal_primary_v > 0:
        ratio = vavg / nominal_primary_v
        if ratio < 0.60 or ratio > 1.40:
            return (
                f"Vavg={vavg:.1f}V is {ratio*100:.0f}% of expected "
                f"{nominal_primary_v:.0f}V (nominal×PT_ratio). "
                f"Check PT ratio (currently {pt:.3g})."
            )

    # 2. Current sanity: Iavg should not massively exceed CT primary rating.
    # We don't know the CT secondary A, but we can check if Iavg > ct_ratio × 10
    # (assumes ≤10 A secondary, which covers 1A and 5A CTs).  Anything beyond
    # that is almost certainly a wrong ratio entry.
    if isinstance(iavg, (int, float)) and iavg > 0 and ct > 1.0:
        ct_primary_max = ct * 10.0 * 1.2  # 10 A secondary × 120% over-range
        if iavg > ct_primary_max:
            return (
                f"Iavg={iavg:.1f}A exceeds CT primary ceiling "
                f"({ct_primary_max:.0f}A = ratio {ct:.3g}×10A×120%). "
                f"Check CT ratio (currently {ct:.3g})."
            )

    # 3. Power consistency: kW should not exceed 3-phase VA ceiling by >5%
    if (isinstance(kw, (int, float)) and isinstance(vavg, (int, float))
            and isinstance(iavg, (int, float)) and vavg > 0 and iavg > 0):
        va_ceil = vavg * iavg * 3.0 * 1.05  # 5% tolerance
        # Only flag if kW > VA ceiling by more than a rounding factor
        if abs(kw) > va_ceil and va_ceil > 0:
            return (
                f"kW={kw:.1f} exceeds 3-phase VA ceiling "
                f"({va_ceil:.1f}kVA). Likely combined CT+PT ratio error."
            )

    return ""


try:
    from pymodbus.client import ModbusSerialClient
except Exception:
    ModbusSerialClient = None

try:
    from pymodbus.client import ModbusTcpClient
except Exception:
    ModbusTcpClient = None

class ModbusWorker(StoppableThread):
    """
    One worker thread polling multiple meters with per-meter scan intervals.
    No UI access inside this thread.
    
    States: DISCONNECTED, CONNECTING, CONNECTED, COM_BUSY, ERROR
    """
    
    # State constants
    STATE_DISCONNECTED = "DISCONNECTED"
    STATE_CONNECTING = "CONNECTING"
    STATE_CONNECTED = "CONNECTED"
    STATE_COM_BUSY = "COM_BUSY"

    # Bus-level link state (operator-facing)
    BUS_OK = "OK"
    BUS_STALE = "STALE"
    BUS_RECOVERING = "RECOVERING"
    BUS_OFFLINE = "OFFLINE"
    STATE_ERROR = "ERROR"
    
    def __init__(
        self,
        meters,
        serial_cfg: dict,
        alarms_cfg: dict,
        read_map_fn,
        raw_read_fn=None,
        snapshot_bus: Optional[SnapshotBus] = None,
        data_store=None,
        event_journal: Optional[EventJournal] = None,
        journal_snapshot_provider=None,
        cfg: Optional[dict] = None,
    ):
        super().__init__()
        self._scan_in_progress = False  # prevent poll/scan contention
        self._busy_task = False  # long client task (setup reads etc)
        self._pause_polling = False  # pause normal polling during long UI operations
        self.meters = meters
        self.serial_cfg = normalize_serial_config(serial_cfg)
        try:
            serial_cfg.update(self.serial_cfg)
        except Exception:
            pass
        self.cfg = cfg or {}
        self.read_map_fn = read_map_fn
        self.raw_read_fn = raw_read_fn
        self.snapshot_bus = snapshot_bus
        self._last_snapshot_ts = {}  # slave_id -> last publish time (monotonic)
        self._snapshot_min_interval_s = float(serial_cfg.get('snapshot_min_interval_s', alarms_cfg.get('snapshot_min_interval_s', 1.0)))
        self.data_store = data_store
        self.event_journal = event_journal
        self._journal_snapshot_provider = journal_snapshot_provider
        self._last_journal_bus_state = None

        self.client = None
        self.connected = False
        
        # Thread-safe status variables
        self._status_lock = threading.Lock()
        # RLock avoids deadlocks when bus helpers call each other while holding the lock.
        self._bus_lock = threading.RLock()
        self._status = self.STATE_DISCONNECTED
        self._last_error = ""
        
        # Client lock for thread-safe Modbus access (setup tab shares this)
        self._client_lock = threading.Lock()

        # Prevent overlapping connect/disconnect attempts.
        # Without this, UI clicks + auto-reconnect can race and open/close COM twice,
        # causing PermissionError(13) and/or reads on a half-closed serial socket.
        self._connect_lock = threading.Lock()
        
        # Auto-reconnect with backoff
        # auto_connect means "auto attempt to connect/reconnect".
        # user_disconnect means "operator pressed Disconnect" → never auto-reconnect
        # until operator presses Connect again.
        self.auto_connect = bool(serial_cfg.get("auto_connect", True))
        self.user_disconnect = False
        self._backoff = BackoffTimer(
            initial_sec=float(self.serial_cfg.get("backoff_initial_sec", 1.0)),
            max_sec=float(self.serial_cfg.get("backoff_max_sec", 30.0)),
            jitter_ratio=float(self.serial_cfg.get("backoff_jitter", 0.15)),
        )

        # Watchdog / data quality thresholds (seconds)
        # - stale_seconds: used by UI/engines to mark STALE
        # - watchdog_seconds: if no successful bus read for this long, force reconnect
        self.stale_seconds = float(serial_cfg.get("stale_seconds", 5.0))
        self.watchdog_seconds = float(serial_cfg.get("watchdog_seconds", 12.0))

        # When UI holds the Modbus client lock for long operations (setup reads, scans),
        # we temporarily suspend watchdog/supervisor-driven restarts to avoid false recovery.
        self._suspend_watchdog_until = 0.0

        self._next_poll = {m.meter_id: 0.0 for m in meters}
        self._last_no_meter_warn_ts = 0.0  # rate-limit noisy 'no meters' warnings
        self.bus_ok_count = 0
        self.bus_fail_count = 0
        self.last_bus_ok_ts = 0.0
        self._set_bus_state(self.BUS_OFFLINE, reason="bus_state")
        self.bus_recover_attempts = 0
        self.bus_next_retry_ts = 0.0
        self.bus_reconnect_threshold = max(1, int(self.serial_cfg.get("bus_reconnect_threshold", 3)))
        self.default_timeout = float(self.serial_cfg.get("timeout", 1))
        self.default_retries = max(1, int(self.serial_cfg.get("meter_retry_count", 2)))
        self.port_check_interval_sec = float(self.serial_cfg.get("port_check_interval_sec", 2.0))
        self.latency_warn_ms = float(self.serial_cfg.get("latency_warn_ms", 1000.0))

        self._comm_metrics_lock = threading.RLock()
        self._latency_ewma_ms = 0.0
        self._last_poll_latency_ms = 0.0
        self._total_poll_ok = 0
        self._total_poll_fail = 0
        self._meter_latency_ms = {}
        self._meter_value_count = {}
        self._meter_last_error = {}
        self._meter_last_ok_ts = {}
        self._meter_fail_counts = defaultdict(int)
        self._meter_ok_counts = defaultdict(int)
        self._last_port_check_ts = 0.0
        self._last_port_available = None

        logger.info(f"ModbusWorker init: auto_connect={self.auto_connect}, port={self.serial_cfg.get('port', 'None')}")

        # Heartbeat (for Supervisor): updated every loop iteration.
        self._hb_lock = threading.Lock()
        self._heartbeat_ts = time.time()

        # Per-meter last successful RX timestamp (safe getter for UI/diagnostics).
        self._rx_lock = threading.Lock()
        self._last_rx_ts = {}

    @property
    def status(self) -> str:
        """Thread-safe status getter."""
        with self._status_lock:
            return self._status
    
    @property
    def last_error(self) -> str:
        """Thread-safe last_error getter."""
        with self._status_lock:
            return self._last_error
    
    def _set_status(self, new_status: str, error_msg: str = ""):
        """Set status and log only on state changes."""
        with self._status_lock:
            old_status = self._status
            self._status = new_status
            self._last_error = error_msg
            
            # Log only on state change
            if old_status != new_status:
                if error_msg:
                    logger.warning(f"Status: {old_status} -> {new_status} ({error_msg})")
                else:
                    logger.info(f"Status: {old_status} -> {new_status}")

    # ------------------------------------------------------------------
    # BUS state helpers (operator-facing + journaling)
    # ------------------------------------------------------------------
    def _set_bus_state(self, new_state: str, reason: str = "") -> None:
        """Set bus state and (optionally) journal transitions.

        Important: may be called from multiple places, sometimes while holding
        self._bus_lock. We use RLock so this is safe.
        """
        try:
            with self._bus_lock:
                old = getattr(self, "_bus_state", None)
                self._bus_state = str(new_state)
                self._bus_state_reason = str(reason or "")
                self._bus_state_ts = time.time()

                # Journal only on transitions (avoid flooding DB).
                if self.event_journal and (old != self._bus_state):
                    try:
                        data = {
                            "reason": self._bus_state_reason,
                            "connected": bool(self.connected),
                            "status": str(self.status),
                            "attempts": int(self.bus_recover_attempts or 0),
                            "next_retry_in": float(self.get_reconnect_countdown() or 0.0),
                        }
                        self.event_journal.log_comm("BUS", self._bus_state, message=self._bus_state_reason or self._bus_state, data=data)
                        self._last_journal_bus_state = self._bus_state
                    except Exception:
                        # Journal is secondary; never let it break comms.
                        pass
        except Exception:
            # Never raise from a state helper.
            return

    def _record_bus_success(self):
        with self._bus_lock:
            self.bus_ok_count += 1
            self.bus_fail_count = 0
            self.last_bus_ok_ts = time.time()
            self._set_bus_state(self.BUS_OK, reason="bus_state")
            self.bus_recover_attempts = 0
            self.bus_next_retry_ts = 0.0

    def _record_bus_failure(self):
        with self._bus_lock:
            self.bus_fail_count += 1
            self.bus_ok_count = 0

    def _close_client(self):
        # Always close under the client lock to avoid racing with an in-flight read.
        with self._client_lock:
            if self.client:
                try:
                    self.client.close()
                except Exception:
                    pass
                self.client = None

    def _is_client_issue(self, exc: Exception) -> bool:
        text = str(exc).lower()
        return ("connection" in text) or ("socket" in text) or ("port" in text) or ("permission" in text) or ("device" in text)

    def _release_connect_lock(self) -> None:
        try:
            self._connect_lock.release()
        except Exception:
            pass

    def _serial_port_available_cached(self, *, force: bool = False):
        transport = str(self.serial_cfg.get("transport", "rtu") or "rtu").strip().lower()
        if transport == "tcp":
            return True
        now = time.time()
        if (not force) and self._last_port_check_ts and (now - self._last_port_check_ts) < self.port_check_interval_sec:
            return self._last_port_available
        port = str(self.serial_cfg.get("port", "") or "").strip()
        available = serial_port_available(port)
        self._last_port_check_ts = now
        self._last_port_available = available
        return available

    def _check_live_transport(self) -> bool:
        """Detect disappeared USB-RS485 adapters while connected."""
        if str(self.serial_cfg.get("transport", "rtu") or "rtu").strip().lower() == "tcp":
            return True
        available = self._serial_port_available_cached()
        if available is False:
            port = str(self.serial_cfg.get("port", "") or "").strip() or "COM port"
            self.connected = False
            self.bus_recover_attempts += 1
            self._set_status(self.STATE_ERROR, f"{port} not available - check USB-RS485 adapter")
            self._set_bus_state(self.BUS_RECOVERING if (self.auto_connect and not self.user_disconnect) else self.BUS_OFFLINE, reason="port_missing")
            self._backoff.schedule_next()
            with self._bus_lock:
                self.bus_next_retry_ts = float(getattr(self._backoff, "next_attempt_time", 0.0) or 0.0)
            self._close_client()
            return False
        return True

    def _record_meter_success(self, meter, latency_ms: float, value_count: int) -> None:
        mid = int(getattr(meter, "meter_id", 0) or 0)
        with self._comm_metrics_lock:
            self._total_poll_ok += 1
            self._meter_ok_counts[mid] += 1
            self._meter_last_ok_ts[mid] = time.time()
            self._meter_latency_ms[mid] = float(latency_ms)
            self._meter_value_count[mid] = int(value_count)
            self._meter_last_error[mid] = ""
            self._last_poll_latency_ms = float(latency_ms)
            if self._latency_ewma_ms <= 0:
                self._latency_ewma_ms = float(latency_ms)
            else:
                self._latency_ewma_ms = (self._latency_ewma_ms * 0.8) + (float(latency_ms) * 0.2)
        if latency_ms > self.latency_warn_ms:
            log_once(
                logger,
                f"meter_latency_m{mid}",
                "warning",
                f"Meter {mid}: slow Modbus response {latency_ms:.0f} ms",
                cooldown_sec=30,
            )

    def _record_meter_failure(self, meter, exc: Exception, backoff: float) -> None:
        mid = int(getattr(meter, "meter_id", 0) or 0)
        with self._comm_metrics_lock:
            self._total_poll_fail += 1
            self._meter_fail_counts[mid] += 1
            self._meter_last_error[mid] = f"{type(exc).__name__}: {exc}"

    def _beat(self) -> None:
        """Update heartbeat timestamp."""
        with self._hb_lock:
            self._heartbeat_ts = time.time()

    def get_heartbeat_age_s(self) -> float:
        """Seconds since last heartbeat update (0 if unknown)."""
        try:
            with self._hb_lock:
                hb = float(self._heartbeat_ts or 0.0)
            if hb <= 0:
                return 0.0
            return max(0.0, time.time() - hb)
        except Exception:
            return 0.0

    def _read_meter_values(self, meter):
        attempts = max(1, int(getattr(meter, "retry_count", self.default_retries)))
        timeout_sec = getattr(meter, "timeout_sec", self.default_timeout)
        original_timeout = getattr(self.client, "timeout", None) if self.client else None
        last_exc = None

        try:
            if not self._is_client_ready():
                raise RuntimeError("Modbus client socket unavailable")
            for attempt in range(attempts):
                try:
                    with self._client_lock:
                        if not self._is_client_ready():
                            raise RuntimeError("Modbus client socket unavailable")
                        if timeout_sec is not None and self.client is not None:
                            self.client.timeout = timeout_sec
                        return self.read_map_fn(self.client, meter.slave_id)
                except Exception as exc:
                    last_exc = exc
                    if attempt + 1 < attempts:
                        time.sleep(min(0.25, 0.05 * (attempt + 1)))
                        continue
                    raise
        finally:
            if original_timeout is not None and self.client is not None:
                self.client.timeout = original_timeout

    def _is_client_ready(self) -> bool:
        if not self.client:
            return False
        # TCP client: pymodbus provides is_socket_open()
        if hasattr(self.client, "is_socket_open"):
            try:
                return bool(self.client.is_socket_open())
            except Exception:
                pass
        # TCP fallback: check .socket attribute
        sock = getattr(self.client, "socket", None)
        if sock is not None:
            return True
        # RTU / serial client: check the underlying serial port is open.
        # pymodbus serial clients expose the port as .comm_params.host on
        # v3.x, but the serial.Serial object is at .framer.client.socket or
        # directly at .socket on v2.x. The safest cross-version check is the
        # transport's serial port object.
        serial_port = (
            getattr(self.client, "serial", None)            # pymodbus 2.x RTU
            or getattr(getattr(self.client, "transport", None), "serial", None)  # pymodbus 3.x
        )
        if serial_port is not None:
            try:
                return bool(serial_port.isOpen() if hasattr(serial_port, "isOpen") else serial_port.is_open)
            except Exception:
                return False
        # Unknown client type — assume ready only if .connected flag says so.
        return bool(getattr(self.client, "connected", False))

    def _mark_meter_comm_lost(self, meter, error_text: str = "") -> None:
        """Update meter metadata immediately on poll failure.

        This keeps meter quality/state authoritative even when SnapshotBus
        consumers lag or drop updates.
        """
        try:
            meter.data_valid = False
            meter.quality = MeterQuality.COMM_LOST
            meter.last_error = str(error_text or "")
        except Exception:
            pass

    def _handle_poll_error(self, exc: Exception) -> None:
        self._record_bus_failure()
        need_reconnect = False

        if self._is_client_issue(exc):
            need_reconnect = True
            reason = "client error"
        elif self.bus_fail_count >= self.bus_reconnect_threshold:
            need_reconnect = True
            reason = f"{self.bus_fail_count} consecutive bus failures"
        else:
            reason = f"bus failure ({self.bus_fail_count})"

        if need_reconnect:
            self.connected = False
            self._set_status(self.STATE_ERROR, f"Bus error ({reason}): {type(exc).__name__}: {exc}")
            self._backoff.schedule_next()
            self._close_client()
    
    def connect(self) -> bool:
        """Attempt connection with proper error handling."""
        # Avoid overlapping connects (UI + auto-reconnect).
        if not self._connect_lock.acquire(blocking=False):
            logger.debug("[CONNECT] Connect already in progress - skipping")
            return False

        logger.info(f"[CONNECT] Starting connection attempt")
        self.serial_cfg = normalize_serial_config(self.serial_cfg)

        # Operator is explicitly requesting a connection.
        self.user_disconnect = False
        self.auto_connect = True
        
        # Determine transport: "tcp" = Modbus TCP gateway; anything else = RTU/serial
        transport = str(self.serial_cfg.get("transport", "rtu")).lower().strip()
        is_tcp = (transport == "tcp")

        if is_tcp:
            if ModbusTcpClient is None:
                logger.error("[CONNECT] pymodbus not available (TCP)")
                self._set_status(self.STATE_ERROR, "pymodbus not available")
                self._release_connect_lock()
                return False
        else:
            if ModbusSerialClient is None:
                logger.error("[CONNECT] pymodbus not available (Serial)")
                self._set_status(self.STATE_ERROR, "pymodbus not available")
                self._release_connect_lock()
                return False

        if is_tcp:
            tcp_host = str(self.serial_cfg.get("tcp_host", "") or "").strip()
            tcp_port = int(self.serial_cfg.get("tcp_port", 502) or 502)
            logger.info(f"[CONNECT] TCP transport — host={tcp_host} port={tcp_port}")
            if not tcp_host:
                logger.warning("[CONNECT] No TCP host configured")
                self._set_status(self.STATE_DISCONNECTED, "No TCP host configured")
                self._release_connect_lock()
                return False
        else:
            port = self.serial_cfg.get("port") or ""
            baud = self.serial_cfg.get("baud", 9600)
            logger.info(f"[CONNECT] Port: {port}, Baud: {baud}")
            if not port or port == "None":
                logger.warning("[CONNECT] No COM port selected")
                self._set_status(self.STATE_DISCONNECTED, "No COM port selected")
                self._release_connect_lock()
                return False
            port_available = self._serial_port_available_cached(force=True)
            if port_available is False:
                msg = f"{port} not available - check USB-RS485 adapter"
                logger.warning("[CONNECT] %s", msg)
                self.bus_recover_attempts += 1
                self._set_bus_state(self.BUS_RECOVERING if (self.auto_connect and not self.user_disconnect) else self.BUS_OFFLINE, reason="port_missing")
                self._set_status(self.STATE_ERROR, msg)
                self._release_connect_lock()
                return False

        self._set_status(self.STATE_CONNECTING)
        if not self.user_disconnect:
            self._set_bus_state(self.BUS_RECOVERING, reason="bus_state")

        try:
            # Close existing client if any (under lock)
            self._close_client()

            if is_tcp:
                logger.info(f"[CONNECT] Creating ModbusTcpClient for {tcp_host}:{tcp_port}")
                with self._client_lock:
                    self.client = ModbusTcpClient(
                        host=tcp_host,
                        port=tcp_port,
                        timeout=self.default_timeout,
                    )
            else:
                logger.info(f"[CONNECT] Creating ModbusSerialClient for {port}")
                # Create client under lock so polling/setup can't use a half-built client.
                with self._client_lock:
                    self.client = ModbusSerialClient(
                        port=port,
                        baudrate=int(self.serial_cfg.get("baud", 9600)),
                        parity=str(self.serial_cfg.get("parity", "N")),
                        stopbits=int(self.serial_cfg.get("stopbits", 1)),
                        bytesize=8,
                        timeout=self.default_timeout,
                    )

            # Optional: limit FC04 read size for adapters/firmwares that fail on large frames.
            try:
                self.client._mfm384_max_regs = int(self.serial_cfg.get("max_regs_per_read", 60))
            except Exception:
                self.client._mfm384_max_regs = 60

            logger.info(f"[CONNECT] Attempting to connect...")
            # Connect under lock so disconnect cannot close mid-connect.
            with self._client_lock:
                self.connected = bool(self.client.connect())
            
            if self.connected:
                logger.info(f"[CONNECT] Connected successfully!")
                self._set_status(self.STATE_CONNECTED)
                self._backoff.reset()  # Reset backoff on success
                self._record_bus_success()  # seed watchdog
            else:
                logger.warning(f"[CONNECT] Failed to connect to {port}")
                self.bus_recover_attempts += 1
                self._set_bus_state(self.BUS_RECOVERING if (self.auto_connect and not self.user_disconnect) else self.BUS_OFFLINE, reason="bus_state")
                # next retry determined by backoff scheduler in run loop
                self._set_status(self.STATE_ERROR, f"Failed to connect to {port}")
            
            return self.connected
            
        except PermissionError as e:
            # COM port busy (held by another app)
            logger.error(f"[CONNECT] Permission denied (port busy): {e}")
            self.bus_recover_attempts += 1
            self._set_bus_state(self.BUS_RECOVERING if (self.auto_connect and not self.user_disconnect) else self.BUS_OFFLINE, reason="bus_state")
            self._set_status(self.STATE_COM_BUSY, f"{port} busy - close other app using this port")
            return False
        except Exception as e:
            # Generic error (wrong port, hardware issue, etc.)
            logger.error(f"[CONNECT] Connection error: {type(e).__name__}: {str(e)}")
            self.bus_recover_attempts += 1
            self._set_bus_state(self.BUS_RECOVERING if (self.auto_connect and not self.user_disconnect) else self.BUS_OFFLINE, reason="bus_state")
            self._set_status(self.STATE_ERROR, f"Connection error: {str(e)}")
            return False
        finally:
            try:
                self._connect_lock.release()
            except Exception:
                pass

    def disconnect(self):
        """Safe disconnect.

        If operator presses Disconnect, we must NOT auto-reconnect.
        """
        # Operator-requested disconnect: stop auto-reconnect and safely close the client.
        self.user_disconnect = True
        self.auto_connect = False

        # Mark disconnected first so polling loop will not start new reads.
        self.connected = False
        self._set_status(self.STATE_DISCONNECTED)

        # Wait for any in-flight read to finish, then close.
        with self._client_lock:
            try:
                if self.client:
                    self.client.close()
            except Exception:
                pass
            self.client = None

        # Publish COMM_LOST for enabled meters so UI / protections reflect reality.
        if self.snapshot_bus:
            now = time.time()
            now_mon = time.monotonic()
            for m in self.meters:
                if not getattr(m, "enabled", True):
                    continue
                last_values = m.values.copy() if isinstance(m.values, dict) else {}
                snapshot = MeterSnapshot(
                    meter_id=m.meter_id,
                    ts=now,
                    monotonic_ts=now_mon,
                    values=last_values,
                    quality=MeterQuality.COMM_LOST,
                    error="Disconnected",
                )
                try:
                    if self.data_store is not None:
                        self.data_store.update_from_snapshot(
                            snapshot,
                            enabled=bool(getattr(m, "enabled", True)),
                            connected=False,
                            last_good_ts=getattr(m, "last_good_ts", None),
                        )
                except Exception:
                    pass
                self.snapshot_bus.publish(snapshot)

    def run(self):
        """Main worker loop with auto-reconnect and polling."""
        logger.info("ModbusWorker started")
        
        # Initial connection attempt if auto_connect enabled
        if self.auto_connect:
            self.connect()
        
        while not self.stop_event.is_set():
            self._beat()
            now = time.time()

            # If nothing is enabled yet, do NOT let watchdog force reconnect loops.
            # Operator can enable meters in Setup/Communication first.
            enabled_count = sum(1 for m in self.meters if getattr(m, 'enabled', False))

            # Auto-reconnect (SCADA-grade): if operator did not press Disconnect,
            # attempt reconnect with exponential backoff. Never hammer the COM port.
            if (not self.connected) and self.auto_connect and (not self.user_disconnect):
                with self._bus_lock:
                    self._set_bus_state(self.BUS_RECOVERING, reason="bus_state")
                    self.bus_next_retry_ts = float(getattr(self._backoff, 'next_attempt_time', 0.0) or 0.0)
                if self._backoff.can_attempt_now():
                    ok = False
                    try:
                        ok = bool(self.connect())
                    except Exception as e:
                        self._set_status(self.STATE_ERROR, f"Reconnect failed: {e}")
                        ok = False
                    if ok:
                        with self._bus_lock:
                            self._set_bus_state(self.BUS_STALE, reason="bus_state")
                            self.bus_next_retry_ts = 0.0
                        self._backoff.reset()
                    else:
                        self._backoff.schedule_next()
                        with self._bus_lock:
                            self.bus_next_retry_ts = float(getattr(self._backoff, 'next_attempt_time', 0.0) or 0.0)
                # Yield slightly to keep CPU low
                self.stop_event.wait(0.05)
                continue

            # Watchdog: if connected but no successful bus read for too long, force reconnect.
            # Suspended during long UI-held client operations (setup reads / scans).
            if (now >= self._suspend_watchdog_until) and self.connected and enabled_count > 0 and self.last_bus_ok_ts and (now - self.last_bus_ok_ts) > self.watchdog_seconds:
                self.connected = False
                with self._bus_lock:
                    self._set_bus_state(self.BUS_RECOVERING, reason="bus_state")
                    self.bus_recover_attempts += 1
                self._set_status(self.STATE_ERROR, f"Watchdog: no successful read for {now - self.last_bus_ok_ts:.1f}s")
                self._backoff.schedule_next()
                with self._bus_lock:
                    self.bus_next_retry_ts = float(getattr(self._backoff, 'next_attempt_time', 0.0) or 0.0)
                self._close_client()
                # Yield a bit and let reconnect logic run
                self.stop_event.wait(0.2)
                continue

            # Manual-disconnect / auto-connect disabled idle path.
            if not self.connected:
                self.stop_event.wait(0.2)
                continue

            if self.connected and not self._check_live_transport():
                self.stop_event.wait(0.2)
                continue

            # If UI is running a long client task (Setup Read/Write), pause polling to avoid
            # lock contention and false disconnects on slow PCs / long register blocks.
            if getattr(self, "_pause_polling", False) or getattr(self, "_busy_task", False):
                self.stop_event.wait(0.1)
                continue

            # Poll enabled meters
            enabled_count = sum(1 for m in self.meters if m.enabled)
            if enabled_count == 0:
                now2 = time.time()
                if now2 - self._last_no_meter_warn_ts > 10.0:
                    logger.warning("[POLL] No enabled meters! (enable meters or fix auto-scan)")
                    self._last_no_meter_warn_ts = now2
                # idle slower when nothing to poll (reduces CPU + log spam)
                self.stop_event.wait(1.0)
                continue

            # If a scan is in progress, pause polling to avoid bus contention
            if getattr(self, "_scan_in_progress", False):
                self.stop_event.wait(0.2)
                continue

            # Per-cycle health: we only treat it as a BUS fault if *no* meters succeed in this cycle.
            any_success = False
            any_polled = 0
            last_exc = None
            client_issue = False

            for m in self.meters:
                if not m.enabled:
                    logger.debug(f"[POLL] Meter {m.meter_id} disabled, skipping")
                    continue
                # Per-meter backoff: never hammer a dead/slave-id and never silently stall.
                suspend_until = float(getattr(m, "suspend_until_ts", 0.0) or 0.0)
                if suspend_until and now < suspend_until:
                    continue
                if now < self._next_poll[m.meter_id]:
                    continue

                self._next_poll[m.meter_id] = now + max(1, int(m.scan_interval_sec))
                try:
                    m.last_poll_ts = now
                except Exception:
                    pass

                try:
                    any_polled += 1
                    logger.debug(f"[POLL] Reading Meter {m.meter_id} (slave_id={m.slave_id})")
                    poll_start = time.monotonic()
                    
                    # Use the hardened read path so normal polling gets the same
                    # timeout override + retry behavior as manual/client tasks.
                    values = self._read_meter_values(m)

                    # Normalize keys (canonical registry) and report mapping issues once.
                    values = _normalize_values(m.meter_id, values)

                    # Apply CT/PT ratio scaling if configured (default 1.0 = no-op)
                    ct = float(getattr(m, "ct_ratio", 1.0) or 1.0)
                    pt = float(getattr(m, "pt_ratio", 1.0) or 1.0)
                    if ct != 1.0 or pt != 1.0:
                        values = _apply_ct_pt_scaling(values, ct, pt)

                    # ── CT/PT sanity check ─────────────────────────────────
                    # After scaling, verify that the resulting values are
                    # physically plausible given the site nominal voltage and
                    # CT primary rating.  Catches: wrong ratio entry, open CT,
                    # wiring swaps that survive the generic range check.
                    ctpt_warn = _check_ctpt_sanity(values, m, self.cfg)
                    if ctpt_warn:
                        log_once(
                            logger,
                            f"ctpt_sanity_m{m.meter_id}",
                            "warning",
                            f"Meter {m.meter_id}: CT/PT sanity warning — {ctpt_warn}",
                            cooldown_sec=60,
                        )

                    dq_issues = []
                    try:
                        values, dq_issues = validate_meter_values(values, self.cfg, meter_id=m.meter_id)
                    except Exception:
                        dq_issues = []
                    quality_reason = summarize_issues(dq_issues) if dq_issues else ""
                    # Append CT/PT warning to quality_reason so it surfaces in the snapshot
                    if ctpt_warn:
                        quality_reason = (quality_reason + "; " if quality_reason else "") + "ct_pt_warn: " + ctpt_warn
                    sample_quality = MeterQuality.GOOD if values else MeterQuality.STALE
                    if dq_issues:
                        log_once(
                            logger,
                            f"data_quality_m{m.meter_id}",
                            "warning",
                            f"Meter {m.meter_id}: rejected {len(dq_issues)} invalid value(s): {quality_reason}",
                            cooldown_sec=30,
                        )

                    logger.debug(f"[POLL] Meter {m.meter_id}: Got {len(values)} values")
                    latency_ms = max(0.0, (time.monotonic() - poll_start) * 1000.0)
                    self._record_meter_success(m, latency_ms, len(values))

                    # Diagnostics (if SnapshotBus path is used, MeterState won't see update_good)
                    try:
                        m.poll_count = int(getattr(m, "poll_count", 0)) + 1
                        m.ok_count = int(getattr(m, "ok_count", 0)) + 1
                        m.consecutive_fail = 0
                        m.suspend_until_ts = 0.0
                    except Exception:
                        pass

                    snapshot = MeterSnapshot(
                        meter_id=m.meter_id,
                        ts=time.time(),
                        monotonic_ts=time.monotonic(),
                        values=values.copy() if isinstance(values, dict) else {},
                        quality=sample_quality,
                        quality_reason=quality_reason,
                        raw_regs=None,
                    )

                    # Per-meter RX timestamp (for diagnostics / supervisor).
                    try:
                        with self._rx_lock:
                            self._last_rx_ts[int(m.meter_id)] = float(snapshot.ts)
                    except Exception:
                        pass

                    # Authoritative latest-value store (SCADA-grade):
                    # Always update, even if SnapshotBus queues drop.
                    try:
                        if self.data_store is not None:
                            self.data_store.update_from_snapshot(
                                snapshot,
                                enabled=bool(getattr(m, "enabled", True)),
                                connected=bool(self.connected),
                                last_good_ts=(float(snapshot.ts) if sample_quality == MeterQuality.GOOD else getattr(m, "last_good_ts", None)),
                            )
                    except Exception:
                        pass

                    # IMPORTANT: update meter metadata here (worker thread) so freshness/quality
                    # is not dependent on SnapshotBus delivery (which can drop if UI lags).
                    try:
                        if sample_quality == MeterQuality.GOOD:
                            m.last_good_ts = float(snapshot.ts)
                            m.last_good_mon = float(snapshot.monotonic_ts or time.monotonic())
                            m.data_valid = True
                            m.first_data_received = True
                            m.quality = MeterQuality.GOOD
                            m.last_error = ""
                        else:
                            m.data_valid = False
                            m.first_data_received = True
                            m.quality = sample_quality
                            m.last_error = quality_reason or "No valid values"
                    except Exception:
                        pass

                    # Per-meter RX timestamp for diagnostics
                    try:
                        with self._rx_lock:
                            self._last_rx_ts[int(m.meter_id)] = float(snapshot.ts)
                    except Exception:
                        pass
                    if self.snapshot_bus:
                        self.snapshot_bus.publish(snapshot)
                    else:
                        m.apply_snapshot(snapshot)

                    any_success = True
                    self._record_bus_success()
                    logger.debug(f"[POLL] Meter {m.meter_id}: Updated successfully")

                except Exception as e:
                    # Per-meter diagnostics + exponential backoff (prevents infinite fail loop).
                    try:
                        m.poll_count = int(getattr(m, "poll_count", 0)) + 1
                        m.fail_count = int(getattr(m, "fail_count", 0)) + 1
                        m.consecutive_fail = int(getattr(m, "consecutive_fail", 0)) + 1
                        m.last_fail_ts = time.time()
                        streak = max(1, int(getattr(m, "consecutive_fail", 1)))
                        backoff = min(30.0, 1.0 * (2 ** min(5, streak - 1)))
                        m.suspend_until_ts = time.time() + backoff
                        self._record_meter_failure(m, e, backoff)
                        log_once(logger, f"meter_fail_{m.meter_id}", "warning",
                                 f"Meter {m.meter_id} (slave {m.slave_id}) failing. streak={streak} backoff={backoff:.1f}s last={type(e).__name__}: {e}")
                    except Exception:
                        pass

                    self._mark_meter_comm_lost(m, str(e))

                    if self.snapshot_bus:
                        last_values = m.values.copy() if isinstance(m.values, dict) else {}
                        snapshot = MeterSnapshot(
                            meter_id=m.meter_id,
                            ts=time.time(),
                            monotonic_ts=time.monotonic(),
                            values=last_values,
                            quality=MeterQuality.COMM_LOST,
                            error=str(e),
                        )

                        # Authoritative latest-value store
                        try:
                            if self.data_store is not None:
                                self.data_store.update_from_snapshot(
                                    snapshot,
                                    enabled=bool(getattr(m, "enabled", True)),
                                    connected=bool(self.connected),
                                    last_good_ts=getattr(m, "last_good_ts", None),
                                )
                        except Exception:
                            pass
                        self.snapshot_bus.publish(snapshot)
                    else:
                        m.mark_fail()
                    logger.error(f"[POLL] Meter {m.meter_id} failed: {type(e).__name__}: {str(e)}")
                    last_exc = e
                    try:
                        if self._is_client_issue(e):
                            client_issue = True
                    except Exception:
                        pass

            # BUS-level decision: only treat it as a BUS fault if *no* meters succeeded this cycle.
            if any_polled > 0 and (not any_success):
                self._record_bus_failure()
                # If adapter/port looks broken OR repeated full-cycle failures, reconnect.
                if client_issue or (self.bus_fail_count >= self.bus_reconnect_threshold):
                    self.connected = False
                    reason = "client error" if client_issue else f"{self.bus_fail_count} consecutive bus failures"
                    if last_exc is not None:
                        self._set_status(self.STATE_ERROR, f"Bus error ({reason}): {type(last_exc).__name__}: {last_exc}")
                    else:
                        self._set_status(self.STATE_ERROR, f"Bus error ({reason})")
                    self._backoff.schedule_next()
                    with self._bus_lock:
                        self._set_bus_state(self.BUS_RECOVERING, reason="bus_state")
                        self.bus_next_retry_ts = float(getattr(self._backoff, 'next_attempt_time', 0.0) or 0.0)
                    self._close_client()
                    # Let reconnect logic run.
                    self.stop_event.wait(0.2)
                    continue

            # Small yield
            self.stop_event.wait(0.05)

        # Thread shutdown: do not treat as operator-requested disconnect.
        # (prevents confusing "auto_connect=False" persistence)
        try:
            self.user_disconnect = False
        except Exception:
            pass
        self._close_client()
        self.connected = False
        self._set_status(self.STATE_DISCONNECTED)
        logger.info("ModbusWorker stopped")
    
    def get_reconnect_countdown(self) -> float:
        """Get seconds until next reconnect attempt (for UI display)."""
        if self.connected:
            return 0.0
        return self._backoff.seconds_until_next()

    def run_client_task(self, task_fn, *args, **kwargs):
        """Execute a function with the Modbus client under lock (Setup / advanced ops).

        SCADA stability rules:
        - Pause polling while the task runs so the poll loop never blocks on the client lock.
        - Suspend watchdog while the task runs (and a little after) to prevent false reconnects.
        - Always restore flags in finally.
        """
        if not self.connected or not self.client:
            raise RuntimeError("Modbus client not connected")

        now = time.time()
        # Long ops (Read All 400xx across meters) can exceed 45s on slow links.
        # Suspend watchdog for a safe window; actual protection is also the busy flag.
        safe_window = max(120.0, float(self.watchdog_seconds or 12.0) * 3.0)
        self._suspend_watchdog_until = max(getattr(self, "_suspend_watchdog_until", 0.0), now + safe_window)

        prev_pause = getattr(self, "_pause_polling", False)
        self._pause_polling = True
        self._busy_task = True

        try:
            with self._client_lock:
                res = task_fn(self.client, *args, **kwargs)
            # Any successful client task counts as bus activity.
            self._record_bus_success()
            return res
        finally:
            self._busy_task = False
            self._pause_polling = prev_pause

    def _probe_input_register(self, sid: int) -> bool:
        """Small safe probe for scan (1x FC04). Works across pymodbus versions."""
        client = self.client
        if not client:
            return False
        try:
            rr = client.read_input_registers(address=0, count=1, device_id=sid)  # pymodbus 3.x
        except TypeError:
            try:
                rr = client.read_input_registers(address=0, count=1, unit=sid)  # pymodbus 2.x
            except TypeError:
                rr = client.read_input_registers(address=0, count=1, slave=sid)  # some installs
        try:
            return (rr is not None) and (not rr.isError())
        except Exception:
            return False

    def scan_for_meters(self, slave_ids: list = None, on_progress=None) -> dict:
        """
        Scan for responding meters on given slave IDs.

        IMPORTANT (stability):
        - Scanning can take longer than the watchdog window if many IDs don't respond.
        - We pause polling + watchdog and serialize access to the Modbus client during scan.
        """
        if slave_ids is None:
            max_sid = int(self.serial_cfg.get("scan_max_slave_id", 3) or 3)
            max_sid = max(1, min(247, max_sid))
            slave_ids = list(range(1, max_sid + 1))

        results = {}
        found = []
        total = len(slave_ids)

        # Freeze polling + watchdog while scanning
        prev_pause = self._pause_polling
        prev_suspend = self._suspend_watchdog_until
        self._pause_polling = True
        self._scan_in_progress = True
        now = time.time()
        self._last_successful_read = now
        self._suspend_watchdog_until = max(prev_suspend, now + 300.0)

        logger.info(f"[SCAN] Starting scan for slave IDs: {slave_ids}")

        try:
            for i, sid in enumerate(slave_ids, start=1):
                if self.stop_event.is_set():
                    break

                # If we got disconnected mid-scan, stop cleanly
                if not self.client:
                    results[sid] = False
                    break

                logger.info(f"[SCAN] Testing slave {sid}...")

                ok = False
                try:
                    # Serialize physical Modbus access
                    with self._client_lock:
                        client = self.client
                        if not client:
                            raise RuntimeError("client became None during scan")
                        # Small, safe probe (1 register)
                        ok = self._probe_input_register(sid)
                except Exception as e:
                    logger.info(f"[SCAN] Slave {sid}: Error - {e}")
                    ok = False

                results[sid] = ok
                if ok:
                    found.append(sid)
                    logger.info(f"[SCAN] Slave {sid}: FOUND (responded)")

                # Keep watchdog baseline fresh while scanning
                self._last_successful_read = time.time()

                if on_progress:
                    try:
                        on_progress(sid, ok, i, total)
                    except Exception:
                        pass

        finally:
            logger.info(f"[SCAN] Scan complete. Found meters at slave IDs: {found}")
            self._scan_in_progress = False
            self._pause_polling = prev_pause
            # Keep watchdog suspended briefly after scan completes.
            # Reason: scans can consume most of the bus time (retries/timeouts). If we
            # drop the suspension immediately, the watchdog may incorrectly flag an
            # ERROR right after a successful scan.
            self._suspend_watchdog_until = max(prev_suspend, time.time() + 3.0)

        return results

    def get_bus_health(self):
        """Return bus health dict for UI: state, age, attempts, next_in.

        State meanings:
          OK, STALE (no good reads recently but connected),
          RECOVERING (auto reconnect/backoff), OFFLINE (manual disconnect / disabled).
        """
        now = time.time()
        with self._bus_lock:
            last_ok = float(self.last_bus_ok_ts or 0.0)
            age = (now - last_ok) if last_ok else None
            # Determine state
            if self.user_disconnect:
                state = self.BUS_OFFLINE
            elif self.connected:
                # If worker marked STALE keep it, otherwise OK
                if self.stale_seconds and age is not None and age > self.stale_seconds:
                    state = self.BUS_STALE
                else:
                    state = self.BUS_OK
            else:
                # Not connected
                if self.auto_connect:
                    state = self.BUS_RECOVERING
                else:
                    state = self.BUS_OFFLINE

            next_in = 0.0
            try:
                next_in = float(self._backoff.seconds_until_next()) if (state == self.BUS_RECOVERING) else 0.0
            except Exception:
                next_in = 0.0

            return {
                'state': state,
                'age': age,
                'attempts': int(self.bus_recover_attempts or 0),
                'next_in': next_in,
                'connected': bool(self.connected),
                'status': str(self.status),
                'last_error': str(self.last_error or ''),
                'bus_ok_count': int(self.bus_ok_count or 0),
                'bus_fail_count': int(self.bus_fail_count or 0),
                'port_available': self._last_port_available,
                'latency_ms': round(float(self._latency_ewma_ms or 0.0), 1),
                'last_latency_ms': round(float(self._last_poll_latency_ms or 0.0), 1),
                'poll_ok': int(self._total_poll_ok or 0),
                'poll_fail': int(self._total_poll_fail or 0),
                'backoff_attempts': int(getattr(self._backoff, "attempt_count", 0) or 0),
            }

    def get_meter_last_rx_ts(self, meter_id: int) -> float:
        """Return last successful read timestamp for meter_id (0.0 if unknown)."""
        try:
            with self._rx_lock:
                return float(self._last_rx_ts.get(int(meter_id), 0.0) or 0.0)
        except Exception:
            return 0.0

    def get_meter_comm_health(self) -> dict:
        now = time.time()
        with self._comm_metrics_lock:
            out = {}
            for m in list(self.meters or []):
                mid = int(getattr(m, "meter_id", 0) or 0)
                last_ok = float(self._meter_last_ok_ts.get(mid, 0.0) or 0.0)
                out[mid] = {
                    "name": str(getattr(m, "name", f"Meter {mid}") or f"Meter {mid}"),
                    "slave_id": int(getattr(m, "slave_id", 0) or 0),
                    "enabled": bool(getattr(m, "enabled", True)),
                    "latency_ms": round(float(self._meter_latency_ms.get(mid, 0.0) or 0.0), 1),
                    "last_ok_age_s": (now - last_ok) if last_ok else None,
                    "ok_count": int(self._meter_ok_counts.get(mid, 0) or 0),
                    "fail_count": int(self._meter_fail_counts.get(mid, 0) or 0),
                    "consecutive_fail": int(getattr(m, "consecutive_fail", 0) or 0),
                    "suspend_for_s": max(0.0, float(getattr(m, "suspend_until_ts", 0.0) or 0.0) - now),
                    "value_count": int(self._meter_value_count.get(mid, 0) or 0),
                    "last_error": str(self._meter_last_error.get(mid, "") or getattr(m, "last_error", "") or ""),
                }
            return out

    def get_comm_diagnostics(self) -> dict:
        return {
            "bus": self.get_bus_health(),
            "meters": self.get_meter_comm_health(),
            "config_issues": validate_serial_config(self.serial_cfg),
            "serial_cfg": {
                "transport": self.serial_cfg.get("transport", "rtu"),
                "port": self.serial_cfg.get("port", ""),
                "baud": self.serial_cfg.get("baud", 9600),
                "parity": self.serial_cfg.get("parity", "N"),
                "stopbits": self.serial_cfg.get("stopbits", 1),
                "tcp_host": self.serial_cfg.get("tcp_host", ""),
                "tcp_port": self.serial_cfg.get("tcp_port", 502),
                "timeout": self.serial_cfg.get("timeout", 1.0),
                "meter_retry_count": self.serial_cfg.get("meter_retry_count", 2),
                "bus_reconnect_threshold": self.serial_cfg.get("bus_reconnect_threshold", 3),
            },
        }


    def run_client_batch(self, items, task_fn, *, item_kw="slave_id", common_kwargs=None):
        if not self.connected or not self.client:
            raise RuntimeError("Client not connected")

        common_kwargs = dict(common_kwargs or {})
        prev = self._pause_polling
        self._pause_polling = True
        self._busy_task = True
        results = {}
        try:
            with self._client_lock:
                for item in items:
                    kw = dict(common_kwargs)
                    kw[item_kw] = item
                    try:
                        results[item] = task_fn(self.client, **kw)
                        self._record_bus_success()
                    except Exception as e:
                        results[item] = e
            return results
        finally:
            self._busy_task = False
            self._pause_polling = prev
