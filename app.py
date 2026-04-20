
from __future__ import annotations
from utils.paths import set_base_dir
from utils.power import apply_from_cfg as apply_power_from_cfg

import os
import threading
from typing import Any, Dict, List, Optional
from config.store import load_config, save_config
from utils.security import migrate_setup_password_hash, resolve_secret
from utils.startup_health import run_startup_health
from core.meter_model import MeterState
from core.snapshot_bus import SnapshotBus
from core.data_store import DataStore
from core.total_aggregator import TotalAggregator
from core.alarm_engine import AlarmEngine
from core.event_journal import EventJournal
from core.logging_engine import LoggingEngine
from core.modbus_worker import ModbusWorker
from core.runtime_service import RuntimeService
from core.modbus_codec import read_mfm384_fc04
from core.firebase_publisher import FirebasePublisher
from core.mqtt_publisher import MQTTPublisher
from core.rest_api import RESTApiServer
from core.remote_sync import RemoteSyncManager
from core.sms_service import SMSAlertService
from core.protection_engine import ProtectionEngine
from ui.main_window import MainWindow
from ui.setup_wizard import run_setup_wizard
from core.auth_service import AuthService
from ui.login_dialog import prompt_login
from ui.startup_health_dialog import show_startup_health
from utils.crash_report import install_crash_reporter
from utils.logger import setup_logger
from utils.watchdog import WatchdogHeartbeat
from utils.version import VERSION

log = setup_logger("mfm384")


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------
def build_meters_from_cfg(cfg: Dict[str, Any], serial_cfg: Optional[Dict[str, Any]] = None) -> List[MeterState]:
    """
    cfg["meters"] is a LIST of dicts (per your schema).
    """
    meters: List[MeterState] = []

    serial_cfg = serial_cfg or (cfg.get("serial") or {})
    default_timeout = float(serial_cfg.get("timeout", 1))
    default_retry = max(1, int(serial_cfg.get("meter_retry_count", 2)))

    for item in cfg.get("meters", []):
        if not isinstance(item, dict):
            continue

        meter_id = int(item.get("id", 0))
        if meter_id <= 0:
            continue

        slave_id = int(item.get("slave_id", meter_id))
        m = MeterState(meter_id=meter_id, slave_id=slave_id)

        m.enabled = bool(item.get("enabled", True))
        m.display = bool(item.get("display", meter_id == 1))
        m.include_in_total = bool(item.get("include_in_total", True))
        try:
            m.total_sign = float(item.get("total_sign", 1.0))
        except Exception:
            m.total_sign = 1.0
        m.priority = bool(item.get("priority", meter_id == 1))
        m.name = str(item.get("name", f"Meter {meter_id}"))
        m.serial_no = str(item.get("serial_no", ""))
        m.scan_interval_sec = int(item.get("scan_interval_sec", 1))
        m.timeout_sec = float(item.get("timeout_sec", default_timeout))
        m.retry_count = max(1, int(item.get("retry_count", default_retry)))
        try:
            m.ct_ratio = max(0.001, float(item.get("ct_ratio", 1.0) or 1.0))
        except Exception:
            m.ct_ratio = 1.0
        try:
            m.pt_ratio = max(0.001, float(item.get("pt_ratio", 1.0) or 1.0))
        except Exception:
            m.pt_ratio = 1.0
        from core.device_drivers import DEVICE_MODEL_IDS, DEFAULT_MODEL
        raw_model = str(item.get("model", DEFAULT_MODEL) or DEFAULT_MODEL).strip().lower()
        m.model = raw_model if raw_model in DEVICE_MODEL_IDS else DEFAULT_MODEL

        meters.append(m)

    # No fixed meter-slot padding here. The configured meter list is the source
    # of truth; padding hidden meters (previously 6 slots) breaks small plants and
    # blocks scalable multi-meter deployments. Keep one disabled fallback only if
    # the wizard/config produced no meters, so downstream UI code has a safe row.
    if not meters:
        m = MeterState(meter_id=1, slave_id=1)
        m.enabled = False
        m.display = False
        m.include_in_total = True
        m.total_sign = 1.0
        m.priority = True
        m.name = "Meter 1"
        m.serial_no = ""
        m.scan_interval_sec = 1
        m.timeout_sec = default_timeout
        m.retry_count = default_retry
        meters.append(m)

    # Enforce exactly ONE priority meter among configured meters.
    pri = [m for m in meters if getattr(m, "priority", False)]
    if not pri:
        meters[0].priority = True
    elif len(pri) > 1:
        keep = pri[0].meter_id
        for m in meters:
            m.priority = (m.meter_id == keep)

    meters.sort(key=lambda m: m.meter_id)
    return meters


def sync_meters_to_cfg(
    cfg: Dict[str, Any],
    meters: List[MeterState],
    serial_cfg: Optional[Dict[str, Any]] = None,
) -> None:
    serial_cfg = serial_cfg or (cfg.get("serial") or {})
    default_timeout = float(serial_cfg.get("timeout", 1))
    default_retry = max(1, int(serial_cfg.get("meter_retry_count", 2)))

    cfg["meters"] = [
        {
            "id": m.meter_id,
            "slave_id": m.slave_id,
            "enabled": bool(m.enabled),
            "display": bool(m.display),
            "include_in_total": bool(getattr(m, "include_in_total", True)),
            "total_sign": float(getattr(m, "total_sign", 1.0)),
            "priority": bool(getattr(m, "priority", False)),
            "name": str(m.name),
            "serial_no": str(getattr(m, "serial_no", "")),
            "scan_interval_sec": int(getattr(m, "scan_interval_sec", 1)),
            "retry_count": int(getattr(m, "retry_count", default_retry)),
            "timeout_sec": float(getattr(m, "timeout_sec", default_timeout)),
            "ct_ratio": float(getattr(m, "ct_ratio", 1.0) or 1.0),
            "pt_ratio": float(getattr(m, "pt_ratio", 1.0) or 1.0),
            "model": str(getattr(m, "model", "selec_mfm384") or "selec_mfm384"),
        }
        for m in meters
    ]


# ----------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------
def main() -> None:
    cfg: Dict[str, Any] = load_config()

    # Migrate plaintext setup_write_password → hashed storage (one-time, silent).
    try:
        if migrate_setup_password_hash(cfg):
            save_config(cfg)
            log.info("setup_write_password migrated to hashed storage")
    except Exception:
        log.warning("setup_write_password migration failed", exc_info=True)

    # Apply user-selected storage root ASAP.
    # NOTE: If the user changes this at runtime, full relocation takes effect
    # on next restart (log handlers are created at import time).
    try:
        set_base_dir((cfg.get("paths", {}) or {}).get("base_dir", ""))
    except Exception:
        log.warning("set_base_dir failed; using default data path", exc_info=True)

    try:
        install_crash_reporter(lambda: cfg)
    except Exception:
        log.warning("crash reporter install failed", exc_info=True)

    # Start watchdog heartbeat — writes a file every 30 s so an external
    # monitor (Task Scheduler / NSSM) can detect a hung process and restart.
    _watchdog = None
    try:
        _watchdog = WatchdogHeartbeat(version=VERSION)
        _watchdog.start()
    except Exception:
        log.warning("watchdog heartbeat failed to start", exc_info=True)

    cfg.setdefault("site", {})
    cfg.setdefault("serial", {})
    cfg.setdefault("alarms", {})
    # UI / display preferences (older configs may not have this key)
    cfg.setdefault("display", {"show_total": True})
    cfg.setdefault("logging", {})
    cfg.setdefault("reports", {})
    cfg.setdefault("email", {})
    cfg.setdefault("remote_sync", {})
    cfg.setdefault("intelligence", {})
    cfg.setdefault("ui", {})
    cfg.setdefault("meters", [])

    # Operator station power preference
    try:
        apply_power_from_cfg(cfg)
    except Exception:
        log.warning("apply_power_from_cfg failed; keep-awake/power settings not applied", exc_info=True)

    # ------------------------------------------------------------------
    # First-run wizard (runs its own Tk mainloop, returns before MainWindow)
    # ------------------------------------------------------------------
    cfg = run_setup_wizard(cfg)
    try:
        save_config(cfg)
    except Exception:
        log.warning("config save after wizard failed", exc_info=True)

    # ------------------------------------------------------------------
    # Startup health audit (log + post-login operator screen)
    # ------------------------------------------------------------------
    startup_checks = []
    try:
        startup_checks = run_startup_health(cfg)
        for check in startup_checks:
            if check.level == "ERROR":
                log.error("Startup health: %s - %s", check.name, check.message)
            elif check.level == "WARN":
                log.warning("Startup health: %s - %s", check.name, check.message)
            else:
                log.info("Startup health: %s - %s", check.name, check.message)
    except Exception:
        log.warning("startup health audit failed", exc_info=True)
        startup_checks = []

    # ------------------------------------------------------------------
    # User login (AuthService + login dialog)
    # ------------------------------------------------------------------
    auth_service = AuthService(cfg, save_cb=lambda: save_config(cfg))
    session = prompt_login(auth_service)
    if not session:
        return

    try:
        if not show_startup_health(cfg, startup_checks, session=session):
            return
    except Exception:
        log.warning("startup health dialog failed; continuing startup", exc_info=True)

    serial_cfg = (cfg.get("serial") or {})
    # Back-compat: if older configs store auto_connect at top-level, mirror it.
    if "auto_connect" not in serial_cfg:
        serial_cfg["auto_connect"] = bool(cfg.get("auto_connect", False))
    cfg["auto_connect"] = bool(serial_cfg.get("auto_connect", False))

    # HARDEN: If no COM port is selected, force auto_connect OFF.
    # Prevents noisy reconnect loops and edge-case UI actions on first run.
    port = str(serial_cfg.get("port", "")).strip()
    if (not port) or (port.lower() == "none"):
        serial_cfg["auto_connect"] = False
        cfg["auto_connect"] = False
    meters = build_meters_from_cfg(cfg, serial_cfg)


    # ---------------- Core engines ----------------
    aggregator = TotalAggregator()

    # Mini TOTAL snapshot cache (used to enrich COMM/ALARM journal entries)
    total_last_mini = {"quality": "OFFLINE", "kW": None, "Vavg": None, "Iavg": None, "PFavg": None}
    total_last_lock = threading.Lock()

    def _get_total_mini_snapshot() -> dict:
        with total_last_lock:
            return dict(total_last_mini)

    def _update_total_mini(mini: dict) -> None:
        with total_last_lock:
            total_last_mini.clear()
            total_last_mini.update(mini or {})

    snapshot_bus = SnapshotBus()
    data_store = DataStore(cfg)

    # ---------------- Firebase cloud publisher (optional) ----------------
    firebase_pub: Optional[FirebasePublisher] = None
    fb_cfg = cfg.get("firebase", {}) or {}
    if fb_cfg.get("enabled", False):
        try:
            fb_key      = resolve_secret(
                str(fb_cfg.get("key_path", os.path.join(os.path.dirname(__file__), "firebase_key.json")) or ""),
                env_var=str(fb_cfg.get("key_path_env", "") or ""),
                default_env_var="SCADA_FIREBASE_KEY_PATH",
                keyring_service=str(fb_cfg.get("key_path_keyring_service", "") or ""),
                keyring_username=str(fb_cfg.get("key_path_keyring_username", "") or ""),
            )
            fb_site     = str(fb_cfg.get("site_id", "site_01"))
            fb_interval = int(fb_cfg.get("push_interval_sec", 30))
            fb_history  = bool(fb_cfg.get("enable_history", False))
            meter_names = {m.meter_id: m.name for m in meters}
            site_info   = cfg.get("site", {}) or {}
            firebase_pub = FirebasePublisher(
                bus=snapshot_bus,
                site_id=fb_site,
                key_path=fb_key,
                push_interval=fb_interval,
                enable_history=fb_history,
                meter_names=meter_names,
            )
            firebase_pub._plant_name = str(site_info.get("plant_name", "") or "")
            firebase_pub._location   = str(site_info.get("location", "") or "")
            if not firebase_pub.start():
                log.warning("Firebase publisher failed to start — cloud sync disabled")
                firebase_pub = None
        except Exception:
            log.warning("Firebase publisher init error — cloud sync disabled", exc_info=True)
            firebase_pub = None


    event_journal = EventJournal()
    event_journal.start()
    # Retention policy: keep last N days, optional VACUUM
    try:
        jcfg = cfg.get("journal", {}) or {}
        retain_days = int(jcfg.get("retain_days", 180))
        vacuum = bool(jcfg.get("vacuum", True))
        event_journal.request_maintenance(retain_days=retain_days, vacuum=vacuum)
    except Exception:
        log.warning("journal maintenance request failed", exc_info=True)

    acfg = cfg.get("alarms", {}) or {}
    alarm_engine = AlarmEngine(
        event_journal=event_journal,
        startup_inhibit_sec=int(acfg.get("startup_inhibit_sec", 10)),
        stale_sec=int(acfg.get("stale_sec", 10)),
        conn_loss_sec=int(acfg.get("conn_loss_sec", 15)),
    )
    # Provide config so AlarmEngine can read beep_critical and other prefs.
    alarm_engine.set_cfg(cfg)

    protection_engine: Optional[ProtectionEngine] = None
    try:
        protection_engine = ProtectionEngine(cfg)
        protection_engine.set_alarm_bridge(alarm_engine)
    except Exception:
        log.warning("ProtectionEngine init failed; UI fallback will create it if needed", exc_info=True)
        protection_engine = None

    lcfg = cfg.get("logging", {}) or {}
    logging_engine = LoggingEngine(interval_sec=int(lcfg.get("interval_sec", 10)))
    logging_engine.configure(cfg, meters=meters, snapshot_bus=snapshot_bus)
    logging_engine.start()

    runtime_service = None

    mqtt_publisher: Optional[MQTTPublisher] = None
    try:
        mqtt_publisher = MQTTPublisher(cfg)
        mqtt_publisher.start()
    except Exception:
        log.warning("MQTT publisher init failed; MQTT disabled", exc_info=True)
        mqtt_publisher = None

    # Wire MQTT publisher into AlarmEngine so alarm events stream remotely.
    if mqtt_publisher is not None:
        try:
            alarm_engine.set_mqtt_publisher(mqtt_publisher)
        except Exception:
            log.warning("AlarmEngine MQTT wiring failed", exc_info=True)

    # SMS / WhatsApp alert service
    sms_service: Optional[SMSAlertService] = None
    try:
        sms_service = SMSAlertService(cfg)
        # Wire into alarm engine so ALARM events trigger SMS/WhatsApp
        alarm_engine.set_sms_service(sms_service)
    except Exception:
        log.warning("SMS alert service init failed; SMS disabled", exc_info=True)
        sms_service = None

    rest_api: Optional[RESTApiServer] = None
    try:
        rest_api = RESTApiServer(cfg)
        rest_api.start()
    except Exception:
        log.warning("REST API init failed; REST disabled", exc_info=True)
        rest_api = None

    remote_sync: Optional[RemoteSyncManager] = None
    try:
        remote_sync = RemoteSyncManager(
            cfg,
            mqtt_publisher=mqtt_publisher,
            rest_api=rest_api,
        )
    except Exception:
        log.warning("RemoteSyncManager init failed; direct MQTT/REST publishing remains active", exc_info=True)
        remote_sync = None

    # ---------------- Modbus worker ----------------
    word_order = str(serial_cfg.get("word_order", "CDAB")).upper()
    if word_order not in ("ABCD", "CDAB", "BADC", "DCBA"):
        word_order = "CDAB"

    BASE_ADDRESS = int(serial_cfg.get("base_address", 0) or 0)

    from core.device_drivers import read_meter as _read_meter_by_model

    def read_map_fn(client, slave_id: int) -> Dict[str, float]:
        # Look up which meter object corresponds to this slave_id
        # to dispatch the correct driver. Fall back to MFM384 if not found.
        meter_obj = next((m for m in meters if m.slave_id == slave_id), None)
        model_id = str(getattr(meter_obj, "model", "selec_mfm384") or "selec_mfm384")
        return _read_meter_by_model(
            model_id=model_id,
            client=client,
            slave_id=slave_id,
            word_order=word_order,
            base_address=BASE_ADDRESS,
        )

    def make_worker() -> ModbusWorker:
        """Factory used by the UI-thread Supervisor to restart the worker safely."""
        return ModbusWorker(
            meters=meters,
            serial_cfg=serial_cfg,
            alarms_cfg=acfg,
            read_map_fn=read_map_fn,
            raw_read_fn=None,
            snapshot_bus=snapshot_bus,
            data_store=data_store,
            event_journal=event_journal,
            journal_snapshot_provider=_get_total_mini_snapshot,
            cfg=cfg,
        )

    worker = make_worker()
    worker.start()

    try:
        runtime_service = RuntimeService(
            cfg=cfg,
            meters=meters,
            aggregator=aggregator,
            alarm_engine=alarm_engine,
            logging_engine=logging_engine,
            protection_engine=protection_engine,
            total_mini_update_cb=_update_total_mini,
            worker=worker,
            mqtt_publisher=mqtt_publisher,
            rest_api=rest_api,
            remote_sync=remote_sync,
        )
        runtime_service.start()
    except Exception:
        log.warning("runtime_service start failed; UI fallback ticks remain active", exc_info=True)
        runtime_service = None

    log.info(
        "Startup: port=%s baud=%s word_order=%s meters=%d auto_connect=%s firebase=%s",
        serial_cfg.get("port", "none"),
        serial_cfg.get("baud_rate", "?"),
        word_order,
        sum(1 for m in meters if m.enabled),
        serial_cfg.get("auto_connect", False),
        "enabled" if firebase_pub else "disabled",
    )

    # ---------------- Callbacks ----------------
    def save_now() -> None:
        sync_meters_to_cfg(cfg, meters, serial_cfg)
        save_config(cfg)

    def apply_comm() -> None:
        """Persist comm/meter settings.

        NOTE:
        This callback is triggered by the Comm tab "Apply" button **and** by the
        auto-scan completion flow. Forcing a disconnect here causes the app to
        drop the COM connection right after auto-scan and leaves the worker in a
        disconnected state (auto_connect is intentionally False by default).

        If the operator changes COM port/baud and wants a reconnect, they should
        use the Connect/Disconnect buttons.
        """
        save_now()

    # ---------------- UI + Lifecycle (login/logout loop) ----------------
    app = None
    try:
        while session:
            app = MainWindow(
                cfg=cfg,
                meters=meters,
                aggregator=aggregator,
                total_mini_update_cb=_update_total_mini,
                alarm_engine=alarm_engine,
                logging_engine=logging_engine,
                worker=worker,
                event_journal=event_journal,
                snapshot_bus=snapshot_bus,
                data_store=data_store,
                on_save_config=save_now,
                on_apply_comm=apply_comm,
                worker_factory=make_worker,
                firebase_publisher=firebase_pub,
                auth_service=auth_service,
                session=session,
                protection_engine=protection_engine,
                mqtt_publisher=mqtt_publisher,
                rest_api=rest_api,
                remote_sync=remote_sync,
                runtime_service=runtime_service,
                sms_service=sms_service,
            )
            app.mainloop()

            if bool(getattr(app, "_logout_requested", False)):
                session = prompt_login(auth_service)
                if not session:
                    break
                continue
            break
    finally:
        try:
            if runtime_service is not None:
                runtime_service.stop()
        except Exception:
            log.warning("runtime_service shutdown error", exc_info=True)
        try:
            # Worker may have been hot-swapped by Supervisor.
            w = getattr(app, "worker", None) or worker
            w.stop()
            w.join(timeout=3.0)
            if w.is_alive():
                log.warning("worker thread did not exit within 3 s")
        except Exception:
            log.warning("worker shutdown error", exc_info=True)
        try:
            if remote_sync is not None:
                remote_sync.close()
        except Exception:
            log.warning("remote_sync shutdown error", exc_info=True)
        try:
            if mqtt_publisher is not None:
                mqtt_publisher.stop()
        except Exception:
            log.warning("mqtt_publisher shutdown error", exc_info=True)
        try:
            if rest_api is not None:
                rest_api.stop()
        except Exception:
            log.warning("rest_api shutdown error", exc_info=True)
        try:
            logging_engine.stop()
            t = getattr(logging_engine, "_thread", None)
            if t is not None:
                t.join(timeout=3.0)
                if t.is_alive():
                    log.warning("logging_engine thread did not exit within 3 s")
        except Exception:
            log.warning("logging_engine shutdown error", exc_info=True)
        try:
            event_journal.stop()
            t = getattr(event_journal, "_thread", None)
            if t is not None:
                t.join(timeout=3.0)
                if t.is_alive():
                    log.warning("event_journal thread did not exit within 3 s")
        except Exception:
            log.warning("event_journal shutdown error", exc_info=True)
        try:
            if firebase_pub is not None:
                firebase_pub.stop()
        except Exception:
            log.warning("firebase_pub shutdown error", exc_info=True)
        try:
            save_now()
        except Exception:
            log.exception("config save on exit failed — last config may not be persisted")
        try:
            if _watchdog is not None:
                _watchdog.stop()
        except Exception:
            log.warning("watchdog stop error", exc_info=True)


if __name__ == "__main__":
    main()
