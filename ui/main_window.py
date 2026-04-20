import time
import os
import tkinter as tk
from tkinter import ttk, messagebox
from pathlib import Path

from utils.resources import resource_path
from utils.version import APP_NAME, VERSION

from ui.styles import apply_styles, get_theme
from ui.meter_selector import MeterSelector
from ui.operator_home import OperatorHomeTab
from ui.dashboard import DashboardTab
from ui.solar_kpi_view import SolarKPITab
from ui.hydro_kpi_view import HydroKPITab
from ui.load_kpi_view import LoadKPITab
from ui.status_view import StatusTab
from ui.data_graphs import DataGraphsTab
from ui.analytics_view import AnalyticsTab
from ui.intelligence_view import IntelligenceTab
from ui.protection_view import ProtectionView
from ui.logging_view import LoggingTab
from ui.reports_view import ReportsTab
from ui.incident_summary import IncidentSummaryTab
from ui.email_view import EmailTab
from ui.setup_home import SetupHomeTab as SetupMainTab
from ui.journal_view import JournalTab
from ui.alarms_view import AlarmsTab
from ui.comm_view import CommTab
from ui.connectivity_view import ConnectivityTab
from ui.access_view import AccessControlTab
from ui.status_bar import StatusBar
from utils.stale_override import set_stale_override
from utils.write_guard import lock_writes
from ui.widgets.scrollable_container import ScrollableContainer
from queue import Empty

from utils.health import stale_seconds
from utils.logger import setup_logger
from utils.log_once import log_once
from utils.paths import logs_dir
from utils.monitors import list_monitors, geometry_for_monitor
from utils.power import set_keep_awake, apply_from_cfg as apply_power_from_cfg
from utils.app_health import summarize_health

from core.supervisor import Supervisor
from core.snapshot_report import SnapshotReporter
from config.features import FEATURE_KEYS, DEFAULT_ROLE_PERMISSIONS

logger = setup_logger("ui")


class MainWindow(tk.Tk):
    """
    Main application window.

    REQUIRED:
    - Window title MUST always be "MFM384 Monitoring"
    - Remove top Plant/Location entry area permanently
    - Plant/Location editing lives only in Communication tab
    """

    APP_NAME = "MFM384 Monitoring"
    REMOVED_PAGES = {"real_values", "advisory"}

    def __init__(
        self,
        cfg,
        meters,
        aggregator,
        alarm_engine,
        logging_engine,
        worker,
        total_mini_update_cb=None,
        event_journal=None,
        snapshot_bus=None,
        data_store=None,
        on_save_config=None,
        on_apply_comm=None,
        worker_factory=None,
        firebase_publisher=None,
        auth_service=None,
        session=None,
        protection_engine=None,
        mqtt_publisher=None,
        rest_api=None,
        remote_sync=None,
        runtime_service=None,
        sms_service=None,
    ):
        super().__init__()

        self.cfg = cfg
        self.meters = list(meters)  # dynamic meter list; no hidden 6-meter cap
        self.aggregator = aggregator
        self._total_mini_update_cb = total_mini_update_cb
        self.alarm_engine = alarm_engine
        self.logging_engine = logging_engine
        self.worker = worker
        self.event_journal = event_journal
        self.snapshot_bus = snapshot_bus
        self.data_store = data_store
        self.on_save_config = on_save_config
        self.on_apply_comm = on_apply_comm
        self.worker_factory = worker_factory
        self._firebase_publisher = firebase_publisher
        self.auth_service = auth_service
        self.protection_engine = protection_engine
        self.runtime_service = runtime_service
        self.session = dict(session or {})
        self.session_username = str(self.session.get("username", "") or "").strip() or "Unknown"
        self.session_role = str(self.session.get("role", "operator") or "operator").strip().lower()
        self.session_is_master = bool(self.session.get("is_master_admin", False))
        self.session_permissions = dict(self.session.get("permissions", {}) or {})
        self.session_timeout_sec = max(60, int(self.session.get("idle_timeout_sec", 7200) or 7200))
        if not self.session_is_master:
            for _key in DEFAULT_ROLE_PERMISSIONS.get(self.session_role, set()):
                self.session_permissions.setdefault(_key, True)
        self._last_activity_ts = time.time()
        self._closing = False
        self._logout_requested = False
        self._audit("SESSION_START", f"role={self.session_role} timeout_sec={self.session_timeout_sec}")

        # UI-thread Supervisor: detects dead/stuck worker and restarts safely.
        self.supervisor = Supervisor(self.cfg)
        self._last_supervisor_tick_ts = 0.0

        # Connectivity services are owned by app.py / RuntimeService.
        # MainWindow only keeps handles for status display and reconfigure actions.
        self._mqtt_publisher = mqtt_publisher
        self._rest_api = rest_api
        self._remote_sync = remote_sync
        self._sms_service = sms_service
        self._last_connectivity_tick_ts = 0.0
        self._start_ts = time.time()

        # ── Snapshot reporter (Ctrl+P / Print button) ─────────────────────
        self._snapshot_reporter = SnapshotReporter(cfg)
        # Latest live values for on-demand snapshot; updated each UI tick
        self._last_values_by_source: dict = {}
        self._last_quality_map: dict = {}

        self._snapshot_queue = self.snapshot_bus.subscribe() if self.snapshot_bus else None

        # DataStore sync (prevents UI lag -> missed snapshots)
        self._ds_last_seq = {}

        self.geometry("1450x880")
        self.minsize(1200, 720)

        # DPI awareness: scale Tkinter to match system DPI on high-DPI panels.
        # Critical for industrial touch-PCs (1920Ã—1200 10" displays at 200% DPI).
        try:
            import ctypes
            dpi = ctypes.windll.user32.GetDpiForSystem()
            if dpi and dpi != 96:
                self.tk.call("tk", "scaling", dpi / 72.0)
        except Exception:
            pass

        apply_styles(self, self.cfg)
        self._theme = get_theme()
        self._apply_app_icon()
        self._build_ui()
        self._bind_user_activity_watch()
        self.after(15000, self._check_idle_session)

        # Apply operator-station settings (keep-awake + external windows)
        try:
            apply_power_from_cfg(self.cfg)
        except Exception:
            pass
        try:
            self.after(600, self._apply_external_from_cfg)
        except Exception:
            pass

        # Realtime clock in the top bar
        self._start_clock()

        # âœ… Title must be constant
        self.title(f"{APP_NAME} v{VERSION}")

        self.protocol("WM_DELETE_WINDOW", self._on_close)

        # Start UI tick loop
        # UI tick: 750ms is a good compromise (less CPU + less lag, still responsive)
        self.after(750, self._ui_tick)

    def report_callback_exception(self, exc, val, tb):
        try:
            from utils.crash_report import write_crash_report

            path = write_crash_report(exc, val, tb, cfg_getter=lambda: self.cfg, origin="tkinter")
            logger.error("Tkinter callback exception captured in crash report: %s", path, exc_info=(exc, val, tb))
        except Exception:
            logger.error("Tkinter callback exception", exc_info=(exc, val, tb))

    def _persist_config(self, context: str = "config") -> None:
        """Persist config through host callback with a single, safe calling convention."""
        if not callable(self.on_save_config):
            return
        try:
            self.on_save_config()
            self._audit("CONFIG_SAVE", context)
        except TypeError:
            # Backward-compat only: some hosts may still accept cfg.
            try:
                self.on_save_config(self.cfg)
                self._audit("CONFIG_SAVE", context)
            except Exception as e:
                log_once(logger, f"ui.save.{context}", "exception",
                         f"Config save failed ({context}): {type(e).__name__}: {e}", cooldown_sec=5)
        except Exception as e:
            log_once(logger, f"ui.save.{context}", "exception",
                     f"Config save failed ({context}): {type(e).__name__}: {e}", cooldown_sec=5)

    def _audit(self, action: str, detail: str = "", success: bool = True) -> None:
        if self.auth_service is None:
            return
        try:
            self.auth_service.record_audit(
                actor=self.session_username,
                action=str(action or ""),
                detail=str(detail or "")[:800],
                success=bool(success),
            )
        except Exception:
            pass

    def _is_master_admin(self) -> bool:
        return bool(self.session_is_master)

    def _runtime_handles_connectivity(self) -> bool:
        svc = getattr(self, "runtime_service", None)
        return bool(getattr(svc, "handles_connectivity", False) and getattr(svc, "is_running", False))

    def _runtime_handles_protection(self) -> bool:
        svc = getattr(self, "runtime_service", None)
        return bool(getattr(svc, "handles_protection", False) and getattr(svc, "is_running", False))

    def _has_permission(self, key: str) -> bool:
        k = str(key or "").strip().lower()
        if not k or k in self.REMOVED_PAGES or k not in FEATURE_KEYS:
            return False
        if self._is_master_admin():
            return True
        return bool(self.session_permissions.get(k, False))

    def _role_label(self) -> str:
        role = str(self.session_role or "").strip().lower()
        if role == "admin_master":
            return "Master Admin"
        if role == "engineer":
            return "Engineer"
        if role == "owner":
            return "Owner"
        return "Operator"

    def _can_show_page(self, key: str) -> bool:
        k = str(key or "").strip().lower()
        if not self._has_permission(k):
            return False
        widgets = getattr(self, "_page_widgets", None)
        if isinstance(widgets, dict):
            return k in widgets
        return True

    def _notify_operator(self, message: str) -> None:
        msg = str(message or "").strip()
        if not msg:
            return
        try:
            sb = getattr(self, "status_bar", None)
            if sb is not None and hasattr(sb, "summary_var"):
                sb.summary_var.set(msg)
                return
        except Exception:
            pass
        try:
            if hasattr(self, "health_detail_var"):
                self.health_detail_var.set(msg)
        except Exception:
            pass

    def _preferred_start_pages(self) -> tuple[str, ...]:
        role = str(self.session_role or "").strip().lower()
        if self._is_master_admin() or role == "engineer":
            return ("home", "dashboard", "status", "comm")
        if role == "owner":
            return ("home", "dashboard", "reports", "status")
        return ("home", "status", "dashboard")

    def _on_user_activity(self, _evt=None):
        self._last_activity_ts = time.time()

    def _bind_user_activity_watch(self):
        self.bind_all("<Motion>", self._on_user_activity, add="+")
        self.bind_all("<ButtonPress>", self._on_user_activity, add="+")
        self.bind_all("<KeyPress>", self._on_user_activity, add="+")
        self.bind_all("<Control-b>", lambda _e: self.toggle_sidebar(), add="+")
        # ── Operator keyboard shortcuts ───────────────────────────────────────
        # F1  — Quick Health overlay (bus state, alarms, live kW/V, logging)
        # F2  — Jump to Comm tab (re-connect / check comms)
        # F3  — ACK all unacknowledged alarms
        # F5  — Refresh current page (calls on_show if available)
        # Ctrl+L — Jump to Logging tab
        self.bind_all("<F1>",        lambda _e: self._show_quick_health(), add="+")
        self.bind_all("<F2>",        lambda _e: self.show_page("comm"),    add="+")
        self.bind_all("<F3>",        lambda _e: self._ack_all_hotkey(),    add="+")
        self.bind_all("<F5>",        lambda _e: self._refresh_current_page(), add="+")
        self.bind_all("<Control-l>", lambda _e: self.show_page("logging"), add="+")
        # Ctrl+P — print live snapshot report in default browser
        self.bind_all("<Control-p>", lambda _e: self._print_snapshot_hotkey(), add="+")

    # ── Keyboard shortcut handlers ────────────────────────────────────────────

    def _ack_all_hotkey(self):
        """F3 — acknowledge all unacknowledged alarms; show brief toast."""
        try:
            actor = str(getattr(self, "session_username", "operator") or "operator")
            count = self.alarm_engine.acknowledge_all(actor)
            if count:
                self._notify_operator(f"F3: Acknowledged {count} alarm(s) — {actor}")
            else:
                self._notify_operator("F3: No unacknowledged alarms.")
            # Refresh alarms page if currently visible
            w = (self._page_widgets or {}).get("alarms")
            if w and hasattr(w, "update_view"):
                try:
                    w.update_view()
                except Exception:
                    pass
        except Exception:
            pass

    def _print_snapshot_hotkey(self):
        """Ctrl+P — generate live snapshot report and open in browser for printing."""
        try:
            self.print_snapshot_now()
        except Exception as exc:
            logger.warning("Snapshot print failed: %s", exc)
            self._notify_operator(f"Print failed: {exc}")

    def print_snapshot_now(self) -> str:
        """
        Generate a live snapshot HTML report and open it in the default browser.

        Pulls from the last cached tick values so it is always near-realtime
        (at most one tick old — typically < 2 s).  Thread-safe: can be called
        from UI code or a toolbar button.

        Returns the output file path.
        """
        vbs    = dict(getattr(self, "_last_values_by_source", {}) or {})
        qmap   = dict(getattr(self, "_last_quality_map", {}) or {})
        alarms = dict(getattr(self.alarm_engine, "active", {}) or {})
        meters = list(getattr(self, "meters", []) or [])

        if not vbs:
            self._notify_operator("No data yet — snapshot report requires at least one poll cycle.")
            return ""

        try:
            path = self._snapshot_reporter.print_snapshot(vbs, qmap, alarms, meters)
            self._notify_operator(f"Snapshot report opened in browser  — Ctrl+P to print")
            return path
        except Exception as exc:
            logger.error("Snapshot report generation failed: %s", exc, exc_info=True)
            self._notify_operator(f"Snapshot report failed: {exc}")
            return ""

    def _refresh_current_page(self):
        """F5 — call on_show() / update_view() on the active page."""
        try:
            key = getattr(self, "_current_page_key", None)
            if not key:
                return
            w = (self._page_widgets or {}).get(key)
            if w is None:
                return
            for method in ("on_show", "update_view", "refresh"):
                fn = getattr(w, method, None)
                if callable(fn):
                    fn()
                    break
        except Exception:
            pass

    def _show_quick_health(self):
        """F1 — dismissible single-screen site health overlay.

        Shows: bus state, active/unacked alarm count, latest aggregate kW/V/freq,
        logging state, and operator shortcut reference.  Read-only, no actions.
        Pressing F1 again or Escape closes it.
        """
        try:
            # Only one overlay at a time — bring existing to front if open.
            existing = getattr(self, "_quick_health_win", None)
            if existing and existing.winfo_exists():
                try:
                    existing.lift()
                    existing.focus_force()
                except Exception:
                    pass
                return

            t = self._theme
            win = tk.Toplevel(self)
            win.title("Quick Health  [F1 / Esc to close]")
            win.resizable(False, False)
            win.configure(bg=t.bg)
            self._quick_health_win = win

            # Position near top-left of main window
            try:
                mx = self.winfo_rootx() + 60
                my = self.winfo_rooty() + 80
                win.geometry(f"480x420+{mx}+{my}")
            except Exception:
                win.geometry("480x420")

            # Close on Escape or F1
            win.bind("<Escape>", lambda _e: win.destroy())
            win.bind("<F1>",     lambda _e: win.destroy())
            win.protocol("WM_DELETE_WINDOW", win.destroy)

            # ── Header ────────────────────────────────────────────────────────
            hdr = tk.Frame(win, bg=t.accent, pady=6)
            hdr.pack(fill="x")
            tk.Label(hdr, text="  ⚡ Quick Health", bg=t.accent, fg="#ffffff",
                     font=("Segoe UI", 12, "bold")).pack(side="left")
            tk.Label(hdr, text="Esc/F1 ✕  ", bg=t.accent, fg="#ffffffaa",
                     font=("Segoe UI", 9)).pack(side="right")

            # ── Content frame ─────────────────────────────────────────────────
            body = tk.Frame(win, bg=t.bg, padx=16, pady=12)
            body.pack(fill="both", expand=True)

            def _row(label: str, value: str, color: str = None):
                row = tk.Frame(body, bg=t.bg)
                row.pack(fill="x", pady=3)
                tk.Label(row, text=label, bg=t.bg, fg=t.text_muted,
                         font=("Segoe UI", 9), width=22, anchor="w").pack(side="left")
                vc = color if color else t.text
                tk.Label(row, text=value, bg=t.bg, fg=vc,
                         font=("Segoe UI", 10, "bold"), anchor="w").pack(side="left")

            def _section(title: str):
                tk.Label(body, text=title, bg=t.bg, fg=t.text_muted,
                         font=("Segoe UI", 8, "bold")).pack(anchor="w", pady=(10, 2))
                ttk.Separator(body, orient="horizontal").pack(fill="x")

            # ── Bus / Comms ───────────────────────────────────────────────────
            _section("COMMUNICATIONS")
            try:
                from ui.ux_labels import plain_comm_state
                bus = self.worker.get_bus_health() if self.worker else {}
                bus_state = plain_comm_state((bus or {}).get("state", "OFFLINE"))
                age = (bus or {}).get("age", None)
                age_txt = f"  (last rx {age:.1f}s ago)" if age is not None else ""
                bus_color = t.good if "ONLINE" in bus_state.upper() else t.alarm
                _row("Bus state:", f"{bus_state}{age_txt}", bus_color)
            except Exception:
                _row("Bus state:", "—")

            enabled_meters = [m for m in self.meters if getattr(m, "enabled", False)]
            _row("Enabled meters:", str(len(enabled_meters)))

            # ── Alarms ────────────────────────────────────────────────────────
            _section("ALARMS")
            try:
                unack  = int(getattr(self.alarm_engine, "unacknowledged_count", 0) or 0)
                active = len(getattr(self.alarm_engine, "active", {}) or {})
                unack_color  = t.alarm if unack  > 0 else t.good
                active_color = t.warn  if active > 0 else t.good
                _row("Unacknowledged:", str(unack),  unack_color)
                _row("Active alarms:", str(active), active_color)
            except Exception:
                _row("Alarms:", "—")

            # ── Live totals ───────────────────────────────────────────────────
            _section("LIVE TOTALS  (aggregator)")
            try:
                agg_vals = {}
                if self.aggregator:
                    snap = None
                    for m in self.meters:
                        if getattr(m, "meter_id", None) == "TOTAL":
                            snap = m
                            break
                    if snap is None and hasattr(self.aggregator, "get_snapshot"):
                        snap = self.aggregator.get_snapshot("TOTAL")
                    if snap and hasattr(snap, "values"):
                        agg_vals = snap.values or {}
                    elif snap and isinstance(snap, dict):
                        agg_vals = snap.get("values") or {}
                kw   = agg_vals.get("kW")
                vavg = agg_vals.get("Vavg")
                freq = agg_vals.get("freq") or agg_vals.get("Hz")
                _row("Total kW:",   f"{kw:.1f} kW"  if isinstance(kw,   float) else "—")
                _row("Vavg (L-N):", f"{vavg:.1f} V" if isinstance(vavg, float) else "—")
                _row("Frequency:",  f"{freq:.2f} Hz" if isinstance(freq, float) else "—")
            except Exception:
                _row("Live data:", "—")

            # ── Logging ───────────────────────────────────────────────────────
            _section("LOGGING")
            try:
                if self.logging_engine:
                    log_state = str(getattr(self.logging_engine, "state", "DISABLED") or "DISABLED")
                    summary = self.logging_engine.get_status_summary() if hasattr(self.logging_engine, "get_status_summary") else log_state
                    log_color = t.good if log_state.upper() == "RUNNING" else (t.warn if "SCHEDULED" in log_state.upper() else t.alarm)
                    _row("State:", log_state, log_color)
                    _row("Summary:", summary[:48] if summary else "—")
                else:
                    _row("State:", "Logging disabled")
            except Exception:
                _row("Logging:", "—")

            # ── Keyboard shortcut reference ───────────────────────────────────
            _section("KEYBOARD SHORTCUTS")
            shortcuts = [
                ("F1", "This overlay (Quick Health)"),
                ("F2", "Go to Comm tab (reconnect)"),
                ("F3", "ACK All alarms"),
                ("F5", "Refresh current page"),
                ("Ctrl+L", "Go to Logging tab"),
                ("Ctrl+B", "Toggle sidebar"),
            ]
            ref = tk.Frame(body, bg=t.bg)
            ref.pack(fill="x", pady=(4, 0))
            for key_txt, desc in shortcuts:
                row = tk.Frame(ref, bg=t.bg)
                row.pack(fill="x", pady=1)
                tk.Label(row, text=key_txt, bg=t.surface, fg=t.text,
                         font=("Courier New", 9, "bold"),
                         width=10, anchor="center", padx=4, pady=1,
                         relief="flat").pack(side="left", padx=(0, 8))
                tk.Label(row, text=desc, bg=t.bg, fg=t.text_muted,
                         font=("Segoe UI", 9), anchor="w").pack(side="left")

            win.update_idletasks()
            win.lift()
            win.focus_force()
        except Exception:
            pass

    def _check_idle_session(self):
        try:
            idle = time.time() - float(self._last_activity_ts or 0.0)
            if idle >= float(self.session_timeout_sec):
                mins = int(self.session_timeout_sec // 60)
                self._audit("SESSION_EXPIRED", f"idle_sec={int(idle)}")
                messagebox.showwarning(
                    "Session expired",
                    f"{self._role_label()} session expired after {mins} minutes of inactivity.\nPlease login again.",
                    parent=self,
                )
                self._do_logout(reason="SESSION_EXPIRED")
                return
        except Exception:
            pass
        self.after(15000, self._check_idle_session)

    # ------------------------------------------------------------------
    # OPERATOR STATION (Multi-monitor + Keep-awake)
    # ------------------------------------------------------------------
    def _set_keep_awake(self, enabled: bool) -> None:
        """Toggle keep-awake (prevent sleep/display-off) and persist to cfg."""
        try:
            set_keep_awake(bool(enabled))
        except Exception:
            pass

        ui = (self.cfg.get("ui") or {})
        ui["keep_awake"] = bool(enabled)
        self.cfg["ui"] = ui

        self._persist_config("keep_awake")

    def _station_apply(self, payload: dict) -> None:
        """Apply operator-station settings: external windows + keep-awake."""
        if not isinstance(payload, dict):
            return

        action = str(payload.get("action") or "apply").lower()

        # keep-awake
        if "keep_awake" in payload:
            self._set_keep_awake(bool(payload.get("keep_awake")))

        # External views state
        views = payload.get("views") or {}

        ui = (self.cfg.get("ui") or {})
        ext = (ui.get("external_views") or {})
        # merge + persist
        for k, v in (views.items() if isinstance(views, dict) else []):
            if isinstance(v, dict):
                ext[str(k)] = {
                    "enabled": bool(v.get("enabled", False)),
                    "monitor": int(v.get("monitor", 1) or 1),
                }
        ui["external_views"] = ext
        self.cfg["ui"] = ui

        # Ensure dict exists
        if not hasattr(self, "_external_windows"):
            self._external_windows = {}

        if action == "focus":
            key = str(payload.get("view") or "")
            win = self._external_windows.get(key)
            try:
                if win is not None and win.winfo_exists():
                    win.deiconify()
                    win.lift()
                    win.focus_force()
            except Exception:
                pass
            return

        # apply/open
        try:
            self._apply_external_from_cfg()
        except Exception:
            pass

        self._persist_config("station_apply")

    def _apply_external_from_cfg(self) -> None:
        """Open/close external windows based on cfg.ui.external_views."""
        ui = (self.cfg.get("ui") or {})
        ext = (ui.get("external_views") or {})

        # Enumerate monitors each time (hotplug safe)
        mons = list_monitors(root_tk=self)
        if not mons:
            mons = [{"index": 1, "left": 0, "top": 0, "right": self.winfo_screenwidth(), "bottom": self.winfo_screenheight(),
                     "width": self.winfo_screenwidth(), "height": self.winfo_screenheight(), "primary": True}]

        def get_mon(idx: int):
            try:
                idx = int(idx)
            except Exception:
                idx = 1
            idx = max(1, idx)
            if idx <= len(mons):
                return mons[idx - 1]
            # fallback: primary else first
            for m in mons:
                if m.get("primary"):
                    return m
            return mons[0]

        for key in ("dashboard", "protection", "alarms"):
            cfgv = (ext.get(key) or {})
            enabled = bool(cfgv.get("enabled", False))
            mon = get_mon(cfgv.get("monitor", 1))

            if not enabled:
                # Close if open
                win = getattr(self, "_external_windows", {}).get(key)
                try:
                    if win is not None and win.winfo_exists():
                        win.destroy()
                except Exception:
                    pass
                if hasattr(self, "_external_windows"):
                    self._external_windows.pop(key, None)
                continue

            # Open or rebuild
            self._open_external_view(key, mon)

    def _open_external_view(self, key: str, mon: dict) -> None:
        """Create external view window (Toplevel) and place on selected monitor."""
        key = str(key or "").lower().strip()
        if not key:
            return

        if not hasattr(self, "_external_windows"):
            self._external_windows = {}

        # Reuse existing window if still alive
        old = self._external_windows.get(key)
        try:
            if old is not None and old.winfo_exists():
                # just move + focus
                old.geometry(geometry_for_monitor(mon))
                old.deiconify()
                old.lift()
                return
        except Exception:
            pass

        win = tk.Toplevel(self)
        win.title(f"{self.APP_NAME} â€” {key.capitalize()}")
        try:
            win.geometry(geometry_for_monitor(mon))
        except Exception:
            pass

        # If operator closes external window manually, mark disabled in cfg
        def _on_close():
            try:
                win.destroy()
            finally:
                try:
                    ui = (self.cfg.get("ui") or {})
                    ext = (ui.get("external_views") or {})
                    if key in ext:
                        ext[key]["enabled"] = False
                    ui["external_views"] = ext
                    self.cfg["ui"] = ui
                    self._persist_config(f"external_close_{key}")
                except Exception:
                    pass
                if hasattr(self, "_external_windows"):
                    self._external_windows.pop(key, None)

        win.protocol("WM_DELETE_WINDOW", _on_close)

        # Build content
        try:
            if key == "dashboard":
                tab = DashboardTab(
                    win,
                    self.meters,
                    self.aggregator,
                    self.selector,
                    self.cfg,
                    alarm_engine=self.alarm_engine,
                    event_journal=getattr(self, "event_journal", None),
                    protection_engine=getattr(self, "protection_engine", None),
                )
                tab.pack(fill="both", expand=True)
                # force immediate render once the window shows
                try:
                    tab.on_show()
                except Exception:
                    pass
            elif key == "protection":
                cont = ScrollableContainer(win)
                cont.pack(fill="both", expand=True)
                ProtectionView(
                    cont.body,
                    cfg=self.cfg,
                    meters=self.meters,
                    snapshot_fn=self._get_snapshot,
                    protection_engine=getattr(self, "protection_engine", None),
                    tier_filter=1,
                ).pack(fill="both", expand=True)
            elif key == "alarms":
                tab = AlarmsTab(win, alarm_engine=self.alarm_engine, meters=self.meters, session=self.session)
                tab.pack(fill="both", expand=True)
        except Exception as e:
            # Never crash startup due to external windows
            log_once(logger, f"ui.external.{key}", "exception", f"External view build failed: {type(e).__name__}: {e}", cooldown_sec=5)

        self._external_windows[key] = win
    # ------------------------------------------------------------------
    # APP ICON (SAFE)
    # ------------------------------------------------------------------
    def _apply_app_icon(self):
        """Apply window icon safely (never crash the app).

        Priority (Windows):
        1) icon.ico via iconbitmap
        2) assets/icon.png via iconphoto

        This must never raise, even on old Tk builds.
        """

        try:
            base = Path(__file__).resolve().parents[1]  # .../mfm384_v8
            ico = base / "icon.ico"
            png = base / "assets" / "icon.png"

            # Windows .ico (best for taskbar)
            if ico.exists():
                try:
                    self.iconbitmap(default=str(ico))
                    return
                except Exception:
                    # fall back to PNG
                    pass

            if png.exists():
                try:
                    img = tk.PhotoImage(file=str(png))
                    # keep a ref or Tk may garbage collect it
                    self._app_icon_img = img
                    self.iconphoto(True, img)
                except Exception as exc:
                    log_once(logger, "icon_apply_png_failed", f"Icon PNG apply failed: {exc}")
        except Exception as exc:
            # Absolute last resort: never block startup due to icon issues.
            log_once(logger, "icon_apply_failed", f"Icon apply failed: {exc}")

    # ------------------------------------------------------------------
    # SIDEBAR NAV (SAFE + OPERATOR-PROOF)
    # ------------------------------------------------------------------
    def _load_nav_images(self) -> dict:
        """Load sidebar icons. Never raise. Keeps references on self."""
        images = {}
        try:
            ico_dir = Path(resource_path("assets","icons"))
            if not ico_dir.exists():
                return images
            mapping = {
                "dashboard": "dashboard.png",
                "status": "status.png",
                "data_graphs": "graphs.png",
                "protection": "control.png",
                "alarms": "alarm.png",
                "logging": "report.png",
                "journal": "journal.png",
                "reports": "report.png",
                "incident_summary": "incident.png",
                "setup": "setting.png",
                "comm": "comm.png",
                "email": "email.png",
            }
            for key, fname in mapping.items():
                p = ico_dir / fname
                if p.exists():
                    try:
                        img = tk.PhotoImage(file=str(p))
                        # Keep icons small to reduce sidebar clutter (operator-safe)
                        try:
                            w = int(img.width() or 0)
                            target = 22
                            if w > target:
                                f = max(1, int(round(w / float(target))))
                                img = img.subsample(f, f)
                        except Exception:
                            pass
                        images[key] = img
                    except Exception:
                        pass
        except Exception:
            return images
        return images

    def _build_sidebar(self):
        """Build left navigation with click-only compact/full toggle."""

        t = self._theme

        # â”€â”€ Design tokens â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        _PANEL   = t.panel       # "#1c1c1e"
        _BG      = t.bg          # "#141414"
        _SURFACE = t.surface     # "#2c2c2e"
        _TEXT    = t.text        # "#e8e4dc"
        _MUTED   = t.text_muted  # "#9a9490"
        _DIM     = t.text_dim    # "#5e5a56"
        _BORDER  = t.border      # "#38383a"

        # Expanded / compact widths (px)
        _W_EXPANDED = 210
        _W_COMPACT  = 44
        self._sidebar_w_expanded = _W_EXPANDED
        self._sidebar_w_compact  = _W_COMPACT

        # Neon group colours
        _C = {
            "monitor":    "#00d4ff",   # electric cyan
            "kpi":        "#39ff6e",   # neon green
            "protection": "#ff4f5e",   # coral-red
            "logging":    "#7b8cff",   # periwinkle blue
            "config":     "#c8a0ff",   # soft violet
        }

        _ACTIVE_BG  = "#111c30"   # deep midnight blue
        _ACTIVE_BAR = "#4da6ff"   # accent blue bar

        # â”€â”€ TOP: 2px accent glow line only â€” no app name text â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        tk.Frame(self.sidebar, bg=_ACTIVE_BAR, height=2).pack(fill="x", side="top")
        tk.Frame(self.sidebar, bg=_BORDER, height=1).pack(fill="x", side="top")

        # â”€â”€ SCROLLABLE NAV â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        canvas = tk.Canvas(self.sidebar, bg=_PANEL, highlightthickness=0, bd=0)
        scrollbar = ttk.Scrollbar(self.sidebar, orient="vertical", command=canvas.yview)
        canvas.configure(yscrollcommand=scrollbar.set)
        scrollbar.pack(side="right", fill="y")
        canvas.pack(side="top", fill="both", expand=True)

        nav_frame = tk.Frame(canvas, bg=_PANEL)
        nav_win = canvas.create_window((0, 0), window=nav_frame, anchor="nw")

        def _on_nav_configure(e=None):
            canvas.configure(scrollregion=canvas.bbox("all"))
        def _on_canvas_resize(e=None):
            canvas.itemconfig(nav_win, width=canvas.winfo_width())
        def _on_mwheel(e):
            try:
                canvas.yview_scroll(int(-1 * (e.delta / 120)), "units")
            except Exception:
                pass

        nav_frame.bind("<Configure>", _on_nav_configure)
        canvas.bind("<Configure>", _on_canvas_resize)
        canvas.bind("<MouseWheel>", _on_mwheel)

        # â”€â”€ Tooltip for compact mode â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        _tip_win = [None]

        def _show_tip(widget, text: str):
            _hide_tip()
            try:
                x = widget.winfo_rootx() + widget.winfo_width() + 4
                y = widget.winfo_rooty() + widget.winfo_height() // 2 - 10
                tw = tk.Toplevel(self)
                tw.wm_overrideredirect(True)
                tw.wm_geometry(f"+{x}+{y}")
                tw.configure(bg=_ACTIVE_BAR)
                # Outer border frame
                outer = tk.Frame(tw, bg=_ACTIVE_BAR, padx=1, pady=1)
                outer.pack()
                tk.Label(
                    outer, text=text,
                    bg="#0d1620", fg=_TEXT,
                    font=("Segoe UI", 9), padx=10, pady=4,
                ).pack()
                _tip_win[0] = tw
            except Exception:
                pass

        def _hide_tip(e=None):
            try:
                if _tip_win[0]:
                    _tip_win[0].destroy()
                    _tip_win[0] = None
            except Exception:
                pass

        def _sidebar_background_click(e=None):
            """Blank sidebar click toggles. Nav-item clicks handle themselves."""
            try:
                if e is not None:
                    owner = getattr(e, "widget", None)
                    if owner is not None and any(owner is b or owner in b.winfo_children() for b in getattr(self, "_sidebar_nav_buttons", [])):
                        return "break"
                self.toggle_sidebar()
                return "break"
            except Exception:
                return "break"

        # â”€â”€ Nav item factory â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        def _make_nav_item(parent, key: str, sym: str, label: str, color: str):
            item = tk.Frame(parent, bg=_PANEL, cursor="hand2", height=36)
            item.pack(side="top", fill="x")
            item.pack_propagate(False)

            # Left glow bar
            bar = tk.Frame(item, bg=_PANEL, width=3)
            bar.pack(side="left", fill="y")

            # Symbol
            sym_lbl = tk.Label(
                item, text=sym,
                bg=_PANEL, fg=color,
                font=("Segoe UI", 12), width=2, anchor="center",
            )
            sym_lbl.pack(side="left", padx=(3, 2))

            # Label text
            txt_lbl = tk.Label(
                item, text=label,
                bg=_PANEL, fg=_TEXT,
                font=("Segoe UI", 9), anchor="w",
            )
            txt_lbl.pack(side="left", fill="x", expand=True, padx=(2, 8))

            # State
            item._active    = False
            item._sym_color = color
            item._sym_lbl   = sym_lbl
            item._txt_lbl   = txt_lbl
            item._bar       = bar
            item._full_text = label

            def _set_active(active: bool):
                item._active = active
                if active:
                    item.config(bg=_ACTIVE_BG)
                    bar.config(bg=_ACTIVE_BAR)
                    sym_lbl.config(bg=_ACTIVE_BG, fg=color)
                    txt_lbl.config(bg=_ACTIVE_BG, fg="#ffffff", font=("Segoe UI", 9, "bold"))
                else:
                    item.config(bg=_PANEL)
                    bar.config(bg=_PANEL)
                    sym_lbl.config(bg=_PANEL, fg=color)
                    txt_lbl.config(bg=_PANEL, fg=_TEXT, font=("Segoe UI", 9))

            def _on_enter(e):
                if not item._active:
                    item.config(bg=_SURFACE)
                    bar.config(bg=color)          # neon bar on hover
                    sym_lbl.config(bg=_SURFACE)
                    txt_lbl.config(bg=_SURFACE)
                if self._sidebar_compact:
                    _show_tip(item, label)

            def _on_leave(e):
                if not item._active:
                    item.config(bg=_PANEL)
                    bar.config(bg=_PANEL)
                    sym_lbl.config(bg=_PANEL)
                    txt_lbl.config(bg=_PANEL)
                _hide_tip()

            def _on_click(e):
                if getattr(self, "_sidebar_compact", False):
                    _hide_tip()
                    self.toggle_sidebar()
                else:
                    self.show_page(key)
                return "break"

            for w in (item, sym_lbl, txt_lbl, bar):
                w.bind("<Enter>",    _on_enter)
                w.bind("<Leave>",    _on_leave)
                w.bind("<Button-1>", _on_click)

            item.set_active = _set_active
            item._page_key  = key

            self._sidebar_nav_buttons.append(item)
            self._nav_buttons[key] = item
            return item

        # â”€â”€ Group factory â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        # items: list of (key, symbol, label)
        def add_group(title: str, color: str, items: list):
            visible_items = [
                (key, sym, label)
                for key, sym, label in (items or [])
                if self._has_permission(key) and key in (self._page_widgets or {})
            ]
            if not visible_items:
                return
            grp = tk.Frame(nav_frame, bg=_PANEL)
            grp.pack(side="top", fill="x", pady=(8, 0))

            # Header: colored pip + label (hidden in compact)
            hdr = tk.Frame(grp, bg=_PANEL)
            hdr.pack(fill="x")

            pip = tk.Frame(hdr, bg=color, width=3, height=12)
            pip.pack(side="left", padx=(10, 6), pady=4)

            hdr_lbl = tk.Label(
                hdr, text=title,
                bg=_PANEL, fg=color,
                font=("Segoe UI", 7, "bold"),
            )
            hdr_lbl.pack(side="left")

            # Store for compact toggle
            hdr._pip = pip
            hdr._lbl = hdr_lbl
            self._sidebar_headers.append(hdr)

            first_item = [None]
            for key, sym, label in visible_items:
                nav = _make_nav_item(grp, key, sym, label, color)
                if first_item[0] is None:
                    first_item[0] = nav
            hdr._first_nav = first_item[0]   # used to re-insert hdr before items

        # â”€â”€ Groups â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        add_group("START", _C["monitor"], [
            ("home",        "H",       "Start Here"),
        ])
        add_group("OPERATIONS", _C["monitor"], [
            ("dashboard",   "\u25a3",  "Dashboard"),
            ("status",      "\u25c9",  "Meter Status"),
            ("data_graphs", "\u223c",  "Data & Graphs"),
        ])
        add_group("INSIGHTS", _C["kpi"], [
            ("analytics",   "A",       "Analytics"),
            ("intelligence", "I",      "Intelligence"),
            ("solar_kpi", "\u25d0",  "Solar KPI"),
            ("hydro_kpi", "\u224b",  "Hydro KPI"),
            ("load_kpi",  "\u25a8",  "Load KPI"),
        ])
        add_group("EVENTS", _C["protection"], [
            ("alarms",           "\u25b2",  "Active Alarms"),
            ("incident_summary", "\u25fc",  "Incident Summary"),
            ("protection",       "\u26a0",  "Protection Rules"),
        ])
        add_group("RECORDS", _C["logging"], [
            ("logging", "\u25a6",  "Data Logger"),
            ("reports", "\u25a4",  "Reports"),
            ("journal", "\u25a5",  "Event Journal"),
        ])
        _config_items = [
            ("setup",        "\u2699",  "Setup"),
            ("comm",         "\u21c4",  "Communication"),
            ("connectivity", "\u25a1",  "Connectivity"),
            ("email",        "\u2709",  "Email Alerts"),
        ]
        if self.auth_service is not None:
            _config_items.append(("access_control", "\u25d7", "User & Access"))
        add_group("SETTINGS", _C["config"], _config_items)

        tk.Frame(nav_frame, bg=_PANEL, height=12).pack(side="top")

        # â”€â”€ FOOTER: toggle + version + online dot â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        tk.Frame(self.sidebar, bg=_BORDER, height=1).pack(side="bottom", fill="x")
        footer = tk.Frame(self.sidebar, bg=_BG)
        footer.pack(side="bottom", fill="x")

        # Online dot
        self._sidebar_online_dot = tk.Label(
            footer, text="â—",
            bg=_BG, fg=_DIM,
            font=("Segoe UI", 9),
        )
        self._sidebar_online_dot.pack(side="right", padx=(0, 10), pady=6)

        # Version (hidden in compact)
        self._sidebar_ver_lbl = tk.Label(
            footer, text=f"v{VERSION}",
            bg=_BG, fg=_DIM,
            font=("Segoe UI", 7), anchor="w",
        )
        self._sidebar_ver_lbl.pack(side="left", padx=(12, 0), pady=6)

        # â”€â”€ TOGGLE BUTTON: collapses / expands sidebar â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        # Placed as a separate narrow strip on the RIGHT edge of the sidebar â€”
        # always visible, acts as a drag-handle / arrow button.
        # Arrow: Â« when expanded (click to collapse), Â» when compact (click to expand).
        self._toggle_rail = tk.Frame(self.sidebar, bg=_BG, width=16, cursor="hand2")
        self._toggle_rail.pack(side="right", fill="y")
        self._toggle_rail.pack_propagate(False)

        self._toggle_arrow = tk.Label(
            self._toggle_rail, text="\u00ab",   # Â«
            bg=_BG, fg=_BORDER,
            font=("Segoe UI", 10, "bold"),
            cursor="hand2",
        )
        self._toggle_arrow.place(relx=0.5, rely=0.5, anchor="center")

        def _arrow_enter(e):
            self._toggle_arrow.config(fg=_ACTIVE_BAR)
            self._toggle_rail.config(bg=_SURFACE)
            self._toggle_arrow.config(bg=_SURFACE)
        def _arrow_leave(e):
            self._toggle_arrow.config(fg=_BORDER)
            self._toggle_rail.config(bg=_BG)
            self._toggle_arrow.config(bg=_BG)

        for w in (self._toggle_rail, self._toggle_arrow):
            w.bind("<Enter>",    _arrow_enter)
            w.bind("<Leave>",    _arrow_leave)
            w.bind("<Button-1>", lambda e: self.toggle_sidebar())

        for w in (self.sidebar, canvas, nav_frame, footer):
            try:
                w.bind("<Button-1>", _sidebar_background_click, add="+")
            except Exception:
                pass

    
    def _open_graphs_from_status(self, meter_id):
        """Status -> Data/Graphs jump. meter_id may be 'TOTAL' or int."""
        try:
            if not self._has_permission("data_graphs"):
                self._audit("ACCESS_DENIED", "page=data_graphs", success=False)
                self._notify_operator("Access denied: Data & Graphs is not enabled for this login.")
                return
            self.show_page("data_graphs")
            tab = self._page_widgets.get("data_graphs")
            if tab and hasattr(tab, "select_source"):
                tab.select_source(meter_id)
        except Exception:
            pass

    def show_page(self, key: str):
        """Show a page by key. Operator-safe: never raises."""
        try:
            key = str(key or "").strip().lower()
            if not self._has_permission(key):
                self._audit("ACCESS_DENIED", f"page={key}", success=False)
                self._notify_operator(f"Access denied: {key or 'unknown page'} is not enabled for this login.")
                return
            if key not in self._page_widgets:
                self._audit("NAVIGATE_UNAVAILABLE", f"page={key}", success=False)
                self._notify_operator(f"Page unavailable: {key}. Check role permissions or enabled features.")
                return
            if self._current_page_key == key:
                return

            # Hide old
            if self._current_page_key and self._current_page_key in self._page_widgets:
                try:
                    self._page_widgets[self._current_page_key].grid_remove()
                except Exception:
                    pass

            # Show new
            w = self._page_widgets[key]
            try:
                w.grid()
                w.lift()
            except Exception:
                pass

            # Active button styling
            try:
                for k, b in (self._nav_buttons or {}).items():
                    if hasattr(b, "set_active"):
                        b.set_active(k == key)
                    else:
                        b.configure(style="SidebarNavActive.TButton" if k == key else "SidebarNav.TButton")
            except Exception:
                pass

            # Optional page lifecycle hook (for pages that need refresh on entry)
            try:
                if hasattr(w, "on_show") and callable(getattr(w, "on_show")):
                    w.on_show()
            except Exception:
                pass

            self._current_page_key = key
            self._audit("NAVIGATE", f"page={key}")
        except Exception:
            return

    def _make_health_chip(self, parent, title: str, initial: str):
        box = ttk.Frame(parent, style="HealthBar.TFrame")
        box.pack(side="left", padx=(0, 8))
        ttk.Label(box, text=f"{title}:", style="HealthTitle.TLabel").pack(side="left", padx=(0, 4))
        var = tk.StringVar(value=initial)
        lbl = tk.Label(box, textvariable=var, font=("Segoe UI", 9, "bold"), padx=8, pady=2, bg=self._theme.surface, fg=self._theme.text)
        lbl.pack(side="left")
        return var, lbl

    def _set_chip_state(self, label, state: str):
        state_u = str(state or "").upper()
        bg = self._theme.surface
        fg = self._theme.text
        if any(k in state_u for k in ("ONLINE", "GOOD", "RUNNING", "OK")):
            bg = self._theme.good; fg = "#ffffff"
        elif any(k in state_u for k in ("STALE", "MIXED", "RECOVERING", "PAUSED")):
            bg = self._theme.warn; fg = "#ffffff"
        elif any(k in state_u for k in ("OFFLINE", "FAULT", "ERROR", "INVALID")):
            bg = self._theme.alarm if ("FAULT" in state_u or "ERROR" in state_u or "INVALID" in state_u) else self._theme.stale; fg = "#ffffff"
        elif "WRITE" in state_u:
            bg = self._theme.accent; fg = "#ffffff"
        label.configure(bg=bg, fg=fg)

    def _update_health_strip(self, health: dict):
        try:
            self.health_detail_var.set(str(health.get("detail", "")))
            self.health_comm_var.set(str(health.get("comm", "â€”")))
            self.health_data_var.set(str(health.get("data", "â€”")))
            self.health_log_var.set(str(health.get("logging", "â€”")))
            self.health_mode_var.set(str(health.get("mode", "RUN")))
            self._set_chip_state(self.health_comm_lbl, self.health_comm_var.get())
            self._set_chip_state(self.health_data_lbl, self.health_data_var.get())
            self._set_chip_state(self.health_log_lbl, self.health_log_var.get())
            self._set_chip_state(self.health_mode_lbl, self.health_mode_var.get())
        except Exception:
            pass
        # Push detail text (auto-reconnect countdown etc.) to bottom status bar right side
        try:
            detail = str(health.get("detail", "") or "")
            sb = getattr(self, "status_bar", None)
            if sb and detail:
                sb.summary_var.set(detail)
        except Exception:
            pass
        # Update sidebar online dot
        try:
            dot = getattr(self, "_sidebar_online_dot", None)
            if dot:
                comm = str(health.get("comm", "")).upper()
                if any(k in comm for k in ("ONLINE", "GOOD", "OK", "LIVE")):
                    dot.config(fg="#00d4ff")
                elif any(k in comm for k in ("STALE", "RECOVERING", "RECONNECTING", "OLD DATA")):
                    dot.config(fg="#f0a030")
                else:
                    dot.config(fg="#5e5a56")
        except Exception:
            pass

    # ------------------------------------------------------------------
    # UI BUILD
    # ------------------------------------------------------------------
    def _build_ui(self):
        # Root layout: use GRID so StatusBar never gets covered by Notebook
        # when the window is maximized/full-screen.
        self.grid_rowconfigure(0, weight=0)  # top bar
        self.grid_rowconfigure(1, weight=1)  # notebook
        self.grid_rowconfigure(2, weight=0)  # status bar
        self.grid_columnconfigure(0, weight=1)

        # ---------------- TOP BAR ----------------
        top_wrap = ttk.Frame(self)
        top_wrap.grid(row=0, column=0, sticky="ew", padx=10, pady=6)
        top_wrap.grid_columnconfigure(0, weight=1)

        top = ttk.Frame(top_wrap)
        top.grid(row=0, column=0, sticky="ew")

        # Top health bar removed â€” status info consolidated into bottom StatusBar.
        # Keep dummy StringVars/Labels so _update_health_strip() doesn't crash.
        _dummy = ttk.Frame(top_wrap)  # never gridded â€” invisible
        self.health_comm_var, self.health_comm_lbl = self._make_health_chip(_dummy, "Connection", "OFFLINE")
        self.health_data_var, self.health_data_lbl = self._make_health_chip(_dummy, "Data", "OFFLINE")
        self.health_log_var, self.health_log_lbl  = self._make_health_chip(_dummy, "Logger", "OFF")
        self.health_mode_var, self.health_mode_lbl = self._make_health_chip(_dummy, "Mode", "RUN")
        self.health_detail_var = tk.StringVar(value="")

        # LEFT: Site / Plant Information (3-line display)
        left = ttk.Frame(top)
        left.pack(side="left", anchor="nw")

        ui_cfg = (self.cfg.get("ui") or {})
        privacy = bool(ui_cfg.get("privacy_mode", True))

        plant_name = (self.cfg.get("site", {}) or {}).get("plant_name", "")
        location = (self.cfg.get("site", {}) or {}).get("location", "")
        description = (self.cfg.get("site", {}) or {}).get("description", "")

        # Show actual plant name/location in header always.
        # Privacy mode only affects external log exports, not the local display.
        if not plant_name:
            plant_name = self.APP_NAME

        # Single-line styled display (reduces top-bar height):
        # Plant (large) + Location (medium) + Description (small)
        line = ttk.Frame(left)
        line.pack(side="top", anchor="w")

        self.lbl_site_name = ttk.Label(
            line,
            text=plant_name,
            font=("Segoe UI", 13, "bold"),
            style="HeaderLine.TLabel",
        )
        self.lbl_site_name.pack(side="left", anchor="w")

        self.lbl_site_location = ttk.Label(
            line,
            text=(f"  â€”  {location}" if (location or "").strip() else ""),
            font=("Segoe UI", 10, "bold"),
            style="HeaderLine.TLabel",
        )
        self.lbl_site_location.pack(side="left", anchor="w")

        self.lbl_site_desc = ttk.Label(
            line,
            text=(f"  |  {description}" if description else ""),
            font=("Segoe UI", 9),
            style="HeaderLineMuted.TLabel",
        )
        self.lbl_site_desc.pack(side="left", anchor="w")

        # MIDDLE (center): realtime clock (large, always visible)
        # We give the clock its own expanding frame so it stays centered,
        # independent of right-side controls.
        mid = ttk.Frame(top)
        mid.pack(side="left", fill="x", expand=True)

        self.clock_var = tk.StringVar(value="")
        self.lbl_clock = ttk.Label(
            mid,
            textvariable=self.clock_var,
            font=("Segoe UI", 13, "bold"),
            style="HeaderLine.TLabel",
        )
        self.lbl_clock.pack(side="top", expand=True)

        # RIGHT: session + compact selector (Total / M1..M6)
        right = ttk.Frame(top)
        right.pack(side="right", anchor="e")

        session_badge = ttk.Frame(right)
        session_badge.pack(side="right", anchor="e", padx=(10, 0))
        self.session_status_var = tk.StringVar(value=f"{self.session_username} ({self._role_label()})")
        ttk.Label(
            session_badge,
            textvariable=self.session_status_var,
            style="HeaderLineMuted.TLabel",
        ).pack(side="left", padx=(0, 8))
        logout_wrap = tk.Frame(session_badge, bg="#4da6ff", padx=1, pady=1)
        logout_wrap.pack(side="left")
        self._logout_btn = tk.Label(
            logout_wrap,
            text="SIGN OUT",
            bg="#122033",
            fg="#b8e3ff",
            font=("Segoe UI", 9, "bold"),
            padx=12,
            pady=4,
            cursor="hand2",
        )
        self._logout_btn.pack(side="left")
        self._logout_btn.bind("<Enter>", lambda _e: self._logout_btn.config(bg="#1b3553", fg="#ffffff"))
        self._logout_btn.bind("<Leave>", lambda _e: self._logout_btn.config(bg="#122033", fg="#b8e3ff"))
        self._logout_btn.bind("<Button-1>", lambda _e: self._do_logout())

        self.selector = MeterSelector(right, self.cfg, self.meters, max_meters=max(1, len(self.meters)))
        self.selector.pack(side="right", anchor="e", padx=(0, 12))

        # ---------------- BODY (SIDEBAR + PAGES) ----------------
        body = ttk.Frame(self, style="App.Panel.TFrame")
        body.grid(row=1, column=0, sticky="nsew", padx=8, pady=(0, 6))
        body.grid_columnconfigure(0, weight=0)  # sidebar
        body.grid_columnconfigure(1, weight=1)  # pages
        body.grid_rowconfigure(0, weight=1)

        # Keep a reference (needed for responsive sidebar minsize updates)
        self._body_frame = body

        # Sidebar (left) â€” plain tk.Frame so configure(width=N) is honoured by Tk.
        # ttk.Frame ignores explicit width when the theme engine is active.
        try:
            _sb_bg = get_theme().panel
        except Exception:
            _sb_bg = "#1c1c1e"
        self.sidebar = tk.Frame(body, bg=_sb_bg, width=210)
        self.sidebar.grid(row=0, column=0, sticky="nsw")
        self.sidebar.grid_propagate(False)
        self.sidebar.pack_propagate(False)

        # Pages container (right)
        self.pages = ttk.Frame(body, style="App.Panel.TFrame")
        self.pages.grid(row=0, column=1, sticky="nsew")
        self.pages.grid_rowconfigure(0, weight=1)
        self.pages.grid_columnconfigure(0, weight=1)

        # Protection engine: shared state + trend markers. app.py normally
        # constructs it so RuntimeService and UI read the same state.
        if self.protection_engine is None:
            from core.protection_engine import ProtectionEngine
            self.protection_engine = ProtectionEngine(self.cfg)
            self.protection_engine.set_alarm_bridge(self.alarm_engine)
        try:
            svc = getattr(self, "runtime_service", None)
            if svc is not None and hasattr(svc, "set_protection_engine"):
                svc.set_protection_engine(self.protection_engine)
        except Exception:
            pass

        # Load sidebar icons (safe, never crash)
        self._nav_images = self._load_nav_images()
        self._sidebar_headers = []
        self._sidebar_nav_buttons = []
        self._sidebar_compact = False
        self._sidebar_hover_job = None
        self._sidebar_leave_job = None
        self._anim_job = None

        # Instantiate pages (all live under self.pages)
        self._page_widgets = {}
        self._external_windows = {}  # view_name -> toplevel
        self._external_cfg_applied = False

        if self._has_permission("home"):
            self._page_widgets["home"] = OperatorHomeTab(
                self.pages,
                self.cfg,
                self.meters,
                worker=self.worker,
                alarm_engine=self.alarm_engine,
                logging_engine=self.logging_engine,
                data_store=self.data_store,
                snapshot_bus=self.snapshot_bus,
                session=self.session,
                on_open_page=self.show_page,
                can_open=self._can_show_page,
            )

        if self._has_permission("dashboard"):
            self._page_widgets["dashboard"] = DashboardTab(
                self.pages,
                self.meters,
                self.aggregator,
                self.selector,
                self.cfg,
                alarm_engine=self.alarm_engine,
                event_journal=getattr(self, "event_journal", None),
                protection_engine=self.protection_engine,
            )
        if self._has_permission("status"):
            self._page_widgets["status"] = StatusTab(
                self.pages,
                self.meters,
                self.aggregator,
                self.selector,
                self.cfg,
                on_open_graphs=self._open_graphs_from_status,
            )

        if self._has_permission("analytics"):
            self._page_widgets["analytics"] = AnalyticsTab(
                self.pages,
                self.cfg,
                self.meters,
                self.aggregator,
                on_open_page=self.show_page,
                can_open=self._can_show_page,
            )

        if self._has_permission("intelligence"):
            self._page_widgets["intelligence"] = IntelligenceTab(
                self.pages,
                self.cfg,
                self.meters,
                self.aggregator,
                worker=self.worker,
                alarm_engine=self.alarm_engine,
                logging_engine=self.logging_engine,
                remote_sync=getattr(self, "_remote_sync", None),
                on_open_page=self.show_page,
                can_open=self._can_show_page,
            )

        _le = self.logging_engine
        if self._has_permission("data_graphs") and "dashboard" in self._page_widgets:
            self._page_widgets["data_graphs"] = DataGraphsTab(
                self.pages, self.meters, self.selector, self.cfg,
                self._page_widgets["dashboard"].trend_manager,
                self.event_journal,
                historian_fn=lambda: getattr(_le, "historian", None),
            )

        if self._has_permission("solar_kpi"):
            self._page_widgets["solar_kpi"] = SolarKPITab(self.pages, self.cfg)
        if self._has_permission("hydro_kpi"):
            self._page_widgets["hydro_kpi"] = HydroKPITab(self.pages, self.cfg)
        if self._has_permission("load_kpi"):
            self._page_widgets["load_kpi"] = LoadKPITab(self.pages, self.cfg)

        self.tab_dashboard = self._page_widgets.get("dashboard")
        self.tab_real = None
        self.tab_data_graphs = self._page_widgets.get("data_graphs")

        if self._has_permission("protection"):
            self._page_widgets["protection"] = ScrollableContainer(self.pages)
            ProtectionView(
                self._page_widgets["protection"].body,
                cfg=self.cfg,
                meters=self.meters,
                snapshot_fn=self._get_snapshot,
                protection_engine=self.protection_engine,
                tier_filter=1,
            ).pack(fill="both", expand=True)

        if self._has_permission("alarms"):
            self._page_widgets["alarms"] = AlarmsTab(
                self.pages,
                alarm_engine=self.alarm_engine,
                meters=self.meters,
                session=self.session,
            )

        # Logging
        # Logging â€” wrapped in scroll container
        if self._has_permission("incident_summary"):
            self._page_widgets["incident_summary"] = IncidentSummaryTab(
                self.pages,
                cfg=self.cfg,
                meters=self.meters,
                selector=self.selector,
            )

        if self._has_permission("logging"):
            self._page_widgets["logging"] = ScrollableContainer(self.pages)
            LoggingTab(
                self._page_widgets["logging"].body,
                self.logging_engine,
                self.cfg,
                snapshot_bus=self.snapshot_bus,
            ).pack(fill="both", expand=True)

        # Shared CSV file used by Reports + Email auto-send watcher.
        # Keep it deterministic and inside logs/ so operators can find it.
        pe_csv = os.path.join(logs_dir(), "protection_events.csv")



        if self._has_permission("reports"):
            self._page_widgets["reports"] = ReportsTab(
                self.pages,
                cfg=self.cfg,
                meters=self.meters,
                snapshot_fn=self._get_snapshot,
                protection_events_csv=pe_csv,
                on_save_config=lambda *_, **__: self._persist_config("reports"),
                on_print_snapshot=self.print_snapshot_now,
            )

        if self._has_permission("email"):
            from core.reports_engine import ReportsEngine
            from core.email_service import EmailService

            self._email_reports_engine = ReportsEngine(
                cfg=self.cfg,
                meters=self.meters,
                snapshot_fn=self._get_snapshot,
                protection_events_csv=pe_csv,
            )
            self._email_service = EmailService(cfg=self.cfg, reports_engine=self._email_reports_engine)
            self._page_widgets["email"] = ScrollableContainer(self.pages)
            EmailTab(
                self._page_widgets["email"].body,
                cfg=self.cfg,
                email_service=self._email_service,
                on_save_config=lambda *_, **__: self._persist_config("email"),
            ).pack(fill="both", expand=True)


        if self._has_permission("setup"):
            self._page_widgets["setup"] = SetupMainTab(
                self.pages,
                self.cfg,
                self.meters,
                root_tk=self,
                station_apply_cb=self._station_apply,
                keep_awake_cb=self._set_keep_awake,
                auth_service=self.auth_service,
                session=self.session,
                logging_engine=self.logging_engine,
                data_store=self.data_store,
                snapshot_bus=self.snapshot_bus,
                worker=self.worker,
            )
            try:
                w = self._page_widgets.get("setup")
                if w is not None and hasattr(w, "set_worker"):
                    w.set_worker(self.worker)
            except Exception:
                pass

        if self._has_permission("comm"):
            self._page_widgets["comm"] = CommTab(
                self.pages,
                self.cfg,
                self.meters,
                worker=self.worker,
                on_apply=self._apply_comm_and_save,
                on_title_change=self._on_comm_title_change,
            )

        if self._has_permission("connectivity"):
            self._page_widgets["connectivity"] = ConnectivityTab(
                self.pages,
                self.cfg,
                on_save_config=lambda *_, **__: self._persist_config("connectivity"),
                mqtt_publisher=getattr(self, "_mqtt_publisher", None),
                rest_api=getattr(self, "_rest_api", None),
                remote_sync=getattr(self, "_remote_sync", None),
                on_apply_comm=self._apply_comm_and_save,
                sms_service=getattr(self, "_sms_service", None),
            )
            _fb_pub = getattr(self, "_firebase_publisher", None)
            if _fb_pub is not None:
                try:
                    self._page_widgets["connectivity"].set_firebase_publisher(_fb_pub)
                except Exception:
                    pass

        if self._has_permission("journal"):
            self._page_widgets["journal"] = JournalTab(
                self.pages,
                self.cfg,
                meters=self.meters,
                event_journal=getattr(self, "event_journal", None),
            )

        if self._has_permission("access_control") and self.auth_service is not None:
            self._page_widgets["access_control"] = AccessControlTab(
                self.pages,
                auth_service=self.auth_service,
                session=self.session,
                on_save_config=lambda *_, **__: self._persist_config("access_control"),
                on_logout=self._do_logout,
            )

        # Layout pages (stacked)
        for w in self._page_widgets.values():
            w.grid(row=0, column=0, sticky="nsew")
            w.grid_remove()

        try:
            home = self._page_widgets.get("home")
            if home is not None and hasattr(home, "update_view"):
                home.update_view()
        except Exception:
            pass

        # Build sidebar groups + buttons
        self._nav_buttons = {}
        self._build_sidebar()

        # Sidebar is click-toggled only. Start compact and do not run hover/autohide polling.
        self._sidebar_mouse_inside = False
        self._sidebar_autohide_job = None
        self._sidebar_enter_job = None
        self._sidebar_leave_job = None
        self._sidebar_compact = False   # force _set_sidebar_compact to act
        self._set_sidebar_compact(True)
        try:
            self.sidebar.configure(width=getattr(self, "_sidebar_w_compact", 44))
        except Exception:
            pass

        # Sidebar size is controlled by explicit operator click only.
        self._resize_job = None

        # Default page: operator-first monitoring view.
        self._current_page_key = None
        for default_page in self._preferred_start_pages():
            if default_page in self._page_widgets and self._has_permission(default_page):
                self.show_page(default_page)
                break

        # ---------------- STATUS BAR ----------------
        self.status_bar = StatusBar(self)
        self.status_bar.grid(row=2, column=0, sticky="ew")

    # ------------------------------------------------------------------
    # COMM TAB CALLBACK (do NOT change window title)
    # ------------------------------------------------------------------
    def _on_comm_title_change(self):
        """
        Communication tab calls this after updating plant/location.
        We keep window title constant.
        This hook exists so CommTab can trigger any future UI refresh if needed.
        """
        self.title(f"{APP_NAME} v{VERSION}")

        # Persist to disk immediately and audit it.
        self._persist_config("comm_title")

        # Push updated site info to Firebase heartbeat so web dashboard shows it
        try:
            fb_pub = getattr(self, "_firebase_publisher", None)
            if fb_pub is not None:
                site = self.cfg.get("site", {})
                fb_pub.push_site_info(
                    plant_name=str(site.get("plant_name", "") or ""),
                    location=str(site.get("location", "") or ""),
                )
        except Exception:
            pass

        # Refresh Site / Plant Information header labels with real values
        site = self.cfg.get("site", {}) or {}
        plant_name  = str(site.get("plant_name",  "") or "").strip() or self.APP_NAME
        location    = str(site.get("location",    "") or "").strip()
        description = str(site.get("description", "") or "").strip()
        try:
            self.lbl_site_name.config(text=plant_name)
            self.lbl_site_location.config(text=(f"  â€”  {location}" if location else ""))
            self.lbl_site_desc.config(text=(f"  |  {description}" if description else ""))
        except Exception:
            pass

    # ------------------------------------------------------------------
    # WORKER HOT-SWAP (used by Supervisor)
    # ------------------------------------------------------------------
    def set_worker(self, worker):
        """Replace the Modbus worker reference safely.

        This updates all tabs that depend on the worker.
        """
        self.worker = worker
        try:
            svc = getattr(self, "runtime_service", None)
            if svc is not None and hasattr(svc, "set_worker"):
                svc.set_worker(worker)
            elif svc is not None:
                svc.worker = worker
        except Exception:
            pass
        # Update any page that depends on the worker.
        try:
            home = (self._page_widgets or {}).get("home")
            if home is not None and hasattr(home, "set_worker"):
                home.set_worker(worker)
        except Exception:
            pass
        try:
            intel = (self._page_widgets or {}).get("intelligence")
            if intel is not None and hasattr(intel, "set_worker"):
                intel.set_worker(worker)
        except Exception:
            pass
        try:
            comm = (self._page_widgets or {}).get("comm")
            if comm is not None:
                comm.worker = worker
        except Exception:
            pass
        try:
            setup = (self._page_widgets or {}).get("setup")
            if setup is not None and hasattr(setup, "set_worker"):
                setup.set_worker(worker)
        except Exception:
            pass

    def _restart_worker(self, reason: str):
        """Stop old worker and start a fresh one using worker_factory.

        Runs in UI thread only.
        """
        if not callable(self.worker_factory):
            return
        log_once(logger, "ui.supervisor.restart", "warning", f"Supervisor restarting worker: {reason}", cooldown_sec=5)

        old = self.worker
        # Stop old
        try:
            if old is not None:
                old.stop()
                old.join(timeout=2.0)
        except Exception:
            pass

        # Start new
        try:
            new_worker = self.worker_factory()
            new_worker.start()
            self.set_worker(new_worker)
        except Exception as e:
            log_once(logger, "ui.supervisor.restart.fail", "exception", f"Worker restart failed: {type(e).__name__}: {e}", cooldown_sec=5)

    # ------------------------------------------------------------------
    # SNAPSHOT FOR PROTECTION
    # ------------------------------------------------------------------
    def _get_snapshot(self):
        now = time.time()
        stale_sec = stale_seconds(self.cfg)

        meters_snap = {}
        for m in self.meters:
            ts = getattr(m, "last_good_ts", None) or getattr(m, "last_update_ts", None) or now
            try:
                ts = float(ts)
            except Exception:
                ts = now

            age = now - ts
            enabled = bool(getattr(m, "enabled", True))
            raw_valid = bool(getattr(m, "data_valid", False))
            data_valid = enabled and raw_valid and age <= stale_sec

            meters_snap[int(m.meter_id)] = {
                "values": getattr(m, "values", {}) or {},
                "data_valid": data_valid,
                "ts": ts,
                "enabled": enabled,
            }

        total_values = self.aggregator.compute(self.meters) or {}
        total_valid = any(v["enabled"] and v["data_valid"] for v in meters_snap.values())

        return {
            "total": {"values": total_values, "data_valid": total_valid, "ts": now},
            "meters": meters_snap,
        }

    # ------------------------------------------------------------------
    # UI TICK LOOP
    # ------------------------------------------------------------------
    def _ui_tick(self):
        # If window is minimized/iconified, suspend heavy UI work (keeps CPU low, prevents flicker on restore).
        try:
            if str(self.state()) == 'iconic':
                self.after(1000, self._ui_tick)
                return
        except Exception:
            pass

        # NOTE: Clock is updated by _tick_clock() (500ms). Do not update here.
        # Keeping clock updates out of the heavy UI tick reduces stutter.
        self._had_new_snapshot = False
        stale_sec = stale_seconds(self.cfg)
        try:
            self._consume_snapshots(stale_sec)
        except Exception as e:
            log_once(logger, "ui.consume_snapshots", "exception", f"UI consume snapshots failed: {type(e).__name__}: {e}", cooldown_sec=5)

        # SCADA-grade safety net: even if SnapshotBus queues drop, pull the
        # authoritative latest snapshots and apply them.
        try:
            self._sync_from_data_store(stale_sec)
        except Exception as e:
            log_once(logger, "ui.ds_sync", "exception", f"DataStore sync failed: {type(e).__name__}: {e}", cooldown_sec=5)

        # Supervisor: monitor worker health and restart safely if needed.
        try:
            now = time.time()
            if (now - float(self._last_supervisor_tick_ts or 0.0)) >= 1.0:
                decision = self.supervisor.tick(self.worker)
                self._last_supervisor_tick_ts = now
                if getattr(decision, "should_restart", False):
                    self._restart_worker(getattr(decision, "reason", ""))
                elif getattr(decision, "limit_hit", False):
                    try:
                        self.alarm_engine._raise(
                            "SYSTEM", "SUPERVISOR_LIMIT",
                            "Worker restart limit reached â€” check hardware/cabling. App will not auto-recover until next hour.",
                            "ALARM",
                        )
                    except Exception:
                        pass
        except Exception as e:
            log_once(logger, "ui.supervisor.tick", "exception", f"Supervisor tick failed: {type(e).__name__}: {e}", cooldown_sec=10)
        # Refresh qualities at a low rate. Per-snapshot refresh already happens in _consume_snapshots().
        try:
            now = time.time()
            if not hasattr(self, "_last_quality_refresh_ts"):
                self._last_quality_refresh_ts = 0.0
            if (now - float(self._last_quality_refresh_ts)) >= 2.0:
                self._refresh_meter_qualities(stale_sec)
                self._last_quality_refresh_ts = now
        except Exception as e:
            log_once(logger, "ui.refresh_quality", "exception", f"UI refresh quality failed: {type(e).__name__}: {e}", cooldown_sec=5)

        # Alarm evaluation is owned by RuntimeService when available. Keep the
        # old UI-thread path as fallback for developer runs without the service.
        if not (getattr(getattr(self, "runtime_service", None), "handles_alarms", False) and getattr(self.runtime_service, "is_running", False)):
            try:
                now = time.time()
                if not hasattr(self, "_last_alarm_eval_ts"):
                    self._last_alarm_eval_ts = 0.0
                if (now - float(self._last_alarm_eval_ts)) >= 1.0:
                    for m in self.meters:
                        self.alarm_engine.evaluate_meter(m)
                    # Expire timed shelves (maintenance windows that have elapsed)
                    try:
                        self.alarm_engine.unshelve_expired()
                    except Exception:
                        pass
                    self._last_alarm_eval_ts = now
            except Exception as e:
                log_once(logger, "ui.alarm_evaluate", "exception", f"Alarm evaluation failed: {type(e).__name__}: {e}", cooldown_sec=5)

        # Logging + trend feed
        # Computing TOTAL + feeding trends can be expensive; do it only when new data arrives
        # and no faster than 1 Hz.
        try:
            now = time.time()
            if not hasattr(self, "_last_total_compute_ts"):
                self._last_total_compute_ts = 0.0
            do_total = self._had_new_snapshot and (now - float(self._last_total_compute_ts)) >= 1.0
            total = None
            if do_total:
                # IMPORTANT: pass cfg so TotalAggregator respects selected meters and
                # computes _valid_meter_count correctly. Without cfg, TOTAL trends can
                # look empty/dotted when the dashboard isn't the active page.
                total = self.aggregator.compute(self.meters, cfg=self.cfg)

            # Update TOTAL mini snapshot for journaling (used by ModbusWorker bus events)
            if callable(getattr(self, "_total_mini_update_cb", None)) and isinstance(total, dict):
                try:
                    mini = {
                        "quality": str(getattr(self, "total_quality", "")) if hasattr(self, "total_quality") else "GOOD",
                        "kW": total.get("kW"),
                        "Vavg": total.get("Vavg"),
                        "Iavg": total.get("Iavg"),
                        "PFavg": total.get("PFavg"),
                    }
                    self._total_mini_update_cb(mini)
                except Exception:
                    pass

                self._last_total_compute_ts = now
            # Logging is owned by RuntimeService when available. Keep fallback for
            # developer runs where RuntimeService is intentionally not provided.
            if not (getattr(getattr(self, "runtime_service", None), "handles_logging", False) and getattr(self.runtime_service, "is_running", False)):
                self.logging_engine.tick(self.meters, total_dict=total)
            # Feed trends only when we computed a fresh TOTAL / received new samples.
            if do_total and hasattr(self, "tab_dashboard") and hasattr(self.tab_dashboard, "feed_trends"):
                extra = None
                try:
                    if hasattr(self, 'tab_data_graphs') and hasattr(self.tab_data_graphs, 'collect_needed_keys'):
                        extra = self.tab_data_graphs.collect_needed_keys()
                except Exception:
                    extra = None
                self.tab_dashboard.feed_trends(self.meters, total_dict=total, extra_needed=extra)
        except Exception as e:
            log_once(logger, "ui.logging_tick", "exception", f"Logging tick failed: {type(e).__name__}: {e}", cooldown_sec=5)

        # Protection supervision is owned by RuntimeService when available.
        # Keep only dashboard overlay injection on the UI thread.
        try:
            if hasattr(self, "protection_engine") and self.protection_engine:
                now = time.time()
                if not hasattr(self, "_last_prot_tick_ts"):
                    self._last_prot_tick_ts = 0.0
                if (not self._runtime_handles_protection()) and (now - float(self._last_prot_tick_ts)) >= 1.0:
                    self.protection_engine.tick(self._get_snapshot())
                    self._last_prot_tick_ts = now
                if hasattr(self, "tab_dashboard") and hasattr(self.tab_dashboard, "set_protection_alarm_ranges"):
                    self.tab_dashboard.set_protection_alarm_ranges(self.protection_engine.get_active_alarm_ranges())
        except Exception as e:
            log_once(logger, "ui.protection_tick", "exception", f"Protection tick failed: {type(e).__name__}: {e}", cooldown_sec=5)

        # Solar KPI tab â€” only update when it is the active page (cheap guard)
        try:
            if self._current_page_key == "solar_kpi":
                solar_tab = self._page_widgets.get("solar_kpi")
                if solar_tab is not None and hasattr(solar_tab, "update_view"):
                    solar_tab.update_view(total if total else None)
        except Exception as e:
            log_once(logger, "ui.solar_kpi", "exception",
                     f"Solar KPI update failed: {type(e).__name__}: {e}", cooldown_sec=5)

        # Hydro KPI tab - only update when active.
        try:
            if self._current_page_key == "hydro_kpi":
                hydro_tab = self._page_widgets.get("hydro_kpi")
                if hydro_tab is not None and hasattr(hydro_tab, "update_view"):
                    hydro_tab.update_view(total if total else None)
        except Exception as e:
            log_once(logger, "ui.hydro_kpi", "exception",
                     f"Hydro KPI update failed: {type(e).__name__}: {e}", cooldown_sec=5)

        # Load KPI tab - only update when active.
        try:
            if self._current_page_key == "load_kpi":
                load_tab = self._page_widgets.get("load_kpi")
                if load_tab is not None and hasattr(load_tab, "update_view"):
                    load_tab.update_view(total if total else None)
        except Exception as e:
            log_once(logger, "ui.load_kpi", "exception",
                     f"Load KPI update failed: {type(e).__name__}: {e}", cooldown_sec=5)


        # Connectivity status refresh. RuntimeService owns snapshot publishing when available.
        try:
            now = time.time()
            if (now - float(self._last_connectivity_tick_ts or 0.0)) >= 1.0:
                self._last_connectivity_tick_ts = now
                if (not self._runtime_handles_connectivity()) and total and (
                    self._remote_sync is not None or self._mqtt_publisher is not None or self._rest_api is not None
                ):
                    # UI fallback only: build values_by_source: TOTAL + per-meter.
                    values_by_source: dict = {"TOTAL": dict(total)}
                    quality_map: dict = {}
                    for m in self.meters:
                        if not bool(getattr(m, "enabled", True)):
                            continue
                        mid = f"M{int(m.meter_id)}"
                        values_by_source[mid] = dict(getattr(m, "values", {}) or {})
                        quality_map[mid] = str(getattr(m, "quality", "GOOD") or "GOOD")
                    quality_map["TOTAL"] = str(total.get("quality", getattr(self, "total_quality", "GOOD")) or "GOOD")

                    # Cache for on-demand snapshot report (Ctrl+P)
                    self._last_values_by_source = values_by_source
                    self._last_quality_map = quality_map

                    if self._remote_sync is not None:
                        try:
                            self._remote_sync.publish_snapshot(values_by_source, quality_map, now)
                        except Exception as _se:
                            log_once(logger, "ui.remote_sync", "exception",
                                     f"Remote sync error: {_se}", cooldown_sec=10)

                    elif self._mqtt_publisher is not None:
                        try:
                            self._mqtt_publisher.publish_snapshot(values_by_source, quality_map, now)
                        except Exception as _me:
                            log_once(logger, "ui.mqtt_pub", "exception",
                                     f"MQTT publish error: {_me}", cooldown_sec=10)

                    if self._remote_sync is None and self._rest_api is not None:
                        try:
                            self._rest_api.update_snapshot(values_by_source, quality_map, now)
                        except Exception as _re:
                            log_once(logger, "ui.rest_snap", "exception",
                                     f"REST snapshot error: {_re}", cooldown_sec=10)

                    # Push alarm state to REST /api/v1/alarms (always, regardless
                    # of remote_sync — alarms are a separate data channel)
                    if self._rest_api is not None:
                        try:
                            self._rest_api.update_alarms(self.alarm_engine)
                        except Exception:
                            pass

                # Historian rows → REST /api/v1/history  (low frequency: every 60 s)
                if self._rest_api is not None:
                    _hist_refresh_interval = 60.0
                    _hist_last = getattr(self, "_rest_hist_last_ts", 0.0)
                    if (now - _hist_last) >= _hist_refresh_interval:
                        self._rest_hist_last_ts = now
                        try:
                            historian = getattr(self.logging_engine, "historian", None)
                            if historian is not None:
                                self._rest_api.update_history(historian, limit=200)
                        except Exception:
                            pass

                # Connectivity tab status refresh (always, to show status changes)
                conn_tab = self._page_widgets.get("connectivity")
                if conn_tab is not None and self._current_page_key == "connectivity":
                    try:
                        conn_tab.update_view()
                    except Exception:
                        pass
        except Exception as e:
            log_once(logger, "ui.connectivity_tick", "exception",
                     f"Connectivity tick failed: {type(e).__name__}: {e}", cooldown_sec=10)

        # Status bar + window title alarm badge
        try:
            health = summarize_health(
                self.cfg,
                self.meters,
                worker=self.worker,
                logging_engine=self.logging_engine,
                stale_sec=stale_sec,
            )
            self._update_health_strip(health)
            self.status_bar.update_status(
                self.cfg,
                self.meters,
                worker=self.worker,
                stale_sec=stale_sec,
                logging_engine=self.logging_engine,
                health=health,
                alarm_engine=self.alarm_engine,
            )
            # Window title: append (N) when there are unacknowledged alarms so
            # the operator sees it even while on a different tab or when the
            # window is minimised (shows in taskbar tooltip).
            try:
                unack = int(getattr(self.alarm_engine, "unacknowledged_count", 0) or 0)
                base_title = f"{APP_NAME} v{VERSION}"
                new_title = f"({unack}) {base_title}" if unack > 0 else base_title
                if self.title() != new_title:
                    self.title(new_title)
            except Exception:
                pass
        except Exception as e:
            log_once(logger, "ui.status_bar", "exception", f"Status bar update failed: {type(e).__name__}: {e}", cooldown_sec=5)

        # System health widget on dashboard
        try:
            dash = self._page_widgets.get("dashboard")
            if dash is not None and hasattr(dash, "update_system_health"):
                bus = self.worker.get_bus_health() if self.worker else {}
                comm_state   = str((bus or {}).get("state", "OFFLINE") or "OFFLINE")
                last_rx_age  = (bus or {}).get("age", None)
                # historian last write age
                hist_size_mb = getattr(self.logging_engine, "db_size_mb", None) if self.logging_engine else None
                if callable(hist_size_mb):
                    hist_size_mb = hist_size_mb()
                _lwt = self.logging_engine.last_write_time if self.logging_engine and hasattr(self.logging_engine, "last_write_time") else None
                if callable(_lwt):
                    _lwt = _lwt()
                hist_last_s  = (time.time() - _lwt.timestamp()) if _lwt is not None else None
                log_health = getattr(self.logging_engine, "write_health", {}) if self.logging_engine else {}
                data_quality = self.data_store.get_quality_health() if self.data_store and hasattr(self.data_store, "get_quality_health") else {}
                bus_stats = self.snapshot_bus.stats() if self.snapshot_bus and hasattr(self.snapshot_bus, "stats") else {}
                # email counters
                _esvc = getattr(self, "_email_service", None)
                email_pending = _esvc.pending_count() if _esvc and hasattr(_esvc, "pending_count") else 0
                email_dead    = _esvc.dead_letter_count() if _esvc and hasattr(_esvc, "dead_letter_count") else 0
                # supervisor restart count
                restart_count = len(getattr(self.supervisor, "_restart_ts_hist", []) or []) if self.supervisor else 0
                dash.update_system_health({
                    "comm_state":    comm_state,
                    "last_rx_age":   last_rx_age,
                    "hist_size_mb":  hist_size_mb,
                    "hist_last_s":   hist_last_s,
                    "hist_queue":    int((log_health or {}).get("queue_depth", 0) or 0),
                    "hist_dropped":  int((log_health or {}).get("dropped_write_count", 0) or 0),
                    "data_quality_issues": int((data_quality or {}).get("total_validation_issue_count", 0) or 0),
                    "snapshot_drops": int((bus_stats or {}).get("dropped_count", 0) or 0),
                    "email_pending": email_pending,
                    "email_dead":    email_dead,
                    "restart_count": restart_count,
                    "uptime_s":      time.time() - getattr(self, "_start_ts", time.time()),
                })
        except Exception:
            pass

        # UI fallback only; RuntimeService owns app-status publishing when running.
        try:
            if (not self._runtime_handles_connectivity()) and (
                getattr(self, "_remote_sync", None) is not None
                or getattr(self, "_rest_api", None) is not None
                or getattr(self, "_mqtt_publisher", None) is not None
            ):
                worker_state = str(getattr(self.worker, "status", "UNKNOWN") or "UNKNOWN") if self.worker else "NO_WORKER"
                meter_count  = len([m for m in self.meters if bool(getattr(m, "enabled", True))])
                if getattr(self, "_remote_sync", None) is not None:
                    self._remote_sync.publish_app_status(worker_state, meter_count)
                elif getattr(self, "_rest_api", None) is not None:
                    self._rest_api.update_app_status(worker_state, meter_count)
                if getattr(self, "_remote_sync", None) is None and getattr(self, "_mqtt_publisher", None) is not None:
                    self._mqtt_publisher.publish_app_status(worker_state, meter_count)
        except Exception:
            pass

        # Selector comm color update
        try:
            self.selector.update_comm_status()
        except Exception as e:
            log_once(logger, "ui.selector_comm", "exception", f"Selector comm update failed: {type(e).__name__}: {e}", cooldown_sec=5)

        # UI refresh: update ONLY the currently visible page.
        # Updating all tabs every 500ms is expensive (matplotlib redraws, Treeview churn)
        # and causes noticeable lag on some PCs.
        try:
            key = getattr(self, "_current_page_key", None)
            if key and key in (self._page_widgets or {}):
                page = self._page_widgets[key]
                if hasattr(page, "update_view"):
                    now = time.time()
                    last = float(getattr(page, "_last_view_update_ts", 0.0) or 0.0)
                    # Performance hardening: Status table churn can stutter on weak PCs.
                    # Keep Status at 1 Hz even during active polling.
                    if key == "status":
                        min_period = 1.0
                    else:
                        min_period = 0.5 if self._had_new_snapshot else 2.0
                    if (now - last) >= min_period:
                        page.update_view()
                        page._last_view_update_ts = now
        except Exception as e:
            log_once(logger, "ui.page_update", "exception", f"Page update failed: {type(e).__name__}: {e}", cooldown_sec=5)

        self.after(750, self._ui_tick)

    def _consume_snapshots(self, stale_sec: float):
        if not self.snapshot_bus or not self._snapshot_queue:
            return
        meter_map = {m.meter_id: m for m in self.meters}
        # Drain a bounded number per tick to avoid UI freezes if snapshots pile up.
        # If the queue is badly backed up (slow PC, tab churn, disk stalls),
        # we "fast-forward" by keeping only the latest snapshot per meter.
        max_per_tick = 60
        n = 0
        try:
            backlog = int(getattr(self._snapshot_queue, "qsize", lambda: 0)() or 0)
        except Exception:
            backlog = 0

        if backlog > 500:
            # Fast-forward: consume a larger chunk but apply only the newest per meter.
            latest_by_meter = {}
            max_fast = 2000
            k = 0
            while k < max_fast:
                try:
                    snapshot = self._snapshot_queue.get_nowait()
                except Empty:
                    break
                latest_by_meter[int(snapshot.meter_id)] = snapshot
                k += 1

            for mid, snapshot in latest_by_meter.items():
                meter = meter_map.get(mid)
                if not meter:
                    continue
                try:
                    meter.apply_snapshot(snapshot)
                    meter.refresh_quality(stale_sec)
                    n += 1
                except Exception:
                    continue
        else:
            while n < max_per_tick:
                try:
                    snapshot = self._snapshot_queue.get_nowait()
                except Empty:
                    break
                meter = meter_map.get(snapshot.meter_id)
                if not meter:
                    continue
                meter.apply_snapshot(snapshot)
                meter.refresh_quality(stale_sec)
                n += 1

        # Used by _ui_tick() to avoid expensive computations when no new data arrived.
        if n > 0:
            self._had_new_snapshot = True

    def _sync_from_data_store(self, stale_sec: float) -> None:
        """Sync MeterState from DataStore (authoritative latest snapshots).

        Why: SnapshotBus queues can drop under UI load. DataStore guarantees
        a latest snapshot per meter, so the UI never "misses" the current state.

        We still consume SnapshotBus for low-latency updates, but DataStore is
        the safety net for SCADA-grade reliability.
        """

        if self.data_store is None:
            return

        any_new = False
        for m in self.meters:
            mid = int(getattr(m, "meter_id", 0))
            try:
                stored = self.data_store.get_latest(mid)
            except Exception:
                continue

            last_seq = int(self._ds_last_seq.get(mid, -1))
            if int(stored.seq) == last_seq:
                continue

            # Apply the authoritative snapshot
            try:
                m.apply_snapshot(stored.snapshot)
            except Exception:
                pass

            # Keep existing refresh_quality() logic for now, but it will
            # converge because apply_snapshot now propagates STALE properly.
            try:
                m.refresh_quality(stale_sec)
            except Exception:
                pass

            self._ds_last_seq[mid] = int(stored.seq)
            any_new = True

        if any_new:
            self._had_new_snapshot = True

    def _refresh_meter_qualities(self, stale_sec: float):
        for m in self.meters:
            m.refresh_quality(stale_sec)

    # ------------------------------------------------------------------
    def _apply_comm_and_save(self):
        try:
            if hasattr(self.selector, "persist_to_config"):
                self.selector.persist_to_config()
        except Exception as e:
            log_once(logger, "ui.persist_config", "exception", f"Persist config failed: {type(e).__name__}: {e}", cooldown_sec=5)

        self._persist_config("ui_apply")

        # Refresh selector labels if meter names changed
        try:
            if hasattr(self.selector, "refresh_labels"):
                self.selector.refresh_labels()
        except Exception as e:
            log_once(logger, "ui.refresh_labels", "exception", f"Refresh selector labels failed: {type(e).__name__}: {e}", cooldown_sec=5)

        # Force rebuild tab captions so renamed meters propagate everywhere.
        try:
            if hasattr(self, "tab_dashboard") and hasattr(self.tab_dashboard, "_build_tabs"):
                self.tab_dashboard._last_selection = None
                self.tab_dashboard._build_tabs()
        except Exception as e:
            log_once(logger, "ui.dashboard_rebuild", "exception", f"Dashboard tab rebuild failed: {type(e).__name__}: {e}", cooldown_sec=5)
        try:
            if hasattr(self, "tab_real") and hasattr(self.tab_real, "_build_tabs"):
                self.tab_real._last_selection = None
                self.tab_real._build_tabs()
        except Exception as e:
            log_once(logger, "ui.real_rebuild", "exception", f"Legacy real-values tab rebuild failed: {type(e).__name__}: {e}", cooldown_sec=5)

        if self.on_apply_comm:
            self.on_apply_comm()

    # ------------------------------------------------------------------
    # CLOCK (top-center)
    # ------------------------------------------------------------------
    def _start_clock(self):
        """Start realtime clock updates for the top bar."""
        try:
            self._tick_clock()
        except Exception as e:
            log_once(logger, "ui.clock", "exception", f"Clock start failed: {type(e).__name__}: {e}", cooldown_sec=10)

    def _tick_clock(self):
        # Format: 2026-01-19  02:48:44
        if hasattr(self, "clock_var"):
            self.clock_var.set(time.strftime("%Y-%m-%d  %H:%M:%S"))
        if hasattr(self, "session_status_var"):
            try:
                idle = max(0.0, time.time() - float(self._last_activity_ts or time.time()))
                remaining = max(0, int(float(self.session_timeout_sec) - idle))
                mins = max(1, int((remaining + 59) // 60))
                self.session_status_var.set(f"{self.session_username} ({self._role_label()}) | idle logout {mins}m")
            except Exception:
                self.session_status_var.set(f"{self.session_username} ({self._role_label()})")
        # 500ms is smooth enough without wasting CPU
        self.after(500, self._tick_clock)

    def _on_close(self):
        if getattr(self, "_closing", False):
            return
        self._closing = True
        if not bool(getattr(self, "_logout_requested", False)):
            self._audit("APP_CLOSE", "window_close")
        try:
            if hasattr(self, "email_service") and self.email_service:
                self.email_service.stop()
        except Exception:
            pass
        try:
            if hasattr(self, "email_reports_engine") and self.email_reports_engine:
                self.email_reports_engine.stop()
        except Exception:
            pass
        # MQTT/REST are process-owned by app.py. Do not stop them here,
        # because logout closes only the window and then recreates MainWindow.
        if self.snapshot_bus and self._snapshot_queue:
            self.snapshot_bus.unsubscribe(self._snapshot_queue)
        try:
            self.destroy()
        except Exception as e:
            log_once(logger, "ui.destroy", "exception", f"Destroy failed: {type(e).__name__}: {e}")

    def _do_logout(self, reason: str = "LOGOUT"):
        """Trigger logout: close app so the login loop re-prompts."""
        self._audit("LOGOUT", f"reason={reason}")
        self._logout_requested = True
        self._on_close()

    def _set_sidebar_compact(self, compact: bool):
        """Switch sidebar between icon-rail (compact=44px) and full (210px)."""
        if compact == self._sidebar_compact:
            return
        self._sidebar_compact = compact

        try:
            self.sidebar.pack_propagate(False)
            self.sidebar.grid_propagate(False)
        except Exception:
            pass

        _W_EXPANDED = getattr(self, "_sidebar_w_expanded", 210)
        _W_COMPACT  = getattr(self, "_sidebar_w_compact",  44)

        if compact:
            # â”€â”€ Hide: group header rows, text labels, version â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
            for h in getattr(self, "_sidebar_headers", []):
                try:
                    h.pack_forget()          # hide the whole header row
                except Exception:
                    pass
            for b in getattr(self, "_sidebar_nav_buttons", []):
                try:
                    b._txt_lbl.pack_forget()
                except Exception:
                    pass
            try:
                self._sidebar_ver_lbl.pack_forget()
            except Exception:
                pass
            # Arrow flips to Â»
            try:
                self._toggle_arrow.config(text="\u00bb")
            except Exception:
                pass
            # Snap width (no animation â€” called directly from resize handler)
            if not getattr(self, "_anim_job", None):
                try:
                    self.sidebar.configure(width=_W_COMPACT)
                    if getattr(self, "_body_frame", None) is not None:
                        self._body_frame.grid_columnconfigure(0, minsize=_W_COMPACT)
                except Exception:
                    pass

        else:
            # â”€â”€ Show: group headers, text labels, version â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
            # Re-pack headers before their first nav item (preserves visual order)
            for h in getattr(self, "_sidebar_headers", []):
                try:
                    first = getattr(h, "_first_nav", None)
                    if first is not None:
                        h.pack(fill="x", before=first)
                    else:
                        h.pack(fill="x")
                except Exception:
                    pass
                try:
                    if not h._pip.winfo_ismapped():
                        h._pip.pack(side="left", padx=(10, 6), pady=4)
                    if not h._lbl.winfo_ismapped():
                        h._lbl.pack(side="left")
                except Exception:
                    pass
            for b in getattr(self, "_sidebar_nav_buttons", []):
                try:
                    b._txt_lbl.pack(side="left", fill="x", expand=True, padx=(2, 8))
                except Exception:
                    pass
            try:
                self._sidebar_ver_lbl.pack(side="left", padx=(12, 0), pady=6)
            except Exception:
                pass
            # Arrow flips to Â«
            try:
                self._toggle_arrow.config(text="\u00ab")
            except Exception:
                pass
            # Snap width (no animation â€” called directly from resize handler)
            if not getattr(self, "_anim_job", None):
                try:
                    self.sidebar.configure(width=_W_EXPANDED)
                    if getattr(self, "_body_frame", None) is not None:
                        self._body_frame.grid_columnconfigure(0, minsize=_W_EXPANDED)
                except Exception:
                    pass

    def toggle_sidebar(self):
        """Operator toggle: instant expand/collapse sidebar."""
        target = not bool(getattr(self, "_sidebar_compact", False))
        self._set_sidebar_compact(target)
        self._animate_sidebar_width(compact=target)
        try:
            self._after_sidebar_layout_change()
            self._audit("SIDEBAR_TOGGLE", f"compact={1 if target else 0}")
        except Exception:
            pass

    def _after_sidebar_layout_change(self):
        """Post-toggle layout refresh without forcing chart redraws."""
        try:
            self.update_idletasks()
        except Exception:
            pass

    def _force_redraw_visible_charts(self):
        """Ask all visible TrendChartFrame widgets to redraw (no data work)."""
        def walk(w):
            try:
                kids = w.winfo_children()
            except Exception:
                kids = []
            for ch in kids:
                yield ch
                yield from walk(ch)

        for w in walk(self.pages):
            # TrendChartFrame exposes _request_draw(); safe to call if present.
            try:
                if hasattr(w, "can_draw_now") and callable(getattr(w, "can_draw_now")):
                    if not w.can_draw_now():
                        continue
                if hasattr(w, "_request_draw") and callable(getattr(w, "_request_draw")):
                    w._request_draw()
            except Exception:
                continue

    def _animate_sidebar_width(self, compact: bool):
        """Instant width update for responsive sidebar toggling."""
        _W_EXPANDED = getattr(self, "_sidebar_w_expanded", 210)
        _W_COMPACT  = getattr(self, "_sidebar_w_compact",  44)
        end = _W_COMPACT if compact else _W_EXPANDED
        try:
            if getattr(self, "_anim_job", None):
                self.after_cancel(self._anim_job)
                self._anim_job = None
        except Exception:
            pass
        try:
            self.sidebar.configure(width=end)
            if getattr(self, "_body_frame", None) is not None:
                self._body_frame.grid_columnconfigure(0, minsize=end)
        except Exception:
            pass

    def _on_window_configure(self, event=None):
        """No-op: sidebar is click-toggled only."""
        return

    def _sidebar_on_enter(self, _e=None):
        # Hover-expand removed â€” sidebar is click-toggled only (no flicker).
        pass

    def _sidebar_on_leave(self, _e=None):
        # Hover-collapse removed â€” sidebar is click-toggled only.
        pass

    def _sidebar_maybe_collapse(self):
        # Kept for compatibility â€” no-op.
        self._sidebar_leave_job = None
