"""Bottom status bar showing COM port, connection state, logging status, and system info."""
from __future__ import annotations
import tkinter as tk
from tkinter import ttk
import time

from ui.styles import get_theme
from ui.ux_labels import plain_comm_state, plain_logging_state


class StatusBar(ttk.Frame):
    """Always-visible status bar at bottom of main window."""

    def __init__(self, parent):
        super().__init__(parent, style="HealthBar.TFrame")
        self._theme = get_theme()

        # Text variables
        self.com_var = tk.StringVar(value="COM: not selected")
        self.conn_var = tk.StringVar(value="OFFLINE")
        self.update_var = tk.StringVar(value="Last update: —")
        self.meters_var = tk.StringVar(value="Active: 0")
        self.priority_var = tk.StringVar(value="Priority: —")
        self.logging_var = tk.StringVar(value="LOG: OFF")
        self.mode_var = tk.StringVar(value="MODE: RUN")
        self.alarm_var = tk.StringVar(value="")
        self.summary_var = tk.StringVar(value="System idle")

        self._build()

    def _badge(self, parent, textvariable):
        lbl = tk.Label(
            parent,
            textvariable=textvariable,
            font=("Segoe UI", 9, "bold"),
            fg=self._theme.text,
            bg=self._theme.surface,
            padx=10,
            pady=4,
            relief="flat",
            bd=0,
        )
        return lbl

    def _build(self):
        sep = ttk.Separator(self, orient="horizontal")
        sep.pack(side="top", fill="x")

        container = ttk.Frame(self, style="HealthBar.TFrame")
        container.pack(fill="x", padx=10, pady=5)

        self.com_label = ttk.Label(container, textvariable=self.com_var, style='HealthInfo.TLabel')
        self.com_label.pack(side="left", padx=(0, 12))

        self.conn_label = self._badge(container, self.conn_var)
        self.conn_label.pack(side="left", padx=(0, 10))

        self.logging_label = self._badge(container, self.logging_var)
        self.logging_label.pack(side="left", padx=(0, 10))

        self.mode_label = self._badge(container, self.mode_var)
        self.mode_label.pack(side="left", padx=(0, 12))

        # Alarm badge — hidden when no unacknowledged alarms
        self.alarm_label = self._badge(container, self.alarm_var)
        self.alarm_label.pack(side="left", padx=(0, 10))
        self.alarm_label.pack_forget()  # hidden by default

        self.update_label = ttk.Label(container, textvariable=self.update_var, style='HealthInfo.TLabel')
        self.update_label.pack(side="left", padx=(0, 12))

        self.meters_label = ttk.Label(container, textvariable=self.meters_var, style='HealthInfo.TLabel')
        self.meters_label.pack(side="left", padx=(0, 12))

        self.priority_label = ttk.Label(container, textvariable=self.priority_var, style='HealthInfo.TLabel')
        self.priority_label.pack(side="left", padx=(0, 12))

        ttk.Label(container, textvariable=self.summary_var, style='HealthTitle.TLabel').pack(side="right")

    def _set_badge_color(self, label, state: str):
        state = str(state or '').upper()
        fg = self._theme.text
        bg = self._theme.surface
        if any(k in state for k in ('ONLINE', 'RUNNING', 'GOOD', 'OK', 'LIVE', 'RECORDING')):
            bg = self._theme.good
            fg = '#ffffff'
        elif any(k in state for k in ('PAUSED', 'SCHEDULED OFF', 'STALE', 'MIXED', 'RECOVERING', 'RECONNECTING', 'OLD DATA')):
            bg = self._theme.warn
            fg = '#ffffff'
        elif any(k in state for k in ('OFF', 'OFFLINE', 'FAULT', 'ERROR', 'INVALID', 'NO DATA')):
            bg = self._theme.alarm if ('FAULT' in state or 'ERROR' in state or 'INVALID' in state) else self._theme.stale
            fg = '#ffffff'
        elif 'WRITE' in state:
            bg = self._theme.accent
            fg = '#ffffff'
        label.config(bg=bg, fg=fg)

    def update_alarm_badge(self, unack_count: int) -> None:
        """Show/hide the unacknowledged alarm badge. Call from UI thread only."""
        try:
            if unack_count > 0:
                label = f"⚠ {unack_count} UNACK" if unack_count < 100 else "⚠ 99+ UNACK"
                self.alarm_var.set(label)
                self.alarm_label.config(bg=self._theme.alarm, fg="#ffffff")
                # Show if hidden
                try:
                    self.alarm_label.pack_info()
                except tk.TclError:
                    # Not packed — insert before update_label
                    self.alarm_label.pack(side="left", padx=(0, 10),
                                          before=self.update_label)
            else:
                self.alarm_var.set("")
                try:
                    self.alarm_label.pack_forget()
                except Exception:
                    pass
        except Exception:
            pass

    def update_status(self, cfg: dict, meters: list, worker=None, stale_sec: float = 10.0, logging_engine=None, health: dict | None = None, alarm_engine=None):
        serial_cfg = cfg.get("serial", {})
        port = serial_cfg.get("port", "")
        if port and port != "None":
            baud = serial_cfg.get("baud", 9600)
            parity = serial_cfg.get("parity", "N")
            stopbits = serial_cfg.get("stopbits", 1)
            self.com_var.set(f"COM: {port} @ {baud},{parity},{stopbits}")
        else:
            self.com_var.set("COM: not selected")

        now = time.time()
        enabled_meters = [m for m in (meters or []) if getattr(m, "enabled", False)]
        state = 'OFFLINE'
        age_text = 'Last update: —'
        if worker:
            bus = worker.get_bus_health()
            state_key = (bus or {}).get('state', 'OFFLINE')
            age = (bus or {}).get('age', None)
            state = plain_comm_state(state_key)
            if str(state_key).upper() == 'RECOVERING':
                nxt = float((bus or {}).get('next_in', 0.0) or 0.0)
                attempt = int((bus or {}).get('attempts', 0) or 0)
                state = f'Reconnecting #{attempt} {nxt:.0f}s'
            if age is not None:
                age_text = f"Last update: {age:.1f} s ago"

        self.conn_var.set(state)
        self.update_var.set(age_text)
        self._set_badge_color(self.conn_label, state)

        active_count = len(enabled_meters)
        data_state = 'OFFLINE'
        if worker is not None and enabled_meters:
            bus = None
            try:
                bus = worker.get_bus_health()
            except Exception:
                bus = None
            bus_state = (bus or {}).get('state', 'OFFLINE')
            if bus_state != 'OFFLINE':
                data_state = 'Live'
                for m in enabled_meters:
                    last_rx = 0.0
                    try:
                        last_rx = float(worker.get_meter_last_rx_ts(getattr(m, 'meter_id', 0)) or 0.0)
                    except Exception:
                        last_rx = 0.0
                    if not last_rx or (now - last_rx) > float(stale_sec):
                        data_state = 'Old Data'
                        break
        self.meters_var.set(f"Active: {active_count} | Data: {plain_comm_state(data_state)}")

        priority_meters = [m for m in (meters or []) if getattr(m, 'priority', False)]
        self.priority_var.set(f"Priority: Meter {priority_meters[0].meter_id}" if priority_meters else "Priority: —")

        log_state = 'OFF'
        if logging_engine:
            summary = logging_engine.get_status_summary()
            raw = str(getattr(logging_engine, 'state', 'DISABLED') or 'DISABLED').upper()
            if raw == 'RUNNING':
                log_state = plain_logging_state(raw)
            elif raw == 'SCHEDULED_OFF':
                log_state = plain_logging_state(raw)
            elif raw == 'ERROR':
                log_state = plain_logging_state(raw)
            self.summary_var.set(summary)
        else:
            self.summary_var.set('Logging disabled')
        self.logging_var.set(f"LOG: {log_state}")
        self._set_badge_color(self.logging_label, log_state)

        mode = 'RUN'
        if health and health.get('mode'):
            mode = str(health['mode'])
        self.mode_var.set(f"MODE: {mode}")
        self._set_badge_color(self.mode_label, mode)

        # Alarm badge — shows unacknowledged count when > 0
        try:
            if alarm_engine is not None:
                unack = int(getattr(alarm_engine, "unacknowledged_count", 0) or 0)
                self.update_alarm_badge(unack)
        except Exception:
            pass

    def set_mode(self, mode: str):
        try:
            m = (mode or "RUN").strip()
            self.mode_var.set(f"MODE: {m}")
            self._set_badge_color(self.mode_label, m)
        except Exception:
            pass
