from __future__ import annotations

import time
import tkinter as tk
from tkinter import ttk
from typing import Callable, Optional

from ui.styles import get_theme
from ui.ux_labels import (
    plain_comm_state,
    plain_logging_state,
    plain_meter_quality,
    role_mode_hint,
    role_mode_name,
)


class OperatorHomeTab(ttk.Frame):
    """Role-aware first screen for operators, owners, and engineers."""

    def __init__(
        self,
        parent,
        cfg: dict,
        meters,
        *,
        worker=None,
        alarm_engine=None,
        logging_engine=None,
        data_store=None,
        snapshot_bus=None,
        session: Optional[dict] = None,
        on_open_page: Optional[Callable[[str], None]] = None,
        can_open: Optional[Callable[[str], bool]] = None,
    ):
        super().__init__(parent)
        self.cfg = cfg
        self.meters = list(meters or [])
        self.worker = worker
        self.alarm_engine = alarm_engine
        self.logging_engine = logging_engine
        self.data_store = data_store
        self.snapshot_bus = snapshot_bus
        self.session = dict(session or {})
        self.on_open_page = on_open_page
        self.can_open = can_open or (lambda _key: True)
        self._theme = get_theme()
        self._cards: dict[str, dict[str, object]] = {}
        self._quick_buttons: dict[str, ttk.Button] = {}

        self._build_ui()
        self.update_view()

    def set_worker(self, worker) -> None:
        self.worker = worker

    def _open_page(self, key: str) -> None:
        if callable(self.on_open_page) and self._can_open(key):
            self.on_open_page(key)

    def _can_open(self, key: str) -> bool:
        try:
            return bool(self.can_open(str(key or "")))
        except Exception:
            return False

    def _build_ui(self) -> None:
        t = self._theme
        root = tk.Frame(self, bg=t.bg)
        root.pack(fill="both", expand=True)

        hero = tk.Frame(root, bg="#0b1220", highlightthickness=1, highlightbackground=t.border)
        hero.pack(fill="x", padx=14, pady=(14, 10))
        tk.Frame(hero, bg=t.accent, height=3).pack(fill="x", side="top")

        hero_body = tk.Frame(hero, bg="#0b1220")
        hero_body.pack(fill="x", padx=16, pady=14)
        hero_body.grid_columnconfigure(0, weight=1)

        site = self.cfg.get("site", {}) or {}
        plant = str(site.get("plant_name", "") or "MFM384 Monitoring").strip()
        location = str(site.get("location", "") or "").strip()
        role = str(self.session.get("role", "") or "")
        master = bool(self.session.get("is_master_admin", False))

        title = role_mode_name(role, master=master)
        hint = role_mode_hint(role, master=master)
        location_text = f"{plant} - {location}" if location else plant

        tk.Label(
            hero_body,
            text=title,
            bg="#0b1220",
            fg=t.text,
            font=("Segoe UI", 18, "bold"),
        ).grid(row=0, column=0, sticky="w")
        tk.Label(
            hero_body,
            text=location_text,
            bg="#0b1220",
            fg=t.accent_lt,
            font=("Segoe UI", 10, "bold"),
        ).grid(row=1, column=0, sticky="w", pady=(3, 0))
        tk.Label(
            hero_body,
            text=hint,
            bg="#0b1220",
            fg=t.text_muted,
            font=("Segoe UI", 9),
        ).grid(row=2, column=0, sticky="w", pady=(5, 0))

        self._primary_action_var = tk.StringVar(value="Open Meter Status")
        self._primary_detail_var = tk.StringVar(value="")
        action_box = tk.Frame(hero_body, bg="#0b1220")
        action_box.grid(row=0, column=1, rowspan=3, sticky="e", padx=(18, 0))
        self._primary_btn = ttk.Button(
            action_box,
            textvariable=self._primary_action_var,
            style="Primary.TButton",
            command=self._run_primary_action,
        )
        self._primary_btn.pack(anchor="e")
        tk.Label(
            action_box,
            textvariable=self._primary_detail_var,
            bg="#0b1220",
            fg=t.text_dim,
            font=("Segoe UI", 8),
            justify="right",
        ).pack(anchor="e", pady=(5, 0))

        cards = tk.Frame(root, bg=t.bg)
        cards.pack(fill="x", padx=14, pady=(0, 10))
        cards.grid_columnconfigure(0, weight=1)
        cards.grid_columnconfigure(1, weight=1)
        cards.grid_columnconfigure(2, weight=1)
        cards.grid_columnconfigure(3, weight=1)

        self._add_card(cards, "comm", 0, 0, "Communication", t.accent)
        self._add_card(cards, "meters", 0, 1, "Meter Data", t.good)
        self._add_card(cards, "alarms", 0, 2, "Alarms", t.alarm)
        self._add_card(cards, "logging", 0, 3, "Historian", t.warn)

        quick = tk.Frame(root, bg=t.card, highlightthickness=1, highlightbackground=t.border)
        quick.pack(fill="x", padx=14, pady=(0, 12))
        tk.Frame(quick, bg=t.border, height=1).pack(fill="x", side="top")
        qbody = tk.Frame(quick, bg=t.card)
        qbody.pack(fill="x", padx=14, pady=12)
        tk.Label(
            qbody,
            text="Quick actions",
            bg=t.card,
            fg=t.text,
            font=("Segoe UI", 11, "bold"),
        ).pack(side="left", padx=(0, 16))

        actions = [
            ("dashboard", "Dashboard"),
            ("status", "Meter Status"),
            ("intelligence", "Intelligence"),
            ("alarms", "Alarms"),
            ("comm", "Fix Communication"),
            ("analytics", "Analytics"),
            ("logging", "Data Logger"),
        ]
        for key, label in actions:
            btn = ttk.Button(
                qbody,
                text=label,
                style="Small.TButton",
                command=lambda k=key: self._open_page(k),
            )
            btn.pack(side="left", padx=(0, 8))
            self._quick_buttons[key] = btn

        self._notice_var = tk.StringVar(value="")
        tk.Label(
            root,
            textvariable=self._notice_var,
            bg=t.bg,
            fg=t.text_muted,
            font=("Segoe UI", 9),
            anchor="w",
        ).pack(fill="x", padx=16, pady=(0, 6))

    def _add_card(self, parent, key: str, row: int, col: int, title: str, accent: str) -> None:
        t = self._theme
        card = tk.Frame(parent, bg=t.card, highlightthickness=1, highlightbackground=t.border)
        card.grid(row=row, column=col, sticky="nsew", padx=(0 if col == 0 else 8, 0), pady=0)
        tk.Frame(card, bg=accent, height=3).pack(fill="x", side="top")

        inner = tk.Frame(card, bg=t.card)
        inner.pack(fill="both", expand=True, padx=12, pady=10)

        tk.Label(
            inner,
            text=title.upper(),
            bg=t.card,
            fg=t.text_muted,
            font=("Segoe UI", 8, "bold"),
        ).pack(anchor="w")
        value_var = tk.StringVar(value="Unknown")
        detail_var = tk.StringVar(value="")
        tk.Label(
            inner,
            textvariable=value_var,
            bg=t.card,
            fg=t.text,
            font=("Segoe UI", 16, "bold"),
        ).pack(anchor="w", pady=(4, 0))
        tk.Label(
            inner,
            textvariable=detail_var,
            bg=t.card,
            fg=t.text_dim,
            font=("Segoe UI", 8),
            wraplength=260,
            justify="left",
        ).pack(anchor="w", pady=(4, 0))

        self._cards[key] = {
            "value": value_var,
            "detail": detail_var,
            "frame": card,
        }

    def _set_card(self, key: str, value: str, detail: str) -> None:
        card = self._cards.get(key) or {}
        value_var = card.get("value")
        detail_var = card.get("detail")
        try:
            value_var.set(value)
            detail_var.set(detail)
        except Exception:
            pass

    def _run_primary_action(self) -> None:
        target = getattr(self, "_primary_target", "status")
        self._open_page(target)

    def update_view(self) -> None:
        now = time.time()
        bus = {}
        if self.worker is not None and hasattr(self.worker, "get_bus_health"):
            try:
                bus = self.worker.get_bus_health() or {}
            except Exception:
                bus = {}

        comm_state = plain_comm_state((bus or {}).get("state", "OFFLINE"))
        age = (bus or {}).get("age")
        latency = (bus or {}).get("latency_ms")
        port_available = (bus or {}).get("port_available")
        comm_detail_parts = []
        if isinstance(age, (int, float)):
            comm_detail_parts.append(f"last data {float(age):.1f}s ago")
        if isinstance(latency, (int, float)):
            comm_detail_parts.append(f"avg latency {float(latency):.0f} ms")
        if port_available is False:
            comm_detail_parts.append("COM adapter missing")
        elif port_available is True:
            comm_detail_parts.append("COM adapter present")
        self._set_card("comm", comm_state, " | ".join(comm_detail_parts) or "No live communication detail yet")

        enabled = [m for m in self.meters if bool(getattr(m, "enabled", True))]
        live = 0
        old = 0
        no_data = 0
        disabled = max(0, len(self.meters) - len(enabled))
        for meter in enabled:
            label = plain_meter_quality(
                getattr(meter, "quality", ""),
                first_data_received=bool(getattr(meter, "first_data_received", False)),
            )
            if label == "Live" and bool(getattr(meter, "data_valid", False)):
                live += 1
            elif label == "Old Data":
                old += 1
            else:
                no_data += 1
        meter_value = f"{live}/{len(enabled)} Live" if enabled else "No Active Meters"
        meter_detail = f"Old data: {old} | No data: {no_data}"
        if disabled:
            meter_detail += f" | Disabled: {disabled}"
        self._set_card("meters", meter_value, meter_detail)

        active = 0
        unacked = 0
        try:
            active = len(getattr(self.alarm_engine, "active", {}) or {})
        except Exception:
            active = 0
        try:
            unacked = int(getattr(self.alarm_engine, "unacknowledged_count", 0) or 0)
        except Exception:
            unacked = 0
        alarm_value = "Clear" if active <= 0 else f"{active} Active"
        alarm_detail = "No active alarms" if active <= 0 else f"{unacked} pending acknowledgement"
        self._set_card("alarms", alarm_value, alarm_detail)

        log_state_raw = getattr(self.logging_engine, "state", "DISABLED") if self.logging_engine else "DISABLED"
        log_state = plain_logging_state(log_state_raw)
        log_detail = ""
        try:
            health = self.logging_engine.write_health() if self.logging_engine and hasattr(self.logging_engine, "write_health") else {}
            qd = int((health or {}).get("queue_depth", 0) or 0)
            dropped = int((health or {}).get("dropped_write_count", 0) or 0)
            log_detail = f"queue {qd} | dropped {dropped}"
        except Exception:
            log_detail = ""
        if not log_detail and self.logging_engine and hasattr(self.logging_engine, "get_status_summary"):
            try:
                log_detail = str(self.logging_engine.get_status_summary() or "")
            except Exception:
                log_detail = ""
        self._set_card("logging", log_state, log_detail or "Historian is not active")

        self._update_primary_action(comm_state, active, live, now)
        self._update_quick_buttons()

    def _update_primary_action(self, comm_state: str, active_alarms: int, live_meters: int, now: float) -> None:
        if comm_state in {"No Data", "Reconnecting"} and self._can_open("comm"):
            self._primary_target = "comm"
            self._primary_action_var.set("Fix Communication")
            self._primary_detail_var.set("Open Communication setup and auto-fix tools")
        elif active_alarms > 0 and self._can_open("alarms"):
            self._primary_target = "alarms"
            self._primary_action_var.set("Review Alarms")
            self._primary_detail_var.set("Acknowledge and investigate active alarms")
        elif live_meters <= 0 and self._can_open("status"):
            self._primary_target = "status"
            self._primary_action_var.set("Open Meter Status")
            self._primary_detail_var.set("Check each meter data state")
        elif self._can_open("dashboard"):
            self._primary_target = "dashboard"
            self._primary_action_var.set("Open Dashboard")
            self._primary_detail_var.set(time.strftime("Updated %H:%M:%S", time.localtime(now)))
        else:
            self._primary_target = "status"
            self._primary_action_var.set("Open Status")
            self._primary_detail_var.set("Limited by current role permissions")

    def _update_quick_buttons(self) -> None:
        hidden = []
        for key, btn in self._quick_buttons.items():
            allowed = self._can_open(key)
            try:
                btn.configure(state="normal" if allowed else "disabled")
            except Exception:
                pass
            if not allowed:
                hidden.append(key)
        if hidden:
            self._notice_var.set("Disabled buttons are not available for this login role.")
        else:
            self._notice_var.set("Use this page first during shift handover and fault recovery.")
