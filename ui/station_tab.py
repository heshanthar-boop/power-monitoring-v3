# ui/station_tab.py
from __future__ import annotations

import tkinter as tk
from tkinter import ttk

from utils.monitors import list_monitors
from utils.power import is_keep_awake


class StationTab(ttk.Frame):
    """
    Operator Station tab:
    - Multi-monitor deployment: open extra windows (Dashboard / Protection / Alarms) on selected monitors.
    - Keep-awake toggle.
    """

    def __init__(self, parent, cfg: dict, root_tk, on_apply, on_keep_awake):
        super().__init__(parent, padding=10)
        self.cfg = cfg
        self.root_tk = root_tk
        self.on_apply = on_apply
        self.on_keep_awake = on_keep_awake

        self._mons = list_monitors(root_tk=self.root_tk)
        self._mon_labels = [self._fmt_mon(m) for m in self._mons]

        self._build_ui()
        self._load_from_cfg()

    def _fmt_mon(self, m: dict) -> str:
        p = " (Primary)" if m.get("primary") else ""
        return f"Monitor {m.get('index')} — {m.get('width')}x{m.get('height')}{p}"

    def _load_from_cfg(self):
        ui = (self.cfg.get("ui") or {})
        ext = (ui.get("external_views") or {})

        self.keep_awake_var.set(bool(ui.get("keep_awake", False)))

        def load_view(name, enable_var, mon_var):
            v = (ext.get(name) or {})
            enable_var.set(bool(v.get("enabled", False)))
            mon = int(v.get("monitor", 1) or 1)
            mon = max(1, min(mon, len(self._mons)))
            mon_var.set(self._mon_labels[mon - 1] if self._mon_labels else "Monitor 1")

        load_view("dashboard", self.ext_dash_var, self.dash_mon_var)
        load_view("protection", self.ext_prot_var, self.prot_mon_var)
        load_view("alarms", self.ext_alarm_var, self.alarm_mon_var)

    def _build_ui(self):
        self.columnconfigure(0, weight=1)

        title = ttk.Label(self, text="Operator Station / Multi-Monitor", style="Header.TLabel")
        title.grid(row=0, column=0, sticky="w", pady=(0, 8))

        # --- Keep awake ---
        power_box = ttk.Labelframe(self, text="PC Power")
        power_box.grid(row=1, column=0, sticky="ew", pady=6)
        power_box.columnconfigure(0, weight=1)

        self.keep_awake_var = tk.BooleanVar(value=is_keep_awake())
        chk = ttk.Checkbutton(
            power_box,
            text="Keep this PC awake (prevent sleep / screen-off while SCADA is running)",
            variable=self.keep_awake_var,
            command=self._on_keep_awake_clicked,
        )
        chk.grid(row=0, column=0, sticky="w", padx=8, pady=6)

        hint = ttk.Label(power_box, text="Recommended for control rooms. You can disable anytime.", style="Muted.TLabel")
        hint.grid(row=1, column=0, sticky="w", padx=8, pady=(0, 6))

        # --- External views ---
        box = ttk.Labelframe(self, text="External Windows (Second Monitor)")
        box.grid(row=2, column=0, sticky="ew", pady=6)
        for i in range(4):
            box.columnconfigure(i, weight=1)

        ttk.Label(box, text="Enable").grid(row=0, column=0, sticky="w", padx=8)
        ttk.Label(box, text="View").grid(row=0, column=1, sticky="w")
        ttk.Label(box, text="Monitor").grid(row=0, column=2, sticky="w")
        ttk.Label(box, text="Action").grid(row=0, column=3, sticky="w", padx=8)

        self.ext_dash_var = tk.BooleanVar(value=False)
        self.ext_prot_var = tk.BooleanVar(value=False)
        self.ext_alarm_var = tk.BooleanVar(value=False)

        self.dash_mon_var = tk.StringVar(value=self._mon_labels[0] if self._mon_labels else "Monitor 1")
        self.prot_mon_var = tk.StringVar(value=self._mon_labels[0] if self._mon_labels else "Monitor 1")
        self.alarm_mon_var = tk.StringVar(value=self._mon_labels[0] if self._mon_labels else "Monitor 1")

        self._row_view(box, 1, "Dashboard", self.ext_dash_var, self.dash_mon_var)
        self._row_view(box, 2, "Protection", self.ext_prot_var, self.prot_mon_var)
        self._row_view(box, 3, "Alarms", self.ext_alarm_var, self.alarm_mon_var)

        btns = ttk.Frame(self)
        btns.grid(row=3, column=0, sticky="ew", pady=(10, 0))
        btns.columnconfigure(0, weight=1)

        self.btn_apply = ttk.Button(btns, text="Apply / Open Windows", command=self._apply)
        self.btn_apply.grid(row=0, column=0, sticky="e")

        note = ttk.Label(
            self,
            text="Tip: Use Dashboard on Monitor 2, and keep Meter Status on Monitor 1.\n"
                 "If a monitor is disconnected, windows will fall back to primary.",
            style="Muted.TLabel",
        )
        note.grid(row=4, column=0, sticky="w", pady=(8, 0))

    def _row_view(self, parent, row, label, enable_var, mon_var):
        chk = ttk.Checkbutton(parent, variable=enable_var)
        chk.grid(row=row, column=0, sticky="w", padx=8, pady=4)

        ttk.Label(parent, text=label).grid(row=row, column=1, sticky="w")

        cb = ttk.Combobox(parent, textvariable=mon_var, values=self._mon_labels, state="readonly", width=34)
        cb.grid(row=row, column=2, sticky="w", padx=(0, 8))

        ttk.Button(parent, text="Focus", command=lambda l=label: self._focus(l)).grid(row=row, column=3, sticky="w", padx=8)

    def _focus(self, label):
        # focuses an already-open window if present
        try:
            self.on_apply({"action": "focus", "view": label.lower()})
        except Exception:
            pass

    def _on_keep_awake_clicked(self):
        try:
            self.on_keep_awake(bool(self.keep_awake_var.get()))
        except Exception:
            pass
        ui = (self.cfg.get("ui") or {})
        ui["keep_awake"] = bool(self.keep_awake_var.get())
        self.cfg["ui"] = ui

    def _apply(self):
        def mon_index(var):
            s = str(var.get() or "")
            for i, lab in enumerate(self._mon_labels, start=1):
                if lab == s:
                    return i
            return 1

        payload = {
            "action": "apply",
            "keep_awake": bool(self.keep_awake_var.get()),
            "views": {
                "dashboard": {"enabled": bool(self.ext_dash_var.get()), "monitor": mon_index(self.dash_mon_var)},
                "protection": {"enabled": bool(self.ext_prot_var.get()), "monitor": mon_index(self.prot_mon_var)},
                "alarms": {"enabled": bool(self.ext_alarm_var.get()), "monitor": mon_index(self.alarm_mon_var)},
            }
        }
        self.on_apply(payload)
