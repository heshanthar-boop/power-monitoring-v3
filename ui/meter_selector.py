import time
import tkinter as tk
from tkinter import ttk

from config.store import save_config

# Stale timeout multiplier (data older than this × scan_interval = stale)
STALE_TIMEOUT_MULTIPLIER = 5


class MeterSelector(ttk.Frame):
    """Compact 'Display:' selector like the original (Total + M1..M16).

    SCADA rules:
    - selected_meters() must be SIDE-EFFECT FREE (no auto-ticking).
    - Persist selection immediately into cfg['display'] so totals/logging align.
    - Visual style uses existing ttk styles: MeterSel.Label.TLabel, MeterSel.On/Off/Stale.TCheckbutton.
    """

    def __init__(self, parent, cfg: dict, meters=None, max_meters: int = 16):
        super().__init__(parent)
        self.cfg = cfg
        self.meters = meters or []
        self.max_meters = int(max(1, max_meters))
        # hard cap to provided meters length (operator-proof)
        try:
            if self.meters:
                self.max_meters = min(self.max_meters, len(self.meters))
        except Exception:
            pass

        self._suspend_persist = False

        display_cfg = (cfg.get("display") or {})
        meters_cfg = (display_cfg.get("meters") or {})

        ttk.Label(self, text="Display:", style="MeterSel.Label.TLabel").pack(side="left", padx=(0, 6))

        # Total checkbox
        self.total_var = tk.BooleanVar(value=bool(display_cfg.get("show_total", True)))
        self.total_cb = ttk.Checkbutton(
            self,
            text="Total",
            variable=self.total_var,
            style="MeterSel.Off.TCheckbutton",
            command=self._on_change,
        )
        self.total_cb.pack(side="left", padx=(0, 6))

        # Meter checkboxes (M1..M16) – compact, no extra padding
        self.m_vars = {}
        self.m_checkbuttons = {}

        for i in range(1, self.max_meters + 1):
            v = tk.BooleanVar(value=bool(meters_cfg.get(str(i), i == 1)))
            self.m_vars[i] = v

            meter_label = f"M{i}"
            for m in self.meters:
                if getattr(m, "meter_id", None) == i and getattr(m, "name", None):
                    name = m.name
                    meter_label = name if len(name) <= 10 else name[:8] + ".."
                    break

            cb = ttk.Checkbutton(
                self,
                text=meter_label,
                variable=v,
                style="MeterSel.Off.TCheckbutton",
                command=self._on_change,
            )
            cb.pack(side="left", padx=(0, 4))
            self.m_checkbuttons[i] = cb

        # Ensure cfg is populated at startup
        self.persist_to_config()

    def _on_change(self):
        if self._suspend_persist:
            return
        try:
            self.persist_to_config()
            self._schedule_save()
        except Exception:
            # Never block UI
            return

    
    def _schedule_save(self):
        # Debounced disk write: never spam writes on checkbox clicks.
        try:
            if getattr(self, "_save_after_id", None):
                try:
                    self.after_cancel(self._save_after_id)
                except Exception:
                    pass
            self._save_after_id = self.after(500, self._save_now)
        except Exception:
            return

    def _save_now(self):
        try:
            self._save_after_id = None
            save_config(self.cfg)
        except Exception:
            # Never block UI
            return

    def refresh_labels(self):
        for i, cb in self.m_checkbuttons.items():
            meter_label = f"M{i}"
            for m in self.meters:
                if getattr(m, "meter_id", None) == i and getattr(m, "name", None):
                    name = m.name
                    meter_label = name if len(name) <= 10 else name[:8] + ".."
                    break
            cb.configure(text=meter_label)

    def update_comm_status(self):
        """Update styles based on per-meter comm freshness."""
        now = time.time()
        online_count = 0
        enabled_count = 0

        for i, cb in self.m_checkbuttons.items():
            # Find the meter object for this ID
            meter = None
            for m in self.meters:
                if getattr(m, "meter_id", None) == i:
                    meter = m
                    break

            # If no meter object or checkbox unchecked => OFF style
            if meter is None or not bool(self.m_vars[i].get()):
                cb.configure(style="MeterSel.Off.TCheckbutton")
                continue

            enabled_count += 1

            scan_interval = float(getattr(meter, "scan_interval_sec", 2.0) or 2.0)
            stale_timeout = max(5.0, scan_interval * STALE_TIMEOUT_MULTIPLIER)

            data_valid = bool(getattr(meter, "data_valid", True))
            last_good_ts = float(getattr(meter, "last_good_ts", 0.0) or 0.0)

            is_online = False
            if data_valid and last_good_ts > 0:
                age = now - last_good_ts
                if age <= stale_timeout:
                    is_online = True

            if is_online:
                cb.configure(style="MeterSel.On.TCheckbutton")
                online_count += 1
            else:
                cb.configure(style="MeterSel.Stale.TCheckbutton")

        # Total style reflects any selected meter online/stale
        if enabled_count == 0:
            self.total_cb.configure(style="MeterSel.Off.TCheckbutton")
        elif online_count > 0:
            self.total_cb.configure(style="MeterSel.On.TCheckbutton")
        else:
            self.total_cb.configure(style="MeterSel.Stale.TCheckbutton")

    def show_total(self) -> bool:
        return bool(self.total_var.get())

    def selected_meters(self):
        """Return selected meter IDs. SIDE-EFFECT FREE."""
        return [i for i, v in self.m_vars.items() if v.get()]

    def persist_to_config(self):
        d = self.cfg.setdefault("display", {})
        d["show_total"] = bool(self.total_var.get())
        # Persist only 1..max_meters to keep config clean
        d["meters"] = {str(i): bool(v.get()) for i, v in self.m_vars.items()}
