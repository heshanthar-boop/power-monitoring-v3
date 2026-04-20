"""ui.setup_wizard

First-run setup wizard.  Runs as a standalone tk.Tk window (its own mainloop)
before MainWindow is created.  Returns an updated config dict on completion,
or the original config unchanged if the user skips.

Steps
-----
1. Site — plant name + plant type
2. COM Port — port selection + baud rate
3. Meters — count, slave IDs, names
4. Done — summary + finish
"""
from __future__ import annotations

import tkinter as tk
from tkinter import ttk
from typing import Any, Dict, List, Optional

try:
    import sv_ttk as _sv_ttk
except ImportError:
    _sv_ttk = None

try:
    from serial.tools.list_ports import comports as _comports
except ImportError:
    _comports = None

from utils.logger import setup_logger

logger = setup_logger("wizard")

# Plant type choices shown in Step 1
MAX_WIZARD_METERS = 32

PLANT_TYPES = [
    ("solar_ground",   "Solar — Ground Mount"),
    ("solar_net",      "Solar — Net Accounting (import + export meters)"),
    ("hydro",          "Hydro Plant"),
    ("industrial",     "Industrial Load Monitoring"),
    ("general",        "General / Other"),
]

BAUD_RATES = [1200, 2400, 4800, 9600, 19200, 38400, 57600, 115200]

STEP_TITLES = [
    "Step 1 of 4 — Site Information",
    "Step 2 of 4 — Serial Port",
    "Step 3 of 4 — Meters",
    "Step 4 of 4 — Ready",
]

_W = 520
_H = 480


class SetupWizard(tk.Tk):
    """First-run wizard.  Call .run() — blocks until done, returns cfg."""

    def __init__(self, cfg: Dict[str, Any]):
        super().__init__()
        self.cfg = {k: v for k, v in cfg.items()}   # shallow copy
        self.result: Optional[Dict[str, Any]] = None
        self._step = 0

        # ------------------------------------------------------------------
        # Window chrome
        # ------------------------------------------------------------------
        self.title("Power Monitor — First-Time Setup")
        self.geometry(f"{_W}x{_H}")
        self.resizable(False, False)
        self.protocol("WM_DELETE_WINDOW", self._on_skip)

        if _sv_ttk:
            try:
                _sv_ttk.set_theme("dark", root=self)
            except Exception:
                pass

        # Center on screen
        self.update_idletasks()
        sw = self.winfo_screenwidth()
        sh = self.winfo_screenheight()
        x = (sw - _W) // 2
        y = (sh - _H) // 2
        self.geometry(f"{_W}x{_H}+{x}+{y}")

        # ------------------------------------------------------------------
        # Persistent working state (populated per step)
        # ------------------------------------------------------------------
        self._plant_name_var = tk.StringVar(
            value=(cfg.get("site", {}) or {}).get("plant_name", "") or ""
        )
        self._plant_type_var = tk.StringVar(
            value=(cfg.get("site", {}) or {}).get("plant_type", "solar_ground")
        )
        self._port_var      = tk.StringVar(value=(cfg.get("serial", {}) or {}).get("port", ""))
        self._baud_var      = tk.StringVar(value=str((cfg.get("serial", {}) or {}).get("baud", 9600)))
        self._meter_count_var = tk.IntVar(value=max(1, len([m for m in cfg.get("meters", []) if (cfg.get("meters") or [])])))
        # Per-meter name + slave-id vars — built when step 3 renders
        self._meter_rows: List[Dict[str, tk.Variable]] = []
        self._available_ports: List[str] = []

        # ------------------------------------------------------------------
        # Layout skeleton
        # ------------------------------------------------------------------
        self._hdr_frame    = ttk.Frame(self)
        self._content_frame = ttk.Frame(self)
        self._nav_frame    = ttk.Frame(self)

        self._hdr_frame.pack(fill="x", padx=0, pady=0)
        self._content_frame.pack(fill="both", expand=True, padx=16, pady=8)
        self._nav_frame.pack(fill="x", padx=16, pady=(0, 12))

        self._build_header()
        self._build_nav()
        self._show_step(0)

    # ------------------------------------------------------------------ run
    def run(self) -> Optional[Dict[str, Any]]:
        """Block until wizard finishes or is dismissed.  Returns updated cfg."""
        self.mainloop()
        return self.result

    # ----------------------------------------------------------- header bar
    def _build_header(self):
        for w in self._hdr_frame.winfo_children():
            w.destroy()

        # Accent bar at the very top
        bar = tk.Frame(self._hdr_frame, height=4, bg="#4ea1ff")
        bar.pack(fill="x")

        self._step_label = ttk.Label(
            self._hdr_frame,
            text=STEP_TITLES[self._step],
            font=("Segoe UI", 12, "bold"),
        )
        self._step_label.pack(anchor="w", padx=16, pady=(10, 2))

        sep = ttk.Separator(self._hdr_frame, orient="horizontal")
        sep.pack(fill="x", padx=0, pady=(6, 0))

    def _refresh_header(self):
        self._step_label.configure(text=STEP_TITLES[self._step])

    # ------------------------------------------------------------- nav bar
    def _build_nav(self):
        self._skip_btn = ttk.Button(
            self._nav_frame, text="Skip Setup", command=self._on_skip, width=12
        )
        self._skip_btn.pack(side="left")

        self._next_btn = ttk.Button(
            self._nav_frame, text="Next →", style="Primary.TButton",
            command=self._on_next, width=12
        )
        self._next_btn.pack(side="right")

        self._back_btn = ttk.Button(
            self._nav_frame, text="← Back", command=self._on_back, width=12
        )
        self._back_btn.pack(side="right", padx=(0, 6))

    # ----------------------------------------------------------- step router
    def _show_step(self, n: int):
        self._step = n
        self._refresh_header()

        for w in self._content_frame.winfo_children():
            w.destroy()

        # Update nav buttons
        self._back_btn.configure(state="normal" if n > 0 else "disabled")
        if n == len(STEP_TITLES) - 1:
            self._next_btn.configure(text="Finish ✓", style="Primary.TButton")
        else:
            self._next_btn.configure(text="Next →", style="Primary.TButton")

        builders = [
            self._step1_site,
            self._step2_comm,
            self._step3_meters,
            self._step4_done,
        ]
        builders[n]()

    # ================================================================ STEP 1
    def _step1_site(self):
        f = self._content_frame

        ttk.Label(f, text="Enter your site / plant name:", font=("Segoe UI", 10)).pack(
            anchor="w", pady=(12, 4)
        )
        name_ent = ttk.Entry(f, textvariable=self._plant_name_var, width=40, font=("Segoe UI", 10))
        name_ent.pack(anchor="w", pady=(0, 16))
        name_ent.focus_set()

        ttk.Label(f, text="Select plant type:", font=("Segoe UI", 10)).pack(anchor="w", pady=(0, 6))

        rb_frame = ttk.Frame(f)
        rb_frame.pack(anchor="w", padx=8)
        for val, label in PLANT_TYPES:
            ttk.Radiobutton(
                rb_frame,
                text=label,
                variable=self._plant_type_var,
                value=val,
            ).pack(anchor="w", pady=3)

        ttk.Label(
            f,
            text="You can change these later in the Communication tab.",
            foreground="#9ca3af",
            font=("Segoe UI", 9),
        ).pack(anchor="w", pady=(16, 0))

    # ================================================================ STEP 2
    def _step2_comm(self):
        f = self._content_frame

        # -- Port selection --
        port_row = ttk.Frame(f)
        port_row.pack(fill="x", pady=(12, 0))

        ttk.Label(port_row, text="COM Port:", width=14, anchor="w").pack(side="left")
        self._port_combo = ttk.Combobox(
            port_row, textvariable=self._port_var, width=18, state="normal"
        )
        self._port_combo.pack(side="left", padx=(0, 8))
        ttk.Button(port_row, text="Scan", command=self._scan_ports, width=8).pack(side="left")

        # -- Baud rate --
        baud_row = ttk.Frame(f)
        baud_row.pack(fill="x", pady=(10, 0))
        ttk.Label(baud_row, text="Baud Rate:", width=14, anchor="w").pack(side="left")
        baud_cb = ttk.Combobox(
            baud_row,
            textvariable=self._baud_var,
            values=[str(b) for b in BAUD_RATES],
            state="readonly",
            width=12,
        )
        baud_cb.pack(side="left")

        # -- Info box --
        info = ttk.LabelFrame(f, text="Wiring guide", padding=8)
        info.pack(fill="x", pady=(20, 0))
        ttk.Label(
            info,
            text=(
                "• Connect RS485 (A/B/GND) from the MFM384 to your USB-RS485 adapter.\n"
                "• Default baud rate for MFM384 is 9600.\n"
                "• If no ports appear, install the adapter driver and re-scan."
            ),
            justify="left",
            font=("Segoe UI", 9),
            foreground="#9ca3af",
            wraplength=_W - 60,
        ).pack(anchor="w")

        # -- Skip note --
        ttk.Label(
            f,
            text="You can skip this and configure the port later in the Communication tab.",
            foreground="#9ca3af",
            font=("Segoe UI", 9),
            wraplength=_W - 60,
        ).pack(anchor="w", pady=(12, 0))

        # Auto-scan on first visit
        if not self._available_ports:
            self.after(100, self._scan_ports_silent)

    def _scan_ports(self):
        """Scan COM ports and populate the combobox; show a message if none found."""
        ports = self._get_ports()
        if not ports:
            tk.messagebox.showwarning(
                "No Ports Found",
                "No COM ports detected.\n\nMake sure your USB-RS485 adapter is connected "
                "and its driver is installed.",
                parent=self,
            )
            return
        self._port_combo["values"] = ports
        if not self._port_var.get() or self._port_var.get() not in ports:
            self._port_var.set(ports[0])
        logger.info("[WIZARD] scanned ports: %s", ports)

    def _scan_ports_silent(self):
        """Scan without showing a dialog — called on step entry."""
        ports = self._get_ports()
        if ports:
            self._port_combo["values"] = ports
            if not self._port_var.get():
                self._port_var.set(ports[0])

    def _get_ports(self) -> List[str]:
        self._available_ports = []
        if _comports is None:
            return []
        try:
            entries = list(_comports())
            self._available_ports = [p.device for p in entries]
            return self._available_ports
        except Exception:
            logger.warning("[WIZARD] port scan failed", exc_info=True)
            return []

    # ================================================================ STEP 3
    def _step3_meters(self):
        f = self._content_frame

        count_row = ttk.Frame(f)
        count_row.pack(fill="x", pady=(12, 0))
        ttk.Label(count_row, text=f"Number of meters (1-{MAX_WIZARD_METERS}):", anchor="w").pack(side="left", padx=(0, 12))
        count_spin = ttk.Spinbox(
            count_row,
            from_=1, to=MAX_WIZARD_METERS,
            textvariable=self._meter_count_var,
            width=4,
            command=self._rebuild_meter_rows,
        )
        count_spin.pack(side="left")
        count_spin.bind("<FocusOut>", lambda _: self._rebuild_meter_rows())
        count_spin.bind("<Return>",   lambda _: self._rebuild_meter_rows())

        ttk.Separator(f, orient="horizontal").pack(fill="x", pady=(10, 6))

        # Column headers
        hdr = ttk.Frame(f)
        hdr.pack(fill="x", padx=4)
        ttk.Label(hdr, text="#",        width=3,  anchor="center", font=("Segoe UI", 9, "bold")).pack(side="left")
        ttk.Label(hdr, text="Name",     width=20, anchor="w",      font=("Segoe UI", 9, "bold")).pack(side="left", padx=(4, 0))
        ttk.Label(hdr, text="Slave ID", width=9,  anchor="w",      font=("Segoe UI", 9, "bold")).pack(side="left", padx=(4, 0))
        ttk.Label(hdr, text="Sign",     width=9,  anchor="w",      font=("Segoe UI", 9, "bold")).pack(side="left", padx=(4, 0))

        # Scrollable rows container
        self._meter_rows_frame = ttk.Frame(f)
        self._meter_rows_frame.pack(fill="x", padx=4, pady=(4, 0))

        ttk.Label(
            f,
            text='Sign: "+" adds to TOTAL, "−" subtracts (use for export/load meters).',
            foreground="#9ca3af",
            font=("Segoe UI", 9),
        ).pack(anchor="w", pady=(10, 0))

        self._rebuild_meter_rows()

    def _rebuild_meter_rows(self):
        """Rebuild the per-meter name/ID rows based on current count."""
        try:
            n = max(1, min(MAX_WIZARD_METERS, int(self._meter_count_var.get())))
        except Exception:
            n = 1
        self._meter_count_var.set(n)

        # Preserve existing entries
        old_rows = list(self._meter_rows)
        self._meter_rows = []

        for w in self._meter_rows_frame.winfo_children():
            w.destroy()

        existing_meters = self.cfg.get("meters") or []

        for i in range(n):
            row = ttk.Frame(self._meter_rows_frame)
            row.pack(fill="x", pady=2)

            # Seed from existing config or sensible defaults
            if i < len(old_rows):
                name_v = old_rows[i]["name"]
                sid_v  = old_rows[i]["slave_id"]
                sign_v = old_rows[i]["sign"]
            elif i < len(existing_meters):
                m = existing_meters[i]
                name_v = tk.StringVar(value=str(m.get("name", f"Meter {i+1}")))
                sid_v  = tk.StringVar(value=str(m.get("slave_id", i + 1)))
                sign   = m.get("total_sign", 1.0)
                sign_v = tk.StringVar(value="+" if float(sign) >= 0 else "−")
            else:
                name_v = tk.StringVar(value=f"Meter {i+1}")
                sid_v  = tk.StringVar(value=str(i + 1))
                sign_v = tk.StringVar(value="+")

            ttk.Label(row, text=str(i + 1), width=3, anchor="center").pack(side="left")
            ttk.Entry(row, textvariable=name_v, width=20).pack(side="left", padx=(4, 0))
            ttk.Spinbox(row, textvariable=sid_v, from_=1, to=247, width=7).pack(side="left", padx=(4, 0))
            ttk.Combobox(
                row,
                textvariable=sign_v,
                values=["+", "−"],
                state="readonly",
                width=4,
            ).pack(side="left", padx=(4, 0))

            self._meter_rows.append({"name": name_v, "slave_id": sid_v, "sign": sign_v})

    # ================================================================ STEP 4
    def _step4_done(self):
        f = self._content_frame

        ttk.Label(f, text="Setup complete!", font=("Segoe UI", 14, "bold")).pack(pady=(20, 4))
        ttk.Label(
            f,
            text="Here is what was configured:",
            font=("Segoe UI", 10),
            foreground="#9ca3af",
        ).pack(pady=(0, 12))

        summary = ttk.LabelFrame(f, text="Summary", padding=10)
        summary.pack(fill="x", padx=4)

        site = self.cfg.get("site", {}) or {}
        serial = self.cfg.get("serial", {}) or {}
        meters = self.cfg.get("meters") or []

        rows = [
            ("Plant name",   site.get("plant_name", "—")),
            ("Plant type",   self._friendly_plant_type(site.get("plant_type", ""))),
            ("COM port",     serial.get("port", "—") or "—"),
            ("Baud rate",    str(serial.get("baud", 9600))),
            ("Meters",       str(len(meters))),
        ]
        for label, value in rows:
            r = ttk.Frame(summary)
            r.pack(fill="x", pady=1)
            ttk.Label(r, text=label + ":", width=14, anchor="w", foreground="#9ca3af").pack(side="left")
            ttk.Label(r, text=value, anchor="w").pack(side="left")

        if meters:
            ttk.Label(summary, text="", font=("Segoe UI", 1)).pack()
            for m in meters:
                r = ttk.Frame(summary)
                r.pack(fill="x")
                sign_str = "+" if float(m.get("total_sign", 1.0)) >= 0 else "−"
                ttk.Label(
                    r,
                    text=f"  • {m.get('name','?')}  (slave {m.get('slave_id','?')}, sign {sign_str})",
                    font=("Segoe UI", 9),
                ).pack(anchor="w")

        ttk.Label(
            f,
            text=(
                "Click Finish to open the main dashboard.\n"
                "You can refine all settings in the Communication and Setup tabs at any time."
            ),
            justify="center",
            foreground="#9ca3af",
            font=("Segoe UI", 9),
            wraplength=_W - 60,
        ).pack(pady=(18, 0))

    def _friendly_plant_type(self, key: str) -> str:
        for val, label in PLANT_TYPES:
            if val == key:
                return label
        return key or "—"

    # ========================================================= navigation
    def _on_next(self):
        if self._step == len(STEP_TITLES) - 1:
            self._on_finish()
            return
        if not self._validate_step(self._step):
            return
        self._apply_step(self._step)
        self._show_step(self._step + 1)

    def _on_back(self):
        if self._step > 0:
            self._show_step(self._step - 1)

    def _on_skip(self):
        """User dismissed wizard — mark complete so it never re-opens."""
        self.cfg["setup_complete"] = True
        self.result = self.cfg
        self.destroy()

    def _on_finish(self):
        self._apply_step(self._step)
        self.cfg["setup_complete"] = True
        self.result = self.cfg
        logger.info("[WIZARD] setup complete: site=%s type=%s port=%s meters=%d",
                    self.cfg.get("site", {}).get("plant_name"),
                    self.cfg.get("site", {}).get("plant_type"),
                    self.cfg.get("serial", {}).get("port"),
                    len(self.cfg.get("meters") or []))
        self.destroy()

    # ======================================================= validate + apply
    def _validate_step(self, step: int) -> bool:
        if step == 0:
            name = self._plant_name_var.get().strip()
            if not name:
                tk.messagebox.showwarning(
                    "Plant Name Required",
                    "Please enter a plant name to continue.",
                    parent=self,
                )
                return False
        return True

    def _apply_step(self, step: int):
        """Write wizard state into self.cfg."""
        if step == 0:
            site = self.cfg.setdefault("site", {})
            site["plant_name"] = self._plant_name_var.get().strip() or "Power Monitor"
            site["plant_type"] = self._plant_type_var.get()

        elif step == 1:
            serial = self.cfg.setdefault("serial", {})
            port = self._port_var.get().strip()
            if port:
                serial["port"] = port
                # Enable auto-connect only if a port was explicitly chosen
                serial["auto_connect"] = True
                self.cfg["auto_connect"] = True
            try:
                serial["baud"] = int(self._baud_var.get())
            except Exception:
                serial["baud"] = 9600

        elif step == 2:
            self._apply_meters()

    def _apply_meters(self):
        """Build cfg["meters"] from the wizard rows."""
        meters = []
        for i, row in enumerate(self._meter_rows):
            name = row["name"].get().strip() or f"Meter {i+1}"
            try:
                slave_id = max(1, min(247, int(row["slave_id"].get())))
            except Exception:
                slave_id = i + 1
            sign_str = row["sign"].get()
            total_sign = -1.0 if sign_str == "−" else 1.0

            meters.append({
                "id":               i + 1,
                "slave_id":         slave_id,
                "enabled":          True,
                "display":          True,
                "include_in_total": True,
                "total_sign":       total_sign,
                "priority":         (i == 0),
                "name":             name,
                "serial_no":        "",
                "scan_interval_sec": 1,
                "retry_count":      2,
                "timeout_sec":      1.0,
            })

        # Keep only configured meter rows. Do not pad hidden disabled slots;
        # small plants should stay small, and larger plants should scale by
        # adding actual rows.
        self.cfg["meters"] = meters


def run_setup_wizard(cfg: Dict[str, Any]) -> Dict[str, Any]:
    """Show the wizard if this is a first run.  Returns (possibly updated) cfg."""
    if cfg.get("setup_complete"):
        return cfg

    try:
        wizard = SetupWizard(cfg)
        result = wizard.run()
        return result if result is not None else cfg
    except Exception:
        logger.exception("[WIZARD] unexpected error — skipping wizard")
        return cfg
