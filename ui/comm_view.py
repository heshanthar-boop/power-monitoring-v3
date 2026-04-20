import os
import json
import threading
import tkinter as tk
from tkinter import ttk, messagebox, filedialog

from utils.paths import profiles_dir, set_base_dir
from utils.atomic import atomic_write_json
from core.modbus_codec import read_meter_serial
from utils.logger import setup_logger

log = setup_logger("comm")

# Try to import serial port detection tools
try:
    from serial.tools.list_ports import comports
except Exception:
    comports = None


class CommTab(ttk.Frame):
    def __init__(self, parent, cfg, meters, worker=None, on_apply=None, on_title_change=None):
        super().__init__(parent, padding=8)
        self.cfg = cfg
        self._privacy = bool((self.cfg.get("ui") or {}).get("privacy_mode", True))
        self.meters = meters
        self.worker = worker
        self.on_apply = on_apply
        self.on_title_change = on_title_change  # Callback to update window title

        # Track connection state for auto-read serials
        self._last_worker_status = None
        self._serials_read = False
        self._reading_serials = False
        self._pending_auto_scan = bool(self.cfg.get("auto_scan_meters", True))  # Auto-scan on first connect
        self._auto_scan_in_progress = False

        # Build scrollable container for long Comm tab
        self._build_scroll_container()

        # Status banner at top
        self._build_status_banner()

        self._build_site_info()
        ttk.Separator(self._content).pack(fill="x", padx=8, pady=4)
        self._build_global()
        ttk.Separator(self._content).pack(fill="x", padx=8, pady=8)
        self._build_per_meter()
        ttk.Separator(self._content).pack(fill="x", padx=8, pady=8)
        self._build_profile_buttons()

    # ------------------------------------------------------------------
    # Scroll container (right-side scrollbar for long Comm tab)
    # ------------------------------------------------------------------
    def _build_scroll_container(self):
        """Create a canvas+scrollbar wrapper so the Comm tab can scroll.

        SCADA note: keep it simple and robust (no fancy widgets). This is
        only for layout; it must not change business logic.
        """
        outer = ttk.Frame(self)
        outer.pack(fill="both", expand=True)

        self._canvas = tk.Canvas(
            outer,
            highlightthickness=0,
            bd=0,
            relief="flat",
        )
        self._vscroll = ttk.Scrollbar(outer, orient="vertical", command=self._canvas.yview)
        self._canvas.configure(yscrollcommand=self._vscroll.set)

        self._vscroll.pack(side="right", fill="y")
        self._canvas.pack(side="left", fill="both", expand=True)

        # Inner content frame that holds all widgets
        self._content = ttk.Frame(self._canvas)
        self._content_window = self._canvas.create_window((0, 0), window=self._content, anchor="nw")

        # Keep scrollregion updated
        def _on_content_configure(_evt=None):
            self._canvas.configure(scrollregion=self._canvas.bbox("all"))

        def _on_canvas_configure(evt):
            # Make inner frame width follow canvas width (no horizontal scroll)
            try:
                self._canvas.itemconfigure(self._content_window, width=evt.width)
            except Exception:
                pass

        self._content.bind("<Configure>", _on_content_configure)
        self._canvas.bind("<Configure>", _on_canvas_configure)

        # Mouse wheel support (Windows)
        def _on_mousewheel(evt):
            # evt.delta is typically 120 per notch on Windows
            if evt.delta:
                self._canvas.yview_scroll(int(-1 * (evt.delta / 120)), "units")

        # Bind on enter/leave so wheel only scrolls when pointer is over this tab
        def _bind_wheel(_evt=None):
            self._canvas.bind_all("<MouseWheel>", _on_mousewheel)

        def _unbind_wheel(_evt=None):
            try:
                self._canvas.unbind_all("<MouseWheel>")
            except Exception:
                pass

        self._canvas.bind("<Enter>", _bind_wheel)
        self._canvas.bind("<Leave>", _unbind_wheel)
    def _build_status_banner(self):
        """Non-blocking status banner showing worker state."""
        self.banner_frame = ttk.Frame(self._content)
        self.banner_frame.pack(fill="x", padx=8, pady=4)

        self.status_var = tk.StringVar(value="")
        # Use ttk label (theme-friendly). Keep it simple: text only.
        self.status_label = ttk.Label(
            self.banner_frame,
            textvariable=self.status_var,
            style="HeaderLineMuted.TLabel",
            anchor="w"
        )
        # Initially hidden
        self.status_label.pack_forget()

    # ------------------------------------------------------------------
    # Site / Plant Information
    # ------------------------------------------------------------------
    def _build_site_info(self):
        """Site and plant information for window title and log headers."""
        f = ttk.LabelFrame(self._content, text="Site / Plant Information")
        f.pack(fill="x", padx=8, pady=8)

        site_cfg = self.cfg.get("site", {})

        row1 = ttk.Frame(f)
        row1.pack(fill="x", padx=8, pady=6)

        ttk.Label(row1, text="Plant Name:").pack(side="left")
        self.ent_plant_name = ttk.Entry(row1, width=40)
        self.ent_plant_name.insert(0, site_cfg.get("plant_name", "SELEC MFM384-C Industrial Monitoring"))
        self.ent_plant_name.pack(side="left", padx=6)

        ttk.Label(row1, text="Location:").pack(side="left", padx=(12, 0))
        self.ent_location = ttk.Entry(row1, width=30)
        self.ent_location.insert(0, site_cfg.get("location", ""))
        self.ent_location.pack(side="left", padx=6)

        row2 = ttk.Frame(f)
        row2.pack(fill="x", padx=8, pady=(0, 6))

        ttk.Label(row2, text="Description:").pack(side="left")
        self.ent_description = ttk.Entry(row2, width=60)
        self.ent_description.insert(0, site_cfg.get("description", ""))
        self.ent_description.pack(side="left", padx=6)

        ttk.Button(row2, text="Update Title", command=self._update_title).pack(side="left", padx=12)

    def _update_title(self):
        """Update window title and save site config."""
        site_cfg = self.cfg.setdefault("site", {})
        site_cfg["plant_name"] = self.ent_plant_name.get().strip()
        site_cfg["location"] = self.ent_location.get().strip()
        site_cfg["description"] = self.ent_description.get().strip()

        # Call the title change callback if set
        if self.on_title_change:
            self.on_title_change()

    # ------------------------------------------------------------------
    # Global communication settings + Storage (One Folder)
    # ------------------------------------------------------------------
    def _build_global(self):
        f = ttk.LabelFrame(self._content, text="Communication Settings")
        f.pack(fill="x", padx=8, pady=8)

        s = self.cfg.get("serial", {})

        # ---- Transport selector (RTU / TCP) ----
        transport_row = ttk.Frame(f)
        transport_row.pack(fill="x", padx=8, pady=(6, 2))

        ttk.Label(transport_row, text="Protocol:").pack(side="left")
        self._transport_var = tk.StringVar(value=str(s.get("transport", "rtu")).lower())
        self._btn_rtu = ttk.Radiobutton(
            transport_row, text="RTU (RS-485 Serial)", variable=self._transport_var,
            value="rtu", command=self._on_transport_change,
        )
        self._btn_rtu.pack(side="left", padx=(8, 0))
        self._btn_tcp = ttk.Radiobutton(
            transport_row, text="TCP (Ethernet/Gateway)", variable=self._transport_var,
            value="tcp", command=self._on_transport_change,
        )
        self._btn_tcp.pack(side="left", padx=(16, 0))

        # ---- RTU fields (COM port + serial params) ----
        self._rtu_frame = ttk.Frame(f)
        self._rtu_frame.pack(fill="x", padx=8, pady=4)

        row1 = self._rtu_frame

        ttk.Label(row1, text="COM Port:").pack(side="left")
        self.ent_com = ttk.Entry(row1, width=12)
        self.ent_com.insert(0, s.get("port", ""))
        self.ent_com.pack(side="left", padx=4)

        ttk.Button(row1, text="Scan Ports", command=self._scan_ports).pack(side="left", padx=4)

        ttk.Label(row1, text="Baud:").pack(side="left", padx=(16, 0))
        self.cmb_baud = ttk.Combobox(
            row1, values=[300, 600, 1200, 2400, 4800, 9600, 19200], width=7, state="readonly"
        )
        self.cmb_baud.set(s.get("baud", 9600))
        self.cmb_baud.pack(side="left", padx=4)

        ttk.Label(row1, text="Parity:").pack(side="left", padx=(16, 0))
        self.cmb_parity = ttk.Combobox(row1, values=["N", "E", "O"], width=4, state="readonly")
        self.cmb_parity.set(s.get("parity", "N"))
        self.cmb_parity.pack(side="left", padx=4)

        ttk.Label(row1, text="Stop:").pack(side="left", padx=(16, 0))
        self.cmb_stop = ttk.Combobox(row1, values=[1, 2], width=4, state="readonly")
        self.cmb_stop.set(s.get("stopbits", 1))
        self.cmb_stop.pack(side="left", padx=4)

        # ---- TCP fields (host + port) ----
        self._tcp_frame = ttk.Frame(f)
        # packed/unpacked by _on_transport_change

        ttk.Label(self._tcp_frame, text="Host / IP:").pack(side="left")
        self.ent_tcp_host = ttk.Entry(self._tcp_frame, width=20)
        self.ent_tcp_host.insert(0, s.get("tcp_host", ""))
        self.ent_tcp_host.pack(side="left", padx=4)

        ttk.Label(self._tcp_frame, text="Port:").pack(side="left", padx=(16, 0))
        self.ent_tcp_port = ttk.Entry(self._tcp_frame, width=6)
        self.ent_tcp_port.insert(0, str(int(s.get("tcp_port", 502) or 502)))
        self.ent_tcp_port.pack(side="left", padx=4)

        ttk.Label(self._tcp_frame, text="(Modbus TCP gateway or RTU-over-TCP converter)", style="Muted.TLabel").pack(side="left", padx=(8, 0))

        # ---- Common params row (Word Order + Base Addr) ----
        row_common = ttk.Frame(f)
        row_common.pack(fill="x", padx=8, pady=4)

        ttk.Label(row_common, text="Word Order:").pack(side="left")
        self.cmb_word = ttk.Combobox(
            row_common, values=["ABCD", "CDAB", "BADC", "DCBA"], width=7, state="readonly"
        )
        self.cmb_word.set(s.get("word_order", "CDAB"))
        self.cmb_word.pack(side="left", padx=4)

        ttk.Label(row_common, text="Base Addr:").pack(side="left", padx=(16, 0))
        self.cmb_base_addr = ttk.Combobox(
            row_common, values=["0", "1"], width=4, state="readonly"
        )
        self.cmb_base_addr.set(str(int(s.get("base_address", 0) or 0)))
        self.cmb_base_addr.pack(side="left", padx=4)
        ttk.Label(row_common, text="(0=standard, 1=legacy)", style="Muted.TLabel").pack(side="left", padx=(2, 0))

        # Apply initial visibility
        self._on_transport_change()

        # Row 2: Checkboxes
        row2 = ttk.Frame(f)
        row2.pack(fill="x", padx=8, pady=6)

        # Auto-connect lives under serial config (legacy top-level key still supported)
        auto_conn = s.get("auto_connect", self.cfg.get("auto_connect", False))
        self.var_auto_connect = tk.BooleanVar(value=bool(auto_conn))
        self.var_auto_scan = tk.BooleanVar(value=bool(self.cfg.get("auto_scan_meters", True)))
        self.var_auto_log = tk.BooleanVar(value=bool(self.cfg.get("auto_start_logging", False)))

        ttk.Checkbutton(row2, text="Auto-connect on startup", variable=self.var_auto_connect).pack(side="left")
        ttk.Checkbutton(row2, text="Auto-scan meters", variable=self.var_auto_scan).pack(side="left", padx=16)
        ttk.Checkbutton(row2, text="Auto-start logging", variable=self.var_auto_log).pack(side="left", padx=16)

        # ---- Storage (One Folder) ----
        ttk.Separator(f).pack(fill="x", padx=8, pady=(6, 6))

        storage = ttk.Frame(f)
        storage.pack(fill="x", padx=8, pady=(0, 6))

        ttk.Label(storage, text="Storage (One Folder):").pack(side="left")

        self.base_dir_var = tk.StringVar(
            value=((self.cfg.get("paths", {}) or {}).get("base_dir", "") or "")
        )

        # Privacy mode: never show full filesystem paths on the main UI.
        # Show only a neutral hint; keep the actual value in base_dir_var.
        self.base_dir_display = tk.StringVar(value="")
        self._sync_base_dir_display()
        self.base_dir_label = ttk.Label(storage, textvariable=self.base_dir_display)
        self.base_dir_label.pack(side="left", padx=8)

        ttk.Button(storage, text="Browse…", command=self._browse_base_dir).pack(side="left", padx=(4, 0))
        ttk.Button(storage, text="Use Default", command=self._use_default_base_dir).pack(side="left", padx=(4, 0))

        hint = ttk.Label(
            f,
            text="This folder will contain: logs / power logs / protection events / reports / snapshots / email DB. Restart recommended after changing.",
            style="Muted.TLabel",
        )
        hint.pack(fill="x", padx=8, pady=(0, 8))

        # Row 3: Action buttons
        row3 = ttk.Frame(f)
        row3.pack(fill="x", padx=8, pady=8)

        ttk.Button(row3, text="Scan Meters", command=self._scan_for_meters).pack(side="left", padx=4)

        # Connect/Disconnect as ttk.Button (no bg painting)
        self.btn_connect = ttk.Button(row3, text="Connect", style="Primary.TButton", command=self.connect_now)
        self.btn_connect.pack(side="left", padx=4)

        self.btn_disconnect = ttk.Button(row3, text="Disconnect", command=self.disconnect_now, state="disabled")
        self.btn_disconnect.pack(side="left", padx=4)

        self.comm_health_var = tk.StringVar(value="Communication diagnostics will appear after connection.")
        ttk.Label(f, textvariable=self.comm_health_var, style="Muted.TLabel").pack(fill="x", padx=8, pady=(0, 8))

    def _on_transport_change(self, *_):
        """Show RTU or TCP fields based on protocol selection."""
        if self._transport_var.get() == "tcp":
            self._rtu_frame.pack_forget()
            self._tcp_frame.pack(fill="x", padx=8, pady=4)
        else:
            self._tcp_frame.pack_forget()
            self._rtu_frame.pack(fill="x", padx=8, pady=4)

    def _browse_base_dir(self):
        p = filedialog.askdirectory(title="Select base folder for ALL app data")
        if p:
            self.base_dir_var.set(p)
            self._sync_base_dir_display()

    def _use_default_base_dir(self):
        # blank = default %APPDATA%\PowerMonitoringReporting
        self.base_dir_var.set("")
        self._sync_base_dir_display()

    def _sync_base_dir_display(self):
        """Update the UI-friendly storage path hint (privacy-safe)."""
        raw = (self.base_dir_var.get() or "").strip()
        if not raw:
            self.base_dir_display.set("Default (AppData)")
            return
        if self._privacy:
            self.base_dir_display.set("Custom folder set")
        else:
            self.base_dir_display.set(raw)

    # ------------------------------------------------------------------
    # Per-meter settings (simplified)
    # ------------------------------------------------------------------
    def _build_per_meter(self):
        f = ttk.LabelFrame(self._content, text="Meters")
        f.pack(fill="x", padx=8, pady=8)

        # Use a grid frame for proper alignment - compact SCADA-style layout
        grid_frame = ttk.Frame(f)
        grid_frame.pack(fill="x", padx=6, pady=4)

        # Configure column weights - NO column expands, all fixed width
        grid_frame.columnconfigure(0, weight=0, minsize=40)
        grid_frame.columnconfigure(1, weight=0, minsize=70)
        grid_frame.columnconfigure(2, weight=0, minsize=60)
        grid_frame.columnconfigure(3, weight=0, minsize=100)
        grid_frame.columnconfigure(4, weight=0, minsize=100)
        grid_frame.columnconfigure(5, weight=0, minsize=70)
        grid_frame.columnconfigure(6, weight=0, minsize=90)
        grid_frame.columnconfigure(7, weight=0, minsize=70)
        grid_frame.columnconfigure(8, weight=0, minsize=70)
        grid_frame.columnconfigure(9, weight=0, minsize=180)

        hdr_font = ("Segoe UI", 9, "bold")
        ttk.Label(grid_frame, text="Meter", font=hdr_font, width=5).grid(
            row=0, column=0, sticky="w", padx=(6, 4), pady=4)
        ttk.Label(grid_frame, text="Slave", font=hdr_font, width=8, anchor="center").grid(
            row=0, column=1, sticky="w", padx=4, pady=4)
        ttk.Label(grid_frame, text="Scan(s)", font=hdr_font, width=7, anchor="center").grid(
            row=0, column=2, sticky="w", padx=4, pady=4)
        ttk.Label(grid_frame, text="Name", font=hdr_font, width=12, anchor="w").grid(
            row=0, column=3, sticky="w", padx=4, pady=4)
        ttk.Label(grid_frame, text="Serial No.", font=hdr_font, width=12, anchor="w").grid(
            row=0, column=4, sticky="w", padx=4, pady=4)
        ttk.Label(grid_frame, text="Status", font=hdr_font, width=8, anchor="center").grid(
            row=0, column=5, sticky="w", padx=4, pady=4)
        ttk.Label(grid_frame, text="TOTAL Calc", font=hdr_font, width=10, anchor="center").grid(
            row=0, column=6, sticky="w", padx=4, pady=4)
        ttk.Label(grid_frame, text="CT Ratio", font=hdr_font, width=8, anchor="center").grid(
            row=0, column=7, sticky="w", padx=4, pady=4)
        ttk.Label(grid_frame, text="PT Ratio", font=hdr_font, width=8, anchor="center").grid(
            row=0, column=8, sticky="w", padx=4, pady=4)
        ttk.Label(grid_frame, text="Meter Model / Driver", font=hdr_font, anchor="w").grid(
            row=0, column=9, sticky="w", padx=4, pady=4)

        self.rows = []

        for i, m in enumerate(self.meters):
            row_num = i + 1

            ttk.Label(grid_frame, text=f"#{m.meter_id}", width=5).grid(
                row=row_num, column=0, sticky="w", padx=(6, 4), pady=2)

            lbl_slave = ttk.Label(
                grid_frame,
                text=str(m.slave_id),
                width=8,
                anchor="center",
                relief="sunken",
            )
            lbl_slave.grid(row=row_num, column=1, sticky="w", padx=4, pady=2)

            cmb_scan = ttk.Combobox(grid_frame, values=[1, 2, 5, 10], width=5, state="readonly")
            cmb_scan.set(int(m.scan_interval_sec))
            cmb_scan.grid(row=row_num, column=2, sticky="w", padx=4, pady=2)

            ent_name = ttk.Entry(grid_frame, width=12)
            ent_name.insert(0, m.name)
            ent_name.grid(row=row_num, column=3, sticky="w", padx=4, pady=2)

            lbl_ser = ttk.Label(
                grid_frame,
                text=m.serial_no or "--",
                width=12,
                anchor="w",
                relief="sunken",
            )
            lbl_ser.grid(row=row_num, column=4, sticky="w", padx=4, pady=2)

            lbl_status = ttk.Label(grid_frame, text="--", width=8, anchor="center")
            lbl_status.grid(row=row_num, column=5, sticky="w", padx=4, pady=2)

            # TOTAL calculation role (ADD / SUBTRACT / OFF)
            cmb_total = ttk.Combobox(
                grid_frame,
                values=["ADD (+)", "SUB (-)", "OFF"],
                width=9,
                state="readonly",
            )
            # Default mapping from existing flags
            if not bool(getattr(m, "include_in_total", True)):
                cmb_total.set("OFF")
            else:
                try:
                    sign = float(getattr(m, "total_sign", 1.0) or 1.0)
                except Exception:
                    sign = 1.0
                cmb_total.set("SUB (-)" if sign < 0 else "ADD (+)")
            cmb_total.grid(row=row_num, column=6, sticky="w", padx=4, pady=2)

            # CT / PT ratio scaling
            ent_ct = ttk.Entry(grid_frame, width=7)
            ent_ct.insert(0, str(round(float(getattr(m, "ct_ratio", 1.0) or 1.0), 4)))
            ent_ct.grid(row=row_num, column=7, sticky="w", padx=4, pady=2)

            ent_pt = ttk.Entry(grid_frame, width=7)
            ent_pt.insert(0, str(round(float(getattr(m, "pt_ratio", 1.0) or 1.0), 4)))
            ent_pt.grid(row=row_num, column=8, sticky="w", padx=4, pady=2)

            # Meter model / driver selector
            from core.device_drivers import DEVICE_MODELS, DEFAULT_MODEL
            model_labels = [label for _, label in DEVICE_MODELS]
            model_ids    = [mid   for mid, _ in DEVICE_MODELS]
            current_model = str(getattr(m, "model", DEFAULT_MODEL) or DEFAULT_MODEL)
            try:
                model_label_default = model_labels[model_ids.index(current_model)]
            except (ValueError, IndexError):
                model_label_default = model_labels[0]
            cmb_model = ttk.Combobox(
                grid_frame, values=model_labels, state="readonly", width=28,
            )
            cmb_model.set(model_label_default)
            cmb_model.grid(row=row_num, column=9, sticky="w", padx=4, pady=2)

            var_en = tk.BooleanVar(value=m.enabled)
            var_disp = tk.BooleanVar(value=m.display)
            var_total = tk.BooleanVar(value=m.include_in_total)
            var_pri = tk.BooleanVar(value=m.priority)

            self.rows.append(
                (m, var_en, var_disp, var_total, var_pri, cmb_scan, ent_name, cmb_total, lbl_ser, lbl_slave, lbl_status, ent_ct, ent_pt, cmb_model)
            )

    def _set_priority(self, meter_id):
        for (m, _, _, _, var_pri, *_rest) in self.rows:
            if m.meter_id != meter_id:
                var_pri.set(False)

    def _read_meter_serials(self):
        """Read serial numbers from all enabled meters (manual button)."""
        if not self.worker:
            messagebox.showerror("Error", "Modbus worker not available.", parent=self)
            return

        if not self.worker.connected:
            messagebox.showwarning("Not Connected", "Modbus not connected. Please connect first.", parent=self)
            return

        for row_data in self.rows:
            m = row_data[0]
            if not m.enabled:
                m.serial_no = ""
            else:
                m.serial_no = ""
        self._update_serial_display()

        def read_task():
            results = {}
            for row_data in self.rows:
                m = row_data[0]
                var_en = row_data[1]

                if not var_en.get():
                    continue

                try:
                    log.info(f"Reading serial for Meter {m.meter_id} (slave_id={m.slave_id})")
                    serial = self.worker.run_client_task(
                        read_meter_serial,
                        slave_id=m.slave_id,
                        base_address=0
                    )
                    results[m.meter_id] = serial
                    log.info(f"Meter {m.meter_id} serial: {serial}")
                except Exception as e:
                    log.error(f"Failed to read serial for Meter {m.meter_id}: {e}")
                    results[m.meter_id] = ""

            self.after(0, lambda: self._on_manual_serials_read(results))

        threading.Thread(target=read_task, daemon=True).start()

    def _on_manual_serials_read(self, results):
        """Handle manual serial read with popup notification."""
        self._on_serials_read(results)
        messagebox.showinfo("Complete", f"Read serial numbers for {len(results)} meter(s)", parent=self)

    def _on_serials_read(self, results):
        """Handle serial numbers read completion."""
        self._reading_serials = False

        for row_data in self.rows:
            m = row_data[0]
            if m.meter_id in results:
                serial = (results.get(m.meter_id) or "").strip()
                if serial and serial.lower() not in ("error", "none"):
                    m.serial_no = serial
                else:
                    m.serial_no = ""

        self._update_serial_display()

        self._serials_read = True
        log.info(f"Serial numbers read for {len(results)} meter(s)")

    def _auto_read_serials(self):
        """Auto-read serial numbers when connected (no popup)."""
        if not self.worker or not self.worker.connected:
            return

        self._reading_serials = True

        for row_data in self.rows:
            m = row_data[0]
            if not m.enabled:
                m.serial_no = ""
            else:
                m.serial_no = ""
        self._update_serial_display()

        def read_task():
            results = {}
            for row_data in self.rows:
                m = row_data[0]
                var_en = row_data[1]

                if not var_en.get():
                    continue

                try:
                    log.info(f"Auto-reading serial for Meter {m.meter_id} (slave_id={m.slave_id})")
                    serial = self.worker.run_client_task(
                        read_meter_serial,
                        slave_id=m.slave_id,
                        base_address=0
                    )
                    results[m.meter_id] = serial
                    log.info(f"Meter {m.meter_id} serial: {serial}")
                except Exception as e:
                    log.error(f"Failed to read serial for Meter {m.meter_id}: {e}")
                    results[m.meter_id] = ""

            self.after(0, lambda: self._on_serials_read(results))

        threading.Thread(target=read_task, daemon=True).start()

    def _update_serial_display(self):
        """Update serial labels from meter objects."""
        for row_data in self.rows:
            m = row_data[0]
            lbl_ser = row_data[8]

            current_text = lbl_ser.cget("text")
            if not getattr(m, "enabled", True):
                new_text = "--"
            else:
                new_text = (m.serial_no or "").strip() or "--"
            if current_text != new_text:
                lbl_ser.config(text=new_text)

    def _scan_ports(self):
        """Scan for available COM ports and show selection dialog."""
        if comports is None:
            messagebox.showerror("Error", "Serial tools not available", parent=self)
            return

        try:
            ports = list(comports())
            if not ports:
                messagebox.showwarning("No Ports", "No COM ports found", parent=self)
                return

            port_list = []
            for port in ports:
                if port.description and port.description != "n/a":
                    port_list.append(f"{port.device} - {port.description}")
                else:
                    port_list.append(port.device)

            self._show_port_selector(port_list, [p.device for p in ports])

        except Exception as e:
            messagebox.showerror("Error", f"Failed to scan ports:\n{str(e)}", parent=self)

    def _show_port_selector(self, display_list, device_list):
        """Show a popup to select from available ports."""
        dialog = tk.Toplevel(self)
        dialog.title("Select COM Port")
        dialog.geometry("400x250")
        dialog.resizable(False, False)

        dialog.transient(self)
        dialog.grab_set()

        ttk.Label(dialog, text="Available COM Ports:", font=("Segoe UI", 10, "bold")).pack(pady=10)

        frame = ttk.Frame(dialog)
        frame.pack(fill="both", expand=True, padx=10, pady=5)

        scrollbar = ttk.Scrollbar(frame)
        scrollbar.pack(side="right", fill="y")

        listbox = tk.Listbox(frame, yscrollcommand=scrollbar.set, height=8, width=45)
        listbox.pack(side="left", fill="both", expand=True)
        scrollbar.config(command=listbox.yview)

        for item in display_list:
            listbox.insert("end", item)

        if display_list:
            listbox.selection_set(0)

        def select_port():
            sel = listbox.curselection()
            if sel:
                idx = sel[0]
                self.ent_com.delete(0, "end")
                self.ent_com.insert(0, device_list[idx])
                dialog.destroy()

        button_frame = ttk.Frame(dialog)
        button_frame.pack(pady=10)

        ttk.Button(button_frame, text="Select", command=select_port).pack(side="left", padx=5)
        ttk.Button(button_frame, text="Cancel", command=dialog.destroy).pack(side="left", padx=5)

    def apply(self):
        """Apply communication settings."""
        s = self.cfg.setdefault("serial", {})

        s["transport"] = self._transport_var.get()
        s["port"] = self.ent_com.get().strip()
        s["baud"] = int(self.cmb_baud.get())
        s["parity"] = self.cmb_parity.get()
        s["stopbits"] = int(self.cmb_stop.get())
        s["tcp_host"] = self.ent_tcp_host.get().strip()
        try:
            s["tcp_port"] = int(self.ent_tcp_port.get().strip() or 502)
        except ValueError:
            s["tcp_port"] = 502
        s["word_order"] = self.cmb_word.get()
        s["base_address"] = int(self.cmb_base_addr.get() or 0)

        s["auto_connect"] = bool(self.var_auto_connect.get())
        # Keep legacy top-level key in sync for backward compatibility
        self.cfg["auto_connect"] = bool(s.get("auto_connect", False))
        self.cfg["auto_start_logging"] = bool(self.var_auto_log.get())
        self.cfg["auto_scan_meters"] = bool(self.var_auto_scan.get())

        # ---- Storage (One Folder): save + apply immediately ----
        self.cfg.setdefault("paths", {})
        self.cfg["paths"]["base_dir"] = (self.base_dir_var.get() or "").strip()
        try:
            set_base_dir(self.cfg["paths"]["base_dir"])
        except Exception:
            # Do not crash UI for folder issues; logging will capture details elsewhere
            pass

        # Enforce: Storage (One Folder) is the single common root.
        # If operator has previously set per-feature folders, clear them so they
        # resolve under base_dir via utils.paths (logs_dir/reports_dir/etc.).
        try:
            self.cfg.setdefault("logging", {})
            self.cfg["logging"]["folder"] = ""
        except Exception:
            pass
        try:
            self.cfg.setdefault("reports", {})
            self.cfg["reports"]["auto_save_dir"] = ""
        except Exception:
            pass
        try:
            self.cfg.setdefault("protection_events", {})
            # blank -> ProtectionEngine will default to logs_dir()/protection_events.csv
            self.cfg["protection_events"]["csv_path"] = ""

            # Smart-Advisory event history has its own CSV (default advisory_events.csv)
            self.cfg.setdefault("advisory_events", {})
            self.cfg["advisory_events"]["csv_path"] = ""
        except Exception:
            pass

        # Save meter settings (scan interval + name + CT/PT ratios)
        for row_data in self.rows:
            m = row_data[0]
            cmb_scan = row_data[5]
            ent_name = row_data[6]

            # TOTAL calc role
            cmb_total = row_data[7]

            m.scan_interval_sec = int(cmb_scan.get())
            m.name = ent_name.get().strip() or f"Meter {m.meter_id}"

            role = str(cmb_total.get() or "ADD (+)").strip().upper()
            if role.startswith("OFF"):
                m.include_in_total = False
                m.total_sign = 1.0
            else:
                m.include_in_total = True
                m.total_sign = -1.0 if role.startswith("SUB") else 1.0

            # CT / PT ratio + model (indices 11, 12, 13)
            if len(row_data) >= 13:
                try:
                    m.ct_ratio = max(0.001, float(row_data[11].get().strip() or "1.0"))
                except Exception:
                    m.ct_ratio = 1.0
                try:
                    m.pt_ratio = max(0.001, float(row_data[12].get().strip() or "1.0"))
                except Exception:
                    m.pt_ratio = 1.0
            if len(row_data) >= 14:
                try:
                    from core.device_drivers import DEVICE_MODELS, DEFAULT_MODEL
                    sel_label = row_data[13].get()
                    model_map = {label: mid for mid, label in DEVICE_MODELS}
                    m.model = model_map.get(sel_label, DEFAULT_MODEL)
                except Exception:
                    m.model = "selec_mfm384"

        # Keep worker in sync with updated communication settings
        if self.worker:
            try:
                from core.comm_config import normalize_serial_config
                self.worker.serial_cfg = normalize_serial_config(self.cfg.get("serial", {}))
            except Exception:
                self.worker.serial_cfg = self.cfg.get("serial", {})
            self.worker.auto_connect = bool(s.get("auto_connect", False))

        if self.on_apply:
            self.on_apply()

        if self.on_title_change:
            try:
                self.on_title_change()
            except Exception:
                pass

    def connect_now(self):
        """Manually trigger connection attempt."""
        if not self.worker:
            messagebox.showwarning("Error", "Worker not available", parent=self)
            return

        transport = self._transport_var.get()
        if transport == "tcp":
            host = self.ent_tcp_host.get().strip()
            if not host:
                messagebox.showwarning("Error", "Please enter a host/IP address first", parent=self)
                return
            target_desc = f"{host}:{self.ent_tcp_port.get().strip() or 502} (TCP)"
        else:
            port = self.ent_com.get().strip()
            if not port:
                messagebox.showwarning("Error", "Please select a COM port first", parent=self)
                return
            target_desc = f"{port} (RTU)"

        # Save settings first
        self.apply()

        self.worker.serial_cfg = self.cfg.get("serial", {})

        self.worker._backoff.reset()
        self.worker.connect()

        if self.var_auto_scan.get():
            self._pending_auto_scan = True
            log.info("Auto-scan enabled - will scan for meters after connection")

        messagebox.showinfo("Connect", f"Connecting to {target_desc}...\nAuto-scan will detect meters.", parent=self)

    def disconnect_now(self):
        """Manually disconnect from COM port."""
        if not self.worker:
            messagebox.showwarning("Error", "Worker not available", parent=self)
            return

        self.var_auto_connect.set(False)
        s = self.cfg.setdefault("serial", {})
        s["auto_connect"] = False
        self.cfg["auto_connect"] = False
        self._pending_auto_scan = False
        self._auto_scan_in_progress = False

        self.worker.auto_connect = False

        self.worker.disconnect()
        log.info("Manual disconnect - auto-connect disabled in worker")
        messagebox.showinfo("Disconnect", "Disconnected from COM port.\nAuto-connect disabled.", parent=self)

    def _scan_slave_ids(self):
        """Return the configured scan range without assuming a 6-meter plant."""
        configured = []
        for m in getattr(self, "meters", []) or []:
            try:
                sid = int(getattr(m, "slave_id", 0) or 0)
                if 1 <= sid <= 247:
                    configured.append(sid)
            except Exception:
                pass
        max_configured = max(configured) if configured else 0
        try:
            max_sid = int(((self.cfg.get("serial") or {}).get("scan_max_slave_id")) or max(3, max_configured, len(configured)))
        except Exception:
            max_sid = max(3, max_configured, len(configured))
        max_sid = max(1, min(247, max_sid))
        return list(range(1, max_sid + 1))

    def _start_auto_scan(self):
        """Automatic meter scan on connect - no dialog, auto-apply results."""
        if not self.worker or self.worker.status != "CONNECTED":
            log.warning("Cannot auto-scan - not connected")
            self._auto_scan_in_progress = False
            return

        self._auto_scan_in_progress = True
        scan_ids = self._scan_slave_ids()
        log.info("Starting automatic meter scan (slave IDs %s)", scan_ids)

        self.status_var.set("Scanning for meters...")
        self.status_label.pack(fill="x", padx=0, pady=(0, 4))

        def do_auto_scan():
            def on_progress(slave_id, found, current, total):
                status = "FOUND" if found else "No response"
                self.after(0, lambda s=slave_id: self.status_var.set(f"Scanning... Slave {s}: {status}"))

            results = self.worker.scan_for_meters(
                slave_ids=scan_ids,
                on_progress=on_progress
            )
            self.after(0, lambda: self._auto_scan_complete(results))

        threading.Thread(target=do_auto_scan, daemon=True).start()

    def _auto_scan_complete(self, results):
        """Handle auto-scan completion - auto-apply results."""
        self._auto_scan_in_progress = False
        found_ids = [sid for sid, ok in results.items() if ok]
        found_count = len(found_ids)

        log.info(f"Auto-scan complete. Found {found_count} meter(s) at slave IDs: {found_ids}")

        if found_count == 0:
            self.status_var.set("No meters found. Check connections and slave IDs.")
            self.after(5000, lambda: self.status_label.pack_forget())
            return

        for row_data in self.rows:
            m = row_data[0]
            var_en = row_data[1]
            var_disp = row_data[2]

            if m.slave_id in found_ids:
                var_en.set(True)
                var_disp.set(True)
                m.enabled = True
                m.display = True
                log.info(f"Auto-enabled Meter {m.meter_id} (slave_id={m.slave_id})")
            else:
                var_en.set(False)
                m.enabled = False
                m.serial_no = ""

        self._update_serial_display()

        self._save_meter_settings()
        if self.on_apply:
            self.on_apply()

        self._update_serial_display()

        self.status_var.set(f"Found {found_count} meter(s) at slave IDs: {found_ids}")
        self.after(500, self._auto_read_serials)
        self.after(5000, lambda: self.status_label.pack_forget())

    def _save_meter_settings(self):
        """Save meter settings without showing messagebox."""
        for row_data in self.rows:
            m = row_data[0]
            var_en = row_data[1]
            var_disp = row_data[2]
            var_pri = row_data[4]
            cmb_scan = row_data[5]
            ent_name = row_data[6]
            cmb_total = row_data[7]

            m.enabled = bool(var_en.get())
            m.display = bool(var_disp.get())
            m.priority = bool(var_pri.get())
            m.scan_interval_sec = int(cmb_scan.get())
            m.name = ent_name.get().strip() or f"Meter {m.meter_id}"

            role = str(cmb_total.get() or "ADD (+)").strip().upper()
            if role.startswith("OFF"):
                m.include_in_total = False
                m.total_sign = 1.0
            else:
                m.include_in_total = True
                m.total_sign = -1.0 if role.startswith("SUB") else 1.0

            if len(row_data) >= 13:
                try:
                    m.ct_ratio = max(0.001, float(row_data[11].get().strip() or "1.0"))
                except Exception:
                    m.ct_ratio = 1.0
                try:
                    m.pt_ratio = max(0.001, float(row_data[12].get().strip() or "1.0"))
                except Exception:
                    m.pt_ratio = 1.0
            if len(row_data) >= 14:
                try:
                    from core.device_drivers import DEVICE_MODELS, DEFAULT_MODEL
                    sel_label = row_data[13].get()
                    model_map = {label: mid for mid, label in DEVICE_MODELS}
                    m.model = model_map.get(sel_label, DEFAULT_MODEL)
                except Exception:
                    m.model = "selec_mfm384"

        enabled_meters = [m for m in self.meters if m.enabled]
        if enabled_meters and not any(m.priority for m in enabled_meters):
            enabled_meters[0].priority = True

        log.info("Meter settings saved")

    def _scan_for_meters(self):
        """Scan for responding meters on slave IDs 1-5."""
        if not self.worker:
            messagebox.showwarning("Error", "Worker not available", parent=self)
            return

        if self.worker.status != "CONNECTED":
            messagebox.showwarning("Not Connected",
                                   "Please connect to COM port first before scanning.", parent=self)
            return

        self._show_scanning_dialog()

    def _show_scanning_dialog(self):
        """Show dialog for meter scanning with progress."""
        dialog = tk.Toplevel(self)
        dialog.title("Scanning for Meters...")
        dialog.geometry("400x300")
        dialog.resizable(False, False)
        dialog.transient(self)
        dialog.grab_set()

        scan_ids = self._scan_slave_ids()
        ttk.Label(dialog, text=f"Scanning slave addresses {scan_ids[0]}-{scan_ids[-1]}...",
                  font=("Segoe UI", 10, "bold")).pack(pady=10)

        self.scan_progress = ttk.Progressbar(dialog, length=300, mode='determinate')
        self.scan_progress.pack(pady=10)

        self.scan_status = ttk.Label(dialog, text="Initializing scan...")
        self.scan_status.pack(pady=5)

        results_frame = ttk.LabelFrame(dialog, text="Found Meters")
        results_frame.pack(fill="both", expand=True, padx=10, pady=10)

        self.scan_results_list = tk.Listbox(results_frame, height=5)
        self.scan_results_list.pack(fill="both", expand=True, padx=5, pady=5)

        btn_frame = ttk.Frame(dialog)
        btn_frame.pack(fill="x", padx=10, pady=10)

        self.btn_apply_scan = ttk.Button(btn_frame, text="Apply Found Meters",
                                         command=lambda: self._apply_scan_results(dialog),
                                         state="disabled")
        self.btn_apply_scan.pack(side="left", padx=5)

        ttk.Button(btn_frame, text="Close", command=dialog.destroy).pack(side="right", padx=5)

        self.scan_dialog = dialog
        self.scan_found = {}

        def do_scan():
            def on_progress(slave_id, found, current, total):
                self.after(0, lambda: self._update_scan_progress(slave_id, found, current, total))

            results = self.worker.scan_for_meters(
                slave_ids=scan_ids,
                on_progress=on_progress
            )
            self.after(0, lambda: self._scan_complete(results))

        threading.Thread(target=do_scan, daemon=True).start()

    def _update_scan_progress(self, slave_id, found, current, total):
        """Update scanning dialog progress."""
        if hasattr(self, 'scan_progress'):
            self.scan_progress['value'] = (current / total) * 100

        if hasattr(self, 'scan_status'):
            status = "FOUND" if found else "No response"
            self.scan_status.config(text=f"Slave {slave_id}: {status}")

        if found and hasattr(self, 'scan_results_list'):
            self.scan_results_list.insert("end", f"Slave ID {slave_id} - Meter responding")
            self.scan_found[slave_id] = True

    def _scan_complete(self, results):
        """Handle scan completion."""
        if hasattr(self, 'scan_status'):
            found_count = sum(1 for v in results.values() if v)
            self.scan_status.config(text=f"Scan complete! Found {found_count} meter(s)")

        if hasattr(self, 'btn_apply_scan') and any(results.values()):
            self.btn_apply_scan.config(state="normal")

        self.scan_found = results

    def _apply_scan_results(self, dialog):
        """Apply scan results to meter configuration."""
        found_ids = [sid for sid, ok in self.scan_found.items() if ok]

        if not found_ids:
            messagebox.showinfo("No Meters", "No meters found to apply.", parent=dialog)
            return

        for row_data in self.rows:
            m = row_data[0]
            var_en = row_data[1]
            var_disp = row_data[2]

            if m.slave_id in found_ids:
                var_en.set(True)
                var_disp.set(True)
                m.enabled = True
                m.display = True
                m.include_in_total = True
                log.info(f"Enabled Meter {m.meter_id} (slave_id={m.slave_id})")
            else:
                var_en.set(False)
                m.enabled = False
                m.serial_no = ""

        self._update_serial_display()

        self._save_meter_settings()
        if self.on_apply:
            self.on_apply()

        messagebox.showinfo("Applied",
                            f"Enabled {len(found_ids)} meter(s) at slave IDs: {found_ids}",
                            parent=dialog)
        dialog.destroy()

    # ------------------------------------------------------------------
    # Profiles
    # ------------------------------------------------------------------
    def _build_profile_buttons(self):
        f = ttk.LabelFrame(self._content, text="Config Profiles")
        f.pack(fill="x", padx=8, pady=8)

        row1 = ttk.Frame(f)
        row1.pack(fill="x", padx=8, pady=(6, 2))

        ttk.Label(row1, text="Quick slot  (profile.json):").pack(side="left")
        ttk.Button(row1, text="Quick Save", command=self.save_profile).pack(side="left", padx=(8, 4))
        ttk.Button(row1, text="Quick Load", command=self.load_profile).pack(side="left", padx=(0, 4))

        row2 = ttk.Frame(f)
        row2.pack(fill="x", padx=8, pady=(2, 6))

        ttk.Label(row2, text="Named files:").pack(side="left")
        ttk.Button(row2, text="Save As…", command=self.save_profile_as).pack(side="left", padx=(8, 4))
        ttk.Button(row2, text="Load From…", command=self.load_profile_from).pack(side="left", padx=(0, 4))

        ttk.Label(row2,
                  text="Profiles are stored in the profiles folder. Load requires restart to fully apply.",
                  style="Caption.TLabel").pack(side="left", padx=12)

    def save_profile(self):
        """Quick-save to the default single slot (profiles/profile.json)."""
        try:
            self.apply()
            path = os.path.join(profiles_dir(), "profile.json")
            if os.path.exists(path):
                if not messagebox.askyesno("Confirm Overwrite", "Quick-save profile already exists. Overwrite?", parent=self):
                    return
            atomic_write_json(path, self.cfg)
            messagebox.showinfo("Saved", f"Profile saved:\n{path}", parent=self)
        except Exception as e:
            messagebox.showerror("Error", str(e), parent=self)

    def save_profile_as(self):
        """Save current config to a user-chosen named file."""
        from tkinter import filedialog
        try:
            self.apply()
            path = filedialog.asksaveasfilename(
                parent=self,
                title="Save Config Profile As",
                initialdir=profiles_dir(),
                defaultextension=".json",
                filetypes=[("JSON profile", "*.json"), ("All files", "*.*")],
            )
            if not path:
                return
            atomic_write_json(path, self.cfg)
            messagebox.showinfo("Saved", f"Profile saved:\n{path}", parent=self)
        except Exception as e:
            messagebox.showerror("Error", str(e), parent=self)

    def update_view(self):
        """Update status banner based on worker state and auto-read serials."""
        if not self.worker:
            return

        status = self.worker.status
        last_error = self.worker.last_error
        countdown = self.worker.get_reconnect_countdown()
        try:
            bus_health = self.worker.get_bus_health()
        except Exception:
            bus_health = {}

        try:
            latency = float((bus_health or {}).get("latency_ms", 0.0) or 0.0)
            ok_count = int((bus_health or {}).get("poll_ok", 0) or 0)
            fail_count = int((bus_health or {}).get("poll_fail", 0) or 0)
            bus_fails = int((bus_health or {}).get("bus_fail_count", 0) or 0)
            port_state = (bus_health or {}).get("port_available", None)
            if port_state is False:
                port_text = "COM missing"
            elif port_state is True:
                port_text = "COM present"
            else:
                port_text = "COM unchecked"
            self.comm_health_var.set(
                f"Bus: {(bus_health or {}).get('state', 'UNKNOWN')} | latency avg {latency:.0f} ms | reads OK/fail {ok_count}/{fail_count} | bus fail streak {bus_fails} | {port_text}"
            )
        except Exception:
            pass

        # Auto-scan and auto-read serials when first connected
        if status == "CONNECTED" and self._last_worker_status != "CONNECTED":
            if self._pending_auto_scan and not self._auto_scan_in_progress:
                log.info("Connection established - starting auto-scan for meters")
                self._pending_auto_scan = False
                self._start_auto_scan()
            elif not self._serials_read and not self._reading_serials:
                log.info("Connection established - auto-reading meter serials")
                self._auto_read_serials()

        if status != "CONNECTED" and self._last_worker_status == "CONNECTED":
            self._serials_read = False

        self._last_worker_status = status

        self._update_serial_display()
        self._update_meter_status()
        self._update_connection_buttons(status)

        # Banner message (text only)
        show_banner = False
        message = ""

        if status == "DISCONNECTED" and last_error:
            show_banner = True
            message = f"Error: {last_error}"
        elif status == "COM_BUSY":
            show_banner = True
            if countdown > 0:
                message = f"COM busy - close other app using the port (retry in {countdown:.0f}s)"
            else:
                message = "COM busy - close other app using the port (reconnecting...)"
        elif status == "ERROR":
            show_banner = True
            if countdown > 0:
                message = f"Error: {last_error} (reconnecting in {countdown:.0f}s)"
            else:
                message = f"Error: {last_error} (reconnecting...)"
        elif status == "CONNECTING":
            show_banner = True
            message = "Connecting..."
        elif status == "CONNECTED":
            show_banner = False

        if show_banner:
            self.status_var.set(message)
            self.status_label.pack(fill="x", padx=0, pady=(0, 4))
        else:
            self.status_label.pack_forget()

    def _update_connection_buttons(self, status: str):
        """SCADA-friendly: state + text only (no bg painting)."""
        if not hasattr(self, 'btn_connect') or not hasattr(self, 'btn_disconnect'):
            return

        if status == "CONNECTED":
            self.btn_connect.configure(text="Connected", state="disabled")
            self.btn_disconnect.configure(state="normal")
        elif status == "CONNECTING":
            self.btn_connect.configure(text="Connecting...", state="disabled")
            self.btn_disconnect.configure(state="normal")
        else:
            # DISCONNECTED / ERROR / COM_BUSY etc.
            self.btn_connect.configure(text="Connect", state="normal")
            self.btn_disconnect.configure(state="disabled")

    def _update_meter_status(self):
        """Update meter status indicators in the table."""
        for row_data in self.rows:
            if len(row_data) < 11:
                continue

            m = row_data[0]
            lbl_status = row_data[10]

            # Prefer quality flags (industrial-grade visibility)
            q = getattr(m, "quality", None)
            qv = (q.value if hasattr(q, "value") else str(q or "")).upper()

            if m.enabled and qv == "GOOD":
                lbl_status.config(text="Online", foreground="green")
            elif m.enabled and qv == "STALE":
                lbl_status.config(text="Stale", foreground="orange")
            elif m.enabled and (qv == "COMM_LOST" or m.comm_failure):
                lbl_status.config(text="Offline", foreground="red")
            elif m.enabled and not m.data_valid:
                lbl_status.config(text="Waiting", foreground="orange")
            else:
                lbl_status.config(text="--", foreground="gray")

    def load_profile(self):
        """Quick-load from the default single slot (profiles/profile.json)."""
        try:
            path = os.path.join(profiles_dir(), "profile.json")
            if not os.path.exists(path):
                messagebox.showwarning("Not Found", "No quick-save profile found.\nUse 'Save Profile' first.", parent=self)
                return
            self._apply_loaded_profile(path)
        except Exception as e:
            messagebox.showerror("Error", str(e), parent=self)

    def load_profile_from(self):
        """Load a named profile file chosen via file picker."""
        from tkinter import filedialog
        try:
            path = filedialog.askopenfilename(
                parent=self,
                title="Load Config Profile",
                initialdir=profiles_dir(),
                filetypes=[("JSON profile", "*.json"), ("All files", "*.*")],
            )
            if not path:
                return
            self._apply_loaded_profile(path)
        except Exception as e:
            messagebox.showerror("Error", str(e), parent=self)

    def _apply_loaded_profile(self, path: str) -> None:
        """Load JSON from path, deep-merge into cfg, show result message."""
        from config.store import merge_defaults
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        # Deep-merge loaded data into current cfg so new keys in schema still
        # get their defaults. cfg.update() would silently drop defaults.
        merged = merge_defaults(self.cfg, data)
        self.cfg.clear()
        self.cfg.update(merged)
        messagebox.showinfo(
            "Loaded",
            f"Profile loaded from:\n{os.path.basename(path)}\n\nRestart the app to fully apply all changes.",
            parent=self,
        )
