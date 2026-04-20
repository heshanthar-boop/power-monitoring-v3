# ui/setup_tab.py
"""
Setup (400xx) Tab - Read/Write MFM384-C Configuration Parameters

Features:
- Read all setup parameters from device
- Edit values with validation
- Write individual or all changed values
- Reset commands (write-only)
- Thread-safe Modbus access via worker lock
"""
from __future__ import annotations
import threading
import time
import tkinter as tk
from tkinter import ttk
from tkinter import messagebox, simpledialog
from typing import Dict, Any, Optional

from core.register_map import SETUP_400XX_DEFS, SETUP_400XX_BY_NAME, SetupRegisterDef, ENDIANNESS_TO_CODE
from core.modbus_codec import (
    read_setup_400xx, write_setup_400xx, write_reset_command,
    format_setup_value, get_setup_range_desc, validate_setup_value
)
from utils.health import stale_seconds
from utils.logger import setup_logger
from utils.stale_override import set_stale_override

log = setup_logger("setup")


class SetupTab(ttk.Frame):
    """
    Setup (400xx) configuration tab.
    
    Provides UI for reading and writing MFM384-C holding registers.
    Uses worker.run_client_task() for thread-safe Modbus access.
    """
    
    def __init__(self, parent, cfg: dict, meters):
        super().__init__(parent, padding=8)
        self.cfg = cfg
        self.meters = meters
        self._sel_vars = {m.meter_id: tk.BooleanVar(value=bool(getattr(m,'enabled', True))) for m in meters}
        self.worker = None  # Set via set_worker()
        
        # Current setup values cache per meter
        self._cache: Dict[int, Dict[str, Any]] = {}
        
        # Edited values (not yet written)
        self._edits: Dict[int, Dict[str, Any]] = {}
        
        # Currently selected meter
        self._current_meter_id: Optional[int] = None
        self._filter_var = tk.StringVar(value="")

        # Read All runtime state
        self._read_all_active = False
        self._read_all_start_ts = 0.0
        self._read_all_override_job: Optional[str] = None
        self._progress_visible = False
        
        # Write safety (operator-proof): writes are locked by default.
        self._writes_unlocked_until = 0.0
        self._lock_tick_after_id = None
        self._build_ui()
        self._set_write_lock_state()
    
    def set_worker(self, worker):
        """Set the ModbusWorker reference for thread-safe Modbus access."""
        self.worker = worker
    
    def _build_ui(self):
        """Build the complete Setup tab UI."""
        # Top control bar
        top_frame = ttk.Frame(self)
        top_frame.pack(fill="x", padx=10, pady=8)
        
        # Title
        ttk.Label(top_frame, text="Setup (400xx) - Device Configuration", style="H2.TLabel").pack(side="left")
        
        # Meter selector
        meter_frame = ttk.Frame(top_frame)
        meter_frame.pack(side="right")
        
        ttk.Label(meter_frame, text="Meter:").pack(side="left", padx=(0, 5))
        
        self._meter_var = tk.StringVar()
        meter_names = [f"Meter {m.meter_id} (ID={m.slave_id})" for m in self.meters]
        self._meter_combo = ttk.Combobox(
            meter_frame, 
            textvariable=self._meter_var,
            values=meter_names,
            state="readonly",
            width=25
        )
        self._meter_combo.pack(side="left")
        if meter_names:
            self._meter_combo.current(0)
            self._current_meter_id = self.meters[0].meter_id if self.meters else None
        self._meter_combo.bind("<<ComboboxSelected>>", self._on_meter_changed)
        
        # Button bar
        btn_frame = ttk.Frame(self)
        btn_frame.pack(fill="x", padx=10, pady=(0, 8))

        # Read-All meter selection (operator-proof: only reads checked meters)
        sel = ttk.Frame(btn_frame)
        sel.pack(side="right", padx=4)
        ttk.Label(sel, text="Read All Meters:", font=("Segoe UI", 9)).pack(side="left", padx=(0,6))
        for m in self.meters:
            try:
                lbl = f"M{m.meter_id}"
                ttk.Checkbutton(sel, text=lbl, variable=self._sel_vars[m.meter_id]).pack(side="left", padx=2)
            except Exception:
                pass
        
        self.btn_unlock = ttk.Button(btn_frame, text="Unlock Writes (60s)", style="Accent.TButton", command=self._on_unlock_writes)
        self.btn_unlock.pack(side="left", padx=2)
        ttk.Button(btn_frame, text="Read Selected", style="Primary.TButton", command=self._on_read_all).pack(side="left", padx=2)
        self.btn_write = ttk.Button(btn_frame, text="Apply Changed", style="Primary.TButton", command=self._on_write_changed)
        self.btn_write.pack(side="left", padx=2)
        
        ttk.Separator(btn_frame, orient="vertical").pack(side="left", fill="y", padx=10)
        
        # Reset command buttons
        ttk.Label(btn_frame, text="Reset Commands:", font=("Segoe UI", 9)).pack(side="left", padx=(0, 5))
        
        reset_commands = [
            ("Reset Energy", "Reset Active Energy"),
            ("Reset Demand", "Reset Active Power Max DMD"),
            ("Reset Run Hr", "Reset Run Hour"),
            ("Factory Reset", "Factory Default"),
        ]
        self._reset_buttons = []
        for btn_text, cmd_name in reset_commands:
            btn = ttk.Button(
                btn_frame,
                text=btn_text,
                command=lambda n=cmd_name: self._on_reset_command(n),
                width=12
            )
            btn.pack(side="left", padx=2)
            self._reset_buttons.append(btn)
        
        # Search / filter
        filter_row = ttk.Frame(self)
        filter_row.pack(fill="x", padx=10, pady=(0, 6))
        ttk.Label(filter_row, text="Filter:", style="FieldLabel.TLabel").pack(side="left", padx=(0, 6))
        self._filter_entry = ttk.Entry(filter_row, textvariable=self._filter_var, width=32)
        self._filter_entry.pack(side="left")
        self._filter_var.trace_add("write", lambda *_: self._populate_tree())
        ttk.Label(filter_row, text="Double-click a row to edit. Highlighted rows contain pending changes.", style="Dim.TLabel").pack(side="left", padx=12)

        # Status bar
        self._status_var = tk.StringVar(value="Ready - select one or more meters and click Read Selected")
        status_lbl = ttk.Label(self, textvariable=self._status_var, font=("Segoe UI", 9))
        status_lbl.pack(fill="x", padx=10, pady=(0, 5))
        self._progress_bar = ttk.Progressbar(self, mode="indeterminate")
        
        # Main content area with Treeview
        content_frame = ttk.Frame(self)
        content_frame.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        
        # Treeview for parameters
        columns = ("address", "name", "value", "range", "edit")
        self._tree = ttk.Treeview(content_frame, columns=columns, show="headings", height=20)
        
        self._tree.heading("address", text="Address")
        self._tree.heading("name", text="Parameter")
        self._tree.heading("value", text="Current Value")
        self._tree.heading("range", text="Range / Options")
        self._tree.heading("edit", text="New Value")
        
        self._tree.column("address", width=80, anchor="center")
        self._tree.column("name", width=220, anchor="w")
        self._tree.column("value", width=180, anchor="e")
        self._tree.column("range", width=280, anchor="w")
        self._tree.column("edit", width=120, anchor="center")
        
        # Scrollbar
        scrollbar = ttk.Scrollbar(content_frame, orient="vertical", command=self._tree.yview)
        self._tree.configure(yscrollcommand=scrollbar.set)
        
        self._tree.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")
        
        # Bind double-click for editing
        self._tree.bind("<Double-1>", self._on_double_click)
        self._tree.tag_configure("edited", background="#243246")
        self._tree.tag_configure("writeonly", foreground="#8e9bb3")

        # Populate initial rows
        self._populate_tree()
    
    def _get_selected_meter(self) -> Optional[Any]:
        """Get the currently selected meter object."""
        idx = self._meter_combo.current()
        if idx >= 0 and idx < len(self.meters):
            return self.meters[idx]
        return None
    
    def _on_meter_changed(self, event=None):
        """Handle meter selection change."""
        meter = self._get_selected_meter()
        if meter:
            self._current_meter_id = meter.meter_id
            self._populate_tree()
            self._status_var.set(f"Selected Meter {meter.meter_id} - click Read Selected to load values")
    
    def _populate_tree(self):
        """Populate tree with setup parameters."""
        # Clear existing items
        for item in self._tree.get_children():
            self._tree.delete(item)
        
        meter = self._get_selected_meter()
        meter_id = meter.meter_id if meter else 0
        
        # Get cached values for this meter
        cached = self._cache.get(meter_id, {})
        edits = self._edits.get(meter_id, {})
        
        # Group parameters by category
        categories = {
            "Device Configuration": ["Password", "Network Selection", "CT Secondary", "CT Primary", 
                                     "PT Secondary", "PT Primary", "Slave ID"],
            "Communication": ["Baud Rate", "Parity", "Stop Bit", "Change Endianness"],
            "Display": ["Backlight OFF Time", "Auto Mode Pages"],
            "Demand Settings": ["Demand Interval Method", "Demand Interval Duration", "Demand Interval Length"],
            "Pulse Output": ["Pulse Duration", "Pulse Weight"],
            "Reset Commands": [],  # Will be populated dynamically
            "Page Sequence": [],   # Will be populated dynamically
        }
        
        # Find reset commands and page sequences
        for reg_def in SETUP_400XX_DEFS:
            if reg_def.write_only:
                categories["Reset Commands"].append(reg_def.name)
            elif reg_def.name.startswith("Page Seq"):
                categories["Page Sequence"].append(reg_def.name)
        
        needle = (self._filter_var.get() or "").strip().lower()

        # Add all parameters to tree
        for reg_def in SETUP_400XX_DEFS:
            address = f"40{reg_def.offset:03d}"
            name = reg_def.name
            range_desc = get_setup_range_desc(name)
            if needle and needle not in name.lower() and needle not in address.lower() and needle not in range_desc.lower():
                continue

            raw_value = cached.get(name, "—")
            display_value = format_setup_value(name, raw_value) if raw_value != "—" else "—"

            edit_value = edits.get(name, "")
            if edit_value != "":
                edit_value = f"→ {edit_value}"

            tags = []
            if reg_def.write_only:
                tags.append("writeonly")
            if name in edits:
                tags.append("edited")

            self._tree.insert("", "end", iid=name, values=(
                address, name, display_value, range_desc, edit_value
            ), tags=tuple(tags))
    
    def _on_read_all(self):
        """Read all setup parameters (400xx) from device(s).

        Behavior:
        - If one or more meters are checked in the 'Read All' selection box: read those meters sequentially.
        - If none checked: read the currently selected meter only.
        """
        log.info("Read All clicked!")

        if not self.worker:
            messagebox.showerror("Error", "Modbus worker not available.")
            return
        if not self.worker.connected:
            messagebox.showwarning("Not Connected", "Modbus not connected. Please connect first.")
            return

        checked = [m for m in self.meters if self._sel_vars.get(m.meter_id) and self._sel_vars[m.meter_id].get()]
        targets = checked

        if not targets:
            messagebox.showwarning("No Meter", "Please select a meter (or check at least one meter) first.")
            return

        self._show_progress(True)
        self._begin_read_all_override()

        def read_one(meter):
            self._status_var.set(f"Reading setup from Meter {meter.meter_id}...")
            log.info(f"Starting setup read for meter {meter.meter_id}, slave_id={meter.slave_id}")
            result = self.worker.run_client_task(
                read_setup_400xx,
                slave_id=meter.slave_id,
                base_address=0
            )
            log.info(f"Read complete for meter {meter.meter_id}, got {len(result)} parameters")
            return result

        def read_task():
            try:
                for meter in targets:
                    if not meter:
                        continue
                    result = read_one(meter)
                    # Update cache + UI incrementally from main thread
                    self.after(0, lambda r=result, mid=meter.meter_id: self._on_read_complete(mid, r))
                self.after(0, lambda: self._status_var.set("Read All complete"))
            except Exception as e:
                log.error(f"Read setup failed: {e}")
                import traceback
                log.error(traceback.format_exc())
                self.after(0, lambda err=str(e): self._on_read_error(err))
            finally:
                self.after(0, self._finalize_read_all)

        threading.Thread(target=read_task, daemon=True).start()
    def _on_read_complete(self, meter_id: int, result: Dict[str, Any]):
        """Handle successful read completion."""
        self._cache[meter_id] = result
        self._edits[meter_id] = {}  # Clear pending edits
        self._populate_tree()
        self._status_var.set(f"Read complete - {len(result)} parameters loaded")
        log.info(f"Read setup complete for meter {meter_id}")
    
    def _on_read_error(self, error_msg: str):
        """Handle read error."""
        self._status_var.set(f"Read failed: {error_msg}")
        messagebox.showerror("Read Error", f"Failed to read setup:\n{error_msg}")
    
    def _show_progress(self, show: bool):
        if show:
            if not self._progress_visible:
                self._progress_bar.pack(fill="x", padx=10, pady=(0, 6))
                self._progress_visible = True
            self._progress_bar.start(60)
        elif self._progress_visible:
            self._progress_bar.stop()
            self._progress_bar.pack_forget()
            self._progress_visible = False

    def _begin_read_all_override(self):
        self._read_all_active = True
        self._read_all_start_ts = time.time()
        self._schedule_read_all_override()

    def _schedule_read_all_override(self):
        if not self._read_all_active:
            return
        base = stale_seconds(self.cfg)
        elapsed = max(0.0, time.time() - self._read_all_start_ts)
        override = max(base, elapsed + 5.0)
        set_stale_override(override)
        self._read_all_override_job = self.after(1000, self._schedule_read_all_override)

    def _end_read_all_override(self):
        self._read_all_active = False
        if self._read_all_override_job:
            try:
                self.after_cancel(self._read_all_override_job)
            except Exception:
                pass
            self._read_all_override_job = None
        set_stale_override(None)

    def _finalize_read_all(self):
        self._show_progress(False)
        self._end_read_all_override()
    
    def _on_double_click(self, event):
        """Handle double-click to edit a value."""
        item = self._tree.identify_row(event.y)
        if not item:
            return
        
        reg_def = SETUP_400XX_BY_NAME.get(item)
        if not reg_def:
            return
        
        # Don't edit write-only reset commands
        if reg_def.write_only:
            messagebox.showinfo(
                "Reset Command",
                f"'{reg_def.name}' is a reset command.\nUse the Reset buttons above to execute."
            )
            return
        
        meter = self._get_selected_meter()
        if not meter:
            return
        
        meter_id = meter.meter_id
        cached = self._cache.get(meter_id, {})
        current_value = cached.get(item, None)
        
        # Determine input method based on dtype
        if reg_def.dtype == "enum" and reg_def.enum_map:
            # Show dropdown for enum
            self._edit_enum_value(item, reg_def, current_value, meter_id)
        else:
            # Show text entry for other values
            self._edit_numeric_value(item, reg_def, current_value, meter_id)
    
    def _edit_enum_value(self, name: str, reg_def: SetupRegisterDef, current_value: Any, meter_id: int):
        """Edit an enum value with dropdown dialog."""
        options = list(reg_def.enum_map.values())
        
        current_label = ""
        if isinstance(current_value, dict):
            current_label = current_value.get("label", "")
        
        # Create simple dialog
        dialog = tk.Toplevel(self)
        dialog.title(f"Edit {name}")
        dialog.geometry("350x150")
        dialog.transient(self)
        dialog.grab_set()
        
        ttk.Label(dialog, text=f"Select value for {name}:").pack(pady=10)
        
        var = tk.StringVar(value=current_label)
        combo = ttk.Combobox(dialog, textvariable=var, values=options, state="readonly", width=40)
        combo.pack(pady=5)
        
        def on_ok():
            selected = var.get()
            if selected:
                # Find the code for this label
                for code, label in reg_def.enum_map.items():
                    if label == selected:
                        if meter_id not in self._edits:
                            self._edits[meter_id] = {}
                        self._edits[meter_id][name] = code
                        self._populate_tree()
                        break
            dialog.destroy()
        
        btn_frame = ttk.Frame(dialog)
        btn_frame.pack(pady=20)
        ttk.Button(btn_frame, text="OK", command=on_ok).pack(side="left", padx=5)
        ttk.Button(btn_frame, text="Cancel", command=dialog.destroy).pack(side="left", padx=5)
    
    def _edit_numeric_value(self, name: str, reg_def: SetupRegisterDef, current_value: Any, meter_id: int):
        """Edit a numeric value with text entry."""
        current_str = ""
        if current_value is not None and not isinstance(current_value, str):
            current_str = str(current_value)
        
        prompt = f"Enter new value for {name}:"
        if reg_def.min_val is not None and reg_def.max_val is not None:
            if reg_def.scale:
                prompt += f"\nRange: {reg_def.min_val/reg_def.scale:.2f} - {reg_def.max_val/reg_def.scale:.2f}"
            else:
                prompt += f"\nRange: {reg_def.min_val} - {reg_def.max_val}"
        
        result = simpledialog.askstring(
            f"Edit {name}",
            prompt,
            initialvalue=current_str,
            parent=self
        )
        
        if result is not None:
            # Validate before accepting
            cached = self._cache.get(meter_id, {})
            valid, error_msg, _ = validate_setup_value(name, result, cached)
            
            if valid:
                if meter_id not in self._edits:
                    self._edits[meter_id] = {}
                self._edits[meter_id][name] = result
                self._populate_tree()
            else:
                messagebox.showerror("Validation Error", error_msg)
    
    def _on_write_changed(self):
        if not self._writes_unlocked():
            messagebox.showwarning("Writes Locked", "Writes are locked. Click 'Unlock Writes (60s)' first.")
            return
        """Write all changed values to device."""
        meter = self._get_selected_meter()
        if not meter:
            messagebox.showwarning("No Meter", "Please select a meter first.")
            return
        
        meter_id = meter.meter_id
        edits = self._edits.get(meter_id, {})
        
        if not edits:
            messagebox.showinfo("No Changes", "No values have been changed.")
            return
        
        if not self.worker or not self.worker.connected:
            messagebox.showwarning("Not Connected", "Modbus not connected.")
            return
        
        # Confirm
        edit_list = "\n".join(f"• {k} = {v}" for k, v in edits.items())
        if not messagebox.askyesno("Confirm Write", f"Write these changes?\n\n{edit_list}"):
            return
        
        self._status_var.set("Writing changes...")
        
        def write_task():
            results = []
            cached = self._cache.get(meter_id, {})
            
            for name, value in edits.items():
                try:
                    success, msg = self.worker.run_client_task(
                        write_setup_400xx,
                        slave_id=meter.slave_id,
                        name=name,
                        value=value,
                        base_address=0,
                        current_setup=cached
                    )
                    results.append((name, success, msg))
                    
                    # Special handling for endianness change
                    if success and name == "Change Endianness":
                        self._update_endianness_config(value)
                        
                except Exception as e:
                    results.append((name, False, str(e)))
            
            self.after(0, lambda: self._on_write_complete(meter_id, results))
        
        threading.Thread(target=write_task, daemon=True).start()
    
    def _update_endianness_config(self, value: Any):
        """Update app config after endianness change."""
        try:
            if isinstance(value, int):
                code = value
            else:
                code = ENDIANNESS_TO_CODE.get(str(value), 0)
            
            new_order = "ABCD" if code == 1 else "CDAB"
            if "serial" not in self.cfg:
                self.cfg["serial"] = {}
            self.cfg["serial"]["word_order"] = new_order
            log.info(f"Updated word_order config to {new_order}")
        except Exception as e:
            log.error(f"Failed to update endianness config: {e}")
    
    def _on_write_complete(self, meter_id: int, results: list):
        """Handle write completion."""
        success_count = sum(1 for _, s, _ in results if s)
        fail_count = len(results) - success_count
        
        if fail_count == 0:
            self._status_var.set(f"Write complete - {success_count} values written")
            self._edits[meter_id] = {}
            # Re-read to confirm
            self._on_read_all()
        else:
            errors = "\n".join(f"• {n}: {m}" for n, s, m in results if not s)
            self._status_var.set(f"Write partial - {success_count} OK, {fail_count} failed")
            messagebox.showwarning("Write Errors", f"Some writes failed:\n\n{errors}")
    
    def _on_reset_command(self, command_name: str):
        """Execute a reset command."""
        meter = self._get_selected_meter()
        if not meter:
            messagebox.showwarning("No Meter", "Please select a meter first.")
            return
        
        if not self.worker or not self.worker.connected:
            messagebox.showwarning("Not Connected", "Modbus not connected.")
            return
        
        # Confirm dangerous commands
        if command_name == "Factory Default":
            if not messagebox.askyesno(
                "Confirm Factory Reset",
                "⚠️ This will reset ALL device settings to factory defaults!\n\n"
                "Are you sure you want to continue?",
                icon="warning"
            ):
                return
        else:
            if not messagebox.askyesno(
                "Confirm Reset",
                f"Execute '{command_name}'?\n\nThis cannot be undone."
            ):
                return
        
        self._status_var.set(f"Executing {command_name}...")
        
        def reset_task():
            try:
                success, msg = self.worker.run_client_task(
                    write_reset_command,
                    slave_id=meter.slave_id,
                    name=command_name,
                    base_address=0
                )
                self.after(0, lambda: self._on_reset_complete(command_name, success, msg))
            except Exception as e:
                self.after(0, lambda: self._on_reset_complete(command_name, False, str(e)))
        
        threading.Thread(target=reset_task, daemon=True).start()
    
    def _on_reset_complete(self, command_name: str, success: bool, message: str):
        """Handle reset command completion."""
        if success:
            self._status_var.set(f"{command_name} executed successfully")
            messagebox.showinfo("Success", message)
            # Re-read if it was an energy/demand reset
            if "Energy" in command_name or "DMD" in command_name:
                self._on_read_all()
        else:
            self._status_var.set(f"{command_name} failed: {message}")
            messagebox.showerror("Error", f"Failed to execute {command_name}:\n{message}")
    
    def update_view(self):
        """Called periodically by main window (optional refresh)."""
        pass  # No periodic updates needed for setup tab

    # ----- Write safety (operator-proof) -----
    def _writes_unlocked(self) -> bool:
        try:
            return time.time() < float(self._writes_unlocked_until or 0.0)
        except Exception:
            return False

    def _set_write_lock_state(self):
        """Enable/disable all write actions."""
        unlocked = self._writes_unlocked()
        try:
            if hasattr(self, "btn_write"):
                self.btn_write.configure(state=("normal" if unlocked else "disabled"))
        except Exception:
            pass
        try:
            if hasattr(self, "_reset_buttons"):
                for b in self._reset_buttons:
                    try:
                        b.configure(state=("normal" if unlocked else "disabled"))
                    except Exception:
                        pass
        except Exception:
            pass
        try:
            if hasattr(self, "btn_unlock"):
                self.btn_unlock.configure(text=("Writes UNLOCKED" if unlocked else "Unlock Writes (60s)"))
        except Exception:
            pass

    def _tick_write_lock(self):
        try:
            self._lock_tick_after_id = None
            self._set_write_lock_state()
            if self._writes_unlocked():
                self._lock_tick_after_id = self.after(500, self._tick_write_lock)
        except Exception:
            return

    def _on_unlock_writes(self):
        # Explicit operator action to allow writes for a short window.
        try:
            ok = messagebox.askyesno(
                "Unlock Writes",
                "This will enable WRITE/RESET actions for 60 seconds.\n\nProceed?"
            )
            if not ok:
                return
            self._writes_unlocked_until = time.time() + 60.0
            self._set_write_lock_state()
            if self._lock_tick_after_id:
                try:
                    self.after_cancel(self._lock_tick_after_id)
                except Exception:
                    pass
            self._lock_tick_after_id = self.after(250, self._tick_write_lock)
            self._status_var.set("Writes unlocked for 60 seconds. Apply changes carefully.")
        except Exception:
            return

