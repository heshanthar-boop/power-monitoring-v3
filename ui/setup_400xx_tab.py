"""
Setup (400xx) tab for configuring Selec MFM384-C holding registers
"""
import tkinter as tk
from tkinter import ttk, messagebox

from utils.logger import setup_logger
from utils.log_once import log_once

logger = setup_logger("setup400xx")
import threading
from core.register_map import SETUP_400XX_DEFS, SETUP_400XX_BY_NAME
from core.modbus_codec import read_setup_400xx, write_setup_400xx, u32_to_regs
from utils.logger import setup_logger

log = setup_logger("setup_400xx")


class SetupTab(ttk.Frame):
    def __init__(self, parent, cfg, meters):
        super().__init__(parent)
        self.cfg = cfg
        self.meters = meters
        self.worker = None  # Will be set by main window
        
        self.current_values = {}  # {name: value}
        self.edited_values = {}   # {name: value} - tracks user edits
        self.is_reading = False
        
        self._build_ui()

    def _build_ui(self):
        """Build the Setup tab UI"""
        # Top controls
        top_frame = ttk.Frame(self)
        top_frame.pack(fill="x", padx=10, pady=10)

        ttk.Label(top_frame, text="Select Meter:", style="Header.TLabel").pack(side="left", padx=5)
        
        meter_names = [f"{m.name} (ID: {m.meter_id})" for m in self.meters]
        self.meter_var = tk.StringVar(value=meter_names[0] if meter_names else "")
        meter_combo = ttk.Combobox(top_frame, textvariable=self.meter_var, values=meter_names, state="readonly", width=25)
        meter_combo.pack(side="left", padx=5)

        ttk.Button(top_frame, text="Read Setup", command=self._on_read_setup).pack(side="left", padx=5)
        ttk.Button(top_frame, text="Write All Changed", command=self._on_write_all).pack(side="left", padx=5)

        # Status label
        self.status_var = tk.StringVar(value="Ready")
        ttk.Label(top_frame, textvariable=self.status_var, foreground="blue").pack(side="left", padx=20)

        # Table frame with scrollbars
        table_frame = ttk.Frame(self)
        table_frame.pack(fill="both", expand=True, padx=10, pady=10)

        # Create Treeview
        columns = ("Offset", "Name", "Current", "Unit", "Input", "Action")
        self.tree = ttk.Treeview(table_frame, columns=columns, height=20, show="headings")
        
        self.tree.column("Offset", width=70, anchor="w")
        self.tree.column("Name", width=250, anchor="w")
        self.tree.column("Current", width=120, anchor="center")
        self.tree.column("Unit", width=80, anchor="w")
        self.tree.column("Input", width=150, anchor="w")
        self.tree.column("Action", width=80, anchor="center")

        self.tree.heading("Offset", text="Address")
        self.tree.heading("Name", text="Parameter Name")
        self.tree.heading("Current", text="Current Value")
        self.tree.heading("Unit", text="Unit/Range")
        self.tree.heading("Input", text="New Value")
        self.tree.heading("Action", text="Write")

        # Scrollbars
        vsb = ttk.Scrollbar(table_frame, orient="vertical", command=self.tree.yview)
        hsb = ttk.Scrollbar(table_frame, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscroll=vsb.set, xscroll=hsb.set)

        self.tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")

        table_frame.grid_rowconfigure(0, weight=1)
        table_frame.grid_columnconfigure(0, weight=1)

        # Store entry widgets for editing
        self.entry_widgets = {}
        
        # Initialize rows
        self._populate_rows()

    def _populate_rows(self):
        """Populate table rows from SETUP_400XX_DEFS"""
        for item in self.tree.get_children():
            self.tree.delete(item)

        for i, reg_def in enumerate(SETUP_400XX_DEFS):
            addr_str = f"40{reg_def.offset:03d}"
            unit_str = self._get_unit_string(reg_def)
            
            item_id = self.tree.insert("", "end", iid=str(i), values=(
                addr_str,
                reg_def.name,
                "---",  # Will be updated after read
                unit_str,
                "",  # Input field (not used directly, will be handled via focus)
                "..."  # Action button
            ))
            
            # Bind row selection to show input field
            self.tree.bind("<Double-1>", self._on_row_double_click)

    def _get_unit_string(self, reg_def) -> str:
        """Get unit/range description for a register"""
        if reg_def.enum_map:
            labels = ", ".join(f"{k}:{v}" for k, v in sorted(reg_def.enum_map.items()))
            return f"Enum: {labels}"
        elif reg_def.min_val is not None and reg_def.max_val is not None:
            return f"{reg_def.min_val}..{reg_def.max_val}"
        else:
            return ""

    def _get_meter_id(self) -> int:
        """Extract meter ID from current selection"""
        selection = self.meter_var.get()
        try:
            # Extract ID from "Meter X (ID: Y)"
            id_part = selection.split("ID: ")[1].rstrip(")")
            return int(id_part)
        except Exception as e:
            log_once(logger, "parse_meter_id", "warning", f"Failed to parse selected meter id '{selection}': {e}", cooldown_sec=30)
            return 1

    def _on_read_setup(self):
        """Read all setup parameters from selected meter"""
        if not self.meters:
            messagebox.showerror("Error", "No meters configured")
            return

        meter_id = self._get_meter_id()
        self.status_var.set("Reading...")
        self.update_idletasks()

        def read_task():
            try:
                meter = next((m for m in self.meters if m.meter_id == meter_id), None)
                if not meter:
                    raise ValueError(f"Meter {meter_id} not found")

                if not hasattr(self, 'worker') or self.worker is None:
                    raise RuntimeError("Modbus worker not available")

                if not getattr(self.worker, "connected", False):
                    raise RuntimeError("Modbus not connected")

                values = self.worker.run_client_task(read_setup_400xx, slave_id=meter.slave_id)
                self.current_values = values
                self.edited_values = {}
                
                # Update table with values
                self.after(0, self._update_table_values)
                self.after(0, lambda: self.status_var.set("Ready"))

            except Exception as e:
                log.error(f"Read setup error: {e}")
                self.after(0, lambda: messagebox.showerror("Read Error", str(e)))
                self.after(0, lambda: self.status_var.set(f"Error: {str(e)[:30]}"))

        threading.Thread(target=read_task, daemon=True).start()

    def _update_table_values(self):
        """Update table display with current values"""
        for i, reg_def in enumerate(SETUP_400XX_DEFS):
            value = self.current_values.get(reg_def.name, "---")
            
            # Format value
            if isinstance(value, dict):
                # Enum with code and label
                display = f"{value.get('label', '?')} ({value.get('code', '?')})"
            elif isinstance(value, float):
                display = f"{value:.2f}"
            elif isinstance(value, str) and value.startswith("Error"):
                display = value
            else:
                display = str(value)

            current_values = self.tree.item(str(i), "values")
            self.tree.item(str(i), values=(
                current_values[0],  # addr
                current_values[1],  # name
                display,  # current value
                current_values[3],  # unit
                "",  # input (stay empty)
                "Write"  # action
            ))

    def _on_row_double_click(self, event):
        """Handle double-click to edit a row"""
        item = self.tree.selection()
        if not item:
            return

        item_id = item[0]
        idx = int(item_id)
        if idx >= len(SETUP_400XX_DEFS):
            return

        reg_def = SETUP_400XX_DEFS[idx]
        current_val = self.current_values.get(reg_def.name, "")

        # Show input dialog
        if reg_def.dtype == "enum" and reg_def.enum_map:
            # Combobox for enum
            labels = list(reg_def.enum_map.values())
            selected = tk.simpledialog.askstring(
                f"Edit {reg_def.name}",
                f"Choose value:\n{', '.join(labels)}",
                initialvalue=str(current_val)
            )
            if selected:
                self.edited_values[reg_def.name] = selected
        else:
            # Text entry for numeric
            selected = tk.simpledialog.askstring(
                f"Edit {reg_def.name}",
                f"Enter value ({reg_def.min_val or 0}..{reg_def.max_val or 65535}):",
                initialvalue=str(current_val)
            )
            if selected:
                try:
                    val = float(selected) if reg_def.scale else int(selected)
                    self.edited_values[reg_def.name] = val
                except ValueError:
                    messagebox.showerror("Invalid Input", "Please enter a valid number")

    def _on_write_all(self):
        """Write all edited values to device"""
        if not self.edited_values:
            messagebox.showinfo("No Changes", "No parameters have been edited")
            return

        meter_id = self._get_meter_id()
        meter = next((m for m in self.meters if m.meter_id == meter_id), None)
        if not meter:
            messagebox.showerror("Error", f"Meter {meter_id} not found")
            return

        self.status_var.set("Writing...")
        self.update_idletasks()

        def write_task():
            try:
                if not hasattr(self, 'worker') or self.worker is None:
                    raise RuntimeError("Modbus worker not available")

                if not getattr(self.worker, "connected", False):
                    raise RuntimeError("Modbus not connected")

                for name, value in self.edited_values.items():
                    reg_def = SETUP_400XX_BY_NAME.get(name)
                    if not reg_def:
                        continue

                    # Validate
                    if reg_def.min_val is not None and value < reg_def.min_val:
                        raise ValueError(f"{name}: value {value} < min {reg_def.min_val}")
                    if reg_def.max_val is not None and value > reg_def.max_val:
                        raise ValueError(f"{name}: value {value} > max {reg_def.max_val}")

                    # Prepare write value
                    if reg_def.scale:
                        write_val = int(value * reg_def.scale)
                    else:
                        write_val = int(value) if isinstance(value, (int, float)) else value

                    # Convert to register format
                    if reg_def.nregs == 2:
                        regs = u32_to_regs(write_val)
                    else:
                        regs = write_val

                    success, msg = self.worker.run_client_task(
                        write_setup_400xx,
                        slave_id=meter.slave_id,
                        name=name,
                        value=value,
                        base_address=0,
                        current_setup=self.current_values,
                    )
                    if not success:
                        raise RuntimeError(msg)
                    log.info(f"Wrote {name}={write_val}")

                self.edited_values = {}
                self.after(0, self._on_read_setup)  # Re-read to verify
                self.after(0, lambda: messagebox.showinfo("Success", "All changes written successfully"))

            except Exception as e:
                log.error(f"Write setup error: {e}")
                self.after(0, lambda: messagebox.showerror("Write Error", str(e)))
                self.after(0, lambda: self.status_var.set(f"Error: {str(e)[:30]}"))

        threading.Thread(target=write_task, daemon=True).start()

    def update_view(self):
        """Called periodically to update UI (if needed)"""
        pass


# Import at end to avoid circular dependency
import tkinter.simpledialog
