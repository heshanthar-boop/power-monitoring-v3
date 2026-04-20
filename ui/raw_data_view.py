"""
Raw Modbus Register View
========================
Shows the raw 16-bit register values read from the selected meter.
Useful for diagnostics and verifying register map accuracy.
"""
from __future__ import annotations

import tkinter as tk
from tkinter import ttk
from typing import Optional

from ui.styles import get_theme


class RawDataTab(ttk.Frame):

    def __init__(self, parent, meters):
        super().__init__(parent)
        self.meters = meters
        self._build_ui()

    # ── build ─────────────────────────────────────────────────────────────────

    def _build_ui(self):
        t = get_theme()

        # Toolbar
        toolbar = ttk.Frame(self, style="App.Panel.TFrame")
        toolbar.pack(fill="x", padx=8, pady=(8, 4))

        ttk.Label(toolbar, text="RAW REGISTERS", style="H3.TLabel").pack(side="left", padx=(0, 16))

        ttk.Label(toolbar, text="Meter:", style="FieldLabel.TLabel").pack(side="left")
        meter_names = [m.name or f"Meter {m.meter_id}" for m in self.meters]
        self._meter_var = tk.StringVar(value=meter_names[0] if meter_names else "—")
        self._cmb_meter = ttk.Combobox(
            toolbar, textvariable=self._meter_var,
            values=meter_names, state="readonly", width=18,
        )
        self._cmb_meter.pack(side="left", padx=(4, 12))
        self._cmb_meter.bind("<<ComboboxSelected>>", lambda _e: self.update_view())

        self._status_var = tk.StringVar(value="")
        ttk.Label(toolbar, textvariable=self._status_var,
                  style="Muted.TLabel").pack(side="right", padx=8)

        # Table
        tbl = ttk.Frame(self)
        tbl.pack(fill="both", expand=True, padx=8, pady=(0, 8))

        cols = ("idx", "hex_addr", "raw_dec", "raw_hex")
        self.tree = ttk.Treeview(tbl, columns=cols, show="headings",
                                 selectmode="browse")
        self.tree.heading("idx",      text="Register #",  anchor="center")
        self.tree.heading("hex_addr", text="Hex Address", anchor="center")
        self.tree.heading("raw_dec",  text="Value (Dec)", anchor="center")
        self.tree.heading("raw_hex",  text="Value (Hex)", anchor="center")

        self.tree.column("idx",      width=100, minwidth=70,  stretch=False, anchor="center")
        self.tree.column("hex_addr", width=110, minwidth=80,  stretch=False, anchor="center")
        self.tree.column("raw_dec",  width=120, minwidth=80,  stretch=False, anchor="center")
        self.tree.column("raw_hex",  width=120, minwidth=80,  stretch=False, anchor="center")

        vsb = ttk.Scrollbar(tbl, orient="vertical",   command=self.tree.yview)
        hsb = ttk.Scrollbar(tbl, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)

        self.tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")
        tbl.grid_rowconfigure(0, weight=1)
        tbl.grid_columnconfigure(0, weight=1)

        # Hint
        ttk.Label(self,
                  text="Raw 16-bit Modbus holding register contents.  "
                       "Values are shown as read — no scaling applied.",
                  style="Dim.TLabel",
        ).pack(side="bottom", anchor="w", padx=8, pady=(0, 4))

    # ── update ────────────────────────────────────────────────────────────────

    def update_view(self):
        self.tree.delete(*self.tree.get_children())

        if not self.meters:
            self._status_var.set("No meters configured")
            return

        # Find selected meter
        name = self._meter_var.get()
        meter = next((m for m in self.meters
                      if (m.name or f"Meter {m.meter_id}") == name), None)
        if meter is None:
            meter = self.meters[0]

        regs = getattr(meter, "raw_regs", None)
        if not regs:
            self._status_var.set(f"{name} — no data yet")
            return

        for i, v in enumerate(regs[:512]):
            v_int = int(v) & 0xFFFF
            self.tree.insert("", "end", values=(
                i,
                f"0x{(40001 + i):04X}",
                v_int,
                f"0x{v_int:04X}",
            ))

        self._status_var.set(f"{name} — {len(regs[:512])} registers  |  "
                             f"Slave ID: {getattr(meter, 'slave_id', '?')}")
