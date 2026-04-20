from __future__ import annotations

import re
import time
import tkinter as tk
from tkinter import ttk

from utils.health import get_meter_age, meter_is_fresh, stale_seconds
from ui.ux_labels import plain_meter_quality
from ui.styles import get_theme
from ui import pro_chrome


class StatusTab(ttk.Frame):
    """Operator-grade status page (IoTaWatt-style, but SCADA-safe).

    Purpose:
    - single truth page for comm health + key values
    - shows TOTAL + per-meter rows
    - never blocks UI (no Modbus calls; reads only from in-memory meter models)

    Required inputs:
    - meters: list of MeterModel
    - aggregator: TotalAggregator
    - selector: MeterSelector (for show_total + selected meters)
    - cfg: config dict
    """

    def __init__(self, parent, meters, aggregator, selector, cfg: dict, on_open_graphs=None):
        super().__init__(parent)
        self.meters = meters or []
        self.aggregator = aggregator
        self.selector = selector
        self.cfg = cfg
        self.on_open_graphs = on_open_graphs

        self._build_ui()

    def _build_ui(self):
        t = get_theme()
        self._root = pro_chrome.page(self)
        top = pro_chrome.header(
            self._root,
            "Meter Status",
            "Live quality, age, energy and electrical values for every configured meter.",
            accent=t.good,
        )
        self.right_var = tk.Label(top, text="", bg="#0b1220", fg=t.accent_lt, font=("Segoe UI", 9, "bold"))
        self.right_var.pack(side="right", pady=(5, 0))

        cards = tk.Frame(self._root, bg=t.bg)
        cards.pack(fill="x", padx=12, pady=(0, 10))
        for col in range(4):
            cards.grid_columnconfigure(col, weight=1)
        self._v_online = tk.StringVar(value="-")
        self._v_kw = tk.StringVar(value="-")
        self._v_voltage = tk.StringVar(value="-")
        self._v_freq = tk.StringVar(value="-")
        pro_chrome.metric(cards, "Meters Live", self._v_online, accent=t.good, col=0)
        pro_chrome.metric(cards, "Total kW", self._v_kw, accent=t.accent, col=1)
        pro_chrome.metric(cards, "Voltage", self._v_voltage, accent="#7b8cff", col=2)
        pro_chrome.metric(cards, "Frequency", self._v_freq, accent=t.warn, col=3)

        body = pro_chrome.card(self._root, accent=t.good, padx=12, pady=(0, 12))
        tk.Label(
            body,
            text="Meter Table",
            bg=t.card,
            fg=t.text,
            font=("Segoe UI", 11, "bold"),
        ).pack(anchor="w", padx=10, pady=(8, 4))
        table_wrap = tk.Frame(body, bg=t.card)
        table_wrap.pack(side="top", fill="both", expand=True, padx=10, pady=(0, 10))

        cols = ("name", "quality", "age", "v", "i", "kw", "pf", "hz", "kwh_imp", "kwh_exp")
        self.tree = ttk.Treeview(table_wrap, columns=cols, show="headings", height=14)

        headings = {
            "name": "Source",
            "quality": "Quality",
            "age": "Age",
            "v": "Vavg (V)",
            "i": "Iavg (A)",
            "kw": "kW",
            "pf": "PF",
            "hz": "Hz",
            "kwh_imp": "Import kWh",
            "kwh_exp": "Export kWh",
        }
        widths = {
            "name": 140,
            "quality": 90,
            "age": 80,
            "v": 90,
            "i": 90,
            "kw": 90,
            "pf": 70,
            "hz": 70,
            "kwh_imp": 110,
            "kwh_exp": 110,
        }
        for c in cols:
            self.tree.heading(c, text=headings.get(c, c))
            self.tree.column(c, width=widths.get(c, 80), anchor="center")

        # Scrollbar
        vsb = ttk.Scrollbar(table_wrap, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=vsb.set)

        self.tree.grid(row=0, column=0, sticky="nsew")

        # Jump-to-graphs on double click (operator workflow)
        self.tree.bind("<Double-1>", self._on_row_open_graphs)
        vsb.grid(row=0, column=1, sticky="ns")

        table_wrap.grid_rowconfigure(0, weight=1)
        table_wrap.grid_columnconfigure(0, weight=1)

    def update_view(self):
        """Called by MainWindow UI tick. Keep it fast."""
        stale_sec = stale_seconds(self.cfg)

        # Compute totals best-effort (ignore OFFLINE)
        try:
            total = self.aggregator.compute(self.meters, cfg=self.cfg) or {}
        except TypeError:
            total = self.aggregator.compute(self.meters) or {}

        # Online count based on freshness
        online = 0
        enabled = 0
        for m in self.meters:
            mid = getattr(m, "meter_id", None)
            if mid is None:
                continue
            enabled += 1
            if meter_is_fresh(m, stale_sec) and bool(getattr(m, "data_valid", False)):
                online += 1

        self.right_var.configure(text=f"Online {online}/{enabled}   stale>{stale_sec:.0f}s")
        try:
            self._v_online.set(f"{online}/{enabled}")
            self._v_kw.set(_fmt(total.get("Total_kW")))
            self._v_voltage.set(f"{_fmt(total.get('Vavg_ref'))} V")
            self._v_freq.set(f"{_fmt(total.get('Freq_ref'))} Hz")
        except Exception:
            pass

        # Refresh rows
        self.tree.delete(*self.tree.get_children())

        # TOTAL row
        if self.selector.show_total():
            self.tree.insert(
                "",
                "end",
                values=(
                    "TOTAL",
                    plain_meter_quality(_qual_text_total(total)),
                    _fmt_age(None),
                    _fmt(total.get("Vavg_ref")),
                    _fmt(total.get("Iavg_ref")),
                    _fmt(total.get("Total_kW")),
                    _fmt(total.get("PF_ref")),
                    _fmt(total.get("Freq_ref")),
                    _fmt(total.get("Import_kWh")),
                    _fmt(total.get("Export_kWh")),
                ),
            )

        # Per-meter rows (respect selector enabled? Status should show all 1..6, but we show list passed)
        for m in self.meters:
            mid = getattr(m, "meter_id", None)
            name = getattr(m, "name", None) or (f"M{mid}" if mid else "—")

            age = get_meter_age(m)
            fresh = meter_is_fresh(m, stale_sec) and bool(getattr(m, "data_valid", False))

            if not getattr(m, "first_data_received", False):
                q = "Waiting"
            elif not bool(getattr(m, "data_valid", False)):
                q = "No Data"
            elif not fresh:
                q = "Old Data"
            else:
                q = "Live"

            vals = getattr(m, "values", {}) or {}
            self.tree.insert(
                "",
                "end",
                values=(
                    name,
                    q,
                    _fmt_age(age),
                    _fmt(vals.get("Vavg")),
                    _fmt(vals.get("Iavg")),
                    _fmt(vals.get("kW")),
                    _fmt(vals.get("PF")),
                    _fmt(vals.get("Hz")),
                    _fmt(vals.get("Import_kWh")),
                    _fmt(vals.get("Export_kWh")),
                ),
            )

    def _on_row_open_graphs(self, _event):
        try:
            sel = self.tree.selection()
            if not sel:
                return
            item = self.tree.item(sel[0])
            vals = item.get("values") or []
            if not vals:
                return
            name = str(vals[0])
            if name.upper() == "TOTAL":
                meter_id = "TOTAL"
            else:
                # expected like "M1" or custom name, try parse trailing digit
                m = re.search(r"(\d+)$", name)
                meter_id = int(m.group(1)) if m else name
            if callable(self.on_open_graphs):
                self.on_open_graphs(meter_id)
        except Exception:
            return



def _fmt(x):
    try:
        if isinstance(x, (int, float)):
            return f"{float(x):.3f}"
    except Exception:
        pass
    return "—"


def _fmt_age(age):
    if age is None:
        return "—"
    try:
        if age < 0:
            return "—"
        if age < 60:
            return f"{age:.1f}s"
        if age < 3600:
            return f"{age/60.0:.1f}m"
        return f"{age/3600.0:.1f}h"
    except Exception:
        return "—"


def _qual_text_total(total: dict) -> str:
    # TOTAL is best-effort and must never lie.
    if not isinstance(total, dict) or not total:
        return "OFFLINE"
    valid = int(total.get("_valid_meter_count") or 0)
    sel = int(total.get("_selected_meter_count") or 0)
    if sel <= 0:
        sel = valid
    if valid <= 0:
        return "OFFLINE"
    if valid < sel:
        return f"STALE ({valid}/{sel})"
    # Required numeric keys for a confident OK
    must = ("Total_kW", "Vavg_ref", "Freq_ref")
    ok = all(k in total and isinstance(total.get(k), (int, float)) for k in must)
    return "OK" if ok else "STALE"
