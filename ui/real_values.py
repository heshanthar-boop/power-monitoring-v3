from __future__ import annotations

from tkinter import ttk
from utils.health import get_meter_age, meter_is_fresh, stale_seconds

DEFAULT_TOTAL_16 = [
    ("Total_kW", "kW", "kW"),
    ("Total_kVA", "kVA", "kVA"),
    ("Total_kVAr", "kVAr", "kVAr"),
    ("Export_kWh", "Export_kWh", "kWh"),
    ("Import_kWh", "Import_kWh", "kWh"),
    ("Net_kWh", "Net_kWh", "kWh"),
    ("I1_total", "I1", "A"),
    ("I2_total", "I2", "A"),
    ("I3_total", "I3", "A"),
    ("Vavg_ref", "Vavg", "V"),
    ("Freq_ref", "Frequency", "Hz"),
    ("THD_V_worst", "THD_V", "%"),
    ("THD_I_worst", "THD_I", "%"),
    ("PF_total", "PF", ""),
    ("RunHour_total", "RunHour", "h"),
    ("Today_kWh", "Today_kWh", "kWh"),
]


class RealValuesTab(ttk.Frame):
    """Real Values view.

    TOTAL values must be computed automatically using the Communication tab
    per-meter TOTAL role (ADD / SUB / OFF). No per-parameter editing here.
    """

    def __init__(self, parent, meters, aggregator, selector, cfg=None):
        super().__init__(parent, padding=8)
        self.meters = meters
        self.aggregator = aggregator
        self.selector = selector
        self.cfg = cfg if cfg is not None else {}

        self.nb = ttk.Notebook(self, style="Sub.TNotebook")
        self.nb.pack(fill="both", expand=True)

        self._trees: dict[str, ttk.Treeview] = {}
        self._last_selection = None

        self._build_tabs()

    # ---------- TOTAL keys (fixed list) ----------
    def _wanted_total_keys(self):
        # Display order. Values come from aggregator.compute(), which already
        # respects Communication tab ADD/SUB/OFF via meter.total_sign.
        return [k for (k, _label, _unit) in DEFAULT_TOTAL_16 if (k or "").strip()]

    # ---------- UI building ----------
    def _make_tree(self, parent):
        tree = ttk.Treeview(parent, columns=("name", "value"), show="headings")
        tree.heading("name", text="REGISTER / PARAMETER")
        tree.heading("value", text="VALUE")
        tree.column("name", width=520, anchor="w")
        tree.column("value", width=240, anchor="e")

        vs = ttk.Scrollbar(parent, orient="vertical", command=tree.yview)
        tree.configure(yscrollcommand=vs.set)

        tree.grid(row=0, column=0, sticky="nsew")
        vs.grid(row=0, column=1, sticky="ns")
        parent.rowconfigure(0, weight=1)
        parent.columnconfigure(0, weight=1)
        return tree

    def _build_tabs(self):
        for tab in self.nb.tabs():
            self.nb.forget(tab)
        self._trees.clear()

        if self.selector.show_total():
            frm = ttk.Frame(self.nb)
            self.nb.add(frm, text="TOTAL")
            tree = self._make_tree(frm)
            self._trees["TOTAL"] = tree

        for mid in self.selector.selected_meters():
            frm = ttk.Frame(self.nb)
            meter_name = f"Meter {mid}"
            for m in self.meters:
                if getattr(m, "meter_id", None) == mid:
                    meter_name = m.name or meter_name
                    break
            self.nb.add(frm, text=meter_name)
            self._trees[mid] = self._make_tree(frm)

    # NOTE: TOTAL editing was intentionally removed. The Communication tab is the
    # single source of truth for add/sub/off logic.

    # ---------- data refresh ----------
    def update_view(self):
        current_state = (self.selector.show_total(), tuple(self.selector.selected_meters()))
        if current_state != self._last_selection:
            self._build_tabs()
            self._last_selection = current_state
        stale_sec = stale_seconds(self.cfg)

        if "TOTAL" in self._trees:
            try:
                total = self.aggregator.compute(self.meters, cfg=self.cfg) or {}
            except TypeError:
                total = self.aggregator.compute(self.meters) or {}
            tree = self._trees["TOTAL"]
            tree.delete(*tree.get_children())

            for key in self._wanted_total_keys():
                val = total.get(key)
                if isinstance(val, (int, float)):
                    display = f"{val:.3f}"
                else:
                    display = "—"
                tree.insert("", "end", values=(key, display))

        for m in self.meters:
            mid = getattr(m, "meter_id", None)
            if mid not in self._trees:
                continue
            tree = self._trees[mid]
            tree.delete(*tree.get_children())

            meter_values = getattr(m, "values", {}) or {}
            meter_has_data = bool(meter_values)
            first_data = bool(getattr(m, "first_data_received", False))
            meter_fresh = meter_is_fresh(m, stale_sec)

            if not meter_has_data:
                if not first_data:
                    status = "WAITING FOR DATA"
                elif not getattr(m, "data_valid", False):
                    status = "DISCONNECTED / NO DATA"
                else:
                    status = "NO DATA"
                tree.insert("", "end", values=("STATUS", status))
                continue

            if not meter_fresh:
                age = get_meter_age(m)
                if age is not None:
                    status = f"STALE DATA ({age:.1f}s ago)"
                else:
                    status = "STALE DATA"
                tree.insert("", "end", values=("STATUS", status))

            for name in sorted(meter_values.keys()):
                v = meter_values.get(name)
                display = f"{v:.4f}" if isinstance(v, (int, float)) else str(v)
                tree.insert("", "end", values=(name, display))
