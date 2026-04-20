import threading
import tkinter as tk
from tkinter import ttk, messagebox

from core.incident_engine import IncidentEngine, IncidentSummary


class IncidentSummaryTab(ttk.Frame):
    """Last 24h Incident Summary (operator-proof, one-click export).

    Runs calculations in a background thread. UI never blocks.
    """

    def __init__(self, parent, cfg: dict, meters, selector=None):
        super().__init__(parent)
        self.cfg = cfg
        self.meters = meters or []
        self.selector = selector

        self.engine = IncidentEngine(cfg)

        self._current_summary: IncidentSummary | None = None
        self._calc_th = None
        self._calc_lock = threading.Lock()

        self._build()

    # ---------------- UI ----------------
    def _build(self):
        self.columnconfigure(0, weight=1)
        self.rowconfigure(1, weight=1)

        header = ttk.Frame(self)
        header.grid(row=0, column=0, sticky="ew", padx=10, pady=(10, 6))
        header.columnconfigure(2, weight=1)

        ttk.Label(header, text="LAST 24 HOURS – INCIDENT SUMMARY", style="Header.TLabel").grid(row=0, column=0, sticky="w")

        # Source selector
        ttk.Label(header, text="Source:").grid(row=0, column=1, sticky="e", padx=(12, 4))
        self.source_var = tk.StringVar(value="TOTAL")
        self.source_cb = ttk.Combobox(header, textvariable=self.source_var, width=10, state="readonly")
        self.source_cb.grid(row=0, column=2, sticky="w")
        self.source_cb["values"] = self._build_source_list()

        self.btn_refresh = ttk.Button(header, text="Refresh", command=self.refresh)
        self.btn_refresh.grid(row=0, column=3, sticky="e", padx=(8, 0))

        # Main content
        body = ttk.Frame(self)
        body.grid(row=1, column=0, sticky="nsew", padx=10, pady=(0, 10))
        body.columnconfigure(0, weight=1)

        # Status line
        self.status_var = tk.StringVar(value="Ready.")
        ttk.Label(body, textvariable=self.status_var, style="Muted.TLabel").grid(row=0, column=0, sticky="w", pady=(0, 8))

        # Stats grids
        g = ttk.Frame(body)
        g.grid(row=1, column=0, sticky="ew")
        for c in range(3):
            g.columnconfigure(c, weight=1, uniform="c")

        self._mk_box(g, 0, "Voltage (V)", "v_max", "v_min")
        self._mk_box(g, 1, "Current (A)", "i_max", "i_min")
        self._mk_box(g, 2, "Power (kW)", "kw_max", "kw_min")

        # Incident stats
        self.inc_frame = ttk.LabelFrame(body, text="INCIDENT STATISTICS")
        self.inc_frame.grid(row=2, column=0, sticky="ew", pady=(12, 0))
        self.inc_frame.columnconfigure(1, weight=1)

        self.alarms_var = tk.StringVar(value="—")
        self.offline_events_var = tk.StringVar(value="—")
        self.longest_offline_var = tk.StringVar(value="—")
        self.stale_var = tk.StringVar(value="—")
        self.recovering_var = tk.StringVar(value="—")

        self._row(self.inc_frame, 0, "Total Alarms Raised", self.alarms_var)
        self._row(self.inc_frame, 1, "Offline Events (BUS)", self.offline_events_var)
        self._row(self.inc_frame, 2, "Longest OFFLINE (BUS)", self.longest_offline_var)
        self._row(self.inc_frame, 3, "Time in STALE (BUS)", self.stale_var)
        self._row(self.inc_frame, 4, "Time in RECOVERING (BUS)", self.recovering_var)

        # Note
        self.note_var = tk.StringVar(value="")
        self.note_lbl = ttk.Label(body, textvariable=self.note_var, style="Muted.TLabel", wraplength=1100, justify="left")
        self.note_lbl.grid(row=3, column=0, sticky="w", pady=(10, 0))

        # Export buttons
        btns = ttk.Frame(body)
        btns.grid(row=4, column=0, sticky="e", pady=(10, 0))
        self.btn_csv = ttk.Button(btns, text="Export CSV", command=self.export_csv, state="disabled")
        self.btn_pdf = ttk.Button(btns, text="Export PDF", command=self.export_pdf, state="disabled")
        self.btn_csv.pack(side="left", padx=6)
        self.btn_pdf.pack(side="left")

    def _build_source_list(self):
        vals = []
        # Prefer TOTAL if enabled
        if self.selector is not None:
            try:
                if bool(self.selector.show_total()):
                    vals.append("TOTAL")
            except Exception:
                vals.append("TOTAL")
            try:
                for mid in self.selector.selected_meters():
                    vals.append(f"M{int(mid)}")
            except Exception:
                pass
        if not vals:
            vals = ["TOTAL"] + [f"M{m.meter_id}" for m in self.meters]
        # de-dup keep order
        out = []
        for v in vals:
            if v not in out:
                out.append(v)
        return out

    def _mk_box(self, parent, col, title, max_key, min_key):
        box = ttk.LabelFrame(parent, text=title)
        box.grid(row=0, column=col, sticky="ew", padx=(0 if col == 0 else 8, 0))
        box.columnconfigure(1, weight=1)

        var_max = tk.StringVar(value="—")
        var_min = tk.StringVar(value="—")
        setattr(self, f"{max_key}_var", var_max)
        setattr(self, f"{min_key}_var", var_min)

        self._row(box, 0, "Max", var_max)
        self._row(box, 1, "Min", var_min)

    def _row(self, parent, r, label, var):
        ttk.Label(parent, text=label + ":").grid(row=r, column=0, sticky="w", padx=8, pady=3)
        ttk.Label(parent, textvariable=var).grid(row=r, column=1, sticky="w", padx=8, pady=3)

    # ---------------- page lifecycle ----------------
    def on_show(self):
        # Refresh list to reflect current selector state
        try:
            self.source_cb["values"] = self._build_source_list()
        except Exception:
            pass
        # Auto-refresh when opened (but never spam)
        self.refresh()

    # ---------------- actions ----------------
    def refresh(self):
        with self._calc_lock:
            if self._calc_th and self._calc_th.is_alive():
                return

            source = (self.source_var.get() or "TOTAL").strip()
            meter_id = "TOTAL" if source == "TOTAL" else source.replace("M", "").strip() or "ALL"

            self._set_busy(True, f"Calculating last 24h for {source}…")

            def worker():
                try:
                    summary = self.engine.compute_last_24h(source=source, meter_id=meter_id)
                    self.after(0, lambda: self._apply_summary(summary))
                except Exception as e:
                    self.after(0, lambda: self._on_error(str(e)))
                finally:
                    self.after(0, lambda: self._set_busy(False, "Ready."))

            self._calc_th = threading.Thread(target=worker, name="IncidentSummaryCalc", daemon=True)
            self._calc_th.start()

    def export_csv(self):
        s = self._current_summary
        if not s:
            return
        try:
            path = self.engine.export_csv(s)
            messagebox.showinfo("Export CSV", f"Saved:\n{path}")
        except Exception as e:
            messagebox.showerror("Export CSV", str(e))

    def export_pdf(self):
        s = self._current_summary
        if not s:
            return
        try:
            path = self.engine.export_pdf(s)
            messagebox.showinfo("Export PDF", f"Saved:\n{path}")
        except Exception as e:
            messagebox.showerror("Export PDF", str(e))

    # ---------------- helpers ----------------
    def _apply_summary(self, s: IncidentSummary):
        self._current_summary = s

        # Numbers
        self.v_max_var.set(self._fmt(s.v_max))
        self.v_min_var.set(self._fmt(s.v_min))
        self.i_max_var.set(self._fmt(s.i_max))
        self.i_min_var.set(self._fmt(s.i_min))
        self.kw_max_var.set(self._fmt(s.kw_max))
        self.kw_min_var.set(self._fmt(s.kw_min))

        # Incident stats
        self.alarms_var.set(str(s.alarms_raised))
        self.offline_events_var.set(str(s.offline_events))
        self.longest_offline_var.set(self._fmt_dur(s.longest_offline_s))
        self.stale_var.set(self._fmt_dur(s.stale_s))
        self.recovering_var.set(self._fmt_dur(s.recovering_s))

        self.note_var.set(s.note or "")

        self.btn_csv.configure(state="normal")
        self.btn_pdf.configure(state="normal")

    def _on_error(self, msg: str):
        self.status_var.set("Error.")
        messagebox.showerror("Incident Summary", msg)

    def _set_busy(self, busy: bool, msg: str):
        self.status_var.set(msg)
        st = "disabled" if busy else "normal"
        try:
            self.btn_refresh.configure(state=st)
            self.source_cb.configure(state="disabled" if busy else "readonly")
        except Exception:
            pass

    def _fmt(self, v):
        if v is None:
            return "N/A"
        try:
            return f"{float(v):.3f}"
        except Exception:
            return "N/A"

    def _fmt_dur(self, sec):
        try:
            sec = float(sec or 0.0)
        except Exception:
            sec = 0.0
        s = int(round(max(0.0, sec)))
        hh = s // 3600
        mm = (s % 3600) // 60
        ss = s % 60
        return f"{hh:02d}:{mm:02d}:{ss:02d}"
