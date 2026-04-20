import time
import tkinter as tk
from tkinter import ttk, filedialog, messagebox

from utils.paths import events_db_path
from utils.logger import setup_logger
from ui.styles import get_theme
from ui import pro_chrome

logger = setup_logger("ui.journal")


class JournalTab(ttk.Frame):
    """Operator-safe event journal viewer (SQLite).

    - Fast filters (time window / meter / kind)
    - Export visible rows to CSV
    - Read-only UI; writes happen in background via core.event_journal.EventJournal
    """

    def __init__(self, parent, cfg, meters, event_journal=None):
        super().__init__(parent)
        self.cfg = cfg
        self.meters = meters
        self.event_journal = event_journal
        self.db_path = events_db_path()

        self._build_ui()

    def _build_ui(self):
        t = get_theme()
        root = pro_chrome.page(self)
        pro_chrome.header(
            root,
            "Event Journal",
            "Chronological alarm, communication and operator events.",
            accent=t.warn,
        )
        top = ttk.Frame(root, style="App.Panel.TFrame", padding=(10, 8))
        top.pack(side="top", fill="x", padx=12, pady=(0, 8))

        ttk.Label(top, text="FILTERS", style="H3.TLabel").pack(side="left", padx=(0, 16))

        ttk.Label(top, text="Window:", style="FieldLabel.TLabel").pack(side="left")
        self.cmb_window = ttk.Combobox(top, width=8, state="readonly",
                                       values=["1h", "24h", "7d", "30d"])
        self.cmb_window.set("24h")
        self.cmb_window.pack(side="left", padx=(4, 12))
        self.cmb_window.bind("<<ComboboxSelected>>", lambda _e: self.refresh())

        ttk.Label(top, text="Kind:", style="FieldLabel.TLabel").pack(side="left")
        self.cmb_kind = ttk.Combobox(top, width=9, state="readonly",
                                     values=["ALL", "ALARM", "COMM", "NOTE"])
        self.cmb_kind.set("ALL")
        self.cmb_kind.pack(side="left", padx=(4, 12))
        self.cmb_kind.bind("<<ComboboxSelected>>", lambda _e: self.refresh())

        ttk.Label(top, text="Meter:", style="FieldLabel.TLabel").pack(side="left")
        meter_vals = ["ALL", "BUS", "TOTAL"] + [str(m.meter_id) for m in self.meters]
        # de-dup while preserving order
        seen = set()
        meter_vals = [x for x in meter_vals if not (x in seen or seen.add(x))]
        self.cmb_meter = ttk.Combobox(top, width=8, state="readonly", values=meter_vals)
        self.cmb_meter.set("ALL")
        self.cmb_meter.pack(side="left", padx=(4, 12))
        self.cmb_meter.bind("<<ComboboxSelected>>", lambda _e: self.refresh())

        ttk.Button(top, text="↻ Refresh", style="Ghost.TButton",
                   command=self.refresh).pack(side="left", padx=(0, 6))
        ttk.Button(top, text="Export CSV…", style="Ghost.TButton",
                   command=self.export_csv).pack(side="left")

        card = pro_chrome.card(root, accent=t.warn, padx=12, pady=(0, 12))
        tk.Label(card, text="Journal Events", bg=t.card, fg=t.text, font=("Segoe UI", 11, "bold")).pack(
            anchor="w", padx=10, pady=(8, 4)
        )
        mid = tk.Frame(card, bg=t.card)
        mid.pack(side="top", fill="both", expand=True, padx=10, pady=(0, 10))

        cols = ("ts", "kind", "meter", "code", "severity", "message")
        self.tree = ttk.Treeview(mid, columns=cols, show="headings",
                                 height=18, selectmode="browse")
        self.tree.heading("ts",       text="Timestamp",  anchor="w")
        self.tree.heading("kind",     text="Kind",       anchor="center")
        self.tree.heading("meter",    text="Meter",      anchor="center")
        self.tree.heading("code",     text="Code",       anchor="w")
        self.tree.heading("severity", text="Sev",        anchor="center")
        self.tree.heading("message",  text="Message",    anchor="w")

        self.tree.column("ts",       width=155, minwidth=120, stretch=False, anchor="w")
        self.tree.column("kind",     width=70,  minwidth=60,  stretch=False, anchor="center")
        self.tree.column("meter",    width=75,  minwidth=60,  stretch=False, anchor="center")
        self.tree.column("code",     width=160, minwidth=80,  stretch=False, anchor="w")
        self.tree.column("severity", width=65,  minwidth=50,  stretch=False, anchor="center")
        self.tree.column("message",  width=600, minwidth=200, stretch=True,  anchor="w")

        # color tags by kind / severity
        self.tree.tag_configure("ALARM", foreground=t.alarm)
        self.tree.tag_configure("WARN",  foreground=t.warn)
        self.tree.tag_configure("COMM",  foreground=t.accent_lt)
        self.tree.tag_configure("NOTE",  foreground=t.text_muted)
        self.tree.tag_configure("INFO",  foreground=t.text)

        vsb = ttk.Scrollbar(mid, orient="vertical",   command=self.tree.yview)
        hsb = ttk.Scrollbar(mid, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        self.tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")
        mid.grid_rowconfigure(0, weight=1)
        mid.grid_columnconfigure(0, weight=1)

        # initial load
        self.after(200, self.refresh)

    def _since_ts(self) -> float:
        sel = (self.cmb_window.get() or "24h").strip().lower()
        now = time.time()
        if sel.endswith("h"):
            hrs = float(sel[:-1] or 24.0)
            return now - hrs * 3600.0
        if sel.endswith("d"):
            days = float(sel[:-1] or 7.0)
            return now - days * 86400.0
        return now - 86400.0

    def refresh(self):
        # clear
        for iid in self.tree.get_children():
            self.tree.delete(iid)

        if not self.event_journal:
            # Journal not wired (should be wired in app.py)
            self.tree.insert("", "end", values=("", "NOTE", "BUS", "JOURNAL", "INFO", "EventJournal not attached"))
            return

        try:
            rows = self.event_journal.query(
                since_ts=self._since_ts(),
                meter_id=self.cmb_meter.get(),
                kind=self.cmb_kind.get(),
                limit=2000,
            )
        except Exception as e:
            logger.error(f"[JOURNAL] query error: {type(e).__name__}: {e}")
            self.tree.insert("", "end", values=("", "NOTE", "BUS", "JOURNAL", "ERROR", f"Query failed: {e}"))
            return

        for r in rows:
            try:
                kind = str(r.get("kind", "")).upper()
                sev  = str(r.get("severity", "")).upper()
                # Pick tag: severity takes priority over kind
                if sev in ("ALARM", "WARN"):
                    tag = sev
                elif kind == "COMM":
                    tag = "COMM"
                elif kind == "NOTE":
                    tag = "NOTE"
                else:
                    tag = "INFO"
                self.tree.insert(
                    "", "end",
                    values=(r["ts_iso"], r["kind"], r["meter_id"], r["code"], r["severity"], r["message"]),
                    tags=(tag,),
                )
            except Exception:
                pass

    def export_csv(self):
        path = filedialog.asksaveasfilename(
            title="Export Journal CSV",
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv")],
        )
        if not path:
            return

        try:
            import csv
            with open(path, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow(["ts_iso", "kind", "meter_id", "code", "severity", "message"])
                for iid in self.tree.get_children():
                    w.writerow(self.tree.item(iid, "values"))
            messagebox.showinfo("Export", "Exported successfully.")
        except Exception as e:
            messagebox.showerror("Export failed", str(e))
