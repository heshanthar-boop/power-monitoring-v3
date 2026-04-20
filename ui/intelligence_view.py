from __future__ import annotations

import csv
import time
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
from typing import Callable, Optional

from core.intelligence_engine import MaintenanceIntelligenceEngine
from ui.styles import get_theme


def _fmt(value, digits: int = 1, empty: str = "-") -> str:
    try:
        if isinstance(value, (int, float)):
            return f"{float(value):.{digits}f}"
    except Exception:
        pass
    return empty


class IntelligenceTab(ttk.Frame):
    """Predictive maintenance page with explainable risk findings."""

    def __init__(
        self,
        parent,
        cfg: dict,
        meters,
        aggregator,
        *,
        worker=None,
        alarm_engine=None,
        logging_engine=None,
        remote_sync=None,
        on_open_page: Optional[Callable[[str], None]] = None,
        can_open: Optional[Callable[[str], bool]] = None,
    ) -> None:
        super().__init__(parent)
        self.cfg = cfg
        self.meters = list(meters or [])
        self.aggregator = aggregator
        self.worker = worker
        self.alarm_engine = alarm_engine
        self.logging_engine = logging_engine
        self.remote_sync = remote_sync
        self.on_open_page = on_open_page
        self.can_open = can_open or (lambda _key: True)
        self.engine = MaintenanceIntelligenceEngine(cfg)
        self._theme = get_theme()
        self._metric_vars: dict[str, tk.StringVar] = {}
        self._summary_var = tk.StringVar(value="")
        self._top_action_var = tk.StringVar(value="")
        self._last_data: dict = {}
        self._finding_pages: dict[str, str] = {}
        self._build_ui()
        self.update_view()

    def set_worker(self, worker) -> None:
        self.worker = worker

    def set_remote_sync(self, remote_sync) -> None:
        self.remote_sync = remote_sync

    def _open_page(self, key: str) -> None:
        try:
            if callable(self.on_open_page) and self.can_open(key):
                self.on_open_page(key)
        except Exception:
            pass

    def _build_ui(self) -> None:
        t = self._theme
        root = tk.Frame(self, bg=t.bg)
        root.pack(fill="both", expand=True)

        header = tk.Frame(root, bg=t.bg)
        header.pack(fill="x", padx=12, pady=(12, 8))
        tk.Label(
            header,
            text="Intelligence",
            bg=t.bg,
            fg=t.text,
            font=("Segoe UI", 18, "bold"),
        ).pack(side="left")
        tk.Label(
            header,
            text="Predictive maintenance and reliability risks",
            bg=t.bg,
            fg=t.text_muted,
            font=("Segoe UI", 9),
        ).pack(side="left", padx=(12, 0), pady=(6, 0))
        tk.Label(
            header,
            textvariable=self._summary_var,
            bg=t.bg,
            fg=t.text_dim,
            font=("Segoe UI", 9),
        ).pack(side="right", pady=(6, 0))

        cards = tk.Frame(root, bg=t.bg)
        cards.pack(fill="x", padx=12, pady=(0, 10))
        for col in range(5):
            cards.grid_columnconfigure(col, weight=1)
        self._add_metric(cards, "score", 0, "Readiness", t.accent)
        self._add_metric(cards, "coverage", 1, "Live Data", t.good)
        self._add_metric(cards, "findings", 2, "Risk Findings", t.warn)
        self._add_metric(cards, "comm", 3, "Comm Health", "#7b8cff")
        self._add_metric(cards, "remote", 4, "Remote Sync", "#c8a0ff")

        action = tk.Frame(root, bg=t.card, highlightthickness=1, highlightbackground=t.border)
        action.pack(fill="x", padx=12, pady=(0, 10))
        tk.Frame(action, bg=t.warn, height=2).pack(fill="x", side="top")
        inner = tk.Frame(action, bg=t.card)
        inner.pack(fill="x", padx=12, pady=10)
        tk.Label(inner, text="NEXT BEST ACTION", bg=t.card, fg=t.text_muted, font=("Segoe UI", 8, "bold")).pack(side="left", padx=(0, 12))
        tk.Label(inner, textvariable=self._top_action_var, bg=t.card, fg=t.text, font=("Segoe UI", 10, "bold")).pack(side="left", fill="x", expand=True, anchor="w")
        ttk.Button(inner, text="Export CSV", style="Small.TButton", command=self._export_csv).pack(side="right")

        body = tk.Frame(root, bg=t.bg)
        body.pack(fill="both", expand=True, padx=12, pady=(0, 12))
        body.grid_columnconfigure(0, weight=2)
        body.grid_columnconfigure(1, weight=1)
        body.grid_rowconfigure(0, weight=1)

        findings = tk.Frame(body, bg=t.card, highlightthickness=1, highlightbackground=t.border)
        findings.grid(row=0, column=0, sticky="nsew", padx=(0, 8))
        tk.Frame(findings, bg=t.accent, height=2).pack(fill="x", side="top")
        tk.Label(findings, text="Predictive Findings", bg=t.card, fg=t.text, font=("Segoe UI", 11, "bold")).pack(anchor="w", padx=10, pady=(8, 4))

        cols = ("severity", "asset", "prediction", "confidence", "action")
        self.findings_tree = ttk.Treeview(findings, columns=cols, show="headings", selectmode="browse", height=12)
        headings = {
            "severity": "Level",
            "asset": "Asset",
            "prediction": "Prediction",
            "confidence": "Confidence",
            "action": "Action",
        }
        widths = {"severity": 80, "asset": 130, "prediction": 260, "confidence": 90, "action": 420}
        for col in cols:
            self.findings_tree.heading(col, text=headings[col], anchor="w")
            self.findings_tree.column(col, width=widths[col], anchor="w", stretch=(col == "action"))
        self.findings_tree.tag_configure("CRITICAL", foreground=t.alarm)
        self.findings_tree.tag_configure("WARN", foreground=t.warn)
        self.findings_tree.tag_configure("INFO", foreground=t.text_muted)
        self.findings_tree.pack(fill="both", expand=True, padx=10, pady=(0, 8))
        self.findings_tree.bind("<Double-1>", self._open_selected_finding)

        side = tk.Frame(body, bg=t.bg)
        side.grid(row=0, column=1, sticky="nsew")
        side.grid_rowconfigure(0, weight=1)
        side.grid_rowconfigure(1, weight=0)

        assets = tk.Frame(side, bg=t.card, highlightthickness=1, highlightbackground=t.border)
        assets.grid(row=0, column=0, sticky="nsew")
        tk.Frame(assets, bg=t.good, height=2).pack(fill="x", side="top")
        tk.Label(assets, text="Meter Risk", bg=t.card, fg=t.text, font=("Segoe UI", 11, "bold")).pack(anchor="w", padx=10, pady=(8, 4))
        acols = ("meter", "status", "risk", "reason")
        self.asset_tree = ttk.Treeview(assets, columns=acols, show="headings", height=10)
        aheads = {"meter": "Meter", "status": "Status", "risk": "Risk", "reason": "Reason"}
        awidths = {"meter": 150, "status": 70, "risk": 70, "reason": 220}
        for col in acols:
            self.asset_tree.heading(col, text=aheads[col], anchor="w")
            self.asset_tree.column(col, width=awidths[col], anchor="w", stretch=(col == "reason"))
        self.asset_tree.pack(fill="both", expand=True, padx=10, pady=(0, 8))

        actions = tk.Frame(side, bg=t.card, highlightthickness=1, highlightbackground=t.border)
        actions.grid(row=1, column=0, sticky="ew", pady=(8, 0))
        tk.Label(actions, text="Open related page", bg=t.card, fg=t.text_muted, font=("Segoe UI", 8, "bold")).pack(anchor="w", padx=10, pady=(8, 4))
        btns = tk.Frame(actions, bg=t.card)
        btns.pack(fill="x", padx=10, pady=(0, 10))
        for key, label in (("status", "Status"), ("comm", "Communication"), ("analytics", "Analytics"), ("alarms", "Alarms"), ("connectivity", "Connectivity")):
            btn = ttk.Button(btns, text=label, style="Small.TButton", command=lambda k=key: self._open_page(k))
            if not self.can_open(key):
                btn.configure(state="disabled")
            btn.pack(side="left", padx=(0, 6), pady=(0, 4))

    def _add_metric(self, parent, key: str, col: int, title: str, accent: str) -> None:
        t = self._theme
        card = tk.Frame(parent, bg=t.card, highlightthickness=1, highlightbackground=t.border)
        card.grid(row=0, column=col, sticky="nsew", padx=(0 if col == 0 else 8, 0))
        tk.Frame(card, bg=accent, height=3).pack(fill="x", side="top")
        tk.Label(card, text=title.upper(), bg=t.card, fg=t.text_muted, font=("Segoe UI", 8, "bold")).pack(anchor="w", padx=10, pady=(8, 0))
        var = tk.StringVar(value="-")
        self._metric_vars[key] = var
        tk.Label(card, textvariable=var, bg=t.card, fg=t.text, font=("Segoe UI", 16, "bold")).pack(anchor="w", padx=10, pady=(4, 10))

    def _compute_total(self) -> dict:
        try:
            return self.aggregator.compute(self.meters, cfg=self.cfg) or {}
        except TypeError:
            return self.aggregator.compute(self.meters) or {}
        except Exception:
            return {}

    def update_view(self) -> None:
        total = self._compute_total()
        data = self.engine.analyze(
            self.meters,
            total=total,
            worker=self.worker,
            alarm_engine=self.alarm_engine,
            logging_engine=self.logging_engine,
            remote_sync=self.remote_sync,
        )
        self._last_data = data

        score = int(data.get("readiness_score", 0) or 0)
        state = str(data.get("state", "") or "")
        self._metric_vars["score"].set(f"{score}/100  {state}")
        self._metric_vars["coverage"].set(f"{int(data.get('live_meter_count', 0) or 0)}/{int(data.get('enabled_meter_count', 0) or 0)} Live")

        findings = data.get("findings", []) or []
        warn_count = sum(1 for item in findings if str(item.get("severity", "")).upper() == "WARN")
        crit_count = sum(1 for item in findings if str(item.get("severity", "")).upper() == "CRITICAL")
        self._metric_vars["findings"].set(f"{crit_count} Critical / {warn_count} Warn")

        bus = data.get("bus", {}) or {}
        bus_state = str(bus.get("state", "UNKNOWN") or "UNKNOWN")
        latency = bus.get("latency_ms")
        self._metric_vars["comm"].set(f"{bus_state}  {_fmt(latency, 0)} ms" if latency is not None else bus_state)

        sync = data.get("remote_sync", {}) or {}
        if sync:
            self._metric_vars["remote"].set(f"Q {int(sync.get('queue_depth', 0) or 0)}/{int(sync.get('max_queue', 0) or 0)}")
        else:
            self._metric_vars["remote"].set("Not wired")

        self._top_action_var.set(str(data.get("top_action", "") or "No immediate maintenance action."))
        self._summary_var.set(time.strftime("Updated %Y-%m-%d %H:%M:%S", time.localtime(float(data.get("ts", time.time())))))

        self.findings_tree.delete(*self.findings_tree.get_children())
        self._finding_pages = {}
        if not findings:
            self.findings_tree.insert("", "end", values=("OK", "Plant", "No predictive maintenance risk detected", "HIGH", "Continue normal monitoring"), tags=("INFO",))
        else:
            for idx, item in enumerate(findings):
                iid = f"finding_{idx}"
                sev = str(item.get("severity", "INFO") or "INFO").upper()
                self.findings_tree.insert(
                    "",
                    "end",
                    iid=iid,
                    values=(
                        sev,
                        item.get("asset", ""),
                        item.get("prediction", ""),
                        item.get("confidence", ""),
                        item.get("action", ""),
                    ),
                    tags=(sev,),
                )
                self._finding_pages[iid] = str(item.get("page", "") or "")

        self.asset_tree.delete(*self.asset_tree.get_children())
        for row in data.get("assets", []) or []:
            self.asset_tree.insert(
                "",
                "end",
                values=(
                    row.get("name", ""),
                    row.get("status", ""),
                    f"{row.get('risk', '')} {int(row.get('risk_score', 0) or 0)}",
                    row.get("reason", ""),
                ),
            )

    def _open_selected_finding(self, _event=None) -> None:
        try:
            sel = self.findings_tree.selection()
            if not sel:
                return
            page = self._finding_pages.get(sel[0], "")
            if page:
                self._open_page(page)
        except Exception:
            pass

    def _export_csv(self) -> None:
        data = self._last_data or {}
        path = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
            title="Export Intelligence Report",
        )
        if not path:
            return
        try:
            with open(path, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow(["Predictive Maintenance Intelligence"])
                w.writerow(["Timestamp", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(float(data.get("ts", time.time()))))])
                w.writerow(["Readiness Score", data.get("readiness_score", "")])
                w.writerow(["State", data.get("state", "")])
                w.writerow(["Live Meters", data.get("live_meter_count", "")])
                w.writerow(["Enabled Meters", data.get("enabled_meter_count", "")])
                w.writerow(["Data Confidence %", data.get("data_confidence_pct", "")])
                w.writerow(["Top Action", data.get("top_action", "")])
                w.writerow([])
                w.writerow(["Findings"])
                w.writerow(["Severity", "Asset", "Prediction", "Evidence", "Likely Cause", "Action", "Page", "Confidence"])
                for item in data.get("findings", []) or []:
                    w.writerow([
                        item.get("severity", ""),
                        item.get("asset", ""),
                        item.get("prediction", ""),
                        item.get("evidence", ""),
                        item.get("likely_cause", ""),
                        item.get("action", ""),
                        item.get("page", ""),
                        item.get("confidence", ""),
                    ])
                w.writerow([])
                w.writerow(["Meter Risk"])
                w.writerow(["Meter ID", "Name", "Status", "Risk", "Risk Score", "Reason", "Action"])
                for row in data.get("assets", []) or []:
                    w.writerow([
                        row.get("meter_id", ""),
                        row.get("name", ""),
                        row.get("status", ""),
                        row.get("risk", ""),
                        row.get("risk_score", ""),
                        row.get("reason", ""),
                        row.get("action", ""),
                    ])
            messagebox.showinfo("Export", f"Saved intelligence report:\n{path}")
        except Exception as exc:
            messagebox.showerror("Export failed", str(exc))
