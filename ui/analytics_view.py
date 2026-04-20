from __future__ import annotations

import csv
import time
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from typing import Callable, Optional

from core.analytics_engine import PlantAnalyticsEngine
from ui.styles import get_theme


def _fmt(value, digits: int = 1, empty: str = "-") -> str:
    try:
        if isinstance(value, (int, float)):
            return f"{float(value):.{digits}f}"
    except Exception:
        pass
    return empty


class AnalyticsTab(ttk.Frame):
    """Plant analytics page: health score, forecast, power quality, and actions."""

    def __init__(
        self,
        parent,
        cfg: dict,
        meters,
        aggregator,
        *,
        on_open_page: Optional[Callable[[str], None]] = None,
        can_open: Optional[Callable[[str], bool]] = None,
    ):
        super().__init__(parent)
        self.cfg = cfg
        self.meters = list(meters or [])
        self.aggregator = aggregator
        self.on_open_page = on_open_page
        self.can_open = can_open or (lambda _key: True)
        self.engine = PlantAnalyticsEngine(cfg)
        self._theme = get_theme()
        self._metric_vars: dict[str, tk.StringVar] = {}
        self._summary_var = tk.StringVar(value="")
        self._last_data: dict = {}
        self._build_ui()
        self.update_view()

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
            text="Plant Analytics",
            bg=t.bg,
            fg=t.text,
            font=("Segoe UI", 18, "bold"),
        ).pack(side="left")
        tk.Label(
            header,
            textvariable=self._summary_var,
            bg=t.bg,
            fg=t.text_muted,
            font=("Segoe UI", 9),
        ).pack(side="right")

        cards = tk.Frame(root, bg=t.bg)
        cards.pack(fill="x", padx=12, pady=(0, 10))
        for col in range(5):
            cards.grid_columnconfigure(col, weight=1)
        self._add_metric(cards, "score", 0, "Health Score", "#4da6ff")
        self._add_metric(cards, "plant_kw", 1, "Live kW", "#30c060")
        self._add_metric(cards, "forecast", 2, "15-min Forecast", "#7b8cff")
        self._add_metric(cards, "pf", 3, "Power Factor", "#d08030")
        self._add_metric(cards, "confidence", 4, "Data Confidence", "#c8a0ff")

        body = tk.Frame(root, bg=t.bg)
        body.pack(fill="both", expand=True, padx=12, pady=(0, 12))
        body.grid_columnconfigure(0, weight=2)
        body.grid_columnconfigure(1, weight=1)
        body.grid_rowconfigure(0, weight=1)

        issue_box = tk.Frame(body, bg=t.card, highlightthickness=1, highlightbackground=t.border)
        issue_box.grid(row=0, column=0, sticky="nsew", padx=(0, 8))
        tk.Frame(issue_box, bg=t.accent, height=2).pack(fill="x", side="top")
        tk.Label(
            issue_box,
            text="Actionable Findings",
            bg=t.card,
            fg=t.text,
            font=("Segoe UI", 11, "bold"),
        ).pack(anchor="w", padx=10, pady=(8, 4))

        cols = ("severity", "area", "title", "action")
        self.issue_tree = ttk.Treeview(issue_box, columns=cols, show="headings", selectmode="browse", height=12)
        headings = {"severity": "Level", "area": "Area", "title": "Finding", "action": "Recommended Action"}
        widths = {"severity": 80, "area": 120, "title": 230, "action": 420}
        for col in cols:
            self.issue_tree.heading(col, text=headings[col], anchor="w")
            self.issue_tree.column(col, width=widths[col], anchor="w", stretch=(col == "action"))
        self.issue_tree.tag_configure("CRITICAL", foreground=t.alarm)
        self.issue_tree.tag_configure("WARN", foreground=t.warn)
        self.issue_tree.tag_configure("INFO", foreground=t.text_muted)
        self.issue_tree.pack(fill="both", expand=True, padx=10, pady=(0, 8))
        self.issue_tree.bind("<Double-1>", self._on_issue_open)

        side = tk.Frame(body, bg=t.bg)
        side.grid(row=0, column=1, sticky="nsew")
        side.grid_rowconfigure(0, weight=1)
        side.grid_rowconfigure(1, weight=0)

        contrib = tk.Frame(side, bg=t.card, highlightthickness=1, highlightbackground=t.border)
        contrib.grid(row=0, column=0, sticky="nsew")
        tk.Frame(contrib, bg=t.good, height=2).pack(fill="x", side="top")
        tk.Label(
            contrib,
            text="Meter Contribution",
            bg=t.card,
            fg=t.text,
            font=("Segoe UI", 11, "bold"),
        ).pack(anchor="w", padx=10, pady=(8, 4))

        ccols = ("meter", "role", "kw", "share")
        self.contrib_tree = ttk.Treeview(contrib, columns=ccols, show="headings", height=9)
        cheads = {"meter": "Meter", "role": "Role", "kw": "Signed kW", "share": "Share"}
        cwidths = {"meter": 180, "role": 70, "kw": 85, "share": 70}
        for col in ccols:
            self.contrib_tree.heading(col, text=cheads[col], anchor="w")
            self.contrib_tree.column(col, width=cwidths[col], anchor="w", stretch=(col == "meter"))
        self.contrib_tree.pack(fill="both", expand=True, padx=10, pady=(0, 8))

        actions = tk.Frame(side, bg=t.card, highlightthickness=1, highlightbackground=t.border)
        actions.grid(row=1, column=0, sticky="ew", pady=(8, 0))
        tk.Label(
            actions,
            text="Open related page",
            bg=t.card,
            fg=t.text_muted,
            font=("Segoe UI", 8, "bold"),
        ).pack(anchor="w", padx=10, pady=(8, 4))
        btns = tk.Frame(actions, bg=t.card)
        btns.pack(fill="x", padx=10, pady=(0, 10))
        for key, label in (("status", "Meter Status"), ("comm", "Communication"), ("logging", "Data Logger"), ("reports", "Reports")):
            btn = ttk.Button(btns, text=label, style="Small.TButton", command=lambda k=key: self._open_page(k))
            if not self.can_open(key):
                btn.configure(state="disabled")
            btn.pack(side="left", padx=(0, 6))
        ttk.Button(btns, text="Export CSV", style="Small.TButton", command=self._export_csv).pack(side="right")

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
        data = self.engine.analyze(self.meters, total)
        self._last_data = data

        score = int(data.get("score", 0) or 0)
        state = str(data.get("state", "") or "")
        self._metric_vars["score"].set(f"{score}/100  {state}")
        plant_kw = data.get("plant_kw")
        self._metric_vars["plant_kw"].set(f"{_fmt(plant_kw, 2)} kW" if plant_kw is not None else "-")
        demand = data.get("demand", {}) or {}
        forecast = demand.get("forecast_kw")
        conf = str(demand.get("confidence", "LOW") or "LOW")
        self._metric_vars["forecast"].set(f"{_fmt(forecast, 2)} kW  {conf}" if forecast is not None else f"Warming up  {conf}")
        pf = data.get("pf")
        self._metric_vars["pf"].set(_fmt(pf, 3))
        self._metric_vars["confidence"].set(f"{_fmt(data.get('data_confidence_pct'), 1)}%")
        self._summary_var.set(time.strftime("Updated %Y-%m-%d %H:%M:%S", time.localtime(float(data.get("ts", time.time())))))

        self.issue_tree.delete(*self.issue_tree.get_children())
        issues = data.get("issues", []) or []
        if not issues:
            self.issue_tree.insert("", "end", values=("OK", "Plant", "No active analytics findings", "Continue normal monitoring"), tags=("INFO",))
        else:
            for idx, issue in enumerate(issues):
                sev = str(issue.get("severity", "INFO") or "INFO").upper()
                self.issue_tree.insert(
                    "",
                    "end",
                    iid=f"issue_{idx}",
                    values=(sev, issue.get("area", ""), issue.get("title", ""), issue.get("action", "")),
                    tags=(sev,),
                )
        self._issue_pages = {f"issue_{idx}": str(issue.get("page", "") or "") for idx, issue in enumerate(issues)}

        self.contrib_tree.delete(*self.contrib_tree.get_children())
        for row in data.get("contributions", []) or []:
            self.contrib_tree.insert(
                "",
                "end",
                values=(
                    row.get("name", ""),
                    row.get("role", ""),
                    _fmt(row.get("signed_kw"), 2),
                    f"{_fmt(row.get('contribution_pct'), 1)}%",
                ),
            )

    def _on_issue_open(self, _event=None) -> None:
        try:
            sel = self.issue_tree.selection()
            if not sel:
                return
            page = getattr(self, "_issue_pages", {}).get(sel[0], "")
            if page:
                self._open_page(page)
        except Exception:
            pass

    def _export_csv(self) -> None:
        data = self._last_data or {}
        path = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
            title="Export Plant Analytics",
        )
        if not path:
            return
        try:
            with open(path, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow(["Plant Analytics"])
                w.writerow(["Timestamp", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(float(data.get("ts", time.time()))))])
                w.writerow(["Health Score", data.get("score", "")])
                w.writerow(["State", data.get("state", "")])
                w.writerow(["Live kW", data.get("plant_kw", "")])
                w.writerow(["Power Factor", data.get("pf", "")])
                w.writerow(["Frequency Hz", data.get("frequency", "")])
                w.writerow(["Average Voltage", data.get("vavg", "")])
                w.writerow(["Data Confidence %", data.get("data_confidence_pct", "")])
                demand = data.get("demand", {}) or {}
                w.writerow(["Forecast kW", demand.get("forecast_kw", "")])
                w.writerow(["Forecast Confidence", demand.get("confidence", "")])
                w.writerow([])
                w.writerow(["Findings"])
                w.writerow(["Severity", "Area", "Title", "Detail", "Action", "Page"])
                for issue in data.get("issues", []) or []:
                    w.writerow([
                        issue.get("severity", ""),
                        issue.get("area", ""),
                        issue.get("title", ""),
                        issue.get("detail", ""),
                        issue.get("action", ""),
                        issue.get("page", ""),
                    ])
                w.writerow([])
                w.writerow(["Meter Contribution"])
                w.writerow(["Meter ID", "Name", "Role", "kW", "Signed kW", "Share %", "PF", "Vavg"])
                for row in data.get("contributions", []) or []:
                    w.writerow([
                        row.get("meter_id", ""),
                        row.get("name", ""),
                        row.get("role", ""),
                        row.get("kw", ""),
                        row.get("signed_kw", ""),
                        row.get("contribution_pct", ""),
                        row.get("pf", ""),
                        row.get("vavg", ""),
                    ])
            messagebox.showinfo("Export", f"Saved analytics report:\n{path}")
        except Exception as exc:
            messagebox.showerror("Export failed", str(exc))
