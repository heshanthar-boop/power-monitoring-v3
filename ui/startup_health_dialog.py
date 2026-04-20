from __future__ import annotations

import tkinter as tk
from tkinter import ttk
from typing import Callable, Iterable, Optional

from utils.startup_health import StartupHealthCheck, run_startup_health

_BG = "#0f1419"
_PANEL = "#17202a"
_SURFACE = "#202b36"
_BORDER = "#2f3f4f"
_TEXT = "#ecf2f8"
_MUTED = "#a8b3bd"
_ACCENT = "#37b5a5"
_WARN = "#f2b84b"
_ERROR = "#ef5d5d"
_OK = "#49d17d"


def _level_counts(checks: Iterable[StartupHealthCheck]) -> dict[str, int]:
    counts = {"OK": 0, "WARN": 0, "ERROR": 0}
    for c in checks:
        level = str(getattr(c, "level", "WARN") or "WARN").upper()
        counts[level] = counts.get(level, 0) + 1
    return counts


class StartupHealthDialog(tk.Tk):
    def __init__(
        self,
        cfg: dict,
        checks: list[StartupHealthCheck],
        *,
        session: Optional[dict] = None,
        refresh_cb: Optional[Callable[[dict], list[StartupHealthCheck]]] = None,
    ) -> None:
        super().__init__()
        self.cfg = cfg
        self.checks = list(checks or [])
        self.session = dict(session or {})
        self.refresh_cb = refresh_cb or run_startup_health
        self.result = False

        self.title("Startup Health - MFM384 Monitoring")
        self.geometry("940x560")
        self.minsize(860, 500)
        self.configure(bg=_BG)
        self.protocol("WM_DELETE_WINDOW", self._continue)

        try:
            self.attributes("-topmost", True)
            self.after(700, lambda: self.attributes("-topmost", False))
        except Exception:
            pass

        self._build_ui()
        self._populate()
        self.update_idletasks()
        self._center()
        self.lift()

    def _center(self) -> None:
        sw = int(self.winfo_screenwidth() or 1280)
        sh = int(self.winfo_screenheight() or 720)
        w = max(860, int(self.winfo_width() or 940))
        h = max(500, int(self.winfo_height() or 560))
        x = max(20, (sw - w) // 2)
        y = max(20, (sh - h) // 3)
        self.geometry(f"{w}x{h}+{x}+{y}")

    def _build_ui(self) -> None:
        outer = tk.Frame(self, bg=_BG)
        outer.pack(fill="both", expand=True, padx=16, pady=16)

        header = tk.Frame(outer, bg=_BG)
        header.pack(fill="x")

        title_box = tk.Frame(header, bg=_BG)
        title_box.pack(side="left", fill="x", expand=True)
        tk.Label(title_box, text="Startup Health", bg=_BG, fg=_TEXT, font=("Segoe UI", 20, "bold")).pack(anchor="w")
        site = self.cfg.get("site", {}) or {}
        plant = str(site.get("plant_name", "MFM384 Monitoring") or "MFM384 Monitoring")
        location = str(site.get("location", "") or "").strip()
        sub = plant if not location else f"{plant} - {location}"
        user = str(self.session.get("username", "") or "").strip()
        role = str(self.session.get("role", "") or "").strip()
        user_part = f" | User: {user} ({role})" if user else ""
        tk.Label(title_box, text=sub + user_part, bg=_BG, fg=_MUTED, font=("Segoe UI", 9)).pack(anchor="w", pady=(2, 0))

        self.summary_var = tk.StringVar()
        self.summary_label = tk.Label(
            header,
            textvariable=self.summary_var,
            bg=_SURFACE,
            fg=_TEXT,
            font=("Segoe UI", 10, "bold"),
            padx=14,
            pady=8,
        )
        self.summary_label.pack(side="right", padx=(12, 0))

        body = tk.Frame(outer, bg=_PANEL, highlightthickness=1, highlightbackground=_BORDER)
        body.pack(fill="both", expand=True, pady=(14, 12))

        guide = tk.Label(
            body,
            text="Review these checks before the operator station starts. Fix ERROR items first; WARN items can run but need attention.",
            bg=_PANEL,
            fg=_MUTED,
            font=("Segoe UI", 9),
            anchor="w",
            padx=12,
            pady=10,
        )
        guide.pack(fill="x")

        columns = ("level", "check", "message", "action")
        self.tree = ttk.Treeview(body, columns=columns, show="headings", height=14)
        self.tree.heading("level", text="Status")
        self.tree.heading("check", text="Check")
        self.tree.heading("message", text="Finding")
        self.tree.heading("action", text="Operator action")
        self.tree.column("level", width=82, anchor="center", stretch=False)
        self.tree.column("check", width=130, anchor="w", stretch=False)
        self.tree.column("message", width=360, anchor="w", stretch=True)
        self.tree.column("action", width=330, anchor="w", stretch=True)
        self.tree.tag_configure("OK", foreground=_OK)
        self.tree.tag_configure("WARN", foreground=_WARN)
        self.tree.tag_configure("ERROR", foreground=_ERROR)
        self.tree.pack(side="left", fill="both", expand=True, padx=(12, 0), pady=(0, 12))

        sb = ttk.Scrollbar(body, orient="vertical", command=self.tree.yview)
        sb.pack(side="right", fill="y", padx=(0, 12), pady=(0, 12))
        self.tree.configure(yscrollcommand=sb.set)

        footer = tk.Frame(outer, bg=_BG)
        footer.pack(fill="x")
        self.info_var = tk.StringVar()
        tk.Label(footer, textvariable=self.info_var, bg=_BG, fg=_MUTED, font=("Segoe UI", 9)).pack(side="left", fill="x", expand=True)

        tk.Button(
            footer,
            text="Refresh",
            bg=_SURFACE,
            fg=_TEXT,
            bd=0,
            padx=14,
            pady=8,
            activebackground=_BORDER,
            activeforeground=_TEXT,
            command=self._refresh,
        ).pack(side="right", padx=(8, 0))
        tk.Button(
            footer,
            text="Copy Summary",
            bg=_SURFACE,
            fg=_TEXT,
            bd=0,
            padx=14,
            pady=8,
            activebackground=_BORDER,
            activeforeground=_TEXT,
            command=self._copy_summary,
        ).pack(side="right", padx=(8, 0))
        self.continue_btn = tk.Button(
            footer,
            text="Continue",
            bg=_ACCENT,
            fg="#00110f",
            bd=0,
            padx=18,
            pady=8,
            font=("Segoe UI", 10, "bold"),
            activebackground="#65d3c5",
            activeforeground="#00110f",
            command=self._continue,
        )
        self.continue_btn.pack(side="right", padx=(8, 0))
        tk.Button(
            footer,
            text="Exit App",
            bg="#3b1f27",
            fg="#ffd5dd",
            bd=0,
            padx=14,
            pady=8,
            activebackground="#5b2d39",
            activeforeground="#ffffff",
            command=self._cancel,
        ).pack(side="right", padx=(8, 0))

    def _populate(self) -> None:
        for iid in self.tree.get_children():
            self.tree.delete(iid)
        for check in self.checks:
            level = str(check.level or "WARN").upper()
            self.tree.insert(
                "",
                "end",
                values=(level, check.name, check.message, check.action or "Review and correct before continuous operation."),
                tags=(level,),
            )

        counts = _level_counts(self.checks)
        if counts.get("ERROR", 0):
            text = f"CRITICAL: {counts['ERROR']} error(s), {counts.get('WARN', 0)} warning(s)"
            self.summary_label.configure(bg="#3b1f27", fg="#ffd5dd")
            self.continue_btn.configure(text="Continue Anyway")
            self.info_var.set("Errors can cause data loss or failed communication. Continue only for troubleshooting.")
        elif counts.get("WARN", 0):
            text = f"ATTENTION: {counts['WARN']} warning(s)"
            self.summary_label.configure(bg="#4a371d", fg="#ffe7af")
            self.continue_btn.configure(text="Continue")
            self.info_var.set("Warnings allow startup, but fix them before leaving the site unattended.")
        else:
            text = "READY: all checks OK"
            self.summary_label.configure(bg="#16352f", fg="#b9fff5")
            self.continue_btn.configure(text="Continue")
            self.info_var.set("Startup checks passed.")
        self.summary_var.set(text)

    def _summary_text(self) -> str:
        lines = ["MFM384 Startup Health"]
        counts = _level_counts(self.checks)
        lines.append(f"Summary: OK={counts.get('OK', 0)} WARN={counts.get('WARN', 0)} ERROR={counts.get('ERROR', 0)}")
        for c in self.checks:
            lines.append(f"[{str(c.level).upper()}] {c.name}: {c.message}")
            if c.action:
                lines.append(f"  Action: {c.action}")
        return "\n".join(lines)

    def _copy_summary(self) -> None:
        try:
            self.clipboard_clear()
            self.clipboard_append(self._summary_text())
            self.info_var.set("Startup health summary copied to clipboard.")
        except Exception as exc:
            self.info_var.set(f"Could not copy summary: {exc}")

    def _refresh(self) -> None:
        try:
            self.checks = list(self.refresh_cb(self.cfg) or [])
            self._populate()
        except Exception as exc:
            self.info_var.set(f"Refresh failed: {exc}")

    def _continue(self) -> None:
        self.result = True
        self.quit()
        self.destroy()

    def _cancel(self) -> None:
        self.result = False
        self.quit()
        self.destroy()


def show_startup_health(
    cfg: dict,
    checks: list[StartupHealthCheck] | None = None,
    *,
    session: Optional[dict] = None,
) -> bool:
    dlg = StartupHealthDialog(cfg, list(checks or run_startup_health(cfg)), session=session)
    try:
        dlg.mainloop()
        return bool(dlg.result)
    finally:
        try:
            if dlg.winfo_exists():
                dlg.destroy()
        except Exception:
            pass
