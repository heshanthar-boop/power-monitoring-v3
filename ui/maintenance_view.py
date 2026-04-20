from __future__ import annotations

import os
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
from typing import Any, Optional

from utils.backup_restore import create_full_backup, create_support_bundle, restore_config_from_backup
from utils.startup_health import run_startup_health


class MaintenanceTab(ttk.Frame):
    def __init__(
        self,
        parent,
        cfg: dict,
        *,
        auth_service: Any = None,
        session: Optional[dict] = None,
        logging_engine: Any = None,
        data_store: Any = None,
        snapshot_bus: Any = None,
        worker: Any = None,
    ):
        super().__init__(parent, padding=12)
        self.cfg = cfg
        self.auth_service = auth_service
        self.session = dict(session or {})
        self.logging_engine = logging_engine
        self.data_store = data_store
        self.snapshot_bus = snapshot_bus
        self.worker = worker
        self.status_var = tk.StringVar(value="Ready.")
        self._build_ui()
        self._refresh_health()

    def _actor(self) -> str:
        return str(self.session.get("username", "") or "system")

    def _audit(self, action: str, detail: str = "", success: bool = True) -> None:
        if self.auth_service is None:
            return
        try:
            self.auth_service.record_audit(actor=self._actor(), action=action, detail=detail, success=success)
        except Exception:
            pass

    def _build_ui(self) -> None:
        self.columnconfigure(0, weight=1)
        self.rowconfigure(2, weight=1)

        header = ttk.Frame(self)
        header.grid(row=0, column=0, sticky="ew")
        ttk.Label(header, text="Maintenance & Backup", style="H2.TLabel").pack(side="left")
        ttk.Label(
            header,
            text="Create backups before field changes. Support bundles are redacted for troubleshooting.",
            style="Muted.TLabel",
        ).pack(side="left", padx=(12, 0))

        actions = ttk.LabelFrame(self, text="Actions", padding=10)
        actions.grid(row=1, column=0, sticky="ew", pady=(12, 10))
        for c in range(4):
            actions.columnconfigure(c, weight=1)

        ttk.Button(actions, text="Run Health Check", command=self._refresh_health).grid(row=0, column=0, sticky="ew", padx=4)
        ttk.Button(actions, text="Create Full Backup", command=self._create_full_backup).grid(row=0, column=1, sticky="ew", padx=4)
        ttk.Button(actions, text="Create Support Bundle", command=self._create_support_bundle).grid(row=0, column=2, sticky="ew", padx=4)
        ttk.Button(actions, text="Restore Config From Backup", command=self._restore_config).grid(row=0, column=3, sticky="ew", padx=4)

        note = ttk.Label(
            actions,
            text="Full backup includes config and SQLite DBs. Config restore requires app restart; live DB restore is intentionally not done while running.",
            style="Muted.TLabel",
        )
        note.grid(row=1, column=0, columnspan=4, sticky="w", padx=4, pady=(8, 0))

        health = ttk.LabelFrame(self, text="Startup Health", padding=8)
        health.grid(row=2, column=0, sticky="nsew")
        health.rowconfigure(0, weight=1)
        health.columnconfigure(0, weight=1)

        self.tree = ttk.Treeview(health, columns=("level", "check", "message", "action"), show="headings", height=12)
        for key, title, width in (
            ("level", "Status", 80),
            ("check", "Check", 150),
            ("message", "Finding", 380),
            ("action", "Operator Action", 420),
        ):
            self.tree.heading(key, text=title)
            self.tree.column(key, width=width, anchor="w", stretch=(key in ("message", "action")))
        self.tree.tag_configure("OK", foreground="#30c060")
        self.tree.tag_configure("WARN", foreground="#d08030")
        self.tree.tag_configure("ERROR", foreground="#c03030")
        self.tree.grid(row=0, column=0, sticky="nsew")
        sb = ttk.Scrollbar(health, orient="vertical", command=self.tree.yview)
        sb.grid(row=0, column=1, sticky="ns")
        self.tree.configure(yscrollcommand=sb.set)

        footer = ttk.Frame(self)
        footer.grid(row=3, column=0, sticky="ew", pady=(8, 0))
        ttk.Label(footer, textvariable=self.status_var, style="Muted.TLabel").pack(side="left", fill="x", expand=True)

    def _refresh_health(self) -> None:
        try:
            checks = run_startup_health(self.cfg)
            for iid in self.tree.get_children():
                self.tree.delete(iid)
            for c in checks:
                level = str(c.level or "WARN").upper()
                self.tree.insert("", "end", values=(level, c.name, c.message, c.action), tags=(level,))
            errors = sum(1 for c in checks if str(c.level).upper() == "ERROR")
            warnings = sum(1 for c in checks if str(c.level).upper() == "WARN")
            self.status_var.set(f"Health check complete: {errors} error(s), {warnings} warning(s).")
            self._audit("MAINT_HEALTH_CHECK", f"errors={errors} warnings={warnings}", True)
        except Exception as exc:
            self.status_var.set(f"Health check failed: {exc}")
            self._audit("MAINT_HEALTH_CHECK", str(exc), False)

    def _create_full_backup(self) -> None:
        if not messagebox.askyesno(
            "Create Full Backup",
            "Full backup may include credentials from config.json. Store it securely. Continue?",
            parent=self,
        ):
            return
        try:
            path = create_full_backup(self.cfg, auth_service=self.auth_service)
            self.status_var.set(f"Full backup created: {path}")
            self._audit("FULL_BACKUP_CREATED", path, True)
            messagebox.showinfo("Backup Created", f"Full backup created:\n\n{path}", parent=self)
        except Exception as exc:
            self.status_var.set(f"Full backup failed: {exc}")
            self._audit("FULL_BACKUP_FAILED", str(exc), False)
            messagebox.showerror("Backup Failed", str(exc), parent=self)

    def _create_support_bundle(self) -> None:
        try:
            path = create_support_bundle(
                self.cfg,
                auth_service=self.auth_service,
                logging_engine=self.logging_engine,
                data_store=self.data_store,
                snapshot_bus=self.snapshot_bus,
                worker=self.worker,
            )
            self.status_var.set(f"Support bundle created: {path}")
            self._audit("SUPPORT_BUNDLE_CREATED", path, True)
            messagebox.showinfo("Support Bundle Created", f"Support bundle created:\n\n{path}", parent=self)
        except Exception as exc:
            self.status_var.set(f"Support bundle failed: {exc}")
            self._audit("SUPPORT_BUNDLE_FAILED", str(exc), False)
            messagebox.showerror("Support Bundle Failed", str(exc), parent=self)

    def _restore_config(self) -> None:
        path = filedialog.askopenfilename(
            parent=self,
            title="Select SCADA backup ZIP",
            filetypes=(("Backup ZIP", "*.zip"), ("All files", "*.*")),
        )
        if not path:
            return
        if not messagebox.askyesno(
            "Restore Config",
            "This restores config.json only and creates a pre-restore backup of the current config. Restart the app after restore. Continue?",
            parent=self,
        ):
            return
        try:
            restored = restore_config_from_backup(path)
            self.status_var.set(f"Config restored to {restored}. Restart required.")
            self._audit("CONFIG_RESTORED", os.path.basename(path), True)
            messagebox.showinfo("Config Restored", f"Config restored to:\n\n{restored}\n\nRestart the app now.", parent=self)
        except Exception as exc:
            self.status_var.set(f"Config restore failed: {exc}")
            self._audit("CONFIG_RESTORE_FAILED", str(exc), False)
            messagebox.showerror("Restore Failed", str(exc), parent=self)
