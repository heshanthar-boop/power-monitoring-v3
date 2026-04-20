# ui/setup_guard.py
from __future__ import annotations
import tkinter as tk
from tkinter import ttk, messagebox

from utils.security import verify_setup_password, setup_password_is_configured

class SetupWriteGuard:
    """
    Session-only unlock. Locks again on app restart.
    """
    def __init__(self, parent, cfg: dict):
        self.parent = parent
        self.cfg = cfg
        self._unlocked = False

    @property
    def unlocked(self) -> bool:
        return self._unlocked

    def lock(self):
        self._unlocked = False

    def unlock_dialog(self) -> bool:
        if not setup_password_is_configured(self.cfg):
            self._unlocked = True
            return True

        dlg = tk.Toplevel(self.parent)
        dlg.title("Unlock Setup Writes")
        dlg.transient(self.parent)
        dlg.grab_set()
        dlg.resizable(False, False)

        frm = ttk.Frame(dlg, padding=12)
        frm.pack(fill="both", expand=True)

        ttk.Label(frm, text="Setup writes are LOCKED for safety.", font=("Segoe UI", 11, "bold")).pack(anchor="w")
        ttk.Label(frm, text="Enter password to unlock for this session only.").pack(anchor="w", pady=(4, 10))

        row = ttk.Frame(frm)
        row.pack(fill="x")

        ttk.Label(row, text="Password:").pack(side="left")
        ent = ttk.Entry(row, show="*", width=16)
        ent.pack(side="left", padx=8)
        ent.focus_set()

        ok = {"val": False}

        def do_unlock():
            pw = ent.get().strip()
            if not verify_setup_password(self.cfg, pw):
                messagebox.showerror("Wrong password", "Password is incorrect.", parent=dlg)
                return
            if not messagebox.askyesno(
                "Confirm",
                "Unlock setup writes?\n\nWARNING: Wrong writes can break measurements/communications.",
                parent=dlg,
            ):
                return
            self._unlocked = True
            ok["val"] = True
            dlg.destroy()

        def cancel():
            dlg.destroy()

        btns = ttk.Frame(frm)
        btns.pack(fill="x", pady=(12, 0))
        ttk.Button(btns, text="Unlock", command=do_unlock).pack(side="right")
        ttk.Button(btns, text="Cancel", command=cancel).pack(side="right", padx=8)

        dlg.bind("<Return>", lambda e: do_unlock())
        dlg.bind("<Escape>", lambda e: cancel())
        self.parent.wait_window(dlg)
        return ok["val"]
