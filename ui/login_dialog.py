from __future__ import annotations

import tkinter as tk
from tkinter import ttk
from typing import Optional


_BG      = "#141414"
_PANEL   = "#1c1c1e"
_SURFACE = "#2c2c2e"
_BORDER  = "#38383a"
_TEXT    = "#e8e4dc"
_MUTED   = "#9a9490"
_DIM     = "#5e5a56"
_ACCENT  = "#4da6ff"
_ALARM   = "#c03030"


class LoginDialog(tk.Tk):
    def __init__(self, auth_service):
        super().__init__()
        self.auth_service = auth_service
        self.result = None
        self._pending_session = None   # session dict from password phase (TOTP pending)

        self.title("MFM384 Monitoring")
        self.resizable(False, False)
        self.configure(bg=_BG)
        self.protocol("WM_DELETE_WINDOW", self._cancel)

        try:
            self.attributes("-topmost", True)
            self.after(600, lambda: self.attributes("-topmost", False))
        except Exception:
            pass

        self._build_ui()
        self.update_idletasks()
        self._center(width=380, height=270)
        self.user_ent.focus_set()
        self.lift()

    def _build_ui(self):
        # Outer border frame
        border = tk.Frame(self, bg=_ACCENT, padx=1, pady=1)
        border.pack(fill="both", expand=True)

        card = tk.Frame(border, bg=_PANEL)
        card.pack(fill="both", expand=True)

        # Top accent strip
        tk.Frame(card, bg=_ACCENT, height=3).pack(fill="x")

        # Header
        hdr = tk.Frame(card, bg=_PANEL)
        hdr.pack(fill="x", padx=24, pady=(20, 0))
        tk.Label(hdr, text="MFM384 Monitoring",
                 bg=_PANEL, fg=_TEXT,
                 font=("Segoe UI", 14, "bold")).pack(anchor="w")
        tk.Label(hdr, text="Enter your credentials to continue",
                 bg=_PANEL, fg=_MUTED,
                 font=("Segoe UI", 8)).pack(anchor="w", pady=(2, 0))

        tk.Frame(card, bg=_BORDER, height=1).pack(fill="x", padx=24, pady=(12, 0))

        # Fields
        fields = tk.Frame(card, bg=_PANEL)
        fields.pack(fill="x", padx=24, pady=14)

        # Username
        tk.Label(fields, text="USERNAME", bg=_PANEL, fg=_MUTED,
                 font=("Segoe UI", 7, "bold")).pack(anchor="w")
        self.user_var = tk.StringVar()
        self.user_ent = tk.Entry(
            fields, textvariable=self.user_var,
            bg=_SURFACE, fg=_TEXT, insertbackground=_TEXT,
            bd=0, highlightthickness=1,
            highlightbackground=_BORDER, highlightcolor=_ACCENT,
            font=("Segoe UI", 10), relief="flat",
        )
        self.user_ent.pack(fill="x", ipady=6, pady=(2, 10))

        # Password
        tk.Label(fields, text="PASSWORD", bg=_PANEL, fg=_MUTED,
                 font=("Segoe UI", 7, "bold")).pack(anchor="w")
        self.pass_var = tk.StringVar()
        self.pass_ent = tk.Entry(
            fields, textvariable=self.pass_var, show="●",
            bg=_SURFACE, fg=_TEXT, insertbackground=_TEXT,
            bd=0, highlightthickness=1,
            highlightbackground=_BORDER, highlightcolor=_ACCENT,
            font=("Segoe UI", 10), relief="flat",
        )
        self.pass_ent.pack(fill="x", ipady=6, pady=(2, 0))

        # TOTP field (hidden until password phase passes)
        self._totp_frame = tk.Frame(fields, bg=_PANEL)
        # NOT packed yet — shown dynamically in _show_totp_step()
        tk.Label(self._totp_frame, text="AUTHENTICATOR CODE  (6 digits)",
                 bg=_PANEL, fg=_MUTED,
                 font=("Segoe UI", 7, "bold")).pack(anchor="w")
        self.totp_var = tk.StringVar()
        self._totp_ent = tk.Entry(
            self._totp_frame, textvariable=self.totp_var,
            bg=_SURFACE, fg=_TEXT, insertbackground=_TEXT,
            bd=0, highlightthickness=1,
            highlightbackground=_BORDER, highlightcolor=_ACCENT,
            font=("Segoe UI", 14, "bold"), relief="flat",
            width=10, justify="center",
        )
        self._totp_ent.pack(fill="x", ipady=8, pady=(3, 0))
        tk.Label(self._totp_frame,
                 text="Open your authenticator app and enter the 6-digit code.",
                 bg=_PANEL, fg=_DIM, font=("Segoe UI", 7)).pack(anchor="w", pady=(3, 0))

        # Error label
        self.info_var = tk.StringVar()
        self._err_lbl = tk.Label(
            fields, textvariable=self.info_var,
            bg=_PANEL, fg="#ff6060",
            font=("Segoe UI", 8), anchor="w",
        )
        self._err_lbl.pack(anchor="w", pady=(4, 0))

        # Buttons
        btns = tk.Frame(card, bg=_PANEL)
        btns.pack(fill="x", padx=24, pady=(0, 20))

        self._btn_login = tk.Button(
            btns, text="Login",
            bg=_ACCENT, fg="#000000",
            font=("Segoe UI", 10, "bold"), bd=0,
            padx=20, pady=6,
            activebackground="#6ab8ff", activeforeground="#000000",
            cursor="hand2",
            command=self._login,
        )
        self._btn_login.pack(side="right")

        tk.Button(
            btns, text="Cancel",
            bg=_SURFACE, fg=_MUTED,
            font=("Segoe UI", 9), bd=0,
            padx=12, pady=6,
            activebackground=_BORDER, activeforeground=_TEXT,
            cursor="hand2",
            command=self._cancel,
        ).pack(side="right", padx=(0, 8))

        # Enter / Escape bindings
        self.bind("<Return>", lambda _e: self._login())
        self.bind("<Escape>", lambda _e: self._cancel())

    def _center(self, width: int, height: int) -> None:
        sw = int(self.winfo_screenwidth()  or 1280)
        sh = int(self.winfo_screenheight() or 720)
        x  = max(20, (sw - width)  // 2)
        y  = max(20, (sh - height) // 3)
        self.geometry(f"{width}x{height}+{x}+{y}")

    def _show_totp_step(self) -> None:
        """Switch the dialog to TOTP input mode (second factor)."""
        self._totp_frame.pack(fill="x", pady=(10, 0))
        self.user_ent.configure(state="disabled")
        self.pass_ent.configure(state="disabled")
        self._btn_login.configure(text="Verify Code")
        self.totp_var.set("")
        self._center(width=380, height=340)
        self._totp_ent.focus_set()

    def _login(self):
        # ── Phase 2: TOTP verification ──────────────────────────────────────
        if self._pending_session is not None:
            code = self.totp_var.get().strip()
            if len(code) != 6 or not code.isdigit():
                self.info_var.set("Enter a valid 6-digit code.")
                return
            self.info_var.set("")
            self._btn_login.configure(state="disabled", text="Verifying…")
            self.update_idletasks()
            username = str(self._pending_session.get("username", ""))
            ok = self.auth_service.verify_totp_for_user(username, code)
            self._btn_login.configure(state="normal", text="Verify Code")
            if not ok:
                self.info_var.set("Incorrect code. Try again.")
                self.totp_var.set("")
                self._totp_ent.focus_set()
                return
            # TOTP passed — finalise session (strip totp_required flag)
            session = dict(self._pending_session)
            session.pop("totp_required", None)
            self.auth_service.record_audit(
                actor=username, action="LOGIN_TOTP_OK",
                detail="2FA verified", success=True,
            )
            self._finish_session(session)
            return

        # ── Phase 1: password ────────────────────────────────────────────────
        user = self.user_var.get().strip()
        pw   = self.pass_var.get()
        if not user or not pw:
            self.info_var.set("Username and password are required.")
            return
        self.info_var.set("")
        self._btn_login.configure(state="disabled", text="Authenticating…")
        self.update_idletasks()
        # Use authenticate_password which returns totp_required if 2FA enabled
        session = self.auth_service.authenticate_password(user, pw)
        self._btn_login.configure(state="normal", text="Login")
        if not session:
            self.info_var.set("Invalid credentials. Please try again.")
            self.pass_var.set("")
            self.pass_ent.focus_set()
            return
        if session.get("totp_required"):
            self._pending_session = session
            self._show_totp_step()
            return
        self._finish_session(session)

    def _finish_session(self, session: dict) -> None:
        if session.get("must_change_password"):
            if not self._prompt_password_change(session):
                return
            session = dict(session)
            session.pop("must_change_password", None)
        self.result = session
        self.quit()
        self.destroy()

    def _prompt_password_change(self, session: dict) -> bool:
        username = str((session or {}).get("username", "") or "")
        dlg = tk.Toplevel(self)
        dlg.title("Change Password Required")
        dlg.resizable(False, False)
        dlg.configure(bg=_PANEL)
        dlg.transient(self)
        dlg.grab_set()

        box = tk.Frame(dlg, bg=_PANEL)
        box.pack(fill="both", expand=True, padx=22, pady=18)
        tk.Label(
            box,
            text="Change temporary password",
            bg=_PANEL,
            fg=_TEXT,
            font=("Segoe UI", 12, "bold"),
        ).pack(anchor="w")
        tk.Label(
            box,
            text="This account is using a default or temporary password. Set a stronger password to continue.",
            bg=_PANEL,
            fg=_MUTED,
            font=("Segoe UI", 8),
            wraplength=330,
            justify="left",
        ).pack(anchor="w", pady=(4, 12))

        old_var = tk.StringVar(value=self.pass_var.get())
        new_var = tk.StringVar()
        confirm_var = tk.StringVar()
        msg_var = tk.StringVar()

        def _entry(label: str, var: tk.StringVar):
            tk.Label(box, text=label, bg=_PANEL, fg=_MUTED, font=("Segoe UI", 7, "bold")).pack(anchor="w")
            ent = tk.Entry(
                box,
                textvariable=var,
                show="●",
                bg=_SURFACE,
                fg=_TEXT,
                insertbackground=_TEXT,
                bd=0,
                highlightthickness=1,
                highlightbackground=_BORDER,
                highlightcolor=_ACCENT,
                font=("Segoe UI", 10),
                relief="flat",
            )
            ent.pack(fill="x", ipady=6, pady=(2, 10))
            return ent

        _entry("CURRENT PASSWORD", old_var)
        new_ent = _entry("NEW PASSWORD", new_var)
        _entry("CONFIRM NEW PASSWORD", confirm_var)
        tk.Label(box, textvariable=msg_var, bg=_PANEL, fg="#ff6060", font=("Segoe UI", 8)).pack(anchor="w", pady=(0, 10))

        result = {"ok": False}

        def _save():
            if new_var.get() != confirm_var.get():
                msg_var.set("New password and confirmation do not match.")
                return
            ok, message = self.auth_service.change_password(
                actor=username,
                username=username,
                old_password=old_var.get(),
                new_password=new_var.get(),
            )
            if not ok:
                msg_var.set(message or "Password change failed.")
                return
            result["ok"] = True
            dlg.destroy()

        btns = tk.Frame(box, bg=_PANEL)
        btns.pack(fill="x")
        tk.Button(
            btns,
            text="Save Password",
            bg=_ACCENT,
            fg="#000000",
            font=("Segoe UI", 9, "bold"),
            bd=0,
            padx=14,
            pady=6,
            command=_save,
        ).pack(side="right")
        tk.Button(
            btns,
            text="Cancel Login",
            bg=_SURFACE,
            fg=_MUTED,
            font=("Segoe UI", 9),
            bd=0,
            padx=12,
            pady=6,
            command=dlg.destroy,
        ).pack(side="right", padx=(0, 8))

        dlg.bind("<Return>", lambda _e: _save())
        dlg.bind("<Escape>", lambda _e: dlg.destroy())
        dlg.update_idletasks()
        x = self.winfo_rootx() + max(20, (self.winfo_width() - 390) // 2)
        y = self.winfo_rooty() + 30
        dlg.geometry(f"390x390+{x}+{y}")
        new_ent.focus_set()
        self.wait_window(dlg)
        return bool(result["ok"])

    def _cancel(self):
        self.result = None
        try:
            self.quit()
        except Exception:
            pass
        try:
            self.destroy()
        except Exception:
            pass


def prompt_login(auth_service) -> Optional[dict]:
    dlg = LoginDialog(auth_service)
    try:
        dlg.mainloop()
        return dlg.result
    finally:
        try:
            if dlg.winfo_exists():
                dlg.destroy()
        except Exception:
            pass
