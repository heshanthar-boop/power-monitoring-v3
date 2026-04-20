"""
ui/pin_lock.py — Operator PIN lock for the SCADA application.

Design:
- PIN stored as SHA-256 hash in cfg["ui"]["pin_hash"] (never plaintext)
- Lock overlays the entire root window with a full-screen frame
- Unlock via PIN dialog (max 3 attempts, then 30s lockout)
- Auto-lock after configurable idle minutes (0 = disabled)
- Lock button in top bar; locked state shown in status bar
- PIN setup / change via dialog (requires existing PIN if set)

Usage in MainWindow:
    from ui.pin_lock import PinLock
    self._pin_lock = PinLock(self, self.cfg, on_save_config=self.on_save_config)
    self._pin_lock.install()   # builds overlay + starts idle timer
    # add lock button:
    self._pin_lock.build_lock_button(parent_frame).pack(side="right")
"""
from __future__ import annotations

import hashlib
import time
import tkinter as tk
from tkinter import ttk, messagebox, simpledialog
from typing import Callable, Optional

from ui.styles import get_theme


_LOCKOUT_SEC = 30
_MAX_ATTEMPTS = 3


def _hash_pin(pin: str) -> str:
    return hashlib.sha256(pin.strip().encode()).hexdigest()


def _pin_valid(pin: str, stored_hash: str) -> bool:
    if not pin or not stored_hash:
        return False
    return _hash_pin(pin) == stored_hash


class PinLock:
    """
    Manages operator PIN locking for the main window.

    Call install() once after the window is built.
    Call lock() / unlock() programmatically or via the lock button.
    """

    def __init__(
        self,
        root: tk.Tk,
        cfg: dict,
        on_save_config: Optional[Callable] = None,
    ):
        self._root = root
        self._cfg = cfg
        self._on_save = on_save_config
        self._locked = False
        self._overlay: Optional[tk.Frame] = None
        self._attempts = 0
        self._lockout_until = 0.0
        self._idle_job: Optional[str] = None
        self._last_activity = time.monotonic()

        # Button StringVars updated on state change
        self._lock_btn_var = tk.StringVar(value="🔓 Lock")
        self._status_var = tk.StringVar(value="")

    # ── public API ────────────────────────────────────────────────────────────

    def install(self) -> None:
        """Bind idle tracker and start auto-lock polling."""
        self._root.bind_all("<Motion>",    self._on_activity, add=True)
        self._root.bind_all("<Key>",       self._on_activity, add=True)
        self._root.bind_all("<Button>",    self._on_activity, add=True)
        self._poll_idle()

    def build_lock_button(self, parent) -> ttk.Button:
        """Return a configured Lock/Unlock button for the top bar."""
        btn = ttk.Button(
            parent,
            textvariable=self._lock_btn_var,
            style="Ghost.TButton",
            command=self._on_lock_button,
            width=10,
        )
        return btn

    def build_setup_button(self, parent) -> ttk.Button:
        """Return a 'PIN Setup' button (for settings page)."""
        return ttk.Button(
            parent,
            text="PIN Setup…",
            style="Ghost.TButton",
            command=self.show_pin_setup,
            width=12,
        )

    @property
    def locked(self) -> bool:
        return self._locked

    def lock(self) -> None:
        if self._locked:
            return
        stored = self._ui_cfg().get("pin_hash", "")
        if not stored:
            messagebox.showinfo(
                "No PIN set",
                "Set a PIN first via PIN Setup before locking.\n"
                "Go to: Settings → PIN Setup…",
                parent=self._root,
            )
            return
        self._do_lock()

    def unlock(self, pin: str) -> bool:
        """Attempt unlock. Returns True on success."""
        stored = self._ui_cfg().get("pin_hash", "")
        if _pin_valid(pin, stored):
            self._attempts = 0
            self._lockout_until = 0.0
            self._do_unlock()
            return True
        self._attempts += 1
        if self._attempts >= _MAX_ATTEMPTS:
            self._lockout_until = time.monotonic() + _LOCKOUT_SEC
            self._attempts = 0
        return False

    def show_pin_setup(self) -> None:
        """Open PIN setup dialog. Requires existing PIN if one is set."""
        stored = self._ui_cfg().get("pin_hash", "")
        if stored:
            # Verify current PIN first
            cur = self._ask_pin("Enter current PIN to continue:")
            if cur is None:
                return
            if not _pin_valid(cur, stored):
                messagebox.showerror("Incorrect PIN", "Current PIN is wrong.", parent=self._root)
                return

        new_pin = self._ask_new_pin()
        if new_pin is None:
            return
        if new_pin == "":
            # Clear PIN
            self._ui_cfg().pop("pin_hash", None)
            self._save()
            messagebox.showinfo("PIN removed", "Operator PIN removed. Screen lock disabled.", parent=self._root)
            return
        self._ui_cfg()["pin_hash"] = _hash_pin(new_pin)
        auto_min = self._ui_cfg().get("auto_lock_min", 0)
        self._save()
        messagebox.showinfo(
            "PIN set",
            f"Operator PIN set.\n"
            f"Auto-lock: {'disabled' if not auto_min else f'{auto_min} min idle'}",
            parent=self._root,
        )

    # ── internal ─────────────────────────────────────────────────────────────

    def _ui_cfg(self) -> dict:
        return self._cfg.setdefault("ui", {})

    def _save(self) -> None:
        if callable(self._on_save):
            try:
                self._on_save()
            except Exception:
                pass

    def _on_lock_button(self) -> None:
        if self._locked:
            self._show_unlock_dialog()
        else:
            self.lock()

    def _do_lock(self) -> None:
        self._locked = True
        self._lock_btn_var.set("🔒 Locked")
        self._build_overlay()

    def _do_unlock(self) -> None:
        self._locked = False
        self._lock_btn_var.set("🔓 Lock")
        self._last_activity = time.monotonic()
        if self._overlay and self._overlay.winfo_exists():
            self._overlay.destroy()
            self._overlay = None

    def _build_overlay(self) -> None:
        """Full-screen lock overlay, drawn over all widgets."""
        if self._overlay and self._overlay.winfo_exists():
            self._overlay.lift()
            return
        t = get_theme()
        ov = tk.Frame(self._root, bg="#0a0a0f")
        ov.place(x=0, y=0, relwidth=1, relheight=1)
        ov.lift()
        self._overlay = ov

        # Centre content
        inner = tk.Frame(ov, bg="#0a0a0f")
        inner.place(relx=0.5, rely=0.45, anchor="center")

        tk.Label(
            inner, text="🔒", font=("Segoe UI", 48),
            bg="#0a0a0f", fg="#e5e7eb",
        ).pack(pady=(0, 8))
        tk.Label(
            inner, text="SCREEN LOCKED",
            font=("Segoe UI", 18, "bold"),
            bg="#0a0a0f", fg="#e5e7eb",
        ).pack()
        tk.Label(
            inner, text="Click anywhere or press Enter to unlock",
            font=("Segoe UI", 11),
            bg="#0a0a0f", fg="#6b7280",
        ).pack(pady=(6, 20))

        btn = tk.Button(
            inner,
            text="Enter PIN to Unlock",
            font=("Segoe UI", 11, "bold"),
            bg="#1d4ed8", fg="#ffffff",
            activebackground="#1e3a8a", activeforeground="#ffffff",
            relief="flat", padx=20, pady=8, cursor="hand2",
            command=self._show_unlock_dialog,
        )
        btn.pack()

        # Bind click/key anywhere on overlay to open dialog
        ov.bind("<Button-1>", lambda _e: self._show_unlock_dialog())
        ov.bind("<Return>",   lambda _e: self._show_unlock_dialog())

    def _show_unlock_dialog(self) -> None:
        if not self._locked:
            return
        # Lockout check
        remaining = self._lockout_until - time.monotonic()
        if remaining > 0:
            messagebox.showwarning(
                "Too many attempts",
                f"Too many failed attempts.\nTry again in {int(remaining)+1}s.",
                parent=self._root,
            )
            return

        pin = self._ask_pin("Enter PIN to unlock:")
        if pin is None:
            return  # cancelled
        if not self.unlock(pin):
            rem2 = self._lockout_until - time.monotonic()
            if rem2 > 0:
                messagebox.showerror(
                    "Locked out",
                    f"Too many incorrect attempts.\nLocked for {int(rem2)+1}s.",
                    parent=self._root,
                )
            else:
                left = _MAX_ATTEMPTS - self._attempts
                messagebox.showerror(
                    "Wrong PIN",
                    f"Incorrect PIN. {left} attempt(s) remaining.",
                    parent=self._root,
                )

    def _ask_pin(self, prompt: str) -> Optional[str]:
        """Show a masked PIN entry dialog. Returns the entered string or None if cancelled."""
        dlg = _PinDialog(self._root, title="PIN Required", prompt=prompt)
        self._root.wait_window(dlg)
        return dlg.result

    def _ask_new_pin(self) -> Optional[str]:
        """Ask for new PIN twice (confirmation). Returns PIN string, '' to clear, or None to cancel."""
        dlg = _NewPinDialog(self._root)
        self._root.wait_window(dlg)
        return dlg.result

    # ── idle auto-lock ────────────────────────────────────────────────────────

    def _on_activity(self, _event=None) -> None:
        self._last_activity = time.monotonic()

    def _poll_idle(self) -> None:
        try:
            auto_min = int(self._ui_cfg().get("auto_lock_min", 0) or 0)
            if auto_min > 0 and not self._locked:
                idle_sec = time.monotonic() - self._last_activity
                if idle_sec >= auto_min * 60:
                    stored = self._ui_cfg().get("pin_hash", "")
                    if stored:
                        self.lock()
        except Exception:
            pass
        self._idle_job = self._root.after(15_000, self._poll_idle)


class _PinDialog(tk.Toplevel):
    """Simple masked PIN entry dialog."""

    def __init__(self, parent, title: str, prompt: str):
        super().__init__(parent)
        self.result: Optional[str] = None
        self.title(title)
        self.resizable(False, False)
        self.grab_set()
        self.focus_set()
        self.transient(parent)

        t = get_theme()
        self.configure(bg=t.card)

        ttk.Label(self, text=prompt, style="FieldLabel.TLabel").pack(padx=20, pady=(16, 6))
        self._var = tk.StringVar()
        entry = ttk.Entry(self, textvariable=self._var, show="●", width=18, font=("Segoe UI", 13))
        entry.pack(padx=20, pady=(0, 12))
        entry.focus_set()
        entry.bind("<Return>", lambda _e: self._ok())

        btns = ttk.Frame(self)
        btns.pack(padx=20, pady=(0, 16))
        ttk.Button(btns, text="Unlock", command=self._ok, style="Accent.TButton").pack(side="left", padx=(0, 8))
        ttk.Button(btns, text="Cancel", command=self._cancel, style="Ghost.TButton").pack(side="left")

        self.protocol("WM_DELETE_WINDOW", self._cancel)
        self._center(parent)

    def _ok(self) -> None:
        self.result = self._var.get()
        self.destroy()

    def _cancel(self) -> None:
        self.result = None
        self.destroy()

    def _center(self, parent) -> None:
        try:
            self.update_idletasks()
            px = parent.winfo_rootx() + parent.winfo_width() // 2 - self.winfo_width() // 2
            py = parent.winfo_rooty() + parent.winfo_height() // 2 - self.winfo_height() // 2
            self.geometry(f"+{px}+{py}")
        except Exception:
            pass


class _NewPinDialog(tk.Toplevel):
    """Two-entry PIN setup dialog with confirmation."""

    def __init__(self, parent):
        super().__init__(parent)
        self.result: Optional[str] = None
        self.title("Set Operator PIN")
        self.resizable(False, False)
        self.grab_set()
        self.focus_set()
        self.transient(parent)

        t = get_theme()
        self.configure(bg=t.card)

        ttk.Label(self, text="New PIN (digits, leave blank to remove):", style="FieldLabel.TLabel").pack(padx=20, pady=(16, 4))
        self._v1 = tk.StringVar()
        e1 = ttk.Entry(self, textvariable=self._v1, show="●", width=18, font=("Segoe UI", 12))
        e1.pack(padx=20, pady=(0, 8))
        e1.focus_set()

        ttk.Label(self, text="Confirm PIN:", style="FieldLabel.TLabel").pack(padx=20, pady=(0, 4))
        self._v2 = tk.StringVar()
        e2 = ttk.Entry(self, textvariable=self._v2, show="●", width=18, font=("Segoe UI", 12))
        e2.pack(padx=20, pady=(0, 12))
        e2.bind("<Return>", lambda _e: self._ok())

        ttk.Label(
            self,
            text="Tip: use 4–8 digits. Leave blank to remove PIN.",
            style="Caption.TLabel",
        ).pack(padx=20, pady=(0, 8))

        btns = ttk.Frame(self)
        btns.pack(padx=20, pady=(0, 16))
        ttk.Button(btns, text="Save", command=self._ok, style="Accent.TButton").pack(side="left", padx=(0, 8))
        ttk.Button(btns, text="Cancel", command=self._cancel, style="Ghost.TButton").pack(side="left")

        self.protocol("WM_DELETE_WINDOW", self._cancel)
        self._center(parent)

    def _ok(self) -> None:
        p1 = self._v1.get().strip()
        p2 = self._v2.get().strip()
        if p1 == "" and p2 == "":
            self.result = ""  # remove PIN
            self.destroy()
            return
        if p1 != p2:
            messagebox.showerror("Mismatch", "PINs do not match.", parent=self)
            return
        if len(p1) < 4:
            messagebox.showerror("Too short", "PIN must be at least 4 characters.", parent=self)
            return
        self.result = p1
        self.destroy()

    def _cancel(self) -> None:
        self.result = None
        self.destroy()

    def _center(self, parent) -> None:
        try:
            self.update_idletasks()
            px = parent.winfo_rootx() + parent.winfo_width() // 2 - self.winfo_width() // 2
            py = parent.winfo_rooty() + parent.winfo_height() // 2 - self.winfo_height() // 2
            self.geometry(f"+{px}+{py}")
        except Exception:
            pass
