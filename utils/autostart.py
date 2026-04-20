"""
Windows Registry autostart helper.

Adds/removes the app from:
  HKEY_CURRENT_USER\Software\Microsoft\Windows\CurrentVersion\Run

Uses HKCU (no admin rights required).
Safe on non-Windows (all functions return False / no-op).
"""
from __future__ import annotations

import os
import sys

_APP_KEY = "MFM384Monitoring"


def _reg_run_key():
    """Return opened HKCU\\...\\Run key for read/write, or None."""
    try:
        import winreg
        return winreg.OpenKey(
            winreg.HKEY_CURRENT_USER,
            r"Software\Microsoft\Windows\CurrentVersion\Run",
            0,
            winreg.KEY_ALL_ACCESS,
        )
    except Exception:
        return None


def get_autostart_enabled() -> bool:
    """Return True if the app is registered to start with Windows."""
    if sys.platform != "win32":
        return False
    try:
        import winreg
        key = _reg_run_key()
        if key is None:
            return False
        try:
            val, _ = winreg.QueryValueEx(key, _APP_KEY)
            return bool(val)
        except FileNotFoundError:
            return False
        finally:
            winreg.CloseKey(key)
    except Exception:
        return False


def set_autostart_enabled(enabled: bool) -> bool:
    """
    Enable or disable Windows autostart.
    Returns True on success, False on failure.
    """
    if sys.platform != "win32":
        return False
    try:
        import winreg
        key = _reg_run_key()
        if key is None:
            return False
        try:
            if enabled:
                # Use the frozen executable path if packaged, else python + script
                if getattr(sys, "frozen", False):
                    exe = sys.executable
                else:
                    exe = f'"{sys.executable}" "{os.path.abspath(sys.argv[0])}"'
                winreg.SetValueEx(key, _APP_KEY, 0, winreg.REG_SZ, exe)
            else:
                try:
                    winreg.DeleteValue(key, _APP_KEY)
                except FileNotFoundError:
                    pass
            return True
        finally:
            winreg.CloseKey(key)
    except Exception:
        return False
