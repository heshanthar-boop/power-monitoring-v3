# utils/monitors.py
"""
Monitor enumeration + window placement helpers.

Design:
- Windows-first (control-room PCs). Safe fallback elsewhere.
- Never crash UI if enumeration fails.
"""
from __future__ import annotations

import sys
from utils.logger import setup_logger

log = setup_logger("monitors")


def list_monitors(root_tk=None):
    """
    Return a list of monitor dicts:
    {index, left, top, right, bottom, width, height, primary}
    """
    mons = []
    if sys.platform == "win32":
        try:
            import ctypes
            from ctypes import wintypes

            user32 = ctypes.windll.user32
            MONITORINFOF_PRIMARY = 0x00000001

            class RECT(ctypes.Structure):
                _fields_ = [("left", wintypes.LONG),
                            ("top", wintypes.LONG),
                            ("right", wintypes.LONG),
                            ("bottom", wintypes.LONG)]

            class MONITORINFO(ctypes.Structure):
                _fields_ = [("cbSize", wintypes.DWORD),
                            ("rcMonitor", RECT),
                            ("rcWork", RECT),
                            ("dwFlags", wintypes.DWORD)]

            MonitorEnumProc = ctypes.WINFUNCTYPE(
                wintypes.BOOL,
                wintypes.HMONITOR, wintypes.HDC,
                ctypes.POINTER(RECT), wintypes.LPARAM
            )

            def _callback(hmon, hdc, lprect, lparam):
                info = MONITORINFO()
                info.cbSize = ctypes.sizeof(MONITORINFO)
                if user32.GetMonitorInfoW(hmon, ctypes.byref(info)):
                    r = info.rcMonitor
                    mons.append({
                        "left": int(r.left),
                        "top": int(r.top),
                        "right": int(r.right),
                        "bottom": int(r.bottom),
                        "width": int(r.right - r.left),
                        "height": int(r.bottom - r.top),
                        "primary": bool(info.dwFlags & MONITORINFOF_PRIMARY),
                    })
                return True

            user32.EnumDisplayMonitors(0, 0, MonitorEnumProc(_callback), 0)

        except Exception as e:
            log.warning("EnumDisplayMonitors failed: %s", e)

    if not mons:
        # Fallback single monitor via tkinter if possible
        try:
            if root_tk is not None:
                w = int(root_tk.winfo_screenwidth())
                h = int(root_tk.winfo_screenheight())
            else:
                w, h = 1024, 768
            mons = [{
                "left": 0, "top": 0, "right": w, "bottom": h,
                "width": w, "height": h, "primary": True
            }]
        except Exception:
            mons = [{
                "left": 0, "top": 0, "right": 1024, "bottom": 768,
                "width": 1024, "height": 768, "primary": True
            }]

    # assign index
    out = []
    for i, m in enumerate(mons):
        m2 = dict(m)
        m2["index"] = i + 1
        out.append(m2)
    # sort: primary first then by index
    out.sort(key=lambda x: (not x.get("primary", False), x["index"]))
    return out


def geometry_for_monitor(m, width=None, height=None, margin=20):
    """
    Compute window geometry string for a monitor: "WxH+X+Y"
    Default: 90% of monitor size, centered-ish with margin.
    """
    mw = int(m["width"])
    mh = int(m["height"])
    w = int(width or (mw * 0.92))
    h = int(height or (mh * 0.92))
    w = max(600, min(w, mw))
    h = max(400, min(h, mh))
    x = int(m["left"] + (mw - w) / 2)
    y = int(m["top"] + (mh - h) / 2)
    # margin clamp
    x = max(int(m["left"] + margin), min(x, int(m["right"] - w - margin)))
    y = max(int(m["top"] + margin), min(y, int(m["bottom"] - h - margin)))
    return f"{w}x{h}+{x}+{y}"
