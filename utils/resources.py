from __future__ import annotations
import os, sys
from pathlib import Path

def app_root() -> Path:
    # When frozen by PyInstaller, files are unpacked under sys._MEIPASS
    if getattr(sys, "frozen", False) and hasattr(sys, "_MEIPASS"):
        return Path(getattr(sys, "_MEIPASS"))
    # source run: project root = .../Power_Monitor_v1_SCADA_CORE
    return Path(__file__).resolve().parents[1]

def resource_path(*parts: str) -> str:
    return str(app_root().joinpath(*parts))
