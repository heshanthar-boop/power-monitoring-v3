import logging
import sys
from logging.handlers import RotatingFileHandler
from utils.paths import logs_dir


class SafeRotatingFileHandler(RotatingFileHandler):
    """RotatingFileHandler that survives rename failures (locked log files)."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._rotation_failed = False

    def rotate(self, source, dest):
        if self._rotation_failed:
            return
        try:
            super().rotate(source, dest)
        except OSError:
            # Log file locked (WinError 32). Disable rotation silently.
            self._rotation_failed = True

    def shouldRollover(self, record):
        if self._rotation_failed:
            return False
        return super().shouldRollover(record)

def setup_logger(name="mfm384", level=logging.INFO) -> logging.Logger:
    lg = logging.getLogger(name)
    if lg.handlers:
        return lg
    lg.setLevel(level)

    fmt = logging.Formatter("%(asctime)s | %(levelname)-7s | %(name)s | %(message)s")

    ch = logging.StreamHandler()
    ch.setLevel(level)
    ch.setFormatter(fmt)
    lg.addHandler(ch)

    fh = SafeRotatingFileHandler(
        f"{logs_dir()}/app.log",
        maxBytes=5 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8"
    )
    fh.setLevel(logging.INFO)  # IMPORTANT: no DEBUG spam to disk
    fh.setFormatter(fmt)
    lg.addHandler(fh)

    return lg