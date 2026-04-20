import json
import os
import shutil
from datetime import datetime

from config.schema import default_config
from utils.paths import config_path
from utils.atomic import atomic_write_json


def _backup_config_file(path: str, suffix: str) -> str:
    """Best-effort config backup before replacing or recovering a config file."""
    if not os.path.exists(path):
        return ""
    base, ext = os.path.splitext(path)
    if suffix == "prev":
        dest = f"{base}.prev{ext or '.json'}"
    else:
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        dest = f"{base}.{suffix}.{stamp}{ext or '.json'}"
    try:
        shutil.copy2(path, dest)
        return dest
    except Exception:
        return ""


def load_config() -> dict:
    p = config_path()
    try:
        with open(p, "r", encoding="utf-8") as f:
            data = json.load(f)
        return merge_defaults(default_config(), data)
    except Exception:
        # Do not silently destroy a corrupted/invalid field config. Preserve it
        # first, then create a clean default so the app can still start.
        if os.path.exists(p):
            _backup_config_file(p, "invalid")
        cfg = default_config()
        save_config(cfg)
        return cfg


def save_config(cfg: dict) -> None:
    p = config_path()
    _backup_config_file(p, "prev")
    atomic_write_json(p, cfg)


def merge_defaults(dflt: dict, got: dict) -> dict:
    # Deep merge defaults with got, never crash on missing keys.
    if not isinstance(got, dict):
        return dflt
    out = dict(dflt)
    for k, v in got.items():
        if k in out and isinstance(out[k], dict) and isinstance(v, dict):
            out[k] = merge_defaults(out[k], v)
        else:
            out[k] = v
    return out
