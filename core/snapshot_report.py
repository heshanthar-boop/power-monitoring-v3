"""
Live Snapshot Report
====================
Generates a single-page HTML snapshot of the current meter readings, active
alarms, and system status — then opens it in the default browser so the
operator can use File → Print or Ctrl+P to produce a hard copy or PDF.

Why HTML not PDF:
  • Zero dependencies — works on any Windows PC without wkhtmltopdf, reportlab,
    weasyprint, or any other library.
  • Chrome/Edge print-to-PDF produces a clean A4 document with one click.
  • The HTML file is self-contained (inline CSS, no external assets).

Usage:
    from core.snapshot_report import SnapshotReporter
    reporter = SnapshotReporter(cfg)

    # From the UI tick or on-demand:
    path = reporter.generate(
        values_by_source={"TOTAL": {...}, "M1": {...}},
        quality_map={"M1": "GOOD"},
        active_alarms=alarm_engine.active,   # dict of (mid,code)->AlarmEvent
        meters=meters,                        # list of MeterState
    )
    reporter.open_in_browser(path)

    # Combined one-call helper (generate + open):
    reporter.print_snapshot(values_by_source, quality_map, active_alarms, meters)
"""
from __future__ import annotations

import html
import os
import math
import tempfile
import time
import webbrowser
from datetime import datetime
from typing import Any, Dict, List, Optional

from utils.logger import setup_logger

logger = setup_logger("snapshot_report")

# ── CSS ─────────────────────────────────────────────────────────────────────
_CSS = """
*, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
@page { size: A4; margin: 12mm 14mm; }
body { font-family: 'Segoe UI', Arial, sans-serif; font-size: 10pt;
       color: #1a1a1a; background: #fff; }
@media print { body { font-size: 9pt; } }

/* ── Header ── */
.report-header { display: flex; align-items: center; justify-content: space-between;
                 border-bottom: 2px solid #1a3a6b; padding-bottom: 8px; margin-bottom: 14px; }
.report-header .site { font-size: 13pt; font-weight: 700; color: #1a3a6b; }
.report-header .meta { text-align: right; font-size: 8pt; color: #555; line-height: 1.6; }

/* ── Section heading ── */
h2 { font-size: 10pt; font-weight: 700; text-transform: uppercase;
     letter-spacing: .5px; color: #1a3a6b;
     border-bottom: 1px solid #d0d8e8; padding-bottom: 3px; margin: 12px 0 6px; }

/* ── Summary tiles ── */
.tiles { display: flex; flex-wrap: wrap; gap: 8px; margin-bottom: 12px; }
.tile  { border: 1px solid #d0d8e8; border-radius: 4px; padding: 7px 12px;
         min-width: 110px; flex: 1; }
.tile .lbl { font-size: 7.5pt; color: #666; text-transform: uppercase; letter-spacing: .4px; }
.tile .val { font-size: 16pt; font-weight: 700; color: #1a3a6b; margin-top: 2px; }
.tile .unit { font-size: 8pt; color: #888; margin-left: 2px; }
.tile.alarm .val { color: #c0392b; }
.tile.warn  .val { color: #d07000; }
.tile.good  .val { color: #1a7a3a; }

/* ── Meter tables ── */
.meter-section { page-break-inside: avoid; margin-bottom: 16px; }
.meter-title { font-size: 10pt; font-weight: 700; color: #1a3a6b;
               background: #eef2f8; padding: 4px 8px; border-radius: 3px;
               margin-bottom: 6px; }
.quality-badge { display: inline-block; font-size: 7.5pt; font-weight: 700;
                 padding: 1px 7px; border-radius: 10px; margin-left: 8px; vertical-align: middle; }
.q-GOOD  { background: #d4edda; color: #155724; }
.q-STALE { background: #fff3cd; color: #856404; }
.q-BAD   { background: #f8d7da; color: #721c24; }

table  { width: 100%; border-collapse: collapse; font-size: 9pt; }
th     { background: #eef2f8; color: #333; font-weight: 600; padding: 4px 8px;
         text-align: left; border: 1px solid #ccd4e0; }
td     { padding: 3px 8px; border: 1px solid #dde4f0; vertical-align: middle; }
tr:nth-child(even) td { background: #f8faff; }
.num   { text-align: right; font-variant-numeric: tabular-nums; }
.ok    { color: #1a7a3a; font-weight: 600; }
.warn  { color: #d07000; font-weight: 600; }
.alarm { color: #c0392b; font-weight: 600; }

/* ── Alarm table ── */
.alarm-row td { background: #fff0f0; }
.warn-row  td { background: #fffbe6; }
.acked td    { color: #888; font-style: italic; }

/* ── Footer ── */
.report-footer { margin-top: 16px; padding-top: 6px;
                 border-top: 1px solid #d0d8e8;
                 font-size: 7.5pt; color: #888; text-align: center; }
"""

# ── Helpers ─────────────────────────────────────────────────────────────────

def _h(x: Any) -> str:
    return html.escape(str(x) if x is not None else "")


def _fmt(v: Any, ndp: int = 2, unit: str = "") -> str:
    """Format a numeric value. Returns '—' for None/nan/non-numeric."""
    try:
        if v is None:
            return "—"
        f = float(v)
        if not math.isfinite(f):
            return "—"
        s = f"{f:.{ndp}f}"
        return f"{s} {unit}".strip() if unit else s
    except (TypeError, ValueError):
        return "—"


def _quality_badge(quality: str) -> str:
    q = str(quality or "").upper()
    cls = "q-GOOD" if q == "GOOD" else ("q-STALE" if q in ("STALE", "COMM_LOST") else "q-BAD")
    label = {"GOOD": "GOOD", "STALE": "STALE", "COMM_LOST": "OFFLINE",
             "DISABLED": "OFF"}.get(q, q or "UNKNOWN")
    return f'<span class="quality-badge {_h(cls)}">{_h(label)}</span>'


def _val_class(v: Any, nominal: float, tol_warn: float = 0.10, tol_alarm: float = 0.15) -> str:
    try:
        f = float(v)
        if not math.isfinite(f) or nominal <= 0:
            return ""
        dev = abs(f - nominal) / nominal
        if dev <= tol_warn:
            return ' class="ok"'
        if dev <= tol_alarm:
            return ' class="warn"'
        return ' class="alarm"'
    except Exception:
        return ""


def _pf_class(pf: Any) -> str:
    try:
        f = abs(float(pf))
        if f >= 0.95:
            return ' class="ok"'
        if f >= 0.85:
            return ' class="warn"'
        return ' class="alarm"'
    except Exception:
        return ""


# ── Main builder ─────────────────────────────────────────────────────────────

def build_snapshot_html(
    cfg: dict,
    values_by_source: Dict[str, Dict[str, Any]],
    quality_map: Dict[str, str],
    active_alarms: dict,          # (meter_id, code) → AlarmEvent
    meters: list,                 # list of MeterState objects
) -> str:
    """
    Build a self-contained HTML snapshot report string.

    Parameters
    ----------
    cfg              : application config dict
    values_by_source : {"TOTAL": {...}, "M1": {...}, ...}
    quality_map      : {"M1": "GOOD", ...}
    active_alarms    : alarm_engine.active dict
    meters           : list of MeterState (for name lookup)

    Returns
    -------
    str — complete HTML document
    """
    now_dt   = datetime.now()
    now_str  = now_dt.strftime("%Y-%m-%d %H:%M:%S")
    now_ts   = now_dt.strftime("%Y%m%d_%H%M%S")

    site_cfg  = cfg.get("site") or {}
    rep_cfg   = cfg.get("reports") or {}
    plant     = str(site_cfg.get("plant_name", "") or rep_cfg.get("facility_name", "") or "Power Monitor")
    location  = str(site_cfg.get("location", "") or "")
    nom_vln   = float(site_cfg.get("nominal_vln") or rep_cfg.get("nominal_v_ln") or 230.0)
    nom_vll   = nom_vln * math.sqrt(3)
    nom_freq  = 50.0

    # Meter name map
    meter_names: Dict[str, str] = {}
    for m in (meters or []):
        mid = str(getattr(m, "meter_id", "") or "")
        name = str(getattr(m, "name", "") or mid)
        meter_names[mid] = name

    def _meter_name(source: str) -> str:
        mid = str(source).lstrip("M")
        return meter_names.get(mid, meter_names.get(source, source))

    # ── TOTAL summary tiles ───────────────────────────────────────────────
    total = values_by_source.get("TOTAL") or values_by_source.get("total") or {}
    kw   = total.get("kW")
    kva  = total.get("kVA")
    pf   = total.get("PFavg") or total.get("PF")
    vavg = total.get("Vavg")
    iavg = total.get("Iavg")
    freq = total.get("Frequency")
    kwh  = total.get("Today_kWh") or total.get("Import_kWh")

    def _tile(label: str, value: Any, unit: str, cls: str = "") -> str:
        v_str = _fmt(value, 1 if unit in ("V", "A", "Hz") else (3 if unit == "" else 2), "")
        return (
            f'<div class="tile {_h(cls)}">'
            f'<div class="lbl">{_h(label)}</div>'
            f'<div class="val">{_h(v_str)}<span class="unit">{_h(unit)}</span></div>'
            f'</div>'
        )

    pf_tile_cls = ""
    try:
        pf_f = abs(float(pf))
        pf_tile_cls = "good" if pf_f >= 0.95 else ("warn" if pf_f >= 0.85 else "alarm")
    except Exception:
        pass

    tiles_html = (
        '<div class="tiles">'
        + _tile("Active Power", kw, "kW")
        + _tile("Apparent Power", kva, "kVA")
        + _tile("Avg Voltage L-N", vavg, "V")
        + _tile("Avg Current", iavg, "A")
        + _tile("Power Factor", _fmt(pf, 3), "", pf_tile_cls)
        + _tile("Frequency", freq, "Hz")
        + _tile("Today kWh", kwh, "kWh")
        + '</div>'
    )

    # ── Alarm summary ─────────────────────────────────────────────────────
    alarm_rows = []
    alarms_list = list((active_alarms or {}).values())
    # Sort: ALARM severity first, then by timestamp descending
    alarms_list.sort(key=lambda e: (0 if str(getattr(e, "severity", "")).upper() == "ALARM" else 1,
                                    -getattr(e, "ts", 0)))
    for ev in alarms_list:
        sev  = str(getattr(ev, "severity", "WARN")).upper()
        mid  = str(getattr(ev, "meter_id", ""))
        code = str(getattr(ev, "code", ""))
        msg  = str(getattr(ev, "message", ""))
        ts   = getattr(ev, "ts", 0)
        acked = bool(getattr(ev, "acknowledged", False))
        ts_str = datetime.fromtimestamp(float(ts)).strftime("%H:%M:%S") if ts else "—"
        row_cls = ("alarm-row" if sev == "ALARM" else "warn-row") + (" acked" if acked else "")
        ack_str = "✔ ACK" if acked else "Active"
        alarm_rows.append(
            f'<tr class="{_h(row_cls)}">'
            f'<td>{_h(ts_str)}</td>'
            f'<td class="{("alarm" if sev == "ALARM" else "warn")}">{_h(sev)}</td>'
            f'<td>{_h(mid)}</td>'
            f'<td>{_h(code)}</td>'
            f'<td>{_h(msg)}</td>'
            f'<td>{_h(ack_str)}</td>'
            f'</tr>'
        )

    if alarm_rows:
        alarm_section = (
            '<h2>Active Alarms</h2>'
            '<table><thead><tr>'
            '<th>Time</th><th>Severity</th><th>Meter</th><th>Code</th><th>Message</th><th>State</th>'
            '</tr></thead><tbody>'
            + "".join(alarm_rows)
            + '</tbody></table>'
        )
    else:
        alarm_section = '<h2>Active Alarms</h2><p style="color:#1a7a3a;font-size:9pt;margin:4px 0">✔ No active alarms</p>'

    # ── Per-meter tables ──────────────────────────────────────────────────
    meter_sections = []
    for source, vals in sorted(values_by_source.items()):
        if str(source).upper() in ("TOTAL", "total"):
            continue
        quality = quality_map.get(str(source), "")
        name    = _meter_name(str(source))
        v1n  = vals.get("V1N")
        v2n  = vals.get("V2N")
        v3n  = vals.get("V3N")
        v12  = vals.get("V12")
        v23  = vals.get("V23")
        v31  = vals.get("V31")
        i1   = vals.get("I1")
        i2   = vals.get("I2")
        i3   = vals.get("I3")
        kw_m = vals.get("kW")
        kva_m= vals.get("kVA")
        kvar = vals.get("kVAr")
        pf_m = vals.get("PFavg") or vals.get("PF")
        freq_m = vals.get("Frequency")
        kwh_m= vals.get("Today_kWh") or vals.get("Import_kWh")
        vavg_m = vals.get("Vavg")
        iavg_m = vals.get("Iavg")

        sec = (
            f'<div class="meter-section">'
            f'<div class="meter-title">'
            f'{_h(source)} — {_h(name)}'
            f'{_quality_badge(quality)}'
            f'</div>'
            f'<table><thead><tr>'
            f'<th>Parameter</th><th>R (L1)</th><th>Y (L2)</th><th>B (L3)</th><th>Avg / Total</th>'
            f'</tr></thead><tbody>'

            f'<tr><td>Voltage L-N (V)</td>'
            f'<td class="num"{_val_class(v1n, nom_vln)}>{_fmt(v1n,1)}</td>'
            f'<td class="num"{_val_class(v2n, nom_vln)}>{_fmt(v2n,1)}</td>'
            f'<td class="num"{_val_class(v3n, nom_vln)}>{_fmt(v3n,1)}</td>'
            f'<td class="num"{_val_class(vavg_m, nom_vln)}>{_fmt(vavg_m,1)}</td></tr>'

            f'<tr><td>Voltage L-L (V)</td>'
            f'<td class="num"{_val_class(v12, nom_vll)}>{_fmt(v12,1)}</td>'
            f'<td class="num"{_val_class(v23, nom_vll)}>{_fmt(v23,1)}</td>'
            f'<td class="num"{_val_class(v31, nom_vll)}>{_fmt(v31,1)}</td>'
            f'<td class="num">—</td></tr>'

            f'<tr><td>Current (A)</td>'
            f'<td class="num">{_fmt(i1,2)}</td>'
            f'<td class="num">{_fmt(i2,2)}</td>'
            f'<td class="num">{_fmt(i3,2)}</td>'
            f'<td class="num">{_fmt(iavg_m,2)}</td></tr>'

            f'<tr><td>Active Power (kW)</td>'
            f'<td class="num" colspan="3">—</td>'
            f'<td class="num">{_fmt(kw_m,2)}</td></tr>'

            f'<tr><td>Reactive Power (kVAr)</td>'
            f'<td class="num" colspan="3">—</td>'
            f'<td class="num">{_fmt(kvar,2)}</td></tr>'

            f'<tr><td>Apparent Power (kVA)</td>'
            f'<td class="num" colspan="3">—</td>'
            f'<td class="num">{_fmt(kva_m,2)}</td></tr>'

            f'<tr><td>Power Factor</td>'
            f'<td class="num" colspan="3">—</td>'
            f'<td class="num"{_pf_class(pf_m)}>{_fmt(pf_m,3)}</td></tr>'

            f'<tr><td>Frequency (Hz)</td>'
            f'<td class="num" colspan="3">—</td>'
            f'<td class="num"{_val_class(freq_m, nom_freq, 0.01, 0.02)}>{_fmt(freq_m,3)}</td></tr>'

            f'<tr><td>Today kWh</td>'
            f'<td class="num" colspan="3">—</td>'
            f'<td class="num">{_fmt(kwh_m,2)}</td></tr>'

            f'</tbody></table></div>'
        )
        meter_sections.append(sec)

    # ── Assemble ─────────────────────────────────────────────────────────
    n_active  = sum(1 for ev in (active_alarms or {}).values()
                    if not getattr(ev, "acknowledged", False))
    n_meters  = len([s for s in values_by_source if str(s).upper() != "TOTAL"])
    loc_html  = f' — {_h(location)}' if location else ''

    doc = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Snapshot Report — {_h(plant)} — {_h(now_str)}</title>
<style>
{_CSS}
</style>
</head>
<body>

<div class="report-header">
  <div>
    <div class="site">{_h(plant)}{loc_html}</div>
    <div style="font-size:8pt;color:#555;margin-top:2px">Live Snapshot Report</div>
  </div>
  <div class="meta">
    Generated: {_h(now_str)}<br>
    Meters: {_h(n_meters)}  &nbsp;|&nbsp;
    Active alarms: {'<span style="color:#c0392b;font-weight:700">' + str(n_active) + '</span>' if n_active else '<span style="color:#1a7a3a">None</span>'}
  </div>
</div>

<h2>System Summary</h2>
{tiles_html}

{alarm_section}

<h2>Meter Readings</h2>
{''.join(meter_sections) if meter_sections else '<p style="color:#888;font-size:9pt">No meter data available.</p>'}

<div class="report-footer">
  MFM384 SCADA &nbsp;|&nbsp; Snapshot generated {_h(now_str)} &nbsp;|&nbsp;
  Use File → Print (Ctrl+P) in your browser to save as PDF
</div>

</body>
</html>"""
    return doc


# ── Public interface ─────────────────────────────────────────────────────────

class SnapshotReporter:
    """
    Generates and opens live snapshot reports.

    Keeps a reference to the last generated file so it can be re-opened
    without regenerating (useful if operator wants another print copy).
    """

    def __init__(self, cfg: dict) -> None:
        self._cfg = cfg
        self._last_path: Optional[str] = None

    def reconfigure(self, cfg: dict) -> None:
        self._cfg = cfg

    def generate(
        self,
        values_by_source: Dict[str, Dict[str, Any]],
        quality_map: Dict[str, str],
        active_alarms: dict,
        meters: list,
        out_dir: Optional[str] = None,
    ) -> str:
        """
        Write the HTML snapshot to a temp file and return the path.

        Parameters
        ----------
        out_dir : optional directory for the HTML file.
                  Defaults to system temp dir.  Reports dir preferred if available.
        """
        html_content = build_snapshot_html(
            cfg=self._cfg,
            values_by_source=values_by_source,
            quality_map=quality_map,
            active_alarms=active_alarms,
            meters=meters,
        )

        # Choose output directory — prefer reports dir if it exists
        if out_dir is None:
            try:
                from utils.paths import reports_dir
                rd = reports_dir()
                if os.path.isdir(rd):
                    out_dir = rd
            except Exception:
                pass

        ts_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"snapshot_{ts_str}.html"

        if out_dir and os.path.isdir(out_dir):
            path = os.path.join(out_dir, filename)
        else:
            # Fallback: temp dir — always writable
            path = os.path.join(tempfile.gettempdir(), filename)

        try:
            with open(path, "w", encoding="utf-8") as fh:
                fh.write(html_content)
            self._last_path = path
            logger.info("[SNAPSHOT] report written: %s", path)
        except Exception as exc:
            logger.error("[SNAPSHOT] write failed: %s", exc)
            # Try temp dir as absolute fallback
            path = os.path.join(tempfile.gettempdir(), filename)
            with open(path, "w", encoding="utf-8") as fh:
                fh.write(html_content)
            self._last_path = path

        return path

    def open_in_browser(self, path: str) -> None:
        """Open the HTML file in the system default browser."""
        try:
            # file:/// URI — works on Windows/Linux/macOS
            uri = "file:///" + path.replace("\\", "/")
            webbrowser.open(uri, new=2)
            logger.info("[SNAPSHOT] opened in browser: %s", uri)
        except Exception as exc:
            logger.error("[SNAPSHOT] browser open failed: %s", exc)

    def print_snapshot(
        self,
        values_by_source: Dict[str, Dict[str, Any]],
        quality_map: Dict[str, str],
        active_alarms: dict,
        meters: list,
        out_dir: Optional[str] = None,
    ) -> str:
        """Generate + open in one call. Returns the output file path."""
        path = self.generate(values_by_source, quality_map, active_alarms, meters, out_dir)
        self.open_in_browser(path)
        return path

    @property
    def last_path(self) -> Optional[str]:
        return self._last_path
