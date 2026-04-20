"""HTML report generator (A4 print-friendly).

Why HTML:
- Zero external dependencies (works on any Windows machine)
- Operator can open in browser and Print -> Save as PDF

This module is deliberately conservative: it never assumes a specific browser,
and it renders cleanly in Chrome/Edge print-to-PDF.
"""

from __future__ import annotations

import html
import os
from datetime import datetime
from typing import Any, Dict, List, Optional


def _fmt_num(v: Any, ndp: int) -> str:
    try:
        if v is None:
            return "N/A"
        if isinstance(v, str):
            s = v.strip()
            if not s or s in ("—", "-", "N/A"):
                return "N/A"
            v = float(s)
        return f"{float(v):.{ndp}f}"
    except Exception:
        return "N/A"


def _get_scope_values(snapshot: Dict[str, Any], scope: str) -> Dict[str, Any]:
    scope = (scope or "TOTAL").upper()
    if scope == "TOTAL":
        return ((snapshot.get("total") or {}).get("values") or {})
    if scope.startswith("M") and scope[1:].isdigit():
        mid = int(scope[1:])
        return (((snapshot.get("meters") or {}).get(mid) or {}).get("values") or {})
    return ((snapshot.get("total") or {}).get("values") or {})


def _value_class(v: Optional[float], nominal: Optional[float] = None) -> str:
    """Return css class based on deviation from nominal.

    Rules:
    - If nominal is unknown -> neutral
    - green: within +/-10%
    - yellow: 10-15%
    - red: >15%
    """
    try:
        if v is None or nominal in (None, 0):
            return "v-neutral"
        dev = abs((float(v) - float(nominal)) / float(nominal)) * 100.0
        if dev <= 10.0:
            return "v-ok"
        if dev <= 15.0:
            return "v-warn"
        return "v-alarm"
    except Exception:
        return "v-neutral"




def _get_src_values(snapshot: Dict[str, Any], source: Any) -> Dict[str, Any]:
    if not snapshot:
        return {}
    if source == 'TOTAL' or source == 'total':
        return (snapshot.get('total') or {}).get('values') or {}
    mid = None
    if isinstance(source, int):
        mid = source
    else:
        s = str(source).strip()
        if s.startswith('M') and s[1:].isdigit():
            mid = int(s[1:])
        elif s.isdigit():
            mid = int(s)
    if mid is None:
        return {}
    return ((snapshot.get('meters') or {}).get(mid) or {}).get('values') or {}


def _pick_thd(src: Dict[str, Any]):
    def sf(x):
        try:
            if x is None: return None
            s = str(x).strip()
            if not s or s in ('—','-','N/A'): return None
            return float(s)
        except Exception:
            return None
    thdv = sf(src.get('THD_V')) or sf(src.get('THD_V_worst'))
    thdi = sf(src.get('THD_I')) or sf(src.get('THD_I_worst'))
    if thdv is None:
        vals = [sf(v) for k,v in (src or {}).items() if str(k).startswith('THD Voltage')]
        vals = [v for v in vals if v is not None]
        if vals: thdv = max(vals)
    if thdi is None:
        vals = [sf(v) for k,v in (src or {}).items() if str(k).startswith('THD Current')]
        vals = [v for v in vals if v is not None]
        if vals: thdi = max(vals)
    return thdv, thdi

def build_report_html(cfg: Dict[str, Any], req: Any, meter_ids: List[int], data: Dict[str, Any], out_path: str) -> None:
    rcfg = cfg.get("reports", {}) or {}
    facility_code = str(rcfg.get("facility_code", "PLANT01") or "PLANT01")
    company_name = str(rcfg.get("company_name", "") or "")
    company_contact = str(rcfg.get("company_contact", "") or "")
    facility_name = str((cfg.get("comm", {}) or {}).get("plant", "") or rcfg.get("facility_name", "") or "")
    facility_loc = str((cfg.get("comm", {}) or {}).get("location", "") or rcfg.get("facility_location", "") or "")
    watermark = str(rcfg.get("watermark", "") or "")
    logo_path = str(rcfg.get("logo_path", "") or "")

    snap = data.get("snapshot") or {}
    events = data.get("events") or []
    stats = data.get("stats") or {}

    # Nominals (configurable; safe defaults for Sri Lanka LV)
    nominal_v_ln = float((rcfg.get("nominal_v_ln") or 230.0))
    nominal_v_ll = float((rcfg.get("nominal_v_ll") or 400.0))
    nominal_f = float((rcfg.get("nominal_f") or 50.0))

    start_s = datetime.fromtimestamp(req.start_ts).strftime("%Y-%m-%d %H:%M:%S")
    end_s = datetime.fromtimestamp(req.end_ts).strftime("%Y-%m-%d %H:%M:%S")
    gen_s = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rtype = str(req.report_type).upper()

    # Executive summary (best-effort)
    total_vals = _get_scope_values(snap, "TOTAL")
    total_kwh = total_vals.get("kWh") or total_vals.get("Energy_kWh") or total_vals.get("E_kWh")
    total_pf = total_vals.get("PFavg") or total_vals.get("PF")
    total_kw = total_vals.get("kW") or total_vals.get("P_kW")

    # System status from events (simple: if any Active -> Alarm/Warning)
    active = 0
    for e in events:
        st = str(e.get("state") or e.get("State") or "").upper()
        if st in ("ALARM", "WARNING"):
            active += 1
    system_status = "NORMAL" if active == 0 else ("WARNING" if active < 3 else "ALARM")

    # Logo embedding via file:// path (browser will load locally). If missing, show placeholder.
    logo_html = ""
    if logo_path and os.path.exists(logo_path):
        logo_uri = "file:///" + logo_path.replace("\\", "/")
        logo_html = f'<img class="logo" src="{html.escape(logo_uri)}" alt="Logo" />'
    else:
        logo_html = '<div class="logo-ph">LOGO</div>'

    def h(x: Any) -> str:
        return html.escape(str(x))

    def meter_block(mid: int) -> str:
        vals = _get_scope_values(snap, f"M{mid}")
        # Best-effort keys
        vavg = vals.get("Vavg")
        freq = vals.get("Frequency")
        iavg = vals.get("Iavg")
        kw = vals.get("kW")
        kva = vals.get("kVA")
        kvar = vals.get("kVAr")
        pf = vals.get("PF") or vals.get("PFavg")
        thdv = vals.get("THD_V")
        thdi = vals.get("THD_I")

        # If phase values exist, use them; else repeat avg.
        def p3(key_r: str, key_y: str, key_b: str, fallback: Any) -> tuple:
            return (
                vals.get(key_r, fallback),
                vals.get(key_y, fallback),
                vals.get(key_b, fallback),
            )

        v1n, v2n, v3n = p3("V1N", "V2N", "V3N", vavg)
        v12, v23, v31 = p3("V12", "V23", "V31", None)
        i1, i2, i3 = p3("I1", "I2", "I3", iavg)
        in_ = vals.get("IN") or vals.get("I_N")

        # Color classes
        vavg_cls = _value_class(_to_float(vavg), nominal_v_ln)
        freq_cls = _value_class(_to_float(freq), nominal_f)

        return f"""
        <div class='meter'>
          <div class='meter-h'>Meter M{mid} <span class='muted'>| Selec MFM384</span></div>

          <div class='grid2'>
            <div class='card'>
              <div class='card-t'>Voltage</div>
              <table class='tbl'>
                <tr><th>Parameter</th><th>R</th><th>Y</th><th>B</th><th>Avg</th></tr>
                <tr><td>V L-N (V)</td>
                    <td class='{_value_class(_to_float(v1n), nominal_v_ln)}'>{_fmt_num(v1n,1)}</td>
                    <td class='{_value_class(_to_float(v2n), nominal_v_ln)}'>{_fmt_num(v2n,1)}</td>
                    <td class='{_value_class(_to_float(v3n), nominal_v_ln)}'>{_fmt_num(v3n,1)}</td>
                    <td class='{vavg_cls}'>{_fmt_num(vavg,1)}</td></tr>
                <tr><td>V L-L (V)</td>
                    <td class='{_value_class(_to_float(v12), nominal_v_ll)}'>{_fmt_num(v12,1)}</td>
                    <td class='{_value_class(_to_float(v23), nominal_v_ll)}'>{_fmt_num(v23,1)}</td>
                    <td class='{_value_class(_to_float(v31), nominal_v_ll)}'>{_fmt_num(v31,1)}</td>
                    <td class='{_value_class(_to_float(v12), nominal_v_ll)}'>{_fmt_num(v12,1)}</td></tr>
                <tr><td>THD-V (%)</td>
                    <td colspan='4'>{_fmt_num(thdv,2)}</td></tr>
              </table>
            </div>

            <div class='card'>
              <div class='card-t'>Current</div>
              <table class='tbl'>
                <tr><th>Parameter</th><th>R</th><th>Y</th><th>B</th><th>N</th></tr>
                <tr><td>Current (A)</td>
                    <td>{_fmt_num(i1,2)}</td><td>{_fmt_num(i2,2)}</td><td>{_fmt_num(i3,2)}</td><td>{_fmt_num(in_,2)}</td></tr>
                <tr><td>THD-I (%)</td>
                    <td colspan='4'>{_fmt_num(thdi,2)}</td></tr>
              </table>
            </div>
          </div>

          <div class='card'>
            <div class='card-t'>Power</div>
            <table class='tbl'>
              <tr><th>Active Power (kW)</th><th>Reactive (kVAr)</th><th>Apparent (kVA)</th><th>PF</th><th>Frequency (Hz)</th></tr>
              <tr>
                <td>{_fmt_num(kw,2)}</td>
                <td>{_fmt_num(kvar,2)}</td>
                <td>{_fmt_num(kva,2)}</td>
                <td>{_fmt_num(pf,3)}</td>
                <td class='{freq_cls}'>{_fmt_num(freq,2)}</td>
              </tr>
            </table>
          </div>
        </div>
        """

    # Virtual meter block
    vblock = f"""
    <div class='box'>
      <div class='box-h'>FACILITY TOTAL (VIRTUAL METER)</div>
      <div class='muted'>Combined reading across all {len(meter_ids)} meters</div>
      <table class='tbl'>
        <tr><th>Total Active Power (kW)</th><th>Total Reactive (kVAr)</th><th>Total Apparent (kVA)</th><th>Combined PF</th><th>Frequency (Hz)</th></tr>
        <tr><td>{_fmt_num(total_vals.get('kW'),2)}</td><td>{_fmt_num(total_vals.get('kVAr'),2)}</td><td>{_fmt_num(total_vals.get('kVA'),2)}</td><td>{_fmt_num(total_vals.get('PFavg') or total_vals.get('PF'),3)}</td><td class='{_value_class(_to_float(total_vals.get('Frequency')), nominal_f)}'>{_fmt_num(total_vals.get('Frequency'),2)}</td></tr>
      </table>
      <table class='tbl'>
        <tr><th>Total Active Energy (kWh)</th><th>Export Energy (kWh)</th><th>Reactive Energy (kVArh)</th><th>Apparent Energy (kVAh)</th></tr>
        <tr>
          <td>{_fmt_num(total_vals.get('kWh') or total_vals.get('Energy_kWh'),1)}</td>
          <td>{_fmt_num(total_vals.get('kWh_export') or total_vals.get('Export_kWh'),1)}</td>
          <td>{_fmt_num(total_vals.get('kVArh') or total_vals.get('Energy_kVArh'),1)}</td>
          <td>{_fmt_num(total_vals.get('kVAh') or total_vals.get('Energy_kVAh'),1)}</td>
        </tr>
      </table>
    </div>
    """

    # Alarm/event log
    ev_rows = []
    for e in events[:500]:
        t = e.get("time") or e.get("Time") or e.get("Timestamp") or ""
        rule = e.get("rule") or e.get("Rule") or e.get("alarm") or e.get("Alarm") or ""
        st = e.get("state") or e.get("State") or ""
        val = e.get("value") or e.get("Value") or ""
        mid = e.get("meter") or e.get("Meter") or e.get("meter_id") or ""
        ev_rows.append(f"<tr><td>{h(t)}</td><td>{h(mid)}</td><td>{h(rule)}</td><td>{h(val)}</td><td>{h(st)}</td></tr>")
    ev_table = "".join(ev_rows) if ev_rows else "<tr><td colspan='5' class='muted'>No events in period</td></tr>"

    # Stats for daily/hourly/custom
    stats_html = ""
    if rtype in ("HOURLY", "DAILY", "CUSTOM"):
        # Only show TOTAL stats by default
        st = (stats.get("TOTAL") or {})
        def st_row(label: str, key: str, ndp: int):
            mn, mx, av, sd = st.get(key) or (None, None, None, None)
            return f"<tr><td>{h(label)}</td><td>{_fmt_num(mn, ndp)}</td><td>{_fmt_num(mx, ndp)}</td><td>{_fmt_num(av, ndp)}</td><td>{_fmt_num(sd, ndp)}</td></tr>"
        stats_html = f"""
        <div class='card'>
          <div class='card-t'>Statistical Analysis (TOTAL)</div>
          <table class='tbl'>
            <tr><th>Parameter</th><th>Min</th><th>Max</th><th>Avg</th><th>Std Dev</th></tr>
            {st_row('Voltage Avg (V)', 'Vavg', 1)}
            {st_row('Current Avg (A)', 'Iavg', 2)}
            {st_row('Active Power (kW)', 'kW', 2)}
            {st_row('Power Factor', 'PFavg', 3)}
            {st_row('Frequency (Hz)', 'Frequency', 2)}
          </table>
        </div>
        """

    # Executive summary
    exec_summary = f"""
    <div class='box'>
      <div class='box-h'>EXECUTIVE SUMMARY</div>
      <table class='tbl'>
        <tr><th>Total meters monitored</th><td>{len(meter_ids)}</td>
            <th>Report period</th><td>{h(start_s)} → {h(end_s)}</td></tr>
        <tr><th>System status</th><td><span class='pill'>{h(system_status)}</span></td>
            <th>Total energy (kWh)</th><td>{_fmt_num(total_kwh,1)}</td></tr>
        <tr><th>Average PF</th><td>{_fmt_num(total_pf,3)}</td>
            <th>Current demand (kW)</th><td>{_fmt_num(total_kw,2)}</td></tr>
      </table>
    </div>
    """

    meters_html = "".join(meter_block(mid) for mid in meter_ids)

    footer_left = f"{h(company_name)} {(' | ' + h(company_contact)) if company_contact else ''}".strip()
    standards = "IEC 61557-12, IEC 61000-2-2, ISO 50001, IEEE 519"

    html_doc = f"""<!doctype html>
<html>
<head>
  <meta charset='utf-8'>
  <title>{h(facility_code)} {h(rtype)} Report</title>
  <style>
    @page {{ size: A4; margin: 14mm 10mm 16mm 10mm; }}
    body {{ font-family: Arial, Helvetica, sans-serif; font-size: 11px; color: #111; }}
    .header {{ display:flex; align-items:center; gap:10px; border-bottom: 2px solid #222; padding-bottom: 6px; margin-bottom: 10px; }}
    .logo {{ height: 38px; }}
    .logo-ph {{ width: 90px; height: 38px; border:1px solid #666; display:flex; align-items:center; justify-content:center; color:#666; font-weight:bold; }}
    .title {{ flex:1; text-align:center; }}
    .title h1 {{ margin:0; font-size: 14px; letter-spacing: 0.5px; }}
    .meta {{ text-align:right; font-size: 10px; color:#222; }}
    .muted {{ color:#666; }}
    .box {{ border: 1px solid #444; padding: 8px; margin: 10px 0; }}
    .box-h {{ font-weight: bold; margin-bottom: 6px; }}
    .card {{ border: 1px solid #ddd; padding: 8px; margin: 8px 0; }}
    .card-t {{ font-weight: bold; margin-bottom: 6px; }}
    .tbl {{ width:100%; border-collapse: collapse; }}
    .tbl th, .tbl td {{ border: 1px solid #bbb; padding: 4px 6px; }}
    .tbl th {{ background:#f0f0f0; text-align:left; }}
    .grid2 {{ display:grid; grid-template-columns: 1fr 1fr; gap: 8px; }}
    .meter {{ page-break-inside: avoid; margin: 10px 0; }}
    .meter-h {{ font-size: 12px; font-weight:bold; padding: 6px 0; border-bottom: 1px solid #bbb; }}
    .pill {{ padding:2px 6px; border:1px solid #333; border-radius: 10px; font-size:10px; }}

    .v-ok {{ color:#0a6; font-weight:bold; }}
    .v-warn {{ color:#c80; font-weight:bold; }}
    .v-alarm {{ color:#c00; font-weight:bold; }}
    .v-neutral {{ color:#111; }}

    .footer {{ position: fixed; bottom: 6mm; left: 10mm; right: 10mm; font-size: 9px; color:#222; border-top:1px solid #222; padding-top:4px; }}
    .footer .row {{ display:flex; justify-content:space-between; }}
    .watermark {{ position: fixed; top: 45%; left: 0; right:0; text-align:center; font-size: 52px; color: rgba(120,120,120,0.18); transform: rotate(-20deg); z-index: -1; }}
  </style>
</head>
<body>
  {f"<div class='watermark'>{h(watermark)}</div>" if watermark else ""}

  <div class='header'>
    {logo_html}
    <div class='title'>
      <h1>ELECTRICAL POWER MONITORING REPORT</h1>
      <div class='muted'>{h(facility_name)} {(' - ' + h(facility_loc)) if facility_loc else ''}</div>
    </div>
    <div class='meta'>
      <div><b>Report Type:</b> {h(rtype)}</div>
      <div><b>Date Range:</b> {h(start_s)} → {h(end_s)}</div>
      <div><b>Generated:</b> {h(gen_s)}</div>
    </div>
  </div>

  {exec_summary}

  <div class='card'>
    <div class='card-t'>INDIVIDUAL METER DATA</div>
    {meters_html}
  </div>

  {vblock}

  <div class='card'>
    <div class='card-t'>ALARM & EVENT LOG</div>
    <table class='tbl'>
      <tr><th>Date/Time</th><th>Meter</th><th>Alarm/Rule</th><th>Value</th><th>Status</th></tr>
      {ev_table}
    </table>
  </div>

  {stats_html}

  <div class='footer'>
    <div class='row'><span>Report Generated By: MFM384 Monitoring | Data Source: Selec MFM384 Meters</span><span>Standards: {h(standards)}</span></div>
    <div class='row'><span>Generated On: {h(gen_s)}</span><span>{footer_left}</span></div>
  </div>
</body>
</html>
"""

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html_doc)


def _to_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        if isinstance(x, (int, float)):
            return float(x)
        s = str(x).strip()
        if not s or s in ("—", "-", "N/A"):
            return None
        return float(s)
    except Exception:
        return None
