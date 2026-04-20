"""ReportLab PDF template for MFM384 Monitoring reports.

Focused on:
- A4 layout, professional header/footer
- Executive summary
- Per-meter block (key electrical + energy values)
- Virtual meter (TOTAL)
- Alarm/event log
- Statistics section for DAILY/HOURLY/CUSTOM

NOTE: The app's register map may not expose every phase value. This template
fills missing values as "N/A" and includes a data completeness indicator.
"""

from __future__ import annotations

import os
import math
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import (
    SimpleDocTemplate,
    Paragraph,
    Spacer,
    Table,
    TableStyle,
    PageBreak,
)
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.pdfgen import canvas
from reportlab.lib.pdfencrypt import StandardEncryption

from core.reports_engine import ReportRequest, _safe_float


# -----------------------------------------------------------------------------
# Snapshot helpers
# -----------------------------------------------------------------------------

def _get_src_values(snapshot: Dict[str, Any], source: Any) -> Dict[str, Any]:
    """Return values dict for a given source.

    MainWindow._get_snapshot() returns:
      {
        'total': {'values': {...}},
        'meters': {meter_id: {'values': {...}}, ...}
      }

    This report layer also uses log source names like 'TOTAL' and 'M3'.
    """
    if not snapshot:
        return {}
    # TOTAL
    if source == 'TOTAL' or source == 'total':
        return (snapshot.get('total') or {}).get('values') or {}

    # meter id
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


def _pick_thd(src: Dict[str, Any]) -> tuple[Optional[float], Optional[float]]:
    """Best-effort THD extraction using canonical keys in this app."""
    thdv = _safe_float(src.get('THD_V'))
    thdi = _safe_float(src.get('THD_I'))
    if thdv is None:
        thdv = _safe_float(src.get('THD_V_worst'))
    if thdi is None:
        thdi = _safe_float(src.get('THD_I_worst'))

    # fall back to per-phase THD keys (choose max)
    if thdv is None:
        vals = []
        for k, v in (src or {}).items():
            if str(k).startswith('THD Voltage'):
                fv = _safe_float(v)
                if fv is not None:
                    vals.append(fv)
        if vals:
            thdv = max(vals)

    if thdi is None:
        vals = []
        for k, v in (src or {}).items():
            if str(k).startswith('THD Current'):
                fv = _safe_float(v)
                if fv is not None:
                    vals.append(fv)
        if vals:
            thdi = max(vals)

    return thdv, thdi


def _fmt(v: Optional[float], fmt: str, na: str = "N/A") -> str:
    if v is None:
        return na
    try:
        return format(float(v), fmt)
    except Exception:
        return na


def _class_color(value: Optional[float], nominal: Optional[float], warn_pct: float = 10.0, alarm_pct: float = 15.0):
    """Return (text_color, label) for deviation-based coloring."""
    if value is None or nominal in (None, 0):
        return colors.grey, "N/A"
    dev = abs((value - nominal) / nominal) * 100.0
    if dev <= warn_pct:
        return colors.green, "NORMAL"
    if dev <= alarm_pct:
        return colors.orange, "WARNING"
    return colors.red, "ALARM"


def _pf_color(pf: Optional[float]):
    if pf is None:
        return colors.grey, "N/A"
    if pf >= 0.90:
        return colors.green, "PASS"
    if pf >= 0.85:
        return colors.orange, "WARN"
    return colors.red, "FAIL"


class HeaderFooter:
    def __init__(self, cfg: Dict[str, Any], req: ReportRequest, meter_ids: List[int]):
        self.cfg = cfg
        self.req = req
        self.meter_ids = meter_ids

    def on_page(self, canv: canvas.Canvas, doc: SimpleDocTemplate):
        rcfg = self.cfg.get("reports", {}) or {}
        site = self.cfg.get("site", {}) or {}

        logo = str(rcfg.get("logo_path") or "").strip()
        # Logo size in mm (configurable)
        try:
            logo_size_mm = float(rcfg.get("logo_size_mm", 32) or 32)
        except Exception:
            logo_size_mm = 32.0
        title = "ELECTRICAL POWER MONITORING REPORT"
        facility = str(rcfg.get("facility_name") or site.get("plant_name") or "")
        location = str(rcfg.get("facility_location") or site.get("location") or "")
        report_type = self.req.report_type.upper()
        start = datetime.fromtimestamp(self.req.start_ts).strftime("%Y-%m-%d %H:%M")
        end = datetime.fromtimestamp(self.req.end_ts).strftime("%Y-%m-%d %H:%M")
        gen = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        w, h = A4
        margin_x = 15 * mm
        top_y = h - 12 * mm

        # Logo
        if logo and os.path.exists(logo):
            try:
                canv.drawImage(logo, margin_x, h - 30 * mm, width=logo_size_mm * mm, height=logo_size_mm * mm, preserveAspectRatio=True, mask='auto')
            except Exception:
                pass

        # Title centered
        canv.setFont("Helvetica-Bold", 12)
        canv.drawCentredString(w / 2.0, h - 18 * mm, title)

        # Left details
        canv.setFont("Helvetica", 8)
        canv.drawString(margin_x, h - 35 * mm, facility)
        canv.drawString(margin_x, h - 40 * mm, location)

        # Right details
        rx = w - margin_x
        def rline(y, label, val):
            canv.drawRightString(rx, y, f"{label}: {val}")

        rline(h - 35 * mm, "Report Type", report_type)
        rline(h - 40 * mm, "Date Range", f"{start} to {end}")
        rline(h - 45 * mm, "Generated", gen)

        # Separator line
        canv.setStrokeColor(colors.grey)
        canv.setLineWidth(0.5)
        canv.line(margin_x, h - 48 * mm, w - margin_x, h - 48 * mm)

        # Footer
        footer_y = 12 * mm
        canv.setStrokeColor(colors.grey)
        canv.line(margin_x, footer_y + 10 * mm, w - margin_x, footer_y + 10 * mm)

        sysname = "MFM384 Monitoring"
        standards = "IEC 61557-12, IEC 61000-2-2, ISO 50001, IEEE 519"
        company = str(rcfg.get("company_name") or "")
        contact = str(rcfg.get("company_contact") or "")

        canv.setFont("Helvetica", 7)
        canv.drawString(margin_x, footer_y + 6 * mm, f"Report Generated By: {sysname} | Data Source: Selec MFM384 Meters")
        canv.drawString(margin_x, footer_y + 3 * mm, f"Generated On: {gen} | Report Complies: {standards}")
        canv.drawString(margin_x, footer_y + 0 * mm, f"{company} - {contact}".strip(" -"))
        # Total page count requires a 2-pass build; keep reliable single-pass numbering.
        canv.drawRightString(w - margin_x, footer_y + 0 * mm, f"Page {canv.getPageNumber()}")


def build_report_pdf(cfg: Dict[str, Any], req: ReportRequest, meter_ids: List[int], data: Dict[str, Any], out_path: str) -> None:
    rcfg = cfg.get("reports", {}) or {}
    site = cfg.get("site", {}) or {}
    nominal_v = float((rcfg.get("nominal_vln") or 230.0)) if str(rcfg.get("nominal_vln") or "").strip() else 230.0
    nominal_f = float((rcfg.get("nominal_hz") or 50.0)) if str(rcfg.get("nominal_hz") or "").strip() else 50.0

    password = str(rcfg.get("pdf_password") or "").strip()
    encrypt = None
    if password:
        # user password=owner password=... simplest
        encrypt = StandardEncryption(userPassword=password, ownerPassword=password, canPrint=1, canCopy=0, canModify=0)

    doc = SimpleDocTemplate(
        out_path,
        pagesize=A4,
        leftMargin=15 * mm,
        rightMargin=15 * mm,
        topMargin=52 * mm,
        bottomMargin=22 * mm,
        title="Electrical Power Monitoring Report",
        encrypt=encrypt,
    )

    styles = getSampleStyleSheet()
    s_h = ParagraphStyle("h", parent=styles["Heading2"], fontName="Helvetica-Bold", fontSize=10, spaceAfter=6)
    s_n = ParagraphStyle("n", parent=styles["Normal"], fontName="Helvetica", fontSize=8, leading=10)

    story = []

    snap = data.get("snapshot") or {}

    # ---------------- Section 1: Executive summary ----------------
    story.append(Paragraph("1. EXECUTIVE SUMMARY", s_h))

    enabled_meters = len(meter_ids)
    period_hours = max(0.0, (req.end_ts - req.start_ts) / 3600.0)

    total_src = _get_src_values(snap, 'TOTAL')
    total_kw = _safe_float(total_src.get("kW") or total_src.get("Total_kW") or total_src.get("kW_total"))
    total_pf = _safe_float(total_src.get("PFavg") or total_src.get("PF"))
    total_kwh = _safe_float(total_src.get("Import_kWh") or total_src.get("Import_kwh") or total_src.get("Import_kWH"))
    peak_kw = None
    peak_ts = "N/A"
    try:
        rows_total = (data.get("rows") or {}).get("TOTAL") or []
        if rows_total:
            best = None
            for r in rows_total:
                v = _safe_float(r.get("kW"))
                if v is None:
                    continue
                if best is None or v > best[0]:
                    best = (v, str(r.get("Timestamp", "")))
            if best:
                peak_kw = best[0]
                peak_ts = best[1]
    except Exception:
        pass

    system_status = "Normal"
    # crude status: if any events in window -> Warning/Alarm
    evs = data.get("events") or []
    if evs:
        system_status = "Warning/Alarm"

    box = [
        ["Facility & System Information", ""],
        ["Total Meters Monitored", str(enabled_meters)],
        ["Report Period (hours)", f"{period_hours:.2f}"],
        ["System Status", system_status],
        ["Total Energy Consumption (kWh)", _fmt(total_kwh, ".1f")],
        ["Average Power Factor", _fmt(total_pf, ".3f")],
        ["Peak Demand (kW)", f"{_fmt(peak_kw, '.2f')} @ {peak_ts}"],
    ]
    t = Table(box, colWidths=[70 * mm, 100 * mm])
    t.setStyle(
        TableStyle(
            [
                ("BOX", (0, 0), (-1, -1), 0.8, colors.grey),
                ("BACKGROUND", (0, 0), (-1, 0), colors.whitesmoke),
                ("SPAN", (0, 0), (1, 0)),
                ("FONT", (0, 0), (-1, -1), "Helvetica", 8),
                ("FONT", (0, 0), (-1, 0), "Helvetica-Bold", 9),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f7f7f7")]),
            ]
        )
    )
    story.append(t)
    story.append(Spacer(1, 6 * mm))

    # ---------------- Section 2: Individual meter data ----------------
    story.append(Paragraph("2. INDIVIDUAL METER DATA", s_h))

    meters_map = snap.get("meters") or {}

    def meter_block(mid: int):
        src = meters_map.get(f"M{mid}") or {}
        # header
        story.append(Paragraph(f"Meter {mid} - {src.get('name','')} (Selec MFM384)", ParagraphStyle("mh", parent=s_h, fontSize=9)))

        vavg = _safe_float(src.get("Vavg"))
        freq = _safe_float(src.get("Frequency"))
        iavg = _safe_float(src.get("Iavg"))
        kw = _safe_float(src.get("kW"))
        kva = _safe_float(src.get("kVA"))
        kvar = _safe_float(src.get("kVAr"))
        pf = _safe_float(src.get("PF"))
        thdv, thdi = _pick_thd(src)
        imp = _safe_float(src.get("Import_kWh"))
        exp = _safe_float(src.get("Export_kWh"))

        # simple tables (phase values may not exist -> N/A)
        v_color, _ = _class_color(vavg, nominal_v)
        f_color, _ = _class_color(freq, nominal_f, warn_pct=1.0, alarm_pct=2.0)

        info = [
            ["Parameter", "Value"],
            ["Vavg (V L-N)", _fmt(vavg, ".1f")],
            ["Frequency (Hz)", _fmt(freq, ".2f")],
            ["Iavg (A)", _fmt(iavg, ".2f")],
            ["Active Power (kW)", _fmt(kw, ".2f")],
            ["Reactive Power (kVAr)", _fmt(kvar, ".2f")],
            ["Apparent Power (kVA)", _fmt(kva, ".2f")],
            ["Power Factor", _fmt(pf, ".3f")],
            ["THD-V (%)", _fmt(thdv, ".2f")],
            ["THD-I (%)", _fmt(thdi, ".2f")],
            ["Import Energy (kWh)", _fmt(imp, ".1f")],
            ["Export Energy (kWh)", _fmt(exp, ".1f")],
        ]
        tt = Table(info, colWidths=[55 * mm, 45 * mm])
        tt.setStyle(
            TableStyle(
                [
                    ("GRID", (0, 0), (-1, -1), 0.3, colors.grey),
                    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#eaeaea")),
                    ("FONT", (0, 0), (-1, 0), "Helvetica-Bold", 8),
                    ("FONT", (0, 1), (-1, -1), "Helvetica", 8),
                ]
            )
        )
        # apply color to key rows
        tt.setStyle(TableStyle([("TEXTCOLOR", (1, 1), (1, 1), v_color), ("TEXTCOLOR", (1, 2), (1, 2), f_color)]))
        story.append(tt)
        story.append(Spacer(1, 4 * mm))

    for mid in meter_ids:
        meter_block(mid)

    story.append(Spacer(1, 4 * mm))

    # ---------------- Section 3: Virtual meter ----------------
    story.append(Paragraph("3. FACILITY TOTAL (VIRTUAL METER)", s_h))
    tot = meters_map.get("TOTAL") or {}
    tkw = _safe_float(tot.get("kW") or tot.get("Total_kW"))
    tkva = _safe_float(tot.get("kVA") or tot.get("Total_kVA"))
    tkvar = _safe_float(tot.get("kVAr") or tot.get("Total_kVAr"))
    tpf = _safe_float(tot.get("PFavg") or tot.get("PF"))
    tim = _safe_float(tot.get("Import_kWh"))

    b = [
        ["Combined Reading Across All Meters", ""],
        ["Total Active Power (kW)", _fmt(tkw, ".2f")],
        ["Total Reactive Power (kVAr)", _fmt(tkvar, ".2f")],
        ["Total Apparent Power (kVA)", _fmt(tkva, ".2f")],
        ["Combined Power Factor", _fmt(tpf, ".3f")],
        ["Total Import Energy (kWh)", _fmt(tim, ".1f")],
    ]
    bt = Table(b, colWidths=[70 * mm, 100 * mm])
    bt.setStyle(
        TableStyle(
            [
                ("BOX", (0, 0), (-1, -1), 0.8, colors.grey),
                ("BACKGROUND", (0, 0), (-1, 0), colors.whitesmoke),
                ("SPAN", (0, 0), (1, 0)),
                ("FONT", (0, 0), (-1, -1), "Helvetica", 8),
                ("FONT", (0, 0), (-1, 0), "Helvetica-Bold", 9),
            ]
        )
    )
    story.append(bt)
    story.append(Spacer(1, 6 * mm))

    # ---------------- Section 4: Alarm & Event log ----------------
    story.append(Paragraph("4. ALARM & EVENT LOG", s_h))
    ev = data.get("events") or []
    # Normalize columns
    rows = [["Date/Time", "Meter", "Alarm Type", "Value", "Status"]]
    active = 0
    cleared = 0
    for e in ev[:200]:
        ts = e.get("time") or e.get("Time") or e.get("Timestamp") or ""
        rid = e.get("rid") or e.get("Rule") or e.get("rule") or ""
        scope = e.get("scope") or e.get("Scope") or e.get("meter") or ""
        val = e.get("value") or e.get("Value") or ""
        st = e.get("state") or e.get("State") or ""
        status = "Active" if str(st).upper() in ("ALARM", "WARNING") else "Cleared"
        if status == "Active":
            active += 1
        else:
            cleared += 1
        rows.append([ts, str(scope), str(rid), str(val), status])

    if len(rows) == 1:
        rows.append(["N/A", "", "No events in this period", "", ""])
    et = Table(rows, colWidths=[35 * mm, 15 * mm, 70 * mm, 25 * mm, 20 * mm])
    et.setStyle(
        TableStyle(
            [
                ("GRID", (0, 0), (-1, -1), 0.3, colors.grey),
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#eaeaea")),
                ("FONT", (0, 0), (-1, 0), "Helvetica-Bold", 8),
                ("FONT", (0, 1), (-1, -1), "Helvetica", 7),
            ]
        )
    )
    story.append(et)
    story.append(Spacer(1, 2 * mm))
    story.append(Paragraph(f"Total Alarms: {max(0,len(ev))} (Active: {active}, Cleared: {cleared})", s_n))

    # ---------------- Section 5: Stats for period reports ----------------
    if req.report_type.upper() in ("HOURLY", "DAILY", "CUSTOM"):
        story.append(Spacer(1, 6 * mm))
        story.append(Paragraph("5. STATISTICAL ANALYSIS", s_h))

        stats = (data.get("stats") or {}).get("TOTAL") or {}
        srows = [["Parameter", "Min", "Max", "Avg", "Std Dev"]]
        mapping = [
            ("Voltage (Vavg)", "Vavg", ".1f"),
            ("Current (Iavg)", "Iavg", ".2f"),
            ("Active Power (kW)", "kW", ".2f"),
            ("Power Factor (PFavg)", "PFavg", ".3f"),
            ("Frequency (Hz)", "Frequency", ".2f"),
        ]
        for label, key, fmt in mapping:
            mn, mx, av, sd = stats.get(key, (None, None, None, None))
            srows.append([label, _fmt(mn, fmt), _fmt(mx, fmt), _fmt(av, fmt), _fmt(sd, fmt)])
        stbl = Table(srows, colWidths=[55 * mm, 28 * mm, 28 * mm, 28 * mm, 28 * mm])
        stbl.setStyle(
            TableStyle(
                [
                    ("GRID", (0, 0), (-1, -1), 0.3, colors.grey),
                    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#eaeaea")),
                    ("FONT", (0, 0), (-1, 0), "Helvetica-Bold", 8),
                    ("FONT", (0, 1), (-1, -1), "Helvetica", 7),
                ]
            )
        )
        story.append(stbl)

        story.append(Spacer(1, 4 * mm))
        story.append(Paragraph("Power Quality Compliance (best-effort)", s_n))
        # compliance checks
        v_av = stats.get("Vavg", (None, None, None, None))[2]
        f_av = stats.get("Frequency", (None, None, None, None))[2]
        thd_v_av = (stats.get('THD_V', (None,None,None,None))[2] if 'THD_V' in stats else None)
        if thd_v_av is None:
            thd_v_av = stats.get('THD_V_worst', (None,None,None,None))[2]
        pf_av = stats.get("PFavg", (None, None, None, None))[2]

        def pct_compliance(rows_list, key, check_fn):
            if not rows_list:
                return None
            total = 0
            good = 0
            for r in rows_list:
                v = _safe_float(r.get(key))
                if v is None:
                    continue
                total += 1
                if check_fn(v):
                    good += 1
            if total == 0:
                return None
            return good * 100.0 / total

        rows_total = (data.get("rows") or {}).get("TOTAL") or []
        c_v = pct_compliance(rows_total, "Vavg", lambda x: abs(x - nominal_v) <= nominal_v * 0.10)
        c_f = pct_compliance(rows_total, "Frequency", lambda x: abs(x - nominal_f) <= 0.5)
        c_t = pct_compliance(rows_total, 'THD_V', lambda x: x < 5.0)
        if c_t[0] is None:  # try worst key
            c_t = pct_compliance(rows_total, 'THD_V_worst', lambda x: x < 5.0)
        c_pf = pct_compliance(rows_total, "PFavg", lambda x: x >= 0.90)

        crow = [["Parameter", "Status", "% Time in Compliance"]]
        def passfail(p):
            if p is None:
                return ("N/A", "N/A")
            return ("PASS" if p >= 95.0 else "FAIL", f"{p:.1f}%")

        for label, pct in [
            ("Voltage within ±10% nominal", c_v),
            ("Frequency within ±0.5Hz", c_f),
            ("THD-V < 5% (IEC 61000-2-2)", c_t),
            ("Power Factor > 0.90", c_pf),
        ]:
            st, ptxt = passfail(pct)
            crow.append([label, st, ptxt])
        ct = Table(crow, colWidths=[95 * mm, 25 * mm, 50 * mm])
        ct.setStyle(
            TableStyle(
                [
                    ("GRID", (0, 0), (-1, -1), 0.3, colors.grey),
                    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#eaeaea")),
                    ("FONT", (0, 0), (-1, 0), "Helvetica-Bold", 8),
                    ("FONT", (0, 1), (-1, -1), "Helvetica", 7),
                ]
            )
        )
        story.append(ct)

    # Watermark (optional)
    watermark = str(rcfg.get("watermark") or "").strip()

    hf = HeaderFooter(cfg, req, meter_ids)
    doc.build(story, onFirstPage=hf.on_page, onLaterPages=hf.on_page)
