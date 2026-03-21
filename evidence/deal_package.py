"""
Deal Package PDF Generator — Multi-page investor-ready document.

Produces a professional 5-page PDF with everything needed to evaluate
a greenfield pharmacy opportunity:

  Page 1: Executive Summary (verdict, key metrics, profitability)
  Page 2: Compliance Evidence (ACPA rules, distances, map)
  Page 3: Financial Projections (revenue, costs, ROI, bar chart)
  Page 4: Competition Analysis (pharmacies, GPs, hospitals, map)
  Page 5: Commercial Property Options (available leases)

Usage:
    from evidence.deal_package import generate_deal_package
    pdf_path = generate_deal_package("supermarket_27601", "pharmacy_finder.db")
"""
from __future__ import annotations

import io
import json
import math
import os
import sqlite3
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm, cm
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    Image, PageBreak, KeepTogether, HRFlowable,
)
from reportlab.graphics.shapes import Drawing, Rect, String
from reportlab.graphics.charts.barcharts import VerticalBarChart
from reportlab.graphics import renderPDF

try:
    import staticmap
    HAS_STATICMAP = True
except ImportError:
    HAS_STATICMAP = False


# ================================================================
# Constants
# ================================================================
PAGE_W, PAGE_H = A4  # 595.27, 841.89 points
MARGIN = 20 * mm

# Colours
NAVY = colors.HexColor("#003366")
BLUE = colors.HexColor("#0066CC")
DARK = colors.HexColor("#333333")
GREY = colors.HexColor("#888888")
LIGHT_BG = colors.HexColor("#F0F5FF")
GREEN = colors.HexColor("#228B22")
AMBER = colors.HexColor("#FF8C00")
RED = colors.HexColor("#DC143C")
WHITE = colors.white

# Financial assumptions
SCRIPTS_PER_DAY_LOW = 80
SCRIPTS_PER_DAY_HIGH = 150
AVG_PBS_REVENUE_PER_SCRIPT = 18.50
FRONT_OF_SHOP_RATIO = 0.35  # FoS as fraction of total revenue
GROSS_MARGIN = 0.33
FITOUT_LOW, FITOUT_HIGH = 250_000, 400_000
STOCK_LOW, STOCK_HIGH = 150_000, 250_000
WORKING_CAP_LOW, WORKING_CAP_HIGH = 50_000, 100_000
EXIT_MULTIPLE = 0.4  # revenue x 0.4


# ================================================================
# Styles
# ================================================================
def _build_styles():
    ss = getSampleStyleSheet()
    styles = {}
    styles["title"] = ParagraphStyle(
        "DPTitle", parent=ss["Title"], fontSize=26, textColor=NAVY,
        spaceAfter=6, alignment=TA_CENTER,
    )
    styles["subtitle"] = ParagraphStyle(
        "DPSubtitle", parent=ss["Normal"], fontSize=14, textColor=DARK,
        spaceAfter=4, alignment=TA_CENTER,
    )
    styles["heading"] = ParagraphStyle(
        "DPHeading", parent=ss["Heading1"], fontSize=16, textColor=NAVY,
        spaceBefore=8, spaceAfter=6,
    )
    styles["subheading"] = ParagraphStyle(
        "DPSubheading", parent=ss["Heading2"], fontSize=12, textColor=DARK,
        spaceBefore=6, spaceAfter=4,
    )
    styles["body"] = ParagraphStyle(
        "DPBody", parent=ss["Normal"], fontSize=10, textColor=DARK,
        spaceAfter=4, leading=14,
    )
    styles["small"] = ParagraphStyle(
        "DPSmall", parent=ss["Normal"], fontSize=8, textColor=GREY,
        spaceAfter=2, leading=10,
    )
    styles["verdict_go"] = ParagraphStyle(
        "VerdictGo", parent=ss["Title"], fontSize=36, textColor=WHITE,
        alignment=TA_CENTER, spaceAfter=0, spaceBefore=0,
    )
    styles["metric_label"] = ParagraphStyle(
        "MetricLabel", parent=ss["Normal"], fontSize=8, textColor=GREY,
        alignment=TA_CENTER, spaceAfter=1,
    )
    styles["metric_value"] = ParagraphStyle(
        "MetricValue", parent=ss["Normal"], fontSize=14, textColor=NAVY,
        alignment=TA_CENTER, spaceBefore=0, spaceAfter=1,
        fontName="Helvetica-Bold",
    )
    styles["disclaimer"] = ParagraphStyle(
        "DPDisclaimer", parent=ss["Normal"], fontSize=7, textColor=GREY,
        alignment=TA_CENTER, leading=9, spaceAfter=2,
    )
    styles["table_header"] = ParagraphStyle(
        "THdr", parent=ss["Normal"], fontSize=9, textColor=WHITE,
        fontName="Helvetica-Bold", alignment=TA_CENTER,
    )
    styles["table_cell"] = ParagraphStyle(
        "TCell", parent=ss["Normal"], fontSize=8, textColor=DARK,
        leading=10,
    )
    styles["table_cell_center"] = ParagraphStyle(
        "TCellC", parent=ss["Normal"], fontSize=8, textColor=DARK,
        alignment=TA_CENTER, leading=10,
    )
    return styles


# ================================================================
# Data helpers
# ================================================================
def _haversine(lat1, lon1, lat2, lon2):
    """Distance in metres between two lat/lon points."""
    R = 6_371_000
    p = math.pi / 180
    a = (
        0.5 - math.cos((lat2 - lat1) * p) / 2
        + math.cos(lat1 * p) * math.cos(lat2 * p)
        * (1 - math.cos((lon2 - lon1) * p)) / 2
    )
    return R * 2 * math.asin(math.sqrt(a))


def _fmt_dist(m: float) -> str:
    if m < 1000:
        return f"{m:.0f}m"
    return f"{m / 1000:.1f}km"


def _fmt_money(val: float) -> str:
    if val >= 1_000_000:
        return f"${val / 1_000_000:.2f}M"
    if val >= 1_000:
        return f"${val / 1_000:.0f}k"
    return f"${val:.0f}"


def _sanitize(text) -> str:
    """Replace problematic Unicode chars — safe for reportlab core fonts."""
    if not text:
        return ""
    text = str(text)
    replacements = {
        "\u2014": "-", "\u2013": "-", "\u2018": "'", "\u2019": "'",
        "\u201c": '"', "\u201d": '"', "\u2022": "*", "\u2026": "...",
        "\u2264": "<=", "\u2265": ">=", "\u2194": "<->",
        "\u2713": "Y", "\u2714": "Y", "\u2717": "X", "\u2716": "X",
        "\u25cf": "*", "\u00b2": "2", "\u00b0": "deg",
        "\u2192": "->", "\u2190": "<-",
    }
    for c, r in replacements.items():
        text = text.replace(c, r)
    # Strip any remaining non-latin1 chars
    try:
        text.encode("latin-1")
    except UnicodeEncodeError:
        text = text.encode("latin-1", errors="replace").decode("latin-1")
    return text


def _load_site(db_path: str, site_id: str) -> Dict[str, Any]:
    """Load site data from v2_results."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT * FROM v2_results WHERE id = ?", (site_id,)).fetchone()
    if not row:
        conn.close()
        raise ValueError(f"Site {site_id} not found in v2_results")
    data = dict(row)
    conn.close()
    return data


def _load_nearby(db_path: str, lat: float, lon: float) -> Dict[str, list]:
    """Load nearby facilities from DB."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    result = {}

    # Pharmacies within ~15km (rough bbox)
    delta = 0.15  # ~15km
    rows = conn.execute(
        "SELECT * FROM pharmacies WHERE latitude BETWEEN ? AND ? AND longitude BETWEEN ? AND ?",
        (lat - delta, lat + delta, lon - delta, lon + delta),
    ).fetchall()
    pharms = []
    for r in rows:
        d = _haversine(lat, lon, r["latitude"], r["longitude"])
        if d <= 15_000:
            pharms.append({**dict(r), "_dist_m": d})
    pharms.sort(key=lambda x: x["_dist_m"])
    result["pharmacies"] = pharms

    # GPs within 5km
    delta_gp = 0.05
    rows = conn.execute(
        "SELECT * FROM gps WHERE latitude BETWEEN ? AND ? AND longitude BETWEEN ? AND ?",
        (lat - delta_gp, lat + delta_gp, lon - delta_gp, lon + delta_gp),
    ).fetchall()
    gps = []
    for r in rows:
        d = _haversine(lat, lon, r["latitude"], r["longitude"])
        if d <= 5_000:
            gps.append({**dict(r), "_dist_m": d})
    gps.sort(key=lambda x: x["_dist_m"])
    result["gps"] = gps

    # Medical centres within 5km
    rows = conn.execute(
        "SELECT * FROM medical_centres WHERE latitude BETWEEN ? AND ? AND longitude BETWEEN ? AND ?",
        (lat - delta_gp, lat + delta_gp, lon - delta_gp, lon + delta_gp),
    ).fetchall()
    mcs = []
    for r in rows:
        d = _haversine(lat, lon, r["latitude"], r["longitude"])
        if d <= 5_000:
            mcs.append({**dict(r), "_dist_m": d})
    mcs.sort(key=lambda x: x["_dist_m"])
    result["medical_centres"] = mcs

    # Hospitals within 10km
    delta_h = 0.1
    rows = conn.execute(
        "SELECT * FROM hospitals WHERE latitude BETWEEN ? AND ? AND longitude BETWEEN ? AND ?",
        (lat - delta_h, lat + delta_h, lon - delta_h, lon + delta_h),
    ).fetchall()
    hosps = []
    for r in rows:
        d = _haversine(lat, lon, r["latitude"], r["longitude"])
        if d <= 10_000:
            hosps.append({**dict(r), "_dist_m": d})
    hosps.sort(key=lambda x: x["_dist_m"])
    result["hospitals"] = hosps

    # Commercial matches
    rows = conn.execute(
        "SELECT * FROM commercial_matches WHERE site_id = ?", (site_id_placeholder := "",)
    ).fetchall()
    result["commercial_matches"] = [dict(r) for r in rows]

    conn.close()
    return result


def _load_commercial(db_path: str, site_id: str) -> list:
    """Load commercial property matches for a site."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT * FROM commercial_matches WHERE site_id = ?", (site_id,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def _determine_verdict(site: dict) -> str:
    """Determine GO / CAUTION / NO-GO from site data."""
    if not site.get("passed_any"):
        return "NO-GO"
    conf = site.get("best_confidence", 0)
    prof = site.get("profitability_score", 0)
    if conf >= 0.7 and prof >= 60:
        return "GO"
    if conf >= 0.5 or prof >= 40:
        return "CAUTION"
    return "NO-GO"


def _confidence_tier(conf: float) -> str:
    if conf >= 0.7:
        return "HIGH"
    if conf >= 0.5:
        return "MEDIUM"
    return "LOW"


def _estimate_financials(site: dict, nearby: dict) -> dict:
    """Estimate financial projections based on site data."""
    prof = site.get("profitability_score", 50)
    # Scale scripts/day based on profitability score
    scale = max(0.3, min(1.0, prof / 100))
    scripts_day = int(SCRIPTS_PER_DAY_LOW + (SCRIPTS_PER_DAY_HIGH - SCRIPTS_PER_DAY_LOW) * scale)
    scripts_year = scripts_day * 365

    pbs_revenue = scripts_year * AVG_PBS_REVENUE_PER_SCRIPT
    total_revenue = pbs_revenue / (1 - FRONT_OF_SHOP_RATIO)
    fos_revenue = total_revenue - pbs_revenue

    gross_profit = total_revenue * GROSS_MARGIN

    fitout = int((FITOUT_LOW + FITOUT_HIGH) / 2)
    stock = int((STOCK_LOW + STOCK_HIGH) / 2)
    working_cap = int((WORKING_CAP_LOW + WORKING_CAP_HIGH) / 2)
    setup_cost = fitout + stock + working_cap

    exit_value = total_revenue * EXIT_MULTIPLE
    flip_profit = exit_value - setup_cost
    roi_pct = (flip_profit / setup_cost) * 100 if setup_cost > 0 else 0

    # Payback in months from gross profit
    payback_months = (setup_cost / (gross_profit / 12)) if gross_profit > 0 else 999

    return {
        "scripts_day": scripts_day,
        "scripts_year": scripts_year,
        "pbs_revenue": pbs_revenue,
        "fos_revenue": fos_revenue,
        "total_revenue": total_revenue,
        "gross_profit": gross_profit,
        "fitout": fitout,
        "stock": stock,
        "working_cap": working_cap,
        "setup_cost": setup_cost,
        "exit_value": exit_value,
        "flip_profit": flip_profit,
        "roi_pct": roi_pct,
        "payback_months": payback_months,
    }


# ================================================================
# Map generation
# ================================================================
def _generate_map(lat: float, lon: float, radius_m: float = 1000,
                  markers: Optional[List[Tuple[float, float, str]]] = None,
                  zoom: int = 14, width: int = 600, height: int = 400) -> Optional[bytes]:
    """Generate a static map image as PNG bytes."""
    if not HAS_STATICMAP:
        return None
    try:
        m = staticmap.StaticMap(width, height,
                                url_template="https://tile.openstreetmap.org/{z}/{x}/{y}.png")

        # Site marker (red)
        m.add_marker(staticmap.CircleMarker(
            (lon, lat), color="red", width=12,
        ))

        # Draw radius circle
        if radius_m > 0:
            steps = 36
            for i in range(steps + 1):
                angle = 2 * math.pi * i / steps
                dlat = (radius_m / 6_371_000) * (180 / math.pi) * math.cos(angle)
                dlon = (radius_m / 6_371_000) * (180 / math.pi) * math.sin(angle) / math.cos(lat * math.pi / 180)
                m.add_marker(staticmap.CircleMarker(
                    (lon + dlon, lat + dlat), color="blue", width=3,
                ))

        # Additional markers
        if markers:
            marker_colors = {"pharmacy": "green", "gp": "orange", "hospital": "purple", "mc": "orange"}
            for mlat, mlon, mtype in markers:
                clr = marker_colors.get(mtype, "gray")
                m.add_marker(staticmap.CircleMarker(
                    (mlon, mlat), color=clr, width=8,
                ))

        img = m.render(zoom=zoom)
        buf = io.BytesIO()
        img.save(buf, format="PNG")
        return buf.getvalue()
    except Exception:
        return None


# ================================================================
# Page builders
# ================================================================
def _build_page1(site: dict, financials: dict, styles: dict) -> list:
    """Page 1: Executive Summary."""
    elements = []
    verdict = _determine_verdict(site)

    # Title
    elements.append(Paragraph("DEAL PACKAGE", styles["title"]))
    elements.append(Paragraph(
        _sanitize(site.get("name", "Unknown Site")), styles["subtitle"]
    ))
    elements.append(Paragraph(
        _sanitize(site.get("address", "")), styles["body"]
    ))
    elements.append(Spacer(1, 4 * mm))

    # Verdict badge
    verdict_color = {
        "GO": GREEN, "CAUTION": AMBER, "NO-GO": RED,
    }.get(verdict, GREY)

    badge_data = [[Paragraph(verdict, styles["verdict_go"])]]
    badge_table = Table(badge_data, colWidths=[160], rowHeights=[50])
    badge_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), verdict_color),
        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("ROUNDEDCORNERS", [8, 8, 8, 8]),
        ("LEFTPADDING", (0, 0), (-1, -1), 10),
        ("RIGHTPADDING", (0, 0), (-1, -1), 10),
    ]))
    # Centre the badge
    outer = Table([[badge_table]], colWidths=[PAGE_W - 2 * MARGIN])
    outer.setStyle(TableStyle([("ALIGN", (0, 0), (-1, -1), "CENTER")]))
    elements.append(outer)
    elements.append(Spacer(1, 6 * mm))

    # Key info row
    info_data = [
        [Paragraph("State", styles["metric_label"]),
         Paragraph("Primary Rule", styles["metric_label"]),
         Paragraph("Confidence", styles["metric_label"]),
         Paragraph("Profitability", styles["metric_label"])],
        [Paragraph(site.get("state", "N/A"), styles["metric_value"]),
         Paragraph(_sanitize(site.get("primary_rule", "N/A")), styles["metric_value"]),
         Paragraph(_confidence_tier(site.get("best_confidence", 0)), styles["metric_value"]),
         Paragraph(f"{site.get('profitability_score', 0):.0f}/100", styles["metric_value"])],
    ]
    info_table = Table(info_data, colWidths=[(PAGE_W - 2 * MARGIN) / 4] * 4)
    info_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), LIGHT_BG),
        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING", (0, 0), (-1, -1), 6),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
        ("BOX", (0, 0), (-1, -1), 0.5, BLUE),
    ]))
    elements.append(info_table)
    elements.append(Spacer(1, 6 * mm))

    # Financial metrics box
    elements.append(Paragraph("KEY FINANCIAL METRICS", styles["subheading"]))

    fin_data = [
        [Paragraph("Est. Annual Revenue", styles["metric_label"]),
         Paragraph("Setup Cost", styles["metric_label"]),
         Paragraph("Payback Period", styles["metric_label"]),
         Paragraph("12-Month Flip Profit", styles["metric_label"])],
        [Paragraph(_fmt_money(financials["total_revenue"]), styles["metric_value"]),
         Paragraph(_fmt_money(financials["setup_cost"]), styles["metric_value"]),
         Paragraph(f"{financials['payback_months']:.0f} months", styles["metric_value"]),
         Paragraph(_fmt_money(financials["flip_profit"]), styles["metric_value"])],
    ]
    fin_table = Table(fin_data, colWidths=[(PAGE_W - 2 * MARGIN) / 4] * 4)
    fin_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), LIGHT_BG),
        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING", (0, 0), (-1, -1), 6),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
        ("BOX", (0, 0), (-1, -1), 0.5, BLUE),
    ]))
    elements.append(fin_table)
    elements.append(Spacer(1, 8 * mm))

    # Coordinates
    lat = site.get("latitude", 0)
    lon = site.get("longitude", 0)
    elements.append(Paragraph(
        f"Coordinates: {lat:.6f}, {lon:.6f} | Generated: {datetime.now().strftime('%d %b %Y %H:%M')}",
        styles["small"],
    ))

    elements.append(Spacer(1, 6 * mm))
    elements.append(Paragraph(
        "CONFIDENTIAL - This document is for internal investment assessment only. "
        "All distances are geodesic estimates. Verify with licensed surveyor before ACPA lodgement.",
        styles["disclaimer"],
    ))

    return elements


def _build_page2(site: dict, styles: dict) -> list:
    """Page 2: Compliance Evidence."""
    elements = []
    elements.append(PageBreak())
    elements.append(Paragraph("COMPLIANCE EVIDENCE", styles["heading"]))
    elements.append(HRFlowable(width="100%", color=BLUE, thickness=1, spaceAfter=4 * mm))

    # Parse rules
    rules_json = site.get("rules_json", "[]")
    all_rules_json = site.get("all_rules_json", "[]")
    passing = json.loads(rules_json) if isinstance(rules_json, str) else rules_json
    all_rules = json.loads(all_rules_json) if isinstance(all_rules_json, str) else all_rules_json

    # Primary qualifying rule
    elements.append(Paragraph("Qualifying Rule", styles["subheading"]))
    primary = site.get("primary_rule", "N/A")
    conf = site.get("best_confidence", 0)
    tier = _confidence_tier(conf)
    tier_color = {"HIGH": GREEN, "MEDIUM": AMBER, "LOW": RED}.get(tier, GREY)

    rule_info = [
        [Paragraph("<b>Primary Rule:</b>", styles["body"]),
         Paragraph(_sanitize(primary), styles["body"])],
        [Paragraph("<b>Confidence:</b>", styles["body"]),
         Paragraph(f"{conf * 100:.0f}% ({tier})", styles["body"])],
    ]
    rule_table = Table(rule_info, colWidths=[120, PAGE_W - 2 * MARGIN - 120])
    rule_table.setStyle(TableStyle([
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("TOPPADDING", (0, 0), (-1, -1), 2),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
    ]))
    elements.append(rule_table)
    elements.append(Spacer(1, 4 * mm))

    # Rules summary table
    elements.append(Paragraph("All Rules Assessment", styles["subheading"]))
    hdr = [Paragraph(h, styles["table_header"]) for h in ["Rule", "Result", "Confidence", "Key Reason"]]
    table_data = [hdr]

    for rule in all_rules:
        item = rule.get("item", "")
        passed = rule.get("passed", False)
        rconf = rule.get("confidence", 0)
        reasons = rule.get("reasons", [])
        reason_text = _sanitize(reasons[0][:60] + "..." if reasons and len(reasons[0]) > 60 else (reasons[0] if reasons else "N/A"))

        table_data.append([
            Paragraph(item, styles["table_cell_center"]),
            Paragraph("PASS" if passed else "FAIL", styles["table_cell_center"]),
            Paragraph(f"{rconf * 100:.0f}%" if passed else "-", styles["table_cell_center"]),
            Paragraph(reason_text, styles["table_cell"]),
        ])

    col_w = [55, 40, 50, PAGE_W - 2 * MARGIN - 145]
    tbl = Table(table_data, colWidths=col_w, repeatRows=1)
    row_styles = [
        ("BACKGROUND", (0, 0), (-1, 0), NAVY),
        ("TEXTCOLOR", (0, 0), (-1, 0), WHITE),
        ("ALIGN", (0, 0), (-1, 0), "CENTER"),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#CCCCCC")),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING", (0, 0), (-1, -1), 3),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
    ]
    # Colour rows by pass/fail
    for i, rule in enumerate(all_rules, 1):
        bg = colors.HexColor("#E8FFE8") if rule.get("passed") else colors.HexColor("#FFE8E8")
        row_styles.append(("BACKGROUND", (0, i), (-1, i), bg))
    tbl.setStyle(TableStyle(row_styles))
    elements.append(tbl)
    elements.append(Spacer(1, 4 * mm))

    # Distance measurements from passing rules
    if passing:
        elements.append(Paragraph("Key Distance Measurements", styles["subheading"]))
        for rule in passing:
            dists = rule.get("distances", {})
            if dists:
                elements.append(Paragraph(f"<b>{rule.get('item', '')}:</b>", styles["body"]))
                for k, v in dists.items():
                    label = k.replace("_", " ").title()
                    if isinstance(v, float):
                        val = f"{v:.0f}m" if "distance" in k or k.endswith("_m") else f"{v:.2f}"
                    else:
                        val = str(v)
                    elements.append(Paragraph(f"  {_sanitize(label)}: {_sanitize(val)}", styles["body"]))
        elements.append(Spacer(1, 3 * mm))

    # Compliance map
    lat = site.get("latitude", 0)
    lon = site.get("longitude", 0)
    if lat and lon and HAS_STATICMAP:
        elements.append(Paragraph("Site Location with Compliance Radius", styles["subheading"]))
        map_bytes = _generate_map(lat, lon, radius_m=1500, zoom=14, width=560, height=300)
        if map_bytes:
            img = Image(io.BytesIO(map_bytes), width=PAGE_W - 2 * MARGIN, height=150 * mm / 2)
            elements.append(img)
            elements.append(Paragraph(
                "Red dot = site location. Blue dots = 1.5km compliance radius. "
                "Map tiles: OpenStreetMap contributors.",
                styles["small"],
            ))

    return elements


def _build_page3(site: dict, financials: dict, styles: dict) -> list:
    """Page 3: Financial Projections."""
    elements = []
    elements.append(PageBreak())
    elements.append(Paragraph("FINANCIAL PROJECTIONS", styles["heading"]))
    elements.append(HRFlowable(width="100%", color=BLUE, thickness=1, spaceAfter=4 * mm))

    # Revenue projections
    elements.append(Paragraph("Revenue Estimates", styles["subheading"]))
    rev_data = [
        ["Metric", "Value"],
        ["Scripts per day", f"{financials['scripts_day']}"],
        ["Scripts per year", f"{financials['scripts_year']:,}"],
        ["PBS dispensing revenue", _fmt_money(financials["pbs_revenue"])],
        ["Front-of-shop revenue", _fmt_money(financials["fos_revenue"])],
        ["Total annual revenue", _fmt_money(financials["total_revenue"])],
        ["Gross profit (33% margin)", _fmt_money(financials["gross_profit"])],
    ]
    rev_table = Table(rev_data, colWidths=[200, 150])
    rev_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), NAVY),
        ("TEXTCOLOR", (0, 0), (-1, 0), WHITE),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 10),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#CCCCCC")),
        ("ALIGN", (1, 1), (1, -1), "RIGHT"),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ("BACKGROUND", (0, -1), (-1, -1), colors.HexColor("#E8F0FE")),
        ("FONTNAME", (0, -1), (-1, -1), "Helvetica-Bold"),
    ]))
    elements.append(rev_table)
    elements.append(Spacer(1, 5 * mm))

    # Setup costs
    elements.append(Paragraph("Setup Cost Breakdown", styles["subheading"]))
    cost_data = [
        ["Component", "Low", "High", "Mid-Point"],
        ["Fitout", _fmt_money(FITOUT_LOW), _fmt_money(FITOUT_HIGH), _fmt_money(financials["fitout"])],
        ["Initial stock", _fmt_money(STOCK_LOW), _fmt_money(STOCK_HIGH), _fmt_money(financials["stock"])],
        ["Working capital", _fmt_money(WORKING_CAP_LOW), _fmt_money(WORKING_CAP_HIGH), _fmt_money(financials["working_cap"])],
        ["TOTAL", _fmt_money(FITOUT_LOW + STOCK_LOW + WORKING_CAP_LOW),
         _fmt_money(FITOUT_HIGH + STOCK_HIGH + WORKING_CAP_HIGH),
         _fmt_money(financials["setup_cost"])],
    ]
    cost_table = Table(cost_data, colWidths=[140, 80, 80, 80])
    cost_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), NAVY),
        ("TEXTCOLOR", (0, 0), (-1, 0), WHITE),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 10),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#CCCCCC")),
        ("ALIGN", (1, 1), (-1, -1), "RIGHT"),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ("BACKGROUND", (0, -1), (-1, -1), colors.HexColor("#E8F0FE")),
        ("FONTNAME", (0, -1), (-1, -1), "Helvetica-Bold"),
    ]))
    elements.append(cost_table)
    elements.append(Spacer(1, 5 * mm))

    # Exit valuation
    elements.append(Paragraph("Investment Returns", styles["subheading"]))
    ret_data = [
        ["12-month exit valuation (revenue x 0.4)", _fmt_money(financials["exit_value"])],
        ["Setup cost (mid-point)", _fmt_money(financials["setup_cost"])],
        ["Flip profit", _fmt_money(financials["flip_profit"])],
        ["ROI", f"{financials['roi_pct']:.0f}%"],
        ["Payback period", f"{financials['payback_months']:.0f} months"],
    ]
    ret_table = Table(ret_data, colWidths=[250, 120])
    ret_table.setStyle(TableStyle([
        ("FONTSIZE", (0, 0), (-1, -1), 10),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#CCCCCC")),
        ("ALIGN", (1, 0), (1, -1), "RIGHT"),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ("FONTNAME", (0, 2), (-1, 2), "Helvetica-Bold"),
        ("BACKGROUND", (0, 2), (-1, 2), colors.HexColor("#E8FFE8")),
    ]))
    elements.append(ret_table)
    elements.append(Spacer(1, 5 * mm))

    # Bar chart: setup cost vs exit value
    elements.append(Paragraph("Setup Cost vs Exit Value", styles["subheading"]))
    drawing = Drawing(350, 180)
    bc = VerticalBarChart()
    bc.x = 60
    bc.y = 30
    bc.height = 130
    bc.width = 250
    bc.data = [[financials["setup_cost"], financials["exit_value"]]]
    bc.categoryAxis.categoryNames = ["Setup Cost", "Exit Value"]
    bc.categoryAxis.labels.fontName = "Helvetica"
    bc.categoryAxis.labels.fontSize = 9
    bc.valueAxis.valueMin = 0
    bc.valueAxis.valueMax = max(financials["setup_cost"], financials["exit_value"]) * 1.2
    bc.valueAxis.labelTextFormat = lambda v: _fmt_money(v)
    bc.valueAxis.labels.fontName = "Helvetica"
    bc.valueAxis.labels.fontSize = 8
    bc.bars[0].fillColor = colors.HexColor("#0066CC")
    bc.bars.strokeColor = None
    bc.barWidth = 40
    drawing.add(bc)
    elements.append(drawing)

    return elements


def _build_page4(site: dict, nearby: dict, styles: dict) -> list:
    """Page 4: Competition Analysis."""
    elements = []
    elements.append(PageBreak())
    elements.append(Paragraph("COMPETITION ANALYSIS", styles["heading"]))
    elements.append(HRFlowable(width="100%", color=BLUE, thickness=1, spaceAfter=4 * mm))

    lat = site.get("latitude", 0)
    lon = site.get("longitude", 0)

    # Pharmacies within 10km
    pharms_10km = [p for p in nearby.get("pharmacies", []) if p["_dist_m"] <= 10_000]
    elements.append(Paragraph(f"Pharmacies Within 10km ({len(pharms_10km)} found)", styles["subheading"]))

    if pharms_10km:
        hdr = [Paragraph(h, styles["table_header"]) for h in ["Name", "Distance", "Address"]]
        p_data = [hdr]
        for p in pharms_10km[:20]:  # Cap at 20
            p_data.append([
                Paragraph(_sanitize(p.get("name", "Unknown")[:40]), styles["table_cell"]),
                Paragraph(_fmt_dist(p["_dist_m"]), styles["table_cell_center"]),
                Paragraph(_sanitize((p.get("address", "") or "")[:50]), styles["table_cell"]),
            ])
        p_tbl = Table(p_data, colWidths=[150, 60, PAGE_W - 2 * MARGIN - 210], repeatRows=1)
        p_tbl.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), NAVY),
            ("TEXTCOLOR", (0, 0), (-1, 0), WHITE),
            ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#CCCCCC")),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("TOPPADDING", (0, 0), (-1, -1), 2),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
            ("ROWBACKGROUNDS", (0, 1), (-1, -1), [WHITE, LIGHT_BG]),
        ]))
        elements.append(p_tbl)
    else:
        elements.append(Paragraph("No pharmacies found within 10km.", styles["body"]))

    elements.append(Spacer(1, 4 * mm))

    # GPs within 2km
    gps_2km = [g for g in nearby.get("gps", []) if g["_dist_m"] <= 2_000]
    elements.append(Paragraph(f"GP Practices Within 2km ({len(gps_2km)} found)", styles["subheading"]))
    if gps_2km:
        for g in gps_2km[:10]:
            elements.append(Paragraph(
                f"  {_sanitize(g.get('name', 'Unknown'))} - {_fmt_dist(g['_dist_m'])}",
                styles["body"],
            ))
    else:
        elements.append(Paragraph("No GP practices found within 2km.", styles["body"]))

    elements.append(Spacer(1, 3 * mm))

    # Medical centres within 2km
    mcs_2km = [m for m in nearby.get("medical_centres", []) if m["_dist_m"] <= 2_000]
    elements.append(Paragraph(f"Medical Centres Within 2km ({len(mcs_2km)} found)", styles["subheading"]))
    if mcs_2km:
        for m in mcs_2km[:10]:
            gps_count = m.get("num_gps", "?")
            elements.append(Paragraph(
                f"  {_sanitize(m.get('name', 'Unknown'))} - {_fmt_dist(m['_dist_m'])} ({gps_count} GPs)",
                styles["body"],
            ))
    else:
        elements.append(Paragraph("No medical centres found within 2km.", styles["body"]))

    elements.append(Spacer(1, 3 * mm))

    # Hospitals within 5km
    hosps_5km = [h for h in nearby.get("hospitals", []) if h["_dist_m"] <= 5_000]
    elements.append(Paragraph(f"Hospitals Within 5km ({len(hosps_5km)} found)", styles["subheading"]))
    if hosps_5km:
        for h in hosps_5km[:10]:
            beds = h.get("bed_count", "?")
            elements.append(Paragraph(
                f"  {_sanitize(h.get('name', 'Unknown'))} - {_fmt_dist(h['_dist_m'])} ({beds} beds)",
                styles["body"],
            ))
    else:
        elements.append(Paragraph("No hospitals found within 5km.", styles["body"]))

    elements.append(Spacer(1, 3 * mm))

    # People per pharmacy ratio
    pharms_5km = [p for p in nearby.get("pharmacies", []) if p["_dist_m"] <= 5_000]
    if pharms_5km:
        elements.append(Paragraph(
            f"Pharmacies within 5km: {len(pharms_5km)} "
            f"(nearest: {_sanitize(pharms_5km[0].get('name', ''))} at {_fmt_dist(pharms_5km[0]['_dist_m'])})",
            styles["body"],
        ))

    elements.append(Spacer(1, 4 * mm))

    # Competition map
    if lat and lon and HAS_STATICMAP:
        elements.append(Paragraph("Competition Map", styles["subheading"]))
        markers = []
        for p in pharms_10km[:30]:
            markers.append((p["latitude"], p["longitude"], "pharmacy"))
        for g in gps_2km[:10]:
            markers.append((g["latitude"], g["longitude"], "gp"))

        map_bytes = _generate_map(lat, lon, radius_m=0, markers=markers, zoom=13,
                                  width=560, height=300)
        if map_bytes:
            img = Image(io.BytesIO(map_bytes), width=PAGE_W - 2 * MARGIN, height=140 * mm / 2)
            elements.append(img)
            elements.append(Paragraph(
                "Red = site | Green = pharmacies | Orange = GPs",
                styles["small"],
            ))

    return elements


def _build_page5(site: dict, commercial: list, styles: dict) -> list:
    """Page 5: Commercial Property Options."""
    elements = []
    elements.append(PageBreak())
    elements.append(Paragraph("COMMERCIAL PROPERTY OPTIONS", styles["heading"]))
    elements.append(HRFlowable(width="100%", color=BLUE, thickness=1, spaceAfter=4 * mm))

    if not commercial:
        elements.append(Spacer(1, 20 * mm))
        elements.append(Paragraph(
            "No commercial listings currently available for this site.",
            ParagraphStyle("NoData", parent=styles["body"], fontSize=14,
                           alignment=TA_CENTER, textColor=GREY),
        ))
        elements.append(Spacer(1, 10 * mm))
        elements.append(Paragraph(
            "Commercial property data is sourced from realcommercial.com.au and updated periodically. "
            "Check back later or search manually for available leases near this location.",
            styles["body"],
        ))
        return elements

    elements.append(Paragraph(
        f"{len(commercial)} listing(s) found", styles["body"],
    ))
    elements.append(Spacer(1, 3 * mm))

    for i, prop in enumerate(commercial, 1):
        elements.append(Paragraph(f"Listing {i}", styles["subheading"]))

        prop_info = [
            ["Address", _sanitize(prop.get("address", "N/A"))],
            ["Annual Rent", _fmt_money(prop.get("rent_annual", 0)) if prop.get("rent_annual") else "N/A"],
            ["Floor Area", f"{prop.get('floor_area_sqm', 'N/A')} sqm" if prop.get("floor_area_sqm") else "N/A"],
            ["Lease Type", prop.get("lease_type", "N/A") or "N/A"],
            ["Property Type", prop.get("property_type", "N/A") or "N/A"],
            ["Agent", prop.get("agent_name", "N/A") or "N/A"],
            ["Distance to Site", _fmt_dist(prop.get("distance_to_site_m", 0)) if prop.get("distance_to_site_m") else "N/A"],
        ]

        url = prop.get("listing_url", "")
        if url:
            prop_info.append(["Listing URL", url[:70]])

        p_tbl = Table(prop_info, colWidths=[110, PAGE_W - 2 * MARGIN - 110])
        p_tbl.setStyle(TableStyle([
            ("FONTNAME", (0, 0), (0, -1), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, -1), 9),
            ("TOPPADDING", (0, 0), (-1, -1), 2),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
            ("LINEBELOW", (0, -1), (-1, -1), 0.5, colors.HexColor("#CCCCCC")),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ]))
        elements.append(p_tbl)
        elements.append(Spacer(1, 4 * mm))

    return elements


# ================================================================
# Header / Footer
# ================================================================
def _header_footer(canvas_obj, doc):
    """Draw header and footer on each page."""
    canvas_obj.saveState()
    # Header
    canvas_obj.setFont("Helvetica-Bold", 8)
    canvas_obj.setFillColor(GREY)
    canvas_obj.drawString(MARGIN, PAGE_H - 12 * mm, "PharmacyFinder - Deal Package")
    canvas_obj.drawRightString(PAGE_W - MARGIN, PAGE_H - 12 * mm,
                               datetime.now().strftime("%d %B %Y"))
    canvas_obj.setStrokeColor(BLUE)
    canvas_obj.setLineWidth(0.5)
    canvas_obj.line(MARGIN, PAGE_H - 14 * mm, PAGE_W - MARGIN, PAGE_H - 14 * mm)

    # Footer
    canvas_obj.setFont("Helvetica-Oblique", 7)
    canvas_obj.setFillColor(GREY)
    canvas_obj.drawString(MARGIN, 10 * mm,
                          "CONFIDENTIAL - For internal assessment only")
    canvas_obj.drawRightString(PAGE_W - MARGIN, 10 * mm,
                               f"Page {doc.page}")
    canvas_obj.restoreState()


# ================================================================
# Public API
# ================================================================
def generate_deal_package(
    site_id: str,
    db_path: str,
    output_dir: str = "output/deal_packages",
) -> str:
    """
    Generate a multi-page deal package PDF for a site.

    Parameters
    ----------
    site_id : str
        The v2_results.id of the site.
    db_path : str
        Path to pharmacy_finder.db.
    output_dir : str
        Directory to save PDFs (created if needed).

    Returns
    -------
    str
        Path to the generated PDF file.
    """
    os.makedirs(output_dir, exist_ok=True)

    # Load data
    site = _load_site(db_path, site_id)
    lat = site.get("latitude", 0)
    lon = site.get("longitude", 0)

    # Load nearby facilities
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    nearby = {}

    # Pharmacies
    delta = 0.15
    rows = conn.execute(
        "SELECT * FROM pharmacies WHERE latitude BETWEEN ? AND ? AND longitude BETWEEN ? AND ?",
        (lat - delta, lat + delta, lon - delta, lon + delta),
    ).fetchall()
    pharms = []
    for r in rows:
        d = _haversine(lat, lon, r["latitude"], r["longitude"])
        if d <= 15_000:
            pharms.append({**dict(r), "_dist_m": d})
    pharms.sort(key=lambda x: x["_dist_m"])
    nearby["pharmacies"] = pharms

    # GPs
    delta_gp = 0.05
    rows = conn.execute(
        "SELECT * FROM gps WHERE latitude BETWEEN ? AND ? AND longitude BETWEEN ? AND ?",
        (lat - delta_gp, lat + delta_gp, lon - delta_gp, lon + delta_gp),
    ).fetchall()
    gps = []
    for r in rows:
        d = _haversine(lat, lon, r["latitude"], r["longitude"])
        if d <= 5_000:
            gps.append({**dict(r), "_dist_m": d})
    gps.sort(key=lambda x: x["_dist_m"])
    nearby["gps"] = gps

    # Medical centres
    rows = conn.execute(
        "SELECT * FROM medical_centres WHERE latitude BETWEEN ? AND ? AND longitude BETWEEN ? AND ?",
        (lat - delta_gp, lat + delta_gp, lon - delta_gp, lon + delta_gp),
    ).fetchall()
    mcs = []
    for r in rows:
        d = _haversine(lat, lon, r["latitude"], r["longitude"])
        if d <= 5_000:
            mcs.append({**dict(r), "_dist_m": d})
    mcs.sort(key=lambda x: x["_dist_m"])
    nearby["medical_centres"] = mcs

    # Hospitals
    delta_h = 0.1
    rows = conn.execute(
        "SELECT * FROM hospitals WHERE latitude BETWEEN ? AND ? AND longitude BETWEEN ? AND ?",
        (lat - delta_h, lat + delta_h, lon - delta_h, lon + delta_h),
    ).fetchall()
    hosps = []
    for r in rows:
        d = _haversine(lat, lon, r["latitude"], r["longitude"])
        if d <= 10_000:
            hosps.append({**dict(r), "_dist_m": d})
    hosps.sort(key=lambda x: x["_dist_m"])
    nearby["hospitals"] = hosps

    conn.close()

    # Commercial matches
    commercial = _load_commercial(db_path, site_id)

    # Financial estimates
    financials = _estimate_financials(site, nearby)

    # Build PDF
    raw_name = site.get("name", site_id)
    # Remove characters illegal in Windows filenames
    safe_name = raw_name.replace(" ", "_")
    for ch in ':/\\*?"<>|':
        safe_name = safe_name.replace(ch, "_")
    # Strip non-ASCII
    safe_name = safe_name.encode("ascii", errors="ignore").decode("ascii")[:40]
    filename = f"deal_package_{safe_name}_{site_id}.pdf"
    filepath = os.path.join(output_dir, filename)

    styles = _build_styles()

    doc = SimpleDocTemplate(
        filepath,
        pagesize=A4,
        topMargin=18 * mm,
        bottomMargin=18 * mm,
        leftMargin=MARGIN,
        rightMargin=MARGIN,
    )

    elements = []
    elements.extend(_build_page1(site, financials, styles))
    elements.extend(_build_page2(site, styles))
    elements.extend(_build_page3(site, financials, styles))
    elements.extend(_build_page4(site, nearby, styles))
    elements.extend(_build_page5(site, commercial, styles))

    doc.build(elements, onFirstPage=_header_footer, onLaterPages=_header_footer)

    return filepath


def get_top_sites(db_path: str, limit: int = 10) -> list:
    """Get top N sites by profitability score."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT id FROM v2_results WHERE passed_any = 1 "
        "ORDER BY profitability_score DESC LIMIT ?",
        (limit,),
    ).fetchall()
    conn.close()
    return [r["id"] for r in rows]
