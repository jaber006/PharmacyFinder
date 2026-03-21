"""
PDF report generator for pharmacy site evaluation evidence packages.

Generates professional-quality PDF reports combining:
  - Site overview
  - Qualifying rules and confidence levels
  - Distance measurements and maps
  - Evidence checklist
  - Risk assessment
  - Ministerial case notes (if borderline)

Uses fpdf2 for PDF generation.
"""
from __future__ import annotations

import json
import os
import textwrap
from datetime import datetime
from io import BytesIO
from typing import Any, Dict, List, Optional

from fpdf import FPDF


# ================================================================== #
#  Helpers
# ================================================================== #

def _sanitize(text: str) -> str:
    """Replace Unicode characters with ASCII equivalents for core fonts."""
    replacements = {
        "\u2014": "-",   # em dash
        "\u2013": "-",   # en dash
        "\u2018": "'",   # left single quote
        "\u2019": "'",   # right single quote
        "\u201c": '"',   # left double quote
        "\u201d": '"',   # right double quote
        "\u2022": "*",   # bullet
        "\u2026": "...", # ellipsis
        "\u2264": "<=",  # less than or equal
        "\u2265": ">=",  # greater than or equal
        "\u2717": "X",   # ballot X
        "\u2713": "v",   # check mark
        "\u2714": "v",   # heavy check mark
        "\u25cf": "*",   # black circle
        "\u2716": "X",   # heavy X
        "\u00b2": "2",   # superscript 2
        "\u00b0": "deg", # degree
    }
    for char, repl in replacements.items():
        text = text.replace(char, repl)
    # Catch any remaining non-latin1 characters
    try:
        text.encode("latin-1")
    except UnicodeEncodeError:
        text = text.encode("latin-1", errors="replace").decode("latin-1")
    return text


# ================================================================== #
#  Custom PDF class with header/footer
# ================================================================== #

class SiteReportPDF(FPDF):
    """Custom FPDF with professional header and footer."""

    def __init__(self, candidate_name: str = "", **kwargs):
        super().__init__(**kwargs)
        self.candidate_name = candidate_name

    def header(self):
        self.set_font("Helvetica", "B", 9)
        self.set_text_color(100, 100, 100)
        self.cell(0, 5, "PharmacyFinder - Site Evaluation Report", align="L")
        self.cell(0, 5, datetime.now().strftime("%d %B %Y"), align="R", new_x="LMARGIN", new_y="NEXT")
        self.set_draw_color(0, 102, 204)
        self.set_line_width(0.5)
        self.line(10, self.get_y(), self.w - 10, self.get_y())
        self.ln(3)

    def footer(self):
        self.set_y(-15)
        self.set_font("Helvetica", "I", 8)
        self.set_text_color(128, 128, 128)
        self.cell(0, 10, "CONFIDENTIAL - For internal assessment only", align="L")
        self.cell(0, 10, f"Page {self.page_no()}/{{nb}}", align="R")

    def section_title(self, title: str):
        self.ln(3)
        self.set_font("Helvetica", "B", 14)
        self.set_text_color(0, 51, 102)
        self.cell(0, 10, _sanitize(title), new_x="LMARGIN", new_y="NEXT")
        self.set_draw_color(0, 102, 204)
        self.set_line_width(0.3)
        self.line(10, self.get_y(), self.w - 10, self.get_y())
        self.ln(3)

    def subsection_title(self, title: str):
        self.ln(2)
        self.set_font("Helvetica", "B", 11)
        self.set_text_color(51, 51, 51)
        self.cell(0, 7, _sanitize(title), new_x="LMARGIN", new_y="NEXT")
        self.ln(1)

    def body_text(self, text: str):
        self.set_font("Helvetica", "", 10)
        self.set_text_color(0, 0, 0)
        self.multi_cell(0, 5, _sanitize(text))
        self.ln(1)

    def key_value(self, key: str, value: str, indent: int = 0):
        x = 10 + indent
        self.set_x(x)
        self.set_font("Helvetica", "B", 10)
        self.set_text_color(51, 51, 51)
        key_w = min(self.get_string_width(key + ": ") + 2, 70)
        self.cell(key_w, 5, _sanitize(key + ":"))
        self.set_font("Helvetica", "", 10)
        self.set_text_color(0, 0, 0)
        remaining_w = max(self.w - self.get_x() - 10, 30)
        self.multi_cell(remaining_w, 5, _sanitize(" " + str(value)))

    def status_badge(self, status: str):
        """Draw a coloured status badge."""
        colors = {
            "PASS": (34, 139, 34),
            "FAIL": (220, 20, 60),
            "PENDING": (255, 165, 0),
            "HIGH": (220, 20, 60),
            "MEDIUM": (255, 165, 0),
            "LOW": (34, 139, 34),
            "NOT_ASSESSED": (128, 128, 128),
        }
        r, g, b = colors.get(status, (128, 128, 128))
        self.set_fill_color(r, g, b)
        self.set_text_color(255, 255, 255)
        self.set_font("Helvetica", "B", 8)
        w = self.get_string_width(status) + 6
        self.cell(w, 5, status, fill=True, align="C")
        self.set_text_color(0, 0, 0)

    def bullet_list(self, items: List[str], indent: int = 5):
        self.set_font("Helvetica", "", 9)
        self.set_text_color(0, 0, 0)
        for item in items:
            text = _sanitize(item.strip())
            if text.startswith("- "):
                text = text[2:]
            self.set_x(10 + indent)
            self.multi_cell(self.w - 20 - indent, 5, "- " + text)


# ================================================================== #
#  Report sections
# ================================================================== #

def _add_cover_page(pdf: SiteReportPDF, candidate: dict, eval_result: dict):
    """Add cover page with site overview."""
    pdf.add_page()
    pdf.alias_nb_pages()

    # Title block
    pdf.ln(20)
    pdf.set_font("Helvetica", "B", 28)
    pdf.set_text_color(0, 51, 102)
    pdf.cell(0, 15, "Site Evaluation Report", align="C", new_x="LMARGIN", new_y="NEXT")

    pdf.set_font("Helvetica", "", 16)
    pdf.set_text_color(51, 51, 51)
    name = candidate.get("name", eval_result.get("name", "Unknown Site"))
    pdf.cell(0, 10, _sanitize(name), align="C", new_x="LMARGIN", new_y="NEXT")

    pdf.ln(5)
    pdf.set_font("Helvetica", "", 12)
    address = candidate.get("address", eval_result.get("address", ""))
    pdf.cell(0, 8, _sanitize(address), align="C", new_x="LMARGIN", new_y="NEXT")

    # Horizontal rule
    pdf.ln(10)
    pdf.set_draw_color(0, 102, 204)
    pdf.set_line_width(1)
    pdf.line(50, pdf.get_y(), pdf.w - 50, pdf.get_y())
    pdf.ln(10)

    # Key metrics box
    pdf.set_fill_color(240, 245, 255)
    pdf.set_draw_color(0, 102, 204)
    box_y = pdf.get_y()
    pdf.rect(30, box_y, pdf.w - 60, 55, style="DF")

    pdf.set_xy(35, box_y + 5)
    pdf.set_font("Helvetica", "B", 12)
    pdf.set_text_color(0, 51, 102)
    pdf.cell(0, 8, "Key Metrics")

    pdf.set_xy(35, box_y + 15)
    pdf.key_value("Primary Rule", eval_result.get("primary_rule", "N/A"))
    pdf.set_x(35)
    pdf.key_value("Confidence", f"{eval_result.get('best_confidence', 0) * 100:.0f}%")
    pdf.set_x(35)
    pdf.key_value("Commercial Score", f"{eval_result.get('commercial_score', 0):.4f}")
    pdf.set_x(35)
    pdf.key_value("State", eval_result.get("state", "N/A"))
    pdf.set_x(35)
    pdf.key_value("Assessment Date", eval_result.get("date_evaluated", datetime.now().isoformat())[:10])

    pdf.set_y(box_y + 60)

    # Coordinates
    pdf.ln(5)
    pdf.set_font("Helvetica", "", 10)
    pdf.set_text_color(100, 100, 100)
    lat = eval_result.get("latitude", 0)
    lon = eval_result.get("longitude", 0)
    pdf.cell(0, 6, f"Coordinates: {lat:.6f}, {lon:.6f}", align="C", new_x="LMARGIN", new_y="NEXT")

    # Disclaimer
    pdf.ln(15)
    pdf.set_font("Helvetica", "I", 9)
    pdf.set_text_color(128, 128, 128)
    pdf.multi_cell(0, 4, _sanitize(
        "This report is generated by the PharmacyFinder automated analysis system. "
        "It is intended for internal screening and due diligence purposes only. "
        "Distances are geodesic (straight-line) estimates and must be verified by a licensed surveyor "
        "before lodging an application with the ACPA. GP FTE counts, operating hours, and other "
        "operational data should be independently verified. This report does not constitute legal advice."
    ), align="C")


def _add_qualifying_rules(pdf: SiteReportPDF, eval_result: dict):
    """Add qualifying rules section."""
    pdf.add_page()
    pdf.section_title("Qualifying Rules Analysis")

    rules_json = eval_result.get("rules_json", "[]")
    all_rules_json = eval_result.get("all_rules_json", "[]")

    if isinstance(rules_json, str):
        passing_rules = json.loads(rules_json)
    else:
        passing_rules = rules_json

    if isinstance(all_rules_json, str):
        all_rules = json.loads(all_rules_json)
    else:
        all_rules = all_rules_json

    # Summary table
    pdf.subsection_title("Rules Summary")
    pdf.set_font("Helvetica", "B", 9)

    # Table header
    col_widths = [30, 18, 25, 100]
    headers = ["Rule Item", "Result", "Confidence", "Key Reason"]
    pdf.set_fill_color(0, 51, 102)
    pdf.set_text_color(255, 255, 255)
    for i, h in enumerate(headers):
        pdf.cell(col_widths[i], 7, h, border=1, fill=True, align="C")
    pdf.ln()

    # Table rows
    pdf.set_font("Helvetica", "", 9)
    for rule in all_rules:
        item = rule.get("item", "")
        passed = rule.get("passed", False)
        confidence = rule.get("confidence", 0)
        reasons = rule.get("reasons", [])
        first_reason = _sanitize(reasons[0][:55] + "..." if reasons and len(reasons[0]) > 55 else (reasons[0] if reasons else ""))

        if passed:
            pdf.set_fill_color(220, 255, 220)
        else:
            pdf.set_fill_color(255, 220, 220)

        pdf.set_text_color(0, 0, 0)
        pdf.cell(col_widths[0], 6, item, border=1, fill=True, align="C")
        pdf.cell(col_widths[1], 6, "PASS" if passed else "FAIL", border=1, fill=True, align="C")
        pdf.cell(col_widths[2], 6, f"{confidence * 100:.0f}%" if passed else "-", border=1, fill=True, align="C")
        pdf.cell(col_widths[3], 6, first_reason, border=1, fill=True)
        pdf.ln()

    # Detailed passing rules
    for rule in passing_rules:
        item = rule.get("item", "")
        pdf.subsection_title(f"Detail: {item}")

        # Reasons
        pdf.set_font("Helvetica", "B", 10)
        pdf.cell(0, 6, "Assessment Reasons:", new_x="LMARGIN", new_y="NEXT")
        pdf.bullet_list(rule.get("reasons", []))

        # Distances
        distances = rule.get("distances", {})
        if distances:
            pdf.ln(2)
            pdf.set_font("Helvetica", "B", 10)
            pdf.cell(0, 6, "Key Measurements:", new_x="LMARGIN", new_y="NEXT")
            pdf.set_font("Helvetica", "", 9)
            for k, v in distances.items():
                label = k.replace("_", " ").title()
                if isinstance(v, float):
                    val = f"{v:.3f}" if v < 100 else f"{v:.0f}"
                else:
                    val = str(v)
                pdf.key_value(label, val, indent=5)

        # Evidence needed
        evidence = rule.get("evidence_needed", [])
        if evidence:
            pdf.ln(2)
            pdf.set_font("Helvetica", "B", 10)
            pdf.set_text_color(204, 102, 0)
            pdf.cell(0, 6, "Evidence Still Required:", new_x="LMARGIN", new_y="NEXT")
            pdf.set_text_color(0, 0, 0)
            pdf.bullet_list(evidence)


def _add_checklist(pdf: SiteReportPDF, checklist: dict):
    """Add evidence checklist section."""
    pdf.add_page()
    pdf.section_title("Evidence Checklist")

    summary = checklist.get("summary", {})
    pdf.body_text(
        f"Total requirements: {summary.get('total_requirements', 0)} | "
        f"Passed: {summary.get('passed', 0)} | "
        f"Failed: {summary.get('failed', 0)} | "
        f"Pending: {summary.get('pending', 0)} | "
        f"Readiness: {summary.get('readiness_pct', 0):.0f}%"
    )

    # General requirements
    pdf.subsection_title("General Requirements (All Items)")
    for req in checklist.get("general_requirements", []):
        pdf.set_font("Helvetica", "B", 9)
        pdf.cell(130, 5, _sanitize(f"{req['id']}: {req['requirement'][:70]}"))
        pdf.status_badge(req["status"])
        pdf.ln(6)

        evidence = req.get("evidence", {})
        manual = evidence.get("requires_manual_verification", [])
        if manual:
            pdf.set_font("Helvetica", "", 8)
            pdf.set_text_color(128, 0, 0)
            for item in manual[:3]:
                text = _sanitize(item if len(item) <= 90 else item[:87] + "...")
                pdf.set_x(15)
                pdf.cell(0, 4, f"  * {text}", new_x="LMARGIN", new_y="NEXT")
            pdf.set_text_color(0, 0, 0)

    # Item-specific requirements
    for item, data in checklist.get("qualifying_rules", {}).items():
        pdf.subsection_title(f"{item} Requirements (Confidence: {data.get('confidence', 0) * 100:.0f}%)")

        for req in data.get("requirements", []):
            # Check for page break
            if pdf.get_y() > 260:
                pdf.add_page()

            pdf.set_font("Helvetica", "B", 9)
            req_text = req["requirement"]
            if len(req_text) > 75:
                req_text = req_text[:72] + "..."
            pdf.cell(130, 5, _sanitize(f"{req['id']}: {req_text}"))
            pdf.status_badge(req["status"])
            pdf.ln(6)

            evidence = req.get("evidence", {})
            available = evidence.get("available_from_data", [])
            manual = evidence.get("requires_manual_verification", [])

            if available:
                pdf.set_font("Helvetica", "", 8)
                pdf.set_text_color(0, 100, 0)
                for item_text in available[:2]:
                    text = _sanitize(item_text if len(item_text) <= 90 else item_text[:87] + "...")
                    pdf.set_x(15)
                    pdf.cell(0, 4, f"  [OK] {text}", new_x="LMARGIN", new_y="NEXT")

            if manual:
                pdf.set_font("Helvetica", "", 8)
                pdf.set_text_color(128, 0, 0)
                for item_text in manual[:3]:
                    text = _sanitize(item_text if len(item_text) <= 90 else item_text[:87] + "...")
                    pdf.set_x(15)
                    pdf.cell(0, 4, f"  [!] MANUAL: {text}", new_x="LMARGIN", new_y="NEXT")

            pdf.set_text_color(0, 0, 0)


def _add_risk_assessment(pdf: SiteReportPDF, risk_report: dict):
    """Add risk assessment section."""
    pdf.add_page()
    pdf.section_title("Risk Assessment")

    # Overall risk badge
    overall = risk_report.get("overall_risk", "MEDIUM")
    pdf.set_font("Helvetica", "B", 12)
    pdf.cell(50, 8, "Overall Risk Rating: ")
    pdf.status_badge(overall)
    pdf.ln(10)

    pdf.body_text(risk_report.get("recommendation", ""))

    # "All relevant times" window
    window = risk_report.get("all_relevant_times_window", {})
    pdf.subsection_title('"All Relevant Times" Window')
    pdf.body_text(window.get("description", ""))
    pdf.body_text(f"Typical gap between application and hearing: {window.get('typical_gap_months', '2-6 months')}")
    pdf.body_text(f"ACPA meetings per year: {window.get('acpa_meetings_per_year', 9)}")

    what_changes = window.get("what_changes", [])
    if what_changes:
        pdf.set_font("Helvetica", "B", 10)
        pdf.cell(0, 6, "What could change during the window:", new_x="LMARGIN", new_y="NEXT")
        pdf.bullet_list(what_changes)

    # Risk factors by rule
    for item, risks in risk_report.get("risk_factors_by_rule", {}).items():
        pdf.subsection_title(f"Risk Factors: {item}")

        for risk in risks:
            if pdf.get_y() > 250:
                pdf.add_page()

            likelihood = risk.get("likelihood", "LOW")
            pdf.set_font("Helvetica", "B", 9)
            pdf.cell(100, 5, _sanitize(risk.get("category", "").replace("_", " ").title()))
            pdf.status_badge(likelihood)
            pdf.ln(6)

            pdf.set_font("Helvetica", "", 9)
            pdf.set_x(15)
            desc = _sanitize(risk.get("description", ""))
            pdf.multi_cell(pdf.w - 25, 4, desc)

            if risk.get("current_measurement"):
                pdf.set_x(15)
                pdf.set_font("Helvetica", "I", 9)
                pdf.cell(0, 4, _sanitize(f"Current: {risk['current_measurement']}"), new_x="LMARGIN", new_y="NEXT")

            pdf.set_x(15)
            pdf.set_font("Helvetica", "", 8)
            pdf.set_text_color(0, 100, 0)
            pdf.multi_cell(pdf.w - 25, 4, _sanitize(f"Monitoring: {risk.get('monitoring', '')}"))
            pdf.set_text_color(0, 0, 0)
            pdf.ln(2)

    # Mitigation plan
    mitigation = risk_report.get("mitigation_plan", [])
    if mitigation:
        pdf.subsection_title("Mitigation Plan")
        for m in mitigation:
            if pdf.get_y() > 260:
                pdf.add_page()
            pdf.set_font("Helvetica", "B", 9)
            pdf.cell(0, 5, _sanitize(f"{m['rule']} - {m['risk'].replace('_', ' ').title()}"), new_x="LMARGIN", new_y="NEXT")
            pdf.set_font("Helvetica", "", 9)
            pdf.set_x(15)
            pdf.cell(0, 4, _sanitize(f"Action: {m.get('action', '')}"), new_x="LMARGIN", new_y="NEXT")
            pdf.set_x(15)
            pdf.cell(0, 4, f"Frequency: {m.get('frequency', '')}", new_x="LMARGIN", new_y="NEXT")
            pdf.ln(2)


def _add_map_page(pdf: SiteReportPDF, map_path: str, rule_item: str):
    """Add a page with a screenshot reference to the map."""
    pdf.add_page()
    pdf.section_title(f"Distance Map - {rule_item}")

    if os.path.exists(map_path):
        # Check for PNG screenshot of the map
        png_path = map_path.replace(".html", ".png")
        if os.path.exists(png_path):
            try:
                pdf.image(png_path, x=15, y=pdf.get_y(), w=pdf.w - 30)
                pdf.ln(5)
            except Exception:
                pdf.body_text(f"Map image: {png_path}")
        else:
            pdf.body_text(
                f"Interactive map generated: {os.path.basename(map_path)}\n\n"
                f"Open the HTML file in a browser to view the interactive distance map.\n"
                f"To include in a printed report, take a screenshot of the map."
            )
    else:
        pdf.body_text("Map not generated for this rule item.")

    pdf.body_text(
        f"Full path: {map_path}\n"
        f"Note: For ACPA submission, distances must be verified by a licensed surveyor."
    )


def _add_ministerial_case(pdf: SiteReportPDF, eval_result: dict):
    """Add ministerial discretion case notes for borderline sites."""
    rules_json = eval_result.get("all_rules_json", "[]")
    if isinstance(rules_json, str):
        all_rules = json.loads(rules_json)
    else:
        all_rules = rules_json

    # Check if any rules were close to passing
    borderline_rules = []
    for rule in all_rules:
        if not rule.get("passed") and rule.get("confidence", 0) > 0:
            borderline_rules.append(rule)

    if not borderline_rules:
        return

    pdf.add_page()
    pdf.section_title("Ministerial Discretion Case Notes")

    pdf.body_text(
        "Under subsection 90A(2) of the National Health Act 1953, the Minister has "
        "discretionary power to approve a pharmacist to supply pharmaceutical benefits "
        "where the ACPA has not recommended approval. This power is only available AFTER "
        "an application has been considered and rejected by the ACPA delegate."
    )

    pdf.ln(3)
    pdf.body_text(
        "The following rules were close to passing and may warrant a ministerial case "
        "if the standard ACPA application is not recommended:"
    )

    for rule in borderline_rules:
        item = rule.get("item", "")
        reasons = rule.get("reasons", [])

        pdf.subsection_title(f"Near-miss: {item}")
        pdf.bullet_list(reasons)

        # Suggest arguments for ministerial case
        pdf.set_font("Helvetica", "I", 9)
        pdf.set_text_color(0, 51, 102)
        pdf.set_x(15)
        pdf.multi_cell(pdf.w - 25, 4,
            "Potential ministerial arguments: Community need, access equity, "
            "growth area demographics, underserviced population."
        )
        pdf.set_text_color(0, 0, 0)
        pdf.ln(3)


# ================================================================== #
#  Public API
# ================================================================== #

def generate_site_report(
    candidate: dict,
    evaluation_result: dict,
    context: Optional[dict] = None,
) -> bytes:
    """
    Generate a comprehensive PDF site evaluation report.

    Parameters
    ----------
    candidate : dict
        Candidate site data (from Candidate.to_dict() or DB row).
    evaluation_result : dict
        Full evaluation result from v2_results table.
    context : dict or None
        Optional context containing:
        - checklist: dict from evidence.checklist.generate_checklist()
        - risk_report: dict from evidence.risk_report.generate_risk_report()
        - map_paths: dict of {rule_item: html_path}
        - risk_assessment: dict from engine.risk_assessment

    Returns
    -------
    bytes
        PDF file contents.
    """
    context = context or {}
    name = candidate.get("name", evaluation_result.get("name", "Site"))

    pdf = SiteReportPDF(candidate_name=name, orientation="P", unit="mm", format="A4")
    pdf.set_auto_page_break(auto=True, margin=20)

    # Cover page
    _add_cover_page(pdf, candidate, evaluation_result)

    # Qualifying rules
    _add_qualifying_rules(pdf, evaluation_result)

    # Evidence checklist
    checklist = context.get("checklist")
    if checklist:
        _add_checklist(pdf, checklist)

    # Distance maps
    map_paths = context.get("map_paths", {})
    for rule_item, map_path in map_paths.items():
        _add_map_page(pdf, map_path, rule_item)

    # Risk assessment
    risk_report = context.get("risk_report")
    if risk_report:
        _add_risk_assessment(pdf, risk_report)

    # Ministerial case (if any borderline rules)
    _add_ministerial_case(pdf, evaluation_result)

    return pdf.output()
