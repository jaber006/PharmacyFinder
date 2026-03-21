#!/usr/bin/env python3
"""
Quick Site Evaluator — PharmacyFinder
Type any address in Australia, get an instant deal assessment in 60 seconds.

Usage:
    py -3.12 scripts/evaluate_site.py "Beveridge VIC"
    py -3.12 scripts/evaluate_site.py "Eynesbury VIC"
    py -3.12 scripts/evaluate_site.py --lat -37.783 --lon 144.537
    py -3.12 scripts/evaluate_site.py "Beveridge VIC" --json
"""

import argparse
import io
import json
import math
import os
import sqlite3
import sys
import time
from datetime import datetime
from pathlib import Path

# Fix Windows console encoding
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

# ── Paths ──────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "pharmacy_finder.db"
OUTPUT_DIR = PROJECT_ROOT / "output" / "site_evaluations"

# ── Geodesic distance (Haversine) ──────────────────────────────────
def haversine_km(lat1, lon1, lat2, lon2):
    """Return distance in km between two lat/lon points."""
    R = 6371.0
    rlat1, rlon1, rlat2, rlon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    dlat = rlat2 - rlat1
    dlon = rlon2 - rlon1
    a = math.sin(dlat / 2) ** 2 + math.cos(rlat1) * math.cos(rlat2) * math.sin(dlon / 2) ** 2
    return R * 2 * math.asin(math.sqrt(a))


# ── Geocoding ──────────────────────────────────────────────────────
def geocode_address(address: str) -> tuple:
    """Geocode an address string. Returns (lat, lon, display_name)."""
    from geopy.geocoders import Nominatim
    from geopy.exc import GeocoderTimedOut

    geo = Nominatim(user_agent="PharmacyFinder", timeout=10)
    # Try with Australia suffix first
    queries = [
        f"{address}, Australia",
        address,
    ]
    for q in queries:
        try:
            loc = geo.geocode(q)
            if loc:
                return loc.latitude, loc.longitude, loc.address
        except GeocoderTimedOut:
            time.sleep(1)
            try:
                loc = geo.geocode(q)
                if loc:
                    return loc.latitude, loc.longitude, loc.address
            except Exception:
                pass
    return None, None, None


# ── DB Queries ─────────────────────────────────────────────────────
def query_nearby(conn, table, lat, lon, radius_km, extra_cols=None):
    """
    Query a table for rows within a bounding box, then filter by haversine.
    Returns list of dicts with 'distance_km' added.
    """
    # Bounding box filter (rough, ~1 degree ≈ 111km)
    deg_offset = radius_km / 111.0 * 1.2  # 20% buffer
    lat_min, lat_max = lat - deg_offset, lat + deg_offset
    lon_min, lon_max = lon - deg_offset, lon + deg_offset

    # Determine lat/lon column names
    c = conn.cursor()
    c.execute(f"PRAGMA table_info([{table}])")
    cols_info = {row[1]: row[1] for row in c.fetchall()}
    lat_col = "lat" if "lat" in cols_info else "latitude"
    lon_col = "lon" if "lon" in cols_info else "longitude"

    all_cols = list(cols_info.keys())
    col_str = ", ".join(f"[{c}]" for c in all_cols)

    sql = f"""
        SELECT {col_str} FROM [{table}]
        WHERE [{lat_col}] BETWEEN ? AND ?
          AND [{lon_col}] BETWEEN ? AND ?
          AND [{lat_col}] IS NOT NULL
          AND [{lon_col}] IS NOT NULL
    """
    c.execute(sql, (lat_min, lat_max, lon_min, lon_max))
    rows = c.fetchall()

    results = []
    for row in rows:
        d = dict(zip(all_cols, row))
        rlat = d.get(lat_col)
        rlon = d.get(lon_col)
        if rlat is None or rlon is None:
            continue
        dist = haversine_km(lat, lon, rlat, rlon)
        if dist <= radius_km:
            d["distance_km"] = round(dist, 2)
            results.append(d)

    results.sort(key=lambda x: x["distance_km"])
    return results


# ── ACPA Compliance ────────────────────────────────────────────────
def check_acpa(pharmacies, supermarkets, gps, medical_centres, hospitals, shopping_centres, nearest_pharm_km):
    """Check ACPA rules. Returns dict of rule -> {pass, reason, details}."""
    rules = {}

    # Item 130: supermarket within 1.5km AND (GP or medical centre within 1.5km) AND nearest pharmacy >1.5km
    super_15 = [s for s in supermarkets if s["distance_km"] <= 1.5]
    gp_15 = [g for g in gps if g["distance_km"] <= 1.5]
    mc_15 = [m for m in medical_centres if m["distance_km"] <= 1.5]
    has_health = len(gp_15) > 0 or len(mc_15) > 0

    if super_15 and has_health and nearest_pharm_km > 1.5:
        rules["Item 130"] = {
            "pass": True,
            "reason": f"supermarket at {super_15[0]['distance_km']}km, health at {min([g['distance_km'] for g in gp_15] + [m['distance_km'] for m in mc_15]):.1f}km, nearest pharmacy {nearest_pharm_km:.1f}km",
        }
    else:
        reasons = []
        if not super_15:
            nearest_s = supermarkets[0]["distance_km"] if supermarkets else 99
            reasons.append(f"no supermarket within 1.5km (nearest: {nearest_s:.1f}km)")
        if not has_health:
            reasons.append("no GP/medical centre within 1.5km")
        if nearest_pharm_km <= 1.5:
            reasons.append(f"pharmacy too close ({nearest_pharm_km:.1f}km)")
        rules["Item 130"] = {"pass": False, "reason": "; ".join(reasons)}

    # Item 131: nearest pharmacy >10km
    if nearest_pharm_km > 10:
        rules["Item 131"] = {
            "pass": True,
            "reason": f"nearest pharmacy {nearest_pharm_km:.1f}km (>10km threshold)",
        }
    else:
        rules["Item 131"] = {
            "pass": False,
            "reason": f"nearest pharmacy {nearest_pharm_km:.1f}km (<10km threshold)",
        }

    # Item 133: shopping centre nearby with sufficient GLA (>1000 sqm)
    sc_nearby = [s for s in shopping_centres if s["distance_km"] <= 2.0]
    sc_with_gla = [s for s in sc_nearby if (s.get("gla_sqm") or s.get("estimated_gla") or 0) >= 1000]
    if sc_with_gla and nearest_pharm_km > 1.5:
        best = sc_with_gla[0]
        gla = best.get("gla_sqm") or best.get("estimated_gla") or 0
        rules["Item 133"] = {
            "pass": True,
            "reason": f"{best.get('name', 'Shopping Centre')} at {best['distance_km']}km, GLA {gla:.0f}sqm",
        }
    else:
        if not sc_nearby:
            rules["Item 133"] = {"pass": False, "reason": "no shopping centre within 2km"}
        elif not sc_with_gla:
            rules["Item 133"] = {"pass": False, "reason": "no shopping centre with GLA ≥1000sqm"}
        else:
            rules["Item 133"] = {"pass": False, "reason": f"pharmacy too close ({nearest_pharm_km:.1f}km)"}

    # Item 135: private hospital within 2km with 150+ beds
    hosp_2km = [h for h in hospitals if h["distance_km"] <= 2.0]
    private_hosp = [h for h in hosp_2km if "private" in (h.get("hospital_type") or "").lower()]
    big_private = [h for h in private_hosp if (h.get("bed_count") or 0) >= 150]
    if big_private:
        h = big_private[0]
        rules["Item 135"] = {
            "pass": True,
            "reason": f"{h.get('name', 'Hospital')} at {h['distance_km']}km, {h.get('bed_count', '?')} beds",
        }
    else:
        if not hosp_2km:
            rules["Item 135"] = {"pass": False, "reason": "no hospital within 2km"}
        elif not private_hosp:
            rules["Item 135"] = {"pass": False, "reason": "no private hospital within 2km"}
        else:
            rules["Item 135"] = {"pass": False, "reason": f"private hospital has <150 beds"}

    # Item 136: medical centre within 500m with 8+ GPs operating 70+ hours/week
    mc_500 = [m for m in medical_centres if m["distance_km"] <= 0.5]
    mc_qualified = [
        m for m in mc_500
        if (m.get("num_gps") or 0) >= 8 and (m.get("hours_per_week") or 0) >= 70
    ]
    if mc_qualified:
        m = mc_qualified[0]
        rules["Item 136"] = {
            "pass": True,
            "reason": f"{m.get('name', 'Medical Centre')} at {m['distance_km']*1000:.0f}m, {m.get('num_gps', '?')} GPs, {m.get('hours_per_week', '?')}hrs/wk",
        }
    else:
        if not mc_500:
            nearest_mc = medical_centres[0]["distance_km"] if medical_centres else 99
            rules["Item 136"] = {"pass": False, "reason": f"no medical centre within 500m (nearest: {nearest_mc:.1f}km)"}
        else:
            mc = mc_500[0]
            rules["Item 136"] = {
                "pass": False,
                "reason": f"medical centre at {mc['distance_km']*1000:.0f}m has {mc.get('num_gps', '?')} GPs / {mc.get('hours_per_week', '?')}hrs (need 8+ GPs, 70+ hrs)",
            }

    return rules


# ── Profitability ──────────────────────────────────────────────────
def estimate_profitability(conn, lat, lon, supermarkets, pharmacies, gps):
    """Estimate financial metrics for a site."""
    # Population proxy: supermarkets within 5km × 10,000
    super_5km = query_nearby(conn, "supermarkets", lat, lon, 5.0)
    pop_estimate = max(len(super_5km) * 10000, 5000)  # floor at 5000

    # Also check population_grid for better estimate
    pop_rows = query_nearby(conn, "population_grid", lat, lon, 5.0)
    if pop_rows:
        grid_pop = sum(r.get("population", 0) for r in pop_rows)
        if grid_pop > 0:
            pop_estimate = grid_pop

    # Pharmacies within 5km
    pharm_5km = [p for p in pharmacies if p["distance_km"] <= 5.0]
    n_pharm_5 = len(pharm_5km)

    # Scripts estimate
    scripts_per_capita = 16
    scripts_raw = (pop_estimate / (n_pharm_5 + 1)) * scripts_per_capita

    # GP boost
    gps_2km = len(gps)
    gp_boost = 1.0 + 0.08 * min(gps_2km, 10)
    scripts = scripts_raw * gp_boost

    # Revenue and profit
    avg_script_value = 8.50
    cogs_ratio = 0.65
    revenue = (scripts * avg_script_value) / cogs_ratio
    gross_profit = revenue * 0.33
    setup_cost = 475_000
    payback_years = setup_cost / gross_profit if gross_profit > 0 else 99
    exit_value = revenue * 0.4
    flip_profit = exit_value - setup_cost

    return {
        "population_5km": pop_estimate,
        "pharmacies_5km": n_pharm_5,
        "gps_2km": gps_2km,
        "gp_boost": round(gp_boost, 2),
        "scripts_year": int(scripts),
        "revenue": revenue,
        "gross_profit": gross_profit,
        "setup_cost": setup_cost,
        "payback_years": round(payback_years, 1),
        "exit_value": exit_value,
        "flip_profit": flip_profit,
    }


# ── Growth Check ───────────────────────────────────────────────────
def check_growth(conn, lat, lon):
    """Check growth signals near the site."""
    signals = []

    # Growth corridors within 15km
    corridors = query_nearby(conn, "growth_corridors", lat, lon, 15.0)
    for gc in corridors[:3]:
        pop_curr = gc.get("population_current") or gc.get("population_2021") or 0
        pop_proj = gc.get("population_projected") or 0
        growth_pct = gc.get("growth_rate_annual") or gc.get("growth_rate_3yr") or 0
        signals.append({
            "type": "growth_corridor",
            "icon": "📈",
            "name": gc.get("sa2_name", "Unknown"),
            "detail": f"Pop: {pop_curr:,} → {pop_proj:,} projected" if pop_proj else f"Growth: {growth_pct:.1f}%/yr",
            "distance_km": gc["distance_km"],
        })

    # Planned retail within 10km
    planned = query_nearby(conn, "planned_retail", lat, lon, 10.0)
    for pr in planned[:3]:
        signals.append({
            "type": "planned_retail",
            "icon": "🏗️",
            "name": pr.get("name", "Development"),
            "detail": f"est. {pr.get('est_completion', '?')}",
            "distance_km": pr["distance_km"],
        })

    # PSP projects within 15km
    psps = query_nearby(conn, "psp_projects", lat, lon, 15.0)
    for p in psps[:3]:
        signals.append({
            "type": "psp",
            "icon": "🏘️",
            "name": p.get("name", "PSP"),
            "detail": f"{p.get('status', '?')}, {p.get('planned_dwellings', '?')} dwellings",
            "distance_km": p["distance_km"],
        })

    # Planned town centres within 10km
    centres = query_nearby(conn, "planned_town_centres", lat, lon, 10.0)
    for tc in centres[:3]:
        signals.append({
            "type": "town_centre",
            "icon": "🏙️",
            "name": tc.get("centre_name", "Town Centre"),
            "detail": f"{tc.get('centre_type', '?')}",
            "distance_km": tc["distance_km"],
        })

    # Council DAs within 5km
    das = query_nearby(conn, "council_da", lat, lon, 5.0)
    for da in das[:2]:
        signals.append({
            "type": "council_da",
            "icon": "📋",
            "name": da.get("description", "DA")[:60],
            "detail": f"status: {da.get('status', '?')}",
            "distance_km": da["distance_km"],
        })

    # Developments within 5km
    devs = query_nearby(conn, "developments", lat, lon, 5.0)
    for d in devs[:3]:
        signals.append({
            "type": "development",
            "icon": "🏗️",
            "name": d.get("name", "Development"),
            "detail": f"{d.get('expected_completion', '?')} — {d.get('scale_dwellings', '?')} dwellings",
            "distance_km": d["distance_km"],
        })

    return signals


# ── Verdict ────────────────────────────────────────────────────────
def determine_verdict(rules, financials, growth_signals, nearest_pharm_km):
    """Determine GO / VIABLE / CAUTION / NO-GO verdict."""
    any_pass = any(r["pass"] for r in rules.values())
    has_growth = len(growth_signals) > 0

    # Profitability score (0-100)
    payback = financials["payback_years"]
    if payback <= 1.5:
        prof_score = 90
    elif payback <= 2.5:
        prof_score = 75
    elif payback <= 4:
        prof_score = 55
    elif payback <= 6:
        prof_score = 35
    else:
        prof_score = 15

    # Adjust for flip profit
    if financials["flip_profit"] > 200_000:
        prof_score = min(100, prof_score + 10)
    elif financials["flip_profit"] < 0:
        prof_score = max(0, prof_score - 20)

    # Growth bonus
    if has_growth:
        prof_score = min(100, prof_score + 5 * min(len(growth_signals), 4))

    # Verdict
    if any_pass and prof_score > 60:
        verdict = "GO"
        emoji = "🟢"
        label = "Strong Opportunity"
    elif any_pass:
        verdict = "VIABLE"
        emoji = "🟡"
        label = "Passes ACPA — Needs Financial Review"
    elif 5 <= nearest_pharm_km <= 10 and has_growth:
        verdict = "VIABLE"
        emoji = "🟡"
        label = "Ministerial Path"
    elif nearest_pharm_km > 3 and has_growth:
        verdict = "CAUTION"
        emoji = "🟠"
        label = "Marginal — Needs Detailed Assessment"
    elif has_growth and prof_score > 50:
        verdict = "CAUTION"
        emoji = "🟠"
        label = "Growth Area — Compliance Uncertain"
    else:
        verdict = "NO-GO"
        emoji = "🔴"
        label = "Fails All Rules"

    return {
        "verdict": verdict,
        "emoji": emoji,
        "label": label,
        "profitability_score": prof_score,
    }


# ── Output formatting ─────────────────────────────────────────────
def fmt_money(val):
    """Format dollar value."""
    if abs(val) >= 1_000_000:
        return f"${val / 1_000_000:.1f}M"
    elif abs(val) >= 1_000:
        return f"${val / 1_000:.0f}K"
    else:
        return f"${val:.0f}"


def print_report(address_display, lat, lon, pharmacies, gps, supermarkets, hospitals,
                 medical_centres, shopping_centres, rules, financials, growth_signals, verdict_info):
    """Print a beautiful boxed terminal report."""
    W = 56  # inner width

    def line(text=""):
        text = str(text)
        # Pad to width accounting for emoji/unicode widths
        padding = W - len(text)
        if padding < 0:
            text = text[:W]
            padding = 0
        return f"║  {text}{' ' * padding}║"

    def header(text):
        return line(text)

    def sep():
        return "╠" + "═" * (W + 2) + "╣"

    print()
    print("╔" + "═" * (W + 2) + "╗")
    print(line(f"SITE EVALUATION: {address_display[:W - 18]}"))
    print(line(f"Verdict: {verdict_info['emoji']} {verdict_info['verdict']} ({verdict_info['label']})"))
    print(sep())

    # Location
    print(line("LOCATION"))
    print(line(f"Coords: {lat:.4f}, {lon:.4f}"))
    print(line())

    # Nearest pharmacies
    print(line("NEAREST PHARMACIES"))
    if pharmacies:
        for i, p in enumerate(pharmacies[:5]):
            name = (p.get("name") or "Unknown")[:35]
            print(line(f"{i+1}. {name} — {p['distance_km']:.1f}km"))
    else:
        print(line("No pharmacies found within 15km"))
    print(line())

    # GPs
    gp_count = len(gps)
    if gp_count > 0:
        print(line(f"GPs WITHIN 2KM: {gp_count}"))
        for g in gps[:3]:
            name = (g.get("name") or "Unknown")[:35]
            print(line(f"  • {name} — {g['distance_km']:.1f}km"))
        print(line())

    # Supermarkets
    if supermarkets:
        print(line(f"SUPERMARKETS WITHIN 2KM: {len(supermarkets)}"))
        for s in supermarkets[:3]:
            name = (s.get("name") or "Unknown")[:35]
            print(line(f"  • {name} — {s['distance_km']:.1f}km"))
        print(line())

    # Medical centres
    if medical_centres:
        print(line(f"MEDICAL CENTRES WITHIN 2KM: {len(medical_centres)}"))
        for m in medical_centres[:3]:
            name = (m.get("name") or "Unknown")[:35]
            gps_n = m.get("num_gps") or "?"
            print(line(f"  • {name} ({gps_n} GPs) — {m['distance_km']:.1f}km"))
        print(line())

    # ACPA
    print(line("ACPA COMPLIANCE"))
    best_pathway = "None identified"
    for rule_name, r in rules.items():
        icon = "✅" if r["pass"] else "❌"
        status = "PASS" if r["pass"] else "FAIL"
        prefix = f"{rule_name}: {icon} {status}"
        reason = r["reason"]
        # Truncate reason to fit box
        max_reason = W - len(prefix) - 4  # 4 for " ()" + buffer
        if len(reason) > max_reason:
            reason = reason[:max_reason - 1] + "…"
        print(line(f"{prefix} ({reason})"))
        if r["pass"] and best_pathway == "None identified":
            best_pathway = rule_name

    nearest_pharm_km = pharmacies[0]["distance_km"] if pharmacies else 99
    if best_pathway == "None identified":
        if 5 <= nearest_pharm_km <= 10:
            best_pathway = "Ministerial discretion"
        elif nearest_pharm_km > 3:
            best_pathway = "Ministerial (marginal)"
    print(line(f"Best pathway: {best_pathway}"))
    print(line())

    # Financials
    print(line("FINANCIALS"))
    print(line(f"Population 5km: ~{financials['population_5km']:,}"))
    print(line(f"Pharmacies 5km: {financials['pharmacies_5km']}"))
    print(line(f"GP boost: {financials['gp_boost']}x ({financials['gps_2km']} GPs)"))
    print(line(f"Est. scripts/year: {financials['scripts_year']:,}"))
    print(line(f"Est. revenue: {fmt_money(financials['revenue'])}"))
    print(line(f"Est. gross profit: {fmt_money(financials['gross_profit'])}"))
    print(line(f"Setup cost: {fmt_money(financials['setup_cost'])}"))
    print(line(f"Payback: {financials['payback_years']:.1f} years"))
    print(line(f"Exit value: {fmt_money(financials['exit_value'])}"))
    fp = financials["flip_profit"]
    if fp >= 0:
        print(line(f"Flip profit: +{fmt_money(fp)}"))
    else:
        print(line(f"Flip profit: -{fmt_money(abs(fp))}"))
    print(line(f"Profitability score: {verdict_info['profitability_score']}/100"))
    print(line())

    # Growth
    if growth_signals:
        print(line("GROWTH SIGNALS"))
        for gs in growth_signals[:6]:
            txt = f"{gs['icon']} {gs['name'][:30]} ({gs['detail'][:20]})"
            print(line(txt))
        print(line())

    print("╚" + "═" * (W + 2) + "╝")
    print()


def save_markdown(address_display, lat, lon, pharmacies, gps, supermarkets, hospitals,
                  medical_centres, shopping_centres, rules, financials, growth_signals, verdict_info):
    """Save evaluation as markdown file."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    safe_name = "".join(c if c.isalnum() or c in " -_" else "_" for c in address_display).strip()
    filepath = OUTPUT_DIR / f"evaluation_{safe_name}.md"

    nearest_pharm_km = pharmacies[0]["distance_km"] if pharmacies else 99

    lines = [
        f"# Site Evaluation: {address_display}",
        f"**Date:** {datetime.now().strftime('%Y-%m-%d %H:%M')}",
        f"**Verdict:** {verdict_info['emoji']} {verdict_info['verdict']} — {verdict_info['label']}",
        f"**Profitability Score:** {verdict_info['profitability_score']}/100",
        "",
        "## Location",
        f"- Coordinates: {lat:.4f}, {lon:.4f}",
        f"- Nearest pharmacy: {nearest_pharm_km:.1f}km",
        "",
        "## Nearest Pharmacies",
    ]
    for i, p in enumerate(pharmacies[:10]):
        lines.append(f"{i+1}. **{p.get('name', 'Unknown')}** — {p['distance_km']:.1f}km ({p.get('address', '')})")

    lines += ["", "## GPs Within 2km"]
    if gps:
        for g in gps[:10]:
            lines.append(f"- {g.get('name', 'Unknown')} — {g['distance_km']:.1f}km")
    else:
        lines.append("- None found")

    lines += ["", "## Supermarkets Within 2km"]
    if supermarkets:
        for s in supermarkets[:10]:
            lines.append(f"- {s.get('name', 'Unknown')} — {s['distance_km']:.1f}km")
    else:
        lines.append("- None found")

    lines += ["", "## Medical Centres Within 2km"]
    if medical_centres:
        for m in medical_centres[:10]:
            lines.append(f"- {m.get('name', 'Unknown')} ({m.get('num_gps', '?')} GPs) — {m['distance_km']:.1f}km")
    else:
        lines.append("- None found")

    lines += ["", "## Hospitals Within 5km"]
    if hospitals:
        for h in hospitals[:10]:
            lines.append(f"- {h.get('name', 'Unknown')} ({h.get('hospital_type', '?')}, {h.get('bed_count', '?')} beds) — {h['distance_km']:.1f}km")
    else:
        lines.append("- None found")

    lines += ["", "## ACPA Compliance"]
    for rule_name, r in rules.items():
        icon = "✅" if r["pass"] else "❌"
        lines.append(f"- **{rule_name}:** {icon} {'PASS' if r['pass'] else 'FAIL'} — {r['reason']}")

    lines += [
        "",
        "## Financials",
        f"| Metric | Value |",
        f"|--------|-------|",
        f"| Population (5km) | {financials['population_5km']:,} |",
        f"| Pharmacies (5km) | {financials['pharmacies_5km']} |",
        f"| GP boost | {financials['gp_boost']}x |",
        f"| Est. scripts/year | {financials['scripts_year']:,} |",
        f"| Est. revenue | {fmt_money(financials['revenue'])} |",
        f"| Gross profit | {fmt_money(financials['gross_profit'])} |",
        f"| Setup cost | {fmt_money(financials['setup_cost'])} |",
        f"| Payback | {financials['payback_years']:.1f} years |",
        f"| Exit value | {fmt_money(financials['exit_value'])} |",
        f"| Flip profit | {fmt_money(financials['flip_profit'])} |",
    ]

    if growth_signals:
        lines += ["", "## Growth Signals"]
        for gs in growth_signals:
            lines.append(f"- {gs['icon']} **{gs['name']}** — {gs['detail']} ({gs['distance_km']:.1f}km)")

    lines.append("")
    filepath.write_text("\n".join(lines), encoding="utf-8")
    return filepath


def build_json_output(address_display, lat, lon, pharmacies, gps, supermarkets, hospitals,
                      medical_centres, shopping_centres, rules, financials, growth_signals, verdict_info):
    """Build machine-readable JSON output."""
    return {
        "address": address_display,
        "latitude": lat,
        "longitude": lon,
        "timestamp": datetime.now().isoformat(),
        "verdict": verdict_info,
        "nearest_pharmacies": [
            {"name": p.get("name"), "distance_km": p["distance_km"], "address": p.get("address")}
            for p in pharmacies[:10]
        ],
        "gps_2km": [
            {"name": g.get("name"), "distance_km": g["distance_km"]}
            for g in gps[:10]
        ],
        "supermarkets_2km": [
            {"name": s.get("name"), "distance_km": s["distance_km"]}
            for s in supermarkets[:10]
        ],
        "medical_centres_2km": [
            {"name": m.get("name"), "num_gps": m.get("num_gps"), "distance_km": m["distance_km"]}
            for m in medical_centres[:10]
        ],
        "hospitals_5km": [
            {"name": h.get("name"), "type": h.get("hospital_type"), "beds": h.get("bed_count"), "distance_km": h["distance_km"]}
            for h in hospitals[:10]
        ],
        "acpa_rules": {k: v for k, v in rules.items()},
        "financials": financials,
        "growth_signals": growth_signals,
    }


# ── Main ───────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Quick Site Evaluator for pharmacy opportunities")
    parser.add_argument("address", nargs="?", help="Address to evaluate (e.g., 'Beveridge VIC')")
    parser.add_argument("--lat", type=float, help="Latitude")
    parser.add_argument("--lon", type=float, help="Longitude")
    parser.add_argument("--json", action="store_true", dest="json_output", help="Output JSON instead of formatted report")
    args = parser.parse_args()

    # Determine coordinates
    if args.lat is not None and args.lon is not None:
        lat, lon = args.lat, args.lon
        address_display = f"{lat:.4f}, {lon:.4f}"
    elif args.address:
        print(f"Geocoding '{args.address}'...", file=sys.stderr)
        lat, lon, display = geocode_address(args.address)
        if lat is None:
            print(f"ERROR: Could not geocode '{args.address}'", file=sys.stderr)
            sys.exit(1)
        address_display = args.address
        print(f"  → {lat:.4f}, {lon:.4f} ({display})", file=sys.stderr)
    else:
        parser.print_help()
        sys.exit(1)

    # Connect to DB
    if not DB_PATH.exists():
        print(f"ERROR: Database not found at {DB_PATH}", file=sys.stderr)
        sys.exit(1)
    conn = sqlite3.connect(str(DB_PATH))

    # Run queries
    print("Scanning pharmacies...", file=sys.stderr)
    pharmacies = query_nearby(conn, "pharmacies", lat, lon, 15.0)

    print("Scanning GPs...", file=sys.stderr)
    gps = query_nearby(conn, "gps", lat, lon, 2.0)

    print("Scanning supermarkets...", file=sys.stderr)
    supermarkets = query_nearby(conn, "supermarkets", lat, lon, 2.0)

    print("Scanning hospitals...", file=sys.stderr)
    hospitals = query_nearby(conn, "hospitals", lat, lon, 5.0)

    print("Scanning medical centres...", file=sys.stderr)
    medical_centres = query_nearby(conn, "medical_centres", lat, lon, 2.0)

    print("Scanning shopping centres...", file=sys.stderr)
    shopping_centres = query_nearby(conn, "shopping_centres", lat, lon, 2.0)

    # Nearest pharmacy distance
    nearest_pharm_km = pharmacies[0]["distance_km"] if pharmacies else 99.0

    # ACPA compliance
    print("Checking ACPA compliance...", file=sys.stderr)
    rules = check_acpa(pharmacies, supermarkets, gps, medical_centres, hospitals, shopping_centres, nearest_pharm_km)

    # Profitability
    print("Estimating profitability...", file=sys.stderr)
    financials = estimate_profitability(conn, lat, lon, supermarkets, pharmacies, gps)

    # Growth
    print("Checking growth signals...", file=sys.stderr)
    growth_signals = check_growth(conn, lat, lon)

    # Verdict
    verdict_info = determine_verdict(rules, financials, growth_signals, nearest_pharm_km)

    conn.close()

    # Output
    if args.json_output:
        data = build_json_output(address_display, lat, lon, pharmacies, gps, supermarkets, hospitals,
                                 medical_centres, shopping_centres, rules, financials, growth_signals, verdict_info)
        print(json.dumps(data, indent=2, default=str))
    else:
        print_report(address_display, lat, lon, pharmacies, gps, supermarkets, hospitals,
                     medical_centres, shopping_centres, rules, financials, growth_signals, verdict_info)

    # Save markdown
    md_path = save_markdown(address_display, lat, lon, pharmacies, gps, supermarkets, hospitals,
                            medical_centres, shopping_centres, rules, financials, growth_signals, verdict_info)
    print(f"Report saved: {md_path}", file=sys.stderr)


if __name__ == "__main__":
    main()
