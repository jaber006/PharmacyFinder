"""
Data Integrity & Quality Checker for PharmacyFinder DB.

Validates all core tables, cross-checks for anomalies, and generates
both JSON and text reports in output/.

Usage:
    py -3.12 scripts/data_integrity_check.py
"""

import sqlite3
import json
import math
import os
from datetime import datetime
from collections import defaultdict

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'pharmacy_finder.db')
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), '..', 'output')

VALID_STATES = {'NSW', 'VIC', 'QLD', 'SA', 'WA', 'TAS', 'NT', 'ACT'}

# Valid Australian coordinate bounds
AU_LAT_MIN, AU_LAT_MAX = -44.0, -10.0
AU_LON_MIN, AU_LON_MAX = 112.0, 154.0


def haversine_m(lat1, lon1, lat2, lon2):
    """Distance in metres between two lat/lon points."""
    R = 6_371_000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def coords_valid(lat, lon):
    """Check if coordinates are within Australia."""
    if lat is None or lon is None:
        return False
    return AU_LAT_MIN <= lat <= AU_LAT_MAX and AU_LON_MIN <= lon <= AU_LON_MAX


def check_pharmacies(conn):
    """Check pharmacies table for quality issues."""
    c = conn.cursor()
    issues = []

    # Total count
    c.execute("SELECT COUNT(*) FROM pharmacies")
    total = c.fetchone()[0]

    # Missing coordinates (should be NOT NULL but check for 0/invalid)
    c.execute("SELECT id, name, address, latitude, longitude FROM pharmacies")
    rows = c.fetchall()

    missing_coords = []
    invalid_coords = []
    invalid_states = []
    no_state = []
    no_name = []

    for row in rows:
        pid, name, addr, lat, lon = row
        if lat is None or lon is None or (lat == 0 and lon == 0):
            missing_coords.append({'id': pid, 'name': name, 'address': addr})
        elif not coords_valid(lat, lon):
            invalid_coords.append({'id': pid, 'name': name, 'lat': lat, 'lon': lon})
        if not name:
            no_name.append({'id': pid, 'address': addr})

    # Invalid states
    c.execute("SELECT id, name, state FROM pharmacies WHERE state IS NOT NULL")
    for pid, name, state in c.fetchall():
        if state not in VALID_STATES:
            invalid_states.append({'id': pid, 'name': name, 'state': state})

    c.execute("SELECT id, name, address FROM pharmacies WHERE state IS NULL OR state = ''")
    for pid, name, addr in c.fetchall():
        no_state.append({'id': pid, 'name': name, 'address': addr})

    # Duplicate names within 100m
    c.execute("SELECT id, name, latitude, longitude FROM pharmacies ORDER BY name")
    all_pharma = c.fetchall()
    name_groups = defaultdict(list)
    for pid, name, lat, lon in all_pharma:
        if name:
            name_groups[name.strip().upper()].append((pid, name, lat, lon))

    duplicate_name_proximity = []
    for norm_name, entries in name_groups.items():
        if len(entries) < 2:
            continue
        for i in range(len(entries)):
            for j in range(i + 1, len(entries)):
                d = haversine_m(entries[i][2], entries[i][3], entries[j][2], entries[j][3])
                if d < 100:
                    duplicate_name_proximity.append({
                        'pharmacy_a': {'id': entries[i][0], 'name': entries[i][1]},
                        'pharmacy_b': {'id': entries[j][0], 'name': entries[j][1]},
                        'distance_m': round(d, 1)
                    })

    # Coord verified stats
    c.execute("SELECT COUNT(*) FROM pharmacies WHERE coord_verified = 1")
    verified = c.fetchone()[0]

    # Source breakdown
    c.execute("SELECT source, COUNT(*) FROM pharmacies GROUP BY source")
    sources = {r[0]: r[1] for r in c.fetchall()}

    return {
        'total': total,
        'coord_verified': verified,
        'coord_verified_pct': round(verified / total * 100, 1) if total else 0,
        'missing_coordinates': missing_coords,
        'invalid_coordinates': invalid_coords,
        'invalid_states': invalid_states,
        'missing_state': no_state,
        'missing_name': no_name,
        'duplicate_names_within_100m': duplicate_name_proximity,
        'sources': sources,
        'issue_counts': {
            'missing_coords': len(missing_coords),
            'invalid_coords': len(invalid_coords),
            'invalid_states': len(invalid_states),
            'missing_state': len(no_state),
            'missing_name': len(no_name),
            'duplicate_name_proximity': len(duplicate_name_proximity),
        }
    }


def check_close_pharmacies(conn, threshold_m=50):
    """Find pharmacies suspiciously close (<threshold_m) — likely duplicates regardless of name."""
    c = conn.cursor()
    c.execute("SELECT id, name, latitude, longitude FROM pharmacies ORDER BY latitude")
    rows = c.fetchall()

    # Sort by lat for early exit optimisation
    close_pairs = []
    lat_threshold = threshold_m / 111_000  # rough degrees

    for i in range(len(rows)):
        for j in range(i + 1, len(rows)):
            if rows[j][2] - rows[i][2] > lat_threshold:
                break
            d = haversine_m(rows[i][2], rows[i][3], rows[j][2], rows[j][3])
            if d < threshold_m:
                close_pairs.append({
                    'pharmacy_a': {'id': rows[i][0], 'name': rows[i][1]},
                    'pharmacy_b': {'id': rows[j][0], 'name': rows[j][1]},
                    'distance_m': round(d, 1)
                })

    return close_pairs


def check_medical_centres(conn):
    """Check medical_centres and gps tables."""
    c = conn.cursor()
    issues = {}

    # Medical centres
    c.execute("SELECT COUNT(*) FROM medical_centres")
    mc_total = c.fetchone()[0]

    c.execute("SELECT id, name, address FROM medical_centres WHERE num_gps IS NULL OR num_gps = 0")
    mc_no_gps = [{'id': r[0], 'name': r[1], 'address': r[2]} for r in c.fetchall()]

    c.execute("SELECT id, name, latitude, longitude FROM medical_centres")
    mc_bad_coords = []
    for pid, name, lat, lon in c.fetchall():
        if not coords_valid(lat, lon):
            mc_bad_coords.append({'id': pid, 'name': name, 'lat': lat, 'lon': lon})

    c.execute("SELECT id, name, num_gps FROM medical_centres WHERE num_gps > 30")
    mc_high_gps = [{'id': r[0], 'name': r[1], 'num_gps': r[2]} for r in c.fetchall()]

    c.execute("SELECT COUNT(*) FROM medical_centres WHERE coord_verified = 1")
    mc_verified = c.fetchone()[0]

    c.execute("SELECT COUNT(*) FROM medical_centres WHERE gp_count_verified = 1")
    mc_gp_verified = c.fetchone()[0]

    issues['medical_centres'] = {
        'total': mc_total,
        'coord_verified': mc_verified,
        'gp_count_verified': mc_gp_verified,
        'missing_or_zero_gps': mc_no_gps,
        'invalid_coordinates': mc_bad_coords,
        'suspicious_high_gps': mc_high_gps,
        'issue_counts': {
            'missing_or_zero_gps': len(mc_no_gps),
            'invalid_coords': len(mc_bad_coords),
            'suspicious_high_gps': len(mc_high_gps),
        }
    }

    # GP practices (gps table)
    c.execute("SELECT COUNT(*) FROM gps")
    gp_total = c.fetchone()[0]

    c.execute("SELECT id, name, latitude, longitude FROM gps")
    gp_bad_coords = []
    for pid, name, lat, lon in c.fetchall():
        if not coords_valid(lat, lon):
            gp_bad_coords.append({'id': pid, 'name': name, 'lat': lat, 'lon': lon})

    c.execute("SELECT COUNT(*) FROM gps WHERE coord_verified = 1")
    gp_verified = c.fetchone()[0]

    c.execute("SELECT COUNT(*) FROM gps WHERE fte IS NULL OR fte = 0")
    gp_no_fte = c.fetchone()[0]

    issues['gp_practices'] = {
        'total': gp_total,
        'coord_verified': gp_verified,
        'coord_verified_pct': round(gp_verified / gp_total * 100, 1) if gp_total else 0,
        'invalid_coordinates': gp_bad_coords,
        'missing_fte': gp_no_fte,
        'issue_counts': {
            'invalid_coords': len(gp_bad_coords),
            'missing_fte': gp_no_fte,
        }
    }

    return issues


def check_shopping_centres(conn):
    """Check shopping_centres table."""
    c = conn.cursor()

    c.execute("SELECT COUNT(*) FROM shopping_centres")
    total = c.fetchone()[0]

    c.execute("SELECT id, name, address FROM shopping_centres WHERE (estimated_tenants IS NULL OR estimated_tenants = 0)")
    no_tenants = [{'id': r[0], 'name': r[1], 'address': r[2]} for r in c.fetchall()]

    c.execute("SELECT id, name, address FROM shopping_centres WHERE (gla_sqm IS NULL OR gla_sqm = 0) AND (estimated_gla IS NULL OR estimated_gla = 0)")
    no_gla = [{'id': r[0], 'name': r[1], 'address': r[2]} for r in c.fetchall()]

    c.execute("SELECT id, name, estimated_tenants FROM shopping_centres WHERE estimated_tenants > 500")
    high_tenants = [{'id': r[0], 'name': r[1], 'estimated_tenants': r[2]} for r in c.fetchall()]

    c.execute("SELECT id, name, latitude, longitude FROM shopping_centres")
    bad_coords = []
    for pid, name, lat, lon in c.fetchall():
        if not coords_valid(lat, lon):
            bad_coords.append({'id': pid, 'name': name, 'lat': lat, 'lon': lon})

    c.execute("SELECT COUNT(*) FROM shopping_centres WHERE coord_verified = 1")
    verified = c.fetchone()[0]

    c.execute("SELECT COUNT(*) FROM shopping_centres WHERE tenants_verified = 1")
    tenants_verified = c.fetchone()[0]

    return {
        'total': total,
        'coord_verified': verified,
        'tenants_verified': tenants_verified,
        'missing_tenants': no_tenants,
        'missing_gla': no_gla,
        'suspicious_high_tenants': high_tenants,
        'invalid_coordinates': bad_coords,
        'issue_counts': {
            'missing_tenants': len(no_tenants),
            'missing_gla': len(no_gla),
            'suspicious_high_tenants': len(high_tenants),
            'invalid_coords': len(bad_coords),
        }
    }


def check_hospitals(conn):
    """Check hospitals table."""
    c = conn.cursor()

    c.execute("SELECT COUNT(*) FROM hospitals")
    total = c.fetchone()[0]

    c.execute("SELECT id, name, address FROM hospitals WHERE bed_count IS NULL OR bed_count = 0")
    no_beds = [{'id': r[0], 'name': r[1], 'address': r[2]} for r in c.fetchall()]

    c.execute("SELECT id, name, address FROM hospitals WHERE hospital_type IS NULL OR hospital_type = ''")
    no_type = [{'id': r[0], 'name': r[1], 'address': r[2]} for r in c.fetchall()]

    c.execute("SELECT id, name, bed_count FROM hospitals WHERE bed_count > 2000")
    high_beds = [{'id': r[0], 'name': r[1], 'bed_count': r[2]} for r in c.fetchall()]

    c.execute("SELECT id, name, latitude, longitude FROM hospitals")
    bad_coords = []
    for pid, name, lat, lon in c.fetchall():
        if not coords_valid(lat, lon):
            bad_coords.append({'id': pid, 'name': name, 'lat': lat, 'lon': lon})

    c.execute("SELECT COUNT(*) FROM hospitals WHERE coord_verified = 1")
    verified = c.fetchone()[0]

    c.execute("SELECT COUNT(*) FROM hospitals WHERE bed_count_verified = 1")
    beds_verified = c.fetchone()[0]

    return {
        'total': total,
        'coord_verified': verified,
        'bed_count_verified': beds_verified,
        'missing_bed_count': no_beds,
        'missing_hospital_type': no_type,
        'suspicious_high_beds': high_beds,
        'invalid_coordinates': bad_coords,
        'issue_counts': {
            'missing_bed_count': len(no_beds),
            'missing_hospital_type': len(no_type),
            'suspicious_high_beds': len(high_beds),
            'invalid_coords': len(bad_coords),
        }
    }


def check_supermarkets(conn):
    """Check supermarkets table."""
    c = conn.cursor()

    c.execute("SELECT COUNT(*) FROM supermarkets")
    total = c.fetchone()[0]

    c.execute("""SELECT id, name, address FROM supermarkets
                 WHERE (estimated_gla IS NULL OR estimated_gla = 0)
                 AND (floor_area_sqm IS NULL OR floor_area_sqm = 0)""")
    no_gla = [{'id': r[0], 'name': r[1], 'address': r[2]} for r in c.fetchall()]

    c.execute("SELECT id, name, latitude, longitude FROM supermarkets")
    bad_coords = []
    for pid, name, lat, lon in c.fetchall():
        if not coords_valid(lat, lon):
            bad_coords.append({'id': pid, 'name': name, 'lat': lat, 'lon': lon})

    c.execute("SELECT COUNT(*) FROM supermarkets WHERE coord_verified = 1")
    verified = c.fetchone()[0]

    c.execute("SELECT brand, COUNT(*) FROM supermarkets GROUP BY brand ORDER BY COUNT(*) DESC")
    brands = {r[0] or 'Unknown': r[1] for r in c.fetchall()}

    return {
        'total': total,
        'coord_verified': verified,
        'coord_verified_pct': round(verified / total * 100, 1) if total else 0,
        'missing_gla': no_gla,
        'invalid_coordinates': bad_coords,
        'brands': brands,
        'issue_counts': {
            'missing_gla': len(no_gla),
            'invalid_coords': len(bad_coords),
        }
    }


def build_summary(report):
    """Build overall summary statistics."""
    tables = {
        'pharmacies': report['pharmacies']['total'],
        'medical_centres': report['medical_centres']['total'],
        'gp_practices': report['gp_practices']['total'],
        'shopping_centres': report['shopping_centres']['total'],
        'hospitals': report['hospitals']['total'],
        'supermarkets': report['supermarkets']['total'],
    }

    total_issues = 0
    for section_key in ['pharmacies', 'medical_centres', 'gp_practices',
                        'shopping_centres', 'hospitals', 'supermarkets']:
        section = report[section_key]
        if 'issue_counts' in section:
            total_issues += sum(section['issue_counts'].values())

    total_issues += len(report['cross_checks']['close_pharmacies_under_50m'])

    return {
        'records_per_table': tables,
        'total_records': sum(tables.values()),
        'total_issues_found': total_issues,
        'close_pharmacy_pairs': len(report['cross_checks']['close_pharmacies_under_50m']),
        'report_generated': datetime.now().isoformat(),
    }


def format_text_report(report):
    """Generate human-readable text report."""
    lines = []
    lines.append("=" * 70)
    lines.append("PHARMACYFINDER DATA QUALITY REPORT")
    lines.append(f"Generated: {report['summary']['report_generated']}")
    lines.append("=" * 70)

    # Summary
    lines.append("\n## SUMMARY")
    lines.append(f"Total records across core tables: {report['summary']['total_records']:,}")
    lines.append(f"Total issues found: {report['summary']['total_issues_found']}")
    lines.append("")
    lines.append("Records per table:")
    for table, count in report['summary']['records_per_table'].items():
        lines.append(f"  {table:<25} {count:>6,}")

    # Pharmacies
    p = report['pharmacies']
    lines.append(f"\n{'=' * 70}")
    lines.append("## PHARMACIES")
    lines.append(f"Total: {p['total']:,}")
    lines.append(f"Coord verified: {p['coord_verified']:,} ({p['coord_verified_pct']}%)")
    lines.append(f"Sources: {p['sources']}")
    lines.append(f"\nIssues:")
    for k, v in p['issue_counts'].items():
        status = "OK" if v == 0 else f"WARN ({v})"
        lines.append(f"  {k:<30} {status}")

    if p['invalid_states']:
        lines.append(f"\n  Invalid states found:")
        for item in p['invalid_states'][:10]:
            lines.append(f"    ID {item['id']}: {item['name']} -> state='{item['state']}'")

    if p['duplicate_names_within_100m']:
        lines.append(f"\n  Duplicate names within 100m:")
        for item in p['duplicate_names_within_100m'][:10]:
            lines.append(f"    {item['pharmacy_a']['name']} (ID {item['pharmacy_a']['id']}) <-> "
                        f"(ID {item['pharmacy_b']['id']}) = {item['distance_m']}m")

    # Medical centres
    mc = report['medical_centres']
    lines.append(f"\n{'=' * 70}")
    lines.append("## MEDICAL CENTRES")
    lines.append(f"Total: {mc['total']}")
    lines.append(f"Coord verified: {mc['coord_verified']}")
    lines.append(f"GP count verified: {mc['gp_count_verified']}")
    lines.append(f"\nIssues:")
    for k, v in mc['issue_counts'].items():
        status = "OK" if v == 0 else f"WARN ({v})"
        lines.append(f"  {k:<30} {status}")

    if mc['suspicious_high_gps']:
        lines.append(f"\n  Suspiciously high GP count (>30):")
        for item in mc['suspicious_high_gps']:
            lines.append(f"    ID {item['id']}: {item['name']} -> {item['num_gps']} GPs")

    # GP practices
    gp = report['gp_practices']
    lines.append(f"\n{'=' * 70}")
    lines.append("## GP PRACTICES")
    lines.append(f"Total: {gp['total']:,}")
    lines.append(f"Coord verified: {gp['coord_verified']} ({gp['coord_verified_pct']}%)")
    lines.append(f"\nIssues:")
    for k, v in gp['issue_counts'].items():
        status = "OK" if v == 0 else f"WARN ({v})"
        lines.append(f"  {k:<30} {status}")

    # Shopping centres
    sc = report['shopping_centres']
    lines.append(f"\n{'=' * 70}")
    lines.append("## SHOPPING CENTRES")
    lines.append(f"Total: {sc['total']}")
    lines.append(f"Coord verified: {sc['coord_verified']}")
    lines.append(f"Tenants verified: {sc['tenants_verified']}")
    lines.append(f"\nIssues:")
    for k, v in sc['issue_counts'].items():
        status = "OK" if v == 0 else f"WARN ({v})"
        lines.append(f"  {k:<30} {status}")

    if sc['suspicious_high_tenants']:
        lines.append(f"\n  Suspiciously high tenant count (>500):")
        for item in sc['suspicious_high_tenants']:
            lines.append(f"    ID {item['id']}: {item['name']} -> {item['estimated_tenants']} tenants")

    # Hospitals
    h = report['hospitals']
    lines.append(f"\n{'=' * 70}")
    lines.append("## HOSPITALS")
    lines.append(f"Total: {h['total']}")
    lines.append(f"Coord verified: {h['coord_verified']}")
    lines.append(f"Bed count verified: {h['bed_count_verified']}")
    lines.append(f"\nIssues:")
    for k, v in h['issue_counts'].items():
        status = "OK" if v == 0 else f"WARN ({v})"
        lines.append(f"  {k:<30} {status}")

    if h['suspicious_high_beds']:
        lines.append(f"\n  Suspiciously high bed count (>2000):")
        for item in h['suspicious_high_beds']:
            lines.append(f"    ID {item['id']}: {item['name']} -> {item['bed_count']} beds")

    # Supermarkets
    s = report['supermarkets']
    lines.append(f"\n{'=' * 70}")
    lines.append("## SUPERMARKETS")
    lines.append(f"Total: {s['total']:,}")
    lines.append(f"Coord verified: {s['coord_verified']} ({s['coord_verified_pct']}%)")
    lines.append(f"\nBrand breakdown:")
    for brand, count in s['brands'].items():
        lines.append(f"  {brand:<25} {count:>5}")
    lines.append(f"\nIssues:")
    for k, v in s['issue_counts'].items():
        status = "OK" if v == 0 else f"WARN ({v})"
        lines.append(f"  {k:<30} {status}")

    # Cross-checks
    cc = report['cross_checks']
    lines.append(f"\n{'=' * 70}")
    lines.append("## CROSS-CHECKS")
    lines.append(f"Pharmacy pairs within 50m: {len(cc['close_pharmacies_under_50m'])}")
    if cc['close_pharmacies_under_50m']:
        lines.append(f"\n  Close pharmacy pairs (likely duplicates):")
        for item in cc['close_pharmacies_under_50m'][:20]:
            lines.append(f"    {item['pharmacy_a']['name']} (ID {item['pharmacy_a']['id']}) <-> "
                        f"{item['pharmacy_b']['name']} (ID {item['pharmacy_b']['id']}) = {item['distance_m']}m")

    lines.append(f"\n{'=' * 70}")
    lines.append("END OF REPORT")
    lines.append("=" * 70)

    return '\n'.join(lines)


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)

    print("Running data integrity checks...")

    print("  Checking pharmacies...")
    pharmacy_results = check_pharmacies(conn)

    print("  Checking close pharmacies (<50m)...")
    close_pharmacies = check_close_pharmacies(conn)

    print("  Checking medical centres & GPs...")
    medical_results = check_medical_centres(conn)

    print("  Checking shopping centres...")
    sc_results = check_shopping_centres(conn)

    print("  Checking hospitals...")
    hospital_results = check_hospitals(conn)

    print("  Checking supermarkets...")
    supermarket_results = check_supermarkets(conn)

    report = {
        'pharmacies': pharmacy_results,
        'medical_centres': medical_results['medical_centres'],
        'gp_practices': medical_results['gp_practices'],
        'shopping_centres': sc_results,
        'hospitals': hospital_results,
        'supermarkets': supermarket_results,
        'cross_checks': {
            'close_pharmacies_under_50m': close_pharmacies,
        },
    }

    report['summary'] = build_summary(report)

    # Write JSON report
    json_path = os.path.join(OUTPUT_DIR, 'data_quality_report.json')
    with open(json_path, 'w') as f:
        json.dump(report, f, indent=2, default=str)
    print(f"\n  JSON report: {json_path}")

    # Write text report
    text_report = format_text_report(report)
    txt_path = os.path.join(OUTPUT_DIR, 'data_quality_report.txt')
    with open(txt_path, 'w') as f:
        f.write(text_report)
    print(f"  Text report: {txt_path}")

    # Print summary to console
    print(f"\n{'=' * 50}")
    print(f"SUMMARY")
    print(f"{'=' * 50}")
    print(f"Total records: {report['summary']['total_records']:,}")
    print(f"Total issues:  {report['summary']['total_issues_found']}")
    for table, count in report['summary']['records_per_table'].items():
        print(f"  {table:<25} {count:>6,}")
    print(f"  Close pharmacy pairs:   {len(close_pharmacies):>6}")

    conn.close()
    print("\nDone.")


if __name__ == '__main__':
    main()
