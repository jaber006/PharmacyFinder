"""
Recalculate all opportunities against the corrected rules.

This script:
1. Loads all opportunities from the database
2. Re-checks each one against ALL corrected rules (Items 130-136)
3. Updates qualifying_rules, evidence, and verification fields
4. Outputs a summary by state

Optimized: pre-caches all reference data to avoid repeated DB queries.
"""
import sqlite3
import sys
from datetime import datetime
from collections import defaultdict

from utils.database import Database
from utils.distance import haversine_distance, find_nearest, find_within_radius, format_distance
import config


class CachedDatabase:
    """Wrapper around Database that caches all reference data in memory."""
    
    def __init__(self, db_path='pharmacy_finder.db'):
        self.db = Database(db_path)
        self.db.connect()
        
        print("Loading reference data into memory...")
        self._pharmacies = self.db.get_all_pharmacies()
        print(f"  Pharmacies: {len(self._pharmacies)}")
        self._gps = self.db.get_all_gps()
        print(f"  GPs: {len(self._gps)}")
        self._supermarkets = self.db.get_all_supermarkets()
        print(f"  Supermarkets: {len(self._supermarkets)}")
        self._hospitals = self.db.get_all_hospitals()
        print(f"  Hospitals: {len(self._hospitals)}")
        self._shopping_centres = self.db.get_all_shopping_centres()
        print(f"  Shopping centres: {len(self._shopping_centres)}")
        self._medical_centres = self.db.get_all_medical_centres()
        print(f"  Medical centres: {len(self._medical_centres)}")
        print("Done loading.\n")
    
    def get_all_pharmacies(self):
        return self._pharmacies
    
    def get_all_gps(self):
        return self._gps
    
    def get_all_supermarkets(self):
        return self._supermarkets
    
    def get_all_hospitals(self):
        return self._hospitals
    
    def get_all_shopping_centres(self):
        return self._shopping_centres
    
    def get_all_medical_centres(self):
        return self._medical_centres
    
    def close(self):
        self.db.close()


def recalc_all():
    """Re-check every opportunity against all corrected rules."""
    
    # Use cached DB to avoid thousands of repeated queries
    cached_db = CachedDatabase('pharmacy_finder.db')
    
    # Import rules and instantiate with cached db
    from rules.item_130 import Item130Rule
    from rules.item_131 import Item131Rule
    from rules.item_132 import Item132Rule
    from rules.item_133 import Item133Rule
    from rules.item_134 import Item134Rule
    from rules.item_134a import Item134ARule
    from rules.item_135 import Item135Rule
    from rules.item_136 import Item136Rule
    
    rules = [
        Item130Rule(cached_db),
        Item131Rule(cached_db),
        Item132Rule(cached_db),
        Item133Rule(cached_db),
        Item134Rule(cached_db),
        Item134ARule(cached_db),
        Item135Rule(cached_db),
        Item136Rule(cached_db),
    ]

    # Load all opportunities
    conn = sqlite3.connect('pharmacy_finder.db')
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM opportunities ORDER BY region, id")
    opportunities = [dict(row) for row in cursor.fetchall()]
    print(f"=== RECALCULATING {len(opportunities)} OPPORTUNITIES ===\n")

    pharmacies = cached_db.get_all_pharmacies()

    # Results tracking
    results_by_state = defaultdict(lambda: {
        'total': 0,
        'qualifying': 0,
        'by_rule': defaultdict(int),
        'opportunities': []
    })

    updated = 0
    changed = 0
    
    # Skip OSRM calls for Item 131/132 to speed things up — use estimates
    # (The real scripts would do OSRM but this is a bulk recalc)

    for i, opp in enumerate(opportunities):
        if i % 100 == 0:
            print(f"  Processing {i}/{len(opportunities)}...", flush=True)

        lat = opp['latitude']
        lon = opp['longitude']
        region = opp.get('region', 'Unknown')
        old_rules = opp.get('qualifying_rules', '')

        results_by_state[region]['total'] += 1

        property_data = {
            'latitude': lat,
            'longitude': lon,
            'address': opp.get('address', ''),
        }

        # Check all rules
        qualifying = []
        all_evidence = []

        for rule in rules:
            try:
                eligible, evidence = rule.check_eligibility(property_data)
                if eligible and evidence:
                    qualifying.append(rule.item_number)
                    all_evidence.append(evidence)
            except Exception as e:
                pass

        # Recalculate nearest pharmacy distance
        nearest_pharm, nearest_dist = find_nearest(lat, lon, pharmacies)
        nearest_name = nearest_pharm.get('name', 'Unknown') if nearest_pharm else ''
        nearest_km = nearest_dist if nearest_dist is not None else None

        new_rules = ', '.join(qualifying) if qualifying else 'NONE'
        new_evidence = ' | '.join(all_evidence) if all_evidence else opp.get('evidence', '')

        if qualifying:
            verification = 'RECALCULATED'
        else:
            verification = 'NO_QUALIFYING_RULE'

        rules_changed = (new_rules != old_rules)

        cursor.execute("""
            UPDATE opportunities SET
                qualifying_rules = ?,
                evidence = ?,
                nearest_pharmacy_km = ?,
                nearest_pharmacy_name = ?,
                verification = ?,
                verification_notes = ?
            WHERE id = ?
        """, (
            new_rules,
            new_evidence[:5000],
            nearest_km,
            nearest_name,
            verification,
            f"Recalculated {datetime.now().strftime('%Y-%m-%d %H:%M')} | Old rules: {old_rules}",
            opp['id']
        ))

        updated += 1
        if rules_changed:
            changed += 1

        if qualifying:
            results_by_state[region]['qualifying'] += 1
            for r in qualifying:
                results_by_state[region]['by_rule'][r] += 1
            results_by_state[region]['opportunities'].append({
                'id': opp['id'],
                'address': opp.get('address', '')[:80],
                'poi_name': opp.get('poi_name', ''),
                'poi_type': opp.get('poi_type', ''),
                'qualifying_rules': new_rules,
                'old_rules': old_rules,
                'changed': rules_changed,
                'nearest_pharmacy_km': nearest_km,
                'nearest_pharmacy_name': nearest_name,
                'confidence': opp.get('confidence', 0),
                'composite_score': opp.get('composite_score', 0),
            })

    conn.commit()
    conn.close()
    cached_db.close()

    print(f"\n=== DONE ===")
    print(f"Updated: {updated}")
    print(f"Rules changed: {changed}")

    summary = generate_summary(results_by_state)
    
    with open('rules/RECALC_RESULTS.md', 'w', encoding='utf-8') as f:
        f.write(summary)
    print(f"\nResults saved to rules/RECALC_RESULTS.md")

    # Also print TAS summary to console
    if 'TAS' in results_by_state:
        tas = results_by_state['TAS']
        print(f"\n=== TAS HIGHLIGHT ===")
        print(f"  Total: {tas['total']}, Qualifying: {tas['qualifying']}")
        for rule, count in sorted(tas['by_rule'].items()):
            print(f"    {rule}: {count}")
        for opp in sorted(tas['opportunities'], key=lambda x: x.get('composite_score', 0), reverse=True)[:10]:
            print(f"  - {opp['poi_name'] or opp['address'][:50]} | {opp['qualifying_rules']} | {opp['nearest_pharmacy_km']:.2f}km")

    return results_by_state


def generate_summary(results_by_state):
    """Generate a markdown summary of recalculated results."""
    lines = []
    lines.append("# Pharmacy Opportunity Recalculation Results")
    lines.append(f"\n**Date:** {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    lines.append(f"**Source:** Corrected ACPA Handbook rules (Jan 2024, V1.9)")
    lines.append("")
    lines.append("## Rule Corrections Applied")
    lines.append("")
    lines.append("| Rule | What Changed |")
    lines.append("|------|-------------|")
    lines.append("| Item 130 | Minor: added measurement note (mid-point of public access door) |")
    lines.append("| Item 131 | No changes |")
    lines.append("| Item 132 | **REWRITTEN**: Same town as existing pharmacy, 200m straight line from nearest, 10km route to others, 4 FTE GPs, 1-2 supermarkets >=2,500sqm combined |")
    lines.append("| Item 133 | **REWRITTEN**: INSIDE small shopping centre (GLA >=5,000sqm, supermarket >=2,500sqm, >=15 tenants), 500m distance (excl. large centres/hospitals) |")
    lines.append("| Item 134 | **REWRITTEN**: INSIDE large shopping centre (>=50 tenants), NO distance requirement, no existing pharmacy |")
    lines.append("| Item 134A | **REWRITTEN**: INSIDE large shopping centre WITH pharmacy, 100-199 tenants=max 1 existing, 200+ tenants=max 2 existing, NO distance |")
    lines.append("| Item 135 | **REWRITTEN**: INSIDE large PRIVATE hospital, >=150 patient admission capacity (not beds), NO distance |")
    lines.append("| Item 136 | Refined: 300m distance excluding pharmacies in large centres/hospitals, 8 FTE PBS prescribers (7 medical), 70hrs/week |")
    lines.append("")

    total_opps = sum(v['total'] for v in results_by_state.values())
    total_qualifying = sum(v['qualifying'] for v in results_by_state.values())
    lines.append(f"## Overall Summary")
    lines.append(f"")
    lines.append(f"- **Total opportunities reviewed:** {total_opps}")
    lines.append(f"- **Still qualifying under corrected rules:** {total_qualifying}")
    lines.append(f"- **No longer qualifying:** {total_opps - total_qualifying}")
    lines.append(f"")

    all_rules = defaultdict(int)
    for v in results_by_state.values():
        for rule, count in v['by_rule'].items():
            all_rules[rule] += count

    lines.append("### Opportunities by Rule (National)")
    lines.append("")
    lines.append("| Rule | Count | Description |")
    lines.append("|------|-------|-------------|")
    rule_desc = {
        'Item 130': '1.5km + supermarket/GP',
        'Item 131': '10km rural',
        'Item 132': 'Additional pharmacy in town',
        'Item 133': 'Small shopping centre',
        'Item 134': 'Large shopping centre (no pharmacy)',
        'Item 134A': 'Large shopping centre (with pharmacy)',
        'Item 135': 'Large private hospital',
        'Item 136': 'Large medical centre',
    }
    for rule in ['Item 130', 'Item 131', 'Item 132', 'Item 133', 'Item 134', 'Item 134A', 'Item 135', 'Item 136']:
        count = all_rules.get(rule, 0)
        desc = rule_desc.get(rule, '')
        lines.append(f"| {rule} | {count} | {desc} |")
    lines.append("")

    lines.append("## State-by-State Breakdown")
    lines.append("")

    # TAS first
    state_order = ['TAS'] + [s for s in sorted(results_by_state.keys()) if s != 'TAS']

    for state in state_order:
        if state not in results_by_state:
            continue
        data = results_by_state[state]
        
        is_tas = state == 'TAS'
        if is_tas:
            lines.append(f"### 🏝️ {state} (FOCUS STATE)")
        else:
            lines.append(f"### {state}")
        lines.append(f"")
        lines.append(f"- Total opportunities: {data['total']}")
        lines.append(f"- Qualifying: {data['qualifying']}")
        lines.append(f"- Eliminated: {data['total'] - data['qualifying']}")
        lines.append(f"")

        if data['by_rule']:
            lines.append("**By Rule:**")
            lines.append("")
            for rule in sorted(data['by_rule'].keys()):
                lines.append(f"- {rule}: {data['by_rule'][rule]}")
            lines.append("")

        if data['opportunities']:
            limit = 30 if is_tas else 15
            lines.append(f"**Top Opportunities (max {limit}):**")
            lines.append("")

            sorted_opps = sorted(data['opportunities'],
                                key=lambda x: x.get('composite_score', 0), reverse=True)

            for opp in sorted_opps[:limit]:
                changed_marker = " 🔄" if opp['changed'] else ""
                dist_str = f"{opp['nearest_pharmacy_km']:.2f}km" if opp['nearest_pharmacy_km'] else "?"
                name = opp['poi_name'] or opp['address'][:60]
                lines.append(
                    f"- **{name}** — "
                    f"`{opp['qualifying_rules']}`{changed_marker} — "
                    f"nearest pharmacy: {dist_str}"
                )
                if opp['changed']:
                    lines.append(f"  - _{opp['old_rules']}_ → `{opp['qualifying_rules']}`")

            if len(sorted_opps) > limit:
                lines.append(f"  - ... and {len(sorted_opps) - limit} more")
            lines.append("")

    return '\n'.join(lines)


if __name__ == '__main__':
    recalc_all()
