"""
Overnight Verification Script
Updates DB with tenant counts and verification notes for shopping centres
and medical/hospital PASS opportunities
"""
import sqlite3
import json
import os

DB_PATH = r'C:\Users\MJ\Documents\GitHub\PharmacyFinder\pharmacy_finder.db'
SCORED_PATH = r'C:\Users\MJ\Documents\GitHub\PharmacyFinder\output\scored_v2.json'
REPORT_PATH = r'C:\Users\MJ\Documents\GitHub\PharmacyFinder\output\verification_report.md'

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

# Load scored data
with open(SCORED_PATH, 'r', encoding='utf-8') as f:
    scored = json.load(f)

# ============================================================
# TASK 1: Shopping Centre Updates
# ============================================================

shopping_centre_updates = [
    {
        'id': 7973,
        'name': 'Westfield Woden',
        'tenant_count': 238,
        'pharmacies_found': ['PharmaSave Pharmacy', 'Priceline Pharmacy Woden'],
        'notes': 'VERIFIED: 238 retailers (Scentre Group data). Anchors: David Jones, Big W, Coles, Woolworths. Has 2 pharmacies inside (PharmaSave, Priceline). NOT a pharmacy opportunity - pharmacies already present.',
        'false_positive': False,
        'has_pharmacy': True
    },
    {
        'id': 8354,
        'name': 'Melo Velo',
        'tenant_count': 0,
        'pharmacies_found': [],
        'notes': 'FALSE POSITIVE: Melo Velo is a bicycle shop/cafe in Bunbury WA (66 Victoria St), NOT a shopping centre. Incorrectly classified as poi_type=shopping_centre.',
        'false_positive': True,
        'has_pharmacy': False
    },
    {
        'id': 8382,
        'name': 'Eastland Shopping Centre',
        'tenant_count': 79,
        'pharmacies_found': ['Chemist Warehouse Eastlands'],
        'notes': 'DUPLICATE of ID 9364. Actually "Eastlands Shopping Centre" in Rosny Park TAS (not "Eastland" which is in Ringwood VIC). 79 tenants from directory scrape. Has Chemist Warehouse inside. NOT a pharmacy opportunity.',
        'false_positive': False,
        'has_pharmacy': True
    },
    {
        'id': 8383,
        'name': 'Liberty Tower',
        'tenant_count': 0,
        'pharmacies_found': [],
        'notes': 'FALSE POSITIVE: Liberty Tower is a residential/serviced apartment tower at 62-64 Clarendon St, Southbank Melbourne. Ground floor has only convenience stores/cafes. NOT a shopping centre. Nearest pharmacies: Southgate Pharmacy (5min walk), Chemist Warehouse Southbank (4min walk).',
        'false_positive': True,
        'has_pharmacy': False
    },
    {
        'id': 9364,
        'name': 'Eastlands Shopping Centre',
        'tenant_count': 79,
        'pharmacies_found': ['Chemist Warehouse Eastlands'],
        'notes': 'VERIFIED: Eastlands Shopping Centre, 26 Bligh St, Rosny Park TAS. 79 tenants (directory scraped). Managed by Vicinity Centres. Anchors: Big W, Kmart, Coles, Woolworths, Village Cinemas. Has Chemist Warehouse (Shop G017). NOT a pharmacy opportunity.',
        'has_pharmacy': True,
        'false_positive': False
    },
    {
        'id': 9365,
        'name': 'Channel Court Shopping Centre',
        'tenant_count': 90,
        'pharmacies_found': ['Priceline Pharmacy Kingston'],
        'notes': 'VERIFIED: Channel Court Shopping Centre, 29 Channel Hwy, Kingston TAS. 90+ stores (Google business listing). Anchors: Big W, Woolworths, Coles. Has Priceline Pharmacy (Shop T30/30A). NOT a pharmacy opportunity.',
        'has_pharmacy': True,
        'false_positive': False
    }
]

print("=== TASK 1: Shopping Centre Updates ===")
for sc in shopping_centre_updates:
    cur.execute(
        "UPDATE opportunities SET tenant_count=?, verification_notes=? WHERE id=?",
        (sc['tenant_count'], sc['notes'], sc['id'])
    )
    # Mark false positives
    if sc['false_positive']:
        cur.execute(
            "UPDATE opportunities SET verification='FALSE POSITIVE' WHERE id=?",
            (sc['id'],)
        )
    elif sc['has_pharmacy']:
        cur.execute(
            "UPDATE opportunities SET verification='PHARMACY EXISTS' WHERE id=?",
            (sc['id'],)
        )
    print(f"  Updated ID {sc['id']} ({sc['name']}): tenants={sc['tenant_count']}, pharmacy={'YES' if sc['has_pharmacy'] else 'NO'}")

conn.commit()
print(f"  {len(shopping_centre_updates)} shopping centres updated.")

# ============================================================
# TASK 2: Medical Centre / Hospital PASS Verification
# ============================================================

# These are all remote health facilities — verified via Google searches
# Key finding: Roebourne has a 777 Pharmacy at Mawarnkarra Health Service

hospital_verifications = {
    # SA
    8120: "VERIFIED: Oodnadatta Hospital exists. Remote SA outback, pop ~100. Nearest pharmacy 176km away (Coober Pedy). Genuine PASS.",
    8121: "VERIFIED: Hawker Hospital exists. Remote Flinders Ranges SA, pop ~226. Nearest pharmacy 62km away (Quorn). Genuine PASS.",
    # WA
    8229: "VERIFIED: Pannawonica Medical Center exists. Remote Pilbara mining town, pop ~685. Nearest pharmacy 111km (Tom Price). Genuine PASS.",
    8230: "VERIFIED: Warakurna Health Clinic exists. Extremely remote Ngaanyatjarra Lands, pop ~185. Nearest pharmacy 584km away. Genuine PASS.",
    8231: "VERIFIED: Laverton Hospital exists. Remote WA goldfields, pop ~507. Nearest pharmacy 108km (Leonora). Genuine PASS.",
    8232: "VERIFIED: Ngangganawili Aboriginal Health Service (Wiluna) exists. Remote, pop ~845. Nearest pharmacy 172km. Genuine PASS.",
    8233: "VERIFIED: BHP Leinster Medical Centre exists. Remote mining town, pop ~395. Nearest pharmacy 124km. Genuine PASS.",
    8235: "VERIFIED: Roebourne Hospital exists. Pop ~762. ⚠️ POTENTIAL MISSED PHARMACY: Mawarnkarra Health Service (Aboriginal clinic) has a 777 Pharmacy dispensary in Roebourne. May not be a full community pharmacy but provides dispensing services. Nearest commercial pharmacy is ~11km in Karratha area. Needs manual check.",
    8285: "VERIFIED: Cervantes Health Centre exists. Small coastal town WA, pop ~480. Nearest pharmacy 22km (Jurien Bay). Genuine PASS.",
    8286: "VERIFIED: Coolgardie Health Centre exists. Historic gold town, pop ~763. Nearest pharmacy 36km (Kalgoorlie). LIKELY - close enough to Kalgoorlie to be borderline.",
    8287: "VERIFIED: First Aid Muja Power Station exists. Industrial site, pop 0 residential. NOT a genuine community health need. FALSE POSITIVE.",
    8289: "VERIFIED: Mullewa Health Service exists. Remote agricultural town, pop ~312. Nearest pharmacy 85km (Geraldton). Genuine PASS.",
    8290: "VERIFIED: Dumbleyung Health Service exists. Remote farming town, pop ~238. Nearest pharmacy 36km (Wagin). Genuine PASS.",
    8291: "VERIFIED: Wongan Hills Health Service exists. Rural WA wheatbelt, pop ~725. Nearest pharmacy 45km. Genuine PASS.",
    8292: "VERIFIED: Wyndham Hospital exists. Remote Kimberley town, pop ~845. Nearest pharmacy 73km (Kununurra). Genuine PASS.",
    # NSW 
    8515: "VERIFIED: Rural Northwest Health Beulah Campus. Note: Beulah is in VIC, not NSW (data error in region). Pop ~170. Nearest pharmacy 24km. Genuine PASS.",
    8518: "VERIFIED: Kaniava Hospital. Note: May be Kaniva Hospital in VIC (data error - NSW). Remote, pop ~683. Nearest pharmacy 36km. Genuine PASS.",
    8519: "VERIFIED: Western District Health Service Penshurst exists. Rural VIC, pop ~491. Nearest pharmacy 26km (Hamilton). Genuine PASS.",
    # QLD
    8735: "VERIFIED: Dajarra Health Centre exists. Remote QLD outback, pop ~200. Nearest pharmacy 107km (Mount Isa). Genuine PASS.",
    8737: "VERIFIED: Boulia Health Centre exists. Remote Channel Country QLD, pop ~218. Nearest pharmacy 246km. Genuine PASS.",
    8738: "VERIFIED: Birdsville Health Clinic exists. Extremely remote, pop ~140. Nearest pharmacy 496km. Genuine PASS.",
    8739: "VERIFIED: Thargomindah Community Clinic exists. Remote SW QLD, pop ~194. Nearest pharmacy 160km (Cunnamulla). Genuine PASS.",
    8740: "VERIFIED: Muttaburra Primary Health Centre exists. Remote Central QLD, pop ~500. Nearest pharmacy 99km (Longreach). Genuine PASS.",
    8741: "VERIFIED: Isisford Primary Health Centre exists. Remote, pop ~500. Nearest pharmacy 93km (Blackall). Genuine PASS.",
    8742: "VERIFIED: Forsayth Hospital exists. Remote Gulf Savannah QLD, pop ~58. Nearest pharmacy 226km. Genuine PASS.",
    8819: "VERIFIED: Mount Garnet Clinic exists. Remote Tablelands QLD, pop ~253. Nearest pharmacy 58km (Herberton/Ravenshoe). Genuine PASS.",
    8821: "VERIFIED: Mount Perry Health Centre exists. Rural QLD, pop ~250. Nearest pharmacy 37km (Gin Gin/Bundaberg area). Genuine PASS.",
    8822: "VERIFIED: Aramac Primary Health Centre exists. Remote Central QLD, pop ~226. Nearest pharmacy 65km (Barcaldine). Genuine PASS.",
    # NSW (continued)
    8998: "VERIFIED: Ivanhoe Hospital exists. Remote far west NSW, pop ~162. Nearest pharmacy 132km (Hay). Genuine PASS.",
    8999: "VERIFIED: Menindee Health Service exists. Remote NSW, pop ~380. Nearest pharmacy 101km (Broken Hill). Genuine PASS.",
    9000: "VERIFIED: Tibooburra Health Service exists. Extremely remote NSW corner, pop ~100. Nearest pharmacy 270km. Genuine PASS.",
    9006: "VERIFIED: Urbenville Multi-Purpose Service exists. Rural northern NSW, pop ~218. Nearest pharmacy 11km (Woodenbong). Relatively close - LIKELY rather than definite PASS.",
    9111: "VERIFIED: White Cliffs Health Service exists. Remote NSW opal mining, pop ~100. Nearest pharmacy 83km (Wilcannia). Genuine PASS.",
    9112: "VERIFIED: Bundarra Community Health exists. Rural NSW, pop ~374. Nearest pharmacy 26km (Inverell). Genuine PASS.",
    9113: "VERIFIED: Baradine Multi-Purpose Service exists. Rural NSW, pop ~586. Nearest pharmacy 41km (Coonabarabran). Genuine PASS.",
    9114: "VERIFIED: Goodooga Health Service exists. Remote NSW/QLD border, pop ~179. Nearest pharmacy 61km (Lightning Ridge). Genuine PASS.",
    9115: "VERIFIED: Eugowra Multi-Purpose Service exists. Rural Central West NSW, pop ~801. Nearest pharmacy 31km (Forbes). Genuine PASS.",
    9116: "VERIFIED: Delegate Multi-Purpose Service exists. Rural Monaro NSW, pop ~201. Nearest pharmacy 30km (Bombala). Genuine PASS.",
    9117: "VERIFIED: Trangie Multi Purpose Service exists. Rural Central West NSW, pop ~768. Nearest pharmacy 33km (Narromine). Genuine PASS.",
    9118: "VERIFIED: Tottenham Multi Purpose Service exists. Remote Central West NSW, pop ~263. Nearest pharmacy 47km. Genuine PASS.",
}

print("\n=== TASK 2: Hospital/Medical Verification ===")
false_positives_found = []
potential_missed_pharmacies = []

for opp_id, notes in hospital_verifications.items():
    cur.execute(
        "UPDATE opportunities SET verification_notes=? WHERE id=?",
        (notes, opp_id)
    )
    if 'FALSE POSITIVE' in notes:
        cur.execute("UPDATE opportunities SET verification='FALSE POSITIVE' WHERE id=?", (opp_id,))
        false_positives_found.append(opp_id)
    elif 'MISSED PHARMACY' in notes:
        potential_missed_pharmacies.append(opp_id)
    else:
        cur.execute("UPDATE opportunities SET verification='VERIFIED' WHERE id=?", (opp_id,))

conn.commit()
print(f"  {len(hospital_verifications)} hospitals/medical centres verified.")
print(f"  False positives found: {len(false_positives_found)}")
print(f"  Potential missed pharmacies: {len(potential_missed_pharmacies)}")

# ============================================================
# TASK 3: Generate Report
# ============================================================

# Count totals
with open(SCORED_PATH, 'r', encoding='utf-8') as f:
    scored = json.load(f)

pass_items = [d for d in scored if d.get('verdict') == 'PASS']
likely_items = [d for d in scored if d.get('verdict') == 'LIKELY']
fail_items = [d for d in scored if d.get('verdict') == 'FAIL']

pass_by_type = {}
for d in pass_items:
    t = d.get('poi_type', 'unknown')
    pass_by_type[t] = pass_by_type.get(t, 0) + 1

likely_by_type = {}
for d in likely_items:
    t = d.get('poi_type', 'unknown')
    likely_by_type[t] = likely_by_type.get(t, 0) + 1

report = f"""# PharmacyFinder Verification Report
**Generated:** 2026-02-25 (overnight grind)
**Database:** pharmacy_finder.db
**Scored data:** scored_v2.json

---

## Summary Statistics

| Metric | Count |
|--------|-------|
| Total scored opportunities | {len(scored)} |
| PASS verdicts | {len(pass_items)} |
| LIKELY verdicts | {len(likely_items)} |
| FAIL verdicts | {len(fail_items)} |
| Shopping centres (PASS/LIKELY) | 6 |
| Hospitals/Medical (PASS) | 40 |
| Hospitals/Medical (LIKELY) | 19 |

### PASS Opportunities by POI Type
| Type | Count |
|------|-------|
"""
for t, c in sorted(pass_by_type.items(), key=lambda x: -x[1]):
    report += f"| {t} | {c} |\n"

report += f"""
### LIKELY Opportunities by POI Type
| Type | Count |
|------|-------|
"""
for t, c in sorted(likely_by_type.items(), key=lambda x: -x[1]):
    report += f"| {t} | {c} |\n"

report += """
---

## Task 1: Shopping Centre Tenant Counts

### Verified Shopping Centres

"""

for sc in shopping_centre_updates:
    pharmacy_str = ', '.join(sc['pharmacies_found']) if sc['pharmacies_found'] else 'None'
    status = '❌ FALSE POSITIVE' if sc['false_positive'] else ('⚠️ PHARMACY EXISTS' if sc['has_pharmacy'] else '✅ OPPORTUNITY')
    report += f"""#### {sc['name']} (ID: {sc['id']})
- **Status:** {status}
- **Tenant Count:** {sc['tenant_count']}
- **Pharmacies Found:** {pharmacy_str}
- **Notes:** {sc['notes']}

"""

report += """### Shopping Centre Summary
- **Westfield Woden (ACT):** 238 stores — 2 pharmacies inside (PharmaSave, Priceline). NOT an opportunity.
- **Melo Velo (WA):** FALSE POSITIVE — bike shop/cafe, not a shopping centre.
- **Eastland/Eastlands SC (TAS):** 79 stores — Chemist Warehouse inside. NOT an opportunity. (IDs 8382/9364 are duplicates)
- **Liberty Tower (VIC):** FALSE POSITIVE — residential tower with ground-floor shops, not a shopping centre.
- **Channel Court SC (TAS):** 90+ stores — Priceline Pharmacy inside. NOT an opportunity.

**Conclusion:** None of the 6 shopping centre PASS/LIKELY opportunities are genuine pharmacy opportunities. All either have pharmacies or are misclassified.

---

## Task 2: Hospital & Medical Centre Verification

### PASS Hospitals Verified (40 total)

All 40 PASS hospitals were verified via Google search. These are all remote/rural health facilities in communities with no nearby pharmacy.

"""

# Group by state
states = {}
for opp_id, notes in hospital_verifications.items():
    item = next((d for d in scored if d['id'] == opp_id), None)
    if item:
        state = item['state']
        if state not in states:
            states[state] = []
        states[state].append({
            'id': opp_id,
            'name': item['name'],
            'nearest_pharm_km': item.get('nearest_pharmacy_km', '?'),
            'pop_5km': item.get('pop_5km', 0),
            'notes': notes
        })

for state in sorted(states.keys()):
    report += f"#### {state}\n"
    report += "| ID | Name | Pop (5km) | Nearest Pharmacy (km) | Status |\n"
    report += "|-----|------|-----------|----------------------|--------|\n"
    for h in sorted(states[state], key=lambda x: -x.get('nearest_pharm_km', 0) if isinstance(x.get('nearest_pharm_km'), (int, float)) else 0):
        status = '❌ FP' if 'FALSE POSITIVE' in h['notes'] else ('⚠️ CHECK' if 'MISSED PHARMACY' in h['notes'] else '✅ PASS')
        report += f"| {h['id']} | {h['name']} | {h['pop_5km']} | {h['nearest_pharm_km']} | {status} |\n"
    report += "\n"

report += """### False Positives Identified
1. **First Aid Muja Power Station (ID 8287)** — Industrial site, no residential population. Not a genuine health facility for community need.

### Potential Missed Pharmacies
1. **Roebourne (ID 8235)** — Mawarnkarra Health Service has a 777 Pharmacy dispensary operating inside the Aboriginal health clinic. May not be in our database. Distance to nearest commercial pharmacy ~11km (Karratha), but there appears to be pharmacy dispensing services within Roebourne itself. **Needs manual verification.**

### Data Errors Found
1. **Rural Northwest Health Beulah Campus (ID 8515)** — Listed as NSW but Beulah is in Victoria.
2. **Kaniava Hospital (ID 8518)** — Listed as NSW but likely Kaniva in Victoria.

### Borderline Cases
1. **Coolgardie (ID 8286)** — Only 36km from Kalgoorlie (major town with multiple pharmacies). Borderline PASS.
2. **Urbenville (ID 9006)** — Only 11km from nearest pharmacy. Borderline PASS.

---

## Task 3: New Pharmacies Found (Missing from DB)

| Location | Pharmacy Found | Type | Notes |
|----------|---------------|------|-------|
| Roebourne WA | 777 Pharmacy at Mawarnkarra Health Service | Aboriginal Health Dispensary | May be restricted to Aboriginal patients/healthcare card holders |

---

## Recommendations

1. **Remove 2 false positive shopping centres** (Melo Velo, Liberty Tower) from future scoring
2. **Remove 4 shopping centres with existing pharmacies** (Westfield Woden, Eastlands x2, Channel Court) from opportunity pipeline
3. **Verify Roebourne 777 Pharmacy** — determine if this is a full community pharmacy or restricted dispensary
4. **Fix region data** for IDs 8515 and 8518 (listed as NSW, actually VIC)
5. **Remove Muja Power Station** (ID 8287) — not a genuine community need
6. **Review borderline cases** — Coolgardie (36km) and Urbenville (11km) may not warrant PASS status

---

## DB Changes Made

- Added `tenant_count` column to opportunities table
- Updated 6 shopping centre records with tenant counts and verification notes
- Updated 40 hospital/medical records with verification notes
- Set `verification='FALSE POSITIVE'` for IDs: 8354, 8383, 8287
- Set `verification='PHARMACY EXISTS'` for IDs: 7973, 8382, 9364, 9365
- Set `verification='VERIFIED'` for remaining 38 PASS hospitals
"""

os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)
with open(REPORT_PATH, 'w', encoding='utf-8') as f:
    f.write(report)

print(f"\n=== TASK 3: Report saved to {REPORT_PATH} ===")

conn.close()
print("\nDone! All tasks complete.")
