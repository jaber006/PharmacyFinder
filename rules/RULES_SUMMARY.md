# Pharmacy Location Rules - Verified Summary
## Source: Official ACPA Applicant's Handbook (January 2024, V1.9)

---

## Item 130: New pharmacy (at least 1.5 km)
**Distance:** ≥ 1.5km straight line from nearest approved premises
**Additional requirements:**
- Within 500m straight line, EITHER:
  - (i) ≥ 1 FTE prescribing medical practitioner + supermarket ≥ 1,000sqm GLA, OR
  - (ii) Supermarket ≥ 2,500sqm GLA
**Distance measurement:** Mid-point at ground level of public access door to mid-point of nearest public access door
**Restriction:** Must stay within 1km radius for 5 years

---

## Item 131: New pharmacy (at least 10 km)
**Distance:** ≥ 10km by SHORTEST LAWFUL ACCESS ROUTE (NOT straight line)
**Additional requirements:** None beyond general requirements
**Distance measurement:** Walking/driving route from centre of public entrance to centre of public entrance
**Restriction:** Cannot relocate from the town. Ever.

---

## Item 132: New ADDITIONAL pharmacy (at least 10 km)
**Distance:**
- (a)(ii) ≥ 200m straight line from nearest approved premises
- (a)(iii) ≥ 10km by shortest lawful access route from ANY OTHER approved premises (not the nearest one)
- (a)(i) Must be in the SAME TOWN as an existing approved premises
**Additional requirements:**
- ≥ 4 FTE prescribing medical practitioners in the same town
- 1 or 2 supermarkets with combined GLA ≥ 2,500sqm in the same town
**Restriction:** Cannot relocate from the town. Ever.

---

## Item 133: New pharmacy in a SMALL SHOPPING CENTRE
**Distance:** ≥ 500m straight line from nearest approved premises (EXCLUDING pharmacies in large shopping centres or private hospitals)
**Must be INSIDE a small shopping centre that has:**
- Single management
- GLA ≥ 5,000sqm (total centre)
- Supermarket with GLA ≥ 2,500sqm
- ≥ 15 other commercial establishments
- Customer parking
**Additional:** No approved premises already in the small shopping centre
**Restriction:** Must stay in same shopping centre for 10 years (unless exceptional circumstances)

### CODE ERRORS:
- ❌ Our code says supermarket ≥ 1,000sqm — WRONG, handbook says ≥ 2,500sqm
- ❌ Our code checks adjacency to any supermarket — WRONG, must be INSIDE a qualifying small shopping centre
- ❌ Our code doesn't check centre GLA (≥ 5,000sqm), number of tenants (≥ 15), or single management
- ❌ Our code only checks major chains — rule doesn't specify chain type for the centre
- ❌ Distance is 500m not 1.5km — but we had this wrong in the recalc

---

## Item 134: New pharmacy in a LARGE SHOPPING CENTRE (no existing pharmacy)
**Distance:** NO minimum distance from nearest pharmacy!
**Must be INSIDE a large shopping centre that has:**
- Single management
- GLA ≥ 5,000sqm (total centre)
- Supermarket with GLA ≥ 2,500sqm
- ≥ 50 other commercial establishments
- Customer parking
**Additional:** No approved premises already in the large shopping centre
**Restriction:** Must stay in same shopping centre for 10 years (unless exceptional circumstances)

### CODE ERRORS:
- ❌ Our code requires 5,000-15,000sqm — WRONG, should be ≥ 5,000sqm with ≥ 50 tenants
- ❌ Our code treats ≥ 15,000sqm as Item 132 territory — WRONG
- ❌ Our config GLA_THRESHOLDS has wrong separation between 132/134

---

## Item 134A: New ADDITIONAL pharmacy in a large shopping centre (with existing pharmacy)
**Distance:** NO minimum distance from nearest pharmacy
**Must be INSIDE a large shopping centre that has:**
- Same requirements as Item 134 PLUS:
- If 100-199 commercial establishments: only 1 existing pharmacy allowed
- If ≥ 200 commercial establishments: 1-2 existing pharmacies allowed
- No pharmacy has relocated OUT of the centre in last 12 months
**Restriction:** Must stay in same shopping centre for 10 years (unless exceptional circumstances)

---

## Item 135: New pharmacy in a LARGE PRIVATE HOSPITAL
**Distance:** NO minimum distance from nearest pharmacy!
**Must be INSIDE a large private hospital:**
- Can admit ≥ 150 patients at any one time (per licence/registration)
- "Admit" = admitted as private patient (including same-day), NOT outpatients
**Additional:** No approved pharmacy already in the hospital
**Restriction:** Must stay in same hospital (unless exceptional circumstances)

### CODE ERRORS:
- ⚠️ Config says 150 beds, docstring says 100 — handbook confirms 150 PATIENTS (not beds!)
- ❌ Code uses 300m adjacency check — WRONG, must be INSIDE the hospital, not just nearby
- ❌ Code checks bed_count — should be patient admission capacity per licence

---

## Item 136: New pharmacy in a LARGE MEDICAL CENTRE
**Distance:** ≥ 300m straight line from nearest approved premises (EXCLUDING pharmacies in large shopping centres or private hospitals)
**Must be INSIDE a large medical centre that has:**
- Single management
- Opens ≥ 70 hours per week
- GP services available ≥ 70 hours per week
**Additional requirements:**
- ≥ 8 FTE PBS prescribers (of which ≥ 7 must be medical practitioners)
- During 2 months before application AND until consideration day
- No approved pharmacy already in the medical centre
- Applicant must make reasonable attempts to match operating hours to patient needs
**Restriction:** Must stay in same medical centre (unless exceptional circumstances)

### CODE STATUS: ✅ Mostly correct
- ✅ 300m distance — correct
- ✅ 8 FTE prescribers check — correct
- ✅ No pharmacy in centre check — correct
- ⚠️ Uses headcount × 0.8 as FTE proxy — reasonable but approximate
- ⚠️ 70hrs/week check exists but data quality varies

---

## KEY CORRECTIONS NEEDED IN CODEBASE:

### Critical:
1. **Item 133 is completely wrong** — must be inside a qualifying small shopping centre (≥5,000sqm, ≥2,500sqm supermarket, ≥15 tenants), not just near any 1,000sqm supermarket
2. **Item 132 is wrong** — it's for towns that already have ONE pharmacy, and the SECOND nearest must be 10km+ by road. Not just "major shopping centre 15,000sqm"
3. **Item 134 is wrong** — it's for large shopping centres with ≥50 commercial establishments, no existing pharmacy. Not "5,000-15,000sqm"
4. **Item 135 distance** — no minimum distance required, but must be INSIDE the hospital

### Important:
5. **Item 133 distance** — 500m from nearest pharmacy (not 1.5km), excluding pharmacies in large shopping centres or hospitals
6. **Item 136 distance** — 300m (our code is correct), excluding pharmacies in large shopping centres or hospitals
7. **Distance exclusions** — Items 133 and 136 both exclude pharmacies that are in large shopping centres or private hospitals from the distance calculation

### Minor:
8. **Item 135 capacity** — 150 patients (admission capacity per licence), not 100 beds
9. **Item 130 measurement** — specifically from mid-point of public access door at ground level
