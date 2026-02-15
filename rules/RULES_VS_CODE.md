# Rules vs Code — Side-by-Side Verification
**Source: ACPA Applicant's Handbook, January 2024 (V1.9)**

Review each rule. Confirm "Handbook Says" matches "Code Checks". Flag anything wrong.

---

## Item 130 — New Pharmacy (1.5km)

| # | Handbook Says | Code Checks | ✓/✗ |
|---|--------------|-------------|-----|
| a | Proposed premises ≥ 1.5km **straight line** from nearest pharmacy | Calculates straight-line distance to nearest pharmacy, checks ≥ 1.5km | |
| b(i) | Within 500m: ≥ 1 FTE prescribing GP **AND** supermarket ≥ 1,000sqm GLA | Searches GPs within 500m for ≥ 1 FTE, searches supermarkets within 500m for ≥ 1,000sqm | |
| b(ii) | **OR** within 500m: supermarket ≥ 2,500sqm GLA | Checks supermarkets within 500m for ≥ 2,500sqm GLA | |
| note | Distance = mid-point of public access door at ground level | Noted in comments, uses lat/lng coordinates as proxy | |
| restriction | Must stay within 1km radius for 5 years | Not enforced in code (informational) | |

---

## Item 131 — New Pharmacy, Rural (10km)

| # | Handbook Says | Code Checks | ✓/✗ |
|---|--------------|-------------|-----|
| a | Proposed premises ≥ 10km by **shortest lawful access route** from nearest pharmacy | Uses OSRM driving distance for borderline (7-15km straight line). If straight line ≥ 15km, estimates route as ×1.3 | |
| note | No supermarket or GP requirements | No supermarket/GP checks — correct | |
| note | Route = shortest lawful access route | Uses OSRM (road network), not straight line | |

---

## Item 132 — Second Pharmacy in a Town

| # | Handbook Says | Code Checks | ✓/✗ |
|---|--------------|-------------|-----|
| a(i) | Proposed premises in the **same town** as an existing pharmacy | Checks nearest pharmacy is within 5km AND optionally matches suburb name | |
| a(ii) | ≥ 200m **straight line** from nearest pharmacy | Checks straight-line distance ≥ 0.2km | |
| a(iii) | ≥ 10km by **road** from any OTHER pharmacy (not the one in town) | Finds second-nearest pharmacy, checks ≥ 10km. Uses OSRM for 7-15km range, estimates ×1.3 for longer | |
| b(i) | ≥ 4 FTE prescribing medical practitioners in same town | Searches GPs within 3km radius, sums FTE. Falls back to headcount × 0.8 | |
| b(ii) | 1 or 2 supermarkets with **combined** GLA ≥ 2,500sqm in same town | Finds top 2 supermarkets within 3km, sums their GLA, checks ≥ 2,500sqm | |
| note | "Same town" = same locality name AND postcode | Code uses distance proxy (5km) + suburb name match. Not postcode. | |
| restriction | Cannot relocate from the town. Ever. | Not enforced in code (informational) | |

---

## Item 133 — Small Shopping Centre

| # | Handbook Says | Code Checks | ✓/✗ |
|---|--------------|-------------|-----|
| a | Proposed premises **inside** a small shopping centre | Checks if opportunity is within 200m of a shopping centre's coordinates | |
| def | Single management | Not verified in code — flagged for manual check | |
| def | Centre GLA ≥ 5,000sqm | Checks centre GLA ≥ 5,000sqm | |
| def | Contains supermarket with GLA ≥ 2,500sqm | Checks for supermarket within 300m of centre with GLA ≥ 2,500sqm, or falls back to listed supermarkets | |
| def | ≥ 15 commercial establishments | Checks estimated_tenants ≥ 15 | |
| def | Customer parking | Not verified in code — flagged for manual check | |
| b | ≥ 500m straight line from nearest pharmacy, **EXCLUDING** pharmacies in large shopping centres or private hospitals | Calculates distance to each pharmacy, skips any within 150m of a large centre (≥50 tenants) or hospital. Checks remaining ≥ 500m | |
| c | No existing pharmacy in the shopping centre | Checks no pharmacy within 150m of centre coordinates | |
| restriction | Must stay in same centre for 10 years | Not enforced (informational) | |

---

## Item 134 — Large Shopping Centre (No Pharmacy)

| # | Handbook Says | Code Checks | ✓/✗ |
|---|--------------|-------------|-----|
| a | Proposed premises **inside** a large shopping centre | Checks within 200m of centre coordinates | |
| def | Single management | Not verified — flagged for manual check | |
| def | Centre GLA ≥ 5,000sqm | Checks centre GLA ≥ 5,000sqm | |
| def | Supermarket GLA ≥ 2,500sqm | Checks supermarkets within 300m of centre, or listed supermarkets | |
| def | ≥ 50 commercial establishments | Checks estimated_tenants ≥ 50 | |
| def | Customer parking | Not verified — flagged for manual check | |
| b | **No** existing pharmacy in the centre | Checks no pharmacy within 150m of centre | |
| note | **NO** minimum distance from nearest pharmacy | No distance check — correct | |
| restriction | Must stay in same centre for 10 years | Not enforced (informational) | |

---

## Item 134A — Large Shopping Centre (With Pharmacy)

| # | Handbook Says | Code Checks | ✓/✗ |
|---|--------------|-------------|-----|
| a | Proposed premises **inside** a large shopping centre | Same as 134: within 200m of centre | |
| def | Same "large shopping centre" definition as Item 134 | Same checks: GLA ≥ 5,000, tenants ≥ 50, supermarket ≥ 2,500sqm | |
| b(i) | 100-199 tenants → max **1** existing pharmacy in centre | Counts pharmacies within 150m of centre, checks = 1 if 100-199 tenants | |
| b(ii) | ≥ 200 tenants → max **2** existing pharmacies in centre | Checks 1-2 pharmacies if ≥ 200 tenants | |
| c | No pharmacy relocated **out** of centre in last 12 months | Not verified in code — flagged for manual check | |
| note | **NO** minimum distance from nearest pharmacy | No distance check — correct | |
| restriction | Must stay in same centre for 10 years | Not enforced (informational) | |

---

## Item 135 — Large Private Hospital

| # | Handbook Says | Code Checks | ✓/✗ |
|---|--------------|-------------|-----|
| a | Proposed premises **inside** a large private hospital | Checks within 200m of hospital coordinates | |
| def | **Private** hospital (not public) | Checks hospital_type contains "private" or is unknown. Rejects if "public" | |
| def | Can admit ≥ 150 patients at any one time (per licence) | Checks bed_count ≥ 150 (bed count used as proxy for admission capacity) | |
| def | "Admit" = admitted for treatment as private patient (inpatient + same-day, NOT outpatients) | Noted in comments, uses bed_count as proxy | |
| b | No existing pharmacy in the hospital | Checks no pharmacy within 150m of hospital | |
| note | **NO** minimum distance from nearest pharmacy | No distance check — correct | |

---

## Item 136 — Large Medical Centre

| # | Handbook Says | Code Checks | ✓/✗ |
|---|--------------|-------------|-----|
| a | Proposed premises **inside** a large medical centre | Checks within 100m of medical centre, or within 200m of GP cluster | |
| def | Single management | Not verified — flagged for manual check | |
| def | Operates ≥ 70 hours/week | Checks hours_per_week if available, flags for verification if not | |
| def | ≥ 1 prescribing GP at centre for ≥ 70 of operating hours | Noted in comments, not separately verified | |
| b | No existing pharmacy in the medical centre | Checks no pharmacy within 100m of centre | |
| c | ≥ 300m straight line from nearest pharmacy, **EXCLUDING** pharmacies in large shopping centres or private hospitals | Calculates distance to each pharmacy, excludes those within 150m of large centres (≥50 tenants) or private hospitals. Checks remaining ≥ 300m | |
| d | ≥ 8 FTE PBS prescribers (≥ 7 must be medical practitioners) | Checks total FTE or GP headcount ≥ 8. Falls back to headcount × 0.8 | |
| note | 8 FTE must be maintained for 2 months before application AND until consideration | Informational — not time-checked in code | |

---

## Known Limitations (applies to all rules)

1. **"Inside" a centre/hospital** — Code uses 150-200m proximity to centre coordinates. Not a true "inside" check. Manual verification always needed.
2. **Single management** — Cannot be verified from data. Always flagged.
3. **Customer parking** — Cannot be verified from data. Always flagged.
4. **FTE data** — Often estimated from headcount × 0.8. Actual FTE requires manual verification.
5. **Supermarket GLA** — Often estimated, not always from official sources. Manual verification recommended.
6. **"Same town" (Item 132)** — Uses distance proxy (5km) + suburb name. Official definition is locality + postcode.
7. **Bed count vs admission capacity (Item 135)** — Code uses bed_count as proxy for "admit ≥ 150 patients at any one time per licence."
8. **Pharmacy relocation history (Item 134A)** — Cannot verify if a pharmacy relocated out in the last 12 months.
9. **70 hrs/week GP presence (Item 136)** — Code checks centre hours but cannot verify a GP is present for all 70 hours.

---

**Instructions:** Go through each ✓/✗ column. If the "Code Checks" column correctly implements the "Handbook Says" column, mark ✓. If not, mark ✗ and note why.
