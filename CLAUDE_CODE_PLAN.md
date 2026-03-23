# PharmacyFinder — Claude Code Build Plan

## Goal

Build an Australia-wide tool that finds real sites where a new pharmacy can legally be established under the Pharmacy Location Rules. Not a theoretical exercise — find actual qualifying addresses across all 8 states/territories, rank them, and alert when new opportunities appear.

This is a national tool. The rules are federal, the PBS Suppliers data is national, and the best opportunity might be in any state. Every scanner must run across NSW, VIC, QLD, WA, SA, TAS, NT, and ACT.

## Context

- Codebase: `github.com/jaber006/PharmacyFinder`
- Handbook version: V1.9 January 2024 (located at `rules/pharmacy-location-rules-handbook-v1.9-jan2024.pdf` and `docs/pharmacy_location_rules_handbook.pdf`)
- Rules reference: `rules/RULES_EXACT.md` (verified against handbook)
- Current rules engine: `engine/rules/item_130.py` through `item_136.py`
- Current evaluator: `engine/evaluator.py` (3-pass: exclusion → rules → scoring)
- Current spatial engine: `engine/context.py` (grid-based index over SQLite)
- Tests: `tests/test_rules_engine.py` (91 tests)

---

## Phase 1: Fix the Rules Engine Bugs

These are verified discrepancies between the code and the V1.9 handbook. Fix them in `engine/rules/` and update tests in `tests/test_rules_engine.py` for each.

### Bug 1: Item 132 — Missing "same town" check

**Handbook reference:** Item 132(a)(i) — "the proposed premises are in the same town as an approved premises"  
**Glossary:** "same town" = same town name AND same postcode  
**Current code:** `engine/rules/item_132.py` uses a 5km radius (`TOWN_RADIUS_KM = 5.0`) to find GPs and supermarkets. Never validates that the candidate and nearest pharmacy share the same town name + postcode. The `town_id` field exists on the `Candidate` model but is never used.

**Fix:**
1. Add `suburb` and `postcode` fields to the pharmacies table (may already exist — check schema)
2. Before the distance checks in `check_item_132()`, verify the nearest pharmacy has the same town name + postcode as the candidate
3. For the GP and supermarket counts in 132(b), filter to same town+postcode, not just 5km radius
4. Add tests: candidate in same postcode as pharmacy → proceed; different postcode → FAIL even if 200m away

### Bug 2: Item 133 — Pharmacy exclusion not implemented

**Handbook reference:** Item 133(b) — "at least 500 m, in a straight line, from the nearest approved premises, **other than approved premises in a large shopping centre or private hospital**"  
**Current code:** `engine/rules/item_133.py` line 117 calls `context.nearest_pharmacy()` which returns the absolute nearest pharmacy with no exclusion filter.

**Fix:**
1. Add a method to `engine/context.py`: `nearest_pharmacy_excluding_complexes(lat, lon)` that skips pharmacies within 300m of a large shopping centre (≥50 tenants) or within 150m of a private hospital
2. In `check_item_133()`, use this filtered method for the 500m distance check
3. Add tests: pharmacy at 400m but inside a Westfield (large SC) → should be excluded, next nearest at 600m → PASS; pharmacy at 400m NOT in a complex → FAIL

### Bug 3: Item 136(c) — Two-branch distance rule collapsed into one

**Handbook reference:** Item 136(c) has two distinct sub-rules:
- **(c)(i)** If the medical centre IS in a shopping centre or hospital: ≥300m from **any** approved premises, except those in a **different** large SC or hospital
- **(c)(ii)** If the medical centre is NOT in a SC or hospital: ≥300m from the **nearest** approved premises, except those in a large SC or hospital

**Current code:** `engine/rules/item_136.py` treats both cases identically — just checks nearest pharmacy ≥300m.

**Fix:**
1. Determine if the medical centre is inside a shopping centre or hospital (check if any SC/hospital is within 50m of the medical centre coordinates)
2. If YES (c)(i): check that ALL pharmacies within 300m are in a **different** large SC or hospital. If any pharmacy within 300m is NOT in a different large SC/hospital → FAIL
3. If NO (c)(ii): find nearest pharmacy excluding those in large SCs or hospitals. If that nearest is <300m → FAIL
4. Add the same `nearest_pharmacy_excluding_complexes()` method from Bug 2 for case (c)(ii)
5. Add tests for both branches

### Bug 4: Item 136 — Missing "7 of 8 must be medical practitioners" validation

**Handbook reference:** Item 136(d) — "at least 8 full-time PBS prescribers, of which at least 7 PBS prescribers must be prescribing medical practitioners"  
**Current code:** `engine/rules/item_136.py` checks `total_fte >= 8` but never validates the 7-medical-practitioner sub-requirement.

**Fix:**
1. Add `medical_fte` field to medical_centres table (distinct from `total_fte` which includes dentists, optometrists, etc.)
2. In `check_item_136()`, after the total FTE check passes, verify `medical_fte >= 7`
3. If `medical_fte` data isn't available, note it in `evidence_needed` and reduce confidence but don't auto-fail
4. Add tests

### Bug 5: Item 136 — "Large medical centre" definition incomplete

**Handbook glossary:** A large medical centre requires: (c) "has one or more prescribing medical practitioners at the centre for at least 70 of the hours each week that the medical centre operates"  
**Current code:** Checks total operating hours ≥70 but not whether a GP is physically present for those 70 hours.

**Fix:**
1. Add `gp_available_hours` field to medical_centres table (hours per week a GP is available for appointments)
2. In `check_item_136()`, validate `gp_available_hours >= 70` as part of the "large medical centre" definition check
3. If data unavailable, flag in evidence_needed, reduce confidence
4. Note: overlapping GP hours don't stack — it's total hours the centre provides GP services, not combined individual hours

### Bug 6: Item 132(b)(ii) — Supermarket count not limited to 1 or 2

**Handbook reference:** Item 132(b)(ii) — "one or 2 supermarkets that have a combined total gross leasable area of at least 2,500 m²"  
**Current code:** `engine/rules/item_132.py` sums ALL supermarkets within 5km radius.

**Fix:**
1. After collecting nearby supermarkets, take the top 1 or top 2 by GLA (not all of them)
2. Check if the best 1, or best 2 combined, meet the 2,500 m² threshold
3. If 3+ supermarkets are needed to reach 2,500 m² → FAIL
4. Add tests

### Bug 7: Item 133(c) — Missing "no pharmacy in the centre" check

**Handbook reference:** Item 133(c) — "there are no approved premises in the small shopping centre"  
**Current code:** Relies on the 500m distance check but doesn't separately verify no pharmacy exists inside the specific shopping centre.

**Fix:**
1. In `check_item_133()`, add an explicit check: are there any pharmacies within ~100m of the shopping centre centroid?
2. This is separate from the 500m distance check — a pharmacy could be 600m from the candidate but still inside the same centre
3. If pharmacy found in centre → FAIL with reason "Item 133(c): existing pharmacy in the small shopping centre"
4. Add tests

---

## Phase 2: Simplify the Scanner for Real Opportunities

The current pipeline generates candidates from every POI nationally. This is slow and produces thousands of results, most of which are useless. Rebuild with focused scanners for ALL 8 new-pharmacy rules (Items 130-136), each with clear logic and national coverage. Leave no stone unturned.

### Task 1: Item 130 opportunity scanner (national)

**What to build:** Find locations across all of Australia where a new pharmacy could open based on distance from existing pharmacies + supermarket proximity.

**Logic:**
1. Load ALL pharmacies nationally (~6,000)
2. Load ALL supermarkets nationally (Woolworths, Coles, ALDI, IGA — ~8,000+)
3. For each supermarket:
   - Calculate geodesic distance to nearest pharmacy
   - If ≥1.5km: check supermarket GLA
     - If GLA ≥2,500 m²: PASS Item 130(b)(ii) — no GP needed
     - If GLA ≥1,000 m² and GP within 500m: PASS Item 130(b)(i)
   - Record as opportunity with confidence based on distance margin

**Data improvement:** Apply default GLA estimates by brand when actual GLA is unknown:
- Woolworths: 3,500 m² (default)
- Coles: 3,500 m²
- ALDI: 1,700 m²
- IGA: 800 m² (usually too small for Item 130(b)(ii), needs GP for (b)(i))

**Output:** National CSV + interactive map of qualifying supermarket locations, sorted by distance margin. Filterable by state.

### Task 2: Item 131 opportunity scanner (national)

**What to build:** Find rural/remote areas across Australia that are 10km+ by road from any pharmacy.

**Logic:**
1. Load ALL pharmacies nationally
2. For each pharmacy, find its nearest neighbour pharmacy
3. If nearest neighbour is >20km geodesic apart, compute the midpoint
4. Check if there's any actual town/settlement near the midpoint (use ABS population data)
5. For midpoints near populated areas, verify road distance to nearest pharmacy using OSRM
6. If road distance ≥10km and population >500: flag as opportunity

**Key improvement over current code:** Don't just use pharmacy midpoints. Also check known small towns (from ABS SAL boundaries) that might have lost their pharmacy. Cross-reference PBS Suppliers list monthly — any pharmacy that disappears from the list creates a potential Item 131 opportunity if the town is now 10km+ from the next one.

**Output:** National CSV + interactive map of qualifying rural gaps, with town name, state, population, and distance to nearest pharmacy.

### Task 3: Item 132 opportunity scanner (national)

**What to build:** Find one-pharmacy towns across Australia where a second pharmacy could be established because every other pharmacy is 10km+ away by road.

**Logic:**
1. Load ALL pharmacies nationally
2. Group pharmacies by town (suburb + postcode)
3. Find towns that have exactly 1 pharmacy
4. For each one-pharmacy town:
   - Check road distance from that pharmacy to every other pharmacy nationally
   - If ALL other pharmacies are ≥10km by road: this town qualifies for the 132(a)(iii) test
   - Then check: does the town have ≥4 FTE GPs? (from GP/medical centre data)
   - Then check: does the town have 1-2 supermarkets with combined GLA ≥2,500 m²?
   - A qualifying candidate must be ≥200m straight line from the existing pharmacy
5. Flag qualifying towns as Item 132 opportunities

**Key insight:** These are growing regional towns with one pharmacy that's probably overworked. The town has enough GPs and a decent supermarket but only one pharmacy serving the whole area. Examples: towns with 5,000-15,000 population, one pharmacy, a Woolworths or Coles, and a medical centre with 4+ GPs.

**Data requirements:** 
- Pharmacy suburb + postcode (for same-town matching)
- GP FTE data per town (not just per 5km radius)
- Supermarket GLA (use brand defaults if unknown)
- OSRM road distances (critical — the 10km is by road, not straight line)

**Output:** National CSV + interactive map of qualifying one-pharmacy towns, with town name, state, population, existing pharmacy name, GP count, supermarket details, and road distance to next nearest pharmacy.

### Task 4: Item 133 opportunity scanner (national)

**What to build:** Find small shopping centres across Australia where a new pharmacy could open.

**Logic:**
1. Load ALL shopping centres nationally
2. Filter to centres with: GLA ≥5,000 m², ≥15 tenants (but <50 — otherwise it's Item 134), supermarket ≥2,500 m² GLA, parking
3. For each qualifying small centre:
   - Check: any pharmacy already inside the centre? → skip (requirement c)
   - Find nearest pharmacy EXCLUDING those in large shopping centres or private hospitals
   - If that nearest non-excluded pharmacy is ≥500m → PASS
4. Flag as opportunity

**Data challenges:** Tenant counts and centre GLA are often unknown in OSM. Where data is missing:
- Estimate tenant count from OSM building footprint or known centre directories
- Flag as "data incomplete — verify tenant count and GLA" in output
- Default to including the centre with reduced confidence rather than excluding it

**Output:** National CSV + interactive map of qualifying small shopping centres, with centre name, state, GLA, tenant count, supermarket details, and distance to nearest non-excluded pharmacy.

### Task 5: Item 134 opportunity scanner (national)

**What to build:** Find large shopping centres with no existing pharmacy.

**Logic:**
1. Load ALL shopping centres nationally
2. Filter to centres with: GLA ≥5,000 m², ≥50 tenants, supermarket ≥2,500 m² GLA, parking
3. For each qualifying large centre:
   - Check: any pharmacy within 300m of centre centroid? → skip (likely already in centre)
   - No distance requirement from pharmacies outside the centre
4. Flag as opportunity

**Reality check:** Most large shopping centres in Australia already have a pharmacy. This scanner will likely find very few results, but those results are gold — a large centre without a pharmacy is a near-guaranteed approval. Focus on:
- Newly built centres not yet fully tenanted
- Centres where a pharmacy recently closed/relocated out
- Regional centres that have grown past 50 tenants recently

**Output:** National CSV + interactive map. Expect very few results — that's fine.

### Task 6: Item 134A opportunity scanner (national)

**What to build:** Find large shopping centres where an additional pharmacy is allowed based on tenant count.

**Logic:**
1. Load ALL shopping centres nationally
2. Filter to large centres (same criteria as 134) that ALREADY have pharmacy(ies) inside
3. For each:
   - Count existing pharmacies within the centre
   - Count tenants (or estimate)
   - Apply tiers:
     - 100-199 tenants + max 1 existing pharmacy → room for 2nd
     - ≥200 tenants + max 2 existing pharmacies → room for 3rd
   - Verify no pharmacy has relocated OUT of the centre in last 12 months (may need manual check)
4. Flag qualifying centres

**Data source:** Cross-reference centre tenant directories, pharmacy chain store locators (Chemist Warehouse, Priceline, TerryWhite), and the PBS Suppliers list to count existing pharmacies per centre.

**Output:** National CSV + interactive map of large centres with room for an additional pharmacy, with tenant count, existing pharmacy count, and tier classification.

### Task 7: Item 135 opportunity scanner (national)

**What to build:** Find large private hospitals with no pharmacy.

**Logic:**
1. Load ALL hospitals nationally
2. Filter to:
   - Private hospitals only (exclude public)
   - Admission capacity ≥150 patients (use bed_count as proxy)
3. For each qualifying private hospital:
   - Check: any pharmacy within 150m? → skip (likely inside hospital)
   - If no pharmacy inside → opportunity
4. Flag as opportunity

**Data enrichment:** 
- AIHW hospital data for bed counts and private/public classification
- State health department hospital registers for admission capacity (more accurate than bed count)
- Cross-reference with PBS Suppliers to check for existing hospital pharmacies

**Reality check:** Like Item 134, most large private hospitals already have a pharmacy. But hospital expansions, new private hospital builds, and hospital pharmacy closures create opportunities. Worth monitoring even if current results are few.

**Output:** National CSV + interactive map of qualifying private hospitals with no pharmacy, with hospital name, state, bed count, and private/public classification.

### Task 8: Item 136 opportunity scanner (national)

**What to build:** Find large medical centres across Australia that qualify for a pharmacy.

**Logic:**
1. Load ALL medical centres nationally from database
2. Filter to those with ≥8 GPs (or ≥10 headcount as proxy for 8 FTE, assuming ~0.8 FTE per GP)
3. For each qualifying centre:
   - Check: any pharmacy within 150m (inside the centre)? → skip
   - Check: nearest non-excluded pharmacy ≥300m? (using the fixed 136(c) logic)
   - Check: operating hours ≥70/week if data available
4. Flag as opportunity with confidence based on data completeness

**Data source improvement:** Enrich medical centre data from Hotdoc and Healthdirect APIs — these list individual GPs per practice, which gives a better headcount than OSM.

**Output:** National CSV + interactive map of qualifying medical centres, with GP count, estimated FTE, state, and distance to nearest pharmacy.

---

## Phase 3: Weekly Monitoring System

### Task 1: PBS Suppliers change detection

**What to build:** A script that downloads the current PBS Approved Suppliers list, compares to the previous snapshot, and flags:
- **New pharmacies** (could invalidate existing opportunities — a new pharmacy opening shrinks the 1.5km/10km gaps)
- **Removed pharmacies** (creates new opportunities — especially Item 131 in rural areas)

**Schedule:** Weekly cron job. Store snapshots in `output/_snapshots/`.

### Task 2: Medical centre GP count monitor

**What to build:** A script that checks Hotdoc/Healthdirect for GP counts at large medical centres on the watchlist.

**Trigger:** Alert when a centre crosses the 8-GP threshold (either up or down).

### Task 3: Weekly digest email

**What to build:** Combine all scanner outputs + change detection into a single markdown report.

**Format:**
```
## PharmacyFinder Weekly — {date}

### New Opportunities
- [Item 130] Near Woolworths, {suburb} {state} — nearest pharmacy {x}km (margin +{y}m)
- [Item 131] {town}, {state} — pharmacy closed, nearest now {x}km by road, pop {n}

### Watchlist Updates
- {medical centre name}: now {n} GPs listed on Hotdoc (was {m})
- {suburb}: new Coles DA approved — monitoring for construction completion

### Risk Alerts
- {pharmacy name} near {your opportunity}: new ACPA application detected
```

---

## Phase 4: Cleanup

1. Move the ~40 one-off `check_*.py`, `fix_*.py` scripts from root to `archive/dev_scripts/`
2. Remove the legacy class-based rules in `rules/item_*.py` — the `engine/rules/` versions are the source of truth
3. Add `.db` files to `.gitignore` (currently 4 database backups committed)
4. Remove `sys.path.insert(0, ...)` hacks from every file — use proper `__init__.py` imports

---

## Priority Order

1. **Phase 1 bugs** — get the rules right first, everything else depends on correctness
2. **Phase 2 Task 1** (Item 130 scanner) — highest volume of opportunities, best data quality
3. **Phase 2 Task 2** (Item 131 scanner) — rural gaps, especially NT, WA, QLD, SA, TAS
4. **Phase 2 Task 3** (Item 132 scanner) — one-pharmacy regional towns. Depends on OSRM road distances
5. **Phase 2 Task 4** (Item 133 scanner) — small shopping centres without a pharmacy
6. **Phase 2 Task 5** (Item 134 scanner) — large shopping centres with no pharmacy (rare but gold)
7. **Phase 2 Task 6** (Item 134A scanner) — large centres where tenant count allows additional pharmacy
8. **Phase 2 Task 7** (Item 135 scanner) — large private hospitals with no pharmacy (rare but worth monitoring)
9. **Phase 2 Task 8** (Item 136 scanner) — large medical centres. Depends on GP data enrichment from Hotdoc/Healthdirect
5. **Phase 3** — monitoring only matters once the scanners work
6. **Phase 4** — housekeeping, do whenever

## Scope

**National coverage:** All scanners MUST process all 8 states/territories in a single run. Use `--state` flag for filtering output, but always load national pharmacy data (a pharmacy in NSW could be the nearest pharmacy to a candidate in VIC near the border).

**State codes:** NSW, VIC, QLD, WA, SA, TAS, NT, ACT

**Data volume expectations:**
- Pharmacies: ~6,000 nationally
- Supermarkets: ~8,000+ nationally
- GPs/medical centres: ~40,000+ nationally
- Hospitals: ~1,300 nationally
- Shopping centres: ~2,000 nationally

SQLite handles this volume fine. PostGIS migration is optional and can come later for spatial query performance if needed.

---

## Testing

For every bug fix in Phase 1, add at least:
- 1 test for the PASS case
- 1 test for the FAIL case
- 1 test for the boundary/edge case

Run existing tests after each change to ensure no regressions:
```bash
python -m pytest tests/ -v
```

Current test count: 91. Target after Phase 1: ~110+.
