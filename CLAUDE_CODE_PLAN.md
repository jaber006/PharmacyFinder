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

The current pipeline generates candidates from every POI nationally. This is slow and produces thousands of results, most of which are useless. Refocus on the 3 rules that actually produce opportunities.

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

### Task 3: Item 136 opportunity scanner (national)

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
2. **Phase 2 Task 1** (Item 130 national scanner) — this is where most opportunities are, and supermarket data is the most reliable
3. **Phase 2 Task 2** (Item 131 national scanner) — rural gaps across all states, especially NT, WA, QLD, SA, TAS
4. **Phase 2 Task 3** (Item 136 national scanner) — depends on medical centre data quality, enrichment from Hotdoc/Healthdirect is key
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
