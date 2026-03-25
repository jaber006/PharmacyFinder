# Engine Audit Report — Pre-OSM Import

**Date:** 2026-03-23  
**Auditor:** Zezima (automated)  
**Trigger:** Preparing to import 72,737 commercial sites from OSM  
**Current candidate pool:** ~1,800 (from 1,439 supermarkets + 163 medical centres + 155 shopping centres + 38 hospitals + gap analysis)  
**Post-import candidate pool:** Could reach **74,000+** if OSM sites become candidates

---

## Database Snapshot

| Table | Count | Role in Engine |
|-------|-------|----------------|
| pharmacies | 6,498 | Reference (spatial index, distance checks) |
| gps | 3,815 | Reference (FTE checks, adjacency scoring) |
| supermarkets | 1,439 | Reference + Candidate source |
| hospitals | 38 | Reference + Candidate source |
| shopping_centres | 155 | Reference + Candidate source |
| medical_centres | 163 | Reference + Candidate source |
| commercial_properties | 546 | NOT used by engine |
| sal_boundaries | 15,334 | NOT used by engine |
| poa_boundaries | 2,641 | NOT used by engine |
| town_boundaries | 15,334 | NOT used by engine |
| population_grid | 2,454 | NOT used by engine |
| growth_corridors | 87 | NOT used by engine |
| planned_retail | 26 | NOT used by engine |
| v2_results | 619 | Output |

---

## Critical Issues Found

### 🔴 CRITICAL-1: No Spatial Indexes on SQLite Tables

**Impact: HIGH | Effort: LOW**

The DB has **zero lat/lon indexes** on pharmacies, gps, supermarkets, hospitals, shopping_centres, or medical_centres. All data is loaded into memory via `SELECT *` at startup, so this doesn't affect the engine today — but it means:

- The `_load_data()` in `context.py` does `SELECT * FROM pharmacies WHERE latitude IS NOT NULL` — full table scan on 6,498 rows every startup. Fine now, but if pharmacies grows (or if you add OSM data to these tables), startup cost scales linearly.
- Any direct DB queries outside the engine (e.g. from the web UI) will be slow without indexes.

**Recommendation:** Add composite indexes on `(latitude, longitude)` for all POI tables. Trivial to add, no downside.

---

### 🔴 CRITICAL-2: Pipeline Only Generates Candidates from 4 POI Types + Gaps

**Impact: HIGH | Effort: MEDIUM**

`pipeline.py` `generate_candidates()` creates candidates from:
1. Supermarkets → Item 130 candidates
2. Medical centres → Item 136 candidates
3. Shopping centres → Item 133/134/134A candidates
4. Hospitals → Item 135 candidates
5. Gap midpoints → Item 131 rural candidates

**Missing entirely:**
- `commercial_properties` (546 rows) — not used as candidate source
- `broker_listings` (36 rows) — not used
- `developments` (79 rows) — not used
- `growth_corridors` (87 rows) — not used
- `planned_retail` (26 rows) — not used
- `planned_town_centres` (136 rows) — not used
- `developer_pipeline` (23 rows) — not used
- **The incoming 72,737 OSM commercial sites** — not integrated

**The OSM extract contains:**
- `building_retail` — retail buildings (shops, malls)
- `building_commercial` — commercial buildings (offices, mixed-use)
- `landuse_retail` — zones designated for retail
- `landuse_commercial` — zones designated for commercial

**Should these be candidates?** 

- **`building_retail`**: YES — these are actual retail sites where a pharmacy could operate. High-value candidates for Items 130, 131, 132 (distance-based rules that just need a valid premises location).
- **`building_commercial`**: MAYBE — only if they're retail-capable. Many will be offices. Consider filtering by sub-type or proximity to retail anchors.
- **`landuse_retail`**: USE AS ZONE FILTER — these define retail zones. Don't generate candidates from them directly (a zone isn't a specific address), but use them to validate that candidates are in appropriately zoned areas (strengthening the `council_zoning_allows_pharmacy` general requirement check).
- **`landuse_commercial`**: USE AS ZONE FILTER — same as above, lower priority than retail zones.

**Recommendation:** Import `building_retail` as candidates. Use `landuse_retail`/`landuse_commercial` as zone validation layers. Skip `building_commercial` unless filtered to retail-capable subtypes.

---

### 🔴 CRITICAL-3: O(n²) in Gap Candidate Generation

**Impact: HIGH at scale | Effort: MEDIUM**

In `_add_gap_candidates()`:
```python
for i, p1 in enumerate(pharmas_sorted):
    for j in range(i + 1, min(i + 10, len(pharmas_sorted))):
```

Currently limited to `min(i + 10, ...)` so it's O(n × 10) = O(n). This is fine. **However**, the limitation to only checking 10 nearest-by-latitude neighbors is a **correctness issue** — it misses pharmacy pairs that are close in longitude but far apart in latitude. A pharmacy 50km due east won't be found.

**Current mitigation:** The `max_gaps = 200` cap limits output regardless.

**Recommendation:** Replace latitude-sort + window with spatial index query. For each pharmacy, find all pharmacies 15-100km away using the existing `_SpatialIndex`, then take midpoints. This is both more correct and naturally bounded.

---

### 🟡 MEDIUM-1: Spatial Grid Cell Size Too Coarse for Small Radii

**Impact: MEDIUM | Effort: LOW**

`GRID_CELL_DEG = 0.1` means each cell is ~8km × 11km. For queries like:
- `pharmacies_within_radius(..., 0.05)` (50m for approved premises check)
- `supermarkets_within_radius(..., 0.03)` (30m for supermarket access)
- `pharmacies_within_radius(..., 0.15)` (150m for in-centre check)

The grid returns the entire cell (~8km × 11km) of candidates, then filters by exact geodesic distance. With 6,498 pharmacies and 0.1° cells, each cell averages ~1-5 pharmacies in urban areas, ~0 in rural. This is acceptable.

**But with 72,737 OSM sites**, cells in urban areas could contain 100+ candidates. Every 50m radius query would compute geodesic distance to all 100+ candidates in the cell. With thousands of evaluations, this adds up.

**Recommendation:** Either:
1. Use a finer grid for small-radius queries (e.g., `GRID_CELL_DEG = 0.01` = ~1km cells) — but this increases memory for large-radius queries
2. Better: use a two-level grid (0.01° for <1km queries, 0.1° for >1km queries)
3. Best: switch to `scipy.spatial.cKDTree` for O(log n) nearest-neighbor queries instead of grid

---

### 🟡 MEDIUM-2: `geodesic()` Is Slow — 50μs per Call, Millions of Calls at Scale

**Impact: MEDIUM | Effort: LOW**

`geopy.distance.geodesic` is accurate but slow (~50μs per call). The engine calls it:
- Once per candidate × once per nearby POI in each radius query
- Each candidate triggers ~8-12 radius queries (pharmacies, GPs, supermarkets, hospitals, shopping centres, medical centres)
- Each radius query hits grid cells and computes geodesic for every item in those cells

**Current:** ~1,800 candidates × ~12 queries × ~5 candidates per cell = ~108,000 geodesic calls ≈ 5.4 seconds

**Post-import:** ~74,000 candidates × ~12 queries × ~50 candidates per cell = ~44,400,000 geodesic calls ≈ **37 minutes** just on distance calculations

**Recommendation:** Replace `geodesic()` with Haversine approximation for pre-filtering (accurate to <0.3% at Australian latitudes), only use geodesic for final measurements that are borderline. Or better, use numpy vectorized Haversine for batch operations:

```python
import numpy as np
def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0
    dlat = np.radians(lat2 - lat1)
    dlon = np.radians(lon2 - lon1)
    a = np.sin(dlat/2)**2 + np.cos(np.radians(lat1)) * np.cos(np.radians(lat2)) * np.sin(dlon/2)**2
    return R * 2 * np.arcsin(np.sqrt(a))
```

---

### 🟡 MEDIUM-3: Every Candidate Runs ALL 9 Rule Checks

**Impact: MEDIUM | Effort: LOW**

In `evaluator.py`, every candidate runs Items 130, 131, 132, 133, 134, 134A, 135, 136 regardless of source type. A supermarket-sourced candidate will never pass Item 135 (hospital) or Item 136 (medical centre), but the engine still runs those checks — including expensive spatial queries.

**Current:** With ~1,800 candidates, wasted evaluations are ~30% of total work.

**Post-import:** With 74,000 candidates (most being retail buildings), running hospital/medical-centre checks on every retail building is pure waste.

**Recommendation:** Add source-type routing in the evaluator:
```python
# Only run rules relevant to the candidate's source type
if candidate.source_type == "supermarket":
    applicable_items = [130, 131, 132]
elif candidate.source_type == "shopping_centre":
    applicable_items = [133, 134, 134A]
elif candidate.source_type == "hospital":
    applicable_items = [135]
elif candidate.source_type == "medical_centre":
    applicable_items = [136]
elif candidate.source_type in ("gap", "retail_building", "commercial"):
    applicable_items = [130, 131, 132]  # Distance-based rules
```

---

### 🟡 MEDIUM-4: Population Data Enrichment Is Fragile

**Impact: MEDIUM | Effort: LOW**

`_load_population_data()` loads from the `opportunities` table (179 rows) and does fuzzy matching at 0.01° resolution (~1km). This means:
- Most candidates get `pop_10km = 0` (the default)
- The `population_grid` table (2,454 rows) exists but is **never used** by the engine
- Commercial scoring's `demand_potential` component returns 0 for most candidates

**Recommendation:** Use `population_grid` table directly. Build a spatial index over it. For each candidate, sum population within 10km using the grid data. This gives actual demand estimates instead of zeros.

---

### 🟡 MEDIUM-5: OSRM Rate Limiting Will Bottleneck Item 131

**Impact: MEDIUM (borderline cases only) | Effort: LOW**

Item 131 uses OSRM for driving distance on borderline cases (5-8km geodesic). The engine rate-limits to 1 request/second. With more candidates in the borderline range, this could become a serious bottleneck.

**Current:** Few borderline candidates → minimal OSRM calls.

**Post-import:** Many more retail buildings at 5-8km from pharmacies → potentially hundreds of OSRM calls → hundreds of seconds of blocking.

**Recommendation:** 
1. Widen the geodesic clear-pass/fail thresholds to reduce borderline cases
2. Batch OSRM requests (OSRM table service supports many-to-many)
3. Consider self-hosting OSRM with Australian road network for unlimited local queries

---

### 🟢 LOW-1: `_infer_state()` Coordinate-Based Fallback Is Brittle

**Impact: LOW | Effort: LOW**

The coordinate-based state inference in `pipeline.py` has overlapping ranges and gaps:
- NSW: lat -37 to -29, lon 141+ (misses ACT, overlaps VIC border)
- VIC: lat -39.5 to -34, lon 141-150 (overlaps NSW)
- No explicit SA range (falls to lon 138-141 catch-all)

**Recommendation:** Use the existing `sal_boundaries` or `poa_boundaries` table for point-in-polygon state determination. These have 15,334 rows of actual boundary data that's sitting unused.

---

### 🟢 LOW-2: Boundary Data Loaded But Never Used

**Impact: LOW (wasted data) | Effort: MEDIUM**

The DB has rich boundary data:
- `sal_boundaries`: 15,334 Statistical Area Level boundaries
- `poa_boundaries`: 2,641 postcode boundaries  
- `town_boundaries`: 15,334 town boundaries

None are loaded or used by the engine. These could:
1. Determine which "town" a candidate is in (Item 132 requires same-town context)
2. Validate state assignment
3. Define "town" boundaries for GP/supermarket counting instead of the arbitrary 5km radius

---

### 🟢 LOW-3: `EvaluationResult` Missing Fields

**Impact: LOW | Effort: LOW**

The `EvaluationResult` model has `general_requirements`, `ministerial_assessments`, and `risk_assessment` fields that are set in the evaluator but **not included in `to_dict()`** output. These get lost when results are serialized to JSON/DB.

---

## Performance Projections

| Metric | Current (~1,800 candidates) | Post-Import (~74,000 candidates) | After Optimization |
|--------|----------------------------|-----------------------------------|--------------------|
| Startup (data load) | ~0.5s | ~0.5s (reference data unchanged) | ~0.5s |
| Candidate generation | ~0.1s | ~2s (iterate 72K OSM rows) | ~1s (with filtering) |
| Evaluation (total) | ~10-30s | **~45-90 minutes** | ~3-5 minutes |
| Geodesic calls | ~108K | ~44M | ~500K (with Haversine pre-filter) |
| OSRM calls | ~10-30 | ~200-500 | ~50-100 (wider thresholds) |
| Memory | ~50MB | ~300MB (72K dicts in grid) | ~150MB (numpy arrays) |

---

## Recommendations Ranked by Impact

### Tier 1 — Must Do Before Import

| # | Change | Impact | Effort | Description |
|---|--------|--------|--------|-------------|
| 1 | **Source-type routing** | 🔴 HIGH | LOW | Only run applicable rules per candidate type. Cuts eval work by 50-70%. |
| 2 | **Haversine pre-filter** | 🔴 HIGH | LOW | Replace geodesic with Haversine for grid filtering. Use geodesic only for final borderline measurements. 10-50× speedup on distance calcs. |
| 3 | **Import building_retail as candidates** | 🔴 HIGH | MEDIUM | These are the 72K sites. Route them through Items 130/131/132 only. |
| 4 | **Finer spatial grid or cKDTree** | 🟡 MEDIUM | LOW | Reduce unnecessary geodesic calls in dense urban cells. |

### Tier 2 — Should Do Soon

| # | Change | Impact | Effort | Description |
|---|--------|--------|--------|-------------|
| 5 | **Use population_grid** | 🟡 MEDIUM | LOW | Replace empty pop_10km with actual ABS grid data for demand scoring. |
| 6 | **Use town_boundaries** | 🟡 MEDIUM | MEDIUM | Proper town membership for Item 132 instead of 5km radius. |
| 7 | **Use landuse_retail as zone filter** | 🟡 MEDIUM | MEDIUM | Strengthen zoning validation. Filter out non-retail OSM buildings early. |
| 8 | **Batch OSRM** | 🟡 MEDIUM | MEDIUM | Use table service for many-to-many route distances. |

### Tier 3 — Nice to Have

| # | Change | Impact | Effort | Description |
|---|--------|--------|--------|-------------|
| 9 | **Use sal_boundaries for state** | 🟢 LOW | MEDIUM | Replace brittle coordinate-based state inference. |
| 10 | **Fix EvaluationResult.to_dict()** | 🟢 LOW | LOW | Include general_requirements, ministerial, risk in output. |
| 11 | **Add DB spatial indexes** | 🟢 LOW | LOW | Future-proof for direct DB queries. |
| 12 | **Use growth_corridors / planned_retail** | 🟢 LOW | MEDIUM | Forward-looking candidate generation for emerging areas. |

---

## OSM Data Integration Plan

```
OSM Extract
├── building_retail (72,737)     → CANDIDATE SOURCE (Items 130/131/132)
│   Filter: must have lat/lon, deduplicate against existing supermarkets/SCs
│   
├── building_commercial          → CANDIDATE SOURCE (filtered)
│   Filter: only include if near existing retail anchors or in retail zones
│   
├── landuse_retail               → ZONE VALIDATION LAYER
│   Use: boost confidence if candidate is inside a retail zone
│   Use: pre-filter building_commercial candidates
│   
└── landuse_commercial           → ZONE VALIDATION LAYER (lower priority)
    Use: secondary confidence signal
```

---

## Immediate Action Items

1. **Before importing OSM:** Implement source-type routing in evaluator (30 min)
2. **Before importing OSM:** Add Haversine fast-path in `_SpatialIndex` (1 hour)
3. **During import:** Create new `osm_retail_sites` table, import building_retail with dedup
4. **During import:** Add to `generate_candidates()` as `source_type="retail_building"`
5. **After first run:** Profile actual bottlenecks with `cProfile` and address
