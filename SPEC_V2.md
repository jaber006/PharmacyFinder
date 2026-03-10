# PharmacyFinder V2 — Geospatial Rules Engine

## Core Principle

This is a **geospatial rules engine**, not a generic site finder.

The core job: for each candidate premises, determine which ACPA item it could
qualify under, measure **exactly** the way the Rules require, then rank only
the compliant sites. The ACPA applies the Pharmacy Location Rules strictly,
and the handbook is explicit that the Rules cannot be overridden.

**Don't score a site until you know which legal bucket it fits. A "great
commercial site" that fails every rule is worthless.**

---

## 1. Legal Model — Rule Modules

Each approval pathway is a separate rule module:

### Item 130 — New pharmacy ≥ 1.5 km straight line
- At least 1.5 km **straight line** from nearest approved premises
- Nearby doctor/supermarket requirements within 500 m

### Item 131 — New pharmacy ≥ 10 km by road
- At least 10 km by **shortest lawful access route** from nearest approved premises

### Item 132 — Same-town additional pharmacy
- At least 200 m **straight line** from nearest approved premises
- At least 10 km **shortest lawful access route** from other approved premises
- At least 4 FTE prescribing medical practitioners in town
- Enough supermarket GLA in town

### Item 133 — Small shopping centre pharmacy
- Specific centre characteristics
- At least 500 m **straight line** from nearest approved premises
- Exclusion categories apply

### Item 135 — Large private hospital pharmacy
- Large private hospital
- No approved pharmacy already there

### Item 136 — Large medical centre pharmacy
- No approved pharmacy already there
- Generally 300 m separation requirements
- At least 8 FTE PBS prescribers (≥ 7 prescribing medical practitioners)
- Centre operating at least 70 hours weekly

---

## 2. Measurement Logic

Two distance systems required:

### Straight-line distance
- Measured from **mid-point at ground level of the public access door** to
  corresponding point on other premises
- Use geodesic distance (not Euclidean on projected coords)
- Required for: Items 130, 132, 133, 136

### Shortest lawful access route
- Can be by car, walking, or another legal public route
- Must be a route available to ordinary members of the public
- Use OSRM routing engine
- Required for: Items 131, 132

### Implementation
- PostGIS ST_Distance / geography for meter-based straight-line
- ST_DWithin or precomputed buffers for fast screening
- OSRM API for route distance calculations

---

## 3. Data Layers

### A. Approved Pharmacy Premises (MOST IMPORTANT)
- Verified coordinates
- Approval type / origin item if known
- Current status
- Public access door location
- Town/locality
- Whether inside a designated complex
- **Note:** No obvious public machine-readable register exists. Build from
  multiple sources, manually verify at candidate stage.

### B. Candidate Premises
- Commercial leases, vacant shops, shopping centres, medical centres,
  hospital retail pads

### C. Complex Polygons
- Shopping centre boundaries
- Hospital boundaries
- Medical centre boundaries
- **Need polygons, not just addresses**

### D. Prescriber Layer
- Medical centres, practitioner counts, hours, PBS-prescriber proxies
- For Item 136 especially, never fully automated — need confidence score
  plus manual verification

### E. Supermarket / GLA Layer
- For Items 130, 132, 133, 134/134A
- Evidence from councils, public planning documents, retail strategies,
  state planning material, or proprietary databases

### F. Geography / Town Layer
- ABS ASGS boundary files (GeoPackage/Shapefile)
- Localities, towns, admin joins

---

## 4. Three-Pass Evaluation

### Pass 1: Hard Legal Exclusion (fast filters)
- Within 1.5 km straight line of approved pharmacy? → fail Item 130
- Inside large medical centre with existing approved premises? → fail 136
- Not capable of pharmacy use under zoning? → fail early
- Directly accessible from within a supermarket? → fail where relevant

**General requirements (boolean gates):**
- Must not already be approved premises
- Applicant must have legal right to occupy
- Must be capable of pharmacy use under relevant laws
- Accessible by the public
- Capable of opening within 6 months
- For new-pharmacy items: must NOT be directly accessible from within a supermarket

### Pass 2: Rule-Path Classification (deterministic decision tree)
For each surviving site, classify into:
- Standalone (130, 131)
- Same-town additional (132)
- Small shopping centre (133)
- Large private hospital (135)
- Large medical centre (136)

**This is a deterministic decision tree, not machine learning.**

### Pass 3: Commercial Ranking
Once legally possible, score for:
- Catchment population
- Scripts density
- GP/prescriber gravity
- Supermarket anchor strength
- Parking
- Competitor quality
- Lease economics
- Demographics
- Growth corridor momentum

---

## 5. Code Structure

```python
class Candidate:
    id: str
    geometry: Point
    public_access_door: Point
    zoning_ok: bool
    legal_right_to_occupy: bool
    accessible_to_public: bool
    can_open_within_6_months: bool
    direct_access_from_supermarket: bool
    complex_type: str | None  # none, small_sc, large_sc, private_hospital, large_medical
    town_id: str | None

class RuleResult:
    item: str
    passed: bool
    reasons: list[str]
    evidence_needed: list[str]
    confidence: float

def evaluate_candidate(candidate, context) -> list[RuleResult]:
    results = []
    results.append(check_item_130(candidate, context))
    results.append(check_item_131(candidate, context))
    results.append(check_item_132(candidate, context))
    results.append(check_item_133(candidate, context))
    results.append(check_item_135(candidate, context))
    results.append(check_item_136(candidate, context))
    return [r for r in results if r.passed]
```

Each rule function:
1. Pull relevant nearby premises / centres / prescribers
2. Run the exact distance method required
3. Return: passed/failed, reasons, missing evidence, confidence score

---

## 6. Two-Stage Precision Model

### Stage A: Heuristic Screening (100,000 → 200 sites)
- Approximate door points
- OSM road network
- Inferred shopping-centre polygons
- Inferred supermarket size
- Inferred medical-centre type

### Stage B: Legal-Grade Verification (200 → shortlist)
- Manually confirm public access doors
- Surveyor-grade distance where close to threshold
- Statutory declaration inputs
- Planning documents
- Centre manager confirmation
- Prescriber rosters / hours
- Tenancy schedules / GLA evidence

---

## 7. Ranking Model (Post-Compliance)

```
Final Score =
  0.35 × legal_robustness
  0.20 × demand_scripts_potential
  0.15 × gp_adjacency
  0.10 × anchor_traffic
  0.10 × lease_economics
  0.05 × parking_access
  0.05 × area_growth
```

**Legal robustness** (biggest weight):
- Margin above threshold
- Quality of evidence
- Complexity of designated complex classification
- Likelihood of objection / dispute
- Dependency on fragile facts

A site 30 m above the limit is much worse than one 600 m above it,
even if foot traffic is slightly better.

---

## 8. Tech Stack

### Backend
- Python + FastAPI
- PostgreSQL + PostGIS
- Celery / background jobs for routing batches

### Spatial / Routing
- PostGIS for geometry math
- OSRM for route distance
- ABS boundary files for geography joins

### Frontend
- React + Leaflet/Mapbox
- Rule explanation panel
- "Why this passed / failed" trace
- Evidence checklist per item

### Admin Tools
- Manual override with audit log
- Evidence upload
- Site verification workflow
- Threshold warnings

---

## 9. Database Schema

### Core Tables
- approved_pharmacies
- candidate_sites
- shopping_centres
- medical_centres
- private_hospitals
- supermarkets
- prescribers
- town_boundaries
- rule_evaluations
- evidence_documents

### Important Columns (every spatial table)
- geom_point
- geom_polygon
- public_access_door_geom
- source
- source_date
- confidence
- verified_by
- verified_at

---

## 10. Build Phases

### Version 1
- Import existing pharmacy locations
- Import candidate shopfronts
- Implement Items 130, 131, 132
- Build map + pass/fail output + evidence checklist

### Version 2
- Add shopping-centre and medical-centre pathways (133, 135, 136)
- Add routing engine
- Add commercial scoring

### Version 3
- Add prescriber / supermarket / planning integrations
- Add objection-risk and legal-robustness scoring
- Add report generation for each shortlisted site

---

## 11. Common Failure Points

- Using centroid-to-centroid instead of public access door midpoint
- Treating 10 km checks as straight-line instead of shortest lawful route
- Misclassifying shopping centres / medical centres without polygon,
  management structure, GLA, or tenancy evidence
- Assuming doctor-count data is easy (it's not)
- Ranking "good business sites" before legal compliance
- Ignoring relocation restrictions tied to original approval pathways

---

## The Winning Logic

1. Encode each rule item exactly
2. Measure the way the Rules require
3. Use GIS to narrow
4. Use evidence workflows to verify
5. Only then rank commercially
