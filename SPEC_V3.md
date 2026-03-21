# PharmacyFinder V3 — The Deal Machine

## Philosophy Change

V1/V2: "Here are zones where a pharmacy COULD go"  
V3: "Here is 123 Main St. It qualifies under Item 131. Here's the evidence package. File this application."

The ACPA doesn't approve zones. It approves **specific premises** with a **specific public access door** measured to the millimetre. Every output must be a real address with a real door.

---

## Architecture: Four Layers

### Layer 1: Data Foundation
All reference data — pharmacies, GPs, supermarkets, hospitals, shopping centres, medical centres, town boundaries, commercial properties.

**Critical change:** Primary pharmacy source must be PBS Approved Suppliers (health.gov.au), not OSM. OSM is supplementary.

### Layer 2: Candidate Premises
Real shopfronts and leasable spaces — not theoretical points on a map.

Sources:
- Commercial RE listings (realcommercial.com.au, commercialrealestate.com.au)
- Shopping centre vacancy lists
- Council DA applications for new commercial builds
- Medical centre retail pads
- Hospital retail tender notices
- Planned developments (DA-approved, not yet built)
- Manual additions (sites MJ finds while scouting)

Each candidate has:
- Exact address + coordinates
- Estimated public access door location
- Current occupant / vacancy status
- Lease status (available, occupied, expiring)
- Council zoning status
- Floor area
- Proximity to anchors (supermarket, GP, hospital)

### Layer 3: Rules Engine
Three-pass evaluation against ALL rules, including:
- Items 130-136 (new pharmacy)
- Items 121-125 (relocation opportunities)
- General requirements checklist
- Ministerial discretion scoring
- "All relevant times" risk assessment

### Layer 4: Output & Monitoring
- Watchlist system (hawk mode)
- Application evidence package generator
- Change detection & alerting
- Interactive dashboard

---

## Rules Engine V3 — Complete Implementation

### General Requirements (EVERY application)
Boolean checklist — must ALL be true:

```python
class GeneralRequirements:
    not_approved_premises: bool          # Not already an approved pharmacy
    legal_right_to_occupy: bool | None   # Lease/ownership evidence (manual)
    council_zoning_allows_pharmacy: bool | None  # Council zoning (manual/scrape DA)
    accessible_by_public: bool | None    # Public access (manual)
    ready_within_6_months: bool | None   # Can open in 6 months (manual)
    not_accessible_from_supermarket: bool # Not directly accessible from inside a supermarket
```

Automatable: `not_approved_premises` (check against pharmacy DB), `not_accessible_from_supermarket` (if in shopping centre, check adjacency).
Manual verification needed: `legal_right_to_occupy`, `council_zoning`, `accessible_by_public`, `ready_within_6_months`.

### Item 130: New pharmacy (≥ 1.5 km)
```
Requirements:
  (a) ≥ 1.5 km STRAIGHT LINE from nearest approved premises
  (b) Within 500m straight line, EITHER:
      (i) ≥ 1 FTE prescribing GP AND supermarket ≥ 1,000m² GLA
      (ii) Supermarket ≥ 2,500m² GLA

Measurements:
  - Straight line = geodesic, door midpoint to door midpoint
  - 500m check also straight line, same method

Data needed:
  - All approved pharmacy locations (door points)
  - GP practices within 500m + FTE status
  - Supermarkets within 500m + GLA

Confidence factors:
  - Distance margin above 1.5km (bigger = safer)
  - GP FTE verification quality
  - Supermarket GLA verification quality
```

### Item 131: New pharmacy (≥ 10 km by road)
```
Requirements:
  - ≥ 10 km by SHORTEST LAWFUL ACCESS ROUTE from nearest approved premises
  - That's it (plus general requirements)

Measurements:
  - Road distance via OSRM (not straight line!)
  - Route must be "generally available to average persons"
  - From door midpoint to door midpoint

Restriction:
  - Cannot EVER relocate from the town where originally granted

Data needed:
  - All approved pharmacy locations
  - OSRM routing engine

Confidence factors:
  - Road distance margin above 10km
  - Route quality (single road vs multiple options)
  - If close to 10km: need surveyor-grade evidence
```

### Item 132: New additional pharmacy (≥ 10 km)
```
Requirements:
  (a)(i) In SAME TOWN (same name + same postcode) as an approved premises
  (a)(ii) ≥ 200m STRAIGHT LINE from NEAREST approved premises
  (a)(iii) ≥ 10km by ROAD from ALL OTHER approved premises (except nearest)
  (b)(i) ≥ 4 FTE prescribing GPs practising in same town
  (b)(ii) 1-2 supermarkets with COMBINED GLA ≥ 2,500m² in same town

Measurements:
  - 200m = straight line geodesic
  - 10km = OSRM road routing (to EVERY other pharmacy, not just nearest)
  - "Same town" = same town name AND same postcode

Data needed:
  - Pharmacy locations + town/postcode
  - GP practices in town + FTE
  - Supermarkets in town + GLA
  - ABS locality/postcode boundaries

Restriction:
  - Cannot EVER relocate from original town

Confidence factors:
  - FTE GP data quality (4+ is strict)
  - Supermarket GLA verification
  - Road distance margins to ALL other pharmacies
```

### Item 133: Small shopping centre
```
Requirements:
  (a) Premises in a SMALL SHOPPING CENTRE:
      - Single management
      - GLA ≥ 5,000m²
      - Contains supermarket ≥ 2,500m² GLA
      - ≥ 15 other commercial establishments
      - Customer parking
  (b) ≥ 500m straight line from nearest approved premises
      (excluding those in large shopping centre or private hospital)
  (c) No approved premises already in this shopping centre

Data needed:
  - Shopping centre details (management, GLA, tenants, parking)
  - Supermarket in centre + GLA
  - Pharmacy locations (with complex classification)
```

### Item 134: Large shopping centre (no existing pharmacy)
```
Requirements:
  (a) Premises in a LARGE SHOPPING CENTRE:
      - Single management
      - GLA ≥ 5,000m²
      - Contains supermarket ≥ 2,500m² GLA
      - ≥ 50 other commercial establishments
      - Customer parking
  (b) No approved premises in the shopping centre

Note: NO distance requirement from other pharmacies outside the centre.
```

### Item 134A: Large shopping centre (additional pharmacy)
```
Requirements:
  (a) Premises in a LARGE SHOPPING CENTRE (same definition as 134)
  (b) Tenant count determines max pharmacies:
      - 100-199 commercial establishments → max 1 existing (allows 2nd)
      - ≥ 200 commercial establishments → max 2 existing (allows 3rd)
  (c) No pharmacy has relocated OUT of the centre in last 12 months

Data needed:
  - Accurate tenant counts (critical!)
  - Current pharmacy count in centre
  - Relocation history
```

### Item 135: Large private hospital
```
Requirements:
  (a) Premises in a LARGE PRIVATE HOSPITAL (can admit ≥ 150 patients)
  (b) No approved premises in the hospital

Data needed:
  - Hospital bed capacity (from license/registration, not just beds)
  - Whether hospital is PRIVATE (not public)
  - Current pharmacy presence
```

### Item 136: Large medical centre
```
Requirements:
  (a) Premises in a LARGE MEDICAL CENTRE:
      - Single management
      - Operates ≥ 70 hours/week
      - ≥ 1 prescribing GP available ≥ 70 hours/week
  (b) No approved premises in the medical centre
  (c) Distance:
      - If IN shopping centre/hospital: ≥ 300m from any approved premises
        EXCEPT those in a DIFFERENT large SC or hospital
      - If NOT in SC/hospital: ≥ 300m from nearest approved premises
        EXCEPT those in a large SC or hospital
  (d) For 2 months before application AND until hearing:
      - ≥ 8 FTE PBS prescribers (304 hrs/week total)
      - ≥ 7 must be prescribing medical practitioners
      - Max 38 hrs/week from non-medical PBS prescribers
  (e) Reasonable attempt to match medical centre operating hours

Critical notes:
  - Telehealth only counts if GP is PHYSICALLY at the centre
  - Time at other centres/hospitals/admin does NOT count
  - Must maintain 8 FTE for 2+ months BEFORE applying
  - "All relevant times" = must STILL meet criteria at hearing date

Data needed:
  - Medical centre details (management, hours, GP roster)
  - Verified GP headcount AND hours per GP
  - PBS prescriber breakdown (medical vs non-medical)
```

### Items 121-125: Relocation Opportunities
```
These find existing pharmacies that SHOULD relocate:
  - Pharmacy stuck in dying strip mall → relocate to new shopping centre
  - Pharmacy in poor location → relocate closer to new GP cluster
  - Shopping centre expanding → pharmacy should relocate within complex

Requirements vary but key constraint:
  - Original approval must have been in force ≥ 5 years continuously
  - Exceptions for same-complex moves (Item 122)

This is a deal-finding angle: approach pharmacy owners with relocation proposals.
```

### Ministerial Discretion Scoring
```
Section 90A(2): Minister CAN approve even if Rules aren't met.
Only available AFTER Authority rejection.

Score borderline sites:
  - How close to each threshold? (1.4km vs 1.5km for Item 130)
  - Community need argument strength
  - Comparable approved pharmacies (precedent)
  - Population underserved metrics
  - Distance to nearest alternative pharmacy

Flag as: MINISTERIAL_CANDIDATE with gap_percentage and community_need_score
```

---

## Watchlist System (Hawk Mode)

### What to watch:
1. **Near-miss sites** — almost qualify, watching for threshold change
2. **Pharmacy closures** — when one closes, nearby sites may newly qualify
3. **GP movements** — new GP clinic opens → re-check Item 130/136
4. **Shopping centre developments** — new build → check Items 133/134
5. **Medical centre expansions** — more GPs hired → re-check Item 136
6. **Commercial lease expirations** — premises becoming available
7. **Council DA approvals** — new commercial zones opening up
8. **Hospital expansions** — bed count changes → re-check Item 135

### Alert triggers:
- New site qualifies (wasn't qualifying before)
- Site drops below threshold (was qualifying, now isn't)
- Commercial property available in qualifying zone
- Competitor pharmacy application lodged (public record)

---

## Evidence Package Generator

For each shortlisted site, auto-generate:

1. **Distance measurements** — screenshots from mapping tool with measurements
2. **Pharmacy proximity map** — all approved pharmacies within relevant radius
3. **GP/prescriber documentation** — practice names, headcounts, FTE estimates
4. **Shopping centre evidence** — tenancy schedule, GLA, management structure
5. **Council zoning check** — zoning map + permitted uses
6. **Risk assessment** — what could change between application and hearing
7. **Ministerial case** (if borderline) — community need, population data, access gaps
8. **Checklist** — all general requirements with status (verified/pending/failed)

---

## Data Model

```python
@dataclass
class CandidatePremises:
    id: str
    address: str
    latitude: float
    longitude: float
    door_latitude: float | None      # Public access door midpoint
    door_longitude: float | None
    state: str
    postcode: str
    town: str
    
    # Premises details
    floor_area_sqm: float | None
    current_occupant: str | None
    vacancy_status: str              # vacant, occupied, expiring, planned
    lease_expiry: date | None
    
    # Classification
    in_shopping_centre: bool
    shopping_centre_id: int | None
    in_medical_centre: bool
    medical_centre_id: int | None
    in_hospital: bool
    hospital_id: int | None
    
    # Source
    source: str                      # commercial_re, manual, da_application, etc
    source_url: str | None
    discovered_date: date
    last_checked: date
    
    # General requirements
    general_requirements: GeneralRequirements

@dataclass  
class RuleEvaluation:
    candidate_id: str
    item: str                        # "130", "131", etc
    passed: bool
    confidence: float                # 0.0-1.0
    margin: float                    # Distance/count above threshold (negative = failed)
    margin_pct: float                # Percentage above threshold
    ministerial_candidate: bool      # Close enough for ministerial discretion?
    evidence_quality: str            # "sufficient", "needs_survey", "preliminary"
    distances: dict                  # All measured distances
    risks: list[str]                 # "All relevant times" risks
    evidence_needed: list[str]       # What evidence to gather
    notes: str

@dataclass
class WatchlistItem:
    candidate_id: str
    watch_reason: str                # "near_miss", "lease_expiring", "gp_growth", etc
    trigger_condition: str           # What change would make this qualify
    check_frequency: str             # "daily", "weekly", "monthly"
    last_checked: date
    status: str                      # "watching", "triggered", "expired"
```

---

## Tech Stack

- **Python 3.12** — core engine
- **SQLite** — portable, no server needed
- **OSRM (self-hosted Docker)** — fast road routing, no rate limits
- **geopy** — geodesic distance calculations
- **FastAPI** — web API for dashboard + on-demand evaluation
- **Leaflet** — interactive maps
- **Playwright** — commercial RE scraping
- **APScheduler** — automated re-scanning and watchlist checks

---

## Project Structure V3

```
PharmacyFinder/
├── main.py                          # CLI entry point
├── config.py                        # All thresholds and settings
├── SPEC_V3.md                       # This document
│
├── data/                            # Data layer
│   ├── sources/                     # Raw data importers
│   │   ├── pbs_suppliers.py         # PBS Approved Suppliers (primary)
│   │   ├── osm_pharmacies.py        # OpenStreetMap (supplementary)
│   │   ├── healthdirect.py          # Healthdirect GP/pharmacy
│   │   ├── aihw_hospitals.py        # AIHW hospital data
│   │   ├── abs_boundaries.py        # ABS town/postcode boundaries
│   │   └── abs_population.py        # ABS census population grids
│   ├── enrichment/                  # Data enrichment
│   │   ├── gp_count_verifier.py     # Verify GP FTE per practice
│   │   ├── sc_tenant_verifier.py    # Verify shopping centre tenants
│   │   ├── hospital_bed_verifier.py # Verify hospital bed counts
│   │   └── mc_hours_verifier.py     # Verify medical centre hours
│   └── database.py                  # SQLite schema + queries
│
├── candidates/                      # Candidate premises layer
│   ├── commercial_re.py             # Commercial RE listings
│   ├── council_da.py                # Council DA applications
│   ├── shopping_centre_vacancies.py # SC vacancy lists
│   ├── manual.py                    # Manual additions
│   └── candidate_manager.py         # CRUD + dedup + geocoding
│
├── engine/                          # Rules engine
│   ├── models.py                    # Data classes
│   ├── context.py                   # Spatial index + data access
│   ├── evaluator.py                 # Three-pass pipeline
│   ├── general_requirements.py      # General requirements checker
│   ├── ministerial.py               # Ministerial discretion scoring
│   ├── scoring.py                   # Commercial ranking
│   ├── rules/
│   │   ├── item_130.py
│   │   ├── item_131.py
│   │   ├── item_132.py
│   │   ├── item_133.py
│   │   ├── item_134.py
│   │   ├── item_134a.py
│   │   ├── item_135.py
│   │   ├── item_136.py
│   │   └── relocations.py          # Items 121-125
│   └── risk_assessment.py           # "All relevant times" risk
│
├── watchlist/                       # Hawk mode
│   ├── monitor.py                   # Change detection engine
│   ├── alerts.py                    # Notification system
│   └── scheduler.py                 # Automated re-checks
│
├── evidence/                        # Evidence package generator
│   ├── distance_maps.py             # Distance measurement screenshots
│   ├── pharmacy_proximity.py        # Nearby pharmacy maps
│   ├── checklist.py                 # General requirements checklist
│   ├── risk_report.py               # Risk assessment report
│   └── pdf_generator.py             # Full application package PDF
│
├── api/                             # Web API
│   ├── app.py                       # FastAPI application
│   ├── routes/
│   │   ├── evaluate.py              # On-demand address evaluation
│   │   ├── candidates.py            # Candidate CRUD
│   │   ├── watchlist.py             # Watchlist management
│   │   └── reports.py               # Report generation
│   └── static/                      # Dashboard frontend
│
├── rules/                           # Reference documents
│   ├── handbook_v1.10_full.md       # Full handbook text
│   ├── RULES_EXACT.md               # Condensed rule requirements
│   └── handbook_2024.docx           # Original document
│
├── utils/
│   ├── distance.py                  # Geodesic + OSRM
│   ├── geocoding.py                 # Nominatim
│   ├── overpass_cache.py            # OSM data caching
│   └── boundaries.py               # State/town boundaries
│
├── cache/                           # Cached data
├── output/                          # Generated reports + maps
└── docker-compose.yml               # OSRM + API containers
```

---

## Build Phases

### Phase 1: Foundation (Track A + B + C in parallel)
- A: PBS Approved Suppliers import (authoritative pharmacy list)
- B: Complete data enrichment (GP FTE, SC tenants, hospital beds)
- C: Self-host OSRM Docker container

### Phase 2: Engine Rebuild (Track D + E)
- D: Implement general requirements + ministerial scoring
- E: Implement Items 134/134A + relocation rules (121-125)

### Phase 3: Candidate System (Track F + G)
- F: Commercial RE scraper → candidate premises pipeline
- G: Watchlist system + change detection

### Phase 4: Product (Track H + I)
- H: FastAPI web app + interactive dashboard
- I: Evidence package PDF generator
