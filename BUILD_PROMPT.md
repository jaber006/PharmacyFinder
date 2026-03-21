# PharmacyFinder V3 — Complete Build Prompt

> This prompt describes how to build the entire PharmacyFinder system from scratch. It was generated from the working codebase as of March 2026.

---

## What Is PharmacyFinder?

PharmacyFinder is an Australian pharmacy greenfield opportunity finder and ACPA (Australian Community Pharmacy Authority) compliance checker. It identifies locations across Australia where a new pharmacy could legally be approved under the National Health Act 1953, scores them for commercial viability, finds available commercial properties, estimates profitability, and generates evidence packages for ACPA applications.

Think of it as a "deal machine" — it finds the site, proves it's legal, estimates the money, finds the shopfront, and packages everything for an investor.

## Who Is It For?

Pharmacy investors and operators who want to:
- Find greenfield pharmacy opportunities (new pharmacies in underserved areas)
- Validate sites against ACPA approval rules before investing
- Generate evidence packages for ACPA applications (which cost $5,000+ to prepare manually)
- Estimate profitability before committing capital
- Track growth corridors where pharmacies will be needed in 2-5 years
- Monitor competitors and market changes

## Tech Stack

- **Language:** Python 3.12
- **Database:** SQLite (pharmacy_finder.db) — ~7.5MB with 12,000+ records
- **Web Framework:** FastAPI + Uvicorn (dashboard)
- **Frontend:** Vanilla HTML/JS + Leaflet.js (maps) + Chart.js (analytics)
- **PDF Generation:** ReportLab + staticmap
- **Geocoding:** geopy (Nominatim), optional self-hosted Nominatim via Docker
- **Routing:** OSRM (Open Source Routing Machine) via Docker for road distances
- **Spatial DB:** PostGIS (optional, for advanced spatial queries)
- **Scraping:** requests + BeautifulSoup, Playwright for JS-heavy sites

## Architecture (4 Layers)

```
┌─────────────────────────────────────────────────────┐
│                 LAYER 4: OUTPUT                      │
│  Dashboard (FastAPI) │ Evidence PDFs │ Deal Packages │
│  Daily/Weekly Reports │ CLI Tools │ Notifications    │
├─────────────────────────────────────────────────────┤
│              LAYER 3: RULES ENGINE                   │
│  ACPA Items 130-136 │ Confidence Scoring │           │
│  Profitability Model │ Growth Corridor Scoring       │
├─────────────────────────────────────────────────────┤
│           LAYER 2: CANDIDATE IDENTIFICATION          │
│  Commercial RE Matcher │ Council DA Scanner │        │
│  PSP Scanner │ Growth Corridors │ Developer Pipeline │
├─────────────────────────────────────────────────────┤
│            LAYER 1: DATA FOUNDATION                  │
│  Pharmacies (PBS) │ GPs │ Hospitals │ Supermarkets  │
│  Shopping Centres │ Medical Centres │ ABS Boundaries │
│  ABS Population │ OSRM Road Network                  │
└─────────────────────────────────────────────────────┘
```

## Database Schema

### Core Tables
- `pharmacies` — 6,498 records: name, address, latitude, longitude, source, suburb, state, postcode, opening_hours, coord_verified
- `gp_practices` — 3,815 records: name, address, state, gp_count (from Healthdirect)
- `hospitals` — 930 records: name, address, latitude, longitude, bed_count, hospital_type (public/private), state
- `medical_centres` — 163+ records: name, address, latitude, longitude, gp_count, operating_hours, source
- `supermarkets` — 1,439 records: name, address, latitude, longitude, chain (from OSM)
- `shopping_centres` — 202 records: name, address, latitude, longitude, gla_sqm, estimated_tenants, management_company

### ABS Data Tables
- `sal_boundaries` — 15,334 Suburb and Locality polygons (GeoJSON)
- `poa_boundaries` — 2,641 Postcode polygons (GeoJSON)
- `town_boundaries` — 15,334 combined town references
- `population_grid` — 2,454 SA2 areas with population estimates (25.6M total)

### Results Tables
- `v2_results` — 619 qualifying sites: name, address, latitude, longitude, state, source_type, passed_any, primary_rule, commercial_score, best_confidence, rules_json, profitability_score
- `commercial_matches` — matched commercial properties per site
- `council_da` — development applications from PlanningAlerts
- `growth_corridors` — high-growth SA2 areas with scores
- `psp_projects` — Precinct Structure Plan projects
- `planned_town_centres` — planned retail centres from PSPs
- `developer_pipeline` — developer project tracking
- `acpa_decisions` — AAT appeal decisions

### Monitoring Tables
- `watchlist_items` — user-defined sites to monitor
- `watchlist_alerts` — triggered alerts

## ACPA Rules (from Handbook V1.10, March 2026)

The ACPA evaluates pharmacy approval applications under these items of the National Health (Australian Community Pharmacy Authority Rules) Determination 2018:

### Item 130 — New Pharmacy Near Supermarket
- Must be within 1.5km (straight line) of a supermarket
- Must have GP/medical services within 1.5km
- Must be >1.5km from nearest existing pharmacy
- Supermarket must NOT have direct internal access to pharmacy

### Item 131 — Remote/Rural New Pharmacy  
- Must be >10km (by road, shortest lawful route) from nearest approved pharmacy
- The 10km is measured by road, not straight line
- Cannot later relocate outside the original town

### Item 132 — Relocation Within Same Town
- Must remain in the same town (ABS SAL boundaries define "town")
- Cannot relocate to a different town
- Item 131/132 pharmacies can NEVER leave their original town

### Item 133 — Pharmacy in Shopping Centre (Large)
- Shopping centre must have 30+ tenancies OR 10,000+ sqm GLA
- Pharmacy must be in a "shopping centre" as defined by ACPA

### Item 134 — Pharmacy in Shopping Centre (Medium)
- Shopping centre with 15-29 tenancies OR 5,000-9,999 sqm GLA

### Item 134A — Pharmacy in Shopping Centre (Small)
- Shopping centre with specific criteria for smaller centres

### Item 135 — Pharmacy Near Private Hospital
- Must be within 2km of a PRIVATE hospital with 150+ beds
- Public hospitals do NOT qualify
- Measured by straight line (geodesic)

### Item 136 — Pharmacy Near Large Medical Centre
- Medical centre must have 8+ FTE GPs
- Must operate 70+ hours per week
- GP must be physically present (telehealth alone doesn't qualify)
- 2-month FTE maintenance required before application

### Key Legal Principles
- **"All Relevant Times" doctrine:** Conditions must hold at application AND hearing
- **Straight line = geodesic** (door-to-door, not centroid-to-centroid)
- **Road distance = shortest lawful access route** (OSRM)
- **Ministerial discretion:** Minister can approve even if rules aren't met, if community need is demonstrated

## Module Descriptions

### Layer 1: Data Sources

**`scrapers/`** — PBS pharmacy scraper, Healthdirect GP scraper, OSM supermarket extractor, hospital/shopping centre scrapers
**`data/sources/abs_boundaries.py`** — Imports ABS SAL/POA boundary polygons for "same town" checks (Item 132)
**`data/sources/abs_population.py`** — Imports ABS SA2 population estimates for profitability scoring
**`data/sources/aihw_hospitals.py`** — Imports AIHW hospital data (930 hospitals with bed counts)
**`data/sources/shopping_centre_directory.py`** — Imports from Vicinity, Scentre, Stockland, Charter Hall, QIC

### Layer 2: Candidate Identification

**`engine/evaluator.py`** — 3-pass rules engine: exclusion → classification → ranking
**`engine/context.py`** — Spatial grid index (0.1° cells) for O(1) facility lookups
**`engine/scoring.py`** — Commercial scoring (7 weighted factors)
**`engine/rules/`** — Individual rule modules for Items 130, 131, 132, 133, 135, 136
**`candidates/commercial_re.py`** — Scrapes commercialrealestate.com.au for available leases near qualifying sites
**`candidates/matcher.py`** — Scores and ranks site + property combinations
**`candidates/council_da.py`** — Scrapes PlanningAlerts for commercial development applications
**`candidates/growth_corridors.py`** — ABS growth data + pharmacy gap analysis + retail pipeline
**`candidates/psp_scanner.py`** — Precinct Structure Plan scanner (78 PSPs, 136 planned town centres)
**`candidates/developer_pipeline.py`** — Tracks major developer project pipelines (Oreana, Stockland, etc.)

### Layer 3: Analysis & Scoring

**`analysis/profitability.py`** — Revenue estimation: scripts/year, PBS income, front-of-shop, GP%, setup costs, payback period, flip profit, ROI
**`scripts/scan_relocations.py`** — Finds relocation opportunities (329 found, 96 prime)
**`scripts/scan_ministerial.py`** — Identifies ministerial discretion candidates

### Layer 4: Output

**`api/app.py`** — FastAPI application
**`api/routes/analytics.py`** — Analytics endpoints (overview, heatmap, gaps, competition)
**`api/routes/sites.py`** — Site detail endpoints with nearby facilities
**`api/static/index.html`** — Interactive Leaflet map + Chart.js analytics dashboard
**`evidence/pdf_generator.py`** — Single-page compliance evidence PDFs
**`evidence/deal_package.py`** — 5-page investor deal package PDFs
**`scripts/evaluate_site.py`** — Quick site evaluator CLI (type address → instant analysis)
**`scripts/notify_opportunities.py`** — Daily summary with top 5 opportunities
**`scripts/weekly_report.py`** — Weekly executive report
**`scripts/acpa_monitor.py`** — ACPA competitor application monitor
**`scripts/generate_deal_package.py`** — Batch deal package generator

### Testing

**`tests/test_rules_engine.py`** — 60 tests across Items 130-136
**`tests/test_profitability.py`** — 31 tests for financial model
**`tests/conftest.py`** — Shared fixtures with in-memory SQLite

## Key Measurements

All distance calculations use:
- **Straight line:** `geopy.distance.geodesic()` (WGS84 ellipsoid, door-to-door)
- **Road distance:** OSRM Docker container (`localhost:5000/route/v1/driving/`)
- **Spatial lookups:** Grid index with 0.1° cells + geodesic verification

## How To Run

```bash
# Install dependencies
pip install fastapi uvicorn geopy requests beautifulsoup4 reportlab staticmap playwright pytest

# Start OSRM (optional, for road distances)
docker run -d --name osrm-australia -p 5000:5000 osrm/osrm-backend:latest

# Run the rules engine
py -3.12 -m engine.pipeline --state TAS

# Start the dashboard
py -3.12 -m uvicorn api.app:app --host 0.0.0.0 --port 8000

# Evaluate a specific site
py -3.12 scripts/evaluate_site.py "Beveridge VIC"

# Generate deal packages for top 10 sites
py -3.12 scripts/generate_deal_package.py --top 10

# Run profitability analysis
py -3.12 scripts/run_profitability.py --top 20

# Scan growth corridors
py -3.12 scripts/scan_growth_corridors.py --national --top 20

# Run tests
py -3.12 -m pytest tests/ -v
```

## Data Flow

1. **Scrape** → Pharmacy, GP, hospital, supermarket, shopping centre data into SQLite
2. **Import** → ABS boundaries, population, AIHW hospitals
3. **Evaluate** → Rules engine tests every candidate against Items 130-136
4. **Score** → Commercial scoring + profitability estimation
5. **Match** → Find available commercial properties near qualifying sites
6. **Monitor** → Track growth corridors, DAs, developer pipelines, ACPA decisions
7. **Report** → Dashboard, PDFs, daily/weekly summaries, deal packages
8. **Alert** → Watchlist triggers, competitor monitoring, growth corridor alerts

## Scoring System

### Compliance Confidence (0.0 - 1.0)
- HIGH (>0.8): Strong compliance, high certainty
- MEDIUM (0.5-0.8): Likely compliant, some uncertainty
- LOW (<0.5): Marginal, needs detailed assessment

### Commercial Score (0.0 - 1.0)
Weighted factors: population density (25%), competition (20%), GP proximity (15%), transport access (15%), growth potential (10%), demographics (10%), visibility (5%)

### Profitability Score (0 - 100)
Based on: estimated revenue, gross profit, payback period, flip profit potential

### Growth Corridor Score (0 - 100)
Based on: population growth rate (30%), pharmacy ratio (25%), distance to pharmacy (20%), planned retail (15%), PSP status (10%)
