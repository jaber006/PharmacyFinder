# PharmacyFinder

**Proactive opportunity zone finder** — discover where in Australia a new
pharmacy can be opened under the
[Pharmacy Location Rules](https://www.health.gov.au/topics/pharmacist-services/pharmacy-location-rules).

Instead of checking individual properties, PharmacyFinder scans the entire map
and runs the Location Rules *in reverse* to find every location that qualifies.

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Scan Tasmania for opportunity zones (collects data + scans + outputs)
python main.py scan --region TAS

# Scan with existing data (skip re-collection)
python main.py scan --region TAS --skip-collect

# Also find commercial properties in those zones
python main.py scan --region TAS --with-properties
```

## How It Works

1. **Collects reference data** — pharmacies, GPs, supermarkets, hospitals,
   shopping centres from OpenStreetMap (plus curated hospital and shopping
   centre data with bed counts / GLA)
2. **Scans for opportunity zones** — checks every POI against 8 Location
   Rules to find gaps where a new pharmacy could get PBS approval
3. **Reverse-geocodes** top opportunities to human-readable addresses
4. **Outputs results** — interactive HTML map + CSV + console summary

### Rules Scanned

| Rule      | What it finds |
|-----------|---------------|
| Item 130  | Areas >= 1.5 km from any pharmacy with a supermarket + GP nearby |
| Item 131  | Areas >= 10 km by road from nearest pharmacy (rural) |
| Item 132  | Major shopping centres (15,000+ sqm GLA) without a pharmacy |
| Item 133  | Supermarkets >= 1,000 sqm without an adjacent pharmacy |
| Item 134  | Shopping centres 5,000-15,000 sqm with supermarket but no pharmacy |
| Item 134A | Areas >= 90 km straight-line from nearest pharmacy (very remote) |
| Item 135  | Hospitals with 100+ beds without an adjacent pharmacy |
| Item 136  | Medical centres with 8+ FTE prescribers without a nearby pharmacy |

## CLI Reference

```
python main.py <command> [options]

Commands:
  scan      Collect data + scan for opportunity zones (recommended)
  collect   Collect reference data only
  check     Legacy mode: check specific commercial properties
  stats     Show database statistics

Scan options:
  --region CODE       State/territory (default: TAS)
  --skip-collect      Use existing data in the database
  --with-properties   Also scrape commercial RE listings
  --output-dir DIR    Output directory (default: output/)

Regions: NSW, VIC, QLD, WA, SA, TAS, NT, ACT
```

## Example Output (Tasmania)

```
SCANNING FOR OPPORTUNITY ZONES - Tasmania

  Reference data loaded:
    Pharmacies:        60
    Supermarkets:      118
    GP practices:      89
    Hospitals:         38
    Shopping centres:  10

  Scanning Item 130...  -> 36 candidates
  Scanning Item 131...  -> 64 candidates
  Scanning Item 132...  ->  2 candidates
  Scanning Item 133...  -> 94 candidates
  Scanning Item 134...  ->  7 candidates
  Scanning Item 134A... ->  7 candidates
  Scanning Item 135...  ->  5 candidates

  After de-duplication: 123 unique opportunity zones

  Top opportunities (by confidence):
    1. [90%] Walkers Supermarket  (Flinders Island - 149 km from pharmacy)
    2. [90%] Grassy Supermarket   (King Island - 176 km from pharmacy)
    3. [90%] Foodworks            (King Island - 198 km from pharmacy)
    4. [85%] Hilly's IGA          (St Helens - 17 km from pharmacy)
    5. [85%] Evandale General Store (rural TAS - 13 km from pharmacy)
```

## Post-Scan Tools

### Opportunity Verification

Cross-references opportunity zones against live OpenStreetMap pharmacy data
to find false positives (pharmacies that exist but aren't in our database).

```bash
# Verify a single state
python verify_opportunities.py --state TAS

# Verify all states
python verify_opportunities.py --all

# Only verify top 20 opportunities per state
python verify_opportunities.py --state NSW --top 20
```

Outputs `verified_opportunities_<STATE>.csv` with columns:
- **Verification**: `VERIFIED`, `FALSE POSITIVE`, `NEEDS REVIEW`, or `UNVERIFIED`
- **Verification Notes**: Details about what was found
- Any newly discovered pharmacies are automatically added to the database

### Population Overlay

Adds estimated population data to each opportunity zone to help prioritize.
A gap near 100,000 people is more valuable than one near 500.

```bash
# Add population data for one state
python population_overlay.py --state TAS

# All states
python population_overlay.py --all
```

Outputs `population_ranked_<STATE>.csv` with:
- **Pop 5km/10km/15km**: Estimated population within radius
- **Nearest Town**: Closest significant settlement
- **Opportunity Score**: Composite score (population x distance x confidence)

## Output Files

- `output/opportunity_zones_TAS.html` — Interactive map with all opportunity
  zones, existing pharmacies, supermarkets, GPs, and hospitals as toggleable
  layers
- `output/opportunity_zones_TAS.csv` — Spreadsheet with coordinates, rules,
  evidence, confidence scores, and reverse-geocoded addresses
- `output/verified_opportunities_TAS.csv` — Verified opportunities with
  false positive flags
- `output/population_ranked_TAS.csv` — Opportunities ranked by population score

## Data Sources

All free, no API keys required:

| Data | Source |
|------|--------|
| Pharmacies | OpenStreetMap (Overpass API) + Healthdirect |
| GPs / Clinics | OpenStreetMap (Overpass API) + Healthdirect |
| Supermarkets | OpenStreetMap (Overpass API) |
| Hospitals | Curated list (AIHW bed counts) + OpenStreetMap |
| Shopping centres | Curated list (GLA data) + OpenStreetMap |
| Driving distances | OSRM (public server) |
| Geocoding | Nominatim (OpenStreetMap) |

## Confidence Scoring

Each opportunity gets a confidence score based on data quality:

| Score | Meaning |
|-------|---------|
| 90% | Very remote (Item 134A) — verified straight-line distance |
| 85% | OSRM-verified route distance (Item 131), major shopping centres (Item 132) |
| 80% | Large supermarket with clear pharmacy gap (Item 130 opt ii), hospitals (Item 135) |
| 75% | Supermarket + GP combination (Item 130 opt i), major chain supermarkets (Item 133) |
| 70% | Estimated routes (Item 131), non-major supermarkets |
| 65% | Medical centre data (Item 136) |
| 60% | GP cluster proxy (Item 136, requires manual verification) |

## Project Structure

```
PharmacyFinder/
  main.py                 # CLI entry point + orchestrator
  config.py               # Configuration and thresholds
  verify_opportunities.py # Opportunity verification against live OSM data
  population_overlay.py   # Population data overlay and scoring
  scanner/
    zone_scanner.py       # Core POI-based opportunity scanner
    output.py             # Map, CSV, and summary generators
  scrapers/
    pharmacies.py         # OSM + Healthdirect pharmacy scraper
    gps.py                # OSM + Healthdirect GP scraper
    supermarkets.py       # OSM supermarket scraper
    hospitals.py          # Curated + OSM hospital scraper
    shopping_centres.py   # Curated + OSM shopping centre scraper
    commercial_re.py      # Commercial real estate scraper
    medical_centers.py    # Medical centre scanner
    development_news.py   # Development news monitor
  rules/
    base_rule.py          # Abstract base rule
    item_130.py - item_136.py  # Per-property rule checkers (legacy)
  utils/
    database.py           # SQLite database layer
    distance.py           # Haversine + OSRM distance utils
    geocoding.py          # Nominatim geocoder
    boundaries.py         # State bounding box validation
    overpass_cache.py     # Overpass API caching layer (7-day cache + failover)
  cache/                  # Cached Overpass API responses (gitignored)
  output/                 # Generated maps and CSVs
```

## Requirements

- Python 3.10+
- No paid API keys needed (all free data sources)
- Internet connection for OSM/OSRM/Healthdirect queries

## Limitations

- **Shopping centre GLA** — OSM doesn't tag GLA reliably; curated data
  covers major centres but may miss smaller ones
- **GP FTE** — defaults to 1.0 per practice; real FTE data would improve
  Item 130/136 accuracy
- **Hospital bed counts** — curated for major hospitals; OSM hospitals
  often have 0 beds tagged
- **Population data** — OSM only has population tags for ~5-10% of places;
  remainder uses conservative estimates based on settlement type
- **Rate limiting** — Overpass API has rate limits; the caching layer
  mitigates this with 7-day caches and mirror failover
- **Road distance** — public OSRM server has rate limits; self-hosted
  OSRM would allow faster batch processing

## License

Private -- not for redistribution.
