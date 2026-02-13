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

1. **Collects reference data** — pharmacies, GPs, supermarkets, hospitals
   from OpenStreetMap and Healthdirect (all free, no API keys needed)
2. **Scans for opportunity zones** — checks every POI against 8 Location
   Rules to find gaps where a new pharmacy could get PBS approval
3. **Outputs results** — interactive HTML map + CSV + console summary

### Rules Scanned

| Rule      | What it finds |
|-----------|---------------|
| Item 130  | Areas >= 1.5 km from any pharmacy with a supermarket + GP nearby |
| Item 131  | Areas >= 10 km by road from nearest pharmacy (rural) |
| Item 132  | Major shopping centres (15,000+ sqm) without a pharmacy |
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
    Hospitals:         36

  Scanning Item 130...  -> 20 candidates
  Scanning Item 131...  -> 64 candidates
  Scanning Item 133...  -> 19 candidates
  Scanning Item 134A... ->  7 candidates
  Scanning Item 135...  ->  3 candidates

  After de-duplication: 69 unique opportunity zones

  Top opportunities (by confidence):
    1. [90%] Walkers Supermarket        (Flinders Island - 149 km from pharmacy)
    2. [90%] Grassy Supermarket         (King Island - 176 km from pharmacy)
    3. [90%] Foodworks                  (King Island - 198 km from pharmacy)
    4. [85%] Hilly's IGA               (rural TAS - 17 km from pharmacy)
    5. [85%] Evandale General Store     (rural TAS - 13 km from pharmacy)
```

## Output Files

- `output/opportunity_zones_TAS.html` — Interactive map with all opportunity
  zones, existing pharmacies, supermarkets, GPs, and hospitals as toggleable
  layers
- `output/opportunity_zones_TAS.csv` — Spreadsheet with coordinates, rules,
  evidence, confidence scores

## Data Sources

All free, no API keys required:

- **OpenStreetMap** (Overpass API) — pharmacies, GPs, supermarkets, hospitals
- **Healthdirect** — additional pharmacy and GP coverage
- **OSRM** — driving distance calculations (public server)
- **Nominatim** — geocoding (OpenStreetMap)
- **AIHW** — curated hospital bed count data

## Project Structure

```
PharmacyFinder/
  main.py                 # CLI entry point
  config.py               # Configuration and thresholds
  scanner/
    zone_scanner.py       # Core POI-based opportunity scanner
    output.py             # Map, CSV, and summary generators
  scrapers/
    pharmacies.py         # OSM + Healthdirect pharmacy scraper
    gps.py                # OSM + Healthdirect GP scraper
    supermarkets.py       # OSM supermarket scraper
    hospitals.py          # Curated + OSM hospital scraper
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
  output/                 # Generated maps and CSVs
```

## Requirements

- Python 3.10+
- No paid API keys needed (all free data sources)
- Internet connection for OSM/OSRM/Healthdirect queries

## License

Private — not for redistribution.
