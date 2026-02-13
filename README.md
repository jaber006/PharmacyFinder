# Pharmacy Location Finder

A Python tool for finding commercial properties eligible for new PBS-approved pharmacies under Australian Pharmacy Location Rules.

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Run full pipeline (Tasmania as test region)
python main.py --all --region TAS

# Run with sample properties (for testing)
python main.py --all --region TAS --use-samples
```

**No API keys needed!** Uses free OpenStreetMap data for all location data.

## What It Does

1. **Scrapes reference data** from OpenStreetMap: pharmacies, GPs, supermarkets, hospitals
2. **Scrapes commercial property listings** (or generates test samples)
3. **Checks each property** against 8 Australian Pharmacy Location Rules
4. **Generates outputs**: CSV report + interactive HTML map

## Data Sources (All Free)

| Data | Source | Coverage |
|------|--------|----------|
| Pharmacies | OpenStreetMap Overpass API | ~82 in TAS, nationwide |
| GP Practices | OpenStreetMap (doctors/clinics) | ~95 in TAS |
| Supermarkets | OpenStreetMap | ~161 in TAS |
| Hospitals | Curated list + OpenStreetMap | ~36 in TAS |
| Driving distances | OSRM (free routing) | Worldwide |
| Geocoding | Nominatim (OpenStreetMap) | Worldwide |

## Implemented Rules

| Rule | Description | Criteria |
|------|------------|----------|
| **Item 130** | New Pharmacy | >= 1.5km from nearest pharmacy + supermarket/GP within 500m |
| **Item 131** | Rural Pharmacy | >= 10km by road from nearest pharmacy |
| **Item 132** | Major Shopping Centre | Within centre with >= 15,000 sqm GLA |
| **Item 133** | Supermarket | Adjacent to supermarket >= 1,000 sqm |
| **Item 134** | Small Shopping Centre | Within 5,000-15,000 sqm centre with supermarket |
| **Item 134A** | Very Remote | >= 90km straight-line from nearest pharmacy |
| **Item 135** | Hospital | Within/adjacent to hospital with >= 100 beds |
| **Item 136** | Medical Centre | Near cluster of 8+ FTE prescribers |

## CLI Usage

```bash
# Full workflow
python main.py --all --region TAS

# Individual steps
python main.py --update-reference-data --region TAS
python main.py --scrape-properties --region TAS --limit 50
python main.py --check-eligibility
python main.py --generate-outputs

# Options
python main.py --all --region TAS --use-samples    # Use sample properties
python main.py --all --region TAS --limit 25        # Limit properties
python main.py --stats                              # Show database stats
```

### Supported Regions

`NSW`, `VIC`, `QLD`, `WA`, `SA`, `TAS`, `NT`, `ACT`

## Output Files

Generated in `output/` directory:

- **`eligible_properties.csv`** - All eligible properties with evidence
- **`eligible_properties_map.html`** - Interactive map with:
  - Eligible properties (star markers, color-coded by rule)
  - Existing pharmacies (red dots)
  - GP practices (green dots)
  - Layer controls to toggle visibility

## Example Results (Tasmania Test Run)

```
Reference Data:
  Pharmacies:   82
  GP Practices: 95
  Supermarkets: 161
  Hospitals:    36

Properties checked: 25
Eligible properties: 6

Matches found:
  - Huonville: Item 130 (1.5km gap) + Item 131 (rural 10km+)
  - Hobart CBD: Item 135 (near Royal Hobart Hospital, 550 beds)
  - Longford: Item 131 (17.8km by road from nearest pharmacy)
  - Campbell Town: Item 131 (56km straight-line, ~73km by road)
  - New Norfolk: Item 131 (16km from nearest pharmacy)
```

## Project Structure

```
PharmacyFinder/
  main.py                   # CLI entry point
  config.py                 # Configuration
  opportunity_scanner.py    # Development news + medical center scanner
  scrapers/
    pharmacies.py           # OSM pharmacy scraper
    gps.py                  # OSM GP/clinic scraper
    supermarkets.py         # OSM supermarket scraper
    hospitals.py            # Known hospitals + OSM
    commercial_re.py        # RE listing scraper + sample generator
    development_news.py     # Growth signal scanner
    medical_centers.py      # Medical center opportunity finder
  rules/
    base_rule.py            # Abstract base class
    item_130.py - item_136.py  # Rule implementations
  utils/
    database.py             # SQLite operations
    distance.py             # Haversine + OSRM routing
    geocoding.py            # Nominatim geocoding
```

## Opportunity Scanner

A higher-level tool that monitors development news for growth signals:

```bash
# Full scan: news monitoring + medical center analysis
python opportunity_scanner.py --mode full --days 30

# Scan specific area
python opportunity_scanner.py --mode area --suburb "South Burnie" --state TAS
```

## Data Import

Import your own data via CSV:

```python
from utils.database import Database
from utils.geocoding import Geocoder
from scrapers.pharmacies import PharmacyScraper

db = Database(); db.connect()
geocoder = Geocoder(db=db)
scraper = PharmacyScraper(db, geocoder)
scraper.import_from_csv('my_pharmacies.csv')  # name, address, latitude, longitude
```

## Requirements

- Python 3.10+
- No API keys (all data sources are free)
- See `requirements.txt` for packages

## Limitations

- Coordinates are building centroids, not exact door locations
- OSM data coverage varies by region (urban areas have better coverage)
- Commercial RE sites block automated scraping (sample data used as fallback)
- Some rules require manual verification (especially Item 132, 136)
- FTE data for GPs is estimated (1.0 FTE per practice by default)

## Disclaimer

This tool is for preliminary screening only. Results should be verified against:
- Australian Community Pharmacy Authority
- Pharmacy Location Rules Applicant's Handbook (v1.9, January 2024)
- Official government data sources
