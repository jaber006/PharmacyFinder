# Pharmacy Location Finder

A Python tool for finding commercial properties eligible for new PBS-approved pharmacies under Australian Pharmacy Location Rules.

## Overview

This tool automates the process of:
1. Scraping commercial real estate listings for tenancies for lease
2. Checking each property's eligibility against 7 specific Australian Pharmacy Location Rules
3. Generating outputs with eligible properties, evidence, and contact information

## Implemented Rules

The tool checks properties against these location rules from the Pharmacy Location Rules Applicant's Handbook:

- **Item 130**: Remote Location (4km from nearest pharmacy)
- **Item 131**: GP Proximity (1.5km + 2 FTE GPs)
- **Item 132**: Major Shopping Centre (15,000+ sqm GLA)
- **Item 133**: Supermarket (1,000+ sqm)
- **Item 134**: Small Shopping Centre (5,000-15,000 sqm + supermarket)
- **Item 134A**: Very Remote Location (90km from nearest pharmacy)
- **Item 135**: Hospital (100+ beds)
- **Item 136**: Medical Centre (4+ FTE GPs)

## Features

- Scrapes commercial property listings from multiple sources
- Validates against pharmacy location rules
- Calculates straight-line and driving distances
- FTE (Full-Time Equivalent) calculations for GPs
- SQLite database for efficient data storage
- CSV export with all eligible properties
- Interactive HTML map with color-coded markers
- Support for manual data import via CSV
- Geocoding with caching to minimize API costs

## Requirements

- Python 3.10+
- **No API keys required!** Uses free Nominatim (OpenStreetMap) for geocoding
- Healthdirect API key (optional, for automated pharmacy/GP data)
- Woolworths API key (optional, for automated supermarket data)
- MyHospitals API key (optional, for automated hospital data)

## Installation

1. Clone or download this repository

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. **(Optional)** Create a `.env` file if you want to add optional API keys:
```bash
cp .env.example .env
# Edit .env to add optional API keys
```

The tool uses **free Nominatim (OpenStreetMap)** for geocoding by default, so no API keys are required to get started!

## Usage

### Complete Workflow

Run the entire workflow (update data, scrape properties, check eligibility, generate outputs):

```bash
python main.py --all --region NSW
```

### Individual Steps

Update reference data (pharmacies, GPs, supermarkets, hospitals):
```bash
python main.py --update-reference-data --region NSW
```

Scrape commercial property listings:
```bash
python main.py --scrape-properties --region NSW --limit 100
```

Check properties against eligibility rules:
```bash
python main.py --check-eligibility
```

Generate outputs (CSV and HTML map):
```bash
python main.py --generate-outputs
```

### Command Line Options

```
--all                    Run complete workflow
--update-reference-data  Update pharmacies, GPs, supermarkets, hospitals
--scrape-properties      Scrape commercial property listings
--check-eligibility      Check properties against rules
--generate-outputs       Generate CSV and HTML map
--region STATE           State/territory code (default: NSW)
--output-dir DIR         Output directory (default: output)
--limit N                Max properties to scrape (default: 100)
```

## Manual Data Import

Since some data sources require manual collection, you can import data from CSV files:

### Import Pharmacies

```python
from utils.database import Database
from utils.geocoding import Geocoder
from scrapers.pharmacies import PharmacyScraper
import config

db = Database()
db.connect()
geocoder = Geocoder(config.GOOGLE_MAPS_API_KEY, db)
scraper = PharmacyScraper(db, geocoder)

scraper.import_from_csv('pharmacies.csv')
db.close()
```

CSV format: `name, address, latitude (optional), longitude (optional)`

### Import GP Practices

```python
from scrapers.gps import GPScraper

scraper = GPScraper(db, geocoder)
scraper.import_from_csv('gps.csv')
```

CSV format: `name, address, hours_per_week, latitude (optional), longitude (optional)`

### Import Supermarkets

```python
from scrapers.supermarkets import SupermarketScraper

scraper = SupermarketScraper(db, geocoder)
scraper.import_from_csv('supermarkets.csv')
```

CSV format: `name, address, floor_area_sqm (optional), latitude (optional), longitude (optional)`

### Import Hospitals

```python
from scrapers.hospitals import HospitalScraper

scraper = HospitalScraper(db, geocoder)
scraper.import_from_csv('hospitals.csv')
```

CSV format: `name, address, bed_count, hospital_type (optional), latitude (optional), longitude (optional)`

### Import Commercial Properties

```python
from scrapers.commercial_re import CommercialREScraper

scraper = CommercialREScraper(db, geocoder)
scraper.import_from_csv('properties.csv')
```

CSV format: `address, listing_url (optional), property_type (optional), size_sqm (optional), agent_name (optional), agent_phone (optional), agent_email (optional)`

## Output Files

The tool generates two output files in the `output/` directory:

### 1. eligible_properties.csv

Columns:
- Address
- Latitude, Longitude
- Listing URL
- Qualifying Rules
- Evidence
- Agent Name, Phone, Email
- Date Checked

### 2. eligible_properties_map.html

Interactive map with:
- Color-coded markers by rule type
- Popups with property details
- Links to listings
- Legend for rule types

## Project Structure

```
pharmacy-location-finder/
├── main.py                          # Main orchestration and CLI
├── config.py                        # Configuration and API keys
├── requirements.txt                 # Python dependencies
├── .env.example                     # Example environment variables
├── README.md                        # This file
├── scrapers/
│   ├── commercial_re.py            # Commercial property scraper
│   ├── pharmacies.py               # Pharmacy location scraper
│   ├── gps.py                      # GP practice scraper
│   ├── supermarkets.py             # Supermarket scraper
│   └── hospitals.py                # Hospital scraper
├── rules/
│   ├── base_rule.py                # Abstract base class
│   ├── item_130.py                 # 4km remote location
│   ├── item_131.py                 # GP proximity
│   ├── item_132.py                 # Major shopping centre
│   ├── item_133.py                 # Supermarket
│   ├── item_134.py                 # Small shopping centre
│   ├── item_134a.py                # 90km very remote
│   ├── item_135.py                 # Hospital
│   └── item_136.py                 # Medical centre
├── utils/
│   ├── database.py                 # SQLite operations
│   ├── distance.py                 # Distance calculations
│   └── geocoding.py                # Google Maps geocoding
└── output/                         # Generated outputs
    ├── eligible_properties.csv
    └── eligible_properties_map.html
```

## Important Implementation Notes

### Distance Measurements

- All distances measured from ground-level public access door midpoints
- Approximated using building/address centroids from geocoding
- Straight-line distance uses Haversine formula
- Driving routes use OSRM routing service

### FTE Calculations

- Full-time equivalent: 38 hours/week = 1.0 FTE
- Minimum 20 hours/week required to count as GP
- Calculated as: `hours_per_week / 38`

### GLA vs Floor Area

- **GLA** (Gross Leasable Area): Used for shopping centres
- **Floor Area**: Used for individual stores/supermarkets

### Shopping Centre Proximity

- Property considered "within" centre if within 200m
- Adjust threshold in code if needed for specific cases

### Supermarket/Hospital Proximity

- Supermarket: Within 100m considered "adjoining"
- Hospital: Within 300m considered "within premises or adjacent"

### Medical Centre Definition

- GPs operating from same location (within 50m)
- Allows for minor geocoding variance

## Web Scraping Notes

The commercial real estate scrapers (`scrapers/commercial_re.py`) provide templates for scraping property listings. Actual implementation requires:

1. Website-specific CSS selectors
2. Handling of dynamic content (JavaScript)
3. Pagination logic
4. Rate limiting and respectful scraping
5. Compliance with robots.txt

Manual property import via CSV is recommended as an alternative to web scraping.

## Data Sources

Reference the Pharmacy Location Rules Applicant's Handbook for complete rule details:
- **Included in this repository**: `pharmacy-location-rules-applicant-s-handbook.pdf`
- **Version**: 1.9 (January 2024)
- **Publisher**: Australian Government Department of Health and Aged Care

## Limitations

1. **Geocoding Accuracy**: Coordinates represent building centroids, not exact door locations
2. **Data Freshness**: Reference data requires manual updates
3. **Rule Simplifications**: Some complex rule aspects may need manual verification
4. **Shopping Centre Data**: Requires manual collection of GLA and tenant information
5. **Web Scraping**: Commercial RE scrapers are templates requiring site-specific implementation

## Legal and Ethical Considerations

- Respect website terms of service
- Follow robots.txt directives
- Implement rate limiting
- Do not overload servers
- Verify eligibility with official channels before making decisions
- This tool provides preliminary screening only

## Geocoding

The tool uses **Nominatim (OpenStreetMap)** for geocoding by default:
- **Completely free**
- **No API key required**
- **No billing or quotas**
- Rate limited to 1 request per second (automatically handled)

### Alternative: Google Maps API

If you prefer Google Maps for geocoding accuracy, you can:
1. Get an API key from [Google Cloud Console](https://console.cloud.google.com/)
2. Modify `utils/geocoding.py` to use Google Maps instead
3. Note: This will incur costs after the free tier

### Healthdirect API

Visit [Healthdirect Developer Portal](https://developer.healthdirect.gov.au/) to request API access.

### Other APIs

- **Woolworths**: Contact Woolworths corporate for API access
- **MyHospitals**: Contact Australian Institute of Health and Welfare

## Support

For issues with:
- **Pharmacy Location Rules**: Contact Australian Community Pharmacy Authority
- **This Tool**: Create an issue in the repository or contact the developer

## License

This tool is provided as-is for research and preliminary screening purposes. Users are responsible for verifying all information and complying with applicable regulations.

## Disclaimer

This tool is not affiliated with or endorsed by:
- Australian Government Department of Health and Aged Care
- Australian Community Pharmacy Authority
- Pharmacy Guild of Australia

Results should be verified with official sources before making any decisions.
