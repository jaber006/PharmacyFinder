# Pharmacy Location Finder - Implementation Plan

## Overview
Build a Python tool that scrapes commercial real estate listings in Australia and checks their eligibility against Australian Pharmacy Location Rules (Items 130-136).

## Architecture

### Core Components

1. **Data Collection Layer** (scrapers/)
   - Commercial real estate listing scrapers
   - Pharmacy location scrapers (NSW Register, Healthdirect API)
   - GP practice scrapers (Healthdirect API)
   - Supermarket location scrapers (Woolworths API, manual scraping)
   - Hospital data scrapers (MyHospitals API)
   - Shopping centre data collection

2. **Rule Engine** (rules/)
   - 7 rule modules (Items 130, 131, 132, 133, 134, 134A, 135, 136)
   - Each rule validates specific eligibility criteria
   - Evidence collection for qualifying properties

3. **Utilities Layer** (utils/)
   - Distance calculations (straight-line using Haversine, driving routes using OSRM)
   - Geocoding services (Google Maps Geocoding API)
   - Database operations (SQLite)
   - Data validation and sanitization

4. **Output Generation** (output/)
   - CSV export with all eligible properties
   - Interactive HTML map using Folium
   - Detailed evidence and contact information

## File Structure

```
pharmacy-location-finder/
├── main.py                          # Orchestration and CLI
├── config.py                        # API keys, constants, settings
├── requirements.txt                 # Dependencies
├── README.md                        # Documentation
├── scrapers/
│   ├── __init__.py
│   ├── commercial_re.py            # RE listings scraper
│   ├── pharmacies.py               # Pharmacy locations
│   ├── gps.py                      # GP practices
│   ├── supermarkets.py             # Supermarket locations
│   ├── hospitals.py                # Hospital data
│   └── shopping_centres.py         # Shopping centre data
├── rules/
│   ├── __init__.py
│   ├── base_rule.py                # Abstract base class
│   ├── item_130.py                 # 4km from nearest pharmacy
│   ├── item_131.py                 # 1.5km + 2 FTE GPs
│   ├── item_132.py                 # Shopping centre > 15,000 sqm GLA
│   ├── item_133.py                 # Supermarket (min 1,000 sqm)
│   ├── item_134.py                 # Shopping centre 5,000-15,000 sqm + supermarket
│   ├── item_134a.py                # 90km from nearest pharmacy
│   ├── item_135.py                 # Hospital (min 100 beds)
│   └── item_136.py                 # Medical centre (4+ FTE GPs)
├── utils/
│   ├── __init__.py
│   ├── distance.py                 # Distance calculations
│   ├── geocoding.py                # Address to coordinates
│   └── database.py                 # SQLite operations
└── output/
    └── (generated files)
```

## Implementation Details

### Phase 1: Foundation (config.py, utils/)

**config.py**
- API keys for: Google Maps, Healthdirect, Woolworths, MyHospitals
- Distance thresholds for each rule
- Database path and table schemas
- Rate limiting settings
- User agent strings for scrapers

**utils/distance.py**
- Haversine formula for straight-line distances
- OSRM API integration for driving routes
- Distance calculation between two coordinates
- Helper to find nearest facility from list

**utils/geocoding.py**
- Google Maps Geocoding API wrapper
- Address normalization
- Coordinate caching to minimize API calls
- Error handling for invalid addresses

**utils/database.py**
- SQLite connection management
- Schema creation:
  - properties table (address, coords, listing_url, date_scraped)
  - pharmacies table (name, address, coords, source)
  - gps table (name, address, coords, fte, hours)
  - supermarkets table (name, address, coords, floor_area)
  - hospitals table (name, address, coords, beds)
  - eligible_properties table (property_id, rule_item, evidence, contact)
- CRUD operations
- Query helpers for proximity searches

### Phase 2: Data Collection (scrapers/)

**scrapers/commercial_re.py**
- Scrape commercialrealestate.com.au:
  - Filter for retail/medical tenancies for lease
  - Extract: address, property type, size, listing URL, agent contact
  - Use Selenium for dynamic content
- Scrape realcommercial.com.au:
  - Similar extraction logic
  - Handle pagination
- Scrape Domain Commercial:
  - API approach if available, otherwise BeautifulSoup
- Focus on NSW initially (can expand later)
- Rate limiting: 1 request per 2 seconds
- Store in database

**scrapers/pharmacies.py**
- NSW Pharmacy Register scraping:
  - Extract all approved pharmacy locations
  - Parse address and approval status
- Healthdirect API:
  - Query pharmacy locations nationwide
  - Extract name, address, coordinates
- Deduplicate entries
- Store with source attribution

**scrapers/gps.py**
- Healthdirect API for GP practices:
  - Filter for general practitioners
  - Extract practice name, address, coordinates
  - Calculate FTE from hours data (38 hours/week = 1 FTE)
  - Minimum 20 hours/week to count
- Handle bulk operations (query by region)
- Store FTE calculations

**scrapers/supermarkets.py**
- Woolworths API for store locations:
  - Extract address, coordinates
  - Get floor area if available (metadata or manual lookup)
- Coles scraping (similar approach)
- ALDI, IGA if data available
- Manual floor area verification for critical stores
- Filter for stores >= 1,000 sqm

**scrapers/hospitals.py**
- MyHospitals API:
  - Extract public hospitals with 100+ beds
  - Get name, address, coordinates, bed count
  - Filter for acute care hospitals
- Store bed count for validation

**scrapers/shopping_centres.py**
- Manual data collection from:
  - Shopping centre websites
  - Property databases
  - Council records
- Key data: GLA (Gross Leasable Area), tenants, anchor stores
- Classification: major (15,000+ sqm) vs smaller (5,000-15,000 sqm)

### Phase 3: Rule Engine (rules/)

**rules/base_rule.py**
- Abstract base class with:
  - `check_eligibility(property, db_connection)` method
  - `get_evidence()` method
  - `rule_name` and `item_number` properties
- Common utilities for distance checks

**rules/item_130.py** - Remote Location (4km)
- Check straight-line distance to nearest pharmacy
- Distance measured from ground-level public access door midpoints
- If >= 4km, property qualifies
- Evidence: "Nearest pharmacy at [address] is [distance] km away"

**rules/item_131.py** - GP Proximity (1.5km + 2 FTE)
- Find all GPs within 1.5km straight-line distance
- Sum their FTE values
- If total >= 2.0 FTE, property qualifies
- Evidence: "[N] GPs within 1.5km totaling [X] FTE: [list]"

**rules/item_132.py** - Major Shopping Centre (15,000+ sqm)
- Check if property is within shopping centre
- Verify GLA >= 15,000 sqm
- Verify at least one major supermarket tenant (Woolworths/Coles/ALDI)
- Evidence: "[Centre name] has [X] sqm GLA with [supermarket]"

**rules/item_133.py** - Supermarket (1,000+ sqm)
- Check if property adjoins or is within supermarket premises
- Verify supermarket floor area >= 1,000 sqm
- Evidence: "Adjacent to [supermarket] with [X] sqm floor area"

**rules/item_134.py** - Small Shopping Centre + Supermarket
- Check if property is within shopping centre with 5,000-15,000 sqm GLA
- Verify major supermarket present
- Evidence: "[Centre name] has [X] sqm GLA with [supermarket]"

**rules/item_134a.py** - Very Remote Location (90km)
- Check straight-line distance to nearest pharmacy
- If >= 90km, property qualifies
- Evidence: "Nearest pharmacy at [address] is [distance] km away"

**rules/item_135.py** - Hospital (100+ beds)
- Check proximity to public hospital with >= 100 beds
- Verify pharmacy would be within hospital premises or adjacent
- Evidence: "[Hospital name] has [X] beds at [address]"

**rules/item_136.py** - Medical Centre (4+ FTE GPs)
- Check if property is within or adjacent to medical centre
- Count FTE GPs operating from the medical centre
- If >= 4.0 FTE, property qualifies
- Evidence: "Medical centre has [X] FTE GPs: [list]"

### Phase 4: Main Orchestration (main.py)

**Workflow:**
1. Initialize database and load reference data
2. Run scrapers to collect commercial property listings
3. For each property:
   - Geocode address to coordinates
   - Check against all 7 rules
   - Store results if any rule matches
4. Generate outputs:
   - CSV with columns: Address, Listing URL, Qualifying Rules, Evidence, Agent Contact
   - HTML map with markers colored by rule type
5. Display summary statistics

**CLI Arguments:**
- `--update-reference-data`: Re-scrape pharmacies, GPs, supermarkets, hospitals
- `--region`: Filter by state/region (default: NSW)
- `--output-dir`: Custom output directory
- `--verbose`: Detailed logging

### Phase 5: Output Generation

**CSV Export:**
- Columns: Address, Coordinates, Listing URL, Qualifying Rule Items, Evidence, Agent Name, Agent Phone, Agent Email, Date Checked
- Sort by number of qualifying rules (multi-rule properties first)

**HTML Map:**
- Folium interactive map centered on region
- Markers for each eligible property:
  - Color-coded by primary rule
  - Popup with full details and evidence
  - Link to listing URL
- Layer controls for different rule types
- Legend explaining color coding

## Testing Strategy

1. **Unit Tests:**
   - Distance calculations (verify Haversine accuracy)
   - Rule logic with known test cases
   - Geocoding with sample addresses

2. **Integration Tests:**
   - Scraper functionality with sample pages
   - Database operations
   - End-to-end with mock data

3. **Validation:**
   - Cross-reference sample properties with manual calculation
   - Verify distances using Google Maps
   - Test with known qualifying and non-qualifying properties

## Implementation Order

1. Set up project structure and requirements.txt
2. Implement utils/ (database, distance, geocoding)
3. Create config.py with placeholders for API keys
4. Implement scrapers/ for reference data (pharmacies, GPs, supermarkets, hospitals)
5. Implement rules/ engine (base class, then each rule)
6. Implement commercial_re.py scraper
7. Build main.py orchestration
8. Create output generators
9. Test with sample data
10. Document setup and usage in README.md

## Key Implementation Notes

- **Distance Measurement:** All distances from ground-level public access door midpoints (approximate using building centroids)
- **FTE Calculation:** 38 hours/week = 1.0 FTE, minimum 20 hours to count as GP
- **GLA vs Floor Area:** GLA for shopping centres, floor area for individual stores
- **Rate Limiting:** Respect robots.txt, add delays between requests
- **Error Handling:** Graceful degradation if geocoding fails or APIs unavailable
- **Caching:** Cache geocoding results and reference data to minimize API calls
- **Data Freshness:** Timestamp all scraped data, allow manual refresh

## Dependencies (requirements.txt)

```
requests>=2.31.0
beautifulsoup4>=4.12.0
selenium>=4.15.0
geopy>=2.4.0
folium>=0.15.0
pandas>=2.1.0
lxml>=4.9.0
webdriver-manager>=4.0.0
python-dotenv>=1.0.0
```

## Configuration Requirements

User must provide in `.env`:
- `GOOGLE_MAPS_API_KEY`
- `HEALTHDIRECT_API_KEY` (if required)
- `WOOLWORTHS_API_KEY` (if available)
- `MYHOSPITALS_API_KEY` (if required)

## Next Steps After Approval

1. Create file structure
2. Implement foundational utilities
3. Set up database schema
4. Build scrapers progressively
5. Implement rule checkers
6. Create main orchestration
7. Build output generators
8. Test and refine
