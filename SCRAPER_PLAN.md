# PharmacyFinder Scraper Plan - findapharmacy.com.au

## Executive Summary

**Data Source:** findapharmacy.com.au (Pharmacy Guild of Australia)
**Data Available:**
- Pharmacy name
- Full address
- Opening hours
- GPS coordinates (lat/lng from Google Maps links)
- Services offered (filterable)

**Estimated Coverage:** ~5,700+ pharmacies nationwide (Australia has ~5,700 community pharmacies)

---

## Discovery Findings

### URL Structure
```
https://findapharmacy.com.au/home
```
- Search is dynamic (JavaScript-rendered)
- Uses town/state input + optional service filter
- Returns pharmacy cards with structured data

### Data Extraction Points

Each pharmacy card contains:
```
<heading>Pharmacy Name</heading>
<strong>Open:</strong> HH:MM to HH:MM | Open/Closed now
<strong>Address:</strong> Full address with suburb, state, postcode
<link>Get directions</link> → URL contains: destination=LONGITUDE,LATITUDE
```

**Critical insight:** GPS coordinates are embedded in Google Maps direction URLs:
```
https://www.google.com/maps/dir/?api=1&destination=151.207266,-33.8650923
```
Format: `destination=LONGITUDE,LATITUDE` (note: longitude first!)

### Search Strategy

**Option A: Search by Suburb/Town**
- Requires list of all Australian suburbs (~16,000+)
- More precise but slower
- Risk of missing pharmacies if geocoding differs

**Option B: Search by Postcode**
- ~2,600 postcodes in Australia
- More manageable
- Better coverage guarantee
- May have duplicates (same pharmacy near postcode boundaries)

**Option C: Search by State + Pagination**
- Search each state (NSW, VIC, QLD, etc.)
- Paginate through all results
- Cleanest approach if pagination is supported

**Recommended: Option B (Postcode iteration) with deduplication**

---

## Technical Architecture

### Scraper Components

```
scrapers/
└── findapharmacy.py          # Main scraper
    ├── FindAPharmacyScraper  # Class
    │   ├── search_by_location(town_state)
    │   ├── extract_pharmacies(page_content)
    │   ├── parse_pharmacy_card(element)
    │   ├── parse_coordinates(maps_url)
    │   ├── scrape_all_australia()
    │   └── deduplicate_results()
```

### Data Flow
```
1. Load postcode list (or suburb list)
2. For each postcode/suburb:
   a. Navigate to findapharmacy.com.au/home
   b. Enter location in search
   c. Click "Take me there"
   d. Wait for results to load
   e. Extract all pharmacy cards
   f. Parse name, address, coords, hours
   g. Store in database
3. Deduplicate by (name + address) or coordinates
4. Output: pharmacies.db table with all Australian pharmacies
```

### Browser Automation Required

**Why Selenium/Playwright is needed:**
- Page uses JavaScript for search
- Results load dynamically
- No static HTML endpoint found

**Implementation:**
- Use headless Chrome via Selenium
- Minimize requests with smart waiting
- Implement retry logic for failures

---

## Detailed Implementation Plan

### Phase 1: Foundation (1-2 hours)
1. Create `scrapers/findapharmacy.py`
2. Set up Selenium with headless Chrome
3. Implement basic navigation and search
4. Test with single suburb

### Phase 2: Data Extraction (2-3 hours)
1. Parse pharmacy card elements
2. Extract coordinates from Maps URLs
3. Handle edge cases (missing data, special chars)
4. Validate extracted data

### Phase 3: Scale (2-3 hours)
1. Load Australian postcode/suburb list
2. Implement iteration with progress tracking
3. Add rate limiting (1-2 sec delay)
4. Implement checkpoint/resume capability

### Phase 4: Deduplication & Storage (1-2 hours)
1. Define uniqueness criteria (coords + name)
2. Implement deduplication logic
3. Store in SQLite database
4. Export to CSV for verification

### Phase 5: Testing & Validation (1-2 hours)
1. Verify sample pharmacies against known data
2. Check coordinate accuracy
3. Ensure no major gaps in coverage
4. Document any limitations

---

## Rate Limiting & Ethics

### Respect Guidelines
- Rate limit: 1 request per 2 seconds minimum
- User-Agent: Identify as research tool
- robots.txt: Check compliance
- No parallel requests (sequential only)

### Expected Time
- ~2,600 postcodes × 3 seconds average = ~2+ hours for full scrape
- With retries and delays: ~3-4 hours total
- Run overnight or in background

---

## Alternative Data Sources (Backup)

If findapharmacy.com.au blocks or changes:

1. **Healthdirect Service Finder**
   - API available (may need key)
   - Government data source
   - Fallback option

2. **Chain Store Locators**
   - Chemist Warehouse: ~500+ stores
   - Priceline: ~470+ stores
   - TerryWhite Chemmart: ~500+ stores
   - Individual APIs/pages

3. **Google Places API**
   - Comprehensive but expensive
   - $17 per 1000 requests
   - Use only for validation

---

## Output Schema

### pharmacies table
```sql
CREATE TABLE pharmacies (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    address TEXT NOT NULL,
    suburb TEXT,
    state TEXT,
    postcode TEXT,
    latitude REAL NOT NULL,
    longitude REAL NOT NULL,
    opening_hours TEXT,
    source TEXT DEFAULT 'findapharmacy.com.au',
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(latitude, longitude, name)
);
```

### Expected Fields
| Field | Example | Notes |
|-------|---------|-------|
| name | "Metcentre Pharmacy" | Exact name from site |
| address | "Shop G39 Metcentre, 273 George Street, SYDNEY NSW 2000" | Full address |
| suburb | "SYDNEY" | Parsed from address |
| state | "NSW" | Parsed from address |
| postcode | "2000" | Parsed from address |
| latitude | -33.8650923 | From Maps URL |
| longitude | 151.207266 | From Maps URL |
| opening_hours | "07:30 to 18:00" | If available |

---

## Risk Assessment

### Risks & Mitigations

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| Site structure changes | Medium | High | Version pin selectors, monitor for changes |
| IP blocking | Low | Medium | Rate limit, use delays, residential IP |
| Incomplete data | Medium | Medium | Cross-reference with chain store locators |
| Coordinate errors | Low | High | Validate against known addresses |
| Missing pharmacies | Low | Medium | Multiple search strategies |

---

## Success Criteria

1. **Coverage:** >5,000 pharmacies captured (87%+ of ~5,700 total)
2. **Accuracy:** 99%+ coordinates match actual locations
3. **Completeness:** Name, address, coords for all entries
4. **Performance:** Full scrape completes in <6 hours
5. **Reliability:** Checkpoint/resume works if interrupted

---

## Next Steps

**Immediate:**
1. [ ] Create `scrapers/findapharmacy.py` with basic structure
2. [ ] Test Selenium navigation on findapharmacy.com.au
3. [ ] Implement single-location scrape
4. [ ] Validate data extraction

**After Validation:**
1. [ ] Load Australian postcode list
2. [ ] Implement full iteration
3. [ ] Run full scrape (overnight)
4. [ ] Verify and deduplicate results

---

## Code Structure Preview

```python
class FindAPharmacyScraper:
    def __init__(self, db: Database):
        self.db = db
        self.driver = None
        self.base_url = "https://findapharmacy.com.au/home"
        
    def setup_driver(self):
        """Initialize headless Chrome"""
        
    def search_location(self, location: str) -> List[Dict]:
        """Search for pharmacies near a location"""
        
    def extract_pharmacy_data(self, card_element) -> Dict:
        """Extract data from a single pharmacy card"""
        
    def parse_coordinates(self, maps_url: str) -> Tuple[float, float]:
        """Extract lat/lng from Google Maps URL"""
        
    def scrape_by_postcode(self, postcode: str) -> List[Dict]:
        """Scrape all pharmacies for a given postcode"""
        
    def scrape_all_australia(self):
        """Iterate through all postcodes and scrape"""
        
    def deduplicate(self):
        """Remove duplicate entries based on coords + name"""
```

---

## Approval Requested

**Before proceeding:**
1. Confirm approach (postcode iteration)
2. Confirm rate limiting (2 sec minimum)
3. Confirm data storage (SQLite + CSV export)
4. Confirm scope (all Australia or specific states first?)

Ready to implement on approval.
