# PharmacyFinder - Plan

## Vision

**Proactive pharmacy opportunity zone finder** -- instead of checking if a
specific property qualifies, scan the entire map to find *where* in Australia
a new pharmacy could be opened under the Pharmacy Location Rules.

## Architecture

```
main.py  (CLI entry-point)
  |
  |-- collect_reference_data()     # scrapers pull data into SQLite
  |-- scan_opportunities()         # scanner runs rules in reverse
  |-- cross_reference_properties() # (optional) match zones to RE listings
  |
  +-- scanner/
  |     zone_scanner.py   - POI-based rule scanner
  |     output.py         - CSV + HTML map + console summary
  |
  +-- scrapers/           - OSM Overpass + curated data scrapers
  |     pharmacies.py
  |     gps.py
  |     supermarkets.py
  |     hospitals.py
  |     shopping_centres.py  # NEW: curated + OSM shopping centre data
  |     medical_centers.py
  |     commercial_re.py
  |     development_news.py
  |
  +-- rules/              - Legacy per-property eligibility checkers
  |     base_rule.py, item_130.py ... item_136.py
  |
  +-- utils/
        database.py       - SQLite (reference data + opportunities)
        distance.py       - Haversine, OSRM routing
        geocoding.py      - Nominatim (free)
        boundaries.py     - State bounding box validation
```

## Workflow

### Primary: Zone Scan (`python main.py scan`)

1. **Collect** -- scrape pharmacies, GPs, supermarkets, hospitals, shopping
   centres from OSM + curated data into SQLite.
2. **Scan** -- for each Location Rule, iterate over relevant POIs and find
   locations that satisfy the rule *in reverse*:
   - Item 130: supermarkets/GPs >= 1.5 km from nearest pharmacy
   - Item 131: any POI >= 10 km by road from nearest pharmacy
   - Item 132: shopping centres >= 15,000 sqm without a pharmacy
   - Item 133: supermarkets >= 1,000 sqm without adjacent pharmacy
   - Item 134: shopping centres 5,000-15,000 sqm without a pharmacy
   - Item 134A: any POI >= 90 km from nearest pharmacy
   - Item 135: hospitals >= 100 beds without adjacent pharmacy
   - Item 136: GP clusters with >= 8 FTE and no nearby pharmacy
3. **De-duplicate** -- merge opportunities within 200 m.
4. **Reverse-geocode** top opportunities to human-readable addresses.
5. **Output** -- interactive HTML map, CSV, console summary.

### Optional: Property Cross-Reference (`--with-properties`)

After scanning, scrape commercial RE listings and check each against the
rules. Overlays property pins on the opportunity zone map.

## Data Sources (all free)

| Data              | Source                                  |
|-------------------|-----------------------------------------|
| Pharmacies        | OSM Overpass + Healthdirect             |
| GPs / Clinics     | OSM Overpass + Healthdirect             |
| Supermarkets      | OSM Overpass                            |
| Hospitals         | Curated list (AIHW) + OSM Overpass      |
| Shopping centres  | Curated list (GLA data) + OSM Overpass  |
| Medical centres   | GP cluster analysis + future scraper    |
| Commercial RE     | OSM + sample generator                  |
| Routing           | OSRM public server                      |
| Geocoding         | Nominatim (OpenStreetMap)               |

## Confidence Scoring

Each opportunity gets a confidence score (0-100%):

- **90%** -- Very remote (Item 134A), verified distance
- **85%** -- OSRM-verified route distance (Item 131), major shopping centres
- **80%** -- Large supermarket with clear pharmacy gap (Item 130 opt ii)
- **75%** -- Supermarket + GP combination (Item 130 opt i), major chains (Item 133)
- **70%** -- Estimated routes (Item 131), non-major supermarkets (Item 133)
- **65%** -- Medical centre data (Item 136)
- **60%** -- GP cluster proxy (Item 136, requires manual verification)

## Data Quality

- **Boundary validation**: all scraped POIs are checked against state
  bounding boxes to prevent cross-state data leaks from alternate
  Overpass servers
- **Supermarket floor areas**: estimated from chain name (Woolworths/Coles
  ~3,000 sqm, ALDI ~1,300 sqm, IGA ~1,200 sqm)
- **GP FTE**: defaults to 1.0 per practice (conservative estimate)
- **Hospital bed counts**: curated for major hospitals; OSM data used where
  `beds` tag is present

## Done

- [x] OSM scrapers for pharmacies, GPs, supermarkets, hospitals
- [x] Shopping centre scraper with curated GLA data
- [x] State bounding box validation for all scrapers
- [x] SQLite database with all reference tables + opportunities table
- [x] Zone scanner with all 8 rules implemented
- [x] Item 130 dual-scan (from supermarkets AND from GPs)
- [x] Item 133 expanded to all supermarkets >= 1,000 sqm (not just major chains)
- [x] De-duplication (merge nearby opportunities)
- [x] Reverse geocoding of top opportunities to addresses
- [x] Interactive HTML map with layered markers
- [x] CSV export
- [x] CLI with `scan`, `collect`, `check`, `stats` subcommands
- [x] Backward-compatible with legacy property-checking flags
- [x] Tested with Tasmania (123 opportunity zones found)

## TODO

- [ ] Dedicated medical centre scraper (for Item 136)
- [ ] Better hospital bed count data (AIHW full dataset)
- [ ] Batch OSRM processing with retry/backoff
- [ ] State-by-state nationwide scan with progress tracking
- [ ] Web dashboard (Streamlit or similar)
- [ ] Scheduled re-scanning with diff detection
- [ ] Shopping centre database expansion (Property Council data)
