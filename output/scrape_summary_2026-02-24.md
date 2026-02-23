# National Pharmacy Scrape Summary — 24 Feb 2026

## Source
- **findapharmacy.com.au** via Funnelback API (paginated, 500/page)
- Scraped using Clawdbot browser control (bypasses Cloudflare)

## Scrape Results
- **Total in index: 4,288** pharmacies
- **All 4,288 scraped** with 0 parse errors
- 9 API pages fetched in ~15 seconds

### By State (scraped)
| State | Count |
|-------|-------|
| NSW   | 1,327 |
| QLD   | 940   |
| VIC   | 938   |
| WA    | 544   |
| SA    | 293   |
| TAS   | 146   |
| ACT   | 70    |
| NT    | 30    |

## Database Update
- **Before: 5,310** pharmacies
- **After: 5,322** pharmacies

### Changes
| Action    | Count |
|-----------|-------|
| Added     | 12    |
| Updated   | 332   |
| Unchanged | 3,944 |
| No longer in index | 31 |

- **Matched by name+postcode:** 4,255
- **Matched by coordinates:** 21
- **Other sources preserved:** 1,025 (chain sites, OSM, etc.)

### 31 Pharmacies No Longer in findapharmacy.com.au Index
These were present in the old scrape but not the new one (possibly closed/renamed):
- Priceline Pharmacy Adelaide City, SA
- Star Discount Chemist Hutt Street, SA
- South Road Pharmacy, SA
- TerryWhite Chemmart Malvern, SA
- And 27 others (see update_summary_2026-02-24.json)

## Pipeline Re-run
- **score_v2.py**: 142 qualifying opportunities scored
- **build_dashboard_v3.py**: Dashboard rebuilt (5,322 pharmacies, 142 opportunities)
- Dashboard: `output/dashboard.html` (764 KB)

## Files Created
- `output/national_pharmacies_2026-02-23.json` (5.7 MB) — full scrape data
- `output/update_summary_2026-02-24.json` — detailed update report
- `scrapers/scrape_national_pharmacies.py` — reusable scraper script
- `scrapers/update_pharmacy_db.py` — reusable DB update script
- `pharmacy_finder_backup_20260224_000108.db` — pre-update backup

## Notes
- The findapharmacy.com.au index contains **4,288** pharmacies (Guild members + non-members)
- The existing DB had 4,285 from findapharmacy + 1,025 from chain websites = 5,310
- The fresh scrape found 3 more from findapharmacy than the original + 12 entirely new ones
- The Funnelback API now supports pagination (`start_rank` parameter), making geographic overlap queries unnecessary
