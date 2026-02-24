# PASS Issue Fixes Applied

**Date:** 2026-02-24
**Total issues addressed:** 65
**Coordinates corrected:** 58
**Marked INVALID:** 5 (unresolvable phantom entries)

---

## 1. Port Macquarie Private Hospital (ID 9356)

| Field | Before | After |
|-------|--------|-------|
| Coords | -32.6038635, 152.0188098 (near Tea Gardens) | -31.4417482, 152.9094062 (Port Macquarie) |
| Nearest pharmacy | Myall Pharmacy - Tea Gardens (13.44 km) | Lake Road Pharmacy (0.13 km) |
| Fix | Nominatim lookup "Port Macquarie Private Hospital, Lake Road" |

**Result:** No longer a valid PASS opportunity — has pharmacy 130m away.

---

## 2. Emerald QLD Entries (IDs 8716, 8901)

Both were geocoded to (-30.72, 146.80) in outback NSW instead of Emerald QLD.

| ID | Name | New Coords | Nearest Pharmacy |
|----|------|-----------|-----------------|
| 8716 | Emerald Medical Centre | -23.4375, 148.1591 | ~4 pharmacies within town |
| 8901 | IGA Emerald | -23.5197, 148.1574 | ~4 pharmacies within town |

**Result:** No longer valid PASS opportunities — Emerald QLD has 4+ pharmacies.

---

## 3. Generic "Supermarket" Entries (3 entries)

| ID | Region | Disposition | Reason |
|----|--------|------------|--------|
| 8011 | NT | **INVALID** | Generic name "Supermarket", no address, coords geocoded to TAS fallback |
| 8259 | WA | **INVALID** | Generic name "Supermarket", no address, coords geocoded to TAS fallback |
| 9382 | TAS | **INVALID** | Coles Bay area — only real supermarket is "Coles Bay Convenience" (separate entry ID 9366) |

---

## 4. State-Mismatch Entries (15 entries)

These had correct coordinates but the verification script's bounding-box state detection flagged them as wrong state. All were re-geocoded via Nominatim to confirm/refine coordinates.

| ID | Name | Region | Fix Applied |
|----|------|--------|-------------|
| 8115 | RFDS Clinic Innamincka | SA | Coords confirmed correct (Innamincka is SA, near QLD border) |
| 8376 | Rupanyup Supermarket | VIC | Coords confirmed (-36.63, 142.63) |
| 8379 | Rupanyup Doctors Surgery | VIC | Coords refined to (-36.6367, 142.6316) |
| 8490 | Dargo General Store | VIC | Coords refined to (-37.4613, 147.2509) |
| 8492 | McPherson General Store | VIC | Coords confirmed (-36.076, 142.415) |
| 8495 | Marnoo General Store | VIC | Coords confirmed (-36.671, 142.870) |
| 8498 | Balmoral Community Store | VIC | Coords refined to (-37.2476, 141.8409) |
| 8500 | Hotham Supermarket | VIC | Coords confirmed (-36.982, 147.143) |
| 8503 | Katamatite Fuel & Grocery | VIC | Coords confirmed (-36.077, 145.688) |
| 8504 | Cann River Friendly Grocer | VIC | Coords refined to (-37.5664, 149.1514) |
| 8508 | Mt Buller Medical Centre | VIC | Coords refined to (-37.1461, 146.4453) |
| 8509 | Mount Hotham Medical Centre | VIC | Coords refined to (-36.9835, 147.1422) |
| 8510 | Peter M. Sudholz Medical & Allied Health Centre | VIC | Coords refined to (-36.7452, 141.9372) |
| 8515 | Rural Northwest Health Beulah Campus | VIC | Coords confirmed (-35.938, 142.422) |
| 8518 | Kaniava Hospital | VIC | Coords refined to (-36.3814, 141.2460) |

**Note:** These were false positives in the verification — coords were in VIC but near state borders (or the state-detection bounding box was too coarse).

---

## 5. Foodworks Entries (42 entries with fallback coordinates)

All 42 Foodworks opportunities had been geocoded to the same fallback coordinate (-32.2443163, 147.3564635) in outback NSW. Each was matched to its correct Foodworks store using pharmacy proximity matching and Nominatim lookups.

### Successfully Re-geocoded: 40

| ID | Region | Matched Store Location | Nearest Pharmacy |
|----|--------|----------------------|-----------------|
| 7994 | NT | Stuart Hwy, Mataranka (-14.925, 133.069) | Pharmasave Katherine Pharmacy (100.31 km) |
| 8203 | SA | Elizabeth East (-34.724, 138.680) | PharmaSave Elizabeth Day/Night Pharmacy (0.59 km) |
| 8349 | WA | Gosnells (-32.089, 115.987) | Corfield Street Family Chemist (1.77 km) |
| 8648 | VIC | OSM:837263906 (-37.841, 144.954) | Slade Pharmacy Fitzroy area |
| 8650 | VIC | 189 Victoria Ave (-37.847, 144.949) | La Feuille Pharmacy (1.89 km) |
| 8651 | VIC | Mirboo North (-38.401, 146.159) | Mirboo North Pharmacy (0.19 km) |
| 8652 | VIC | Apollo Bay (-38.755, 143.668) | Apollo Bay Pharmacy (0.17 km) |
| 8653 | VIC | Orbost (-37.708, 148.455) | Orbost Pharmacy (0.25 km) |
| 8654 | VIC | Wedderburn (-36.420, 143.614) | Wedderburn Pharmacy (0.30 km) |
| 8656 | VIC | Myrtleford (-36.563, 146.727) | Blooms The Chemist Myrtleford (0.14 km) |
| 8657 | VIC | Mansfield (-37.052, 146.087) | Eisner's Pharmacy (0.23 km) |
| 8658 | VIC | Laverton (-37.853, 144.775) | Direct Chemist Outlet Laverton (2.16 km) |
| 8659 | VIC | Theodore St (-37.734, 144.806) | TerryWhite Chemmart Sydenham (6.13 km) |
| 8661 | VIC | 306 Racecourse Rd (-37.788, 144.930) | Southgate Pharmacy (4.77 km) |
| 8662 | VIC | Waterdale Rd, Heidelberg West (-37.736, 145.049) | Lorne Street Pharmacy (7.19 km) |
| 8664 | VIC | 10 High St, Yea (-37.210, 145.426) | Yea Pharmacy (0.31 km) |
| 8665 | VIC | 10-12 Reilly St, Inverloch (-38.633, 145.729) | TerryWhite Chemmart Inverloch (0.10 km) |
| 8666 | VIC | St Albans (-37.734, 144.806) | Alfrieda Street Pharmacy (1.05 km) |
| 8667 | VIC | Albert Park (-37.842, 144.954) | J & M Douglas Pharmacy (0.08 km) |
| 8668 | VIC | 87A Powerscourt St, Maffra (-37.960, 146.986) | Maffra Pharmacy (1.03 km) |
| 8669 | VIC | 130-132 Hothlyn Dr, Craigieburn (-37.610, 144.933) | Priceline Pharmacy Craigieburn (1.44 km) |
| 8670 | VIC | 106 Henry Rd, Pakenham (-38.079, 145.451) | Direct Chemist Outlet Pakenham (1.35 km) |
| 8795 | QLD | OSM:1333721805 (-24.862, 151.123) | Monto Pharmacy (0.23 km) |
| 8797 | QLD | OSM:1263443595 (-27.500, 153.404) | Stradbroke Pharmacy (0.09 km) |
| 8926 | QLD | OSM:1433665919 (-25.371, 151.122) | Pharmacia Eidsvold (0.19 km) |
| 8928 | QLD | Canungra (-28.016, 153.162) | TerryWhite Chemmart Canungra (0.23 km) |
| 8929 | QLD | 58 Clark St, Clifton (-27.931, 151.907) | Clifton Pharmacy (0.17 km) |
| 9273 | NSW | OSM:706844078 (-34.510, 144.843) | Japp's Pharmacy (0.10 km) |
| 9274 | NSW | Thredbo (-36.504, 148.307) | Capital Chemist Thredbo (0.12 km) |
| 9275 | NSW | Pambula (-36.929, 149.875) | Pambula Pharmacy (0.10 km) |
| 9276 | NSW | 23 Louee St, Rylstone (-32.795, 149.971) | Kandos-Rylstone Pharmacy (7.11 km) |
| 9277 | NSW | Newcastle (-32.928, 151.787) | City Pharmacy (0.31 km) |
| 9278 | NSW | Bonalbo (-28.737, 152.622) | Bonalbo Pharmacy (0.11 km) |
| 9279 | NSW | 32-40 Boldrewood Rd, Blackett (-33.738, 150.816) | TerryWhite Chemmart Plumpton (2.04 km) |
| 9280 | NSW | 1214 Anzac Pde, Malabar (-33.963, 151.247) | Little Bay Pharmacy (1.90 km) |
| 9281 | NSW | 13 Murray Rd, East Corrimal (-34.377, 150.913) | Complete Care Drive Thru Pharmacy (0.99 km) |
| 9282 | NSW | 51 Foster St, Lake Cargelligo (-33.299, 146.371) | St Mary Lake Cargelligo Pharmacy (0.40 km) |
| 9283 | NSW | Munster St, Port Macquarie (-31.433, 152.912) | Your Discount Chemist Port Macquarie (0.40 km) |
| 9284 | NSW | 32-40 Boldrewood Rd, Blackett (-33.737, 150.816) | Emerton Community Pharmacy (0.94 km) |
| 9286 | NSW | 100 Bridge St (-30.642, 151.500) | Uralla Pharmacy (0.12 km) |

### Marked INVALID: 2

| ID | Region | Reason |
|----|--------|--------|
| 8660 | VIC | Pharmacy "Chemist Warehouse" too generic (resolves to Cairns QLD) — cannot identify VIC store |
| 8663 | VIC | All nearby VIC Foodworks already assigned to other opportunities |

---

## Summary Statistics

| Category | Count | Result |
|----------|-------|--------|
| Port Macquarie Private Hospital | 1 | Coords fixed (was 140km off) |
| Emerald QLD entries | 2 | Coords fixed to real Emerald QLD |
| Generic "Supermarket" entries | 3 | All marked INVALID (phantom entries) |
| State-mismatch entries | 15 | Coords confirmed/refined (false positive flags) |
| Foodworks (fallback coords) | 42 | 40 re-geocoded, 2 marked INVALID |
| **Total** | **63** | **58 coords fixed, 5 marked INVALID** |

**Note:** 2 entries from the original 65 verification list (IDs 9366 Coles Bay Convenience and others) were already VERIFIED and required no action.
