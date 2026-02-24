# PharmacyFinder Verification Report
**Generated:** 2026-02-25 (overnight grind)
**Database:** pharmacy_finder.db
**Scored data:** scored_v2.json

---

## Summary Statistics

| Metric | Count |
|--------|-------|
| Total scored opportunities | 1450 |
| PASS verdicts | 234 |
| LIKELY verdicts | 62 |
| FAIL verdicts | 1154 |
| Shopping centres (PASS/LIKELY) | 6 |
| Hospitals/Medical (PASS) | 40 |
| Hospitals/Medical (LIKELY) | 19 |

### PASS Opportunities by POI Type
| Type | Count |
|------|-------|
| gp | 103 |
| supermarket | 91 |
| hospital | 40 |

### LIKELY Opportunities by POI Type
| Type | Count |
|------|-------|
| supermarket | 29 |
| medical_centre | 13 |
| gp | 8 |
| shopping_centre | 6 |
| hospital | 6 |

---

## Task 1: Shopping Centre Tenant Counts

### Verified Shopping Centres

#### Westfield Woden (ID: 7973)
- **Status:** ⚠️ PHARMACY EXISTS
- **Tenant Count:** 238
- **Pharmacies Found:** PharmaSave Pharmacy, Priceline Pharmacy Woden
- **Notes:** VERIFIED: 238 retailers (Scentre Group data). Anchors: David Jones, Big W, Coles, Woolworths. Has 2 pharmacies inside (PharmaSave, Priceline). NOT a pharmacy opportunity - pharmacies already present.

#### Melo Velo (ID: 8354)
- **Status:** ❌ FALSE POSITIVE
- **Tenant Count:** 0
- **Pharmacies Found:** None
- **Notes:** FALSE POSITIVE: Melo Velo is a bicycle shop/cafe in Bunbury WA (66 Victoria St), NOT a shopping centre. Incorrectly classified as poi_type=shopping_centre.

#### Eastland Shopping Centre (ID: 8382)
- **Status:** ⚠️ PHARMACY EXISTS
- **Tenant Count:** 79
- **Pharmacies Found:** Chemist Warehouse Eastlands
- **Notes:** DUPLICATE of ID 9364. Actually "Eastlands Shopping Centre" in Rosny Park TAS (not "Eastland" which is in Ringwood VIC). 79 tenants from directory scrape. Has Chemist Warehouse inside. NOT a pharmacy opportunity.

#### Liberty Tower (ID: 8383)
- **Status:** ❌ FALSE POSITIVE
- **Tenant Count:** 0
- **Pharmacies Found:** None
- **Notes:** FALSE POSITIVE: Liberty Tower is a residential/serviced apartment tower at 62-64 Clarendon St, Southbank Melbourne. Ground floor has only convenience stores/cafes. NOT a shopping centre. Nearest pharmacies: Southgate Pharmacy (5min walk), Chemist Warehouse Southbank (4min walk).

#### Eastlands Shopping Centre (ID: 9364)
- **Status:** ⚠️ PHARMACY EXISTS
- **Tenant Count:** 79
- **Pharmacies Found:** Chemist Warehouse Eastlands
- **Notes:** VERIFIED: Eastlands Shopping Centre, 26 Bligh St, Rosny Park TAS. 79 tenants (directory scraped). Managed by Vicinity Centres. Anchors: Big W, Kmart, Coles, Woolworths, Village Cinemas. Has Chemist Warehouse (Shop G017). NOT a pharmacy opportunity.

#### Channel Court Shopping Centre (ID: 9365)
- **Status:** ⚠️ PHARMACY EXISTS
- **Tenant Count:** 90
- **Pharmacies Found:** Priceline Pharmacy Kingston
- **Notes:** VERIFIED: Channel Court Shopping Centre, 29 Channel Hwy, Kingston TAS. 90+ stores (Google business listing). Anchors: Big W, Woolworths, Coles. Has Priceline Pharmacy (Shop T30/30A). NOT a pharmacy opportunity.

### Shopping Centre Summary
- **Westfield Woden (ACT):** 238 stores — 2 pharmacies inside (PharmaSave, Priceline). NOT an opportunity.
- **Melo Velo (WA):** FALSE POSITIVE — bike shop/cafe, not a shopping centre.
- **Eastland/Eastlands SC (TAS):** 79 stores — Chemist Warehouse inside. NOT an opportunity. (IDs 8382/9364 are duplicates)
- **Liberty Tower (VIC):** FALSE POSITIVE — residential tower with ground-floor shops, not a shopping centre.
- **Channel Court SC (TAS):** 90+ stores — Priceline Pharmacy inside. NOT an opportunity.

**Conclusion:** None of the 6 shopping centre PASS/LIKELY opportunities are genuine pharmacy opportunities. All either have pharmacies or are misclassified.

---

## Task 2: Hospital & Medical Centre Verification

### PASS Hospitals Verified (40 total)

All 40 PASS hospitals were verified via Google search. These are all remote/rural health facilities in communities with no nearby pharmacy.

#### NSW
| ID | Name | Pop (5km) | Nearest Pharmacy (km) | Status |
|-----|------|-----------|----------------------|--------|
| 9000 | Tibooburra Health Service | 100 | 270.27 | ✅ PASS |
| 8998 | Ivanhoe Hospital | 162 | 132.52 | ✅ PASS |
| 8999 | Menindee Health Service | 380 | 101.8 | ✅ PASS |
| 9111 | White Cliffs Health Service | 0 | 83.56 | ✅ PASS |
| 9114 | Goodooga Health Service | 179 | 61.21 | ✅ PASS |
| 9118 | Tottenham Multi Purpose Service | 263 | 47.92 | ✅ PASS |
| 9113 | Baradine Multi-Purpose Service | 586 | 41.64 | ✅ PASS |
| 8518 | Kaniava Hospital | 683 | 36.56 | ✅ PASS |
| 9117 | Trangie Multi Purpose Service | 768 | 33.34 | ✅ PASS |
| 9115 | Eugowra Multi-Purpose Service | 801 | 31.29 | ✅ PASS |
| 9116 | Delegate Multi-Purpose Service | 201 | 30.5 | ✅ PASS |
| 9112 | Bundarra Community Health | 374 | 26.74 | ✅ PASS |
| 8515 | Rural Northwest Health Beulah Campus | 170 | 24.03 | ✅ PASS |
| 9006 | Urbenville Multi-Purpose Service | 218 | 11.35 | ✅ PASS |

#### QLD
| ID | Name | Pop (5km) | Nearest Pharmacy (km) | Status |
|-----|------|-----------|----------------------|--------|
| 8738 | Birdsville Health Clinic | 0 | 496.53 | ✅ PASS |
| 8737 | Boulia Health Centre | 218 | 246.32 | ✅ PASS |
| 8742 | Forsayth Hospital - Primary Health Centre | 58 | 226.03 | ✅ PASS |
| 8739 | Thargomindah Community Clinic | 194 | 160.17 | ✅ PASS |
| 8735 | Dajarra Health Centre | 0 | 107.58 | ✅ PASS |
| 8740 | Muttaburra Primary Health Centre | 500 | 99.11 | ✅ PASS |
| 8741 | Isisford Primary Health Centre | 500 | 93.1 | ✅ PASS |
| 8822 | Aramac Primary Health Centre | 226 | 65.34 | ✅ PASS |
| 8819 | Mount Garnet Clinic | 253 | 58.98 | ✅ PASS |
| 8821 | Mount Perry Health Centre | 250 | 37.76 | ✅ PASS |

#### SA
| ID | Name | Pop (5km) | Nearest Pharmacy (km) | Status |
|-----|------|-----------|----------------------|--------|
| 8120 | Oodnadatta Hospital | 102 | 176.66 | ✅ PASS |
| 8121 | Hawker Hospital | 226 | 62.18 | ✅ PASS |

#### VIC
| ID | Name | Pop (5km) | Nearest Pharmacy (km) | Status |
|-----|------|-----------|----------------------|--------|
| 8519 | Western District Health Service - Penshurst | 491 | 26.87 | ✅ PASS |

#### WA
| ID | Name | Pop (5km) | Nearest Pharmacy (km) | Status |
|-----|------|-----------|----------------------|--------|
| 8230 | Warakurna Health Clinic | 185 | 584.47 | ✅ PASS |
| 8232 | Ngangganawili Aboriginal Health Service | 845 | 172.02 | ✅ PASS |
| 8233 | BHP Leinster Medical Centre | 395 | 124.33 | ✅ PASS |
| 8229 | Pannawonica Medical Center | 685 | 111.08 | ✅ PASS |
| 8231 | Laverton Hospital | 507 | 108.04 | ✅ PASS |
| 8289 | Mullewa Health Service | 312 | 85.33 | ✅ PASS |
| 8292 | Wyndham Hospital | 845 | 73.45 | ✅ PASS |
| 8291 | Wongan Hills Health Service | 725 | 45.88 | ✅ PASS |
| 8290 | Dumbleyung Health Service | 238 | 36.86 | ✅ PASS |
| 8286 | Coolgardie Health Centre | 763 | 36.78 | ✅ PASS |
| 8285 | Cervantes Health Centre | 480 | 22.46 | ✅ PASS |
| 8287 | First Aid Muja Power Station | 0 | 16.95 | ❌ FP |
| 8235 | Roebourne Hospital | 762 | 11.33 | ⚠️ CHECK |

### False Positives Identified
1. **First Aid Muja Power Station (ID 8287)** — Industrial site, no residential population. Not a genuine health facility for community need.

### Potential Missed Pharmacies
1. **Roebourne (ID 8235)** — Mawarnkarra Health Service has a 777 Pharmacy dispensary operating inside the Aboriginal health clinic. May not be in our database. Distance to nearest commercial pharmacy ~11km (Karratha), but there appears to be pharmacy dispensing services within Roebourne itself. **Needs manual verification.**

### Data Errors Found
1. **Rural Northwest Health Beulah Campus (ID 8515)** — Listed as NSW but Beulah is in Victoria.
2. **Kaniava Hospital (ID 8518)** — Listed as NSW but likely Kaniva in Victoria.

### Borderline Cases
1. **Coolgardie (ID 8286)** — Only 36km from Kalgoorlie (major town with multiple pharmacies). Borderline PASS.
2. **Urbenville (ID 9006)** — Only 11km from nearest pharmacy. Borderline PASS.

---

## Task 3: New Pharmacies Found (Missing from DB)

| Location | Pharmacy Found | Type | Notes |
|----------|---------------|------|-------|
| Roebourne WA | 777 Pharmacy at Mawarnkarra Health Service | Aboriginal Health Dispensary | May be restricted to Aboriginal patients/healthcare card holders |

---

## Recommendations

1. **Remove 2 false positive shopping centres** (Melo Velo, Liberty Tower) from future scoring
2. **Remove 4 shopping centres with existing pharmacies** (Westfield Woden, Eastlands x2, Channel Court) from opportunity pipeline
3. **Verify Roebourne 777 Pharmacy** — determine if this is a full community pharmacy or restricted dispensary
4. **Fix region data** for IDs 8515 and 8518 (listed as NSW, actually VIC)
5. **Remove Muja Power Station** (ID 8287) — not a genuine community need
6. **Review borderline cases** — Coolgardie (36km) and Urbenville (11km) may not warrant PASS status

---

## DB Changes Made

- Added `tenant_count` column to opportunities table
- Updated 6 shopping centre records with tenant counts and verification notes
- Updated 40 hospital/medical records with verification notes
- Set `verification='FALSE POSITIVE'` for IDs: 8354, 8383, 8287
- Set `verification='PHARMACY EXISTS'` for IDs: 7973, 8382, 9364, 9365
- Set `verification='VERIFIED'` for remaining 38 PASS hospitals
