# PharmacyFinder Overnight Verification Report
**Generated:** 2026-02-25 23:04
**Database:** pharmacy_finder.db

## Summary

| Metric | Count |
|--------|-------|
| PASS opportunities verified | 120 |
| LIKELY opportunities verified | 2 |
| Clean (no issues) | 117 |
| Issues found | 0 |
| Needs review | 5 |
| Previously flagged | 0 |

## Verified PASS by State

| State | Count |
|-------|-------|
| ACT | 1 |
| NSW | 32 |
| NT | 35 |
| QLD | 18 |
| SA | 4 |
| TAS | 1 |
| VIC | 3 |
| WA | 21 |

## Issues Found

*No critical issues found.*

## Items Needing Review

- **[7994] Foodworks** (PASS, supermarket, NT)
  - GENERIC_NAME: "Foodworks" is too generic, may not be a real POI
- **[8285] Cervantes Health Centre** (PASS, hospital, WA)
  - DISTANCE_MISMATCH: Stated 62.7759222826431km vs actual 22.46km (diff=40.3km)
- **[8508] Mt Buller Medical Centre** (PASS, gp, NSW)
  - ZERO_POP: No population within 5km
- **[8819] Mount Garnet Clinic** (PASS, hospital, QLD)
  - DISTANCE_MISMATCH: Stated 58.9787954448941km vs actual 39.95km (diff=19.0km)
- **[9006] Urbenville Multi-Purpose Service** (PASS, hospital, NSW)
  - DISTANCE_MISMATCH: Stated 30.592617331342282km vs actual 11.35km (diff=19.2km)

## Top 20 Verified PASS Opportunities

| Rank | ID | Name | State | Type | Pop 5km | Nearest Pharm (km) | Score |
|------|-----|------|-------|------|---------|-------------------|-------|
| 1 | 7995 | Yulara Medical Centre | NT | gp | 853 | None | 75 |
| 2 | 7997 | Adjumarllarl Store & Takeaway | NT | supermarket | 1153 | None | 75 |
| 3 | 7999 | Ntaria Supermarket | NT | supermarket | 751 | None | 75 |
| 4 | 8000 | Yuendumu Store | NT | supermarket | 740 | None | 75 |
| 5 | 8001 | Lajamanu Store | NT | supermarket | 653 | None | 75 |
| 6 | 8006 | Fink River Mission General Store | NT | supermarket | 651 | None | 75 |
| 7 | 8007 | Beswick Store | NT | supermarket | 542 | None | 75 |
| 8 | 8014 | Angurugu Community Store | NT | supermarket | 883 | None | 75 |
| 9 | 8016 | Xpress Mart | NT | supermarket | 751 | None | 75 |
| 10 | 8018 | Aurora Kakadu Lodge | NT | supermarket | 755 | None | 75 |
| 11 | 8020 | Yuendumu Health Centre | NT | gp | 740 | None | 75 |
| 12 | 8021 | Lajamanu Health Centre | NT | gp | 653 | None | 75 |
| 13 | 8022 | Kalkaringi Health Centre | NT | gp | 544 | None | 75 |
| 14 | 8029 | Wilora Health Centre | NT | gp | 500 | None | 75 |
| 15 | 8031 | Amanbidji Health Centre | NT | gp | 500 | None | 75 |
| 16 | 8037 | Borroloola Health Centre | NT | gp | 955 | None | 75 |
| 17 | 8044 | Ngukurr Health Centre | NT | gp | 1088 | None | 75 |
| 18 | 8045 | Baniyala Health Centre | NT | gp | 500 | None | 75 |
| 19 | 8048 | Alyangula Health Centre | NT | gp | 751 | None | 75 |
| 20 | 8052 | Pmara Jutunta Health Centre | NT | gp | 500 | None | 75 |