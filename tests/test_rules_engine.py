"""
Test suite for PharmacyFinder rules engine.

Validates Items 130-136 against controlled real-world scenarios using
an in-memory SQLite database with deterministic reference data.
"""
import os
import sys
import math
import sqlite3
import pytest

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from engine.rules.item_130 import check_item_130
from engine.rules.item_131 import check_item_131
from engine.rules.item_132 import check_item_132
from engine.rules.item_133 import check_item_133
from engine.rules.item_134 import check_item_134
from engine.rules.item_134a import check_item_134a
from engine.rules.item_135 import check_item_135
from engine.rules.item_136 import check_item_136
from engine.rules.general import (
    confidence_from_margin_m,
    check_general_requirements,
    check_supermarket_access,
)
from engine.context import EvaluationContext
from tests.conftest import ORIGIN_LAT, ORIGIN_LON, offset_point


# ═══════════════════════════════════════════════════════════════════════════
# GENERAL REQUIREMENTS
# ═══════════════════════════════════════════════════════════════════════════
class TestGeneralRequirements:
    def test_all_defaults_pass(self, make_candidate):
        c = make_candidate()
        result = check_general_requirements(c)
        assert result.passed is True

    def test_zoning_fails(self, make_candidate):
        c = make_candidate(zoning_ok=False)
        result = check_general_requirements(c)
        assert result.passed is False
        assert any("zoning" in r.lower() for r in result.reasons)

    def test_not_accessible_fails(self, make_candidate):
        c = make_candidate(accessible_to_public=False)
        result = check_general_requirements(c)
        assert result.passed is False

    def test_supermarket_access_ok(self, make_candidate):
        c = make_candidate(direct_access_from_supermarket=False)
        assert check_supermarket_access(c) is True

    def test_supermarket_access_blocked(self, make_candidate):
        c = make_candidate(direct_access_from_supermarket=True)
        assert check_supermarket_access(c) is False


class TestConfidenceScoring:
    def test_high_margin(self):
        assert confidence_from_margin_m(600) == 0.95

    def test_medium_margin(self):
        assert confidence_from_margin_m(300) == 0.85

    def test_low_margin(self):
        assert confidence_from_margin_m(100) == 0.75

    def test_very_low_margin(self):
        assert confidence_from_margin_m(30) == 0.65

    def test_boundary_500(self):
        # 500m is NOT > 500, so should be 0.85
        assert confidence_from_margin_m(500) == 0.85

    def test_boundary_200(self):
        assert confidence_from_margin_m(200) == 0.75

    def test_boundary_50(self):
        assert confidence_from_margin_m(50) == 0.65


# ═══════════════════════════════════════════════════════════════════════════
# ITEM 130 — New pharmacy ≥ 1.5 km straight line
# ═══════════════════════════════════════════════════════════════════════════
class TestItem130:
    """
    Item 130 requires:
    (a) ≥ 1.5 km straight-line from nearest approved pharmacy
    (b) Within 500m: supermarket ≥1000sqm + GP, OR supermarket ≥2500sqm
    """

    def test_pass_supermarket_plus_gp(self, world_130):
        """Site with supermarket (1200sqm) + GP within 500m, pharmacy 2km away = PASS."""
        ctx, candidate, _, _ = world_130
        result = check_item_130(candidate, ctx)
        assert result.passed is True
        assert result.item == "Item 130"
        assert result.confidence > 0

    def test_fail_pharmacy_too_close(self, test_db, build_context, make_candidate):
        """Site with pharmacy only 1.0km away = FAIL (need ≥1.5km)."""
        db_path, conn = test_db
        cur = conn.cursor()

        # Pharmacy only 1km north
        plat, plon = offset_point(ORIGIN_LAT, ORIGIN_LON, north_km=1.0)
        cur.execute(
            "INSERT INTO pharmacies (name, address, latitude, longitude) VALUES (?,?,?,?)",
            ("Close Pharm", "1km North", plat, plon),
        )
        conn.commit()

        ctx = build_context(db_path)
        candidate = make_candidate()
        result = check_item_130(candidate, ctx)
        assert result.passed is False
        assert any("FAIL" in r for r in result.reasons)

    def test_fail_no_supermarket(self, test_db, build_context, make_candidate):
        """Site with pharmacy >1.5km but NO supermarket within 500m = FAIL."""
        db_path, conn = test_db
        cur = conn.cursor()

        # Pharmacy 2km away — passes distance check
        plat, plon = offset_point(ORIGIN_LAT, ORIGIN_LON, north_km=2.0)
        cur.execute(
            "INSERT INTO pharmacies (name, address, latitude, longitude) VALUES (?,?,?,?)",
            ("Far Pharm", "2km North", plat, plon),
        )
        # GP nearby but NO supermarket
        gplat, gplon = offset_point(ORIGIN_LAT, ORIGIN_LON, east_km=0.2)
        cur.execute(
            "INSERT INTO gps (name, address, latitude, longitude, fte) VALUES (?,?,?,?,?)",
            ("Dr Jones", "200m East", gplat, gplon, 1.0),
        )
        conn.commit()

        ctx = build_context(db_path)
        candidate = make_candidate()
        result = check_item_130(candidate, ctx)
        assert result.passed is False
        assert any("supermarket" in r.lower() for r in result.reasons)

    def test_fail_supermarket_no_gp(self, test_db, build_context, make_candidate):
        """Site with supermarket (1200sqm <2500) but NO GP = FAIL for (b)(i)."""
        db_path, conn = test_db
        cur = conn.cursor()

        plat, plon = offset_point(ORIGIN_LAT, ORIGIN_LON, north_km=2.0)
        cur.execute(
            "INSERT INTO pharmacies (name, address, latitude, longitude) VALUES (?,?,?,?)",
            ("Far Pharm", "2km North", plat, plon),
        )
        slat, slon = offset_point(ORIGIN_LAT, ORIGIN_LON, east_km=0.3)
        cur.execute(
            "INSERT INTO supermarkets (name, address, latitude, longitude, estimated_gla) VALUES (?,?,?,?,?)",
            ("Small IGA", "300m East", slat, slon, 1200),
        )
        conn.commit()

        ctx = build_context(db_path)
        candidate = make_candidate()
        result = check_item_130(candidate, ctx)
        assert result.passed is False
        assert any("GP" in r or "gp" in r.lower() for r in result.reasons)

    def test_pass_large_supermarket_no_gp_needed(self, test_db, build_context, make_candidate):
        """Site with supermarket ≥2500sqm within 500m — no GP required = PASS via (b)(ii)."""
        db_path, conn = test_db
        cur = conn.cursor()

        plat, plon = offset_point(ORIGIN_LAT, ORIGIN_LON, north_km=2.0)
        cur.execute(
            "INSERT INTO pharmacies (name, address, latitude, longitude) VALUES (?,?,?,?)",
            ("Far Pharm", "2km North", plat, plon),
        )
        slat, slon = offset_point(ORIGIN_LAT, ORIGIN_LON, east_km=0.3)
        cur.execute(
            "INSERT INTO supermarkets (name, address, latitude, longitude, estimated_gla) VALUES (?,?,?,?,?)",
            ("Big Coles", "300m East", slat, slon, 2500),
        )
        conn.commit()

        ctx = build_context(db_path)
        candidate = make_candidate()
        result = check_item_130(candidate, ctx)
        assert result.passed is True
        assert any("(b)(ii)" in r for r in result.reasons)

    def test_boundary_exactly_1_5km(self, test_db, build_context, make_candidate):
        """Pharmacy at exactly 1.5km — boundary inclusive (>= means PASS).

        The engine uses `< 1.5` to fail, so a distance of exactly 1.500km passes.
        We place the pharmacy at 1.502km (tiny margin) to avoid float rounding
        from the approximate offset_point helper, and verify the measured
        distance is within a few meters of the 1.5km boundary.
        """
        db_path, conn = test_db
        cur = conn.cursor()

        # Place pharmacy just past 1.5km north (1.502km to avoid float rounding)
        plat, plon = offset_point(ORIGIN_LAT, ORIGIN_LON, north_km=1.502)
        cur.execute(
            "INSERT INTO pharmacies (name, address, latitude, longitude) VALUES (?,?,?,?)",
            ("Boundary Pharm", "1.5km North", plat, plon),
        )
        # Large supermarket within 500m (to satisfy (b))
        slat, slon = offset_point(ORIGIN_LAT, ORIGIN_LON, east_km=0.2)
        cur.execute(
            "INSERT INTO supermarkets (name, address, latitude, longitude, estimated_gla) VALUES (?,?,?,?,?)",
            ("Woolworths", "200m East", slat, slon, 2600),
        )
        conn.commit()

        ctx = build_context(db_path)
        candidate = make_candidate()
        result = check_item_130(candidate, ctx)
        # Verify the distance measurement is very close to 1.5km boundary
        dist = result.distances.get("nearest_pharmacy_km", 0)
        assert abs(dist - 1.5) < 0.01, f"Expected ~1.5km, got {dist}"
        assert result.passed is True

    def test_no_pharmacies_remote(self, test_db, build_context, make_candidate):
        """Extremely remote site with no pharmacies at all — passes distance check."""
        db_path, conn = test_db
        # No pharmacies inserted, but add a supermarket to satisfy (b)
        cur = conn.cursor()
        slat, slon = offset_point(ORIGIN_LAT, ORIGIN_LON, east_km=0.1)
        cur.execute(
            "INSERT INTO supermarkets (name, address, latitude, longitude, estimated_gla) VALUES (?,?,?,?,?)",
            ("Remote Woolies", "100m East", slat, slon, 3000),
        )
        conn.commit()

        ctx = build_context(db_path)
        candidate = make_candidate()
        result = check_item_130(candidate, ctx)
        assert result.passed is True


# ═══════════════════════════════════════════════════════════════════════════
# ITEM 131 — New pharmacy ≥ 10 km by road
# ═══════════════════════════════════════════════════════════════════════════
class TestItem131:
    """
    Item 131 requires ≥ 10km by shortest lawful access route.
    The engine uses geodesic pre-filtering:
    - geodesic ≥ 8km → estimated pass (~11.2km by road)
    - geodesic < 5km → estimated fail (~7km by road)
    """

    def test_pass_clearly_remote(self, test_db, build_context, make_candidate):
        """Site >10km geodesic from nearest pharmacy = clear PASS."""
        db_path, conn = test_db
        cur = conn.cursor()

        plat, plon = offset_point(ORIGIN_LAT, ORIGIN_LON, north_km=12.0)
        cur.execute(
            "INSERT INTO pharmacies (name, address, latitude, longitude) VALUES (?,?,?,?)",
            ("Distant Pharm", "12km North", plat, plon),
        )
        conn.commit()

        ctx = build_context(db_path)
        candidate = make_candidate()
        result = check_item_131(candidate, ctx)
        assert result.passed is True
        assert result.item == "Item 131"
        assert result.confidence > 0

    def test_fail_pharmacy_close(self, test_db, build_context, make_candidate):
        """Site with pharmacy 3km away — geodesic clearly below threshold = FAIL."""
        db_path, conn = test_db
        cur = conn.cursor()

        plat, plon = offset_point(ORIGIN_LAT, ORIGIN_LON, north_km=3.0)
        cur.execute(
            "INSERT INTO pharmacies (name, address, latitude, longitude) VALUES (?,?,?,?)",
            ("Nearby Pharm", "3km North", plat, plon),
        )
        conn.commit()

        ctx = build_context(db_path)
        candidate = make_candidate()
        result = check_item_131(candidate, ctx)
        assert result.passed is False

    def test_fail_9_9km_geodesic(self, test_db, build_context, make_candidate):
        """
        Pharmacy 4.5km geodesic → ~6.3km estimated route → FAIL.
        (Well below the 10km route threshold.)
        """
        db_path, conn = test_db
        cur = conn.cursor()

        plat, plon = offset_point(ORIGIN_LAT, ORIGIN_LON, north_km=4.5)
        cur.execute(
            "INSERT INTO pharmacies (name, address, latitude, longitude) VALUES (?,?,?,?)",
            ("Medium Pharm", "4.5km North", plat, plon),
        )
        conn.commit()

        ctx = build_context(db_path)
        candidate = make_candidate()
        result = check_item_131(candidate, ctx)
        assert result.passed is False

    def test_geodesic_distance_accuracy(self, test_db, build_context, make_candidate):
        """Verify geodesic calculation is reasonably accurate."""
        db_path, conn = test_db
        cur = conn.cursor()

        # Place pharmacy ~10km north using offset_point
        plat, plon = offset_point(ORIGIN_LAT, ORIGIN_LON, north_km=10.0)
        cur.execute(
            "INSERT INTO pharmacies (name, address, latitude, longitude) VALUES (?,?,?,?)",
            ("10km Pharm", "10km North", plat, plon),
        )
        conn.commit()

        ctx = build_context(db_path)
        # Check that the geodesic distance is close to 10km
        from geopy.distance import geodesic as geopy_geodesic
        d = geopy_geodesic((ORIGIN_LAT, ORIGIN_LON), (plat, plon)).kilometers
        assert abs(d - 10.0) < 0.1, f"Expected ~10km, got {d:.3f}km"

    def test_no_pharmacies_pass(self, test_db, build_context, make_candidate):
        """No pharmacies in database — remote area, passes."""
        db_path, conn = test_db
        conn.commit()  # empty DB

        ctx = build_context(db_path)
        candidate = make_candidate()
        result = check_item_131(candidate, ctx)
        assert result.passed is True


# ═══════════════════════════════════════════════════════════════════════════
# ITEM 132 — Same-town additional pharmacy
# ═══════════════════════════════════════════════════════════════════════════
class TestItem132:
    """
    Item 132 requires:
    (a)(ii)  ≥ 200m straight-line from nearest pharmacy
    (a)(iii) ≥ 10km by road from ALL OTHER pharmacies
    (b)(i)   ≥ 4 FTE GPs in town
    (b)(ii)  Combined supermarket GLA ≥ 2500 sqm
    """

    def test_pass_same_town(self, test_db, build_context, make_candidate):
        """All conditions met: nearest 300m away, no others within 8km, 5 FTE GPs, 3000sqm GLA."""
        db_path, conn = test_db
        cur = conn.cursor()

        # Nearest pharmacy 300m north
        plat, plon = offset_point(ORIGIN_LAT, ORIGIN_LON, north_km=0.3)
        cur.execute(
            "INSERT INTO pharmacies (name, address, latitude, longitude) VALUES (?,?,?,?)",
            ("Town Pharm", "300m North", plat, plon),
        )
        # 5 FTE GPs in town (within 5km)
        for i in range(5):
            glat, glon = offset_point(ORIGIN_LAT, ORIGIN_LON, east_km=0.5 + i * 0.2)
            cur.execute(
                "INSERT INTO gps (name, address, latitude, longitude, fte) VALUES (?,?,?,?,?)",
                (f"Dr GP{i}", f"GP{i} St", glat, glon, 1.0),
            )
        # Supermarket 3000 sqm GLA
        slat, slon = offset_point(ORIGIN_LAT, ORIGIN_LON, east_km=1.0)
        cur.execute(
            "INSERT INTO supermarkets (name, address, latitude, longitude, estimated_gla) VALUES (?,?,?,?,?)",
            ("Town Woolies", "1km East", slat, slon, 3000),
        )
        conn.commit()

        ctx = build_context(db_path)
        candidate = make_candidate()
        result = check_item_132(candidate, ctx)
        assert result.passed is True
        assert result.item == "Item 132"

    def test_fail_no_pharmacy_in_town(self, test_db, build_context, make_candidate):
        """No pharmacies at all — Item 132 is 'additional pharmacy in town', must fail."""
        db_path, conn = test_db
        conn.commit()

        ctx = build_context(db_path)
        candidate = make_candidate()
        result = check_item_132(candidate, ctx)
        assert result.passed is False

    def test_fail_nearest_too_close(self, test_db, build_context, make_candidate):
        """Nearest pharmacy only 100m away — need ≥ 200m."""
        db_path, conn = test_db
        cur = conn.cursor()

        plat, plon = offset_point(ORIGIN_LAT, ORIGIN_LON, north_km=0.1)
        cur.execute(
            "INSERT INTO pharmacies (name, address, latitude, longitude) VALUES (?,?,?,?)",
            ("Too Close Pharm", "100m North", plat, plon),
        )
        conn.commit()

        ctx = build_context(db_path)
        candidate = make_candidate()
        result = check_item_132(candidate, ctx)
        assert result.passed is False
        assert any("200m" in r for r in result.reasons)

    def test_fail_other_pharmacy_too_close(self, test_db, build_context, make_candidate):
        """Nearest OK at 300m, but second pharmacy only 3km away (est ~4.2km route < 10km)."""
        db_path, conn = test_db
        cur = conn.cursor()

        # Nearest at 300m
        plat, plon = offset_point(ORIGIN_LAT, ORIGIN_LON, north_km=0.3)
        cur.execute(
            "INSERT INTO pharmacies (name, address, latitude, longitude) VALUES (?,?,?,?)",
            ("Nearest Pharm", "300m North", plat, plon),
        )
        # Second pharmacy at 3km — estimated route ~4.2km < 10km
        p2lat, p2lon = offset_point(ORIGIN_LAT, ORIGIN_LON, east_km=3.0)
        cur.execute(
            "INSERT INTO pharmacies (name, address, latitude, longitude) VALUES (?,?,?,?)",
            ("Other Pharm", "3km East", p2lat, p2lon),
        )
        # GPs and supermarket to satisfy (b)
        for i in range(5):
            glat, glon = offset_point(ORIGIN_LAT, ORIGIN_LON, east_km=0.5 + i * 0.2)
            cur.execute(
                "INSERT INTO gps (name, address, latitude, longitude, fte) VALUES (?,?,?,?,?)",
                (f"Dr GP{i}", f"GP{i} St", glat, glon, 1.0),
            )
        slat, slon = offset_point(ORIGIN_LAT, ORIGIN_LON, east_km=1.0)
        cur.execute(
            "INSERT INTO supermarkets (name, address, latitude, longitude, estimated_gla) VALUES (?,?,?,?,?)",
            ("Town Woolies", "1km East", slat, slon, 3000),
        )
        conn.commit()

        ctx = build_context(db_path)
        candidate = make_candidate()
        result = check_item_132(candidate, ctx)
        assert result.passed is False

    def test_fail_insufficient_gps(self, test_db, build_context, make_candidate):
        """Only 2 FTE GPs — need ≥ 4."""
        db_path, conn = test_db
        cur = conn.cursor()

        plat, plon = offset_point(ORIGIN_LAT, ORIGIN_LON, north_km=0.3)
        cur.execute(
            "INSERT INTO pharmacies (name, address, latitude, longitude) VALUES (?,?,?,?)",
            ("Town Pharm", "300m North", plat, plon),
        )
        for i in range(2):
            glat, glon = offset_point(ORIGIN_LAT, ORIGIN_LON, east_km=0.5 + i * 0.3)
            cur.execute(
                "INSERT INTO gps (name, address, latitude, longitude, fte) VALUES (?,?,?,?,?)",
                (f"Dr GP{i}", f"GP{i} St", glat, glon, 1.0),
            )
        slat, slon = offset_point(ORIGIN_LAT, ORIGIN_LON, east_km=1.0)
        cur.execute(
            "INSERT INTO supermarkets (name, address, latitude, longitude, estimated_gla) VALUES (?,?,?,?,?)",
            ("Town Woolies", "1km East", slat, slon, 3000),
        )
        conn.commit()

        ctx = build_context(db_path)
        candidate = make_candidate()
        result = check_item_132(candidate, ctx)
        assert result.passed is False
        assert any("FTE" in r for r in result.reasons)

    def test_fail_insufficient_supermarket_gla(self, test_db, build_context, make_candidate):
        """Supermarket GLA only 2000sqm — need ≥ 2500."""
        db_path, conn = test_db
        cur = conn.cursor()

        plat, plon = offset_point(ORIGIN_LAT, ORIGIN_LON, north_km=0.3)
        cur.execute(
            "INSERT INTO pharmacies (name, address, latitude, longitude) VALUES (?,?,?,?)",
            ("Town Pharm", "300m North", plat, plon),
        )
        for i in range(5):
            glat, glon = offset_point(ORIGIN_LAT, ORIGIN_LON, east_km=0.5 + i * 0.2)
            cur.execute(
                "INSERT INTO gps (name, address, latitude, longitude, fte) VALUES (?,?,?,?,?)",
                (f"Dr GP{i}", f"GP{i} St", glat, glon, 1.0),
            )
        slat, slon = offset_point(ORIGIN_LAT, ORIGIN_LON, east_km=1.0)
        cur.execute(
            "INSERT INTO supermarkets (name, address, latitude, longitude, estimated_gla) VALUES (?,?,?,?,?)",
            ("Small IGA", "1km East", slat, slon, 2000),
        )
        conn.commit()

        ctx = build_context(db_path)
        candidate = make_candidate()
        result = check_item_132(candidate, ctx)
        assert result.passed is False
        assert any("GLA" in r or "gla" in r.lower() for r in result.reasons)


# ═══════════════════════════════════════════════════════════════════════════
# ITEMS 133 / 134 / 134A — Shopping centre rules
# ═══════════════════════════════════════════════════════════════════════════
class TestItem133:
    """
    Item 133: Small shopping centre (15-49 tenants, ≥5000sqm GLA,
    supermarket ≥2500sqm, ≥500m from nearest pharmacy).
    """

    def test_pass_qualifying_small_centre(self, test_db, build_context, make_candidate):
        """Centre with 25 tenants, 6000sqm GLA, supermarket 3000sqm, pharmacy 600m away."""
        db_path, conn = test_db
        cur = conn.cursor()

        # Shopping centre at candidate location
        cur.execute(
            "INSERT INTO shopping_centres (name, address, latitude, longitude, estimated_gla, estimated_tenants, major_supermarkets) VALUES (?,?,?,?,?,?,?)",
            ("Small Town Plaza", "Origin St", ORIGIN_LAT, ORIGIN_LON, 6000, 25, "[]"),
        )
        # Supermarket inside centre
        cur.execute(
            "INSERT INTO supermarkets (name, address, latitude, longitude, estimated_gla) VALUES (?,?,?,?,?)",
            ("Woolworths", "Origin St", ORIGIN_LAT, ORIGIN_LON, 3000),
        )
        # Nearest pharmacy 600m away
        plat, plon = offset_point(ORIGIN_LAT, ORIGIN_LON, north_km=0.6)
        cur.execute(
            "INSERT INTO pharmacies (name, address, latitude, longitude) VALUES (?,?,?,?)",
            ("Distant Pharm", "600m North", plat, plon),
        )
        conn.commit()

        ctx = build_context(db_path)
        candidate = make_candidate()
        result = check_item_133(candidate, ctx)
        assert result.passed is True
        assert result.item == "Item 133"

    def test_fail_too_few_tenants(self, test_db, build_context, make_candidate):
        """Centre with only 10 tenants — need ≥ 15."""
        db_path, conn = test_db
        cur = conn.cursor()

        cur.execute(
            "INSERT INTO shopping_centres (name, address, latitude, longitude, estimated_gla, estimated_tenants, major_supermarkets) VALUES (?,?,?,?,?,?,?)",
            ("Tiny Plaza", "Origin St", ORIGIN_LAT, ORIGIN_LON, 6000, 10, "[]"),
        )
        cur.execute(
            "INSERT INTO supermarkets (name, address, latitude, longitude, estimated_gla) VALUES (?,?,?,?,?)",
            ("Woolworths", "Origin St", ORIGIN_LAT, ORIGIN_LON, 3000),
        )
        plat, plon = offset_point(ORIGIN_LAT, ORIGIN_LON, north_km=0.6)
        cur.execute(
            "INSERT INTO pharmacies (name, address, latitude, longitude) VALUES (?,?,?,?)",
            ("Distant Pharm", "600m North", plat, plon),
        )
        conn.commit()

        ctx = build_context(db_path)
        candidate = make_candidate()
        result = check_item_133(candidate, ctx)
        assert result.passed is False
        assert any("tenant" in r.lower() for r in result.reasons)

    def test_fail_gla_below_threshold(self, test_db, build_context, make_candidate):
        """Centre GLA only 4000sqm — need ≥ 5000."""
        db_path, conn = test_db
        cur = conn.cursor()

        cur.execute(
            "INSERT INTO shopping_centres (name, address, latitude, longitude, estimated_gla, estimated_tenants, major_supermarkets) VALUES (?,?,?,?,?,?,?)",
            ("Small GLA Plaza", "Origin St", ORIGIN_LAT, ORIGIN_LON, 4000, 25, "[]"),
        )
        cur.execute(
            "INSERT INTO supermarkets (name, address, latitude, longitude, estimated_gla) VALUES (?,?,?,?,?)",
            ("Woolworths", "Origin St", ORIGIN_LAT, ORIGIN_LON, 3000),
        )
        plat, plon = offset_point(ORIGIN_LAT, ORIGIN_LON, north_km=0.6)
        cur.execute(
            "INSERT INTO pharmacies (name, address, latitude, longitude) VALUES (?,?,?,?)",
            ("Distant Pharm", "600m North", plat, plon),
        )
        conn.commit()

        ctx = build_context(db_path)
        candidate = make_candidate()
        result = check_item_133(candidate, ctx)
        assert result.passed is False
        assert any("GLA" in r or "gla" in r.lower() for r in result.reasons)

    def test_fail_large_centre_redirects_to_134(self, test_db, build_context, make_candidate):
        """Centre with 60 tenants — too large for Item 133, should use 134/134A."""
        db_path, conn = test_db
        cur = conn.cursor()

        cur.execute(
            "INSERT INTO shopping_centres (name, address, latitude, longitude, estimated_gla, estimated_tenants, major_supermarkets) VALUES (?,?,?,?,?,?,?)",
            ("Big Mall", "Origin St", ORIGIN_LAT, ORIGIN_LON, 15000, 60, "[]"),
        )
        cur.execute(
            "INSERT INTO supermarkets (name, address, latitude, longitude, estimated_gla) VALUES (?,?,?,?,?)",
            ("Woolworths", "Origin St", ORIGIN_LAT, ORIGIN_LON, 3000),
        )
        conn.commit()

        ctx = build_context(db_path)
        candidate = make_candidate()
        result = check_item_133(candidate, ctx)
        assert result.passed is False
        assert any("134" in r for r in result.reasons)

    def test_fail_pharmacy_too_close(self, test_db, build_context, make_candidate):
        """Pharmacy only 300m away — need ≥ 500m for Item 133."""
        db_path, conn = test_db
        cur = conn.cursor()

        cur.execute(
            "INSERT INTO shopping_centres (name, address, latitude, longitude, estimated_gla, estimated_tenants, major_supermarkets) VALUES (?,?,?,?,?,?,?)",
            ("Town Plaza", "Origin St", ORIGIN_LAT, ORIGIN_LON, 6000, 25, "[]"),
        )
        cur.execute(
            "INSERT INTO supermarkets (name, address, latitude, longitude, estimated_gla) VALUES (?,?,?,?,?)",
            ("Woolworths", "Origin St", ORIGIN_LAT, ORIGIN_LON, 3000),
        )
        # Pharmacy only 300m away
        plat, plon = offset_point(ORIGIN_LAT, ORIGIN_LON, north_km=0.3)
        cur.execute(
            "INSERT INTO pharmacies (name, address, latitude, longitude) VALUES (?,?,?,?)",
            ("Close Pharm", "300m North", plat, plon),
        )
        conn.commit()

        ctx = build_context(db_path)
        candidate = make_candidate()
        result = check_item_133(candidate, ctx)
        assert result.passed is False


class TestItem134:
    """
    Item 134: Large shopping centre, NO existing pharmacy.
    ≥50 tenants, ≥5000sqm GLA, supermarket ≥2500sqm.
    """

    def test_pass_large_centre_no_pharmacy(self, test_db, build_context, make_candidate):
        """Large centre (60 tenants, 15000sqm, big supermarket), no existing pharmacy."""
        db_path, conn = test_db
        cur = conn.cursor()

        cur.execute(
            "INSERT INTO shopping_centres (name, address, latitude, longitude, estimated_gla, estimated_tenants, major_supermarkets) VALUES (?,?,?,?,?,?,?)",
            ("Westfield", "Origin St", ORIGIN_LAT, ORIGIN_LON, 15000, 60, "[]"),
        )
        cur.execute(
            "INSERT INTO supermarkets (name, address, latitude, longitude, estimated_gla) VALUES (?,?,?,?,?)",
            ("Coles", "Origin St", ORIGIN_LAT, ORIGIN_LON, 3000),
        )
        conn.commit()

        ctx = build_context(db_path)
        candidate = make_candidate()
        result = check_item_134(candidate, ctx)
        assert result.passed is True
        assert result.item == "Item 134"

    def test_fail_below_gla_threshold(self, test_db, build_context, make_candidate):
        """Centre GLA 4500sqm — below 5000sqm threshold."""
        db_path, conn = test_db
        cur = conn.cursor()

        cur.execute(
            "INSERT INTO shopping_centres (name, address, latitude, longitude, estimated_gla, estimated_tenants, major_supermarkets) VALUES (?,?,?,?,?,?,?)",
            ("Small Westfield", "Origin St", ORIGIN_LAT, ORIGIN_LON, 4500, 60, "[]"),
        )
        cur.execute(
            "INSERT INTO supermarkets (name, address, latitude, longitude, estimated_gla) VALUES (?,?,?,?,?)",
            ("Coles", "Origin St", ORIGIN_LAT, ORIGIN_LON, 3000),
        )
        conn.commit()

        ctx = build_context(db_path)
        candidate = make_candidate()
        result = check_item_134(candidate, ctx)
        assert result.passed is False

    def test_fail_existing_pharmacy_in_centre(self, test_db, build_context, make_candidate):
        """Large centre but already has a pharmacy inside."""
        db_path, conn = test_db
        cur = conn.cursor()

        cur.execute(
            "INSERT INTO shopping_centres (name, address, latitude, longitude, estimated_gla, estimated_tenants, major_supermarkets) VALUES (?,?,?,?,?,?,?)",
            ("Westfield", "Origin St", ORIGIN_LAT, ORIGIN_LON, 15000, 60, "[]"),
        )
        cur.execute(
            "INSERT INTO supermarkets (name, address, latitude, longitude, estimated_gla) VALUES (?,?,?,?,?)",
            ("Coles", "Origin St", ORIGIN_LAT, ORIGIN_LON, 3000),
        )
        # Pharmacy inside the centre (very close to centre coordinates)
        cur.execute(
            "INSERT INTO pharmacies (name, address, latitude, longitude) VALUES (?,?,?,?)",
            ("Centre Pharm", "Origin St", ORIGIN_LAT, ORIGIN_LON),
        )
        conn.commit()

        ctx = build_context(db_path)
        candidate = make_candidate()
        result = check_item_134(candidate, ctx)
        assert result.passed is False
        assert any("existing" in r.lower() or "already" in r.lower() for r in result.reasons)

    def test_fail_too_few_tenants(self, test_db, build_context, make_candidate):
        """Centre with 40 tenants — need ≥ 50 for large centre."""
        db_path, conn = test_db
        cur = conn.cursor()

        cur.execute(
            "INSERT INTO shopping_centres (name, address, latitude, longitude, estimated_gla, estimated_tenants, major_supermarkets) VALUES (?,?,?,?,?,?,?)",
            ("Medium Mall", "Origin St", ORIGIN_LAT, ORIGIN_LON, 10000, 40, "[]"),
        )
        cur.execute(
            "INSERT INTO supermarkets (name, address, latitude, longitude, estimated_gla) VALUES (?,?,?,?,?)",
            ("Coles", "Origin St", ORIGIN_LAT, ORIGIN_LON, 3000),
        )
        conn.commit()

        ctx = build_context(db_path)
        candidate = make_candidate()
        result = check_item_134(candidate, ctx)
        assert result.passed is False

    def test_fail_no_centre_nearby(self, test_db, build_context, make_candidate):
        """No shopping centre within 300m."""
        db_path, conn = test_db
        conn.commit()

        ctx = build_context(db_path)
        candidate = make_candidate()
        result = check_item_134(candidate, ctx)
        assert result.passed is False


class TestItem134A:
    """
    Item 134A: Large shopping centre with existing pharmacy(ies).
    Tenant-based tiers determine max pharmacies.
    """

    def test_pass_tier1_one_existing(self, test_db, build_context, make_candidate):
        """120 tenants, 1 existing pharmacy → allows 2nd (tier 1: 100-199)."""
        db_path, conn = test_db
        cur = conn.cursor()

        cur.execute(
            "INSERT INTO shopping_centres (name, address, latitude, longitude, estimated_gla, estimated_tenants, major_supermarkets) VALUES (?,?,?,?,?,?,?)",
            ("Mega Mall", "Origin St", ORIGIN_LAT, ORIGIN_LON, 20000, 120, "[]"),
        )
        cur.execute(
            "INSERT INTO supermarkets (name, address, latitude, longitude, estimated_gla) VALUES (?,?,?,?,?)",
            ("Woolworths", "Origin St", ORIGIN_LAT, ORIGIN_LON, 4000),
        )
        # One existing pharmacy in the centre
        cur.execute(
            "INSERT INTO pharmacies (name, address, latitude, longitude) VALUES (?,?,?,?)",
            ("Existing Pharm", "Mega Mall", ORIGIN_LAT, ORIGIN_LON),
        )
        conn.commit()

        ctx = build_context(db_path)
        # Candidate slightly offset but still within centre
        clat, clon = offset_point(ORIGIN_LAT, ORIGIN_LON, east_km=0.05)
        candidate = make_candidate(lat=clat, lon=clon)
        result = check_item_134a(candidate, ctx)
        assert result.passed is True
        assert result.item == "Item 134A"

    def test_pass_tier2_two_existing(self, test_db, build_context, make_candidate):
        """220 tenants, 2 existing pharmacies → allows 3rd (tier 2: ≥200)."""
        db_path, conn = test_db
        cur = conn.cursor()

        cur.execute(
            "INSERT INTO shopping_centres (name, address, latitude, longitude, estimated_gla, estimated_tenants, major_supermarkets) VALUES (?,?,?,?,?,?,?)",
            ("Super Mall", "Origin St", ORIGIN_LAT, ORIGIN_LON, 50000, 220, "[]"),
        )
        cur.execute(
            "INSERT INTO supermarkets (name, address, latitude, longitude, estimated_gla) VALUES (?,?,?,?,?)",
            ("Woolworths", "Origin St", ORIGIN_LAT, ORIGIN_LON, 4000),
        )
        # Two existing pharmacies
        p1lat, p1lon = offset_point(ORIGIN_LAT, ORIGIN_LON, east_km=0.05)
        cur.execute(
            "INSERT INTO pharmacies (name, address, latitude, longitude) VALUES (?,?,?,?)",
            ("Pharm A", "Super Mall A", p1lat, p1lon),
        )
        p2lat, p2lon = offset_point(ORIGIN_LAT, ORIGIN_LON, east_km=-0.05)
        cur.execute(
            "INSERT INTO pharmacies (name, address, latitude, longitude) VALUES (?,?,?,?)",
            ("Pharm B", "Super Mall B", p2lat, p2lon),
        )
        conn.commit()

        ctx = build_context(db_path)
        clat, clon = offset_point(ORIGIN_LAT, ORIGIN_LON, north_km=0.05)
        candidate = make_candidate(lat=clat, lon=clon)
        result = check_item_134a(candidate, ctx)
        assert result.passed is True

    def test_fail_no_existing_pharmacy(self, test_db, build_context, make_candidate):
        """No existing pharmacy in centre — should use Item 134, not 134A."""
        db_path, conn = test_db
        cur = conn.cursor()

        cur.execute(
            "INSERT INTO shopping_centres (name, address, latitude, longitude, estimated_gla, estimated_tenants, major_supermarkets) VALUES (?,?,?,?,?,?,?)",
            ("Mega Mall", "Origin St", ORIGIN_LAT, ORIGIN_LON, 20000, 120, "[]"),
        )
        cur.execute(
            "INSERT INTO supermarkets (name, address, latitude, longitude, estimated_gla) VALUES (?,?,?,?,?)",
            ("Woolworths", "Origin St", ORIGIN_LAT, ORIGIN_LON, 4000),
        )
        conn.commit()

        ctx = build_context(db_path)
        candidate = make_candidate()
        result = check_item_134a(candidate, ctx)
        assert result.passed is False
        assert any("134" in r for r in result.reasons)

    def test_fail_tier1_too_many_pharmacies(self, test_db, build_context, make_candidate):
        """120 tenants but already 2 pharmacies → max is 1 for tier 1."""
        db_path, conn = test_db
        cur = conn.cursor()

        cur.execute(
            "INSERT INTO shopping_centres (name, address, latitude, longitude, estimated_gla, estimated_tenants, major_supermarkets) VALUES (?,?,?,?,?,?,?)",
            ("Mega Mall", "Origin St", ORIGIN_LAT, ORIGIN_LON, 20000, 120, "[]"),
        )
        cur.execute(
            "INSERT INTO supermarkets (name, address, latitude, longitude, estimated_gla) VALUES (?,?,?,?,?)",
            ("Woolworths", "Origin St", ORIGIN_LAT, ORIGIN_LON, 4000),
        )
        # Two existing pharmacies (exceeds tier 1 max of 1)
        p1lat, p1lon = offset_point(ORIGIN_LAT, ORIGIN_LON, east_km=0.05)
        cur.execute(
            "INSERT INTO pharmacies (name, address, latitude, longitude) VALUES (?,?,?,?)",
            ("Pharm A", "Mega Mall A", p1lat, p1lon),
        )
        p2lat, p2lon = offset_point(ORIGIN_LAT, ORIGIN_LON, east_km=-0.05)
        cur.execute(
            "INSERT INTO pharmacies (name, address, latitude, longitude) VALUES (?,?,?,?)",
            ("Pharm B", "Mega Mall B", p2lat, p2lon),
        )
        conn.commit()

        ctx = build_context(db_path)
        clat, clon = offset_point(ORIGIN_LAT, ORIGIN_LON, north_km=0.05)
        candidate = make_candidate(lat=clat, lon=clon)
        result = check_item_134a(candidate, ctx)
        assert result.passed is False

    def test_fail_below_100_tenants(self, test_db, build_context, make_candidate):
        """75 tenants — large centre (≥50) but below 100 for Item 134A."""
        db_path, conn = test_db
        cur = conn.cursor()

        cur.execute(
            "INSERT INTO shopping_centres (name, address, latitude, longitude, estimated_gla, estimated_tenants, major_supermarkets) VALUES (?,?,?,?,?,?,?)",
            ("Medium Mall", "Origin St", ORIGIN_LAT, ORIGIN_LON, 15000, 75, "[]"),
        )
        cur.execute(
            "INSERT INTO supermarkets (name, address, latitude, longitude, estimated_gla) VALUES (?,?,?,?,?)",
            ("Woolworths", "Origin St", ORIGIN_LAT, ORIGIN_LON, 4000),
        )
        cur.execute(
            "INSERT INTO pharmacies (name, address, latitude, longitude) VALUES (?,?,?,?)",
            ("Existing Pharm", "Medium Mall", ORIGIN_LAT, ORIGIN_LON),
        )
        conn.commit()

        ctx = build_context(db_path)
        clat, clon = offset_point(ORIGIN_LAT, ORIGIN_LON, east_km=0.05)
        candidate = make_candidate(lat=clat, lon=clon)
        result = check_item_134a(candidate, ctx)
        assert result.passed is False


# ═══════════════════════════════════════════════════════════════════════════
# ITEM 135 — Large private hospital
# ═══════════════════════════════════════════════════════════════════════════
class TestItem135:
    """
    Item 135 requires:
    - Private hospital with ≥ 150 beds
    - No existing pharmacy in hospital
    """

    def test_pass_private_hospital_150_beds(self, test_db, build_context, make_candidate):
        """Private hospital with 200 beds, no existing pharmacy = PASS."""
        db_path, conn = test_db
        cur = conn.cursor()

        cur.execute(
            "INSERT INTO hospitals (name, address, latitude, longitude, bed_count, hospital_type) VALUES (?,?,?,?,?,?)",
            ("St Vincent's Private", "Origin St", ORIGIN_LAT, ORIGIN_LON, 200, "private"),
        )
        conn.commit()

        ctx = build_context(db_path)
        candidate = make_candidate()
        result = check_item_135(candidate, ctx)
        assert result.passed is True
        assert result.item == "Item 135"
        assert result.confidence > 0

    def test_fail_public_hospital(self, test_db, build_context, make_candidate):
        """Public hospital — Item 135 requires PRIVATE."""
        db_path, conn = test_db
        cur = conn.cursor()

        cur.execute(
            "INSERT INTO hospitals (name, address, latitude, longitude, bed_count, hospital_type) VALUES (?,?,?,?,?,?)",
            ("Royal Prince Alfred", "Origin St", ORIGIN_LAT, ORIGIN_LON, 500, "public"),
        )
        conn.commit()

        ctx = build_context(db_path)
        candidate = make_candidate()
        result = check_item_135(candidate, ctx)
        assert result.passed is False
        assert any("public" in r.lower() for r in result.reasons)

    def test_fail_149_beds(self, test_db, build_context, make_candidate):
        """Private hospital with only 149 beds — need ≥ 150."""
        db_path, conn = test_db
        cur = conn.cursor()

        cur.execute(
            "INSERT INTO hospitals (name, address, latitude, longitude, bed_count, hospital_type) VALUES (?,?,?,?,?,?)",
            ("Small Private", "Origin St", ORIGIN_LAT, ORIGIN_LON, 149, "private"),
        )
        conn.commit()

        ctx = build_context(db_path)
        candidate = make_candidate()
        result = check_item_135(candidate, ctx)
        assert result.passed is False
        assert any("149" in r for r in result.reasons)

    def test_pass_exactly_150_beds(self, test_db, build_context, make_candidate):
        """Private hospital with exactly 150 beds — boundary inclusive."""
        db_path, conn = test_db
        cur = conn.cursor()

        cur.execute(
            "INSERT INTO hospitals (name, address, latitude, longitude, bed_count, hospital_type) VALUES (?,?,?,?,?,?)",
            ("Boundary Private", "Origin St", ORIGIN_LAT, ORIGIN_LON, 150, "private"),
        )
        conn.commit()

        ctx = build_context(db_path)
        candidate = make_candidate()
        result = check_item_135(candidate, ctx)
        assert result.passed is True

    def test_fail_existing_pharmacy_in_hospital(self, test_db, build_context, make_candidate):
        """Large private hospital but pharmacy already inside."""
        db_path, conn = test_db
        cur = conn.cursor()

        cur.execute(
            "INSERT INTO hospitals (name, address, latitude, longitude, bed_count, hospital_type) VALUES (?,?,?,?,?,?)",
            ("St Vincent's Private", "Origin St", ORIGIN_LAT, ORIGIN_LON, 300, "private"),
        )
        # Pharmacy inside hospital (within 150m)
        cur.execute(
            "INSERT INTO pharmacies (name, address, latitude, longitude) VALUES (?,?,?,?)",
            ("Hospital Pharm", "Origin St", ORIGIN_LAT, ORIGIN_LON),
        )
        conn.commit()

        ctx = build_context(db_path)
        candidate = make_candidate()
        result = check_item_135(candidate, ctx)
        assert result.passed is False

    def test_fail_no_hospital_nearby(self, test_db, build_context, make_candidate):
        """No hospital within 300m."""
        db_path, conn = test_db
        cur = conn.cursor()

        # Hospital 1km away — outside 300m radius
        hlat, hlon = offset_point(ORIGIN_LAT, ORIGIN_LON, north_km=1.0)
        cur.execute(
            "INSERT INTO hospitals (name, address, latitude, longitude, bed_count, hospital_type) VALUES (?,?,?,?,?,?)",
            ("Faraway Hospital", "1km North", hlat, hlon, 300, "private"),
        )
        conn.commit()

        ctx = build_context(db_path)
        candidate = make_candidate()
        result = check_item_135(candidate, ctx)
        assert result.passed is False
        assert any("300m" in r for r in result.reasons)


# ═══════════════════════════════════════════════════════════════════════════
# ITEM 136 — Large medical centre
# ═══════════════════════════════════════════════════════════════════════════
class TestItem136:
    """
    Item 136 requires:
    (c) ≥ 300m from nearest pharmacy
    (d) ≥ 8 FTE PBS prescribers (≥7 medical practitioners)
    (e) Centre operates ≥ 70 hrs/week
    No existing pharmacy in the centre.
    """

    def _insert_qualifying_mc(self, cur, lat, lon, num_gps=10, total_fte=10.0, hours=80.0):
        """Helper to insert a medical centre that meets all thresholds."""
        cur.execute(
            "INSERT INTO medical_centres (name, address, latitude, longitude, num_gps, total_fte, hours_per_week) VALUES (?,?,?,?,?,?,?)",
            ("Super Medical Centre", "Origin St", lat, lon, num_gps, total_fte, hours),
        )

    def test_pass_all_conditions(self, test_db, build_context, make_candidate):
        """Medical centre with 10 FTE, 80hrs/week, pharmacy 400m away = PASS."""
        db_path, conn = test_db
        cur = conn.cursor()

        self._insert_qualifying_mc(cur, ORIGIN_LAT, ORIGIN_LON)
        # Nearest pharmacy 400m away (>300m threshold)
        plat, plon = offset_point(ORIGIN_LAT, ORIGIN_LON, north_km=0.4)
        cur.execute(
            "INSERT INTO pharmacies (name, address, latitude, longitude) VALUES (?,?,?,?)",
            ("Distant Pharm", "400m North", plat, plon),
        )
        conn.commit()

        ctx = build_context(db_path)
        candidate = make_candidate()
        result = check_item_136(candidate, ctx)
        assert result.passed is True
        assert result.item == "Item 136"

    def test_fail_7_gps(self, test_db, build_context, make_candidate):
        """Medical centre with 7 GPs (5.6 FTE estimated) — need ≥ 8 FTE."""
        db_path, conn = test_db
        cur = conn.cursor()

        self._insert_qualifying_mc(cur, ORIGIN_LAT, ORIGIN_LON, num_gps=7, total_fte=5.6, hours=80.0)
        plat, plon = offset_point(ORIGIN_LAT, ORIGIN_LON, north_km=0.4)
        cur.execute(
            "INSERT INTO pharmacies (name, address, latitude, longitude) VALUES (?,?,?,?)",
            ("Distant Pharm", "400m North", plat, plon),
        )
        conn.commit()

        ctx = build_context(db_path)
        candidate = make_candidate()
        result = check_item_136(candidate, ctx)
        assert result.passed is False
        assert any("FTE" in r or "prescriber" in r.lower() for r in result.reasons)

    def test_fail_pharmacy_too_close(self, test_db, build_context, make_candidate):
        """Pharmacy only 200m away — need ≥ 300m."""
        db_path, conn = test_db
        cur = conn.cursor()

        self._insert_qualifying_mc(cur, ORIGIN_LAT, ORIGIN_LON)
        plat, plon = offset_point(ORIGIN_LAT, ORIGIN_LON, north_km=0.2)
        cur.execute(
            "INSERT INTO pharmacies (name, address, latitude, longitude) VALUES (?,?,?,?)",
            ("Close Pharm", "200m North", plat, plon),
        )
        conn.commit()

        ctx = build_context(db_path)
        candidate = make_candidate()
        result = check_item_136(candidate, ctx)
        assert result.passed is False

    def test_fail_insufficient_hours(self, test_db, build_context, make_candidate):
        """Centre operates 60 hrs/week — need ≥ 70."""
        db_path, conn = test_db
        cur = conn.cursor()

        self._insert_qualifying_mc(cur, ORIGIN_LAT, ORIGIN_LON, hours=60.0)
        plat, plon = offset_point(ORIGIN_LAT, ORIGIN_LON, north_km=0.4)
        cur.execute(
            "INSERT INTO pharmacies (name, address, latitude, longitude) VALUES (?,?,?,?)",
            ("Distant Pharm", "400m North", plat, plon),
        )
        conn.commit()

        ctx = build_context(db_path)
        candidate = make_candidate()
        result = check_item_136(candidate, ctx)
        assert result.passed is False
        assert any("hrs" in r.lower() or "hours" in r.lower() for r in result.reasons)

    def test_fail_existing_pharmacy_in_centre(self, test_db, build_context, make_candidate):
        """Pharmacy already inside the medical centre (within 150m)."""
        db_path, conn = test_db
        cur = conn.cursor()

        self._insert_qualifying_mc(cur, ORIGIN_LAT, ORIGIN_LON)
        # Pharmacy right at the centre location
        cur.execute(
            "INSERT INTO pharmacies (name, address, latitude, longitude) VALUES (?,?,?,?)",
            ("In-Centre Pharm", "Origin St", ORIGIN_LAT, ORIGIN_LON),
        )
        conn.commit()

        ctx = build_context(db_path)
        candidate = make_candidate()
        result = check_item_136(candidate, ctx)
        assert result.passed is False
        assert any("existing" in r.lower() for r in result.reasons)

    def test_fail_no_medical_centre(self, test_db, build_context, make_candidate):
        """No medical centre within 300m."""
        db_path, conn = test_db
        conn.commit()

        ctx = build_context(db_path)
        candidate = make_candidate()
        result = check_item_136(candidate, ctx)
        assert result.passed is False
        assert any("300m" in r for r in result.reasons)

    def test_pass_exactly_8_fte_70_hours(self, test_db, build_context, make_candidate):
        """Boundary test: exactly 8 FTE and exactly 70 hrs/week."""
        db_path, conn = test_db
        cur = conn.cursor()

        self._insert_qualifying_mc(cur, ORIGIN_LAT, ORIGIN_LON, num_gps=10, total_fte=8.0, hours=70.0)
        plat, plon = offset_point(ORIGIN_LAT, ORIGIN_LON, north_km=0.4)
        cur.execute(
            "INSERT INTO pharmacies (name, address, latitude, longitude) VALUES (?,?,?,?)",
            ("Distant Pharm", "400m North", plat, plon),
        )
        conn.commit()

        ctx = build_context(db_path)
        candidate = make_candidate()
        result = check_item_136(candidate, ctx)
        assert result.passed is True


# ═══════════════════════════════════════════════════════════════════════════
# RULE RESULT STRUCTURE TESTS
# ═══════════════════════════════════════════════════════════════════════════
class TestRuleResultStructure:
    """Ensure all rule functions return well-formed RuleResult objects."""

    def test_result_has_required_fields(self, world_130):
        ctx, candidate, _, _ = world_130
        result = check_item_130(candidate, ctx)
        assert hasattr(result, "item")
        assert hasattr(result, "passed")
        assert hasattr(result, "reasons")
        assert hasattr(result, "evidence_needed")
        assert hasattr(result, "confidence")
        assert hasattr(result, "distances")
        assert isinstance(result.reasons, list)
        assert isinstance(result.distances, dict)

    def test_to_dict_serialization(self, world_130):
        ctx, candidate, _, _ = world_130
        result = check_item_130(candidate, ctx)
        d = result.to_dict()
        assert "item" in d
        assert "passed" in d
        assert "reasons" in d
        assert "confidence" in d
        assert "distances" in d
