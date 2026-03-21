"""
Shared fixtures for PharmacyFinder test suite.

Builds an in-memory SQLite database pre-loaded with controlled reference
data so every rule-engine test runs against a known, deterministic world.
"""
import os
import sys
import sqlite3
import pytest

# Ensure project root is on sys.path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from engine.models import Candidate
from engine.context import EvaluationContext


# ---------------------------------------------------------------------------
# Coordinate helpers
# ---------------------------------------------------------------------------
# Sydney CBD as origin (~-33.8688, 151.2093)
ORIGIN_LAT, ORIGIN_LON = -33.8688, 151.2093

def offset_point(lat, lon, north_km=0.0, east_km=0.0):
    """Shift a lat/lon by approximate km offsets (good enough for tests)."""
    # 1 degree latitude ≈ 111 km
    # 1 degree longitude ≈ 111 km * cos(lat)
    import math
    new_lat = lat + north_km / 111.0
    new_lon = lon + east_km / (111.0 * math.cos(math.radians(lat)))
    return round(new_lat, 6), round(new_lon, 6)


# ---------------------------------------------------------------------------
# Database fixture
# ---------------------------------------------------------------------------
def _create_tables(conn):
    """Create all reference tables needed by EvaluationContext."""
    cur = conn.cursor()
    cur.executescript("""
        CREATE TABLE IF NOT EXISTS pharmacies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            address TEXT NOT NULL,
            latitude REAL NOT NULL,
            longitude REAL NOT NULL,
            source TEXT,
            date_scraped TEXT,
            suburb TEXT,
            state TEXT,
            postcode TEXT,
            opening_hours TEXT
        );
        CREATE TABLE IF NOT EXISTS gps (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            address TEXT NOT NULL,
            latitude REAL NOT NULL,
            longitude REAL NOT NULL,
            fte REAL,
            hours_per_week REAL,
            date_scraped TEXT
        );
        CREATE TABLE IF NOT EXISTS supermarkets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            address TEXT NOT NULL,
            latitude REAL NOT NULL,
            longitude REAL NOT NULL,
            floor_area_sqm REAL,
            estimated_gla REAL,
            brand TEXT,
            gla_confidence TEXT DEFAULT 'estimated',
            date_scraped TEXT
        );
        CREATE TABLE IF NOT EXISTS hospitals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            address TEXT NOT NULL,
            latitude REAL NOT NULL,
            longitude REAL NOT NULL,
            bed_count INTEGER,
            hospital_type TEXT,
            date_scraped TEXT
        );
        CREATE TABLE IF NOT EXISTS shopping_centres (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            address TEXT NOT NULL,
            latitude REAL NOT NULL,
            longitude REAL NOT NULL,
            gla_sqm REAL,
            estimated_gla REAL,
            estimated_tenants INTEGER,
            centre_class TEXT DEFAULT 'unknown',
            major_supermarkets TEXT,
            date_scraped TEXT
        );
        CREATE TABLE IF NOT EXISTS medical_centres (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            address TEXT NOT NULL,
            latitude REAL NOT NULL,
            longitude REAL NOT NULL,
            num_gps INTEGER DEFAULT 0,
            total_fte REAL DEFAULT 0,
            practitioners_json TEXT,
            hours_per_week REAL DEFAULT 0,
            source TEXT,
            state TEXT,
            date_scraped TEXT
        );
        CREATE TABLE IF NOT EXISTS v2_results (
            id TEXT PRIMARY KEY,
            name TEXT,
            address TEXT,
            latitude REAL,
            longitude REAL,
            state TEXT,
            source_type TEXT,
            passed_any INTEGER,
            primary_rule TEXT,
            commercial_score REAL,
            best_confidence REAL,
            rules_json TEXT,
            all_rules_json TEXT,
            profitability_score REAL,
            date_evaluated TEXT
        );
        CREATE TABLE IF NOT EXISTS opportunities (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            latitude REAL NOT NULL,
            longitude REAL NOT NULL,
            address TEXT,
            qualifying_rules TEXT NOT NULL,
            evidence TEXT NOT NULL,
            confidence REAL DEFAULT 0.0,
            nearest_pharmacy_km REAL,
            nearest_pharmacy_name TEXT,
            poi_name TEXT,
            poi_type TEXT,
            region TEXT,
            pop_5km INTEGER,
            date_scanned TEXT
        );
    """)
    conn.commit()


def _build_test_context(db_path):
    """
    Build an EvaluationContext that reads from the given SQLite file.

    We monkey-patch __init__ to skip the default path resolution and use
    our test database path directly.
    """
    ctx = EvaluationContext.__new__(EvaluationContext)
    ctx.db_path = db_path
    ctx.state_filter = None
    ctx.pharmacies = []
    ctx.gps = []
    ctx.supermarkets = []
    ctx.hospitals = []
    ctx.shopping_centres = []
    ctx.medical_centres = []
    ctx._pharm_idx = None
    ctx._gp_idx = None
    ctx._super_idx = None
    ctx._hosp_idx = None
    ctx._sc_idx = None
    ctx._mc_idx = None
    ctx._load_data()
    return ctx


# ---------------------------------------------------------------------------
# Public fixtures
# ---------------------------------------------------------------------------
@pytest.fixture
def test_db(tmp_path):
    """
    Yield a (db_path, connection) tuple with empty tables.
    The caller populates data, then calls ``build_context(db_path)`` to get
    a ready-to-use EvaluationContext.
    """
    db_path = str(tmp_path / "test_pharmacy.db")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    _create_tables(conn)
    yield db_path, conn
    conn.close()


@pytest.fixture
def build_context():
    """Factory fixture: call with a db_path to get an EvaluationContext."""
    def _factory(db_path):
        return _build_test_context(db_path)
    return _factory


@pytest.fixture
def make_candidate():
    """Factory fixture to create Candidate objects with sensible defaults."""
    _counter = [0]

    def _factory(lat=ORIGIN_LAT, lon=ORIGIN_LON, **overrides):
        _counter[0] += 1
        defaults = dict(
            id=f"test_{_counter[0]}",
            latitude=lat,
            longitude=lon,
            name=f"Test Site {_counter[0]}",
            address=f"{_counter[0]} Test St",
            source_type="gap",
            state="NSW",
        )
        defaults.update(overrides)
        return Candidate(**defaults)

    return _factory


# ---------------------------------------------------------------------------
# Convenience: a fully-loaded "standard" world for Item 130 tests
# ---------------------------------------------------------------------------
@pytest.fixture
def world_130(test_db, build_context, make_candidate):
    """
    Pre-populated world for Item 130 tests.

    Layout (distances from ORIGIN):
    - Pharmacy "Existing Pharm" at 2.0 km north  (well beyond 1.5 km)
    - Supermarket "Big Woolies" at 0.3 km east (1200 sqm GLA — qualifies for (b)(i))
    - GP "Dr Smith" at 0.2 km east (1 FTE — qualifies for (b)(i))
    """
    db_path, conn = test_db
    cur = conn.cursor()

    pharm_lat, pharm_lon = offset_point(ORIGIN_LAT, ORIGIN_LON, north_km=2.0)
    cur.execute(
        "INSERT INTO pharmacies (name, address, latitude, longitude) VALUES (?,?,?,?)",
        ("Existing Pharm", "2km North St", pharm_lat, pharm_lon),
    )

    super_lat, super_lon = offset_point(ORIGIN_LAT, ORIGIN_LON, east_km=0.3)
    cur.execute(
        "INSERT INTO supermarkets (name, address, latitude, longitude, estimated_gla) VALUES (?,?,?,?,?)",
        ("Big Woolies", "300m East St", super_lat, super_lon, 1200),
    )

    gp_lat, gp_lon = offset_point(ORIGIN_LAT, ORIGIN_LON, east_km=0.2)
    cur.execute(
        "INSERT INTO gps (name, address, latitude, longitude, fte) VALUES (?,?,?,?,?)",
        ("Dr Smith", "200m East St", gp_lat, gp_lon, 1.0),
    )
    conn.commit()

    ctx = build_context(db_path)
    candidate = make_candidate()
    return ctx, candidate, db_path, conn
