"""
Database utilities for managing SQLite storage of properties, reference data,
and opportunity zones.
"""
import sqlite3
from typing import List, Dict, Optional, Tuple
from datetime import datetime
import json


class Database:
    def __init__(self, db_path: str = "pharmacy_finder.db"):
        self.db_path = db_path
        self.connection = None

    def connect(self):
        """Establish database connection and create tables if needed."""
        self.connection = sqlite3.connect(self.db_path)
        self.connection.row_factory = sqlite3.Row
        self.create_tables()

    def close(self):
        """Close database connection."""
        if self.connection:
            self.connection.close()

    def create_tables(self):
        """Create all required tables."""
        cursor = self.connection.cursor()

        # Properties table - commercial real estate listings
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS properties (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                address TEXT NOT NULL,
                latitude REAL,
                longitude REAL,
                listing_url TEXT,
                property_type TEXT,
                size_sqm REAL,
                agent_name TEXT,
                agent_phone TEXT,
                agent_email TEXT,
                date_scraped TEXT,
                UNIQUE(address, listing_url)
            )
        """)

        # Pharmacies table - existing pharmacy locations
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS pharmacies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT,
                address TEXT NOT NULL,
                latitude REAL NOT NULL,
                longitude REAL NOT NULL,
                source TEXT,
                date_scraped TEXT,
                UNIQUE(address)
            )
        """)

        # GPs table - general practitioner practices
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS gps (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT,
                address TEXT NOT NULL,
                latitude REAL NOT NULL,
                longitude REAL NOT NULL,
                fte REAL,
                hours_per_week REAL,
                date_scraped TEXT,
                UNIQUE(name, address)
            )
        """)

        # Supermarkets table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS supermarkets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT,
                address TEXT NOT NULL,
                latitude REAL NOT NULL,
                longitude REAL NOT NULL,
                floor_area_sqm REAL,
                date_scraped TEXT,
                UNIQUE(address)
            )
        """)

        # Hospitals table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS hospitals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                address TEXT NOT NULL,
                latitude REAL NOT NULL,
                longitude REAL NOT NULL,
                bed_count INTEGER,
                hospital_type TEXT,
                date_scraped TEXT,
                UNIQUE(name, address)
            )
        """)

        # Shopping centres table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS shopping_centres (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                address TEXT NOT NULL,
                latitude REAL NOT NULL,
                longitude REAL NOT NULL,
                gla_sqm REAL,
                major_supermarkets TEXT,
                date_scraped TEXT,
                UNIQUE(name, address)
            )
        """)

        # Medical centres table (for Item 136)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS medical_centres (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                address TEXT NOT NULL,
                latitude REAL NOT NULL,
                longitude REAL NOT NULL,
                num_gps INTEGER DEFAULT 0,
                total_fte REAL DEFAULT 0,
                source TEXT,
                date_scraped TEXT,
                UNIQUE(name, address)
            )
        """)

        # Opportunity zones table - proactive scanner results
        cursor.execute("""
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
                date_scanned TEXT,
                UNIQUE(latitude, longitude, qualifying_rules)
            )
        """)

        # Eligible properties table - properties that match rules
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS eligible_properties (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                property_id INTEGER NOT NULL,
                rule_item TEXT NOT NULL,
                evidence TEXT,
                date_checked TEXT,
                FOREIGN KEY (property_id) REFERENCES properties(id),
                UNIQUE(property_id, rule_item)
            )
        """)

        # Geocoding cache to minimize API calls
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS geocode_cache (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                address TEXT NOT NULL UNIQUE,
                latitude REAL NOT NULL,
                longitude REAL NOT NULL,
                date_cached TEXT
            )
        """)

        self.connection.commit()

    def insert_property(self, property_data: Dict) -> int:
        """Insert a commercial property listing."""
        cursor = self.connection.cursor()
        cursor.execute("""
            INSERT OR IGNORE INTO properties
            (address, latitude, longitude, listing_url, property_type, size_sqm,
             agent_name, agent_phone, agent_email, date_scraped)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            property_data.get('address'),
            property_data.get('latitude'),
            property_data.get('longitude'),
            property_data.get('listing_url'),
            property_data.get('property_type'),
            property_data.get('size_sqm'),
            property_data.get('agent_name'),
            property_data.get('agent_phone'),
            property_data.get('agent_email'),
            datetime.now().isoformat()
        ))
        self.connection.commit()
        return cursor.lastrowid

    def insert_pharmacy(self, pharmacy_data: Dict) -> int:
        """Insert a pharmacy location."""
        cursor = self.connection.cursor()
        cursor.execute("""
            INSERT OR IGNORE INTO pharmacies
            (name, address, latitude, longitude, source, date_scraped)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            pharmacy_data.get('name'),
            pharmacy_data.get('address'),
            pharmacy_data.get('latitude'),
            pharmacy_data.get('longitude'),
            pharmacy_data.get('source'),
            datetime.now().isoformat()
        ))
        self.connection.commit()
        return cursor.lastrowid

    def insert_gp(self, gp_data: Dict) -> int:
        """Insert a GP practice."""
        cursor = self.connection.cursor()
        cursor.execute("""
            INSERT OR IGNORE INTO gps
            (name, address, latitude, longitude, fte, hours_per_week, date_scraped)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            gp_data.get('name'),
            gp_data.get('address'),
            gp_data.get('latitude'),
            gp_data.get('longitude'),
            gp_data.get('fte'),
            gp_data.get('hours_per_week'),
            datetime.now().isoformat()
        ))
        self.connection.commit()
        return cursor.lastrowid

    def insert_supermarket(self, supermarket_data: Dict) -> int:
        """Insert a supermarket location."""
        cursor = self.connection.cursor()
        cursor.execute("""
            INSERT OR IGNORE INTO supermarkets
            (name, address, latitude, longitude, floor_area_sqm, date_scraped)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            supermarket_data.get('name'),
            supermarket_data.get('address'),
            supermarket_data.get('latitude'),
            supermarket_data.get('longitude'),
            supermarket_data.get('floor_area_sqm'),
            datetime.now().isoformat()
        ))
        self.connection.commit()
        return cursor.lastrowid

    def insert_hospital(self, hospital_data: Dict) -> int:
        """Insert a hospital location."""
        cursor = self.connection.cursor()
        cursor.execute("""
            INSERT OR IGNORE INTO hospitals
            (name, address, latitude, longitude, bed_count, hospital_type, date_scraped)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            hospital_data.get('name'),
            hospital_data.get('address'),
            hospital_data.get('latitude'),
            hospital_data.get('longitude'),
            hospital_data.get('bed_count'),
            hospital_data.get('hospital_type'),
            datetime.now().isoformat()
        ))
        self.connection.commit()
        return cursor.lastrowid

    def insert_shopping_centre(self, centre_data: Dict) -> int:
        """Insert a shopping centre."""
        cursor = self.connection.cursor()
        major_supermarkets = json.dumps(centre_data.get('major_supermarkets', []))
        cursor.execute("""
            INSERT OR IGNORE INTO shopping_centres
            (name, address, latitude, longitude, gla_sqm, major_supermarkets, date_scraped)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            centre_data.get('name'),
            centre_data.get('address'),
            centre_data.get('latitude'),
            centre_data.get('longitude'),
            centre_data.get('gla_sqm'),
            major_supermarkets,
            datetime.now().isoformat()
        ))
        self.connection.commit()
        return cursor.lastrowid

    def insert_eligible_property(self, property_id: int, rule_item: str, evidence: str):
        """Record a property as eligible under a specific rule."""
        cursor = self.connection.cursor()
        cursor.execute("""
            INSERT OR IGNORE INTO eligible_properties
            (property_id, rule_item, evidence, date_checked)
            VALUES (?, ?, ?, ?)
        """, (property_id, rule_item, evidence, datetime.now().isoformat()))
        self.connection.commit()

    def get_all_pharmacies(self) -> List[Dict]:
        """Retrieve all pharmacy locations."""
        cursor = self.connection.cursor()
        cursor.execute("SELECT * FROM pharmacies")
        return [dict(row) for row in cursor.fetchall()]

    def get_all_gps(self) -> List[Dict]:
        """Retrieve all GP practices."""
        cursor = self.connection.cursor()
        cursor.execute("SELECT * FROM gps")
        return [dict(row) for row in cursor.fetchall()]

    def get_all_supermarkets(self) -> List[Dict]:
        """Retrieve all supermarkets."""
        cursor = self.connection.cursor()
        cursor.execute("SELECT * FROM supermarkets")
        return [dict(row) for row in cursor.fetchall()]

    def get_all_hospitals(self) -> List[Dict]:
        """Retrieve all hospitals."""
        cursor = self.connection.cursor()
        cursor.execute("SELECT * FROM hospitals")
        return [dict(row) for row in cursor.fetchall()]

    def get_all_shopping_centres(self) -> List[Dict]:
        """Retrieve all shopping centres."""
        cursor = self.connection.cursor()
        cursor.execute("SELECT * FROM shopping_centres")
        results = []
        for row in cursor.fetchall():
            data = dict(row)
            data['major_supermarkets'] = json.loads(data['major_supermarkets'])
            results.append(data)
        return results

    def get_all_properties(self) -> List[Dict]:
        """Retrieve all commercial properties."""
        cursor = self.connection.cursor()
        cursor.execute("SELECT * FROM properties")
        return [dict(row) for row in cursor.fetchall()]

    def get_eligible_properties(self) -> List[Dict]:
        """Retrieve all eligible properties with their matching rules."""
        cursor = self.connection.cursor()
        cursor.execute("""
            SELECT
                p.*,
                GROUP_CONCAT(e.rule_item) as qualifying_rules,
                GROUP_CONCAT(e.evidence, ' | ') as evidence
            FROM properties p
            JOIN eligible_properties e ON p.id = e.property_id
            GROUP BY p.id
            ORDER BY COUNT(e.rule_item) DESC
        """)
        return [dict(row) for row in cursor.fetchall()]

    def cache_geocode(self, address: str, latitude: float, longitude: float):
        """Cache a geocoded address."""
        cursor = self.connection.cursor()
        cursor.execute("""
            INSERT OR IGNORE INTO geocode_cache (address, latitude, longitude, date_cached)
            VALUES (?, ?, ?, ?)
        """, (address, latitude, longitude, datetime.now().isoformat()))
        self.connection.commit()

    def get_cached_geocode(self, address: str) -> Optional[Tuple[float, float]]:
        """Retrieve cached geocode for an address."""
        cursor = self.connection.cursor()
        cursor.execute("""
            SELECT latitude, longitude FROM geocode_cache WHERE address = ?
        """, (address,))
        result = cursor.fetchone()
        if result:
            return (result[0], result[1])
        return None

    def clear_properties(self):
        """Clear all commercial property listings (for fresh scrape)."""
        cursor = self.connection.cursor()
        cursor.execute("DELETE FROM properties")
        cursor.execute("DELETE FROM eligible_properties")
        self.connection.commit()

    def clear_reference_data(self):
        """Clear all reference data (pharmacies, GPs, etc.) for refresh."""
        cursor = self.connection.cursor()
        cursor.execute("DELETE FROM pharmacies")
        cursor.execute("DELETE FROM gps")
        cursor.execute("DELETE FROM supermarkets")
        cursor.execute("DELETE FROM hospitals")
        cursor.execute("DELETE FROM shopping_centres")
        self.connection.commit()

    def get_property_by_id(self, property_id: int) -> Optional[Dict]:
        """Get a specific property by ID."""
        cursor = self.connection.cursor()
        cursor.execute("SELECT * FROM properties WHERE id = ?", (property_id,))
        result = cursor.fetchone()
        return dict(result) if result else None

    # ── Opportunity zone methods ──────────────────────────────────

    def insert_opportunity(self, opportunity_data: Dict) -> int:
        """Insert a discovered opportunity zone."""
        cursor = self.connection.cursor()
        cursor.execute("""
            INSERT OR IGNORE INTO opportunities
            (latitude, longitude, address, qualifying_rules, evidence,
             confidence, nearest_pharmacy_km, nearest_pharmacy_name,
             poi_name, poi_type, region, date_scanned)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            opportunity_data['latitude'],
            opportunity_data['longitude'],
            opportunity_data.get('address', ''),
            opportunity_data['qualifying_rules'],
            opportunity_data['evidence'],
            opportunity_data.get('confidence', 0.0),
            opportunity_data.get('nearest_pharmacy_km'),
            opportunity_data.get('nearest_pharmacy_name'),
            opportunity_data.get('poi_name', ''),
            opportunity_data.get('poi_type', ''),
            opportunity_data.get('region', ''),
            datetime.now().isoformat(),
        ))
        self.connection.commit()
        return cursor.lastrowid

    def get_all_opportunities(self, region: str = None) -> List[Dict]:
        """Retrieve all opportunity zones, optionally filtered by region."""
        cursor = self.connection.cursor()
        if region:
            cursor.execute(
                "SELECT * FROM opportunities WHERE region = ? ORDER BY confidence DESC",
                (region,),
            )
        else:
            cursor.execute("SELECT * FROM opportunities ORDER BY confidence DESC")
        return [dict(row) for row in cursor.fetchall()]

    def clear_opportunities(self, region: str = None):
        """Clear opportunity zones, optionally only for a specific region."""
        cursor = self.connection.cursor()
        if region:
            cursor.execute("DELETE FROM opportunities WHERE region = ?", (region,))
        else:
            cursor.execute("DELETE FROM opportunities")
        self.connection.commit()

    # ── Medical centre methods ────────────────────────────────────

    def insert_medical_centre(self, data: Dict) -> int:
        """Insert a medical centre."""
        cursor = self.connection.cursor()
        cursor.execute("""
            INSERT OR IGNORE INTO medical_centres
            (name, address, latitude, longitude, num_gps, total_fte, source, date_scraped)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            data.get('name'),
            data.get('address'),
            data.get('latitude'),
            data.get('longitude'),
            data.get('num_gps', 0),
            data.get('total_fte', 0),
            data.get('source', ''),
            datetime.now().isoformat(),
        ))
        self.connection.commit()
        return cursor.lastrowid

    def get_all_medical_centres(self) -> List[Dict]:
        """Retrieve all medical centres."""
        cursor = self.connection.cursor()
        cursor.execute("SELECT * FROM medical_centres")
        return [dict(row) for row in cursor.fetchall()]

    def clear_medical_centres(self):
        """Clear all medical centres."""
        cursor = self.connection.cursor()
        cursor.execute("DELETE FROM medical_centres")
        self.connection.commit()

    # ── Summary / stats ───────────────────────────────────────────

    def get_reference_data_stats(self) -> Dict:
        """Get counts of all reference data tables."""
        cursor = self.connection.cursor()
        stats = {}
        for table in ['pharmacies', 'gps', 'supermarkets', 'hospitals',
                       'shopping_centres', 'medical_centres']:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            stats[table] = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM opportunities")
        stats['opportunities'] = cursor.fetchone()[0]
        return stats
