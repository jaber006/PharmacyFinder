#!/usr/bin/env python3
"""
One-time migration: classify supermarket brands and update estimated_gla
for all existing supermarket records in the database.
"""
import sqlite3
import re

DB_PATH = 'pharmacy_finder.db'

# Brand classification rules (order matters — specific before generic)
BRAND_RULES = [
    ('woolworths', r'woolworths|woolies', 3500.0, 'high'),
    ('coles', r'\bcoles\b', 3500.0, 'high'),
    ('aldi', r'\baldi\b', 1650.0, 'high'),
    ('drakes', r'\bdrakes?\b', 2500.0, 'medium'),
    ('harris_farm', r'harris\s*farm', 2500.0, 'medium'),
    ('iga_express', r'iga\s*(express|x-?press|xpress)', 400.0, 'medium'),
    ('iga_everyday', r'iga\s*everyday', 650.0, 'medium'),
    ('iga', r'\biga\b', 1500.0, 'medium'),
    ('foodworks', r'\bfoodworks?\b', 1000.0, 'medium'),
]


def classify(name: str):
    """Return (brand, estimated_gla, gla_confidence) for a supermarket name."""
    name_lower = name.lower()
    for brand, pattern, gla, conf in BRAND_RULES:
        if re.search(pattern, name_lower):
            return brand, gla, conf
    return 'independent', 800.0, 'low'


def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # Ensure columns exist
    for col in ["estimated_gla REAL", "brand TEXT",
                 "gla_confidence TEXT DEFAULT 'estimated'"]:
        try:
            cur.execute(f"ALTER TABLE supermarkets ADD COLUMN {col}")
        except sqlite3.OperationalError:
            pass

    cur.execute("SELECT id, name, floor_area_sqm FROM supermarkets")
    rows = cur.fetchall()
    print(f"Migrating {len(rows)} supermarkets...")

    updated = 0
    for row in rows:
        brand, est_gla, conf = classify(row['name'] or '')
        cur.execute("""
            UPDATE supermarkets
            SET estimated_gla = ?, brand = ?, gla_confidence = ?
            WHERE id = ?
        """, (est_gla, brand, conf, row['id']))
        updated += 1

    conn.commit()
    conn.close()
    print(f"Updated {updated} supermarkets with brand/GLA classification.")

    # Print summary
    conn2 = sqlite3.connect(DB_PATH)
    cur2 = conn2.cursor()
    cur2.execute("SELECT brand, COUNT(*), AVG(estimated_gla) FROM supermarkets GROUP BY brand ORDER BY COUNT(*) DESC")
    print("\nBrand distribution:")
    for row in cur2.fetchall():
        print(f"  {row[0] or 'unknown':20s}  count={row[1]:3d}  avg_gla={row[2]:,.0f}")
    conn2.close()


if __name__ == '__main__':
    main()
