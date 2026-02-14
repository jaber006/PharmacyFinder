#!/usr/bin/env python3
"""
One-time migration: classify shopping centres with estimated_tenants and centre_class.
"""
import sqlite3
import re
import json

DB_PATH = 'pharmacy_finder.db'


def estimate_tenants(name: str, gla_sqm: float) -> int:
    name_lower = name.lower()
    if 'westfield' in name_lower:
        if gla_sqm >= 80000:
            return 300
        elif gla_sqm >= 50000:
            return 200
        else:
            return 120
    elif 'chadstone' in name_lower:
        return 550
    elif 'pacific fair' in name_lower:
        return 400

    if gla_sqm >= 50000:
        return int(gla_sqm / 180)
    elif gla_sqm >= 15000:
        return int(gla_sqm / 200)
    elif gla_sqm >= 5000:
        return int(gla_sqm / 250)
    elif gla_sqm >= 1000:
        return int(gla_sqm / 300)
    else:
        return max(5, int(gla_sqm / 400))


def classify_centre(name: str, gla_sqm: float, tenants: int) -> str:
    name_lower = name.lower()
    if 'westfield' in name_lower or 'chadstone' in name_lower:
        return 'major'
    if gla_sqm >= 15000:
        return 'major'
    elif gla_sqm >= 5000:
        if tenants >= 50:
            return 'large'
        elif tenants >= 15:
            return 'small'
        else:
            return 'small'
    elif gla_sqm >= 1000:
        if tenants >= 15:
            return 'small'
        else:
            return 'strip'
    else:
        return 'strip'


def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # Ensure columns exist
    for col in ["estimated_gla REAL", "estimated_tenants INTEGER",
                 "centre_class TEXT DEFAULT 'unknown'"]:
        try:
            cur.execute(f"ALTER TABLE shopping_centres ADD COLUMN {col}")
        except sqlite3.OperationalError:
            pass

    cur.execute("SELECT id, name, gla_sqm FROM shopping_centres")
    rows = cur.fetchall()
    print(f"Migrating {len(rows)} shopping centres...")

    for row in rows:
        gla = row['gla_sqm'] or 0
        tenants = estimate_tenants(row['name'] or '', gla)
        cls = classify_centre(row['name'] or '', gla, tenants)
        cur.execute("""
            UPDATE shopping_centres
            SET estimated_gla = ?, estimated_tenants = ?, centre_class = ?
            WHERE id = ?
        """, (gla, tenants, cls, row['id']))

    conn.commit()

    # Summary
    cur.execute("""
        SELECT centre_class, COUNT(*), AVG(gla_sqm), AVG(estimated_tenants)
        FROM shopping_centres GROUP BY centre_class ORDER BY AVG(gla_sqm) DESC
    """)
    print("\nCentre classification:")
    for row in cur.fetchall():
        print(f"  {row[0] or 'unknown':10s}  count={row[1]:3d}  avg_gla={row[2]:,.0f}  avg_tenants={row[3]:.0f}")

    conn.close()


if __name__ == '__main__':
    main()
