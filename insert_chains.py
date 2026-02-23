#!/usr/bin/env python3
"""Insert scraped chain pharmacy data into the database."""

import sqlite3
import json
import os
from datetime import datetime

DB_PATH = "pharmacy_finder.db"
DATA_DIR = "chain_data"

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def is_duplicate(conn, lat, lng, threshold=0.001):
    cursor = conn.execute(
        "SELECT id, name FROM pharmacies WHERE ABS(latitude - ?) < ? AND ABS(longitude - ?) < ?",
        (lat, threshold, lng, threshold)
    )
    return cursor.fetchone() is not None

def insert_pharmacy(conn, name, address, lat, lng, source, suburb=None, state=None, postcode=None):
    if lat is None or lng is None or lat == 0 or lng == 0:
        return False
    if is_duplicate(conn, lat, lng):
        return False
    try:
        conn.execute(
            """INSERT INTO pharmacies (name, address, latitude, longitude, source, date_scraped, suburb, state, postcode)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (name, address, lat, lng, source, datetime.now().isoformat(), suburb, state, postcode)
        )
        return True
    except sqlite3.IntegrityError:
        return False

def process_file(conn, filepath, source):
    """Process a JSON file of stores and insert into DB."""
    with open(filepath) as f:
        data = json.load(f)
    
    stores = data if isinstance(data, list) else []
    # Handle dict with state keys (like amcal)
    if isinstance(data, dict):
        for key, val in data.items():
            if isinstance(val, list):
                stores.extend(val)
    
    new_count = 0
    for s in stores:
        name = s.get('n') or s.get('name') or ''
        address = s.get('a') or s.get('address') or ''
        lat = s.get('lat') or s.get('latitude') or 0
        lng = s.get('lng') or s.get('longitude') or 0
        suburb = s.get('sub') or s.get('suburb') or None
        state = s.get('st') or s.get('state') or None
        postcode = s.get('pc') or s.get('postcode') or None
        
        if isinstance(lat, str):
            lat = float(lat)
        if isinstance(lng, str):
            lng = float(lng)
        
        if insert_pharmacy(conn, name, address, lat, lng, source, suburb, state, postcode):
            new_count += 1
    
    return len(stores), new_count

def main():
    conn = get_db()
    before_count = conn.execute("SELECT COUNT(*) FROM pharmacies").fetchone()[0]
    print(f"Database has {before_count} pharmacies before insert\n")
    
    # Process each chain data file
    chain_files = {
        'all_amcal_stores.json': 'amcal.com.au',
        'chain_data/dds.json': 'discountdrugstores.com.au',
        'chain_data/guardian.json': 'guardianpharmacies.com.au',
        'chain_data/soulpattinson.json': 'soulpattinson.com.au',
        'chain_data/blooms.json': 'bloomsthechemist.com.au',
        'chain_data/goodprice.json': 'goodpricepharmacy.com.au',
        'chain_data/wizard.json': 'wizardpharmacy.com.au',
        'chain_data/national.json': 'nationalpharmacies.com.au',
        'chain_data/capital.json': 'capitalchemist.com.au',
        'chain_data/pharmasave.json': 'pharmasave.com.au',
        'chain_data/pharmacy4less.json': 'pharmacy4less.com.au',
        'chain_data/alive.json': 'alivepharmacy.com.au',
        'chain_data/friendlies.json': 'friendliespharmacy.com.au',
        'chain_data/cincotta.json': 'cincottachemist.com.au',
    }
    
    results = {}
    for filepath, source in chain_files.items():
        if os.path.exists(filepath):
            total, new = process_file(conn, filepath, source)
            chain_name = source.split('.')[0]
            results[chain_name] = {'total': total, 'new': new}
            print(f"  {source:35s}: {total:4d} found, {new:4d} new")
            conn.commit()
        else:
            print(f"  {filepath:35s}: FILE NOT FOUND")
    
    after_count = conn.execute("SELECT COUNT(*) FROM pharmacies").fetchone()[0]
    print(f"\nAfter: {after_count} pharmacies (added {after_count - before_count})")
    
    # Show sources breakdown
    print("\nSources breakdown:")
    for row in conn.execute("SELECT source, COUNT(*) as cnt FROM pharmacies GROUP BY source ORDER BY cnt DESC"):
        print(f"  {row[0]:35s}: {row[1]:4d}")
    
    conn.close()
    
    with open("chain_scrape_results.json", "w") as f:
        json.dump(results, f, indent=2)

if __name__ == "__main__":
    main()
