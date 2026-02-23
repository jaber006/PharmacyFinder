#!/usr/bin/env python3
"""
Scrape chain pharmacy store locators and add new pharmacies to the database.
"""

import requests
import sqlite3
import json
import re
import time
import math
from datetime import datetime

DB_PATH = "pharmacy_finder.db"
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/html, */*',
    'Accept-Language': 'en-AU,en;q=0.9',
}

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def is_duplicate(conn, lat, lng, threshold=0.001):
    """Check if a pharmacy exists within ~100m (0.001 degrees)."""
    cursor = conn.execute(
        "SELECT id, name FROM pharmacies WHERE ABS(latitude - ?) < ? AND ABS(longitude - ?) < ?",
        (lat, threshold, lng, threshold)
    )
    row = cursor.fetchone()
    return row is not None

def insert_pharmacy(conn, name, address, lat, lng, source, suburb=None, state=None, postcode=None):
    """Insert a pharmacy if not duplicate."""
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

def scrape_blooms():
    """Blooms The Chemist - try sitemap or store pages."""
    print("\n=== Blooms The Chemist ===")
    stores = []
    
    # Try getting sitemap for store pages
    try:
        r = requests.get("https://www.bloomsthechemist.com.au/sitemap.xml", headers=HEADERS, timeout=15)
        if r.status_code == 200:
            # Find store URLs
            store_urls = re.findall(r'<loc>(https://www\.bloomsthechemist\.com\.au/store/[^<]+)</loc>', r.text)
            if not store_urls:
                store_urls = re.findall(r'<loc>(https://www\.bloomsthechemist\.com\.au/pharmacy/[^<]+)</loc>', r.text)
            print(f"  Found {len(store_urls)} store URLs in sitemap")
            
            for url in store_urls[:5]:  # Sample
                print(f"  Sample URL: {url}")
    except Exception as e:
        print(f"  Sitemap error: {e}")
    
    # Try the store finder page for embedded JSON data
    try:
        r = requests.get("https://www.bloomsthechemist.com.au/store-finder", headers=HEADERS, timeout=15, allow_redirects=True)
        print(f"  Store finder: {r.status_code}, {len(r.text)} bytes")
        
        # Look for embedded store data patterns
        patterns = [
            r'var\s+stores\s*=\s*(\[.*?\]);',
            r'"stores"\s*:\s*(\[.*?\])',
            r'storeData\s*=\s*(\[.*?\]);',
            r'locations\s*=\s*(\[.*?\]);',
            r'markers\s*=\s*(\[.*?\]);',
        ]
        for pat in patterns:
            matches = re.findall(pat, r.text, re.DOTALL)
            if matches:
                print(f"  Found data with pattern: {pat[:30]}")
                try:
                    data = json.loads(matches[0])
                    print(f"  {len(data)} stores found")
                    stores = data
                    break
                except:
                    pass
        
        # Check for Yext/Google Maps embedded data
        lat_matches = re.findall(r'"lat(?:itude)?"\s*:\s*"?(-?\d+\.?\d*)"?', r.text)
        print(f"  Found {len(lat_matches)} lat values in page")
        
    except Exception as e:
        print(f"  Error: {e}")
    
    return stores

def scrape_pharmacy4less():
    """Pharmacy 4 Less - Magento/amasty store locator."""
    print("\n=== Pharmacy 4 Less ===")
    stores = []
    
    try:
        # Amasty store locator typically has this endpoint
        r = requests.get(
            "https://www.pharmacy4less.com.au/amlocator/index/ajax/",
            headers={**HEADERS, 'X-Requested-With': 'XMLHttpRequest'},
            timeout=15
        )
        print(f"  Ajax endpoint: {r.status_code}")
        if r.status_code == 200:
            try:
                data = r.json()
                print(f"  Data keys: {list(data.keys()) if isinstance(data, dict) else 'list'}")
                stores = data if isinstance(data, list) else data.get('items', [])
            except:
                pass
    except Exception as e:
        print(f"  Error: {e}")
    
    # Try the store locator page
    try:
        r = requests.get("https://www.pharmacy4less.com.au/storelocator", headers=HEADERS, timeout=15)
        print(f"  Store locator page: {r.status_code}, {len(r.text)} bytes")
        
        # Look for amLocator data
        match = re.search(r'amLocator\s*=\s*({.*?});', r.text, re.DOTALL)
        if match:
            print(f"  Found amLocator data")
        
        # Look for JSON array of stores
        match = re.search(r'"items"\s*:\s*(\[.*?\])\s*[,}]', r.text, re.DOTALL)
        if match:
            try:
                items = json.loads(match.group(1))
                print(f"  Found {len(items)} items")
                stores = items
            except:
                pass
                
    except Exception as e:
        print(f"  Error: {e}")
    
    return stores

def scrape_nationalpharmacies():
    """National Pharmacies."""
    print("\n=== National Pharmacies ===")
    stores = []
    
    try:
        r = requests.get("https://www.nationalpharmacies.com.au/stores", headers=HEADERS, timeout=15)
        print(f"  Stores page: {r.status_code}, {len(r.text)} bytes")
        
        # Look for embedded store data
        lat_matches = re.findall(r'"lat(?:itude)?"\s*:\s*"?(-?\d+\.?\d*)"?', r.text)
        print(f"  Found {len(lat_matches)} lat values")
        
        # Look for store JSON
        match = re.search(r'stores\s*[=:]\s*(\[{.*?}\])', r.text, re.DOTALL)
        if match:
            try:
                stores = json.loads(match.group(1))
                print(f"  Found {len(stores)} stores")
            except:
                pass
    except Exception as e:
        print(f"  Error: {e}")
    
    return stores

def scrape_capitalchemist():
    """Capital Chemist."""
    print("\n=== Capital Chemist ===")
    stores = []
    
    try:
        r = requests.get("https://www.capitalchemist.com.au/stores", headers=HEADERS, timeout=15)
        print(f"  Stores page: {r.status_code}, {len(r.text)} bytes")
        
        lat_matches = re.findall(r'"lat(?:itude)?"\s*:\s*"?(-?\d+\.?\d*)"?', r.text)
        print(f"  Found {len(lat_matches)} lat values")
        
    except Exception as e:
        print(f"  Error: {e}")
    
    return stores

def explore_chain(name, url):
    """Generic explorer for a chain URL."""
    print(f"\n=== {name} ===")
    try:
        r = requests.get(url, headers=HEADERS, timeout=15, allow_redirects=True)
        print(f"  URL: {r.url}")
        print(f"  Status: {r.status_code}, {len(r.text)} bytes")
        
        # Look for coordinate data
        lat_matches = re.findall(r'"lat(?:itude)?"\s*:\s*"?(-?\d+\.?\d*)"?', r.text)
        lng_matches = re.findall(r'"lng|lon(?:gitude)?"\s*:\s*"?(-?\d+\.?\d*)"?', r.text)
        print(f"  Lat values: {len(lat_matches)}, Lng values: {len(lng_matches)}")
        
        # Look for JSON store data
        for pat in [r'"stores"\s*:\s*\[', r'"locations"\s*:\s*\[', r'"markers"\s*:\s*\[', r'"results"\s*:\s*\[']:
            if re.search(pat, r.text):
                print(f"  Found pattern: {pat}")
        
        # Check for sitemaps
        return r.text
    except Exception as e:
        print(f"  Error: {e}")
        return ""

if __name__ == "__main__":
    chains = {
        "Blooms The Chemist": "https://www.bloomsthechemist.com.au/store-finder",
        "Discount Drug Stores": "https://www.discountdrugstores.com.au/store-locator/",
        "Soul Pattinson": "https://soulpattinson.com.au/store-locator/",
        "Good Price Pharmacy": "https://www.goodpricepharmacy.com.au/store-locator/",
        "Wizard Pharmacy": "https://www.wizardpharmacy.com.au/store-locator",
        "National Pharmacies": "https://www.nationalpharmacies.com.au/stores",
        "Capital Chemist": "https://www.capitalchemist.com.au/stores",
        "PharmaSave": "https://www.pharmasave.com.au/store-locator",
        "Pharmacy 4 Less": "https://www.pharmacy4less.com.au/storelocator",
        "Alive Pharmacy": "https://www.alivepharmacy.com.au/store-locator",
        "Friendlies": "https://www.friendliespharmacy.com.au/store-locator",
        "Cincotta": "https://www.cincottachemist.com.au/stores",
    }
    
    for name, url in chains.items():
        explore_chain(name, url)
