"""
Shared utilities for all PharmacyFinder scanners.
Handles output, state detection, and summary printing.
"""
import warnings
warnings.filterwarnings("ignore")

import csv
import json
import os
import re
import sys
import time

# Ensure project root is on path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

OUTPUT_DIR = os.path.join(PROJECT_ROOT, "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)

ALL_STATES = ["NSW", "VIC", "QLD", "WA", "SA", "TAS", "NT", "ACT"]

# Standard output columns
OUTPUT_COLUMNS = [
    "lat", "lon", "suburb", "postcode", "state", "rule_item",
    "confidence", "nearest_pharmacy_km", "reason",
    # Extra useful columns
    "name", "address", "source_type",
]


def detect_state_from_address(address: str) -> str:
    """Extract Australian state code from an address string."""
    if not address:
        return ""
    addr_upper = address.upper()
    for st in ALL_STATES:
        # Match state code as word boundary
        if re.search(rf'\b{st}\b', addr_upper):
            return st
    return ""


def extract_suburb_postcode(address: str) -> tuple:
    """Try to extract suburb and postcode from address string."""
    suburb = ""
    postcode = ""
    if not address:
        return suburb, postcode
    
    # Try to find 4-digit postcode
    pc_match = re.search(r'\b(\d{4})\b', address)
    if pc_match:
        postcode = pc_match.group(1)
    
    # Try to find suburb (word before state code or postcode)
    # Common pattern: "Street, SUBURB STATE POSTCODE"
    parts = [p.strip() for p in address.replace(',', ' ').split()]
    for i, p in enumerate(parts):
        if p.upper() in ALL_STATES and i > 0:
            suburb = parts[i-1].upper()
            break
    
    return suburb, postcode


def write_results(results: list, item_name: str, extra_columns: list = None):
    """Write scanner results to CSV and JSON files."""
    # Normalize item name for filename: "Item 130" -> "item130", "Item 134A" -> "item134a"
    fname = item_name.lower().replace(" ", "").replace("item", "item")
    csv_path = os.path.join(OUTPUT_DIR, f"{fname}_opportunities.csv")
    json_path = os.path.join(OUTPUT_DIR, f"{fname}_opportunities.json")
    
    columns = list(OUTPUT_COLUMNS)
    if extra_columns:
        columns.extend(extra_columns)
    
    # Write CSV
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        for r in results:
            writer.writerow(r)
    
    # Write JSON
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, default=str)
    
    print(f"\n[Output] {len(results)} results written to:")
    print(f"  CSV:  {csv_path}")
    print(f"  JSON: {json_path}")


def print_summary(results: list, item_name: str):
    """Print summary counts per state."""
    state_counts = {}
    for st in ALL_STATES:
        state_counts[st] = 0
    unknown = 0
    
    for r in results:
        st = r.get("state", "")
        if st in state_counts:
            state_counts[st] += 1
        else:
            unknown += 1
    
    print(f"\n{'='*50}")
    print(f"  {item_name} Scanner — National Summary")
    print(f"{'='*50}")
    print(f"  Total opportunities: {len(results)}")
    print(f"{'='*50}")
    for st in ALL_STATES:
        count = state_counts[st]
        bar = "#" * min(count, 40)
        print(f"  {st:3s}: {count:5d}  {bar}")
    if unknown:
        print(f"  ???: {unknown:5d}")
    print(f"{'='*50}")


def timed_run(func):
    """Decorator to time scanner execution."""
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        elapsed = time.time() - start
        minutes = int(elapsed // 60)
        seconds = elapsed % 60
        print(f"\n[Timer] Completed in {minutes}m {seconds:.1f}s")
        return result
    return wrapper
