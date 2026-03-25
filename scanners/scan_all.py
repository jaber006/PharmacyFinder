"""
Run all 8 national scanners in sequence.
Usage: py -3.12 scanners/scan_all.py
"""
import sys
import os
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scanners.scan_item130 import scan_item130
from scanners.scan_item131 import scan_item131
from scanners.scan_item132 import scan_item132
from scanners.scan_item133 import scan_item133
from scanners.scan_item134 import scan_item134
from scanners.scan_item134a import scan_item134a
from scanners.scan_item135 import scan_item135
from scanners.scan_item136 import scan_item136


def main():
    start = time.time()
    print("=" * 60)
    print("  PharmacyFinder — National Scanner Suite")
    print("  Running all 8 rule scanners across AU")
    print("=" * 60)
    
    scanners = [
        ("Item 130", scan_item130),
        ("Item 131", scan_item131),
        ("Item 132", scan_item132),
        ("Item 133", scan_item133),
        ("Item 134", scan_item134),
        ("Item 134A", scan_item134a),
        ("Item 135", scan_item135),
        ("Item 136", scan_item136),
    ]
    
    all_results = {}
    for name, scanner in scanners:
        print(f"\n{'#' * 60}")
        print(f"  Starting: {name}")
        print(f"{'#' * 60}")
        try:
            results = scanner()
            all_results[name] = len(results) if results else 0
        except Exception as e:
            print(f"  ERROR in {name}: {e}")
            import traceback
            traceback.print_exc()
            all_results[name] = f"ERROR: {e}"
    
    elapsed = time.time() - start
    minutes = int(elapsed // 60)
    seconds = elapsed % 60
    
    print(f"\n{'=' * 60}")
    print(f"  FINAL SUMMARY — All Scanners")
    print(f"{'=' * 60}")
    for name, count in all_results.items():
        print(f"  {name:12s}: {count}")
    print(f"{'=' * 60}")
    print(f"  Total time: {minutes}m {seconds:.1f}s")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
