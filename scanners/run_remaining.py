"""Run Items 132-136 (skip 130/131 which are already done)."""
import sys, os, time
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scanners.scan_item132 import scan_item132
from scanners.scan_item133 import scan_item133
from scanners.scan_item134 import scan_item134
from scanners.scan_item134a import scan_item134a
from scanners.scan_item135 import scan_item135
from scanners.scan_item136 import scan_item136

def main():
    start = time.time()
    print("=" * 60)
    print("  Running Items 132-136 (130/131 already complete)")
    print("=" * 60)
    
    scanners = [
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
        sys.stdout.flush()
        try:
            results = scanner()
            all_results[name] = len(results) if results else 0
        except Exception as e:
            print(f"  ERROR in {name}: {e}")
            import traceback
            traceback.print_exc()
            all_results[name] = f"ERROR: {e}"
    
    elapsed = time.time() - start
    print(f"\n{'=' * 60}")
    print(f"  FINAL SUMMARY")
    print(f"{'=' * 60}")
    print(f"  Item 130:  11 (previous run)")
    print(f"  Item 131:  3083 (previous run)")
    for name, count in all_results.items():
        print(f"  {name:12s}: {count}")
    print(f"{'=' * 60}")
    print(f"  Time: {int(elapsed//60)}m {elapsed%60:.1f}s")

if __name__ == "__main__":
    main()
