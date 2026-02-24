"""Check remaining VERIFIED PASS opps that aren't remote NT - see which need manual Google Maps check"""
import json

with open(r'C:\Users\MJ\Documents\GitHub\PharmacyFinder\output\pass_verification.json', encoding='utf-8') as f:
    results = json.load(f)

# Find verified non-remote that might need checking
need_check = []
for r in results:
    if r['status'] == 'VERIFIED':
        km = r.get('nearest_pharmacy_km', 0)
        if km and km < 15:  # Not obviously remote
            need_check.append(r)

print(f"VERIFIED but nearest pharmacy <15km ({len(need_check)} entries):")
for r in sorted(need_check, key=lambda x: x.get('nearest_pharmacy_km', 999)):
    print(f"  ID={r['id']} | {r['name']} ({r['state']}) | nearest={r.get('nearest_pharmacy_km',0):.1f}km | type={r.get('poi_type','')} | notes={r.get('notes','')[:80]}")

# Also show score distribution for VERIFIED
print(f"\n--- All VERIFIED by score ---")
verified = [r for r in results if r['status'] == 'VERIFIED']
scores = {}
for r in verified:
    s = r.get('score', 0)
    scores[s] = scores.get(s, 0) + 1
for s, c in sorted(scores.items(), reverse=True):
    print(f"  Score {s}: {c}")
