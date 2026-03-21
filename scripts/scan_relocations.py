"""
Relocation Opportunity Scanner
==============================
Scans all states for pharmacy relocation opportunities under Items 122, 124, 125.
Outputs ranked deals to output/relocation_opportunities.json and .csv.

Usage: py -3.12 scripts/scan_relocations.py
"""
import sys, os, json, csv
from datetime import datetime

# Add project root to path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from engine.context import EvaluationContext
from engine.rules.relocations import scan_relocation_opportunities


def main():
    print("=" * 70)
    print("RELOCATION OPPORTUNITY SCANNER")
    print(f"Run: {datetime.now().isoformat()}")
    print("=" * 70)

    # Load context (all data from pharmacy_finder.db)
    db_path = os.path.join(PROJECT_ROOT, "pharmacy_finder.db")
    print(f"\nLoading data from {db_path}...")
    context = EvaluationContext(db_path=db_path)

    # Scan all states
    states = ["NSW", "VIC", "QLD", "SA", "WA", "TAS", "NT", "ACT"]
    all_opportunities = []

    for state in states:
        print(f"\nScanning {state}...")
        opps = scan_relocation_opportunities(context, state_filter=state)
        print(f"  Found {len(opps)} opportunities")
        for opp in opps:
            opp_dict = opp.to_dict()
            opp_dict["state"] = state
            all_opportunities.append(opp_dict)

    # Sort all by deal_score descending
    all_opportunities.sort(key=lambda x: x["deal_score"], reverse=True)

    print(f"\n{'=' * 70}")
    print(f"TOTAL OPPORTUNITIES: {len(all_opportunities)}")
    print(f"{'=' * 70}")

    # Print top 30
    print(f"\nTOP 30 RELOCATION DEALS:")
    print(f"{'-' * 70}")
    for i, opp in enumerate(all_opportunities[:30], 1):
        pharm = opp["existing_pharmacy"]
        proposed = opp["proposed_location"]
        dist = opp["distances"]

        print(f"\n#{i} — Score: {opp['deal_score']:.3f} | {opp['item']} | State: {opp['state']}")
        print(f"  Pharmacy: {pharm['name']}")
        print(f"  Address:  {pharm['address']}")
        if proposed:
            print(f"  Target:   {proposed['name']}")
        print(f"  Distance: {dist.get('pharmacy_to_centre_m', 'N/A')}m to centre")
        if dist.get("centre_tenants"):
            print(f"  Centre:   {dist.get('centre_tenants', '?')} tenants, "
                  f"{dist.get('centre_gla_sqm', '?')} sqm GLA")
        pharmas_in = dist.get("existing_pharmacies_in_centre", "?")
        print(f"  Pharmacies already in centre: {pharmas_in}")

        # Build "why" summary
        why_parts = []
        if pharmas_in == 0:
            why_parts.append(f"centre has NO pharmacy")
        if dist.get("pharmacy_to_centre_m") and dist["pharmacy_to_centre_m"] < 500:
            why_parts.append(f"pharmacy only {dist['pharmacy_to_centre_m']:.0f}m away")
        if dist.get("centre_tenants", 0) >= 100:
            why_parts.append(f"large centre ({dist['centre_tenants']} tenants)")
        if why_parts:
            print(f"  Why:      {'; '.join(why_parts)}")

        # Key reasons
        for reason in opp["reasons"][:2]:
            print(f"  > {reason}")

    # Save JSON
    output_dir = os.path.join(PROJECT_ROOT, "output")
    os.makedirs(output_dir, exist_ok=True)

    json_path = os.path.join(output_dir, "relocation_opportunities.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump({
            "scan_date": datetime.now().isoformat(),
            "total_opportunities": len(all_opportunities),
            "opportunities": all_opportunities,
        }, f, indent=2, ensure_ascii=False)
    print(f"\n[OK] Saved {len(all_opportunities)} opportunities to {json_path}")

    # Save CSV
    csv_path = os.path.join(output_dir, "relocation_opportunities.csv")
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "Rank", "Deal Score", "Item", "State",
            "Pharmacy Name", "Pharmacy Address",
            "Target Centre", "Distance (m)", "Centre Tenants", "Centre GLA (sqm)",
            "Existing Pharmacies in Centre", "Key Reason", "Why Opportunity"
        ])
        for i, opp in enumerate(all_opportunities, 1):
            pharm = opp["existing_pharmacy"]
            proposed = opp["proposed_location"]
            dist = opp["distances"]

            # Build why summary
            why_parts = []
            pharmas_in = dist.get("existing_pharmacies_in_centre", "?")
            if pharmas_in == 0:
                why_parts.append("centre has NO pharmacy")
            if dist.get("centre_tenants", 0) >= 100:
                why_parts.append(f"large centre ({dist['centre_tenants']} tenants)")
            if dist.get("pharmacy_to_centre_m") and dist["pharmacy_to_centre_m"] < 800:
                why_parts.append(f"pharmacy {dist['pharmacy_to_centre_m']:.0f}m from centre")

            writer.writerow([
                i,
                opp["deal_score"],
                opp["item"],
                opp["state"],
                pharm.get("name", ""),
                pharm.get("address", ""),
                proposed.get("name", "") if proposed else "",
                dist.get("pharmacy_to_centre_m", ""),
                dist.get("centre_tenants", ""),
                dist.get("centre_gla_sqm", ""),
                pharmas_in,
                opp["reasons"][0] if opp["reasons"] else "",
                "; ".join(why_parts),
            ])
    print(f"[OK] Saved CSV to {csv_path}")

    # Summary by item type
    print(f"\n{'=' * 70}")
    print("SUMMARY BY ITEM TYPE:")
    from collections import Counter
    item_counts = Counter(o["item"] for o in all_opportunities)
    for item, count in item_counts.most_common():
        avg_score = sum(o["deal_score"] for o in all_opportunities if o["item"] == item) / count
        print(f"  {item}: {count} opportunities (avg score: {avg_score:.3f})")

    # Summary by state
    print("\nSUMMARY BY STATE:")
    state_counts = Counter(o["state"] for o in all_opportunities)
    for state, count in state_counts.most_common():
        avg_score = sum(o["deal_score"] for o in all_opportunities if o["state"] == state) / count
        print(f"  {state}: {count} opportunities (avg score: {avg_score:.3f})")

    # Top deals with no pharmacy in centre (prime opportunities)
    prime = [o for o in all_opportunities if o["distances"].get("existing_pharmacies_in_centre") == 0]
    print(f"\nPRIME OPPORTUNITIES (centre with NO pharmacy): {len(prime)}")
    for i, opp in enumerate(prime[:10], 1):
        pharm = opp["existing_pharmacy"]
        proposed = opp["proposed_location"]
        dist = opp["distances"]
        print(f"  #{i} [{opp['deal_score']:.3f}] {pharm['name']} -> {proposed['name'] if proposed else 'N/A'} "
              f"({dist.get('pharmacy_to_centre_m', '?')}m, {dist.get('centre_tenants', '?')} tenants) [{opp['state']}]")


if __name__ == "__main__":
    main()
