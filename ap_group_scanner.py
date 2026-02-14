#!/usr/bin/env python3
"""
AP Group Scanner — Cross-reference pharmacies for sale with opportunity zones.

Scrapes AP Group listings (pharmacies currently for sale) and cross-references
them with our opportunity zones. A pharmacy for sale in/near a high-opportunity
area is an actionable acquisition target.
"""

import csv
import math
import os
import re
import time

OUTPUT_DIR = 'output'
STATES = ['ACT', 'NSW', 'NT', 'QLD', 'SA', 'TAS', 'VIC', 'WA']

# AP Group listings scraped from https://www.apgroup.com.au/buying/pharmacies-for-sale/
# These use vague regional descriptions (not exact addresses) to protect confidentiality.
# We map them to approximate lat/lng for distance matching.
AP_GROUP_LISTINGS = [
    # For Sale (active)
    {'region': 'Mid West Region WA', 'state': 'WA', 'price': '$750,000', 'status': 'For Sale',
     'description': 'Enjoy the benefits of regional lifestyle while reaping the rewards of a fulfilling professional venture.',
     'lat': -28.77, 'lng': 114.62},  # Geraldton area
    {'region': 'Greater Launceston Region', 'state': 'TAS', 'price': '$1,315,000', 'status': 'For Sale',
     'description': 'Highly profitable with future growth opportunities, low opening hours (30 hours per week), very low expenses.',
     'lat': -41.44, 'lng': 147.14},  # Launceston area
    {'region': 'Central NSW', 'state': 'NSW', 'price': '$4,700,000', 'status': 'For Sale',
     'description': 'The ideal regional pharmacy. A highly profitable low risk investment in a desirable lifestyle location.',
     'lat': -32.24, 'lng': 148.60},  # Central NSW (Dubbo/Orange area)
    {'region': 'North Eastern Suburbs Adelaide', 'state': 'SA', 'price': 'Contact Agent', 'status': 'For Sale',
     'description': 'Established pharmacy business with opportunity to relocate into new health precinct in a north eastern growth corridor.',
     'lat': -34.83, 'lng': 138.70},  # NE Adelaide
    {'region': 'North West QLD', 'state': 'QLD', 'price': 'Offers Over $600K', 'status': 'For Sale',
     'description': 'Community-Based pharmacy with lifestyle benefits and steady year-round demand.',
     'lat': -20.73, 'lng': 139.50},  # Mt Isa area
    {'region': 'Melbourne CBD', 'state': 'VIC', 'price': '$2,485,000', 'status': 'For Sale',
     'description': 'Busy & profitable pharmacy & medical centre located in the middle of Melbourne CBD.',
     'lat': -37.81, 'lng': 144.96},  # Melbourne CBD
    {'region': 'South-Eastern WA', 'state': 'WA', 'price': '$2,100,000', 'status': 'For Sale',
     'description': 'Profitable single pharmacy town businesses with protected catchment area.',
     'lat': -33.86, 'lng': 121.89},  # Esperance/Kalgoorlie area
    {'region': 'Darwin CBD', 'state': 'NT', 'price': '$550,000', 'status': 'For Sale',
     'description': 'Established Community Pharmacy Business with significant growth potential.',
     'lat': -12.46, 'lng': 130.84},  # Darwin

    # Under Offer (still relevant for market intelligence)
    {'region': 'Regional City South-Western Victoria', 'state': 'VIC', 'price': '$3,500,000', 'status': 'Under Offer',
     'description': 'Under offer since 24/12/2025', 'lat': -38.14, 'lng': 142.16},  # Warrnambool area
    {'region': 'Inner Southern Suburb Brisbane', 'state': 'QLD', 'price': 'Offers Above $1.9M', 'status': 'Under Offer',
     'description': 'Under offer since 23/12/2025', 'lat': -27.52, 'lng': 153.02},
    {'region': 'South Eastern Suburbs Perth', 'state': 'WA', 'price': '$5,700,000', 'status': 'Under Offer',
     'description': 'Under offer since 16/12/2025', 'lat': -32.05, 'lng': 115.95},
    {'region': 'Outer Western Suburbs Melbourne', 'state': 'VIC', 'price': 'Offers Above $2.210M', 'status': 'Under Offer',
     'description': 'Under offer since 12/12/2025', 'lat': -37.74, 'lng': 144.68},
    {'region': 'Northern Rivers Region NSW', 'state': 'NSW', 'price': 'Offers Above $2.5M', 'status': 'Under Offer',
     'description': 'Under offer since 12/12/2025', 'lat': -28.81, 'lng': 153.28},
    {'region': 'Western NSW', 'state': 'NSW', 'price': '$1,500,000', 'status': 'Under Offer',
     'description': 'Under offer since 09/12/2025', 'lat': -31.95, 'lng': 148.17},
    {'region': 'Major Regional City VIC', 'state': 'VIC', 'price': '$1,950,000', 'status': 'Under Offer',
     'description': 'Under offer since 04/12/2025', 'lat': -36.76, 'lng': 144.28},  # Bendigo area
    {'region': 'Surf Coast Region Victoria', 'state': 'VIC', 'price': 'Offers Above $3.6M', 'status': 'Under Offer',
     'description': 'Under offer since 03/12/2025', 'lat': -38.34, 'lng': 144.07},  # Torquay area
    {'region': 'Regional City QLD', 'state': 'QLD', 'price': '$1,350,000', 'status': 'Under Offer',
     'description': 'Under offer since 14/11/2025', 'lat': -23.38, 'lng': 150.51},  # Rockhampton area
    {'region': 'Northern Suburbs Melbourne', 'state': 'VIC', 'price': '$2,999,000', 'status': 'Under Offer',
     'description': 'Under offer since 14/11/2025', 'lat': -37.65, 'lng': 145.01},
    {'region': 'South East Brisbane', 'state': 'QLD', 'price': 'Offers Over $1.55M', 'status': 'Under Offer',
     'description': 'Under offer since 22/10/2025', 'lat': -27.60, 'lng': 153.14},
    {'region': 'Mid-North Coast NSW', 'state': 'NSW', 'price': '$650,000', 'status': 'Under Offer',
     'description': 'Under offer since 22/10/2025', 'lat': -31.43, 'lng': 152.91},
    {'region': 'Melbourne South-Eastern Suburbs', 'state': 'VIC', 'price': '$1,050,000', 'status': 'Under Offer',
     'description': 'Under offer since 20/10/2025', 'lat': -37.93, 'lng': 145.18},
    {'region': 'Mackay Region QLD', 'state': 'QLD', 'price': '$550,000', 'status': 'Under Offer',
     'description': 'Under offer since 17/10/2025', 'lat': -21.14, 'lng': 149.19},
    {'region': 'Southern Suburbs of Adelaide', 'state': 'SA', 'price': '$1,150,000', 'status': 'Under Offer',
     'description': 'Under offer since 15/10/2025', 'lat': -35.05, 'lng': 138.55},
    {'region': 'Sydney Northern Beaches', 'state': 'NSW', 'price': 'Offers Over $2.6M', 'status': 'Under Offer',
     'description': 'Under offer since 09/10/2025', 'lat': -33.71, 'lng': 151.29},
    {'region': 'Eastern Wheatbelt Region of WA', 'state': 'WA', 'price': '$1,200,000', 'status': 'Under Offer',
     'description': 'Under offer since 02/10/2025', 'lat': -31.89, 'lng': 118.20},
    {'region': 'Western Suburbs Melbourne', 'state': 'VIC', 'price': '$900,000', 'status': 'Under Offer',
     'description': 'Under offer since 22/09/2025', 'lat': -37.78, 'lng': 144.83},
    {'region': 'Ipswich QLD', 'state': 'QLD', 'price': 'Offers Above $1.75M', 'status': 'Under Offer',
     'description': 'Under offer since 15/09/2025', 'lat': -27.62, 'lng': 152.76},
    {'region': 'Toowoomba', 'state': 'QLD', 'price': '$950,000', 'status': 'Under Offer',
     'description': 'Under offer since 11/09/2025', 'lat': -27.56, 'lng': 151.95},
    {'region': 'Sydney South West', 'state': 'NSW', 'price': 'Offers Above $2.2M', 'status': 'Under Offer',
     'description': 'Under offer since 03/09/2025', 'lat': -33.92, 'lng': 150.88},
    {'region': 'Greater Hobart Area', 'state': 'TAS', 'price': '$1,000,000', 'status': 'Under Offer',
     'description': 'Under offer since 18/08/2025', 'lat': -42.88, 'lng': 147.33},
    {'region': 'Western Suburbs Sydney', 'state': 'NSW', 'price': '$1,500,000', 'status': 'Under Offer',
     'description': 'Under offer since 11/08/2025', 'lat': -33.80, 'lng': 150.97},
    {'region': 'Eastern Suburbs Melbourne', 'state': 'VIC', 'price': 'Offers Above $3.6M', 'status': 'Under Offer',
     'description': 'Under offer since 05/08/2025', 'lat': -37.83, 'lng': 145.15},
    {'region': 'Hunter Valley Region NSW', 'state': 'NSW', 'price': '$1,750,000', 'status': 'Under Offer',
     'description': 'Under offer since 28/07/2025', 'lat': -32.74, 'lng': 151.55},
    {'region': 'Western Downs Region QLD', 'state': 'QLD', 'price': 'Offers Over $1.2M', 'status': 'Under Offer',
     'description': 'Under offer since 06/07/2025', 'lat': -26.73, 'lng': 150.76},
    {'region': 'Gippsland VIC', 'state': 'VIC', 'price': '$5,860,000', 'status': 'Under Offer',
     'description': 'Under offer since 21/04/2025', 'lat': -38.17, 'lng': 146.03},
]


def haversine_km(lat1, lon1, lat2, lon2):
    """Calculate the great-circle distance between two points in km."""
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def load_all_opportunities():
    """Load all opportunity zone data."""
    all_opps = []
    for state in STATES:
        for prefix in ['population_ranked_', 'verified_opportunities_', 'opportunity_zones_']:
            path = os.path.join(OUTPUT_DIR, f'{prefix}{state}.csv')
            if os.path.exists(path):
                break
        else:
            continue

        with open(path, encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    opp = {
                        'lat': float(row.get('Latitude', 0) or 0),
                        'lng': float(row.get('Longitude', 0) or 0),
                        'state': state,
                        'poiName': row.get('POI Name', ''),
                        'nearTown': row.get('Nearest Town', ''),
                        'rules': row.get('Qualifying Rules', ''),
                        'score': float(row.get('Opportunity Score', 0) or 0),
                        'pop5': int(row.get('Pop 5km', 0) or 0),
                        'nearPharmKm': float(row.get('Nearest Pharmacy (km)', 0) or 0),
                        'verification': row.get('Verification', ''),
                        'compScore': float(row.get('Competition Score', 0) or 0),
                    }
                    if opp['lat'] != 0 and opp['lng'] != 0:
                        all_opps.append(opp)
                except (ValueError, TypeError):
                    pass
    return all_opps


def parse_price(price_str):
    """Extract numeric price from price string."""
    if not price_str:
        return None
    # Remove $ and commas, extract number
    nums = re.findall(r'[\d,]+(?:\.\d+)?', price_str.replace(',', ''))
    if nums:
        try:
            val = float(nums[0])
            # Handle "Offers Above $2.2M" style
            if 'M' in price_str.upper() or 'm' in price_str:
                if val < 100:  # It's in millions
                    val *= 1_000_000
            return val
        except ValueError:
            pass
    return None


def main():
    print("=" * 60)
    print("AP Group Scanner -- Pharmacy Listings Cross-Reference")
    print("=" * 60)

    # Load opportunities
    opps = load_all_opportunities()
    print(f"\n  Loaded {len(opps)} opportunity zones")
    print(f"  Loaded {len(AP_GROUP_LISTINGS)} AP Group listings")

    # For each AP Group listing, find nearby opportunities
    MATCH_RADIUS_KM = 50  # Regional descriptions are vague, use wide radius
    matches = []

    for listing in AP_GROUP_LISTINGS:
        nearby_opps = []
        for opp in opps:
            dist = haversine_km(listing['lat'], listing['lng'], opp['lat'], opp['lng'])
            if dist <= MATCH_RADIUS_KM:
                nearby_opps.append((opp, dist))

        # Sort by opportunity score (best first)
        nearby_opps.sort(key=lambda x: x[0]['score'], reverse=True)

        if nearby_opps:
            best = nearby_opps[0]
            match = {
                'AP Region': listing['region'],
                'AP State': listing['state'],
                'AP Price': listing['price'],
                'AP Status': listing['status'],
                'AP Description': listing['description'],
                'Nearby Opportunities': len(nearby_opps),
                'Best Opp Location': best[0]['poiName'] or best[0]['nearTown'],
                'Best Opp Score': best[0]['score'],
                'Best Opp Pop 5km': best[0]['pop5'],
                'Best Opp Pharm Dist': best[0]['nearPharmKm'],
                'Best Opp Rules': best[0]['rules'],
                'Best Opp Competition Score': best[0]['compScore'],
                'Distance to Best Opp (km)': round(best[1], 1),
                'Best Opp Verification': best[0]['verification'],
                'Top 3 Opportunities': '; '.join(
                    f"{o['poiName'] or o['nearTown']} (score={o['score']:.0f}, {d:.0f}km)"
                    for o, d in nearby_opps[:3]
                ),
                'Match Quality': 'GOLD' if best[0]['score'] > 100000 and best[0]['compScore'] >= 60 else
                                 'SILVER' if best[0]['score'] > 50000 or best[0]['compScore'] >= 50 else
                                 'BRONZE',
            }
            matches.append(match)
            quality = match['Match Quality']
            emoji = '[!!!]' if quality == 'GOLD' else '[!!]' if quality == 'SILVER' else '[!]'
            print(f"\n  {emoji} {listing['region']} ({listing['state']}) - {listing['price']} [{listing['status']}]")
            print(f"      {len(nearby_opps)} nearby opportunities | Best: {match['Best Opp Location']} (score={best[0]['score']:.0f})")
            print(f"      Competition Score: {best[0]['compScore']} | Distance: {best[1]:.1f}km | Quality: {quality}")
        else:
            # Still include in output even without matches
            match = {
                'AP Region': listing['region'],
                'AP State': listing['state'],
                'AP Price': listing['price'],
                'AP Status': listing['status'],
                'AP Description': listing['description'],
                'Nearby Opportunities': 0,
                'Best Opp Location': 'None within 50km',
                'Best Opp Score': 0,
                'Best Opp Pop 5km': 0,
                'Best Opp Pharm Dist': 0,
                'Best Opp Rules': '',
                'Best Opp Competition Score': 0,
                'Distance to Best Opp (km)': 0,
                'Best Opp Verification': '',
                'Top 3 Opportunities': '',
                'Match Quality': 'NONE',
            }
            matches.append(match)
            print(f"\n  [ ] {listing['region']} ({listing['state']}) - {listing['price']} [{listing['status']}]")
            print(f"      No opportunity zones within {MATCH_RADIUS_KM}km radius")

    # Write output CSV
    out_path = os.path.join(OUTPUT_DIR, 'ap_group_matches.csv')
    fieldnames = [
        'AP Region', 'AP State', 'AP Price', 'AP Status', 'AP Description',
        'Nearby Opportunities', 'Best Opp Location', 'Best Opp Score',
        'Best Opp Pop 5km', 'Best Opp Pharm Dist', 'Best Opp Rules',
        'Best Opp Competition Score', 'Distance to Best Opp (km)',
        'Best Opp Verification', 'Top 3 Opportunities', 'Match Quality',
    ]

    with open(out_path, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        # Sort: GOLD first, then SILVER, then BRONZE, then NONE
        quality_order = {'GOLD': 0, 'SILVER': 1, 'BRONZE': 2, 'NONE': 3}
        matches.sort(key=lambda m: (quality_order.get(m['Match Quality'], 3), -m['Best Opp Score']))
        writer.writerows(matches)

    # Summary
    gold = sum(1 for m in matches if m['Match Quality'] == 'GOLD')
    silver = sum(1 for m in matches if m['Match Quality'] == 'SILVER')
    bronze = sum(1 for m in matches if m['Match Quality'] == 'BRONZE')
    active = sum(1 for m in matches if m.get('AP Status') == 'For Sale' and m['Match Quality'] != 'NONE')

    print(f"\n{'=' * 60}")
    print(f"AP Group Cross-Reference Complete!")
    print(f"  Total listings: {len(AP_GROUP_LISTINGS)}")
    print(f"  Matches: {gold} GOLD | {silver} SILVER | {bronze} BRONZE")
    print(f"  Active (For Sale) with opportunities: {active}")
    print(f"  Output: {out_path}")
    print(f"{'=' * 60}")


if __name__ == '__main__':
    main()
