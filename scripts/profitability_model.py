"""
Greenfield Pharmacy Profitability Model

Estimates profitability for each scanner opportunity based on:
- Population catchment (from census SA1 data)
- Proximity to GPs (script generation)
- Competition density
- Location type (urban/suburban/rural/remote)
- Estimated scripts/day -> revenue -> profit

Key assumptions (conservative):
- Average PBS script revenue: $15.50 (dispensing fee + markup)
- Average OTC/front-of-shop per script customer: $8
- Average scripts/day for viable pharmacy: 80-150
- GP generates ~30 scripts/day
- 30% of nearby GP scripts captured by nearest pharmacy
- Cost structure: ~65-70% of revenue (COGS 60%, wages 20-25%, rent 5-8%, other 5-7%)
- Setup cost: $350-500k
- Exit multiple: 1.5-2.5x net profit (goodwill)
"""

import json
import sqlite3
import math
import os

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'pharmacy_finder.db')

# Financial assumptions
PBS_REVENUE_PER_SCRIPT = 15.50  # avg dispensing fee + markup
OTC_PER_SCRIPT_CUSTOMER = 8.00  # front of shop per script customer
SCRIPTS_PER_GP_PER_DAY = 30  # average GP generates
CAPTURE_RATE_NEAREST = 0.30  # % of nearby GP scripts we'd capture
CAPTURE_RATE_ONLY = 0.60  # % if only pharmacy in area
TRADING_DAYS = 300  # ~6 days/week

# Cost structure (% of revenue)
COGS_RATE = 0.58  # pharmacy wholesale + PBS markup
WAGES_RATE = 0.18  # lean staffing model (partner operates)
RENT_URBAN = 70000  # annual
RENT_SUBURBAN = 50000
RENT_RURAL = 35000
RENT_REMOTE = 25000
OTHER_COSTS_RATE = 0.04  # insurance, utilities, software, consumables

# Setup costs
SETUP_URBAN = 450000
SETUP_SUBURBAN = 380000
SETUP_RURAL = 320000
SETUP_REMOTE = 280000

# Exit multiples (goodwill = multiple x net profit)
EXIT_MULTIPLE_LOW = 1.5
EXIT_MULTIPLE_HIGH = 2.5

def classify_location(nearest_pharmacy_km, state, rule_item=''):
    """Classify location type. Item 136/133/134 are typically suburban even if close to pharmacy."""
    if nearest_pharmacy_km > 50:
        return 'remote'
    elif nearest_pharmacy_km > 10:
        return 'rural'
    elif nearest_pharmacy_km > 2:
        return 'suburban'
    elif any(item in rule_item for item in ['136', '133', '134']):
        # Medical centres and shopping centres in metro areas are suburban cost, not CBD
        return 'suburban'
    else:
        return 'urban'

def estimate_scripts_per_day(opp, loc_type):
    """Estimate daily scripts based on opportunity type and location."""
    rule = opp.get('rule_item', '')
    gp_count = 0
    
    # Extract GP count from reason text
    reason = opp.get('reason', '')
    import re
    # Try multiple patterns
    gp_match = re.search(r'(\d+(?:\.\d+)?)\s*FTE\s*GPs?', reason)
    if gp_match:
        gp_count = float(gp_match.group(1))
    gp_match2 = re.search(r'GPs?[=:]\s*(\d+)', reason)
    if gp_match2 and gp_count == 0:
        gp_count = int(gp_match2.group(1))
    fte_match = re.search(r'FTE[=]\s*(\d+(?:\.\d+)?)', reason)
    if fte_match and gp_count == 0:
        gp_count = float(fte_match.group(1))
    
    nearest = opp.get('nearest_pharmacy_km', 0)
    
    if 'Item 136' in rule:
        # Medical centre - high GP density, pharmacy RIGHT NEXT to GPs
        # Being in/adjacent to the medical centre gives much higher capture
        capture = 0.45 if nearest < 0.5 else 0.40 if nearest < 1.0 else 0.35
        scripts = gp_count * SCRIPTS_PER_GP_PER_DAY * capture
        # Also add walk-in/community scripts (more GPs = more foot traffic)
        scripts += 20  # base community walk-ins
        scripts = max(scripts, 80)
        
    elif 'Item 130' in rule:
        # Supermarket pharmacy
        if nearest > 10:
            scripts = max(gp_count * SCRIPTS_PER_GP_PER_DAY * CAPTURE_RATE_ONLY, 60)
        else:
            scripts = max(gp_count * SCRIPTS_PER_GP_PER_DAY * CAPTURE_RATE_NEAREST, 50)
        # Supermarket traffic bonus
        scripts *= 1.2
        
    elif 'Item 131' in rule:
        # Rural gap - limited population
        if loc_type == 'remote':
            scripts = 30  # low volume
        elif loc_type == 'rural':
            scripts = 50
        else:
            scripts = 70
            
    elif 'Item 132' in rule:
        # One-pharmacy town upgrade
        scripts = max(gp_count * SCRIPTS_PER_GP_PER_DAY * CAPTURE_RATE_NEAREST, 40)
        
    elif 'Item 133' in rule:
        # Shopping centre - foot traffic + surrounding population
        scripts = 100  # shopping centre foot traffic baseline
        if gp_count > 0:
            scripts += gp_count * SCRIPTS_PER_GP_PER_DAY * CAPTURE_RATE_NEAREST
            
    elif 'Item 134' in rule:
        # Large shopping centre
        scripts = 120
        
    else:
        scripts = 60
    
    return round(scripts)

def calculate_profitability(opp):
    nearest = opp.get('nearest_pharmacy_km', 0)
    state = opp.get('state', '??')
    rule_item = opp.get('rule_item', '')
    loc_type = classify_location(nearest, state, rule_item)
    
    scripts_day = estimate_scripts_per_day(opp, loc_type)
    
    # Revenue
    script_revenue = scripts_day * PBS_REVENUE_PER_SCRIPT * TRADING_DAYS
    otc_revenue = scripts_day * OTC_PER_SCRIPT_CUSTOMER * TRADING_DAYS * 0.6  # 60% also buy OTC
    total_revenue = script_revenue + otc_revenue
    
    # Costs
    cogs = total_revenue * COGS_RATE
    wages = total_revenue * WAGES_RATE
    rent = {'urban': RENT_URBAN, 'suburban': RENT_SUBURBAN, 'rural': RENT_RURAL, 'remote': RENT_REMOTE}[loc_type]
    other = total_revenue * OTHER_COSTS_RATE
    total_costs = cogs + wages + rent + other
    
    net_profit = total_revenue - total_costs
    margin = net_profit / total_revenue * 100 if total_revenue > 0 else 0
    
    setup = {'urban': SETUP_URBAN, 'suburban': SETUP_SUBURBAN, 'rural': SETUP_RURAL, 'remote': SETUP_REMOTE}[loc_type]
    
    # Pharmacy sale value = revenue multiple (typically 0.8-1.2x annual revenue for established)
    # Greenfield after 12-24 months typically sells at 0.6-0.9x revenue 
    sale_value_low = total_revenue * 0.6
    sale_value_high = total_revenue * 0.9
    
    # Flip profit = sale value + profit during hold period (18 months) - setup
    hold_months = 18
    hold_profit = net_profit * (hold_months / 12)
    flip_profit_low = sale_value_low + hold_profit - setup
    flip_profit_high = sale_value_high + hold_profit - setup
    
    roi_year1 = net_profit / setup * 100 if setup > 0 else 0
    payback_months = setup / (net_profit / 12) if net_profit > 0 else 999
    
    return {
        'location_type': loc_type,
        'scripts_day': scripts_day,
        'revenue': round(total_revenue),
        'costs': round(total_costs),
        'net_profit': round(net_profit),
        'margin_pct': round(margin, 1),
        'setup_cost': setup,
        'sale_value_low': round(sale_value_low),
        'sale_value_high': round(sale_value_high),
        'roi_year1_pct': round(roi_year1, 1),
        'payback_months': round(payback_months),
        'rent': rent,
        'flip_profit_low': round(flip_profit_low),
        'flip_profit_high': round(flip_profit_high),
        'hold_months': hold_months,
    }

def main():
    # Process all scanner outputs
    output_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'output')
    
    all_results = []
    
    for fname in ['item130_opportunities.json', 'item131_opportunities.json', 
                   'item132_opportunities.json', 'item133_opportunities.json',
                   'item136_opportunities.json']:
        fpath = os.path.join(output_dir, fname)
        if not os.path.exists(fpath):
            continue
        with open(fpath, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        for opp in data:
            p = calculate_profitability(opp)
            opp['profitability'] = p
            all_results.append(opp)
    
    # Sort by flip profit (high estimate)
    all_results.sort(key=lambda x: x['profitability']['flip_profit_high'], reverse=True)
    
    # Print top opportunities
    print("=" * 100)
    print("  GREENFIELD PHARMACY PROFITABILITY RANKINGS")
    print("  (Conservative estimates — actual results vary)")
    print("=" * 100)
    print()
    
    # Top 30 by flip profit
    print(f"{'#':>3} {'Rule':>8} {'State':>4} {'Name':<40} {'Scr/d':>5} {'Revenue':>10} {'Profit/yr':>10} {'Margin':>6} {'Setup':>8} {'18mo Flip':>10} {'ROI%':>6} {'Payback':>8}")
    print("-" * 160)
    
    shown = 0
    for opp in all_results:
        if shown >= 50:
            break
        p = opp['profitability']
        if p['net_profit'] <= 0:
            continue
        
        name = opp.get('name', 'Unknown')[:38]
        rule = opp.get('rule_item', '??').replace('Item ', '')
        state = opp.get('state', '??')[:3]
        
        flip_avg = (p['flip_profit_low'] + p['flip_profit_high']) // 2
        payback = f"{p['payback_months']}mo" if p['payback_months'] < 100 else "N/A"
        print(f"{shown+1:>3} {rule:>8} {state:>4} {name:<40} {p['scripts_day']:>5} ${p['revenue']:>9,} ${p['net_profit']:>9,} {p['margin_pct']:>5.1f}% ${p['setup_cost']:>7,} ${flip_avg:>+9,} {p['roi_year1_pct']:>5.1f}% {payback:>8}")
        shown += 1
    
    print()
    print("=" * 100)
    print(f"  Total profitable opportunities: {shown}")
    print()
    
    # Summary by rule
    print("  SUMMARY BY RULE ITEM")
    print("-" * 60)
    rules = {}
    for opp in all_results:
        rule = opp.get('rule_item', '??')
        p = opp['profitability']
        if rule not in rules:
            rules[rule] = {'count': 0, 'total_profit': 0, 'profitable': 0}
        rules[rule]['count'] += 1
        rules[rule]['total_profit'] += p['net_profit']
        if p['net_profit'] > 0:
            rules[rule]['profitable'] += 1
    
    for rule, stats in sorted(rules.items()):
        avg = stats['total_profit'] / stats['count'] if stats['count'] > 0 else 0
        print(f"  {rule:<12} {stats['count']:>5} sites | {stats['profitable']:>5} profitable | Avg profit: ${avg:>9,.0f}")
    
    # Save enriched results
    outpath = os.path.join(output_dir, 'profitability_rankings.json')
    with open(outpath, 'w', encoding='utf-8') as f:
        json.dump(all_results[:50], f, indent=2, ensure_ascii=False)
    print(f"\n  Top 50 saved to: {outpath}")

if __name__ == '__main__':
    main()
