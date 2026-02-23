#!/usr/bin/env python3
"""
PharmacyFinder Financial Model v2 — Greenfield Business Cases

Builds detailed financial models for the 8 verified greenfield
pharmacy opportunities from scored_v2.json.

For each opportunity:
  - Setup costs (fit-out, stock, PBS approval, working capital)
  - Revenue projections (scripts/day, front-of-shop, clinical services)
  - Profitability (gross margin, EBITDA, break-even timeline)
  - Exit value (goodwill multiples, ROI at 12/24/36 months)
  - Risk assessment (competition, location, regulatory, operational)

Outputs:
  - output/financial_models_v2.json    (full models)
  - output/financial_comparison.md     (comparison report)
  - Console summary

Usage:
    python financial_model_v2.py
"""

import json
import os
import math
from datetime import datetime
from typing import Dict, List, Tuple

# ================================================================
# PATHS
# ================================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR, 'output')
SCORED_PATH = os.path.join(OUTPUT_DIR, 'scored_v2.json')

# ================================================================
# LOCATION RESEARCH DATA (compiled from Wikipedia, ABS, local knowledge)
# ================================================================
# Each entry keyed by opportunity name
LOCATION_DATA = {
    "IGA": {
        # Evidence says "town: CYGNET" — lat/lng actually in Cygnet, TAS (not Yulara NT)
        # The address says Yulara but coords are Cygnet. Model as Cygnet, TAS.
        # Cygnet: small town ~1,700 pop in Huon Valley, 50km SW of Hobart
        "actual_location": "Cygnet, Tasmania (coords mismatch — scored as NT)",
        "description": "Small heritage town in Huon Valley, ~50km south of Hobart. "
                       "Tourism + agriculture economy. Only 1 existing pharmacy (Cygnet Pharmacy). "
                       "IGA supermarket anchors the small town centre.",
        "area_type": "rural_town",
        "population_local": 1700,
        "population_catchment": 5000,  # Huon Valley catchment
        "growth_outlook": "stable",  # Tourism growing slowly
        "median_income": 42000,  # Lower income area
        "commercial_rent_sqm_pa": 180,  # Regional TAS very low
        "pharmacy_size_sqm": 100,
        "area_thriving": True,  # Tourism + tree-changers keeping it alive
        "gp_count": 0,  # Item 132 (town-based, not medical centre)
        "nearby_amenities": "IGA supermarket, post office, cafes, Huon Valley Hub",
        "pharmacist_demand": "low",  # Remote, hard to recruit
        "competition_notes": "Only Cygnet Pharmacy at 219m. Next is Huonville ~15km.",
        "demographic_notes": "Aging population, retirees, tourism workers. High PBS demand per capita.",
    },
    "Murdoch Medical Centre": {
        "actual_location": "Murdoch, Perth WA",
        "description": "Major medical hub in southern Perth. Home to Fiona Stanley Hospital, "
                       "St John of God Murdoch, Murdoch University. High-traffic medical precinct.",
        "area_type": "metro_medical_hub",
        "population_local": 3352,
        "population_catchment": 52417,  # ratio figure = effective catchment per pharmacy
        "growth_outlook": "strong",  # Major infrastructure, train line
        "median_income": 72000,
        "commercial_rent_sqm_pa": 550,  # Premium medical precinct
        "pharmacy_size_sqm": 120,
        "area_thriving": True,
        "gp_count": 10,
        "nearby_amenities": "Fiona Stanley Hospital, St John of God, Murdoch University, "
                           "Murdoch Station, Murdoch Drive retail strip",
        "pharmacist_demand": "high",  # Lots of hospital + retail pharmacy jobs
        "competition_notes": "Nearest pharmacy 694m (Good Price Pharmacy Warehouse Baldivis — "
                            "but this seems wrong, Baldivis is 20km away). 33 pharmacies in 10km.",
        "demographic_notes": "University students, hospital workers, affluent surrounding suburbs "
                            "(Bateman, Winthrop, Bull Creek).",
    },
    "GP Super Clinic Wynnum": {
        "actual_location": "Wynnum, Brisbane QLD",
        "description": "Coastal suburb 20km east of Brisbane CBD. Popular residential area "
                       "on Moreton Bay. GP Super Clinic at 95 Edith Street.",
        "area_type": "metro_suburban",
        "population_local": 14036,
        "population_catchment": 44764,
        "growth_outlook": "moderate",  # Established suburb, infill development
        "median_income": 62000,
        "commercial_rent_sqm_pa": 420,
        "pharmacy_size_sqm": 110,
        "area_thriving": True,
        "gp_count": 8,
        "nearby_amenities": "Wynnum Central shopping strip, Wynnum Plaza, train station, "
                           "foreshore/jetty, schools",
        "pharmacist_demand": "moderate",
        "competition_notes": "Nearest: Wynnum Day & Night Chempro 731m. 39 pharmacies in 10km.",
        "demographic_notes": "Families, older residents, bayside lifestyle. Growing cafe culture.",
    },
    "Sunshine Hospital Medical Centre": {
        "actual_location": "St Albans, Melbourne VIC",
        "description": "Western Melbourne suburb, 17km NW of CBD. Major growth corridor. "
                       "Sunshine Hospital is a major public hospital. Medical centre has 15 GPs.",
        "area_type": "metro_growth",
        "population_local": 38042,
        "population_catchment": 36161,
        "growth_outlook": "very_strong",  # One of Melbourne's fastest growth corridors
        "median_income": 48000,  # Lower income, high cultural diversity
        "commercial_rent_sqm_pa": 380,
        "pharmacy_size_sqm": 120,
        "area_thriving": True,
        "gp_count": 15,
        "nearby_amenities": "Sunshine Hospital, train station, Alfrieda St shopping strip, "
                           "Victoria University",
        "pharmacist_demand": "high",
        "competition_notes": "Nearest pharmacy 1.55km (Happy Pharmacy). 101 pharmacies in 10km "
                            "but this is a huge metro pop area.",
        "demographic_notes": "High multicultural population, growing families, new housing estates. "
                            "Lower socioeconomic — high PBS concession card usage.",
    },
    "Canning Vale Medical Centre": {
        "actual_location": "Canning Vale, Perth WA",
        "description": "Large southern Perth suburb, 22km from CBD. Major residential growth "
                       "area with young families. Medical centre at 290 Ranford Road.",
        "area_type": "metro_suburban",
        "population_local": 34504,
        "population_catchment": 19945,
        "growth_outlook": "strong",  # Continued residential growth
        "median_income": 68000,
        "commercial_rent_sqm_pa": 450,
        "pharmacy_size_sqm": 110,
        "area_thriving": True,
        "gp_count": 10,
        "nearby_amenities": "Livingston Marketplace, Ranford Road shopping, Nicholson Road "
                           "commercial, Canning Vale College",
        "pharmacist_demand": "moderate",
        "competition_notes": "Nearest: Wizard Pharmacy Livingston 366m. 76 pharmacies in 10km. "
                            "Very competitive market.",
        "demographic_notes": "Young families, multicultural, middle income. Growing rapidly.",
    },
    "Supa IGA Gordonvale": {
        "actual_location": "Gordonvale, Cairns QLD",
        "description": "Rural sugar town 23km south of Cairns. Population ~7,000. "
                       "Supa IGA is the main supermarket. Only 2 pharmacies in 10km.",
        "area_type": "regional_town",
        "population_local": 6944,
        "population_catchment": 18020,
        "growth_outlook": "moderate",  # Cairns southern corridor growth
        "median_income": 52000,
        "commercial_rent_sqm_pa": 250,
        "pharmacy_size_sqm": 100,
        "area_thriving": True,  # Growing with Cairns sprawl
        "gp_count": 0,  # Item 132 (town-based)
        "nearby_amenities": "Supa IGA, Gordonvale State School, mill, post office, "
                           "Gordonvale Medical Centre",
        "pharmacist_demand": "low",  # Hard to recruit to regional QLD
        "competition_notes": "Only Gordonvale Discount Drug Store at 1.92km. Next in Cairns suburbs.",
        "demographic_notes": "Working class, agriculture/mining, families. Growing slowly with "
                            "Cairns southern corridor development.",
    },
    "Lake Macquarie Medical Centre": {
        "actual_location": "Charlestown, Newcastle NSW",
        "description": "Major suburb in Lake Macquarie LGA, 10km from Newcastle CBD. "
                       "Large medical centre with 10 GPs. Charlestown Square nearby.",
        "area_type": "metro_suburban",
        "population_local": 13601,
        "population_catchment": 6224,
        "growth_outlook": "moderate",
        "median_income": 58000,
        "commercial_rent_sqm_pa": 400,
        "pharmacy_size_sqm": 110,
        "area_thriving": True,
        "gp_count": 10,
        "nearby_amenities": "Charlestown Square (major shopping centre), John Hunter Hospital "
                           "nearby, schools, Charlestown Swim Centre",
        "pharmacist_demand": "moderate",
        "competition_notes": "Nearest: Pharmacy 4 Less Mt Hutton 352m. 56 pharmacies in 10km! "
                            "Very saturated market.",
        "demographic_notes": "Established suburban, families + retirees. Stable population.",
    },
    "Gladstone Medical Centre": {
        "actual_location": "Gladstone, QLD",
        "description": "Industrial port city, 517km NW of Brisbane. Major LNG/resources hub. "
                       "Population ~45,000. Medical centre at 19 Tank Street.",
        "area_type": "regional_city",
        "population_local": 45185,
        "population_catchment": 34703,
        "growth_outlook": "moderate",  # Resources boom/bust cycles
        "median_income": 65000,  # Mining/port jobs boost income
        "commercial_rent_sqm_pa": 320,
        "pharmacy_size_sqm": 100,
        "area_thriving": True,  # Currently in resources upswing
        "gp_count": 8,
        "nearby_amenities": "Gladstone CBD, Gladstone Hospital, Stockland Gladstone, port",
        "pharmacist_demand": "high",  # Hard to attract to regional QLD
        "competition_notes": "Nearest: GP Discount Pharmacy 583m. 7 pharmacies in 10km — "
                            "reasonable for regional city.",
        "demographic_notes": "Workers (port, LNG, mining), families. Boom-bust cycles affect "
                            "population. High turnover.",
    },
}


# ================================================================
# FINANCIAL ASSUMPTIONS
# ================================================================

# Setup costs
FITOUT_COSTS = {
    "rural_town": 250000,
    "regional_town": 280000,
    "regional_city": 300000,
    "metro_suburban": 350000,
    "metro_growth": 350000,
    "metro_medical_hub": 400000,
}

STOCK_COSTS = {
    "rural_town": 80000,
    "regional_town": 90000,
    "regional_city": 100000,
    "metro_suburban": 100000,
    "metro_growth": 100000,
    "metro_medical_hub": 120000,
}

PBS_APPROVAL_COSTS = {
    "rural_town": 5000,
    "regional_town": 6000,
    "regional_city": 7000,
    "metro_suburban": 8000,
    "metro_growth": 8000,
    "metro_medical_hub": 10000,
}

WORKING_CAPITAL_MONTHS = 3

# Revenue assumptions
AVG_SCRIPT_VALUE = 25.0  # Mix of PBS + private
FRONT_OF_SHOP_FRACTION = 0.35  # 30-40% of total revenue
CLINICAL_SERVICES_FRACTION = 0.05  # Flu vaxx, medschecks, etc.
SCRIPTS_PER_CAPITA_YEAR = 13  # Australian avg ~12-15

# Item 136 medical centre capture rates
ITEM_136_BASE_CAPTURE = 0.40  # 40% of nearby GP scripts
ITEM_136_HIGH_CAPTURE = 0.60  # 60% if directly co-located

# Profitability
GROSS_MARGIN = 0.30  # 28-32%, use 30%
WAGES_FRACTION = 0.57  # 55-60% of revenue
RENT_FRACTION_CAP = 0.10  # Rent shouldn't exceed 10% of revenue

# Exit value
GOODWILL_MULTIPLES = {
    12: 1.0,   # 1x revenue at 12 months
    24: 1.5,   # 1.5x at 24 months (established)
    36: 2.0,   # 2x at 36 months (proven)
}

# Benchmarks
BREAKEVEN_SCRIPTS_DAY = 80
HEALTHY_SCRIPTS_DAY = 120
EXCEPTIONAL_SCRIPTS_DAY = 200

# Operating cost items (annual)
INSURANCE_ANNUAL = 12000
UTILITIES_ANNUAL = 8000
SOFTWARE_IT_ANNUAL = 6000
MARKETING_ANNUAL = 10000
ACCOUNTING_ANNUAL = 6000
SUNDRY_ANNUAL = 8000

# Staff
PHARMACIST_SALARY = 95000  # Including super
DISPENSE_TECH_SALARY = 62000
RETAIL_STAFF_SALARY = 55000
SECOND_PHARMACIST_THRESHOLD_SCRIPTS = 150  # scripts/day


# ================================================================
# MODEL FUNCTIONS
# ================================================================

def estimate_scripts_per_day(opp: Dict, loc: Dict) -> Tuple[float, Dict]:
    """
    Estimate daily prescription volume.
    
    Key drivers:
    - Population/pharmacy ratio (how underserviced)
    - GP proximity (Item 136 = medical centre)
    - Area type (metro = more walk-ins, regional = more loyalty)
    - Competition proximity
    """
    ratio = opp.get('ratio', 0)
    pop_10km = opp.get('pop_10km', 0)
    pharmacies_10km = opp.get('pharmacies_10km', 1)
    rules = opp.get('rules', '')
    nearest_km = opp.get('nearest_pharmacy_km', 0)
    gp_count = loc.get('gp_count', 0)
    area_type = loc.get('area_type', 'metro_suburban')
    
    details = {}
    
    if 'Item 136' in rules:
        # Medical centre pharmacy — script volume driven by GP patients
        # Each GP sees ~25-35 patients/day, ~70% get a script
        patients_per_gp_day = 30
        script_rate = 0.70
        
        # Capture rate depends on distance to nearest competing pharmacy
        if nearest_km > 1.0:
            capture_rate = ITEM_136_HIGH_CAPTURE  # 60%
        else:
            capture_rate = ITEM_136_BASE_CAPTURE  # 40%
        
        gp_scripts = gp_count * patients_per_gp_day * script_rate * capture_rate
        
        # Walk-in/local scripts (not from the medical centre)
        # Use population ratio to estimate
        effective_pop = min(pop_10km, 50000)
        local_share = 1.0 / max(pharmacies_10km + 1, 1)  # +1 for the new pharmacy
        local_scripts_year = effective_pop * SCRIPTS_PER_CAPITA_YEAR * local_share
        local_scripts_day = local_scripts_year / 365
        
        # Total scripts
        scripts_day = gp_scripts + local_scripts_day * 0.3  # Only capture 30% of walk-ins initially
        
        details['gp_driven_scripts'] = round(gp_scripts, 1)
        details['local_walk_in_scripts'] = round(local_scripts_day * 0.3, 1)
        details['capture_rate'] = capture_rate
        details['method'] = 'item_136_medical_centre'
        
    elif 'Item 132' in rules:
        # Town-based pharmacy — population-driven
        # Use local population (not 10km radius which can include nearby cities)
        local_pop = loc.get('population_local', 5000)
        catchment_pop = loc.get('population_catchment', local_pop)
        # Cap catchment to realistic pharmacy service area
        # A single pharmacy typically serves 3,000-8,000 people
        effective_catchment = min(catchment_pop, local_pop * 2.0)
        existing_pharmacies = pharmacies_10km
        
        # Market share: new entrant in a small town
        # Divide market by total pharmacies (existing + new)
        total_after = existing_pharmacies + 1
        if existing_pharmacies <= 1:
            market_share = 1.0 / total_after  # Split evenly in duopoly
        elif existing_pharmacies <= 2:
            market_share = 0.30
        else:
            market_share = 0.20
        
        # IGA co-location bonus (foot traffic)
        if 'IGA' in opp.get('name', ''):
            market_share *= 1.15  # 15% boost from supermarket foot traffic
        
        annual_scripts = effective_catchment * SCRIPTS_PER_CAPITA_YEAR * market_share
        scripts_day = annual_scripts / 365
        
        details['local_pop'] = local_pop
        details['effective_catchment'] = effective_catchment
        details['catchment_pop'] = catchment_pop
        details['market_share'] = round(market_share, 3)
        details['method'] = 'item_132_town'
    else:
        # Fallback
        scripts_day = 60  # Conservative default
        details['method'] = 'default'
    
    # Apply area-type adjustments
    area_multipliers = {
        "rural_town": 0.85,       # Lower foot traffic
        "regional_town": 0.90,
        "regional_city": 1.00,
        "metro_suburban": 1.00,
        "metro_growth": 1.10,     # Growing area = more demand
        "metro_medical_hub": 1.15, # Hospital precinct = high demand
    }
    multiplier = area_multipliers.get(area_type, 1.0)
    scripts_day *= multiplier
    details['area_multiplier'] = multiplier
    
    # Ramp-up: Year 1 average is ~60% of mature volume
    details['mature_scripts_day'] = round(scripts_day, 1)
    details['year1_avg_scripts_day'] = round(scripts_day * 0.60, 1)
    details['year2_avg_scripts_day'] = round(scripts_day * 0.85, 1)
    details['year3_avg_scripts_day'] = round(scripts_day * 1.00, 1)
    
    return scripts_day, details


def calculate_setup_costs(loc: Dict) -> Dict:
    """Calculate total establishment costs."""
    area_type = loc.get('area_type', 'metro_suburban')
    
    fitout = FITOUT_COSTS.get(area_type, 350000)
    stock = STOCK_COSTS.get(area_type, 100000)
    pbs_approval = PBS_APPROVAL_COSTS.get(area_type, 8000)
    
    # Working capital = 3 months of operating costs (estimate)
    monthly_opex_estimate = 35000  # Rough monthly operating
    if area_type in ('metro_medical_hub', 'metro_growth'):
        monthly_opex_estimate = 45000
    elif area_type in ('rural_town', 'regional_town'):
        monthly_opex_estimate = 28000
    
    working_capital = monthly_opex_estimate * WORKING_CAPITAL_MONTHS
    
    total = fitout + stock + pbs_approval + working_capital
    
    return {
        'fitout': fitout,
        'stock': stock,
        'pbs_approval': pbs_approval,
        'working_capital': working_capital,
        'total_establishment': total,
    }


def calculate_revenue(scripts_day: float, ramp_year: int = 3) -> Dict:
    """
    Calculate annual revenue based on scripts/day.
    ramp_year: 1, 2, or 3 (for ramp-up adjustment)
    """
    ramp_factors = {1: 0.60, 2: 0.85, 3: 1.00}
    ramp = ramp_factors.get(ramp_year, 1.0)
    effective_scripts = scripts_day * ramp
    
    # Script revenue
    annual_script_revenue = effective_scripts * AVG_SCRIPT_VALUE * 365
    
    # Front-of-shop (OTC, retail)
    # Total revenue = script revenue / (1 - front_of_shop_fraction - clinical_fraction)
    total_revenue = annual_script_revenue / (1.0 - FRONT_OF_SHOP_FRACTION - CLINICAL_SERVICES_FRACTION)
    front_of_shop = total_revenue * FRONT_OF_SHOP_FRACTION
    clinical = total_revenue * CLINICAL_SERVICES_FRACTION
    
    return {
        'scripts_per_day': round(effective_scripts, 1),
        'annual_script_revenue': round(annual_script_revenue),
        'front_of_shop_revenue': round(front_of_shop),
        'clinical_services_revenue': round(clinical),
        'total_annual_revenue': round(total_revenue),
    }


def calculate_operating_costs(revenue: float, loc: Dict, scripts_day: float) -> Dict:
    """Calculate annual operating costs."""
    area_type = loc.get('area_type', 'metro_suburban')
    rent_sqm = loc.get('commercial_rent_sqm_pa', 400)
    size_sqm = loc.get('pharmacy_size_sqm', 110)
    
    # Rent
    annual_rent = rent_sqm * size_sqm
    
    # Staff
    # Minimum: 1 pharmacist, 1 dispense tech, 1 retail
    pharmacist_cost = PHARMACIST_SALARY
    if scripts_day > SECOND_PHARMACIST_THRESHOLD_SCRIPTS:
        pharmacist_cost += PHARMACIST_SALARY  # 2nd pharmacist
    
    # Regional loading for hard-to-recruit areas
    regional_loading = 1.0
    if area_type in ('rural_town', 'regional_town'):
        regional_loading = 1.15  # 15% premium to attract pharmacists
    elif area_type == 'regional_city':
        regional_loading = 1.08
    
    pharmacist_cost *= regional_loading
    
    # Scale retail staff with revenue
    retail_staff_count = max(1, round(revenue / 500000))
    retail_cost = retail_staff_count * RETAIL_STAFF_SALARY
    
    total_wages = pharmacist_cost + DISPENSE_TECH_SALARY + retail_cost
    
    # Other costs
    cogs = revenue * (1 - GROSS_MARGIN)  # Cost of goods
    insurance = INSURANCE_ANNUAL
    utilities = UTILITIES_ANNUAL
    software = SOFTWARE_IT_ANNUAL
    marketing = MARKETING_ANNUAL
    accounting = ACCOUNTING_ANNUAL
    sundry = SUNDRY_ANNUAL
    
    total_opex = (annual_rent + total_wages + insurance + utilities + 
                  software + marketing + accounting + sundry)
    total_costs = cogs + total_opex
    
    return {
        'cogs': round(cogs),
        'annual_rent': round(annual_rent),
        'wages': round(total_wages),
        'pharmacist_cost': round(pharmacist_cost),
        'retail_staff_count': retail_staff_count,
        'regional_loading': regional_loading,
        'insurance': insurance,
        'utilities': utilities,
        'software_it': software,
        'marketing': marketing,
        'accounting': accounting,
        'sundry': sundry,
        'total_opex': round(total_opex),
        'total_costs': round(total_costs),
    }


def calculate_profitability(revenue: float, costs: Dict) -> Dict:
    """Calculate profitability metrics."""
    gross_profit = revenue * GROSS_MARGIN
    ebitda = revenue - costs['total_costs']
    ebitda_margin = ebitda / revenue if revenue > 0 else 0
    
    return {
        'gross_profit': round(gross_profit),
        'gross_margin_pct': round(GROSS_MARGIN * 100, 1),
        'ebitda': round(ebitda),
        'ebitda_margin_pct': round(ebitda_margin * 100, 1),
        'profitable': ebitda > 0,
    }


def calculate_break_even(setup_costs: Dict, year_revenues: Dict, year_costs: Dict) -> Dict:
    """
    Calculate break-even timeline.
    Break-even = month at which cumulative EBITDA recovers total investment.
    We simulate month-by-month with linear ramp within each year.
    """
    total_investment = setup_costs['total_establishment']
    
    # Get annual EBITDA for years 1-5
    annual_ebitda = {}
    for year in range(1, 6):
        if year <= 3:
            rev = year_revenues[year]['total_annual_revenue']
            costs = year_costs[year]['total_costs']
        else:
            rev = year_revenues[3]['total_annual_revenue'] * (1.02 ** (year - 3))
            costs = year_costs[3]['total_costs'] * (1.01 ** (year - 3))
        annual_ebitda[year] = rev - costs
    
    # Simulate month by month
    cumulative = -total_investment
    break_even_month = None
    yearly_cumulative = {}
    
    for year in range(1, 8):  # Up to 7 years
        ebitda = annual_ebitda.get(year, annual_ebitda.get(5, 0) * (1.02 ** (year - 5)))
        monthly = ebitda / 12
        
        for month in range(1, 13):
            cumulative += monthly
            abs_month = (year - 1) * 12 + month
            
            if break_even_month is None and cumulative >= 0:
                break_even_month = abs_month
        
        yearly_cumulative[year] = round(cumulative)
    
    return {
        'break_even_months': break_even_month if break_even_month else 84,  # Max 7 years
        'cumulative_profit_year1': yearly_cumulative.get(1, 0),
        'cumulative_profit_year2': yearly_cumulative.get(2, 0),
        'cumulative_profit_year3': yearly_cumulative.get(3, 0),
        'cumulative_profit_year5': yearly_cumulative.get(5, 0),
    }


def calculate_exit_value(year_revenues: Dict, setup_costs: Dict) -> Dict:
    """Calculate exit/goodwill value at 12, 24, 36 months."""
    total_investment = setup_costs['total_establishment']
    
    results = {}
    for months, multiple in GOODWILL_MULTIPLES.items():
        year = months // 12
        rev = year_revenues.get(year, year_revenues[3])['total_annual_revenue']
        exit_value = rev * multiple
        roi = (exit_value - total_investment) / total_investment
        
        results[f'exit_value_{months}m'] = round(exit_value)
        results[f'roi_{months}m_pct'] = round(roi * 100, 1)
    
    return results


def assess_risk(opp: Dict, loc: Dict) -> Dict:
    """
    Risk assessment on 1-5 scale (1=low risk, 5=high risk).
    """
    area_type = loc.get('area_type', 'metro_suburban')
    ratio = opp.get('ratio', 0)
    pharmacies_10km = opp.get('pharmacies_10km', 0)
    nearest_km = opp.get('nearest_pharmacy_km', 0)
    rules = opp.get('rules', '')
    growth = loc.get('growth_outlook', 'moderate')
    
    # --- Competition Risk (1-5) ---
    if pharmacies_10km <= 2:
        comp_risk = 1
        comp_note = f"Very low competition — only {pharmacies_10km} pharmacies in 10km"
    elif pharmacies_10km <= 10:
        comp_risk = 2
        comp_note = f"Low competition — {pharmacies_10km} pharmacies in 10km"
    elif pharmacies_10km <= 30:
        comp_risk = 3
        comp_note = f"Moderate competition — {pharmacies_10km} pharmacies in 10km"
    elif pharmacies_10km <= 60:
        comp_risk = 4
        comp_note = f"High competition — {pharmacies_10km} pharmacies in 10km"
    else:
        comp_risk = 5
        comp_note = f"Very high competition — {pharmacies_10km} pharmacies in 10km"
    
    # Nearest pharmacy distance modifies risk
    if nearest_km < 0.3:
        comp_risk = min(5, comp_risk + 1)
        comp_note += f". Nearest pharmacy very close ({nearest_km*1000:.0f}m)"
    elif nearest_km > 1.5:
        comp_risk = max(1, comp_risk - 1)
        comp_note += f". Nearest pharmacy {nearest_km:.1f}km away (good buffer)"
    
    # --- Location Risk (1-5) ---
    growth_risk_map = {
        'very_strong': 1,
        'strong': 2,
        'moderate': 3,
        'stable': 3,
        'declining': 5,
    }
    loc_risk = growth_risk_map.get(growth, 3)
    
    if area_type in ('rural_town', 'regional_town'):
        loc_risk = min(5, loc_risk + 1)  # Small town risk premium
        loc_note = f"Regional/rural location ({growth} growth). Population dependency risk."
    elif area_type == 'metro_medical_hub':
        loc_risk = max(1, loc_risk - 1)  # Medical hub = resilient
        loc_note = f"Major medical precinct ({growth} growth). Highly resilient location."
    else:
        loc_note = f"{area_type.replace('_', ' ').title()} area with {growth} growth outlook."
    
    # --- Regulatory Risk (1-5) ---
    if 'Item 132' in rules:
        reg_risk = 2
        reg_note = "Item 132 (new town pharmacy) — well-established pathway, lower risk"
    elif 'Item 136' in rules:
        gp_count = loc.get('gp_count', 0)
        if gp_count >= 10:
            reg_risk = 2
            reg_note = f"Item 136 (large medical centre, {gp_count} GPs) — strong case with clear FTE threshold"
        elif gp_count >= 8:
            reg_risk = 3
            reg_note = f"Item 136 ({gp_count} GPs) — meets threshold but borderline FTE after part-time adjustment"
        else:
            reg_risk = 4
            reg_note = f"Item 136 ({gp_count} GPs) — may not meet 8 FTE threshold after adjustment"
    else:
        reg_risk = 4
        reg_note = "Non-standard pathway — higher regulatory uncertainty"
    
    # --- Operational Risk (1-5) ---
    if area_type in ('rural_town', 'regional_town'):
        op_risk = 4
        op_note = "Regional location — pharmacist recruitment very difficult, supply chain challenges"
    elif area_type == 'regional_city':
        op_risk = 3
        op_note = "Regional city — moderate recruitment difficulty, adequate supply chain"
    else:
        op_risk = 2
        op_note = "Metro area — good pharmacist supply, reliable supply chain"
    
    if loc.get('pharmacist_demand') == 'high':
        op_note += ". High demand for pharmacists in area."
    
    # Overall risk = weighted average
    overall = round((comp_risk * 0.30 + loc_risk * 0.25 + reg_risk * 0.25 + op_risk * 0.20), 1)
    
    return {
        'competition_risk': comp_risk,
        'competition_note': comp_note,
        'location_risk': loc_risk,
        'location_note': loc_note,
        'regulatory_risk': reg_risk,
        'regulatory_note': reg_note,
        'operational_risk': op_risk,
        'operational_note': op_note,
        'overall_risk': overall,
        'risk_rating': 'LOW' if overall <= 2.0 else ('MEDIUM' if overall <= 3.5 else 'HIGH'),
    }


def build_financial_model(opp: Dict) -> Dict:
    """Build complete financial model for a single opportunity."""
    name = opp['name']
    loc = LOCATION_DATA.get(name, {})
    
    if not loc:
        print(f"  WARNING: No location data for '{name}', using defaults")
        loc = {
            'area_type': 'metro_suburban',
            'commercial_rent_sqm_pa': 400,
            'pharmacy_size_sqm': 110,
            'gp_count': 0,
            'growth_outlook': 'moderate',
            'pharmacist_demand': 'moderate',
        }
    
    # --- Setup Costs ---
    setup = calculate_setup_costs(loc)
    
    # --- Scripts Estimate ---
    mature_scripts, script_details = estimate_scripts_per_day(opp, loc)
    
    # --- Revenue by year (1-3 with ramp-up) ---
    year_revenues = {}
    for yr in range(1, 4):
        year_revenues[yr] = calculate_revenue(mature_scripts, ramp_year=yr)
    
    # --- Operating Costs by year ---
    year_costs = {}
    for yr in range(1, 4):
        rev = year_revenues[yr]['total_annual_revenue']
        scripts = year_revenues[yr]['scripts_per_day']
        year_costs[yr] = calculate_operating_costs(rev, loc, scripts)
    
    # --- Profitability by year ---
    year_profit = {}
    for yr in range(1, 4):
        year_profit[yr] = calculate_profitability(
            year_revenues[yr]['total_annual_revenue'],
            year_costs[yr]
        )
    
    # --- Break-even ---
    breakeven = calculate_break_even(setup, year_revenues, year_costs)
    
    # --- Exit Value ---
    exit_val = calculate_exit_value(year_revenues, setup)
    
    # --- Risk ---
    risk = assess_risk(opp, loc)
    
    # --- Script benchmark comparison ---
    benchmark = 'below_breakeven'
    if mature_scripts >= EXCEPTIONAL_SCRIPTS_DAY:
        benchmark = 'exceptional'
    elif mature_scripts >= HEALTHY_SCRIPTS_DAY:
        benchmark = 'healthy_profit'
    elif mature_scripts >= BREAKEVEN_SCRIPTS_DAY:
        benchmark = 'breakeven_plus'
    
    # --- Compile model ---
    model = {
        'opportunity': {
            'name': name,
            'state': opp.get('state', ''),
            'address': opp.get('address', ''),
            'lat': opp.get('lat', 0),
            'lng': opp.get('lng', 0),
            'rules': opp.get('rules', ''),
            'score': opp.get('score', 0),
            'ratio': opp.get('ratio', 0),
            'pop_10km': opp.get('pop_10km', 0),
            'pharmacies_10km': opp.get('pharmacies_10km', 0),
            'nearest_pharmacy_km': round(opp.get('nearest_pharmacy_km', 0), 2),
            'nearest_pharmacy': opp.get('nearest_pharmacy', ''),
        },
        'location_research': {
            'actual_location': loc.get('actual_location', ''),
            'description': loc.get('description', ''),
            'area_type': loc.get('area_type', ''),
            'growth_outlook': loc.get('growth_outlook', ''),
            'nearby_amenities': loc.get('nearby_amenities', ''),
            'competition_notes': loc.get('competition_notes', ''),
            'demographic_notes': loc.get('demographic_notes', ''),
        },
        'setup_costs': setup,
        'script_analysis': {
            'mature_scripts_per_day': round(mature_scripts, 1),
            'benchmark': benchmark,
            **script_details,
        },
        'revenue': {
            'year1': year_revenues[1],
            'year2': year_revenues[2],
            'year3_mature': year_revenues[3],
        },
        'operating_costs': {
            'year1': year_costs[1],
            'year2': year_costs[2],
            'year3_mature': year_costs[3],
        },
        'profitability': {
            'year1': year_profit[1],
            'year2': year_profit[2],
            'year3_mature': year_profit[3],
        },
        'break_even': breakeven,
        'exit_value': exit_val,
        'risk_assessment': risk,
        'summary': {
            'total_investment': setup['total_establishment'],
            'mature_annual_revenue': year_revenues[3]['total_annual_revenue'],
            'mature_ebitda': year_profit[3]['ebitda'],
            'break_even_months': breakeven['break_even_months'],
            'exit_value_36m': exit_val['exit_value_36m'],
            'roi_36m_pct': exit_val['roi_36m_pct'],
            'overall_risk': risk['overall_risk'],
            'risk_rating': risk['risk_rating'],
            'scripts_per_day_mature': round(mature_scripts, 1),
            'recommendation': '',  # Filled in later
        },
    }
    
    return model


def generate_recommendation(model: Dict) -> str:
    """Generate a brief recommendation for each opportunity."""
    s = model['summary']
    risk = model['risk_assessment']
    scripts = s['scripts_per_day_mature']
    ebitda = s['mature_ebitda']
    be_months = s['break_even_months']
    
    if scripts >= EXCEPTIONAL_SCRIPTS_DAY and risk['overall_risk'] <= 2.5:
        return "STRONG BUY — Exceptional script volume with low risk. Top-tier opportunity."
    elif scripts >= HEALTHY_SCRIPTS_DAY and risk['overall_risk'] <= 3.0:
        return "BUY — Healthy profit potential with manageable risk. Solid business case."
    elif scripts >= BREAKEVEN_SCRIPTS_DAY and ebitda > 0:
        if risk['overall_risk'] <= 3.0:
            return "CONSIDER — Viable opportunity but moderate returns. Proceed with due diligence."
        else:
            return "CAUTIOUS — Viable but higher risk. Requires careful risk mitigation strategy."
    elif scripts >= BREAKEVEN_SCRIPTS_DAY * 0.8:
        return "MARGINAL — Close to break-even. High sensitivity to assumptions. Extra due diligence needed."
    else:
        return "PASS — Below break-even threshold. Not recommended without significant additional factors."


def generate_comparison_report(models: List[Dict]) -> str:
    """Generate markdown comparison report."""
    now = datetime.now().strftime('%Y-%m-%d %H:%M')
    
    md = f"""# PharmacyFinder — Financial Model Comparison Report
*Generated: {now}*

## Executive Summary

This report compares 8 verified greenfield pharmacy opportunities identified through
PharmacyFinder's ACPA rule analysis. Each opportunity has been modelled with setup costs,
revenue projections (3-year ramp), profitability analysis, exit valuations, and risk assessment.

**Key benchmarks:**
- 80 scripts/day = roughly break-even
- 120 scripts/day = healthy profit
- 200+ scripts/day = exceptional
- Item 136 (medical centre) pharmacies capture 40-60% of nearby GP scripts

---

## Side-by-Side Comparison

| Metric | {' | '.join(m['opportunity']['name'][:15] for m in models)} |
|--------|{'|'.join(['---' for _ in models])}|
| **State** | {' | '.join(m['opportunity']['state'] for m in models)} |
| **Rule** | {' | '.join(m['opportunity']['rules'] for m in models)} |
| **Ratio** | {' | '.join(f"{m['opportunity']['ratio']:,}" for m in models)} |
| **Score** | {' | '.join(str(m['opportunity']['score']) for m in models)} |
| **Area Type** | {' | '.join(m['location_research']['area_type'].replace('_', ' ') for m in models)} |
| **Setup Cost** | {' | '.join(f"${m['setup_costs']['total_establishment']:,}" for m in models)} |
| **Scripts/day (mature)** | {' | '.join(f"{m['summary']['scripts_per_day_mature']}" for m in models)} |
| **Year 1 Revenue** | {' | '.join(f"${m['revenue']['year1']['total_annual_revenue']:,}" for m in models)} |
| **Year 3 Revenue** | {' | '.join(f"${m['revenue']['year3_mature']['total_annual_revenue']:,}" for m in models)} |
| **Year 3 EBITDA** | {' | '.join(f"${m['profitability']['year3_mature']['ebitda']:,}" for m in models)} |
| **Break-even (months)** | {' | '.join(f"{m['break_even']['break_even_months']}" for m in models)} |
| **Exit Value (36m)** | {' | '.join(f"${m['exit_value']['exit_value_36m']:,}" for m in models)} |
| **ROI @ 36m** | {' | '.join(f"{m['exit_value']['roi_36m_pct']}%" for m in models)} |
| **Risk Rating** | {' | '.join(m['risk_assessment']['risk_rating'] for m in models)} |
| **Risk Score** | {' | '.join(f"{m['risk_assessment']['overall_risk']}" for m in models)} |

---

## Ranked by ROI (36 months)

"""
    # Sort by ROI descending
    by_roi = sorted(models, key=lambda m: m['exit_value']['roi_36m_pct'], reverse=True)
    md += "| Rank | Opportunity | State | ROI (36m) | Exit Value | EBITDA (Yr3) | Risk |\n"
    md += "|------|------------|-------|-----------|-----------|-------------|------|\n"
    for i, m in enumerate(by_roi, 1):
        md += (f"| {i} | {m['opportunity']['name']} | {m['opportunity']['state']} | "
               f"{m['exit_value']['roi_36m_pct']}% | "
               f"${m['exit_value']['exit_value_36m']:,} | "
               f"${m['profitability']['year3_mature']['ebitda']:,} | "
               f"{m['risk_assessment']['risk_rating']} ({m['risk_assessment']['overall_risk']}) |\n")
    
    md += "\n---\n\n## Ranked by Lowest Risk\n\n"
    by_risk = sorted(models, key=lambda m: m['risk_assessment']['overall_risk'])
    md += "| Rank | Opportunity | State | Risk Score | Risk Rating | EBITDA (Yr3) | ROI (36m) |\n"
    md += "|------|------------|-------|------------|------------|-------------|----------|\n"
    for i, m in enumerate(by_risk, 1):
        md += (f"| {i} | {m['opportunity']['name']} | {m['opportunity']['state']} | "
               f"{m['risk_assessment']['overall_risk']} | "
               f"{m['risk_assessment']['risk_rating']} | "
               f"${m['profitability']['year3_mature']['ebitda']:,} | "
               f"{m['exit_value']['roi_36m_pct']}% |\n")
    
    # Risk-adjusted ROI (ROI / risk)
    md += "\n---\n\n## Risk-Adjusted Ranking (ROI ÷ Risk Score)\n\n"
    for m in models:
        risk_score = max(m['risk_assessment']['overall_risk'], 0.1)
        m['_risk_adj_score'] = m['exit_value']['roi_36m_pct'] / risk_score
    
    by_risk_adj = sorted(models, key=lambda m: m['_risk_adj_score'], reverse=True)
    md += "| Rank | Opportunity | State | ROI/Risk | ROI (36m) | Risk | Scripts/day |\n"
    md += "|------|------------|-------|----------|-----------|------|-------------|\n"
    for i, m in enumerate(by_risk_adj, 1):
        md += (f"| {i} | {m['opportunity']['name']} | {m['opportunity']['state']} | "
               f"{m['_risk_adj_score']:.1f} | "
               f"{m['exit_value']['roi_36m_pct']}% | "
               f"{m['risk_assessment']['overall_risk']} | "
               f"{m['summary']['scripts_per_day_mature']} |\n")
    
    # Detailed section for each
    md += "\n---\n\n## Detailed Analysis\n\n"
    for m in models:
        opp = m['opportunity']
        loc = m['location_research']
        setup = m['setup_costs']
        risk = m['risk_assessment']
        scripts = m['script_analysis']
        
        md += f"### {opp['name']} ({opp['state']}) — {opp['rules']}\n\n"
        md += f"**Location:** {loc.get('actual_location', opp['address'])}\n\n"
        md += f"> {loc.get('description', 'No description')}\n\n"
        
        md += f"**Area Profile:**\n"
        md += f"- Type: {loc.get('area_type', '').replace('_', ' ').title()}\n"
        md += f"- Growth: {loc.get('growth_outlook', '').replace('_', ' ').title()}\n"
        md += f"- Nearby: {loc.get('nearby_amenities', '')}\n"
        md += f"- Demographics: {loc.get('demographic_notes', '')}\n\n"
        
        md += f"**Setup Costs:**\n"
        md += f"- Fit-out: ${setup['fitout']:,}\n"
        md += f"- Initial stock: ${setup['stock']:,}\n"
        md += f"- PBS approval: ${setup['pbs_approval']:,}\n"
        md += f"- Working capital: ${setup['working_capital']:,}\n"
        md += f"- **Total: ${setup['total_establishment']:,}**\n\n"
        
        md += f"**Script Analysis:**\n"
        md += f"- Mature scripts/day: **{scripts['mature_scripts_day']}** ({scripts['benchmark']})\n"
        md += f"- Year 1 avg: {scripts['year1_avg_scripts_day']}/day\n"
        md += f"- Year 2 avg: {scripts['year2_avg_scripts_day']}/day\n"
        md += f"- Method: {scripts['method'].replace('_', ' ').title()}\n"
        if 'gp_driven_scripts' in scripts:
            md += f"- GP-driven scripts: {scripts['gp_driven_scripts']}/day\n"
            md += f"- Walk-in scripts: {scripts['local_walk_in_scripts']}/day\n"
            md += f"- Capture rate: {scripts['capture_rate']:.0%}\n"
        if 'market_share' in scripts:
            md += f"- Market share: {scripts['market_share']:.1%}\n"
        md += "\n"
        
        md += f"**Revenue Projections:**\n"
        md += f"| | Year 1 | Year 2 | Year 3 (Mature) |\n"
        md += f"|---|--------|--------|------------------|\n"
        for key in ['scripts_per_day', 'total_annual_revenue', 'annual_script_revenue', 
                     'front_of_shop_revenue', 'clinical_services_revenue']:
            label = key.replace('_', ' ').title()
            v1 = m['revenue']['year1'][key]
            v2 = m['revenue']['year2'][key]
            v3 = m['revenue']['year3_mature'][key]
            if isinstance(v1, float) and v1 == int(v1):
                md += f"| {label} | {v1:.0f} | {v2:.0f} | {v3:.0f} |\n"
            elif key == 'scripts_per_day':
                md += f"| {label} | {v1} | {v2} | {v3} |\n"
            else:
                md += f"| {label} | ${v1:,} | ${v2:,} | ${v3:,} |\n"
        md += "\n"
        
        md += f"**Profitability:**\n"
        md += f"- Year 1 EBITDA: ${m['profitability']['year1']['ebitda']:,}\n"
        md += f"- Year 2 EBITDA: ${m['profitability']['year2']['ebitda']:,}\n"
        md += f"- Year 3 EBITDA: **${m['profitability']['year3_mature']['ebitda']:,}** "
        md += f"({m['profitability']['year3_mature']['ebitda_margin_pct']}% margin)\n"
        md += f"- Break-even: **{m['break_even']['break_even_months']} months**\n\n"
        
        md += f"**Exit Value:**\n"
        md += f"- 12 months: ${m['exit_value']['exit_value_12m']:,} (ROI: {m['exit_value']['roi_12m_pct']}%)\n"
        md += f"- 24 months: ${m['exit_value']['exit_value_24m']:,} (ROI: {m['exit_value']['roi_24m_pct']}%)\n"
        md += f"- 36 months: **${m['exit_value']['exit_value_36m']:,} (ROI: {m['exit_value']['roi_36m_pct']}%)**\n\n"
        
        md += f"**Risk Assessment:**\n"
        md += f"| Risk Type | Score | Notes |\n"
        md += f"|-----------|-------|-------|\n"
        md += f"| Competition | {risk['competition_risk']}/5 | {risk['competition_note']} |\n"
        md += f"| Location | {risk['location_risk']}/5 | {risk['location_note']} |\n"
        md += f"| Regulatory | {risk['regulatory_risk']}/5 | {risk['regulatory_note']} |\n"
        md += f"| Operational | {risk['operational_risk']}/5 | {risk['operational_note']} |\n"
        md += f"| **Overall** | **{risk['overall_risk']}/5** | **{risk['risk_rating']}** |\n\n"
        
        md += f"**Recommendation:** {m['summary']['recommendation']}\n\n"
        md += "---\n\n"
    
    # Top recommendation
    md += "## 🏆 Top Recommendation\n\n"
    
    # Pick best risk-adjusted opportunity
    best = by_risk_adj[0]
    second = by_risk_adj[1] if len(by_risk_adj) > 1 else None
    
    md += f"### #1: {best['opportunity']['name']} ({best['opportunity']['state']})\n\n"
    md += f"**Why this is the top pick:**\n\n"
    md += f"- **Risk-adjusted ROI score:** {best['_risk_adj_score']:.1f} (highest)\n"
    md += f"- **Mature scripts/day:** {best['summary']['scripts_per_day_mature']}\n"
    md += f"- **Year 3 EBITDA:** ${best['profitability']['year3_mature']['ebitda']:,}\n"
    md += f"- **36-month exit value:** ${best['exit_value']['exit_value_36m']:,}\n"
    md += f"- **Risk rating:** {best['risk_assessment']['risk_rating']} ({best['risk_assessment']['overall_risk']}/5)\n"
    md += f"- **Break-even:** {best['break_even']['break_even_months']} months\n\n"
    
    md += f"**Key strengths:**\n"
    md += f"- {best['location_research'].get('description', '')}\n"
    md += f"- {best['risk_assessment']['competition_note']}\n"
    md += f"- {best['risk_assessment']['location_note']}\n\n"
    
    if second:
        md += f"### #2: {second['opportunity']['name']} ({second['opportunity']['state']})\n\n"
        md += f"- Risk-adjusted score: {second['_risk_adj_score']:.1f}\n"
        md += f"- Year 3 EBITDA: ${second['profitability']['year3_mature']['ebitda']:,}\n"
        md += f"- Risk: {second['risk_assessment']['risk_rating']} ({second['risk_assessment']['overall_risk']}/5)\n\n"
    
    md += "---\n\n"
    md += "## Methodology Notes\n\n"
    md += """- **Script estimates** use GP patient volumes (Item 136) or population-based models (Item 132)
- **Ramp-up:** Year 1 = 60% of mature volume, Year 2 = 85%, Year 3 = 100%
- **Revenue mix:** PBS scripts + front-of-shop (35%) + clinical services (5%)
- **Average script value:** $25 (PBS + private mix)
- **Gross margin:** 30% (pharmacy average 28-32%)
- **Exit multiples:** 1x revenue (12m), 1.5x (24m), 2x (36m) — pharmacy industry standard
- **Risk scores:** 1 (lowest) to 5 (highest), weighted average of competition (30%), location (25%), regulatory (25%), operational (20%)
- **Risk-adjusted ranking:** ROI ÷ Risk Score — balances return against risk

### Assumptions & Limitations
- Script volume estimates are conservative; actual volumes may vary ±30%
- Rent estimates based on area averages; actual quotes needed
- Staff costs assume base team; high-volume sites may need more staff
- PBS policy changes could affect dispensing fees and margins
- ACPA approval is not guaranteed — each application assessed on merit
- COVID/pandemic effects not modelled
"""
    
    # Clean up temp field
    for m in models:
        if '_risk_adj_score' in m:
            del m['_risk_adj_score']
    
    return md


# ================================================================
# MAIN
# ================================================================

def main():
    print("=" * 70)
    print("  PHARMACYFINDER — FINANCIAL MODEL v2")
    print("  Greenfield Business Cases for 8 Verified Opportunities")
    print("=" * 70)
    print()
    
    # Load scored opportunities
    if not os.path.exists(SCORED_PATH):
        print(f"ERROR: {SCORED_PATH} not found")
        return
    
    with open(SCORED_PATH, 'r', encoding='utf-8') as f:
        all_opportunities = json.load(f)
    
    # Filter to the 8 verified opportunities
    TARGET_NAMES = [
        "IGA",
        "Murdoch Medical Centre",
        "GP Super Clinic Wynnum",
        "Sunshine Hospital Medical Centre",
        "Canning Vale Medical Centre",
        "Supa IGA Gordonvale",
        "Lake Macquarie Medical Centre",
        "Gladstone Medical Centre",
    ]
    opportunities = [o for o in all_opportunities if o['name'] in TARGET_NAMES]
    
    # Sort by the target order
    name_order = {name: i for i, name in enumerate(TARGET_NAMES)}
    opportunities.sort(key=lambda o: name_order.get(o['name'], 999))
    
    print(f"Loaded {len(all_opportunities)} scored, filtered to {len(opportunities)} verified opportunities\n")
    
    # Build models
    models = []
    for opp in opportunities:
        name = opp['name']
        print(f"  Modelling: {name} ({opp['state']}) — {opp['rules']}, ratio={opp['ratio']:,}")
        
        model = build_financial_model(opp)
        model['summary']['recommendation'] = generate_recommendation(model)
        models.append(model)
        
        s = model['summary']
        print(f"    -> Scripts/day: {s['scripts_per_day_mature']} | "
              f"Revenue: ${s['mature_annual_revenue']:,} | "
              f"EBITDA: ${s['mature_ebitda']:,} | "
              f"Risk: {s['risk_rating']} ({s['overall_risk']})")
    
    print()
    
    # Save JSON
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    json_path = os.path.join(OUTPUT_DIR, 'financial_models_v2.json')
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(models, f, indent=2, ensure_ascii=False)
    print(f"✅ Saved full models: {json_path}")
    
    # Generate comparison report
    md_path = os.path.join(OUTPUT_DIR, 'financial_comparison.md')
    report = generate_comparison_report(models)
    with open(md_path, 'w', encoding='utf-8') as f:
        f.write(report)
    print(f"✅ Saved comparison report: {md_path}")
    
    # Console summary
    print()
    print("=" * 70)
    print("  SUMMARY — ALL 8 OPPORTUNITIES")
    print("=" * 70)
    print()
    print(f"{'Opportunity':<30} {'Scripts':>8} {'Revenue':>12} {'EBITDA':>12} {'B/E':>6} {'Risk':>8} {'ROI 36m':>10}")
    print("-" * 90)
    
    for m in models:
        s = m['summary']
        print(f"{m['opportunity']['name']:<30} "
              f"{s['scripts_per_day_mature']:>8.0f} "
              f"${s['mature_annual_revenue']:>10,} "
              f"${s['mature_ebitda']:>10,} "
              f"{s['break_even_months']:>4}mo "
              f"{s['risk_rating']:>8} "
              f"{s['roi_36m_pct']:>8.0f}%")
    
    print("-" * 90)
    
    # Top pick
    for m in models:
        risk_score = max(m['risk_assessment']['overall_risk'], 0.1)
        m['_risk_adj'] = m['exit_value']['roi_36m_pct'] / risk_score
    
    best = max(models, key=lambda m: m['_risk_adj'])
    print(f"\n🏆 TOP PICK: {best['opportunity']['name']} ({best['opportunity']['state']})")
    print(f"   {best['summary']['recommendation']}")
    
    # Cleanup
    for m in models:
        if '_risk_adj' in m:
            del m['_risk_adj']
    
    print(f"\nDone.")


if __name__ == '__main__':
    main()
