"""
Configuration file for Pharmacy Location Finder.
API keys and constants for the application.
"""
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# API Keys - load from environment variables
# Note: GOOGLE_MAPS_API_KEY is optional - using free Nominatim (OpenStreetMap) instead
GOOGLE_MAPS_API_KEY = os.getenv('GOOGLE_MAPS_API_KEY', '')
HEALTHDIRECT_API_KEY = os.getenv('HEALTHDIRECT_API_KEY', '')
WOOLWORTHS_API_KEY = os.getenv('WOOLWORTHS_API_KEY', '')
MYHOSPITALS_API_KEY = os.getenv('MYHOSPITALS_API_KEY', '')

# Database configuration
DATABASE_PATH = 'pharmacy_finder.db'

# OSRM server for routing
OSRM_SERVER = "http://router.project-osrm.org"

# Distance thresholds (in kilometers) for each rule item
RULE_DISTANCES = {
    'item_130': 1.5,      # Item 130: >= 1.5km straight line from nearest pharmacy
    'item_130_supermarket': 0.5,  # Item 130: supermarket/GP must be within 500m straight line
    'item_131': 10.0,     # Item 131: >= 10km by shortest lawful access route from nearest
    'item_132_nearest': 0.2,   # Item 132(a)(ii): >= 200m straight line from nearest pharmacy
    'item_132_other': 10.0,    # Item 132(a)(iii): >= 10km route from any OTHER pharmacy
    'item_133': 0.5,      # Item 133(b): >= 500m straight line (excl. large centres/hospitals)
    # Item 134: NO minimum distance
    # Item 134A: NO minimum distance
    # Item 135: NO minimum distance
    'item_136': 0.3,      # Item 136(c): >= 300m straight line (excl. large centres/hospitals)
}

# FTE (Full-Time Equivalent) thresholds
FTE_REQUIREMENTS = {
    'item_130_gp': 1.0,   # Item 130(b)(i): 1 FTE prescribing medical practitioner
    'item_132_gp': 4.0,   # Item 132(b)(i): 4 FTE prescribing medical practitioners in town
    'item_136_prescribers': 8.0,   # Item 136(d): 8 FTE PBS prescribers total
    'item_136_medical': 7.0,       # Item 136(d): of which at least 7 must be medical practitioners
}

# Hours per week calculation
HOURS_PER_WEEK_FULL_TIME = 38.0
MINIMUM_HOURS_FOR_GP = 20.0

# Shopping centre thresholds (from handbook definitions)
SHOPPING_CENTRE_THRESHOLDS = {
    # Small shopping centre (Item 133)
    'small_centre_gla': 5000,       # Centre GLA >= 5,000sqm
    'small_centre_tenants': 15,     # >= 15 other commercial establishments
    'supermarket_gla': 2500,        # Supermarket GLA >= 2,500sqm (applies to all centre items)
    'item_133_distance_km': 0.5,    # 500m from nearest pharmacy (excl. large centres/hospitals)

    # Large shopping centre (Items 134, 134A)
    'large_centre_gla': 5000,       # Centre GLA >= 5,000sqm (same as small)
    'large_centre_tenants': 50,     # >= 50 other commercial establishments

    # Item 134A additional pharmacy thresholds
    'item_134a_tier1_tenants': 100,  # 100-199 tenants: max 1 existing pharmacy
    'item_134a_tier2_tenants': 200,  # 200+ tenants: max 2 existing pharmacies
}

# Supermarket GLA thresholds (individual supermarket checks)
FLOOR_AREA_THRESHOLDS = {
    'supermarket_item_130_small': 1000,  # Item 130(b)(i): >= 1,000sqm with 1 FTE GP
    'supermarket_item_130_large': 2500,  # Item 130(b)(ii): >= 2,500sqm alone
    'supermarket_in_centre': 2500,       # Items 133/134/134A: supermarket in centre >= 2,500sqm
    'item_132_combined': 2500,           # Item 132(b)(ii): 1-2 supermarkets combined >= 2,500sqm
}

# Hospital requirements - Item 135
# "Large private hospital" = can admit >= 150 patients at any one time per licence
# Note: "admit" means admitted as private patient (incl. same-day), NOT outpatients
HOSPITAL_ADMISSION_CAPACITY = 150  # Item 135: >= 150 patients admission capacity
HOSPITAL_BED_COUNT = 150  # Legacy alias — same value, bed_count is our DB proxy for admission capacity
HOSPITAL_BED_COUNT_UNKNOWN_THRESHOLD = 50  # Minimum beds to consider unknown-type hospitals

# Proximity thresholds for "in the complex" checks (Items 132-136)
PROXIMITY_IN_COMPLEX_KM = 0.05   # 50m — pharmacy must be INSIDE the complex
PROXIMITY_ADJACENT_KM = 0.20     # 200m — pharmacy is adjacent/nearby
PROXIMITY_NOTE_IN = 'Note: Pharmacy must be located INSIDE the complex, not just nearby. This distance-based check cannot verify physical containment.'
PROXIMITY_NOTE_ADJACENT = '⚠️ Adjacent but may not be inside the complex (50-200m). Physical verification required.'

# Supermarket access rule — applies to ALL rules
SUPERMARKET_ACCESS_WARNING = '⚠️ SUPERMARKET ACCESS RULE: The proposed pharmacy must NOT be directly accessible by the public from within a supermarket. Ensure separate public entrance.'

# Major supermarket chains
MAJOR_SUPERMARKETS = [
    'woolworths',
    'coles',
    'aldi',
]

# API endpoints
API_ENDPOINTS = {
    'healthdirect': 'https://api.healthdirect.gov.au/v1',
    'myhospitals': 'https://www.myhospitals.gov.au/api',
}

# Web scraping configuration
SCRAPER_CONFIG = {
    'user_agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'rate_limit_delay': 2.0,  # Seconds between requests
    'timeout': 30,  # Request timeout in seconds
    'max_retries': 3,
}

# Commercial real estate websites
COMMERCIAL_RE_SITES = {
    'commercial_real_estate': {
        'url': 'https://www.commercialrealestate.com.au',
        'search_path': '/for-lease/property/',
    },
    'real_commercial': {
        'url': 'https://www.realcommercial.com.au',
        'search_path': '/for-lease/',
    },
    'domain_commercial': {
        'url': 'https://www.commercialrealestate.com.au',
        'search_path': '/for-lease/',
    },
}

# Property types to target
TARGET_PROPERTY_TYPES = [
    'retail',
    'medical',
    'office',
    'shop',
]

# Australian states/territories
AUSTRALIAN_STATES = {
    'NSW': 'New South Wales',
    'VIC': 'Victoria',
    'QLD': 'Queensland',
    'WA': 'Western Australia',
    'SA': 'South Australia',
    'TAS': 'Tasmania',
    'NT': 'Northern Territory',
    'ACT': 'Australian Capital Territory',
}

# Default region for scraping
DEFAULT_REGION = 'NSW'

# Output configuration
OUTPUT_DIR = 'output'
CSV_OUTPUT_FILE = 'eligible_properties.csv'
MAP_OUTPUT_FILE = 'eligible_properties_map.html'

# Map visualization settings
MAP_CONFIG = {
    'center_lat': -33.8688,  # Sydney coordinates (default)
    'center_lng': 151.2093,
    'zoom_start': 10,
    'tile_layer': 'OpenStreetMap',
}

# Rule item colors for map markers
RULE_COLORS = {
    'Item 130': 'blue',       # New pharmacy (1.5km + supermarket/GP)
    'Item 131': 'green',      # New pharmacy (10km rural)
    'Item 132': 'purple',     # New additional pharmacy in town (10km from others)
    'Item 133': 'orange',     # Small shopping centre (15+ tenants)
    'Item 134': 'pink',       # Large shopping centre - no pharmacy (50+ tenants)
    'Item 134A': 'darkblue',  # Large shopping centre - with pharmacy (100+/200+ tenants)
    'Item 135': 'red',        # Large private hospital (150+ patients)
    'Item 136': 'darkgreen',  # Large medical centre (8 FTE prescribers)
    'Ministerial': 'cadetblue',  # Ministerial opportunity (community need)
}

# Ministerial opportunity scoring weights
MINISTERIAL_SCORING = {
    'weight_population': 0.25,
    'weight_distance': 0.20,
    'weight_gp_presence': 0.15,
    'weight_near_miss': 0.20,
    'weight_pharmacy_density': 0.15,
    'weight_growth': 0.05,
    'min_score_threshold': 25,  # Minimum score to qualify as ministerial opportunity
}

# Ministerial near-miss thresholds
MINISTERIAL_NEAR_MISS = {
    'item_130_min_km': 1.0,
    'item_130_max_km': 1.49,
    'item_131_min_km': 7.0,
    'item_131_max_km': 9.99,
    'item_133_min_m': 300,
    'item_133_max_m': 499,
    'item_136_min_m': 200,
    'item_136_max_m': 299,
}

# Logging configuration
LOG_LEVEL = 'INFO'
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

# Validation settings
VALIDATE_ADDRESSES = True  # Ensure addresses are in Australia
REQUIRE_COORDINATES = True  # Skip properties without valid coordinates

# Cache settings
GEOCODE_CACHE_ENABLED = True
CACHE_EXPIRY_DAYS = 30

def validate_config():
    """
    Validate that required configuration is present.
    Returns list of missing configuration items.
    """
    # No required API keys - using free Nominatim for geocoding
    missing = []

    # All API keys are optional
    optional_missing = []
    if not GOOGLE_MAPS_API_KEY:
        optional_missing.append('GOOGLE_MAPS_API_KEY (using free Nominatim instead)')
    if not HEALTHDIRECT_API_KEY:
        optional_missing.append('HEALTHDIRECT_API_KEY')
    if not WOOLWORTHS_API_KEY:
        optional_missing.append('WOOLWORTHS_API_KEY')
    if not MYHOSPITALS_API_KEY:
        optional_missing.append('MYHOSPITALS_API_KEY')

    return missing, optional_missing


if __name__ == '__main__':
    # Test configuration
    missing, optional = validate_config()

    if missing:
        print("ERROR: Missing required configuration:")
        for item in missing:
            print(f"  - {item}")
    else:
        print("All required configuration present. Using free Nominatim for geocoding!")

    if optional:
        print("\nOptional API keys not configured:")
        for item in optional:
            print(f"  - {item}")

    print(f"\nDatabase path: {DATABASE_PATH}")
    print(f"Output directory: {OUTPUT_DIR}")
    print(f"Default region: {DEFAULT_REGION}")
