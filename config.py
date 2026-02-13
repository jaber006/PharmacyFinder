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
    'item_130': 1.5,      # New pharmacy - at least 1.5km from nearest pharmacy
    'item_130_supermarket': 0.5,  # Item 130 - supermarket/GP must be within 500m
    'item_131': 10.0,     # New pharmacy (rural) - at least 10km from nearest pharmacy
    'item_134a': 90.0,    # Very remote location - 90km from nearest pharmacy
}

# FTE (Full-Time Equivalent) thresholds
FTE_REQUIREMENTS = {
    'item_130_gp': 1.0,   # Item 130 option (i): 1 FTE GP within 500m (with 1,000m² supermarket)
    'item_132_gp': 4.0,   # Item 132: 4 FTE GPs in same town
    'item_136_prescribers': 8.0,   # Item 136: 8 FTE PBS prescribers total
    'item_136_medical': 7.0,       # Item 136: of which 7 must be medical practitioners
}

# Hours per week calculation
HOURS_PER_WEEK_FULL_TIME = 38.0
MINIMUM_HOURS_FOR_GP = 20.0

# Shopping centre and supermarket thresholds (in square meters)
GLA_THRESHOLDS = {
    'major_centre': 15000,     # Item 132 - Major shopping centre >= 15,000 sqm
    'small_centre_min': 5000,  # Item 134 - Small centre >= 5,000 sqm
    'small_centre_max': 15000, # Item 134 - Small centre <= 15,000 sqm
}

FLOOR_AREA_THRESHOLDS = {
    'supermarket': 1000,  # Item 133 - Supermarket >= 1,000 sqm
}

# Hospital requirements
HOSPITAL_BED_COUNT = 100  # Item 135 - Minimum 100 beds

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
    'Item 130': 'blue',       # 4km remote
    'Item 131': 'green',      # GP proximity
    'Item 132': 'purple',     # Major shopping centre
    'Item 133': 'orange',     # Supermarket
    'Item 134': 'pink',       # Small shopping centre
    'Item 134A': 'darkblue',  # 90km very remote
    'Item 135': 'red',        # Hospital
    'Item 136': 'darkgreen',  # Medical centre
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
