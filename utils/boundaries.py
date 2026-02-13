"""
Rough bounding boxes for Australian states/territories.
Used to validate that scraped data is in the correct region.
"""

# Approximate bounding boxes: (min_lat, max_lat, min_lon, max_lon)
STATE_BOUNDING_BOXES = {
    'NSW': (-37.6, -28.2, 140.9, 153.7),
    'VIC': (-39.2, -33.9, 140.9, 150.0),
    'QLD': (-29.2, -10.0, 138.0, 153.6),
    'SA':  (-38.1, -26.0, 129.0, 141.0),
    'WA':  (-35.2, -13.7, 112.9, 129.0),
    'TAS': (-43.7, -39.5, 143.5, 148.5),
    'NT':  (-26.0, -10.9, 129.0, 138.0),
    'ACT': (-35.9, -35.1, 148.7, 149.4),
}

# Australia-wide box
AUSTRALIA_BOX = (-44.0, -10.0, 112.0, 154.0)


def in_state(lat: float, lon: float, state: str) -> bool:
    """Check if coordinates are within a state's bounding box."""
    box = STATE_BOUNDING_BOXES.get(state)
    if not box:
        return in_australia(lat, lon)
    min_lat, max_lat, min_lon, max_lon = box
    return min_lat <= lat <= max_lat and min_lon <= lon <= max_lon


def in_australia(lat: float, lon: float) -> bool:
    """Check if coordinates are in Australia."""
    min_lat, max_lat, min_lon, max_lon = AUSTRALIA_BOX
    return min_lat <= lat <= max_lat and min_lon <= lon <= max_lon
