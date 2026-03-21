"""
Distance map generator using Folium/Leaflet.

Generates interactive HTML maps showing candidate site and nearby pharmacies
with straight-line distance markers, colour-coded by compliance status.

Colours:
  - Green:  comfortably passes distance threshold
  - Orange: borderline (within 10% margin above threshold)
  - Red:    fails distance threshold (too close)
"""
from __future__ import annotations

import math
import os
from typing import List, Tuple, Optional

import folium
from folium import plugins
from geopy.distance import geodesic


# Default distance thresholds per rule item (metres)
RULE_THRESHOLDS_M = {
    "Item 130": 1500,
    "Item 131": 10000,  # road distance, but we show straight-line for reference
    "Item 132": 200,     # nearest; 10km road for others
    "Item 133": 500,
    "Item 134": 0,       # no distance requirement
    "Item 134A": 0,
    "Item 135": 0,
    "Item 136": 300,
}


def _distance_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Geodesic distance in metres."""
    return geodesic((lat1, lon1), (lat2, lon2)).meters


def _colour_for_distance(distance_m: float, threshold_m: float) -> str:
    """Return colour based on how distance compares to threshold."""
    if threshold_m <= 0:
        return "green"
    margin = distance_m - threshold_m
    margin_pct = margin / threshold_m if threshold_m > 0 else 1.0
    if margin < 0:
        return "red"
    elif margin_pct < 0.10:
        return "orange"
    else:
        return "green"


def _midpoint(lat1: float, lon1: float, lat2: float, lon2: float) -> Tuple[float, float]:
    """Simple midpoint for label placement."""
    return ((lat1 + lat2) / 2, (lon1 + lon2) / 2)


def generate_distance_map(
    candidate_lat: float,
    candidate_lon: float,
    pharmacies: List[dict],
    radius_km: float = 5.0,
    rule_item: str = "Item 130",
    candidate_name: str = "Candidate Site",
    output_path: Optional[str] = None,
) -> str:
    """
    Generate an interactive HTML map showing the candidate and nearby pharmacies.

    Parameters
    ----------
    candidate_lat, candidate_lon : float
        Candidate site coordinates.
    pharmacies : list of dict
        Each dict must have: name, latitude, longitude.
        Optionally: address, id.
    radius_km : float
        Only show pharmacies within this radius.
    rule_item : str
        Rule item to determine distance threshold colouring.
    candidate_name : str
        Display name for the candidate marker.
    output_path : str or None
        If provided, save HTML to this path.

    Returns
    -------
    str
        Path to the generated HTML file.
    """
    threshold_m = RULE_THRESHOLDS_M.get(rule_item, 1500)

    # Filter pharmacies within radius
    nearby = []
    for p in pharmacies:
        dist = _distance_m(candidate_lat, candidate_lon, p["latitude"], p["longitude"])
        if dist <= radius_km * 1000:
            nearby.append({**p, "_distance_m": dist})

    # Sort by distance
    nearby.sort(key=lambda x: x["_distance_m"])

    # Create map centred on candidate
    m = folium.Map(
        location=[candidate_lat, candidate_lon],
        zoom_start=13 if radius_km <= 5 else 11,
        tiles="OpenStreetMap",
    )

    # Add candidate marker (star icon)
    folium.Marker(
        location=[candidate_lat, candidate_lon],
        popup=folium.Popup(
            f"<b>{candidate_name}</b><br>"
            f"Rule: {rule_item}<br>"
            f"Threshold: {threshold_m}m",
            max_width=300,
        ),
        tooltip=candidate_name,
        icon=folium.Icon(color="blue", icon="star", prefix="fa"),
    ).add_to(m)

    # Add threshold circle
    if threshold_m > 0:
        folium.Circle(
            location=[candidate_lat, candidate_lon],
            radius=threshold_m,
            color="blue",
            fill=False,
            dash_array="10",
            weight=2,
            opacity=0.6,
            tooltip=f"{rule_item} threshold: {threshold_m}m",
        ).add_to(m)

    # Add radius circle
    folium.Circle(
        location=[candidate_lat, candidate_lon],
        radius=radius_km * 1000,
        color="gray",
        fill=False,
        dash_array="5",
        weight=1,
        opacity=0.4,
        tooltip=f"Search radius: {radius_km}km",
    ).add_to(m)

    # Add pharmacy markers and distance lines
    for i, p in enumerate(nearby):
        dist_m = p["_distance_m"]
        dist_km = dist_m / 1000
        colour = _colour_for_distance(dist_m, threshold_m)
        p_name = p.get("name", f"Pharmacy {i+1}")
        p_addr = p.get("address", "")

        status = "PASS" if colour != "red" else "FAIL"
        if colour == "orange":
            status = "BORDERLINE"

        # Marker icon colours
        icon_color = {"green": "green", "orange": "orange", "red": "red"}[colour]

        # Pharmacy marker
        folium.Marker(
            location=[p["latitude"], p["longitude"]],
            popup=folium.Popup(
                f"<b>{p_name}</b><br>"
                f"{p_addr}<br>"
                f"Distance: {dist_m:.0f}m ({dist_km:.3f}km)<br>"
                f"Status: <b>{status}</b>",
                max_width=300,
            ),
            tooltip=f"{p_name} ({dist_m:.0f}m)",
            icon=folium.Icon(color=icon_color, icon="plus", prefix="fa"),
        ).add_to(m)

        # Distance line
        folium.PolyLine(
            locations=[
                [candidate_lat, candidate_lon],
                [p["latitude"], p["longitude"]],
            ],
            color=colour,
            weight=2,
            opacity=0.7,
            dash_array="5" if colour == "orange" else None,
            tooltip=f"{dist_m:.0f}m to {p_name}",
        ).add_to(m)

        # Distance label at midpoint
        mid_lat, mid_lon = _midpoint(
            candidate_lat, candidate_lon,
            p["latitude"], p["longitude"]
        )
        folium.Marker(
            location=[mid_lat, mid_lon],
            icon=folium.DivIcon(
                html=f'<div style="font-size:11px;font-weight:bold;'
                     f'color:{colour};background:white;padding:1px 4px;'
                     f'border:1px solid {colour};border-radius:3px;'
                     f'white-space:nowrap;">'
                     f'{dist_m:.0f}m</div>',
                icon_size=(60, 20),
                icon_anchor=(30, 10),
            ),
        ).add_to(m)

    # Legend
    legend_html = f"""
    <div style="position:fixed;bottom:30px;left:30px;z-index:1000;
         background:white;padding:10px 15px;border:2px solid #333;
         border-radius:5px;font-size:13px;box-shadow:2px 2px 6px rgba(0,0,0,0.3);">
        <b>{rule_item} Distance Map</b><br>
        <span style="color:green">● Green</span> — Passes ({'>'}10% margin)<br>
        <span style="color:orange">● Orange</span> — Borderline ({'<'}10% margin)<br>
        <span style="color:red">● Red</span> — Fails (too close)<br>
        <span style="color:blue">◯ Blue circle</span> — {threshold_m}m threshold<br>
        Pharmacies shown: {len(nearby)} within {radius_km}km
    </div>
    """
    m.get_root().html.add_child(folium.Element(legend_html))

    # Title
    title_html = f"""
    <div style="position:fixed;top:10px;left:50%;transform:translateX(-50%);
         z-index:1000;background:white;padding:8px 20px;border:2px solid #333;
         border-radius:5px;font-size:16px;font-weight:bold;
         box-shadow:2px 2px 6px rgba(0,0,0,0.3);">
        {candidate_name} — {rule_item} Distance Analysis
    </div>
    """
    m.get_root().html.add_child(folium.Element(title_html))

    # Determine output path
    if output_path is None:
        safe_name = "".join(c if c.isalnum() or c in "-_ " else "_" for c in candidate_name)
        output_path = os.path.join("output", "maps", f"{safe_name}_{rule_item.replace(' ', '_')}.html")

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    m.save(output_path)
    return output_path
