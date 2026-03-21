"""
Analytics routes — pre-computed statistics, heatmap, gaps, competition.
"""
import json
import math
import sqlite3
import os
from typing import Optional

from fastapi import APIRouter, Query

DB_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "pharmacy_finder.db",
)

router = APIRouter(tags=["analytics"])


def _get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _haversine_km(lat1, lon1, lat2, lon2):
    """Great-circle distance between two points in km."""
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2 +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
         math.sin(dlon / 2) ** 2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _state_from_coords(lat, lon):
    """Rough state assignment from coordinates (fallback)."""
    if lat > -10:
        return "NT"
    if lat > -20:
        if lon < 135:
            return "NT"
        return "QLD"
    if lat > -26:
        if lon < 130:
            return "WA" if lon < 125 else "NT"
        return "QLD"
    if lat > -29:
        if lon < 130:
            return "WA" if lon < 129 else "SA"
        if lon > 150:
            return "QLD" if lat < -28 else "NSW"
        return "QLD" if lon > 140 else "SA"
    if lat > -34:
        if lon < 129:
            return "WA"
        if lon > 149:
            return "NSW"
        if lon > 141:
            return "NSW" if lon > 147 else "SA"
        return "SA"
    if lat > -39:
        if lon < 129:
            return "WA"
        if lon > 147:
            return "VIC" if lat > -37.5 else "VIC"
        if lon > 141:
            return "VIC" if lat > -37 else "VIC"
        return "SA" if lon > 134 else "WA"
    if lat > -44:
        if lon > 143.5:
            return "TAS"
        return "VIC"
    return "TAS"


# ─── Australian state populations (2024 ABS estimates) ──────────────────
STATE_POPULATIONS = {
    "NSW": 8_400_000,
    "VIC": 6_800_000,
    "QLD": 5_400_000,
    "WA": 2_900_000,
    "SA": 1_850_000,
    "TAS": 575_000,
    "ACT": 465_000,
    "NT": 250_000,
}


# ─── 1. Overview ────────────────────────────────────────────────────────
@router.get("/analytics/overview")
async def analytics_overview():
    """National summary: totals by rule & state, avg scores, top 10, data freshness."""
    conn = _get_db()
    cur = conn.cursor()

    # By rule item
    cur.execute(
        "SELECT primary_rule, COUNT(*) as cnt "
        "FROM v2_results WHERE passed_any = 1 "
        "GROUP BY primary_rule ORDER BY cnt DESC"
    )
    by_rule = {row["primary_rule"]: row["cnt"] for row in cur.fetchall()}

    # By state
    cur.execute(
        "SELECT state, COUNT(*) as cnt "
        "FROM v2_results WHERE passed_any = 1 "
        "GROUP BY state ORDER BY cnt DESC"
    )
    by_state = {row["state"]: row["cnt"] for row in cur.fetchall()}

    # Avg commercial score by state
    cur.execute(
        "SELECT state, AVG(commercial_score) as avg_score "
        "FROM v2_results WHERE passed_any = 1 "
        "GROUP BY state ORDER BY avg_score DESC"
    )
    avg_score_by_state = {
        row["state"]: round(row["avg_score"] or 0, 4)
        for row in cur.fetchall()
    }

    # Top 10 nationally
    cur.execute(
        "SELECT id, name, address, latitude, longitude, state, "
        "primary_rule, commercial_score, best_confidence "
        "FROM v2_results WHERE passed_any = 1 "
        "ORDER BY commercial_score DESC LIMIT 10"
    )
    top_10 = [
        {
            "id": row["id"],
            "name": row["name"],
            "address": row["address"],
            "latitude": row["latitude"],
            "longitude": row["longitude"],
            "state": row["state"],
            "primary_rule": row["primary_rule"],
            "commercial_score": row["commercial_score"],
            "best_confidence": row["best_confidence"],
        }
        for row in cur.fetchall()
    ]

    # Data freshness — last scan dates per key table
    freshness = {}
    scan_tables = {
        "v2_results": "date_evaluated",
        "pharmacies": "date_scraped",
        "supermarkets": "date_scraped",
    }
    for table, date_col in scan_tables.items():
        try:
            cur.execute(f"SELECT MAX({date_col}) as last_date FROM [{table}]")
            row = cur.fetchone()
            freshness[table] = row["last_date"] if row else None
        except Exception:
            freshness[table] = None

    # Also count rows for context
    for table in ["pharmacies", "supermarkets", "medical_centres", "hospitals", "shopping_centres"]:
        try:
            cur.execute(f"SELECT COUNT(*) as cnt FROM [{table}]")
            freshness[f"{table}_count"] = cur.fetchone()["cnt"]
        except Exception:
            freshness[f"{table}_count"] = 0

    total = sum(by_state.values())

    conn.close()
    return {
        "total_qualifying": total,
        "by_rule": by_rule,
        "by_state": by_state,
        "avg_score_by_state": avg_score_by_state,
        "top_10": top_10,
        "data_freshness": freshness,
    }


# ─── 2. Heatmap ────────────────────────────────────────────────────────
@router.get("/analytics/heatmap")
async def analytics_heatmap():
    """
    Population-weighted opportunity density as GeoJSON.
    Uses supermarket proximity within 5km as a population proxy.
    """
    conn = _get_db()
    cur = conn.cursor()

    # Get qualifying sites
    cur.execute(
        "SELECT id, name, latitude, longitude, state, primary_rule, commercial_score "
        "FROM v2_results WHERE passed_any = 1"
    )
    sites = [dict(row) for row in cur.fetchall()]

    # Get supermarkets
    cur.execute("SELECT latitude, longitude FROM supermarkets")
    supermarkets = [(row["latitude"], row["longitude"]) for row in cur.fetchall()]

    conn.close()

    features = []
    for site in sites:
        # Count supermarkets within 5km as population proxy
        nearby_count = 0
        for s_lat, s_lon in supermarkets:
            dist = _haversine_km(site["latitude"], site["longitude"], s_lat, s_lon)
            if dist <= 5.0:
                nearby_count += 1

        # Density value: combines supermarket proximity with commercial score
        # Each nearby supermarket ~ 5,000-15,000 catchment population
        est_population = nearby_count * 10000
        density = round(nearby_count * (site["commercial_score"] or 0.1), 4)

        features.append({
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [site["longitude"], site["latitude"]],
            },
            "properties": {
                "id": site["id"],
                "name": site["name"],
                "state": site["state"],
                "rule": site["primary_rule"],
                "commercial_score": site["commercial_score"],
                "nearby_supermarkets": nearby_count,
                "est_population_5km": est_population,
                "density": density,
            },
        })

    return {
        "type": "FeatureCollection",
        "features": features,
    }


# ─── 3. Gaps (Pharmacy Deserts) ─────────────────────────────────────────
@router.get("/analytics/gaps")
async def analytics_gaps(
    min_gap_km: float = Query(10.0, description="Minimum gap in km for pharmacy desert"),
    min_population: int = Query(2000, description="Min population estimate for town gaps"),
):
    """
    Pharmacy desert analysis:
    - Sites where nearest pharmacy is >min_gap_km (Item 131 candidates)
    - SAL areas with population proxy >min_population but no nearby pharmacy
    """
    conn = _get_db()
    cur = conn.cursor()

    # --- Part 1: Qualifying sites with large pharmacy gaps ---
    cur.execute(
        "SELECT id, name, address, latitude, longitude, state, "
        "primary_rule, commercial_score, all_rules_json "
        "FROM v2_results WHERE passed_any = 1"
    )
    sites = [dict(row) for row in cur.fetchall()]

    # Get all pharmacy locations
    cur.execute("SELECT id, name, latitude, longitude, state FROM pharmacies")
    pharmacies = [dict(row) for row in cur.fetchall()]

    conn.close()

    # Pre-sort pharmacies for fast spatial filtering
    # ~1 degree latitude ≈ 111km, so min_gap_km / 111 is the rough degree threshold
    degree_threshold = min_gap_km / 111.0 * 0.7  # conservative filter

    desert_features = []
    town_gap_features = []

    for site in sites:
        # Find nearest pharmacy distance - use bounding box pre-filter
        min_dist = float("inf")
        nearest_name = ""
        slat, slon = site["latitude"], site["longitude"]
        for ph in pharmacies:
            # Quick lat/lon pre-filter to skip distant pharmacies
            if abs(ph["latitude"] - slat) > degree_threshold * 2:
                continue
            if abs(ph["longitude"] - slon) > degree_threshold * 2:
                continue
            dist = _haversine_km(slat, slon, ph["latitude"], ph["longitude"])
            if dist < min_dist:
                min_dist = dist
                nearest_name = ph["name"]
                if dist < min_gap_km:
                    break  # Early exit - not a desert

        # If no pharmacy found in bounding box, do full scan
        if min_dist == float("inf"):
            for ph in pharmacies:
                dist = _haversine_km(slat, slon, ph["latitude"], ph["longitude"])
                if dist < min_dist:
                    min_dist = dist
                    nearest_name = ph["name"]

        if min_dist >= min_gap_km and min_dist < 99999:
            gap_km = round(min_dist, 2)
            est_road_km = round(min_dist * 1.4, 2)  # rough road factor

            desert_features.append({
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [site["longitude"], site["latitude"]],
                },
                "properties": {
                    "id": site["id"],
                    "name": site["name"],
                    "address": site["address"],
                    "state": site["state"],
                    "rule": site["primary_rule"],
                    "gap_km_geodesic": gap_km,
                    "gap_km_est_road": est_road_km,
                    "nearest_pharmacy": nearest_name,
                    "commercial_score": site["commercial_score"],
                    "type": "pharmacy_desert",
                },
            })

    # --- Part 2: Towns/areas with supermarkets but no pharmacy ---
    # Use supermarkets as proxy for populated areas
    conn2 = _get_db()
    cur2 = conn2.cursor()
    cur2.execute("SELECT id, name, latitude, longitude FROM supermarkets")
    supermarkets = [dict(row) for row in cur2.fetchall()]
    conn2.close()

    for sm in supermarkets:
        # Find nearest pharmacy with spatial pre-filter
        min_dist = float("inf")
        nearest_name = ""
        slat, slon = sm["latitude"], sm["longitude"]
        for ph in pharmacies:
            if abs(ph["latitude"] - slat) > degree_threshold * 2:
                continue
            if abs(ph["longitude"] - slon) > degree_threshold * 2:
                continue
            dist = _haversine_km(slat, slon, ph["latitude"], ph["longitude"])
            if dist < min_dist:
                min_dist = dist
                nearest_name = ph["name"]
                if dist < min_gap_km:
                    break

        # Full scan fallback
        if min_dist == float("inf"):
            for ph in pharmacies:
                dist = _haversine_km(slat, slon, ph["latitude"], ph["longitude"])
                if dist < min_dist:
                    min_dist = dist
                    nearest_name = ph["name"]

        # Supermarket present + no pharmacy nearby = potential gap
        if min_dist >= min_gap_km and min_dist < 99999:
            # Supermarket implies population ~5000+
            est_pop = 5000 if min_dist < 20 else 3000
            town_gap_features.append({
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [sm["longitude"], sm["latitude"]],
                },
                "properties": {
                    "name": sm["name"],
                    "gap_km": round(min_dist, 2),
                    "nearest_pharmacy": nearest_name,
                    "est_population": est_pop,
                    "type": "town_gap",
                },
            })

    return {
        "pharmacy_deserts": {
            "type": "FeatureCollection",
            "features": desert_features,
            "count": len(desert_features),
        },
        "town_gaps": {
            "type": "FeatureCollection",
            "features": town_gap_features,
            "count": len(town_gap_features),
        },
    }


# ─── 4. Competition ─────────────────────────────────────────────────────
@router.get("/analytics/competition")
async def analytics_competition():
    """
    Competitive landscape:
    - Pharmacy density per state
    - Average inter-pharmacy distance per state
    - Opportunity ranking (high pop, low density)
    """
    conn = _get_db()
    cur = conn.cursor()

    # Get pharmacies grouped by state
    cur.execute(
        "SELECT id, name, latitude, longitude, state FROM pharmacies WHERE state IS NOT NULL"
    )
    all_pharmacies = [dict(row) for row in cur.fetchall()]

    # Get qualifying site counts by state
    cur.execute(
        "SELECT state, COUNT(*) as cnt "
        "FROM v2_results WHERE passed_any = 1 "
        "GROUP BY state"
    )
    qualifying_by_state = {row["state"]: row["cnt"] for row in cur.fetchall()}

    conn.close()

    # Group pharmacies by state
    state_pharmacies = {}
    for ph in all_pharmacies:
        st = ph["state"]
        if st:
            state_pharmacies.setdefault(st, []).append(ph)

    # For pharmacies without explicit state, infer from coordinates
    cur2 = _get_db()
    cur2_c = cur2.cursor()
    cur2_c.execute(
        "SELECT id, name, latitude, longitude, address FROM pharmacies WHERE state IS NULL OR state = ''"
    )
    unassigned = [dict(row) for row in cur2_c.fetchall()]
    cur2.close()

    for ph in unassigned:
        addr = ph.get("address", "") or ""
        st = None
        # Try to extract state from address
        for state_code in STATE_POPULATIONS:
            if f", {state_code}," in addr or addr.endswith(f", {state_code}") or f" {state_code} " in addr:
                st = state_code
                break
        if not st and ph["latitude"] and ph["longitude"]:
            st = _state_from_coords(ph["latitude"], ph["longitude"])
        if st:
            state_pharmacies.setdefault(st, []).append(ph)

    results = []
    for state_code, pop in STATE_POPULATIONS.items():
        pharms = state_pharmacies.get(state_code, [])
        count = len(pharms)
        density_per_10k = round(count / (pop / 10000), 2) if pop > 0 else 0

        # Average distance between pharmacies (sample for performance)
        avg_dist = 0.0
        if count >= 2:
            # Sample up to 200 pairs for avg inter-pharmacy distance
            import random
            sample_size = min(count, 200)
            sample = random.sample(pharms, sample_size) if count > 200 else pharms
            total_dist = 0
            pair_count = 0
            for i in range(len(sample)):
                for j in range(i + 1, min(i + 10, len(sample))):
                    d = _haversine_km(
                        sample[i]["latitude"], sample[i]["longitude"],
                        sample[j]["latitude"], sample[j]["longitude"],
                    )
                    total_dist += d
                    pair_count += 1
            avg_dist = round(total_dist / pair_count, 2) if pair_count > 0 else 0

        qualifying = qualifying_by_state.get(state_code, 0)

        # Opportunity score: higher = more opportunity
        # Factor in: population, inverse density, qualifying sites
        opportunity_score = 0
        if density_per_10k > 0:
            opportunity_score = round(
                (pop / 1_000_000) * (1 / density_per_10k) * (1 + qualifying * 0.1), 4
            )

        results.append({
            "state": state_code,
            "population": pop,
            "pharmacy_count": count,
            "density_per_10k": density_per_10k,
            "avg_inter_pharmacy_km": avg_dist,
            "qualifying_sites": qualifying,
            "opportunity_score": opportunity_score,
        })

    # Sort by opportunity score desc
    results.sort(key=lambda x: x["opportunity_score"], reverse=True)

    return {
        "states": results,
        "total_pharmacies": len(all_pharmacies) + len(unassigned),
        "total_population": sum(STATE_POPULATIONS.values()),
        "national_density_per_10k": round(
            (len(all_pharmacies) + len(unassigned)) / (sum(STATE_POPULATIONS.values()) / 10000), 2
        ),
    }
