"""
Pharmacy Profitability Estimator.

Predicts revenue potential for each qualifying v2_results site based on:
- Population within 5km (from population_grid or opportunities fallback)
- Competition (existing pharmacies within 5km)
- GP proximity (script generators within 2km)
- National benchmark: ~73,000 scripts per pharmacy per year
"""
import math
import os
import sqlite3
from typing import Any, Dict, List, Optional, Tuple

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(PROJECT_ROOT, "pharmacy_finder.db")

# Constants
NATIONAL_SCRIPTS_PER_PHARMACY = 73_000
SCRIPTS_PER_CAPITA_YEAR = 16.0  # ~73k / (26M pop / 5800 pharmacies)
PBS_DISPENSING_FEE = 8.50
PBS_REVENUE_SHARE = 0.65  # PBS is 65%, front-of-shop 35%
GP_MARGIN = 0.33
FITOUT_MIN, FITOUT_MAX = 250_000, 400_000
STOCK_MIN, STOCK_MAX = 150_000, 250_000
WORKING_CAPITAL_MIN, WORKING_CAPITAL_MAX = 50_000, 100_000
FLOOR_AREA_DEFAULT_SQM = 100
FITOUT_PER_SQM = 3000  # $2500-4000 per sqm
GOODWILL_MULTIPLE = 0.4  # 12-month exit value = revenue * 0.4


def _population_5km(lat: float, lon: float, conn: sqlite3.Connection) -> int:
    """Get population within 5km. Tries population_grid, falls back to opportunities."""
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='population_grid'")
        if cur.fetchone()[0] > 0:
            from geopy.distance import geodesic
            lat_margin = 5.0 / 111.0
            lon_margin = 5.0 / (111.0 * max(math.cos(math.radians(lat)), 0.01))
            rows = cur.execute(
                "SELECT population, lat, lon FROM population_grid WHERE population IS NOT NULL "
                "AND lat BETWEEN ? AND ? AND lon BETWEEN ? AND ?",
                (lat - lat_margin, lat + lat_margin, lon - lon_margin, lon + lon_margin),
            ).fetchall()
            total = 0
            for pop, sa2_lat, sa2_lon in rows:
                if geodesic((lat, lon), (sa2_lat, sa2_lon)).kilometers <= 5.0:
                    total += pop or 0
            return total
    except Exception:
        pass

    # Fallback: nearest opportunity's pop_5km (bounding box)
    try:
        margin = 0.1
        cur = conn.cursor()
        cur.execute(
            "SELECT pop_5km, latitude, longitude FROM opportunities "
            "WHERE pop_5km IS NOT NULL AND latitude IS NOT NULL AND longitude IS NOT NULL "
            "AND latitude BETWEEN ? AND ? AND longitude BETWEEN ? AND ? "
            "ORDER BY ABS(latitude - ?) + ABS(longitude - ?) LIMIT 5",
            (lat - margin, lat + margin, lon - margin, lon + margin, lat, lon),
        )
        for row in cur.fetchall():
            if row[0]:
                return int(row[0])
    except Exception:
        pass

    return 0


def _pharmacies_within_5km(lat: float, lon: float, conn: sqlite3.Connection) -> int:
    """Count pharmacies within 5km (excluding self - we're evaluating a new site)."""
    try:
        from geopy.distance import geodesic
        cur = conn.cursor()
        lat_margin = 5.0 / 111.0
        lon_margin = 5.0 / (111.0 * max(math.cos(math.radians(lat)), 0.01))
        rows = cur.execute(
            "SELECT latitude, longitude FROM pharmacies "
            "WHERE latitude IS NOT NULL AND longitude IS NOT NULL "
            "AND latitude BETWEEN ? AND ? AND longitude BETWEEN ? AND ?",
            (lat - lat_margin, lat + lat_margin, lon - lon_margin, lon + lon_margin),
        ).fetchall()
        count = 0
        for row in rows:
            if geodesic((lat, lon), (row[0], row[1])).kilometers <= 5.0:
                count += 1
        return count
    except Exception:
        return 0


def _gps_within_2km(lat: float, lon: float, conn: sqlite3.Connection) -> int:
    """Count GP practices within 2km."""
    try:
        from geopy.distance import geodesic
        cur = conn.cursor()
        lat_margin = 2.5 / 111.0
        lon_margin = 2.5 / (111.0 * max(math.cos(math.radians(lat)), 0.01))
        rows = cur.execute(
            "SELECT latitude, longitude FROM gps "
            "WHERE latitude IS NOT NULL AND longitude IS NOT NULL "
            "AND latitude BETWEEN ? AND ? AND longitude BETWEEN ? AND ?",
            (lat - lat_margin, lat + lat_margin, lon - lon_margin, lon + lon_margin),
        ).fetchall()
        count = 0
        for row in rows:
            if geodesic((lat, lon), (row[0], row[1])).kilometers <= 2.0:
                count += 1
        return count
    except Exception:
        return 0


def estimate_scripts(
    population_5km: int,
    pharmacies_5km: int,
    gps_2km: int,
) -> float:
    """
    Estimate annual PBS script volume for a new pharmacy.
    Formula: (population_5km / (pharmacies_5km + 1)) * scripts_per_capita * gp_proximity_boost
    """
    pharmacies_total = max(pharmacies_5km + 1, 1)
    pop_per_pharmacy = population_5km / pharmacies_total
    base_scripts = pop_per_pharmacy * SCRIPTS_PER_CAPITA_YEAR
    gp_boost = 1.0 + 0.08 * min(gps_2km, 10)
    return max(0, base_scripts * gp_boost)


def estimate_revenue(annual_scripts: float) -> float:
    """PBS revenue + front-of-shop. PBS is 65%, so total = (scripts * fee) / 0.65."""
    pbs_revenue = annual_scripts * PBS_DISPENSING_FEE
    return pbs_revenue / PBS_REVENUE_SHARE


def estimate_gp(revenue: float) -> float:
    """Gross profit at 33% margin."""
    return revenue * GP_MARGIN


def estimate_setup_costs(floor_area_sqm: Optional[float] = None) -> Tuple[float, float, float, float]:
    """Returns (fitout, stock, working_capital, total)."""
    area = floor_area_sqm or FLOOR_AREA_DEFAULT_SQM
    fitout = min(FITOUT_MAX, max(FITOUT_MIN, area * (FITOUT_PER_SQM * 0.8)))
    stock = (STOCK_MIN + STOCK_MAX) / 2
    working_capital = (WORKING_CAPITAL_MIN + WORKING_CAPITAL_MAX) / 2
    return (fitout, stock, working_capital, fitout + stock + working_capital)


def calculate_roi(
    annual_gp: float,
    setup_cost: float,
    annual_revenue: float,
) -> Tuple[float, float, float]:
    """Returns (payback_years, exit_value_12mo, flip_profit)."""
    payback = setup_cost / annual_gp if annual_gp > 0 else 999.0
    exit_value = annual_revenue * GOODWILL_MULTIPLE
    flip_profit = exit_value - setup_cost
    return (payback, exit_value, flip_profit)


def profitability_score(
    annual_revenue: float,
    annual_gp: float,
    payback_years: float,
    flip_profit: float,
) -> float:
    """Score 0-100 combining revenue, GP, payback, flip potential."""
    if annual_gp <= 0:
        return 0.0
    rev_score = min(100, annual_revenue / 2_000_000 * 30)
    gp_score = min(100, annual_gp / 500_000 * 25)
    payback_score = max(0, 25 - payback_years * 5)
    flip_score = max(0, min(25, flip_profit / 200_000 * 25))
    return round(min(100, rev_score + gp_score + payback_score + flip_score), 1)


def analyze_site(site: Dict[str, Any], conn: sqlite3.Connection) -> Dict[str, Any]:
    """Full profitability analysis for a single v2_results site."""
    lat = site.get("latitude")
    lon = site.get("longitude")
    if lat is None or lon is None:
        return {**site, "profitability_score": 0, "error": "No coordinates"}

    pop_5km = _population_5km(lat, lon, conn)
    pharm_5km = _pharmacies_within_5km(lat, lon, conn)
    gps_2km = _gps_within_2km(lat, lon, conn)

    annual_scripts = estimate_scripts(pop_5km, pharm_5km, gps_2km)
    annual_revenue = estimate_revenue(annual_scripts)
    annual_gp = estimate_gp(annual_revenue)
    fitout, stock, working_capital, setup_cost = estimate_setup_costs()
    payback, exit_value, flip_profit = calculate_roi(annual_gp, setup_cost, annual_revenue)
    score = profitability_score(annual_revenue, annual_gp, payback, flip_profit)

    return {
        **site,
        "population_5km": pop_5km,
        "pharmacies_5km": pharm_5km,
        "gps_2km": gps_2km,
        "estimated_scripts": round(annual_scripts, 0),
        "annual_revenue": round(annual_revenue, 0),
        "annual_gp": round(annual_gp, 0),
        "setup_cost": round(setup_cost, 0),
        "payback_years": round(payback, 1),
        "exit_value_12mo": round(exit_value, 0),
        "flip_profit": round(flip_profit, 0),
        "profitability_score": score,
    }


def analyze_all_sites(db_path: Optional[str] = None) -> List[Dict[str, Any]]:
    """Analyze all v2_results (passing sites). Returns list of analyzed site dicts."""
    db = db_path or DB_PATH
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute(
        "SELECT * FROM v2_results WHERE passed_any = 1 AND latitude IS NOT NULL AND longitude IS NOT NULL"
    )
    sites = [dict(r) for r in cur.fetchall()]

    results = []
    for s in sites:
        try:
            results.append(analyze_site(s, conn))
        except Exception as e:
            results.append({**s, "profitability_score": 0, "error": str(e)})

    results.sort(key=lambda x: -(x.get("profitability_score") or 0))
    conn.close()
    return results


def update_v2_results_profitability(results: List[Dict], db_path: Optional[str] = None) -> None:
    """Add profitability_score column to v2_results and update each row."""
    db = db_path or DB_PATH
    conn = sqlite3.connect(db)
    cur = conn.cursor()
    try:
        cur.execute("ALTER TABLE v2_results ADD COLUMN profitability_score REAL")
    except sqlite3.OperationalError:
        pass

    for r in results:
        site_id = r.get("id")
        score = r.get("profitability_score")
        if site_id is not None and score is not None:
            cur.execute("UPDATE v2_results SET profitability_score = ? WHERE id = ?", (score, site_id))
    conn.commit()
    conn.close()
