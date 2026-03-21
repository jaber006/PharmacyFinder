"""
Watchlist system (Hawk Mode) for PharmacyFinder.

Monitors near-miss sites, pharmacy closures, GP growth, lease expirations,
and other changes that may affect qualification status.
"""
from watchlist.models import WatchlistItem, WatchlistAlert
from watchlist.manager import add_to_watchlist, remove_from_watchlist, get_watchlist, auto_populate_watchlist
from watchlist.alerts import format_alert, get_pending_alerts, acknowledge_alert, alerts_to_json, alerts_to_csv
from watchlist.monitor import (
    compare_scan_results,
    detect_pharmacy_closures,
    detect_new_gp_clinics,
    detect_threshold_crossings,
)

__all__ = [
    "WatchlistItem",
    "WatchlistAlert",
    "add_to_watchlist",
    "remove_from_watchlist",
    "get_watchlist",
    "auto_populate_watchlist",
    "format_alert",
    "get_pending_alerts",
    "acknowledge_alert",
    "alerts_to_json",
    "alerts_to_csv",
    "compare_scan_results",
    "detect_pharmacy_closures",
    "detect_new_gp_clinics",
    "detect_threshold_crossings",
]
