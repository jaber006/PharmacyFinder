"""
Data models for the Watchlist system (Hawk Mode).
"""
from dataclasses import dataclass, field
from datetime import date
from typing import Optional

# Valid enum-like values for schema validation
WATCH_REASONS = frozenset([
    "near_miss",
    "lease_expiring",
    "gp_growth",
    "pharmacy_closure",
    "new_development",
])

CHECK_FREQUENCIES = frozenset(["daily", "weekly", "monthly"])

ITEM_STATUSES = frozenset(["watching", "triggered", "expired"])

ALERT_TYPES = frozenset([
    "new_qualification",
    "lost_qualification",
    "property_available",
    "competitor_application",
])

ALERT_SEVERITIES = frozenset(["low", "medium", "high"])


@dataclass
class WatchlistItem:
    """A candidate premises being watched for qualification changes."""
    candidate_id: str
    watch_reason: str  # near_miss, lease_expiring, gp_growth, pharmacy_closure, new_development
    trigger_condition: str  # Text description of what change would make this qualify
    check_frequency: str  # daily, weekly, monthly
    last_checked: date
    status: str  # watching, triggered, expired
    created_date: date
    notes: str = ""

    id: Optional[int] = None  # DB primary key when loaded

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "candidate_id": self.candidate_id,
            "watch_reason": self.watch_reason,
            "trigger_condition": self.trigger_condition,
            "check_frequency": self.check_frequency,
            "last_checked": self.last_checked.isoformat() if self.last_checked else None,
            "status": self.status,
            "created_date": self.created_date.isoformat() if self.created_date else None,
            "notes": self.notes,
        }


@dataclass
class WatchlistAlert:
    """An alert generated when a watchlist trigger condition is met."""
    item_id: int
    alert_type: str  # new_qualification, lost_qualification, property_available, competitor_application
    message: str
    severity: str  # low, medium, high
    triggered_date: date
    acknowledged: bool = False

    id: Optional[int] = None  # DB primary key when loaded

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "item_id": self.item_id,
            "alert_type": self.alert_type,
            "message": self.message,
            "severity": self.severity,
            "triggered_date": self.triggered_date.isoformat() if self.triggered_date else None,
            "acknowledged": self.acknowledged,
        }
