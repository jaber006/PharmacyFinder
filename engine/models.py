"""
Data models for the V2 rules engine.
"""
from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class Candidate:
    """A candidate premises for pharmacy approval evaluation."""
    id: str
    latitude: float
    longitude: float
    name: str = ""
    address: str = ""
    source_type: str = ""          # supermarket, medical_centre, shopping_centre, hospital, gap
    source_id: Optional[int] = None
    state: str = ""

    # General requirement flags — default to True (assume ok for Stage A screening)
    # These get set to False when we have evidence they fail
    zoning_ok: bool = True
    legal_right_to_occupy: bool = True
    accessible_to_public: bool = True
    can_open_within_6_months: bool = True
    direct_access_from_supermarket: bool = False  # Must NOT be true for new-pharmacy items

    # Complex classification
    complex_type: Optional[str] = None   # None, small_sc, large_sc, private_hospital, large_medical
    town_id: Optional[str] = None

    # Extra data from POI
    bed_count: Optional[int] = None      # For hospitals
    num_gps: Optional[int] = None        # For medical centres
    total_fte: Optional[float] = None
    hours_per_week: Optional[float] = None
    gla_sqm: Optional[float] = None      # For shopping centres
    estimated_tenants: Optional[int] = None
    centre_class: Optional[str] = None

    # Population data (from opportunities table or computed)
    pop_10km: int = 0
    growth_indicator: str = ""


@dataclass
class RuleResult:
    """Result of evaluating a single rule item against a candidate."""
    item: str                          # e.g. "Item 130"
    passed: bool
    reasons: List[str] = field(default_factory=list)
    evidence_needed: List[str] = field(default_factory=list)
    confidence: float = 0.0            # 0.0–1.0
    distances: dict = field(default_factory=dict)  # key measurements for audit trail
    
    def to_dict(self) -> dict:
        return {
            "item": self.item,
            "passed": self.passed,
            "reasons": self.reasons,
            "evidence_needed": self.evidence_needed,
            "confidence": self.confidence,
            "distances": self.distances,
        }


@dataclass
class EvaluationResult:
    """Full evaluation of a candidate across all rules."""
    candidate: Candidate
    rule_results: List[RuleResult] = field(default_factory=list)
    passed_any: bool = False
    commercial_score: float = 0.0
    primary_rule: str = ""             # Highest-confidence passing rule
    
    @property
    def passing_rules(self) -> List[RuleResult]:
        return [r for r in self.rule_results if r.passed]
    
    @property
    def best_confidence(self) -> float:
        passing = self.passing_rules
        return max((r.confidence for r in passing), default=0.0)
    
    def to_dict(self) -> dict:
        return {
            "id": self.candidate.id,
            "name": self.candidate.name,
            "address": self.candidate.address,
            "latitude": self.candidate.latitude,
            "longitude": self.candidate.longitude,
            "state": self.candidate.state,
            "source_type": self.candidate.source_type,
            "passed_any": self.passed_any,
            "primary_rule": self.primary_rule,
            "commercial_score": round(self.commercial_score, 3),
            "best_confidence": round(self.best_confidence, 3),
            "rules": [r.to_dict() for r in self.rule_results if r.passed],
            "all_rules": [r.to_dict() for r in self.rule_results],
        }
