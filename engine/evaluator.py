"""
Three-pass evaluator: exclusion → classification → ranking.

Pass 1: General boolean gates (fast exclusion)
Pass 2: Run all 6 rule items (deterministic decision tree)
Pass 3: Commercial ranking (only for passing candidates)
"""
from engine.models import Candidate, RuleResult, EvaluationResult
from engine.rules.general import check_general_requirements, check_supermarket_access
from engine.rules.item_130 import check_item_130
from engine.rules.item_131 import check_item_131
from engine.rules.item_132 import check_item_132
from engine.rules.item_133 import check_item_133
from engine.rules.item_135 import check_item_135
from engine.rules.item_136 import check_item_136
from engine.context import EvaluationContext


def evaluate_candidate(candidate: Candidate, context: EvaluationContext) -> EvaluationResult:
    """
    Full 3-pass evaluation of a candidate against all rule items.
    Returns EvaluationResult with all rule results.
    """
    result = EvaluationResult(candidate=candidate)

    # --- Pass 1: General exclusion ---
    general = check_general_requirements(candidate)
    if not general.passed:
        result.rule_results = [general]
        return result

    # --- Pass 2: Rule-path classification ---
    # Run all applicable rules
    rule_checks = []

    # Items 130, 131 (new pharmacy — standalone)
    # Supermarket access check for new-pharmacy items
    if check_supermarket_access(candidate):
        rule_checks.append(check_item_130(candidate, context))
        rule_checks.append(check_item_131(candidate, context))
        rule_checks.append(check_item_132(candidate, context))
    else:
        # Candidate is directly accessible from supermarket — fails new-pharmacy items
        for item in ("Item 130", "Item 131", "Item 132"):
            rule_checks.append(RuleResult(
                item=item, passed=False,
                reasons=["FAIL: Directly accessible from within a supermarket"],
                confidence=0.0,
            ))

    # Items 133, 135, 136 (complex-based — no supermarket access restriction)
    rule_checks.append(check_item_133(candidate, context))
    rule_checks.append(check_item_135(candidate, context))
    rule_checks.append(check_item_136(candidate, context))

    result.rule_results = rule_checks
    result.passed_any = any(r.passed for r in rule_checks)

    # Determine primary rule (highest confidence among passing)
    passing = [r for r in rule_checks if r.passed]
    if passing:
        best = max(passing, key=lambda r: r.confidence)
        result.primary_rule = best.item

    return result
