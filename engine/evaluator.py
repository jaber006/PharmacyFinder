"""
Three-pass evaluator: exclusion → classification → ranking.

Pass 1: General boolean gates (fast exclusion) + V3 general requirements checker
Pass 2: Run all 6 rule items (deterministic decision tree)
Pass 3: Commercial ranking (only for passing candidates)
        + Ministerial discretion scoring for near-misses
        + Risk assessment for passing candidates
"""
from engine.models import Candidate, RuleResult, EvaluationResult
from engine.rules.general import check_general_requirements, check_supermarket_access
from engine.rules.item_130 import check_item_130
from engine.rules.item_131 import check_item_131
from engine.rules.item_132 import check_item_132
from engine.rules.item_133 import check_item_133
from engine.rules.item_134 import check_item_134
from engine.rules.item_134a import check_item_134a
from engine.rules.item_135 import check_item_135
from engine.rules.item_136 import check_item_136
from engine.context import EvaluationContext
from engine.general_requirements import GeneralRequirements, run_automated_checks
from engine.ministerial import assess_all_failed_rules
from engine.risk_assessment import assess_risks


def evaluate_candidate(candidate: Candidate, context: EvaluationContext) -> EvaluationResult:
    """
    Full 3-pass evaluation of a candidate against all rule items.
    Returns EvaluationResult with all rule results.
    """
    result = EvaluationResult(candidate=candidate)

    # --- Pass 1: General exclusion ---

    # V3 general requirements check (automated + manual tracking)
    general_reqs = run_automated_checks(candidate, context)
    result.general_requirements = general_reqs

    # Hard fail: if automated checks fail, no further evaluation
    if general_reqs.overall_status() == "failed":
        result.rule_results = [RuleResult(
            item="General",
            passed=False,
            reasons=[f"FAIL: {chk}" for chk in general_reqs.failed_checks],
            evidence_needed=[],
            confidence=0.0,
        )]
        return result

    # Legacy general check (V2 compatibility — flags on Candidate model)
    general = check_general_requirements(candidate)
    if not general.passed:
        result.rule_results = [general]
        return result

    # Note pending verifications in the general result
    if general_reqs.overall_status() == "pending_verification":
        general.evidence_needed.extend([
            f"Pending manual verification: {chk}"
            for chk in general_reqs.pending_checks
        ])

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

    # Items 133, 134, 134A, 135, 136 (complex-based — no supermarket access restriction)
    rule_checks.append(check_item_133(candidate, context))
    rule_checks.append(check_item_134(candidate, context))
    rule_checks.append(check_item_134a(candidate, context))
    rule_checks.append(check_item_135(candidate, context))
    rule_checks.append(check_item_136(candidate, context))

    result.rule_results = rule_checks
    result.passed_any = any(r.passed for r in rule_checks)

    # Determine primary rule (highest confidence among passing)
    passing = [r for r in rule_checks if r.passed]
    if passing:
        best = max(passing, key=lambda r: r.confidence)
        result.primary_rule = best.item

    # --- Pass 3 extensions ---

    # Ministerial discretion scoring for failed rules (near-misses)
    if not result.passed_any:
        result.ministerial_assessments = assess_all_failed_rules(
            candidate, rule_checks, context
        )

    # Risk assessment for passing candidates
    if result.passed_any and result.primary_rule:
        result.risk_assessment = assess_risks(
            candidate, result.primary_rule, context
        )

    return result
