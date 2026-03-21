"""
Test suite for PharmacyFinder profitability estimator.

Validates revenue formulas, gross profit, setup costs, ROI,
and the composite profitability score.
"""
import os
import sys
import pytest

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from analysis.profitability import (
    estimate_scripts,
    estimate_revenue,
    estimate_gp,
    estimate_setup_costs,
    calculate_roi,
    profitability_score,
    PBS_DISPENSING_FEE,
    PBS_REVENUE_SHARE,
    GP_MARGIN,
    SCRIPTS_PER_CAPITA_YEAR,
    FITOUT_MIN,
    FITOUT_MAX,
    GOODWILL_MULTIPLE,
)


# ═══════════════════════════════════════════════════════════════════════════
# REVENUE FORMULA: total_revenue = (scripts * 8.50) / 0.65
# ═══════════════════════════════════════════════════════════════════════════
class TestRevenueFormula:
    def test_basic_revenue(self):
        """Revenue = (scripts * $8.50) / 0.65."""
        scripts = 50_000
        expected = (scripts * PBS_DISPENSING_FEE) / PBS_REVENUE_SHARE
        result = estimate_revenue(scripts)
        assert result == pytest.approx(expected, rel=1e-6)

    def test_revenue_known_value(self):
        """73,000 scripts → $953,846 revenue."""
        scripts = 73_000
        pbs = scripts * 8.50       # $620,500
        total = pbs / 0.65         # $954,615.38...
        result = estimate_revenue(scripts)
        assert result == pytest.approx(total, rel=1e-6)

    def test_zero_scripts(self):
        """Zero scripts → zero revenue."""
        assert estimate_revenue(0) == 0.0

    def test_pbs_is_65_pct_of_total(self):
        """PBS revenue should be exactly 65% of total revenue."""
        scripts = 60_000
        total = estimate_revenue(scripts)
        pbs = scripts * PBS_DISPENSING_FEE
        assert pbs / total == pytest.approx(PBS_REVENUE_SHARE, rel=1e-6)


# ═══════════════════════════════════════════════════════════════════════════
# GROSS PROFIT: GP = revenue * 0.33
# ═══════════════════════════════════════════════════════════════════════════
class TestGrossProfit:
    def test_gp_calculation(self):
        """GP = revenue * 33%."""
        revenue = 1_000_000
        expected = revenue * GP_MARGIN
        assert estimate_gp(revenue) == pytest.approx(expected, rel=1e-6)

    def test_gp_known_value(self):
        """$954,615 revenue → $315,023 GP."""
        revenue = 954_615.38
        result = estimate_gp(revenue)
        assert result == pytest.approx(revenue * 0.33, rel=1e-6)

    def test_gp_zero_revenue(self):
        assert estimate_gp(0) == 0.0

    def test_gp_margin_constant(self):
        """Ensure GP_MARGIN is 0.33."""
        assert GP_MARGIN == 0.33


# ═══════════════════════════════════════════════════════════════════════════
# PAYBACK: payback_years = setup_cost / annual_GP
# ═══════════════════════════════════════════════════════════════════════════
class TestPayback:
    def test_payback_formula(self):
        """Payback = setup_cost / annual_GP."""
        annual_gp = 300_000
        setup_cost = 525_000
        expected_payback = setup_cost / annual_gp  # 1.75 years
        payback, _, _ = calculate_roi(annual_gp, setup_cost, 1_000_000)
        assert payback == pytest.approx(expected_payback, rel=1e-6)

    def test_payback_zero_gp(self):
        """Zero GP → 999 years (sentinel)."""
        payback, _, _ = calculate_roi(0, 500_000, 0)
        assert payback == 999.0

    def test_exit_value(self):
        """Exit value = annual_revenue * 0.4 (goodwill multiple)."""
        annual_revenue = 1_000_000
        _, exit_val, _ = calculate_roi(300_000, 500_000, annual_revenue)
        assert exit_val == pytest.approx(annual_revenue * GOODWILL_MULTIPLE, rel=1e-6)

    def test_flip_profit(self):
        """Flip profit = exit_value - setup_cost."""
        annual_revenue = 1_000_000
        setup_cost = 500_000
        _, exit_val, flip = calculate_roi(300_000, setup_cost, annual_revenue)
        assert flip == pytest.approx(exit_val - setup_cost, rel=1e-6)

    def test_negative_flip(self):
        """When exit value < setup cost, flip is negative."""
        annual_revenue = 500_000  # exit = 200k
        setup_cost = 525_000
        _, _, flip = calculate_roi(200_000, setup_cost, annual_revenue)
        assert flip < 0


# ═══════════════════════════════════════════════════════════════════════════
# SCRIPT ESTIMATION
# ═══════════════════════════════════════════════════════════════════════════
class TestScriptEstimation:
    def test_basic_scripts(self):
        """Scripts = (pop / (pharmacies+1)) * 16.0 * GP boost."""
        pop = 50_000
        pharmacies = 4
        gps = 5
        pop_per_pharm = pop / (pharmacies + 1)  # 10,000
        base = pop_per_pharm * SCRIPTS_PER_CAPITA_YEAR  # 160,000
        gp_boost = 1.0 + 0.08 * min(gps, 10)  # 1.40
        expected = base * gp_boost  # 224,000
        result = estimate_scripts(pop, pharmacies, gps)
        assert result == pytest.approx(expected, rel=1e-6)

    def test_zero_population(self):
        """Zero population → zero scripts."""
        assert estimate_scripts(0, 5, 3) == 0.0

    def test_gp_boost_capped_at_10(self):
        """GP boost capped at 10 GPs (1.0 + 0.08*10 = 1.80)."""
        result_10 = estimate_scripts(50_000, 0, 10)
        result_20 = estimate_scripts(50_000, 0, 20)
        # Should be identical since boost caps at 10
        assert result_10 == pytest.approx(result_20, rel=1e-6)

    def test_no_gps_no_boost(self):
        """Zero GPs → boost = 1.0 (no multiplier)."""
        pop = 50_000
        pharmacies = 0
        expected = (pop / 1) * SCRIPTS_PER_CAPITA_YEAR * 1.0
        result = estimate_scripts(pop, pharmacies, 0)
        assert result == pytest.approx(expected, rel=1e-6)

    def test_denominator_never_zero(self):
        """Even with 0 pharmacies, denominator is max(0+1, 1) = 1."""
        # Should not raise ZeroDivisionError
        result = estimate_scripts(10_000, 0, 0)
        assert result > 0


# ═══════════════════════════════════════════════════════════════════════════
# SETUP COSTS
# ═══════════════════════════════════════════════════════════════════════════
class TestSetupCosts:
    def test_default_floor_area(self):
        """Default 100sqm: fitout = min(400k, max(250k, 100 * 2400)) = 250k."""
        fitout, stock, wc, total = estimate_setup_costs()
        assert fitout == pytest.approx(max(FITOUT_MIN, 100 * 3000 * 0.8), rel=1e-6)
        assert stock == pytest.approx(200_000, rel=1e-6)  # (150k + 250k) / 2
        assert wc == pytest.approx(75_000, rel=1e-6)      # (50k + 100k) / 2
        assert total == pytest.approx(fitout + stock + wc, rel=1e-6)

    def test_fitout_capped_at_max(self):
        """Large area → fitout capped at $400k."""
        fitout, _, _, _ = estimate_setup_costs(floor_area_sqm=500)
        assert fitout == FITOUT_MAX

    def test_fitout_floor_at_min(self):
        """Tiny area → fitout floored at $250k."""
        fitout, _, _, _ = estimate_setup_costs(floor_area_sqm=10)
        assert fitout == FITOUT_MIN

    def test_total_is_sum_of_components(self):
        fitout, stock, wc, total = estimate_setup_costs(floor_area_sqm=120)
        assert total == pytest.approx(fitout + stock + wc, rel=1e-6)


# ═══════════════════════════════════════════════════════════════════════════
# PROFITABILITY SCORE (0-100)
# ═══════════════════════════════════════════════════════════════════════════
class TestProfitabilityScore:
    def test_zero_gp_returns_zero(self):
        """Zero GP → score 0."""
        assert profitability_score(1_000_000, 0, 2.0, 100_000) == 0.0

    def test_max_score_cap(self):
        """Score is capped at 100."""
        score = profitability_score(10_000_000, 5_000_000, 0.5, 5_000_000)
        assert score <= 100.0

    def test_score_components(self):
        """Verify individual component contributions."""
        revenue = 2_000_000
        gp = 500_000
        payback = 1.0
        flip = 200_000

        rev_score = min(100, revenue / 2_000_000 * 30)       # 30
        gp_score = min(100, gp / 500_000 * 25)               # 25
        payback_score = max(0, 25 - payback * 5)              # 20
        flip_score = max(0, min(25, flip / 200_000 * 25))     # 25
        expected = min(100, rev_score + gp_score + payback_score + flip_score)

        result = profitability_score(revenue, gp, payback, flip)
        assert result == pytest.approx(expected, abs=0.1)

    def test_high_payback_reduces_score(self):
        """Payback of 5+ years → payback_score = 0."""
        score_fast = profitability_score(1_000_000, 300_000, 1.0, 100_000)
        score_slow = profitability_score(1_000_000, 300_000, 6.0, 100_000)
        assert score_fast > score_slow

    def test_negative_flip_zero_contribution(self):
        """Negative flip profit → flip_score = 0."""
        score = profitability_score(500_000, 200_000, 3.0, -100_000)
        # Flip component should be 0, but other components contribute
        assert score > 0


# ═══════════════════════════════════════════════════════════════════════════
# END-TO-END: scripts → revenue → GP → ROI
# ═══════════════════════════════════════════════════════════════════════════
class TestEndToEnd:
    def test_full_pipeline(self):
        """Run the full financial pipeline and verify chain consistency."""
        pop = 30_000
        pharmacies = 2
        gps = 5

        scripts = estimate_scripts(pop, pharmacies, gps)
        revenue = estimate_revenue(scripts)
        gp = estimate_gp(revenue)
        fitout, stock, wc, setup = estimate_setup_costs()
        payback, exit_val, flip = calculate_roi(gp, setup, revenue)
        score = profitability_score(revenue, gp, payback, flip)

        # Chain checks
        assert scripts > 0
        assert revenue > scripts  # revenue includes front-of-shop markup
        assert gp < revenue       # GP is a fraction of revenue
        assert setup > 0
        assert payback > 0
        assert exit_val == pytest.approx(revenue * 0.4, rel=1e-6)
        assert flip == pytest.approx(exit_val - setup, rel=1e-6)
        assert 0 <= score <= 100

    def test_revenue_formula_identity(self):
        """Verify: revenue = (scripts * 8.50) / 0.65 exactly."""
        for scripts in [10_000, 50_000, 73_000, 100_000]:
            revenue = estimate_revenue(scripts)
            expected = (scripts * 8.50) / 0.65
            assert revenue == pytest.approx(expected, rel=1e-9)

    def test_gp_formula_identity(self):
        """Verify: GP = revenue * 0.33 exactly."""
        for revenue in [500_000, 1_000_000, 2_000_000]:
            gp = estimate_gp(revenue)
            assert gp == pytest.approx(revenue * 0.33, rel=1e-9)

    def test_payback_formula_identity(self):
        """Verify: payback = setup_cost / annual_GP exactly."""
        for gp, setup in [(300_000, 525_000), (500_000, 500_000), (100_000, 525_000)]:
            payback, _, _ = calculate_roi(gp, setup, 1_000_000)
            assert payback == pytest.approx(setup / gp, rel=1e-9)
