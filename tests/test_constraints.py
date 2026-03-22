"""
TDD tests for is_impossible_candidate() in load_db.py.

These tests are written FIRST and will all FAIL until the function is
implemented. Once implemented they must all pass before the feature is
considered complete.
"""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from load_db import is_impossible_candidate


class TestSameTypesSameGroup:
    def test_same_group_gap_2_is_possible(self):
        assert not is_impossible_candidate("player", "12U", 2016, "player", "12U", 2018)

    def test_same_group_gap_3_is_impossible(self):
        assert is_impossible_candidate("player", "12U", 2016, "player", "12U", 2019)

    def test_10u_gap_3_is_impossible(self):
        assert is_impossible_candidate("player", "10U", 2013, "player", "10U", 2016)

    def test_same_group_gap_1_is_possible(self):
        assert not is_impossible_candidate("player", "12U", 2016, "player", "12U", 2017)

    def test_same_season_same_group_is_possible(self):
        assert not is_impossible_candidate("player", "12U", 2016, "player", "12U", 2016)


class TestCrossTypeSameGroup:
    def test_kyle_kaleb_cross_type_gap_3_is_impossible(self):
        """Kyle Rodgers (player, 12U, 2015-16) vs Kaleb Rodgers (goalie, 12U, 2018-19)."""
        assert is_impossible_candidate("player", "12U", 2016, "goalie", "12U", 2019)

    def test_cross_type_gap_1_is_possible(self):
        """Same person could play skater one season, goalie the next."""
        assert not is_impossible_candidate("player", "12U", 2016, "goalie", "12U", 2017)

    def test_cross_type_gap_2_is_impossible(self):
        """Tighter threshold for cross-type: gap ≥ 2 → impossible."""
        assert is_impossible_candidate("player", "12U", 2016, "goalie", "12U", 2018)

    def test_goalie_to_player_gap_2_is_impossible(self):
        """Goalie→player direction also uses tighter threshold."""
        assert is_impossible_candidate("goalie", "12U", 2016, "player", "12U", 2018)

    def test_cross_type_same_season_is_possible(self):
        """Same season, different type (dual role in same year)."""
        assert not is_impossible_candidate("player", "12U", 2016, "goalie", "12U", 2016)


class TestAgeGroupRegression:
    def test_16u_to_12u_later_season_is_impossible(self):
        """Can't appear in a lower age group in a later season."""
        assert is_impossible_candidate("player", "16U", 2016, "player", "12U", 2019)

    def test_18u_to_10u_later_season_is_impossible(self):
        assert is_impossible_candidate("player", "18U", 2015, "player", "10U", 2018)

    def test_same_season_lower_group_is_possible(self):
        """Conservative: same season cross-group not auto-dismissed."""
        assert not is_impossible_candidate("player", "16U", 2016, "player", "12U", 2016)

    def test_natural_aging_12u_to_14u_is_possible(self):
        """12U in year X naturally ages into 14U two years later."""
        assert not is_impossible_candidate("player", "12U", 2016, "player", "14U", 2018)


class TestCrossAgeGroupTimeMismatch:
    def test_valid_10u_to_18u_8yr_is_possible(self):
        assert not is_impossible_candidate("player", "10U", 2012, "player", "18U", 2020)

    def test_sub14_mismatch_too_far_is_impossible(self):
        """10U/2013 → 12U/2020: year_diff=7, expected=2 → 7 > 2+2 → impossible."""
        assert is_impossible_candidate("player", "10U", 2013, "player", "12U", 2020)

    def test_14u_to_18u_playing_up_1yr_is_possible(self):
        """14U player can play up to 18U in next season."""
        assert not is_impossible_candidate("player", "14U", 2016, "player", "18U", 2017)

    def test_14u_to_16u_playing_up_same_season_is_possible(self):
        assert not is_impossible_candidate("player", "14U", 2016, "player", "16U", 2016)

    def test_10u_to_12u_valid_progression_2yr_is_possible(self):
        assert not is_impossible_candidate("player", "10U", 2013, "player", "12U", 2015)


class TestHighSchool:
    def test_hs_gap_4_is_possible(self):
        assert not is_impossible_candidate("player", "HS", 2015, "player", "HS", 2019)

    def test_hs_gap_5_is_impossible(self):
        assert is_impossible_candidate("player", "HS", 2015, "player", "HS", 2020)

    def test_hs_gap_3_is_possible(self):
        assert not is_impossible_candidate("player", "HS", 2015, "player", "HS", 2018)

    def test_mixed_hs_and_numeric_is_possible(self):
        """Conservative: don't auto-dismiss mixed HS/numeric pairs."""
        assert not is_impossible_candidate("player", "12U", 2016, "player", "HS", 2020)


class TestEdgeCases:
    def test_argument_order_reversed_is_symmetric(self):
        """Swapping arguments should give same result."""
        assert is_impossible_candidate("player", "12U", 2019, "player", "12U", 2016)

    def test_unknown_age_group_is_possible(self):
        assert not is_impossible_candidate("player", None, 2016, "player", "12U", 2018)

    def test_both_unknown_age_group_is_possible(self):
        assert not is_impossible_candidate("player", None, 2016, "player", None, 2019)

    def test_non_standard_age_group_format_is_possible(self):
        assert not is_impossible_candidate("player", "U12", 2016, "player", "12U", 2019)
