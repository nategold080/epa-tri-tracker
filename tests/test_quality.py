"""Tests for quality scoring."""

import pytest
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.validation.quality import (
    score_facility,
    validate_facility,
    validate_release,
    WEIGHTS,
)


@pytest.fixture
def complete_facility():
    return {
        "tri_facility_id": "TEST001",
        "facility_name": "Test Chemical Plant",
        "canonical_name": "Test Chemical Plant",
        "state": "TX",
        "latitude": 29.76,
        "longitude": -95.36,
        "fips_county": "48201",
        "sic_code": "2819",
        "industry_sector": "Chemical Manufacturing",
    }


@pytest.fixture
def minimal_facility():
    return {
        "tri_facility_id": "TEST002",
        "facility_name": "Unknown Plant",
        "state": "TX",
    }


class TestWeights:
    def test_weights_sum_to_one(self):
        total = sum(WEIGHTS.values())
        assert abs(total - 1.0) < 0.001

    def test_all_components_present(self):
        expected = [
            "has_facility_name", "has_location_data",
            "has_chemical_releases", "has_release_quantities",
            "has_health_data_linked", "has_demographic_data",
            "has_ej_indicators", "has_industry_classification",
            "has_enforcement_data", "has_source_url",
            "has_historical_trend",
        ]
        for comp in expected:
            assert comp in WEIGHTS


class TestScoreFacility:
    def test_complete_facility_high_score(self, complete_facility):
        result = score_facility(
            complete_facility,
            has_health_data=True,
            has_demographics=True,
            has_ej_data=True,
            has_enforcement=True,
            has_historical=True,
            release_count=10,
            total_releases_lbs=50000.0,
        )
        assert result["quality_score"] >= 0.9

    def test_minimal_facility_low_score(self, minimal_facility):
        result = score_facility(minimal_facility)
        assert result["quality_score"] < 0.3

    def test_score_range(self, complete_facility):
        result = score_facility(complete_facility)
        assert 0.0 <= result["quality_score"] <= 1.0

    def test_issues_populated(self, minimal_facility):
        result = score_facility(minimal_facility)
        assert len(result["issues"]) > 0
        assert any("location" in i.lower() for i in result["issues"])

    def test_component_scores_returned(self, complete_facility):
        result = score_facility(complete_facility, release_count=5, total_releases_lbs=1000)
        assert "component_scores" in result
        assert len(result["component_scores"]) == len(WEIGHTS)

    def test_health_data_linkage_matters(self, complete_facility):
        without_health = score_facility(
            complete_facility, release_count=5, total_releases_lbs=1000,
        )
        with_health = score_facility(
            complete_facility, has_health_data=True,
            release_count=5, total_releases_lbs=1000,
        )
        assert with_health["quality_score"] > without_health["quality_score"]

    def test_demographic_linkage_matters(self, complete_facility):
        without = score_facility(
            complete_facility, release_count=5, total_releases_lbs=1000,
        )
        with_demo = score_facility(
            complete_facility, has_demographics=True,
            release_count=5, total_releases_lbs=1000,
        )
        assert with_demo["quality_score"] > without["quality_score"]

    def test_ej_linkage_matters(self, complete_facility):
        without = score_facility(
            complete_facility, release_count=5, total_releases_lbs=1000,
        )
        with_ej = score_facility(
            complete_facility, has_ej_data=True,
            release_count=5, total_releases_lbs=1000,
        )
        assert with_ej["quality_score"] > without["quality_score"]

    def test_release_count_affects_score(self, complete_facility):
        no_releases = score_facility(complete_facility, release_count=0)
        with_releases = score_facility(complete_facility, release_count=10, total_releases_lbs=5000)
        assert with_releases["quality_score"] > no_releases["quality_score"]

    def test_canonical_name_bonus(self, complete_facility, minimal_facility):
        with_canonical = score_facility(complete_facility)
        without_canonical = score_facility(minimal_facility)
        # Canonical name gives full 1.0 vs partial 0.8
        assert with_canonical["component_scores"]["has_facility_name"] > without_canonical["component_scores"]["has_facility_name"]

    def test_source_url_always_scored(self, minimal_facility):
        result = score_facility(minimal_facility)
        assert result["component_scores"]["has_source_url"] == 1.0

    def test_industry_from_sic_partial(self):
        fac = {
            "tri_facility_id": "T1",
            "facility_name": "Test",
            "state": "TX",
            "sic_code": "2819",
        }
        result = score_facility(fac)
        assert result["component_scores"]["has_industry_classification"] == 0.7


class TestValidateFacility:
    def test_valid_facility(self, complete_facility):
        errors = validate_facility(complete_facility)
        assert len(errors) == 0

    def test_missing_tri_id(self):
        errors = validate_facility({"facility_name": "Test", "state": "TX"})
        assert any("tri_facility_id" in e for e in errors)

    def test_missing_name(self):
        errors = validate_facility({"tri_facility_id": "T1", "state": "TX"})
        assert any("facility_name" in e for e in errors)

    def test_missing_state(self):
        errors = validate_facility({"tri_facility_id": "T1", "facility_name": "Test"})
        assert any("state" in e.lower() for e in errors)

    def test_invalid_state_code(self):
        errors = validate_facility({
            "tri_facility_id": "T1",
            "facility_name": "Test",
            "state": "Texas",
        })
        assert any("2 characters" in e for e in errors)

    def test_latitude_range(self):
        errors = validate_facility({
            "tri_facility_id": "T1",
            "facility_name": "Test",
            "state": "TX",
            "latitude": 100.0,
        })
        assert any("Latitude" in e for e in errors)

    def test_longitude_range(self):
        errors = validate_facility({
            "tri_facility_id": "T1",
            "facility_name": "Test",
            "state": "TX",
            "longitude": 50.0,
        })
        assert any("Longitude" in e for e in errors)


class TestValidateRelease:
    def test_valid_release(self):
        release = {
            "tri_facility_id": "T1",
            "chemical_name": "Toluene",
            "reporting_year": 2023,
            "total_releases_lbs": 1000.0,
        }
        errors = validate_release(release)
        assert len(errors) == 0

    def test_missing_facility_id(self):
        errors = validate_release({"chemical_name": "Toluene"})
        assert any("tri_facility_id" in e for e in errors)

    def test_missing_chemical(self):
        errors = validate_release({"tri_facility_id": "T1"})
        assert any("chemical_name" in e for e in errors)

    def test_invalid_year(self):
        errors = validate_release({
            "tri_facility_id": "T1",
            "chemical_name": "Toluene",
            "reporting_year": 1900,
        })
        assert any("year" in e.lower() for e in errors)

    def test_negative_releases(self):
        errors = validate_release({
            "tri_facility_id": "T1",
            "chemical_name": "Toluene",
            "total_releases_lbs": -100,
        })
        assert any("Negative" in e for e in errors)
