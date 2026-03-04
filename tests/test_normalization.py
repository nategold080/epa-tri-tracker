"""Tests for facility normalization."""

import pytest
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.normalization.facilities import (
    _normalize_name,
    _expand_abbreviations,
    classify_industry_sector,
    resolve_facility_name,
)


class TestNormalizeName:
    def test_strip_llc(self):
        result = _normalize_name("Acme Industries LLC")
        assert "LLC" not in result
        assert "Acme" in result

    def test_strip_inc(self):
        assert "Test" in _normalize_name("Test, Inc.")

    def test_strip_corp(self):
        assert "Test" in _normalize_name("Test Corp.")

    def test_strip_plant(self):
        assert "Exxon" in _normalize_name("Exxon Plant")

    def test_strip_facility(self):
        assert "Chevron" in _normalize_name("Chevron Facility")

    def test_strip_dash_suffix(self):
        name = _normalize_name("BASF Corp - Freeport Operations")
        assert "BASF" in name

    def test_whitespace_normalization(self):
        name = _normalize_name("Test   Company   LLC")
        assert "  " not in name

    def test_preserves_core_name(self):
        assert _normalize_name("Dow Chemical") == "Dow Chemical"


class TestExpandAbbreviations:
    def test_mfg(self):
        assert "Manufacturing" in _expand_abbreviations("ABC Mfg")

    def test_chem(self):
        assert "Chemical" in _expand_abbreviations("Acme Chem")

    def test_ampersand(self):
        assert "and" in _expand_abbreviations("A & B")

    def test_no_match(self):
        assert _expand_abbreviations("Normal Name") == "Normal Name"


class TestClassifyIndustrySector:
    def test_chemical(self):
        assert classify_industry_sector("2819") == "Chemical Manufacturing"

    def test_petroleum(self):
        assert classify_industry_sector("2911") == "Petroleum Refining"

    def test_primary_metals(self):
        assert classify_industry_sector("3312") == "Primary Metals"

    def test_utilities(self):
        assert classify_industry_sector("4911") == "Electric & Gas Utilities"

    def test_mining(self):
        assert classify_industry_sector("1040") == "Mining"

    def test_none_input(self):
        assert classify_industry_sector(None) is None

    def test_empty_string(self):
        assert classify_industry_sector("") is None

    def test_invalid_code(self):
        assert classify_industry_sector("9999") is None

    def test_fabricated_metals(self):
        assert classify_industry_sector("3441") == "Fabricated Metal Products"

    def test_paper(self):
        assert classify_industry_sector("2611") == "Paper & Allied Products"


class TestResolveFacilityName:
    def test_exact_match(self):
        canonicals = {"ExxonMobil Baytown Complex", "Dow Chemical Freeport"}
        result = resolve_facility_name("ExxonMobil Baytown Complex", canonicals)
        assert result == "ExxonMobil Baytown Complex"

    def test_fuzzy_match(self):
        canonicals = {"ExxonMobil Baytown Complex"}
        result = resolve_facility_name("EXXONMOBIL BAYTOWN COMPLEX", canonicals)
        assert result == "ExxonMobil Baytown Complex"

    def test_no_match_returns_original(self):
        canonicals = {"ExxonMobil"}
        result = resolve_facility_name("Totally Different Name", canonicals)
        assert result == "Totally Different Name"

    def test_empty_input(self):
        assert resolve_facility_name("") == ""
        assert resolve_facility_name("  ") == "  "

    def test_none_canonicals(self):
        result = resolve_facility_name("Test Plant")
        assert result == "Test Plant"
