"""Tests for County Health Rankings downloader and parser."""

import pytest
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.scrapers.health_downloader import (
    _safe_float,
    parse_chr_csv,
    STATE_FIPS,
    CHR_COLUMNS,
)


class TestSafeFloat:
    def test_valid_float(self):
        assert _safe_float("123.45") == 123.45

    def test_integer_string(self):
        assert _safe_float("100") == 100.0

    def test_comma_separated(self):
        assert _safe_float("1,234.56") == 1234.56

    def test_empty_string(self):
        assert _safe_float("") is None

    def test_none(self):
        assert _safe_float(None) is None

    def test_dot_only(self):
        assert _safe_float(".") is None

    def test_whitespace(self):
        assert _safe_float("  ") is None

    def test_negative(self):
        assert _safe_float("-45.6") == -45.6


class TestStateFips:
    def test_texas(self):
        assert STATE_FIPS["48"] == "TX"

    def test_ohio(self):
        assert STATE_FIPS["39"] == "OH"

    def test_all_states(self):
        assert len(STATE_FIPS) >= 50

    def test_no_duplicate_values(self):
        values = list(STATE_FIPS.values())
        assert len(values) == len(set(values))


class TestChrColumns:
    def test_health_columns_present(self):
        values = set(CHR_COLUMNS.values())
        assert "premature_death_rate" in values
        assert "poor_health_pct" in values
        assert "life_expectancy" in values
        assert "adult_obesity_pct" in values

    def test_demographic_columns_present(self):
        values = set(CHR_COLUMNS.values())
        assert "median_household_income" in values
        assert "poverty_pct" in values
        assert "unemployment_pct" in values

    def test_fips_columns_present(self):
        assert "statecode" in CHR_COLUMNS
        assert "countycode" in CHR_COLUMNS


class TestParseChrCsv:
    def test_parse_valid_csv(self, tmp_path):
        """Create a minimal CHR CSV with dual headers and parse it."""
        csv_content = (
            '"State FIPS Code","County FIPS Code","State","County",'
            '"Premature Death raw value","Poor Health raw value"\n'
            '"statecode","countycode","state","county",'
            '"v001_rawvalue","v002_rawvalue"\n'
            '"48","201","TX","Harris County",'
            '"7500.0","18.5"\n'
            '"48","113","TX","Dallas County",'
            '"8200.0","20.1"\n'
        )
        csv_path = tmp_path / "test_chr.csv"
        csv_path.write_text(csv_content)

        health, demo = parse_chr_csv(csv_path, year=2024)
        assert len(health) == 2
        assert health[0]["fips_county"] == "48201"
        assert health[0]["premature_death_rate"] == 7500.0
        assert health[0]["state"] == "TX"

    def test_skips_state_summaries(self, tmp_path):
        csv_content = (
            '"State FIPS Code","County FIPS Code","State","County",'
            '"Premature Death raw value"\n'
            '"statecode","countycode","state","county",'
            '"v001_rawvalue"\n'
            '"48","000","TX","","7500.0"\n'
            '"48","201","TX","Harris County","8000.0"\n'
        )
        csv_path = tmp_path / "test_skip.csv"
        csv_path.write_text(csv_content)

        health, demo = parse_chr_csv(csv_path)
        assert len(health) == 1
        assert health[0]["fips_county"] == "48201"

    def test_handles_bom(self, tmp_path):
        csv_content = (
            '\ufeff"State FIPS Code","County FIPS Code","State","County",'
            '"Premature Death raw value"\n'
            '"statecode","countycode","state","county",'
            '"v001_rawvalue"\n'
            '"48","201","TX","Harris County","7500"\n'
        )
        csv_path = tmp_path / "bom.csv"
        csv_path.write_text(csv_content)

        health, demo = parse_chr_csv(csv_path)
        assert len(health) == 1

    def test_routes_demographics(self, tmp_path):
        csv_content = (
            '"State FIPS Code","County FIPS Code","State","County",'
            '"Median Household Income raw value","Poverty raw value"\n'
            '"statecode","countycode","state","county",'
            '"v058_rawvalue","v069_rawvalue"\n'
            '"48","201","TX","Harris County","57791","16.5"\n'
        )
        csv_path = tmp_path / "demo.csv"
        csv_path.write_text(csv_content)

        health, demo = parse_chr_csv(csv_path)
        assert len(demo) == 1
        assert demo[0]["median_household_income"] == 57791.0
        assert demo[0]["poverty_pct"] == 16.5

    def test_empty_csv(self, tmp_path):
        csv_path = tmp_path / "empty.csv"
        csv_path.write_text("header1\nheader2\n")
        health, demo = parse_chr_csv(csv_path)
        assert health == []
        assert demo == []

    def test_too_short_csv(self, tmp_path):
        csv_path = tmp_path / "short.csv"
        csv_path.write_text("only one line\n")
        health, demo = parse_chr_csv(csv_path)
        assert health == []
        assert demo == []

    def test_fips_zero_padding(self, tmp_path):
        csv_content = (
            '"sc","cc","state","county","v001_rawvalue"\n'
            '"statecode","countycode","state","county","v001_rawvalue"\n'
            '"1","1","AL","Autauga County","6000"\n'
        )
        csv_path = tmp_path / "pad.csv"
        csv_path.write_text(csv_content)

        health, demo = parse_chr_csv(csv_path)
        assert len(health) == 1
        assert health[0]["fips_county"] == "01001"
