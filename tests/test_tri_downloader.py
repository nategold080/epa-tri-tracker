"""Tests for TRI data downloader and parser."""

import csv
import io
import pytest
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.scrapers.tri_downloader import (
    _safe_float,
    _safe_int,
    _map_row,
    parse_tri_csv,
    extract_facilities,
    extract_releases,
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

    def test_zero(self):
        assert _safe_float("0.000") == 0.0


class TestSafeInt:
    def test_valid_int(self):
        assert _safe_int("123") == 123

    def test_float_string(self):
        assert _safe_int("123.0") == 123

    def test_empty(self):
        assert _safe_int("") is None

    def test_none(self):
        assert _safe_int(None) is None


class TestMapRow:
    def test_maps_standard_columns(self):
        row = {
            "1. YEAR": "2023",
            "2. TRIFD": "TEST001",
            "4. FACILITY NAME": "Test Plant",
            "8. ST": "TX",
            "37. CHEMICAL": "Benzene",
            "107. TOTAL RELEASES": "5000.0",
            "46. CARCINOGEN": "YES",
            "12. LATITUDE": "29.76",
            "13. LONGITUDE": "-95.36",
            "23. INDUSTRY SECTOR": "Chemical Manufacturing",
            "15. PARENT CO NAME": "Big Corp",
        }
        mapped = _map_row(row)
        assert mapped["reporting_year"] == 2023
        assert mapped["tri_facility_id"] == "TEST001"
        assert mapped["facility_name"] == "Test Plant"
        assert mapped["state"] == "TX"
        assert mapped["chemical_name"] == "Benzene"
        assert mapped["total_releases_lbs"] == 5000.0
        assert mapped["carcinogen_flag"] == "YES"
        assert mapped["latitude"] == 29.76
        assert mapped["industry_sector"] == "Chemical Manufacturing"
        assert mapped["parent_company_name"] == "Big Corp"

    def test_handles_empty_values(self):
        row = {
            "1. YEAR": "2023",
            "2. TRIFD": "TEST002",
            "4. FACILITY NAME": "Test",
            "8. ST": "TX",
            "37. CHEMICAL": "Toluene",
            "107. TOTAL RELEASES": "",
            "12. LATITUDE": "",
        }
        mapped = _map_row(row)
        assert mapped["tri_facility_id"] == "TEST002"
        assert mapped.get("total_releases_lbs") is None
        assert mapped.get("latitude") is None

    def test_numeric_conversion(self):
        row = {
            "1. YEAR": "2023",
            "2. TRIFD": "TEST003",
            "37. CHEMICAL": "Methanol",
            "51. 5.1 - FUGITIVE AIR": "100.5",
            "52. 5.2 - STACK AIR": "200.3",
            "53. 5.3 - WATER": "50.0",
        }
        mapped = _map_row(row)
        assert mapped["fugitive_air_lbs"] == 100.5
        assert mapped["stack_air_lbs"] == 200.3
        assert mapped["water_lbs"] == 50.0

    def test_fallback_column_matching(self):
        """Test partial column name matching for non-standard headers."""
        row = {
            "YEAR": "2023",
            "TRIFD": "TEST004",
            "FACILITY NAME": "Alt Format Plant",
            "CHEMICAL": "Xylene",
            "TOTAL RELEASES": "1000.0",
            "CARCINOGEN": "NO",
            "LATITUDE": "30.0",
            "LONGITUDE": "-95.0",
            "COUNTY": "Harris",
            "CITY": "Houston",
        }
        mapped = _map_row(row)
        # Fallback matching should pick up at least some fields
        assert mapped.get("chemical_name") == "Xylene" or mapped.get("tri_facility_id") == "TEST004"


class TestExtractFacilities:
    def test_deduplicates(self):
        records = [
            {"tri_facility_id": "F1", "facility_name": "Plant A", "state": "TX",
             "chemical_name": "Benzene"},
            {"tri_facility_id": "F1", "facility_name": "Plant A", "state": "TX",
             "chemical_name": "Toluene"},
            {"tri_facility_id": "F2", "facility_name": "Plant B", "state": "OH",
             "chemical_name": "Benzene"},
        ]
        facilities = extract_facilities(records)
        assert len(facilities) == 2

    def test_preserves_fields(self):
        records = [
            {
                "tri_facility_id": "F1",
                "facility_name": "Test Plant",
                "state": "TX",
                "city": "Houston",
                "county": "Harris",
                "latitude": 29.76,
                "longitude": -95.36,
                "industry_sector": "Chemical Manufacturing",
                "parent_company_name": "Parent Corp",
                "chemical_name": "Benzene",
            },
        ]
        facilities = extract_facilities(records)
        assert len(facilities) == 1
        f = facilities[0]
        assert f["facility_name"] == "Test Plant"
        assert f["city"] == "Houston"
        assert f["industry_sector"] == "Chemical Manufacturing"

    def test_empty_input(self):
        assert extract_facilities([]) == []

    def test_skips_missing_id(self):
        records = [{"facility_name": "No ID", "chemical_name": "X"}]
        assert extract_facilities(records) == []


class TestExtractReleases:
    def test_basic_extraction(self):
        records = [
            {
                "tri_facility_id": "F1",
                "reporting_year": 2023,
                "chemical_name": "Benzene",
                "carcinogen_flag": "YES",
                "total_releases_lbs": 1000.0,
            },
        ]
        releases = extract_releases(records)
        assert len(releases) == 1
        assert releases[0]["chemical_name"] == "Benzene"
        assert releases[0]["carcinogen_flag"] == "YES"

    def test_skips_missing_fields(self):
        records = [
            {"tri_facility_id": "F1"},  # No chemical name
            {"chemical_name": "Benzene"},  # No facility ID
        ]
        releases = extract_releases(records)
        assert len(releases) == 0

    def test_empty_input(self):
        assert extract_releases([]) == []


class TestParseTriCsv:
    def test_parse_valid_csv(self, tmp_path):
        """Create a minimal TRI CSV and parse it."""
        csv_content = (
            '"1. YEAR","2. TRIFD","4. FACILITY NAME","8. ST","37. CHEMICAL",'
            '"46. CARCINOGEN","107. TOTAL RELEASES","12. LATITUDE","13. LONGITUDE",'
            '"23. INDUSTRY SECTOR","7. COUNTY","6. CITY"\n'
            '"2023","TEST001","Sample Plant","TX","Benzene",'
            '"YES","5000.0","29.76","-95.36",'
            '"Chemical Manufacturing","Harris","Houston"\n'
        )
        csv_path = tmp_path / "test.csv"
        csv_path.write_text(csv_content)

        records = parse_tri_csv(csv_path)
        assert len(records) == 1
        assert records[0]["tri_facility_id"] == "TEST001"
        assert records[0]["chemical_name"] == "Benzene"
        assert records[0]["total_releases_lbs"] == 5000.0

    def test_parse_empty_csv(self, tmp_path):
        csv_path = tmp_path / "empty.csv"
        csv_path.write_text('"1. YEAR","2. TRIFD","37. CHEMICAL"\n')
        records = parse_tri_csv(csv_path)
        assert len(records) == 0

    def test_bom_handling(self, tmp_path):
        csv_content = (
            '\ufeff"1. YEAR","2. TRIFD","4. FACILITY NAME","8. ST","37. CHEMICAL"\n'
            '"2023","BOM001","BOM Plant","TX","Toluene"\n'
        )
        csv_path = tmp_path / "bom.csv"
        csv_path.write_text(csv_content)
        records = parse_tri_csv(csv_path)
        assert len(records) == 1
