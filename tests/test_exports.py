"""Tests for data export functionality."""

import csv
import json
import sqlite3
import pytest
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.export.exporter import (
    _export_csv,
    _export_json,
    _export_markdown,
)


@pytest.fixture
def sample_facilities():
    return [
        {
            "tri_facility_id": "F001",
            "facility_name": "Test Plant A",
            "city": "Houston",
            "county": "HARRIS",
            "state": "TX",
            "zip_code": "77001",
            "latitude": 29.76,
            "longitude": -95.37,
            "fips_county": "48201",
            "fips_state": "48",
            "sic_code": "2911",
            "industry_sector": "Petroleum Refining",
            "parent_company_name": "Test Corp",
            "quality_score": 0.85,
        },
        {
            "tri_facility_id": "F002",
            "facility_name": "Test Plant B",
            "city": "Dallas",
            "county": "DALLAS",
            "state": "TX",
            "zip_code": "75201",
            "latitude": 32.78,
            "longitude": -96.80,
            "fips_county": "48113",
            "fips_state": "48",
            "sic_code": "2819",
            "industry_sector": "Chemical Manufacturing",
            "parent_company_name": "Chem Inc",
            "quality_score": 0.90,
        },
    ]


@pytest.fixture
def sample_releases():
    return [
        {
            "tri_facility_id": "F001",
            "reporting_year": 2023,
            "chemical_name": "Benzene",
            "cas_number": "71-43-2",
            "carcinogen_flag": "YES",
            "total_releases_lbs": 500.0,
            "fugitive_air_lbs": 200.0,
            "stack_air_lbs": 300.0,
            "water_lbs": 0.0,
            "on_site_release_total": 500.0,
            "off_site_release_total": 0.0,
        },
        {
            "tri_facility_id": "F002",
            "reporting_year": 2023,
            "chemical_name": "Methanol",
            "cas_number": "67-56-1",
            "carcinogen_flag": "NO",
            "total_releases_lbs": 1000.0,
            "fugitive_air_lbs": 400.0,
            "stack_air_lbs": 600.0,
            "water_lbs": 0.0,
            "on_site_release_total": 1000.0,
            "off_site_release_total": 0.0,
        },
    ]


class TestCsvExport:
    def test_creates_facility_csv(self, tmp_path, sample_facilities, sample_releases):
        _export_csv(sample_facilities, sample_releases, tmp_path)
        fac_path = tmp_path / "tri_facilities.csv"
        assert fac_path.exists()

    def test_facility_csv_row_count(self, tmp_path, sample_facilities, sample_releases):
        _export_csv(sample_facilities, sample_releases, tmp_path)
        fac_path = tmp_path / "tri_facilities.csv"
        with open(fac_path) as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        assert len(rows) == 2

    def test_creates_release_csv(self, tmp_path, sample_facilities, sample_releases):
        _export_csv(sample_facilities, sample_releases, tmp_path)
        rel_path = tmp_path / "tri_releases.csv"
        assert rel_path.exists()

    def test_release_csv_row_count(self, tmp_path, sample_facilities, sample_releases):
        _export_csv(sample_facilities, sample_releases, tmp_path)
        rel_path = tmp_path / "tri_releases.csv"
        with open(rel_path) as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        assert len(rows) == 2

    def test_csv_headers_correct(self, tmp_path, sample_facilities, sample_releases):
        _export_csv(sample_facilities, sample_releases, tmp_path)
        fac_path = tmp_path / "tri_facilities.csv"
        with open(fac_path) as f:
            reader = csv.DictReader(f)
            row = next(reader)
        assert "tri_facility_id" in row
        assert "facility_name" in row
        assert "quality_score" in row

    def test_empty_data(self, tmp_path):
        _export_csv([], [], tmp_path)
        # Should not crash


class TestJsonExport:
    def test_creates_facility_json(self, tmp_path, sample_facilities, sample_releases):
        _export_json(sample_facilities, sample_releases, tmp_path)
        fac_path = tmp_path / "tri_facilities.json"
        assert fac_path.exists()

    def test_json_is_valid(self, tmp_path, sample_facilities, sample_releases):
        _export_json(sample_facilities, sample_releases, tmp_path)
        fac_path = tmp_path / "tri_facilities.json"
        with open(fac_path) as f:
            data = json.load(f)
        assert len(data) == 2

    def test_json_has_correct_fields(self, tmp_path, sample_facilities, sample_releases):
        _export_json(sample_facilities, sample_releases, tmp_path)
        fac_path = tmp_path / "tri_facilities.json"
        with open(fac_path) as f:
            data = json.load(f)
        assert data[0]["tri_facility_id"] == "F001"
        assert data[0]["quality_score"] == 0.85

    def test_release_json_valid(self, tmp_path, sample_facilities, sample_releases):
        _export_json(sample_facilities, sample_releases, tmp_path)
        rel_path = tmp_path / "tri_releases.json"
        with open(rel_path) as f:
            data = json.load(f)
        assert len(data) == 2
        assert data[0]["chemical_name"] == "Benzene"


class TestMarkdownExport:
    def test_creates_markdown(self, tmp_path, sample_facilities, sample_releases):
        _export_markdown(sample_facilities, sample_releases, tmp_path)
        md_path = tmp_path / "summary_stats.md"
        assert md_path.exists()

    def test_markdown_has_title(self, tmp_path, sample_facilities, sample_releases):
        _export_markdown(sample_facilities, sample_releases, tmp_path)
        md_path = tmp_path / "summary_stats.md"
        content = md_path.read_text()
        assert "EPA TRI" in content

    def test_markdown_has_counts(self, tmp_path, sample_facilities, sample_releases):
        _export_markdown(sample_facilities, sample_releases, tmp_path)
        md_path = tmp_path / "summary_stats.md"
        content = md_path.read_text()
        assert "2" in content  # 2 facilities

    def test_markdown_has_attribution(self, tmp_path, sample_facilities, sample_releases):
        _export_markdown(sample_facilities, sample_releases, tmp_path)
        md_path = tmp_path / "summary_stats.md"
        content = md_path.read_text()
        assert "Nathan Goldberg" in content
