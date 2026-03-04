"""Tests for FIPS county code resolution."""

import sqlite3
import pytest
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.normalization.facilities import (
    _normalize_county_name,
    build_fips_lookup,
    resolve_fips_codes,
    _STATE_ABBR_TO_FIPS,
)


class TestNormalizeCountyName:
    def test_strip_county_suffix(self):
        assert _normalize_county_name("Harris County") == "HARRIS"

    def test_strip_parish_suffix(self):
        assert _normalize_county_name("Calcasieu Parish") == "CALCASIEU"

    def test_strip_borough_suffix(self):
        assert _normalize_county_name("Anchorage Borough") == "ANCHORAGE"

    def test_strip_census_area(self):
        assert _normalize_county_name("Bethel Census Area") == "BETHEL"

    def test_strip_city_suffix(self):
        assert _normalize_county_name("Baltimore city") == "BALTIMORE"

    def test_no_suffix(self):
        assert _normalize_county_name("HARRIS") == "HARRIS"

    def test_whitespace(self):
        assert _normalize_county_name("  Harris County  ") == "HARRIS"

    def test_uppercase(self):
        assert _normalize_county_name("harris county") == "HARRIS"

    def test_multi_word(self):
        assert _normalize_county_name("East Baton Rouge Parish") == "EAST BATON ROUGE"


class TestStateAbbrToFips:
    def test_texas(self):
        assert _STATE_ABBR_TO_FIPS["TX"] == "48"

    def test_ohio(self):
        assert _STATE_ABBR_TO_FIPS["OH"] == "39"

    def test_california(self):
        assert _STATE_ABBR_TO_FIPS["CA"] == "06"

    def test_all_50_states(self):
        assert len(_STATE_ABBR_TO_FIPS) >= 50


class TestBuildFipsLookup:
    @pytest.fixture
    def db_with_health(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.execute("""
            CREATE TABLE county_health (
                fips_county TEXT, county_name TEXT, state TEXT
            )
        """)
        conn.executemany(
            "INSERT INTO county_health (fips_county, county_name, state) VALUES (?, ?, ?)",
            [
                ("48201", "Harris County", "TX"),
                ("48113", "Dallas County", "TX"),
                ("39035", "Cuyahoga County", "OH"),
                ("22019", "Calcasieu Parish", "LA"),
                ("18033", "De Kalb County", "IN"),
                ("36061", "New York County", "NY"),
            ],
        )
        conn.commit()
        yield conn
        conn.close()

    def test_basic_lookup(self, db_with_health):
        lookup = build_fips_lookup(db_with_health)
        assert lookup[("HARRIS", "TX")] == "48201"
        assert lookup[("DALLAS", "TX")] == "48113"

    def test_parish_stripped(self, db_with_health):
        lookup = build_fips_lookup(db_with_health)
        assert lookup[("CALCASIEU", "LA")] == "22019"

    def test_county_stripped(self, db_with_health):
        lookup = build_fips_lookup(db_with_health)
        assert lookup[("CUYAHOGA", "OH")] == "39035"

    def test_multi_word_county(self, db_with_health):
        lookup = build_fips_lookup(db_with_health)
        assert lookup[("DE KALB", "IN")] == "18033"

    def test_lookup_count(self, db_with_health):
        lookup = build_fips_lookup(db_with_health)
        # At least 6 entries (one per county, plus possible alternates)
        assert len(lookup) >= 6


class TestResolveFipsCodes:
    @pytest.fixture
    def db_with_data(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row

        conn.executescript("""
            CREATE TABLE tri_facilities (
                tri_facility_id TEXT UNIQUE,
                facility_name TEXT,
                county TEXT,
                state TEXT,
                fips_county TEXT,
                fips_state TEXT,
                updated_at TEXT
            );
            CREATE TABLE county_health (
                fips_county TEXT, county_name TEXT, state TEXT
            );
        """)

        conn.executemany(
            "INSERT INTO county_health (fips_county, county_name, state) VALUES (?, ?, ?)",
            [
                ("48201", "Harris County", "TX"),
                ("22019", "Calcasieu Parish", "LA"),
                ("18033", "De Kalb County", "IN"),
            ],
        )

        conn.executemany(
            "INSERT INTO tri_facilities (tri_facility_id, facility_name, county, state) VALUES (?, ?, ?, ?)",
            [
                ("F001", "Plant A", "HARRIS", "TX"),
                ("F002", "Plant B", "CALCASIEU PARISH", "LA"),
                ("F003", "Plant C", "DEKALB", "IN"),
                ("F004", "Plant D", "NONEXISTENT", "TX"),
            ],
        )
        conn.commit()
        yield conn
        conn.close()

    def test_resolves_basic(self, db_with_data):
        updated = resolve_fips_codes(conn=db_with_data)
        row = db_with_data.execute(
            "SELECT fips_county FROM tri_facilities WHERE tri_facility_id = 'F001'"
        ).fetchone()
        assert row["fips_county"] == "48201"

    def test_resolves_parish(self, db_with_data):
        resolve_fips_codes(conn=db_with_data)
        row = db_with_data.execute(
            "SELECT fips_county FROM tri_facilities WHERE tri_facility_id = 'F002'"
        ).fetchone()
        assert row["fips_county"] == "22019"

    def test_resolves_collapsed_spacing(self, db_with_data):
        resolve_fips_codes(conn=db_with_data)
        row = db_with_data.execute(
            "SELECT fips_county FROM tri_facilities WHERE tri_facility_id = 'F003'"
        ).fetchone()
        assert row["fips_county"] == "18033"

    def test_sets_state_fips_even_when_county_missing(self, db_with_data):
        resolve_fips_codes(conn=db_with_data)
        row = db_with_data.execute(
            "SELECT fips_state FROM tri_facilities WHERE tri_facility_id = 'F004'"
        ).fetchone()
        assert row["fips_state"] == "48"

    def test_returns_update_count(self, db_with_data):
        updated = resolve_fips_codes(conn=db_with_data)
        assert updated >= 3  # F001, F002, F003 should resolve


class TestCityAndSpecialCases:
    """Test resolution of Virginia independent cities, Alaska truncated names, etc."""

    @pytest.fixture
    def db_with_cities(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.executescript("""
            CREATE TABLE tri_facilities (
                tri_facility_id TEXT UNIQUE,
                facility_name TEXT,
                county TEXT,
                state TEXT,
                fips_county TEXT,
                fips_state TEXT,
                updated_at TEXT
            );
            CREATE TABLE county_health (
                fips_county TEXT, county_name TEXT, state TEXT
            );
        """)
        conn.executemany(
            "INSERT INTO county_health (fips_county, county_name, state) VALUES (?, ?, ?)",
            [
                ("51550", "Chesapeake city", "VA"),
                ("51650", "Hampton city", "VA"),
                ("24510", "Baltimore city", "MD"),
                ("24005", "Baltimore County", "MD"),
                ("29510", "St. Louis city", "MO"),
                ("29189", "St. Louis County", "MO"),
                ("02090", "Fairbanks North Star Borough", "AK"),
                ("02016", "Aleutians West Census Area", "AK"),
                ("55109", "St. Croix County", "WI"),
            ],
        )
        conn.executemany(
            "INSERT INTO tri_facilities (tri_facility_id, facility_name, county, state) VALUES (?, ?, ?, ?)",
            [
                ("V001", "Plant VA1", "CHESAPEAKE (CITY)", "VA"),
                ("V002", "Plant VA2", "HAMPTON (CITY)", "VA"),
                ("V003", "Plant MD1", "BALTIMORE (CITY)", "MD"),
                ("V004", "Plant MD2", "BALTIMORE", "MD"),
                ("V005", "Plant MO1", "ST LOUIS (CITY)", "MO"),
                ("V006", "Plant AK1", "FAIRBANKS NORTH STAR BORO", "AK"),
                ("V007", "Plant AK2", "ALEUTIANS WEST CENSUS ARE", "AK"),
                ("V008", "Plant WI1", "ST. CROIX ISLAND", "WI"),
            ],
        )
        conn.commit()
        yield conn
        conn.close()

    def test_virginia_city_resolved(self, db_with_cities):
        resolve_fips_codes(conn=db_with_cities)
        row = db_with_cities.execute(
            "SELECT fips_county FROM tri_facilities WHERE tri_facility_id = 'V001'"
        ).fetchone()
        assert row["fips_county"] == "51550"

    def test_hampton_city_resolved(self, db_with_cities):
        resolve_fips_codes(conn=db_with_cities)
        row = db_with_cities.execute(
            "SELECT fips_county FROM tri_facilities WHERE tri_facility_id = 'V002'"
        ).fetchone()
        assert row["fips_county"] == "51650"

    def test_baltimore_city_vs_county(self, db_with_cities):
        resolve_fips_codes(conn=db_with_cities)
        city = db_with_cities.execute(
            "SELECT fips_county FROM tri_facilities WHERE tri_facility_id = 'V003'"
        ).fetchone()
        county = db_with_cities.execute(
            "SELECT fips_county FROM tri_facilities WHERE tri_facility_id = 'V004'"
        ).fetchone()
        assert city["fips_county"] == "24510"  # Baltimore city
        assert county["fips_county"] == "24005"  # Baltimore County

    def test_st_louis_city(self, db_with_cities):
        resolve_fips_codes(conn=db_with_cities)
        row = db_with_cities.execute(
            "SELECT fips_county FROM tri_facilities WHERE tri_facility_id = 'V005'"
        ).fetchone()
        assert row["fips_county"] == "29510"

    def test_alaska_truncated_borough(self, db_with_cities):
        resolve_fips_codes(conn=db_with_cities)
        row = db_with_cities.execute(
            "SELECT fips_county FROM tri_facilities WHERE tri_facility_id = 'V006'"
        ).fetchone()
        assert row["fips_county"] == "02090"

    def test_alaska_truncated_census_area(self, db_with_cities):
        resolve_fips_codes(conn=db_with_cities)
        row = db_with_cities.execute(
            "SELECT fips_county FROM tri_facilities WHERE tri_facility_id = 'V007'"
        ).fetchone()
        assert row["fips_county"] == "02016"

    def test_st_croix_island(self, db_with_cities):
        resolve_fips_codes(conn=db_with_cities)
        row = db_with_cities.execute(
            "SELECT fips_county FROM tri_facilities WHERE tri_facility_id = 'V008'"
        ).fetchone()
        assert row["fips_county"] == "55109"
