"""Tests for SQLite storage layer."""

import sqlite3
import pytest
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.storage.database import (
    get_connection,
    init_db,
    upsert_facility,
    upsert_release,
    upsert_county_health,
    upsert_county_demographics,
    store_facilities_batch,
    store_releases_batch,
    store_county_health_batch,
    store_county_demographics_batch,
    get_all_facilities,
    get_facility_releases,
    get_county_context,
    update_facility_quality_scores,
    get_stats,
    start_pipeline_run,
    complete_pipeline_run,
)


@pytest.fixture
def db_conn():
    """Create an in-memory database for testing."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn = init_db(conn)
    yield conn
    conn.close()


@pytest.fixture
def sample_facility():
    return {
        "tri_facility_id": "TEST001",
        "facility_name": "Test Chemical Plant",
        "street_address": "123 Industrial Ave",
        "city": "Houston",
        "county": "Harris",
        "state": "TX",
        "zip_code": "77001",
        "latitude": 29.76,
        "longitude": -95.36,
        "fips_state": "48",
        "fips_county": "48201",
        "sic_code": "2819",
        "industry_sector": "Chemical Manufacturing",
        "parent_company_name": "Test Corp",
    }


@pytest.fixture
def sample_release():
    return {
        "tri_facility_id": "TEST001",
        "reporting_year": 2023,
        "chemical_name": "Toluene",
        "cas_number": "108-88-3",
        "carcinogen_flag": "NO",
        "classification": "TRI",
        "unit_of_measure": "Pounds",
        "total_releases_lbs": 5000.0,
        "fugitive_air_lbs": 1000.0,
        "stack_air_lbs": 3000.0,
        "water_lbs": 500.0,
        "land_lbs": 500.0,
    }


class TestInitDb:
    def test_creates_tables(self, db_conn):
        tables = db_conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()
        table_names = [t["name"] for t in tables]
        assert "tri_facilities" in table_names
        assert "tri_releases" in table_names
        assert "county_health" in table_names
        assert "county_demographics" in table_names
        assert "ej_indicators" in table_names
        assert "pipeline_runs" in table_names

    def test_creates_indexes(self, db_conn):
        indexes = db_conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND name LIKE 'idx_%'"
        ).fetchall()
        assert len(indexes) >= 10

    def test_idempotent(self, db_conn):
        # Running init again should not error
        init_db(db_conn)
        row = db_conn.execute("SELECT COUNT(*) as cnt FROM tri_facilities").fetchone()
        assert row["cnt"] == 0


class TestUpsertFacility:
    def test_insert_new(self, db_conn, sample_facility):
        row_id = upsert_facility(sample_facility, conn=db_conn)
        assert row_id > 0

        row = db_conn.execute(
            "SELECT * FROM tri_facilities WHERE tri_facility_id = ?",
            ("TEST001",),
        ).fetchone()
        assert row is not None
        assert row["facility_name"] == "Test Chemical Plant"
        assert row["state"] == "TX"
        assert row["latitude"] == 29.76

    def test_upsert_updates(self, db_conn, sample_facility):
        upsert_facility(sample_facility, conn=db_conn)
        sample_facility["facility_name"] = "Updated Plant Name"
        upsert_facility(sample_facility, conn=db_conn)

        row = db_conn.execute(
            "SELECT * FROM tri_facilities WHERE tri_facility_id = ?",
            ("TEST001",),
        ).fetchone()
        assert row["facility_name"] == "Updated Plant Name"

    def test_unique_constraint(self, db_conn, sample_facility):
        upsert_facility(sample_facility, conn=db_conn)
        upsert_facility(sample_facility, conn=db_conn)

        row = db_conn.execute(
            "SELECT COUNT(*) as cnt FROM tri_facilities WHERE tri_facility_id = ?",
            ("TEST001",),
        ).fetchone()
        assert row["cnt"] == 1


class TestUpsertRelease:
    def test_insert_new(self, db_conn, sample_release):
        row_id = upsert_release(sample_release, conn=db_conn)
        assert row_id > 0

        row = db_conn.execute(
            "SELECT * FROM tri_releases WHERE tri_facility_id = ? AND chemical_name = ?",
            ("TEST001", "Toluene"),
        ).fetchone()
        assert row is not None
        assert row["total_releases_lbs"] == 5000.0
        assert row["carcinogen_flag"] == "NO"

    def test_upsert_updates(self, db_conn, sample_release):
        upsert_release(sample_release, conn=db_conn)
        sample_release["total_releases_lbs"] = 7500.0
        upsert_release(sample_release, conn=db_conn)

        row = db_conn.execute(
            "SELECT * FROM tri_releases WHERE tri_facility_id = ? AND chemical_name = ?",
            ("TEST001", "Toluene"),
        ).fetchone()
        assert row["total_releases_lbs"] == 7500.0

    def test_unique_constraint(self, db_conn, sample_release):
        upsert_release(sample_release, conn=db_conn)
        upsert_release(sample_release, conn=db_conn)

        row = db_conn.execute(
            "SELECT COUNT(*) as cnt FROM tri_releases WHERE tri_facility_id = ? AND reporting_year = ? AND chemical_name = ?",
            ("TEST001", 2023, "Toluene"),
        ).fetchone()
        assert row["cnt"] == 1


class TestCountyHealth:
    def test_insert(self, db_conn):
        data = {
            "fips_county": "48201",
            "year": 2023,
            "state": "TX",
            "county_name": "Harris",
            "premature_death_rate": 7500.0,
            "poor_health_pct": 18.5,
            "adult_obesity_pct": 31.2,
            "life_expectancy": 78.5,
        }
        row_id = upsert_county_health(data, conn=db_conn)
        assert row_id > 0

    def test_batch_store(self, db_conn):
        records = [
            {"fips_county": "48201", "year": 2023, "state": "TX", "county_name": "Harris"},
            {"fips_county": "48113", "year": 2023, "state": "TX", "county_name": "Dallas"},
        ]
        created, updated = store_county_health_batch(records, conn=db_conn)
        assert created == 2
        assert updated == 0


class TestCountyDemographics:
    def test_insert(self, db_conn):
        data = {
            "fips_county": "48201",
            "year": 2023,
            "state": "TX",
            "county_name": "Harris",
            "total_population": 4713325,
            "median_household_income": 57791.0,
            "poverty_pct": 16.5,
            "pct_white": 28.5,
            "pct_black": 19.7,
            "pct_hispanic": 43.4,
        }
        row_id = upsert_county_demographics(data, conn=db_conn)
        assert row_id > 0


class TestBatchOperations:
    def test_store_facilities_batch(self, db_conn):
        facilities = [
            {"tri_facility_id": "F001", "facility_name": "Plant A", "state": "TX"},
            {"tri_facility_id": "F002", "facility_name": "Plant B", "state": "OH"},
        ]
        created, updated = store_facilities_batch(facilities, conn=db_conn)
        assert created == 2
        assert updated == 0

        # Store again — should update
        created, updated = store_facilities_batch(facilities, conn=db_conn)
        assert created == 0
        assert updated == 2

    def test_store_releases_batch(self, db_conn):
        releases = [
            {"tri_facility_id": "F001", "reporting_year": 2023, "chemical_name": "Benzene", "total_releases_lbs": 100},
            {"tri_facility_id": "F001", "reporting_year": 2023, "chemical_name": "Toluene", "total_releases_lbs": 200},
        ]
        created, updated = store_releases_batch(releases, conn=db_conn)
        assert created == 2
        assert updated == 0


class TestQueryFunctions:
    def test_get_all_facilities_empty(self, db_conn):
        result = get_all_facilities(conn=db_conn)
        assert result == []

    def test_get_all_facilities_with_data(self, db_conn, sample_facility):
        upsert_facility(sample_facility, conn=db_conn)
        result = get_all_facilities(conn=db_conn)
        assert len(result) == 1
        assert result[0]["facility_name"] == "Test Chemical Plant"

    def test_filter_by_state(self, db_conn):
        upsert_facility({"tri_facility_id": "F1", "facility_name": "A", "state": "TX"}, conn=db_conn)
        upsert_facility({"tri_facility_id": "F2", "facility_name": "B", "state": "OH"}, conn=db_conn)

        tx = get_all_facilities(state="TX", conn=db_conn)
        assert len(tx) == 1
        assert tx[0]["state"] == "TX"

    def test_get_facility_releases(self, db_conn, sample_facility, sample_release):
        upsert_facility(sample_facility, conn=db_conn)
        upsert_release(sample_release, conn=db_conn)

        releases = get_facility_releases("TEST001", conn=db_conn)
        assert len(releases) == 1
        assert releases[0]["chemical_name"] == "Toluene"

    def test_get_county_context(self, db_conn):
        upsert_county_health({"fips_county": "48201", "year": 2023, "state": "TX"}, conn=db_conn)
        upsert_county_demographics({"fips_county": "48201", "year": 2023, "state": "TX"}, conn=db_conn)

        ctx = get_county_context("48201", conn=db_conn)
        assert "health" in ctx
        assert "demographics" in ctx

    def test_update_quality_scores(self, db_conn, sample_facility):
        upsert_facility(sample_facility, conn=db_conn)
        count = update_facility_quality_scores({"TEST001": 0.85}, conn=db_conn)
        assert count == 1

        row = db_conn.execute(
            "SELECT quality_score FROM tri_facilities WHERE tri_facility_id = ?",
            ("TEST001",),
        ).fetchone()
        assert row["quality_score"] == 0.85


class TestPipelineRuns:
    def test_start_and_complete(self, db_conn):
        run_id = start_pipeline_run("epa_tri", "download", conn=db_conn)
        assert len(run_id) == 8

        complete_pipeline_run(
            run_id, records_processed=100, records_created=95,
            records_updated=5, conn=db_conn,
        )

        row = db_conn.execute(
            "SELECT * FROM pipeline_runs WHERE run_id = ?", (run_id,)
        ).fetchone()
        assert row["status"] == "completed"
        assert row["records_processed"] == 100


class TestGetStats:
    def test_empty_db(self, db_conn):
        stats = get_stats(conn=db_conn, print_output=False)
        assert stats["total_facilities"] == 0
        assert stats["total_releases"] == 0

    def test_with_data(self, db_conn, sample_facility, sample_release):
        upsert_facility(sample_facility, conn=db_conn)
        upsert_release(sample_release, conn=db_conn)

        stats = get_stats(conn=db_conn, print_output=False)
        assert stats["total_facilities"] == 1
        assert stats["total_releases"] == 1
        assert stats["chemicals_tracked"] == 1
