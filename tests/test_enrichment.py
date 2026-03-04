"""Tests for enrichment phases: ECHO enforcement, Superfund, EJScreen, risk scoring."""

import sqlite3
import math
import pytest
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.storage.database import (
    init_db,
    upsert_facility,
    upsert_release,
    store_frs_links_batch,
    store_enforcement_batch,
    store_inspections_batch,
    store_compliance_batch,
    store_superfund_sites_batch,
    store_superfund_proximity_batch,
    store_ej_indicators_batch,
    get_stats,
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
def seeded_db(db_conn):
    """DB with sample facility and releases."""
    upsert_facility({
        "tri_facility_id": "TEST001",
        "facility_name": "Test Chemical Plant",
        "state": "TX",
        "latitude": 29.76,
        "longitude": -95.36,
        "fips_county": "48201",
        "industry_sector": "Chemical Manufacturing",
    }, conn=db_conn)
    upsert_facility({
        "tri_facility_id": "TEST002",
        "facility_name": "Steel Works Inc",
        "state": "OH",
        "latitude": 41.50,
        "longitude": -81.69,
        "fips_county": "39035",
        "industry_sector": "Primary Metals",
    }, conn=db_conn)
    upsert_release({
        "tri_facility_id": "TEST001",
        "reporting_year": 2023,
        "chemical_name": "Toluene",
        "total_releases_lbs": 5000.0,
        "carcinogen_flag": "NO",
    }, conn=db_conn)
    upsert_release({
        "tri_facility_id": "TEST001",
        "reporting_year": 2022,
        "chemical_name": "Benzene",
        "total_releases_lbs": 1000.0,
        "carcinogen_flag": "YES",
    }, conn=db_conn)
    return db_conn


# ===========================================================
# PHASE 1: Database Schema Tests — New Tables
# ===========================================================

class TestNewTablesCreated:
    def test_tri_frs_links_table_exists(self, db_conn):
        tables = db_conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()
        table_names = [t["name"] for t in tables]
        assert "tri_frs_links" in table_names

    def test_enforcement_actions_table_exists(self, db_conn):
        tables = db_conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()
        table_names = [t["name"] for t in tables]
        assert "enforcement_actions" in table_names

    def test_facility_inspections_table_exists(self, db_conn):
        tables = db_conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()
        table_names = [t["name"] for t in tables]
        assert "facility_inspections" in table_names

    def test_compliance_status_table_exists(self, db_conn):
        tables = db_conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()
        table_names = [t["name"] for t in tables]
        assert "compliance_status" in table_names

    def test_superfund_sites_table_exists(self, db_conn):
        tables = db_conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()
        table_names = [t["name"] for t in tables]
        assert "superfund_sites" in table_names

    def test_tri_superfund_proximity_table_exists(self, db_conn):
        tables = db_conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()
        table_names = [t["name"] for t in tables]
        assert "tri_superfund_proximity" in table_names

    def test_new_indexes_created(self, db_conn):
        indexes = db_conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND name LIKE 'idx_%'"
        ).fetchall()
        idx_names = {i["name"] for i in indexes}
        assert "idx_enforcement_registry" in idx_names
        assert "idx_inspections_registry" in idx_names
        assert "idx_frs_links_tri" in idx_names
        assert "idx_superfund_state" in idx_names


# ===========================================================
# PHASE 1: FRS Links
# ===========================================================

class TestFrsLinks:
    def test_store_frs_links(self, db_conn):
        links = [
            {"tri_facility_id": "TEST001", "registry_id": "REG001", "program_system_acronym": "TRIS"},
            {"tri_facility_id": "TEST002", "registry_id": "REG002", "program_system_acronym": "TRIS"},
        ]
        count = store_frs_links_batch(links, conn=db_conn)
        assert count == 2

    def test_frs_links_ignore_duplicates(self, db_conn):
        links = [
            {"tri_facility_id": "TEST001", "registry_id": "REG001", "program_system_acronym": "TRIS"},
        ]
        store_frs_links_batch(links, conn=db_conn)
        count = store_frs_links_batch(links, conn=db_conn)
        # Should still work (INSERT OR IGNORE)
        row = db_conn.execute("SELECT COUNT(*) as cnt FROM tri_frs_links").fetchone()
        assert row["cnt"] == 1

    def test_frs_links_query(self, db_conn):
        links = [
            {"tri_facility_id": "TEST001", "registry_id": "REG001", "program_system_acronym": "TRIS"},
            {"tri_facility_id": "TEST001", "registry_id": "REG002", "program_system_acronym": "TRIS"},
        ]
        store_frs_links_batch(links, conn=db_conn)
        rows = db_conn.execute(
            "SELECT registry_id FROM tri_frs_links WHERE tri_facility_id = ?", ("TEST001",)
        ).fetchall()
        assert len(rows) == 2


# ===========================================================
# PHASE 1: Enforcement Actions
# ===========================================================

class TestEnforcementActions:
    def test_store_enforcement(self, db_conn):
        records = [
            {
                "case_number": "ENF001",
                "registry_id": "REG001",
                "case_name": "Test Case 1",
                "enforcement_type": "CAA",
                "penalty_amount": 50000.0,
                "enforcement_outcome": "Consent Order",
            },
            {
                "case_number": "ENF002",
                "registry_id": "REG001",
                "case_name": "Test Case 2",
                "enforcement_type": "CWA",
                "penalty_amount": 25000.0,
            },
        ]
        created, updated = store_enforcement_batch(records, conn=db_conn)
        assert created == 2
        assert updated == 0

    def test_enforcement_upsert(self, db_conn):
        records = [
            {"case_number": "ENF001", "registry_id": "REG001", "penalty_amount": 50000.0},
        ]
        store_enforcement_batch(records, conn=db_conn)
        records[0]["penalty_amount"] = 75000.0
        created, updated = store_enforcement_batch(records, conn=db_conn)
        assert created == 0
        assert updated == 1

    def test_enforcement_skip_empty_case(self, db_conn):
        records = [
            {"case_number": "", "registry_id": "REG001"},
            {"case_number": None, "registry_id": "REG001"},
        ]
        created, updated = store_enforcement_batch(records, conn=db_conn)
        assert created == 0

    def test_enforcement_penalty_query(self, db_conn):
        records = [
            {"case_number": "ENF001", "registry_id": "REG001", "penalty_amount": 50000.0},
            {"case_number": "ENF002", "registry_id": "REG001", "penalty_amount": 25000.0},
            {"case_number": "ENF003", "registry_id": "REG002", "penalty_amount": None},
        ]
        store_enforcement_batch(records, conn=db_conn)
        row = db_conn.execute("SELECT SUM(penalty_amount) as total FROM enforcement_actions").fetchone()
        assert row["total"] == 75000.0


# ===========================================================
# PHASE 1: Inspections
# ===========================================================

class TestInspections:
    def test_store_inspections(self, db_conn):
        records = [
            {
                "inspection_id": "INSP001",
                "registry_id": "REG001",
                "program": "CWA",
                "inspection_type": "Compliance Evaluation",
                "start_date": "2023-01-15",
                "found_violation": 1,
            },
            {
                "inspection_id": "INSP002",
                "registry_id": "REG001",
                "program": "CAA",
                "start_date": "2023-06-01",
                "found_violation": 0,
            },
        ]
        created, updated = store_inspections_batch(records, conn=db_conn)
        assert created == 2
        assert updated == 0

    def test_inspections_upsert(self, db_conn):
        records = [
            {"inspection_id": "INSP001", "registry_id": "REG001", "program": "CWA", "found_violation": 0},
        ]
        store_inspections_batch(records, conn=db_conn)
        records[0]["found_violation"] = 1
        created, updated = store_inspections_batch(records, conn=db_conn)
        assert created == 0
        assert updated == 1

    def test_inspections_violation_count(self, db_conn):
        records = [
            {"inspection_id": "I1", "registry_id": "R1", "program": "CWA", "found_violation": 1},
            {"inspection_id": "I2", "registry_id": "R1", "program": "CAA", "found_violation": 0},
            {"inspection_id": "I3", "registry_id": "R1", "program": "RCRA", "found_violation": 1},
        ]
        store_inspections_batch(records, conn=db_conn)
        row = db_conn.execute(
            "SELECT SUM(found_violation) as viols FROM facility_inspections WHERE registry_id = 'R1'"
        ).fetchone()
        assert row["viols"] == 2


# ===========================================================
# PHASE 1: Compliance Status
# ===========================================================

class TestComplianceStatus:
    def test_store_compliance(self, db_conn):
        records = [
            {"registry_id": "REG001", "program": "CAA", "status": "Significant Non-Compliance", "quarters_in_nc": 4},
            {"registry_id": "REG001", "program": "CWA", "status": "In Compliance", "quarters_in_nc": 0},
        ]
        count = store_compliance_batch(records, conn=db_conn)
        assert count == 2

    def test_compliance_upsert(self, db_conn):
        records = [
            {"registry_id": "REG001", "program": "CAA", "status": "Violation", "quarters_in_nc": 2},
        ]
        store_compliance_batch(records, conn=db_conn)
        records[0]["status"] = "Significant Non-Compliance"
        records[0]["quarters_in_nc"] = 5
        count = store_compliance_batch(records, conn=db_conn)
        assert count == 1
        row = db_conn.execute(
            "SELECT status, quarters_in_nc FROM compliance_status WHERE registry_id = 'REG001' AND program = 'CAA'"
        ).fetchone()
        assert row["status"] == "Significant Non-Compliance"
        assert row["quarters_in_nc"] == 5


# ===========================================================
# PHASE 1: Enforcement Linker
# ===========================================================

class TestEnforcementLinker:
    def test_facility_enforcement_summary(self, seeded_db):
        from src.normalization.enforcement_linker import get_facility_enforcement_summary

        # Link facility to FRS
        store_frs_links_batch([
            {"tri_facility_id": "TEST001", "registry_id": "REG001", "program_system_acronym": "TRIS"},
        ], conn=seeded_db)

        # Add enforcement
        store_enforcement_batch([
            {"case_number": "ENF001", "registry_id": "REG001", "penalty_amount": 50000.0},
        ], conn=seeded_db)

        # Add inspection
        store_inspections_batch([
            {"inspection_id": "I1", "registry_id": "REG001", "program": "CWA", "found_violation": 1},
        ], conn=seeded_db)

        summary = get_facility_enforcement_summary("TEST001", conn=seeded_db)
        assert summary["enforcement_count"] == 1
        assert summary["inspection_count"] == 1
        assert summary["total_penalties"] == 50000.0
        assert summary["violation_count"] == 1
        assert summary["has_enforcement"] is True

    def test_facility_no_enforcement(self, seeded_db):
        from src.normalization.enforcement_linker import get_facility_enforcement_summary
        summary = get_facility_enforcement_summary("TEST002", conn=seeded_db)
        assert summary["enforcement_count"] == 0
        assert summary["has_enforcement"] is False

    def test_risk_score_low_risk(self):
        from src.normalization.enforcement_linker import compute_facility_risk_score, get_risk_tier

        score = compute_facility_risk_score(
            facility={"tri_facility_id": "T1"},
            enforcement_summary={"enforcement_count": 0, "violation_count": 0, "total_penalties": 0},
            release_stats={"total_releases_lbs": 100, "carcinogen_lbs": 0},
        )
        assert score >= 0.7
        assert get_risk_tier(score) in ("LOW", "MEDIUM")

    def test_risk_score_high_risk(self):
        from src.normalization.enforcement_linker import compute_facility_risk_score, get_risk_tier

        score = compute_facility_risk_score(
            facility={"tri_facility_id": "T1"},
            enforcement_summary={"enforcement_count": 15, "violation_count": 10, "total_penalties": 500000},
            release_stats={"total_releases_lbs": 5000000, "carcinogen_lbs": 2000000},
            ej_data={"ej_index_pctl": 95},
            trend_data={"trend_pct": 50},
        )
        assert score < 0.4
        assert get_risk_tier(score) in ("HIGH", "CRITICAL")

    def test_risk_tier_boundaries(self):
        from src.normalization.enforcement_linker import get_risk_tier
        assert get_risk_tier(0.9) == "LOW"
        assert get_risk_tier(0.8) == "LOW"
        assert get_risk_tier(0.6) == "MEDIUM"
        assert get_risk_tier(0.5) == "MEDIUM"
        assert get_risk_tier(0.4) == "HIGH"
        assert get_risk_tier(0.3) == "HIGH"
        assert get_risk_tier(0.2) == "CRITICAL"
        assert get_risk_tier(0.0) == "CRITICAL"


# ===========================================================
# PHASE 1: ECHO Downloader Parsing
# ===========================================================

class TestEchoDownloaderParsing:
    def test_detect_enforcement_type_caa(self):
        from src.scrapers.echo_downloader import _detect_enforcement_type
        row = {"ENF_STATUTE": "CAA - Clean Air Act"}
        assert _detect_enforcement_type(row) == "CAA"

    def test_detect_enforcement_type_cwa(self):
        from src.scrapers.echo_downloader import _detect_enforcement_type
        row = {"ENF_STATUTE": "CWA/NPDES", "STATUTE_CODE": ""}
        assert _detect_enforcement_type(row) == "CWA"

    def test_detect_enforcement_type_rcra(self):
        from src.scrapers.echo_downloader import _detect_enforcement_type
        row = {"ENF_STATUTE": "RCRA", "STATUTE_CODE": ""}
        assert _detect_enforcement_type(row) == "RCRA"

    def test_detect_enforcement_type_unknown(self):
        from src.scrapers.echo_downloader import _detect_enforcement_type
        row = {"ENF_STATUTE": "", "STATUTE_CODE": ""}
        result = _detect_enforcement_type(row)
        assert result is None

    def test_extract_compliance_status(self):
        from src.scrapers.echo_downloader import extract_compliance_status
        enforcement = [
            {"registry_id": "R1", "enforcement_type": "CAA", "settlement_date": "2023-01-01"},
            {"registry_id": "R1", "enforcement_type": "CAA", "settlement_date": "2023-06-01"},
            {"registry_id": "R1", "enforcement_type": "CAA", "settlement_date": "2023-09-01"},
        ]
        inspections = []
        result = extract_compliance_status(enforcement, inspections)
        assert len(result) == 1
        assert result[0]["status"] == "Significant Non-Compliance"
        assert result[0]["quarters_in_nc"] == 3

    def test_extract_compliance_from_inspections(self):
        from src.scrapers.echo_downloader import extract_compliance_status
        enforcement = []
        inspections = [
            {"registry_id": "R1", "program": "CWA", "found_violation": 1, "start_date": "2023-03-01"},
        ]
        result = extract_compliance_status(enforcement, inspections)
        assert len(result) == 1
        assert result[0]["status"] == "Violation"


# ===========================================================
# PHASE 3: Superfund Proximity
# ===========================================================

class TestSuperfundSites:
    def test_store_superfund_sites(self, db_conn):
        sites = [
            {"site_id": "SF001", "site_name": "Test Superfund", "state": "TX",
             "latitude": 29.8, "longitude": -95.4, "npl_status": "Final"},
            {"site_id": "SF002", "site_name": "Another Site", "state": "OH",
             "latitude": 41.5, "longitude": -81.7, "npl_status": "Proposed"},
        ]
        created, updated = store_superfund_sites_batch(sites, conn=db_conn)
        assert created == 2
        assert updated == 0

    def test_superfund_upsert(self, db_conn):
        sites = [
            {"site_id": "SF001", "site_name": "Test Superfund", "state": "TX",
             "npl_status": "Proposed"},
        ]
        store_superfund_sites_batch(sites, conn=db_conn)
        sites[0]["npl_status"] = "Final"
        created, updated = store_superfund_sites_batch(sites, conn=db_conn)
        assert created == 0
        assert updated == 1

    def test_store_proximity(self, db_conn):
        records = [
            {"tri_facility_id": "TEST001", "site_id": "SF001", "distance_miles": 2.5, "same_county": 1},
            {"tri_facility_id": "TEST001", "site_id": "SF002", "distance_miles": 4.8, "same_county": 0},
        ]
        count = store_superfund_proximity_batch(records, conn=db_conn)
        assert count == 2


class TestHaversineDistance:
    def test_same_point(self):
        from src.scrapers.superfund_downloader import haversine_distance
        d = haversine_distance(29.76, -95.36, 29.76, -95.36)
        assert d == 0.0

    def test_known_distance(self):
        from src.scrapers.superfund_downloader import haversine_distance
        # Houston to Dallas: ~225 miles
        d = haversine_distance(29.76, -95.36, 32.78, -96.80)
        assert 220 < d < 240

    def test_short_distance(self):
        from src.scrapers.superfund_downloader import haversine_distance
        # ~1 mile offset
        d = haversine_distance(29.76, -95.36, 29.775, -95.36)
        assert 0.5 < d < 2.0


class TestProximityComputation:
    def test_compute_proximity_finds_nearby(self):
        from src.scrapers.superfund_downloader import compute_proximity
        facilities = [
            {"tri_facility_id": "F1", "latitude": 29.76, "longitude": -95.36, "fips_county": "48201"},
        ]
        sites = [
            {"site_id": "S1", "latitude": 29.77, "longitude": -95.37},  # Very close
            {"site_id": "S2", "latitude": 40.0, "longitude": -74.0},   # Far away
        ]
        result = compute_proximity(facilities, sites, radius_miles=5.0)
        assert len(result) == 1
        assert result[0]["site_id"] == "S1"
        assert result[0]["distance_miles"] < 5.0

    def test_compute_proximity_empty_inputs(self):
        from src.scrapers.superfund_downloader import compute_proximity
        result = compute_proximity([], [], radius_miles=5.0)
        assert result == []

    def test_compute_proximity_no_coords(self):
        from src.scrapers.superfund_downloader import compute_proximity
        facilities = [
            {"tri_facility_id": "F1", "latitude": None, "longitude": None},
        ]
        sites = [
            {"site_id": "S1", "latitude": 29.77, "longitude": -95.37},
        ]
        result = compute_proximity(facilities, sites, radius_miles=5.0)
        assert len(result) == 0


# ===========================================================
# PHASE 4: EJ Indicators
# ===========================================================

class TestEjIndicators:
    def test_store_ej_indicators(self, db_conn):
        records = [
            {
                "fips_tract": "48201000100",
                "fips_county": "48201",
                "state": "TX",
                "ej_index_pctl": 75.5,
                "pm25_pctl": 60.0,
                "low_income_pctl": 80.0,
                "people_of_color_pctl": 65.0,
            },
            {
                "fips_tract": "48201000200",
                "fips_county": "48201",
                "state": "TX",
                "ej_index_pctl": 45.0,
                "pm25_pctl": 30.0,
            },
        ]
        created, updated = store_ej_indicators_batch(records, conn=db_conn)
        assert created == 2
        assert updated == 0

    def test_ej_indicators_upsert(self, db_conn):
        records = [
            {"fips_tract": "48201000100", "fips_county": "48201", "ej_index_pctl": 50.0},
        ]
        store_ej_indicators_batch(records, conn=db_conn)
        records[0]["ej_index_pctl"] = 75.0
        created, updated = store_ej_indicators_batch(records, conn=db_conn)
        assert created == 0
        assert updated == 1

    def test_ej_county_lookup(self, db_conn):
        records = [
            {"fips_tract": "48201000100", "fips_county": "48201", "ej_index_pctl": 75.0},
            {"fips_tract": "48201000200", "fips_county": "48201", "ej_index_pctl": 45.0},
            {"fips_tract": "39035000100", "fips_county": "39035", "ej_index_pctl": 60.0},
        ]
        store_ej_indicators_batch(records, conn=db_conn)
        rows = db_conn.execute(
            "SELECT COUNT(*) as cnt FROM ej_indicators WHERE fips_county = '48201'"
        ).fetchone()
        assert rows["cnt"] == 2


# ===========================================================
# Updated Quality Scoring
# ===========================================================

class TestUpdatedQualityScoring:
    def test_enforcement_component_scored(self):
        from src.validation.quality import score_facility
        fac = {"tri_facility_id": "T1", "facility_name": "Test", "state": "TX"}
        without = score_facility(fac, has_enforcement=False)
        with_enf = score_facility(fac, has_enforcement=True)
        assert with_enf["quality_score"] > without["quality_score"]
        assert with_enf["component_scores"]["has_enforcement_data"] == 1.0
        assert without["component_scores"]["has_enforcement_data"] == 0.0

    def test_historical_component_scored(self):
        from src.validation.quality import score_facility
        fac = {"tri_facility_id": "T1", "facility_name": "Test", "state": "TX"}
        without = score_facility(fac, has_historical=False)
        with_hist = score_facility(fac, has_historical=True)
        assert with_hist["quality_score"] > without["quality_score"]
        assert with_hist["component_scores"]["has_historical_trend"] == 1.0

    def test_all_components_full_score(self):
        from src.validation.quality import score_facility, WEIGHTS
        fac = {
            "tri_facility_id": "T1",
            "facility_name": "Test",
            "canonical_name": "Test",
            "state": "TX",
            "latitude": 29.76,
            "longitude": -95.36,
            "fips_county": "48201",
            "industry_sector": "Chemical Manufacturing",
        }
        result = score_facility(
            fac,
            has_health_data=True,
            has_demographics=True,
            has_ej_data=True,
            has_enforcement=True,
            has_historical=True,
            release_count=10,
            total_releases_lbs=50000,
        )
        assert result["quality_score"] == 1.0
        assert len(result["component_scores"]) == len(WEIGHTS)

    def test_weights_sum_to_one(self):
        from src.validation.quality import WEIGHTS
        total = sum(WEIGHTS.values())
        assert abs(total - 1.0) < 0.001


# ===========================================================
# Stats with Enrichment Data
# ===========================================================

class TestStatsWithEnrichment:
    def test_stats_includes_enforcement(self, seeded_db):
        store_frs_links_batch([
            {"tri_facility_id": "TEST001", "registry_id": "REG001"},
        ], conn=seeded_db)
        store_enforcement_batch([
            {"case_number": "ENF001", "registry_id": "REG001", "penalty_amount": 50000.0},
        ], conn=seeded_db)

        stats = get_stats(conn=seeded_db, print_output=False)
        assert stats["frs_links"] == 1
        assert stats["enforcement_actions"] == 1
        assert stats["total_penalties"] == 50000.0

    def test_stats_includes_superfund(self, db_conn):
        store_superfund_sites_batch([
            {"site_id": "SF001", "site_name": "Test", "state": "TX"},
        ], conn=db_conn)
        stats = get_stats(conn=db_conn, print_output=False)
        assert stats["superfund_sites"] == 1

    def test_stats_includes_inspections(self, db_conn):
        store_inspections_batch([
            {"inspection_id": "I1", "registry_id": "R1", "program": "CWA"},
        ], conn=db_conn)
        stats = get_stats(conn=db_conn, print_output=False)
        assert stats["facility_inspections"] == 1
