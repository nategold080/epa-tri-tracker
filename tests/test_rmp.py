"""Tests for RMP (Risk Management Program) enrichment.

Tests cover: RMP facility storage, chemical inventory, accident history,
TRI→RMP cross-linking, and risk scoring integration.
"""

import sqlite3
import pytest
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.storage.database import (
    init_db,
    upsert_facility,
    store_frs_links_batch,
    store_rmp_facilities_batch,
    store_rmp_chemicals_batch,
    store_rmp_accidents_batch,
    store_tri_rmp_links_batch,
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
    """DB with TRI facilities and FRS links for cross-linking."""
    upsert_facility({
        "tri_facility_id": "TRI001",
        "facility_name": "Acme Chemical Plant",
        "street_address": "100 MAIN ST",
        "city": "HOUSTON",
        "state": "TX",
        "latitude": 29.76,
        "longitude": -95.36,
        "fips_county": "48201",
    }, conn=db_conn)
    upsert_facility({
        "tri_facility_id": "TRI002",
        "facility_name": "Steel Works Inc",
        "street_address": "200 INDUSTRIAL BLVD",
        "city": "CLEVELAND",
        "state": "OH",
        "latitude": 41.50,
        "longitude": -81.69,
        "fips_county": "39035",
    }, conn=db_conn)
    # Add FRS link for TRI001
    store_frs_links_batch([{
        "tri_facility_id": "TRI001",
        "registry_id": "FRS_REG_001",
        "program_system_acronym": "TRIS",
    }], conn=db_conn)
    return db_conn


# --- RMP Facility Tests ---

class TestRmpFacilities:
    def test_store_rmp_facilities_creates(self, db_conn):
        facilities = [
            {
                "rmp_id": "RMP001",
                "facility_name": "Test RMP Facility",
                "state": "TX",
                "city": "HOUSTON",
                "latitude": 29.77,
                "longitude": -95.37,
                "frs_registry_id": "FRS_REG_001",
            },
        ]
        c, u = store_rmp_facilities_batch(facilities, conn=db_conn)
        assert c == 1
        assert u == 0

    def test_store_rmp_facilities_upsert(self, db_conn):
        fac = {
            "rmp_id": "RMP001",
            "facility_name": "Test RMP",
            "state": "TX",
        }
        store_rmp_facilities_batch([fac], conn=db_conn)
        fac["facility_name"] = "Updated Name"
        c, u = store_rmp_facilities_batch([fac], conn=db_conn)
        assert c == 0
        assert u == 1
        row = db_conn.execute(
            "SELECT facility_name FROM rmp_facilities WHERE rmp_id = 'RMP001'"
        ).fetchone()
        assert row["facility_name"] == "Updated Name"

    def test_rmp_facility_all_fields(self, db_conn):
        fac = {
            "rmp_id": "RMP002",
            "facility_name": "Full RMP Facility",
            "street_address": "123 Chemical Way",
            "city": "Baytown",
            "state": "TX",
            "zip_code": "77520",
            "latitude": 29.73,
            "longitude": -94.97,
            "frs_registry_id": "FRS123",
            "naics_code": "325110",
            "num_processes": 5,
            "num_chemicals": 12,
            "last_submission_date": "2024-06-15",
            "deregistration_date": None,
        }
        store_rmp_facilities_batch([fac], conn=db_conn)
        row = db_conn.execute(
            "SELECT * FROM rmp_facilities WHERE rmp_id = 'RMP002'"
        ).fetchone()
        assert row["naics_code"] == "325110"
        assert row["num_processes"] == 5
        assert row["num_chemicals"] == 12

    def test_skip_empty_rmp_id(self, db_conn):
        c, u = store_rmp_facilities_batch([{"rmp_id": "", "state": "TX"}], conn=db_conn)
        assert c == 0

    def test_multiple_facilities(self, db_conn):
        facs = [
            {"rmp_id": f"RMP{i:03d}", "facility_name": f"Fac {i}", "state": "TX"}
            for i in range(10)
        ]
        c, u = store_rmp_facilities_batch(facs, conn=db_conn)
        assert c == 10


# --- RMP Chemical Tests ---

class TestRmpChemicals:
    def test_store_chemicals(self, db_conn):
        store_rmp_facilities_batch([{"rmp_id": "RMP001", "state": "TX"}], conn=db_conn)
        chems = [
            {
                "rmp_id": "RMP001",
                "chemical_name": "Chlorine",
                "cas_number": "7782-50-5",
                "quantity_lbs": 50000.0,
                "is_toxic": 1,
                "is_flammable": 0,
                "worst_case_distance_miles": 3.5,
            },
            {
                "rmp_id": "RMP001",
                "chemical_name": "Ammonia",
                "cas_number": "7664-41-7",
                "quantity_lbs": 25000.0,
                "is_toxic": 1,
                "is_flammable": 0,
            },
        ]
        c, u = store_rmp_chemicals_batch(chems, conn=db_conn)
        assert c == 2

    def test_chemical_upsert(self, db_conn):
        store_rmp_facilities_batch([{"rmp_id": "RMP001", "state": "TX"}], conn=db_conn)
        chem = {
            "rmp_id": "RMP001",
            "chemical_name": "Chlorine",
            "quantity_lbs": 50000.0,
        }
        store_rmp_chemicals_batch([chem], conn=db_conn)
        chem["quantity_lbs"] = 75000.0
        c, u = store_rmp_chemicals_batch([chem], conn=db_conn)
        assert u == 1
        row = db_conn.execute(
            "SELECT quantity_lbs FROM rmp_chemicals WHERE rmp_id = 'RMP001' AND chemical_name = 'Chlorine'"
        ).fetchone()
        assert row["quantity_lbs"] == 75000.0

    def test_skip_missing_chemical_name(self, db_conn):
        c, u = store_rmp_chemicals_batch([{"rmp_id": "RMP001"}], conn=db_conn)
        assert c == 0


# --- RMP Accident Tests ---

class TestRmpAccidents:
    def test_store_accidents(self, db_conn):
        store_rmp_facilities_batch([{"rmp_id": "RMP001", "state": "TX"}], conn=db_conn)
        accidents = [
            {
                "rmp_id": "RMP001",
                "accident_date": "2023-05-15",
                "chemical_name": "Chlorine",
                "quantity_released_lbs": 500.0,
                "release_event": "gas_release",
                "deaths_workers": 0,
                "deaths_public": 0,
                "injuries_workers": 3,
                "injuries_public": 12,
                "evacuations": 500,
                "property_damage_usd": 250000.0,
            },
        ]
        c, u = store_rmp_accidents_batch(accidents, conn=db_conn)
        assert c == 1

    def test_accident_defaults(self, db_conn):
        store_rmp_facilities_batch([{"rmp_id": "RMP001", "state": "TX"}], conn=db_conn)
        store_rmp_accidents_batch([{
            "rmp_id": "RMP001",
            "accident_date": "2022-01-01",
        }], conn=db_conn)
        row = db_conn.execute(
            "SELECT * FROM rmp_accidents WHERE rmp_id = 'RMP001'"
        ).fetchone()
        assert row["deaths_workers"] == 0
        assert row["injuries_public"] == 0

    def test_multiple_accidents(self, db_conn):
        store_rmp_facilities_batch([{"rmp_id": "RMP001", "state": "TX"}], conn=db_conn)
        accs = [
            {"rmp_id": "RMP001", "accident_date": f"2023-0{i}-01"}
            for i in range(1, 6)
        ]
        c, u = store_rmp_accidents_batch(accs, conn=db_conn)
        assert c == 5


# --- TRI→RMP Cross-Link Tests ---

class TestTriRmpLinks:
    def test_store_links(self, seeded_db):
        store_rmp_facilities_batch([{
            "rmp_id": "RMP001",
            "facility_name": "Acme RMP",
            "frs_registry_id": "FRS_REG_001",
            "state": "TX",
        }], conn=seeded_db)
        links = [{
            "tri_facility_id": "TRI001",
            "rmp_id": "RMP001",
            "link_method": "frs_registry",
            "confidence": 1.0,
        }]
        count = store_tri_rmp_links_batch(links, conn=seeded_db)
        assert count >= 1

    def test_duplicate_link_ignored(self, seeded_db):
        store_rmp_facilities_batch([{
            "rmp_id": "RMP001",
            "frs_registry_id": "FRS_REG_001",
            "state": "TX",
        }], conn=seeded_db)
        link = {
            "tri_facility_id": "TRI001",
            "rmp_id": "RMP001",
            "link_method": "frs_registry",
            "confidence": 1.0,
        }
        store_tri_rmp_links_batch([link], conn=seeded_db)
        # Insert again — should be ignored
        store_tri_rmp_links_batch([link], conn=seeded_db)
        count = seeded_db.execute(
            "SELECT COUNT(*) as cnt FROM tri_rmp_links"
        ).fetchone()["cnt"]
        assert count == 1

    def test_frs_cross_link(self, seeded_db):
        """FRS registry ID matching: TRI→FRS→RMP."""
        store_rmp_facilities_batch([{
            "rmp_id": "RMP001",
            "frs_registry_id": "FRS_REG_001",
            "state": "TX",
            "city": "HOUSTON",
        }], conn=seeded_db)

        from src.normalization.rmp_linker import build_tri_rmp_links
        links = build_tri_rmp_links(conn=seeded_db)
        assert len(links) >= 1
        assert links[0]["link_method"] == "frs_registry"
        assert links[0]["confidence"] == 1.0
        assert links[0]["tri_facility_id"] == "TRI001"
        assert links[0]["rmp_id"] == "RMP001"

    def test_address_cross_link(self, seeded_db):
        """Address matching when FRS link doesn't exist."""
        store_rmp_facilities_batch([{
            "rmp_id": "RMP002",
            "facility_name": "Different Name",
            "street_address": "200 INDUSTRIAL BLVD",
            "city": "CLEVELAND",
            "state": "OH",
        }], conn=seeded_db)
        from src.normalization.rmp_linker import build_tri_rmp_links
        links = build_tri_rmp_links(conn=seeded_db)
        addr_links = [l for l in links if l["link_method"] == "address_match"]
        assert len(addr_links) >= 1
        assert addr_links[0]["tri_facility_id"] == "TRI002"

    def test_name_cross_link(self, seeded_db):
        """Name + city + state matching as fallback."""
        store_rmp_facilities_batch([{
            "rmp_id": "RMP003",
            "facility_name": "Steel Works",
            "street_address": "999 OTHER ROAD",
            "city": "CLEVELAND",
            "state": "OH",
        }], conn=seeded_db)
        from src.normalization.rmp_linker import build_tri_rmp_links
        links = build_tri_rmp_links(conn=seeded_db)
        name_links = [l for l in links if l["link_method"] == "name_match"]
        assert len(name_links) >= 1


# --- RMP Schema Tests ---

class TestRmpSchema:
    def test_rmp_tables_exist(self, db_conn):
        tables = db_conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
        table_names = [t["name"] for t in tables]
        assert "rmp_facilities" in table_names
        assert "rmp_chemicals" in table_names
        assert "rmp_accidents" in table_names
        assert "tri_rmp_links" in table_names

    def test_rmp_indexes_exist(self, db_conn):
        indexes = db_conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index'"
        ).fetchall()
        idx_names = [i["name"] for i in indexes]
        assert "idx_rmp_frs" in idx_names
        assert "idx_rmp_state" in idx_names
        assert "idx_rmp_chem_facility" in idx_names
        assert "idx_tri_rmp_tri" in idx_names


# --- RMP Downloader Parse Tests ---

class TestRmpDownloader:
    def test_safe_float(self):
        from src.scrapers.rmp_downloader import _safe_float
        assert _safe_float("123.45") == 123.45
        assert _safe_float("1,234.5") == 1234.5
        assert _safe_float("") is None
        assert _safe_float(None) is None
        assert _safe_float("N/A") is None

    def test_safe_int(self):
        from src.scrapers.rmp_downloader import _safe_int
        assert _safe_int("42") == 42
        assert _safe_int("1,234") == 1234
        assert _safe_int("") is None
        assert _safe_int(None) is None

    def test_clean_str(self):
        from src.scrapers.rmp_downloader import _clean_str
        assert _clean_str("  Hello  ") == "Hello"
        assert _clean_str("") is None
        assert _clean_str(None) is None
        assert _clean_str("None") is None


# --- RMP Linker Normalization Tests ---

class TestRmpLinkerNormalization:
    def test_normalize_address(self):
        from src.normalization.rmp_linker import _normalize_address
        assert _normalize_address("123 Main Street") == "123 MAIN ST"
        assert _normalize_address("456 North Avenue") == "456 N AVE"
        assert _normalize_address("789 West Highway 10") == "789 W HWY 10"

    def test_normalize_name(self):
        from src.normalization.rmp_linker import _normalize_name
        assert "LLC" not in _normalize_name("Acme Chemical LLC")
        assert "INC" not in _normalize_name("Steel Works Inc.")
        assert "CORP" not in _normalize_name("Big Corp Corporation")

    def test_normalize_address_empty(self):
        from src.normalization.rmp_linker import _normalize_address
        assert _normalize_address("") == ""
        assert _normalize_address(None) == ""

    def test_normalize_name_empty(self):
        from src.normalization.rmp_linker import _normalize_name
        assert _normalize_name("") == ""
        assert _normalize_name(None) == ""


# --- Integration: RMP + TRI Combined Query ---

class TestRmpIntegration:
    def test_facility_with_rmp_data(self, seeded_db):
        """Verify a facility can have both TRI releases and RMP accident data."""
        store_rmp_facilities_batch([{
            "rmp_id": "RMP001",
            "frs_registry_id": "FRS_REG_001",
            "state": "TX",
        }], conn=seeded_db)
        store_rmp_chemicals_batch([{
            "rmp_id": "RMP001",
            "chemical_name": "Chlorine",
            "quantity_lbs": 50000,
        }], conn=seeded_db)
        store_rmp_accidents_batch([{
            "rmp_id": "RMP001",
            "accident_date": "2023-01-15",
            "release_event": "gas_release",
        }], conn=seeded_db)
        store_tri_rmp_links_batch([{
            "tri_facility_id": "TRI001",
            "rmp_id": "RMP001",
            "link_method": "frs_registry",
            "confidence": 1.0,
        }], conn=seeded_db)

        # Query: facilities with both TRI and RMP data
        row = seeded_db.execute("""
            SELECT f.tri_facility_id, f.facility_name,
                   rf.rmp_id, rf.facility_name as rmp_name,
                   COUNT(DISTINCT rc.chemical_name) as rmp_chemicals,
                   COUNT(DISTINCT ra.accident_id) as rmp_accidents
            FROM tri_facilities f
            JOIN tri_rmp_links trl ON f.tri_facility_id = trl.tri_facility_id
            JOIN rmp_facilities rf ON trl.rmp_id = rf.rmp_id
            LEFT JOIN rmp_chemicals rc ON rf.rmp_id = rc.rmp_id
            LEFT JOIN rmp_accidents ra ON rf.rmp_id = ra.rmp_id
            WHERE f.tri_facility_id = 'TRI001'
            GROUP BY f.tri_facility_id
        """).fetchone()
        assert row is not None
        assert row["rmp_chemicals"] == 1
        assert row["rmp_accidents"] == 1
