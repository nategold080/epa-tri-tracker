"""SQLite storage layer for EPA TRI Community Health & Demographics Tracker.

Uses WAL mode for concurrent reads. All database functions accept an
optional `conn` parameter for testability and transaction control.
"""

from __future__ import annotations

import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from rich.console import Console
from rich.table import Table

PROJECT_ROOT = Path(__file__).parent.parent.parent
DB_PATH = PROJECT_ROOT / "data" / "epa_tri_tracker.db"

console = Console()


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS tri_facilities (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tri_facility_id TEXT NOT NULL,
    facility_name TEXT NOT NULL,
    canonical_name TEXT,
    street_address TEXT,
    city TEXT,
    county TEXT,
    state TEXT NOT NULL,
    zip_code TEXT,
    latitude REAL,
    longitude REAL,
    fips_state TEXT,
    fips_county TEXT,
    sic_code TEXT,
    sic_name TEXT,
    naics_code TEXT,
    industry_sector TEXT,
    parent_company_name TEXT,
    parent_company_db_num TEXT,
    quality_score REAL,
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now')),
    UNIQUE(tri_facility_id)
);

CREATE TABLE IF NOT EXISTS tri_releases (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tri_facility_id TEXT NOT NULL,
    reporting_year INTEGER NOT NULL,
    chemical_name TEXT NOT NULL,
    cas_number TEXT,
    carcinogen_flag TEXT,
    classification TEXT,
    unit_of_measure TEXT DEFAULT 'Pounds',
    total_releases_lbs REAL,
    fugitive_air_lbs REAL,
    stack_air_lbs REAL,
    water_lbs REAL,
    land_lbs REAL,
    underground_injection_lbs REAL,
    off_site_transfers_lbs REAL,
    on_site_release_total REAL,
    off_site_release_total REAL,
    source TEXT DEFAULT 'epa_tri',
    created_at TEXT DEFAULT (datetime('now')),
    UNIQUE(tri_facility_id, reporting_year, chemical_name)
);

CREATE TABLE IF NOT EXISTS county_health (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    fips_county TEXT NOT NULL,
    year INTEGER NOT NULL,
    state TEXT,
    county_name TEXT,
    premature_death_rate REAL,
    poor_health_pct REAL,
    poor_physical_health_days REAL,
    poor_mental_health_days REAL,
    low_birthweight_pct REAL,
    adult_smoking_pct REAL,
    adult_obesity_pct REAL,
    physical_inactivity_pct REAL,
    excessive_drinking_pct REAL,
    uninsured_pct REAL,
    preventable_hospital_stays REAL,
    life_expectancy REAL,
    child_mortality_rate REAL,
    infant_mortality_rate REAL,
    source TEXT DEFAULT 'county_health_rankings',
    created_at TEXT DEFAULT (datetime('now')),
    UNIQUE(fips_county, year)
);

CREATE TABLE IF NOT EXISTS county_demographics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    fips_county TEXT NOT NULL,
    year INTEGER NOT NULL,
    state TEXT,
    county_name TEXT,
    total_population INTEGER,
    median_household_income REAL,
    poverty_pct REAL,
    unemployment_pct REAL,
    pct_white REAL,
    pct_black REAL,
    pct_hispanic REAL,
    pct_asian REAL,
    pct_native REAL,
    pct_under_18 REAL,
    pct_over_65 REAL,
    pct_no_highschool REAL,
    rural_pct REAL,
    source TEXT DEFAULT 'census_acs',
    created_at TEXT DEFAULT (datetime('now')),
    UNIQUE(fips_county, year)
);

CREATE TABLE IF NOT EXISTS ej_indicators (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    fips_tract TEXT,
    fips_county TEXT NOT NULL,
    state TEXT,
    ej_index_pctl REAL,
    pm25_pctl REAL,
    ozone_pctl REAL,
    diesel_pm_pctl REAL,
    air_toxics_cancer_risk_pctl REAL,
    respiratory_hazard_pctl REAL,
    traffic_proximity_pctl REAL,
    superfund_proximity_pctl REAL,
    rmp_proximity_pctl REAL,
    wastewater_pctl REAL,
    low_income_pctl REAL,
    people_of_color_pctl REAL,
    linguistic_isolation_pctl REAL,
    under_5_pctl REAL,
    over_64_pctl REAL,
    source TEXT DEFAULT 'ejscreen',
    created_at TEXT DEFAULT (datetime('now')),
    UNIQUE(fips_tract)
);

-- Phase 1: ECHO Enforcement & Compliance tables

CREATE TABLE IF NOT EXISTS tri_frs_links (
    tri_facility_id TEXT NOT NULL,
    registry_id TEXT NOT NULL,
    program_system_acronym TEXT,
    PRIMARY KEY (tri_facility_id, registry_id)
);

CREATE TABLE IF NOT EXISTS enforcement_actions (
    case_number TEXT PRIMARY KEY,
    registry_id TEXT NOT NULL,
    case_name TEXT,
    activity_type TEXT,
    enforcement_type TEXT,
    lead_agency TEXT,
    case_status TEXT,
    settlement_date TEXT,
    penalty_amount REAL,
    fed_penalty_assessed REAL,
    state_local_penalty REAL,
    compliance_action_cost REAL,
    enforcement_outcome TEXT,
    quality_score REAL,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS facility_inspections (
    inspection_id TEXT PRIMARY KEY,
    registry_id TEXT NOT NULL,
    program TEXT,
    inspection_type TEXT,
    start_date TEXT,
    end_date TEXT,
    lead_agency TEXT,
    found_violation INTEGER DEFAULT 0,
    quality_score REAL,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS compliance_status (
    registry_id TEXT NOT NULL,
    program TEXT NOT NULL,
    status TEXT,
    status_date TEXT,
    quarters_in_nc INTEGER,  -- NOTE: stores violation event count, not literal quarters
    PRIMARY KEY (registry_id, program)
);

-- Phase 3: Superfund/NPL Site Proximity tables

CREATE TABLE IF NOT EXISTS superfund_sites (
    site_id TEXT PRIMARY KEY,
    site_name TEXT,
    address TEXT,
    city TEXT,
    state TEXT,
    zip_code TEXT,
    latitude REAL,
    longitude REAL,
    npl_status TEXT,
    federal_facility INTEGER DEFAULT 0,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS tri_superfund_proximity (
    tri_facility_id TEXT NOT NULL,
    site_id TEXT NOT NULL,
    distance_miles REAL,
    same_county INTEGER DEFAULT 0,
    PRIMARY KEY (tri_facility_id, site_id)
);

-- Phase 5: RMP (Risk Management Program) tables

CREATE TABLE IF NOT EXISTS rmp_facilities (
    rmp_id TEXT PRIMARY KEY,
    facility_name TEXT,
    street_address TEXT,
    city TEXT,
    state TEXT,
    zip_code TEXT,
    latitude REAL,
    longitude REAL,
    frs_registry_id TEXT,
    naics_code TEXT,
    num_processes INTEGER,
    num_chemicals INTEGER,
    last_submission_date TEXT,
    deregistration_date TEXT,
    quality_score REAL,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS rmp_chemicals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    rmp_id TEXT NOT NULL,
    chemical_name TEXT,
    cas_number TEXT,
    quantity_lbs REAL,
    is_toxic INTEGER DEFAULT 0,
    is_flammable INTEGER DEFAULT 0,
    worst_case_scenario TEXT,
    worst_case_distance_miles REAL,
    alt_case_distance_miles REAL,
    quality_score REAL,
    created_at TEXT DEFAULT (datetime('now')),
    UNIQUE(rmp_id, chemical_name)
);

CREATE TABLE IF NOT EXISTS rmp_accidents (
    accident_id INTEGER PRIMARY KEY AUTOINCREMENT,
    rmp_id TEXT NOT NULL,
    accident_date TEXT,
    chemical_name TEXT,
    cas_number TEXT,
    quantity_released_lbs REAL,
    release_duration_hours REAL,
    release_event TEXT,
    deaths_workers INTEGER DEFAULT 0,
    deaths_public INTEGER DEFAULT 0,
    injuries_workers INTEGER DEFAULT 0,
    injuries_public INTEGER DEFAULT 0,
    evacuations INTEGER DEFAULT 0,
    property_damage_usd REAL,
    quality_score REAL,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS tri_rmp_links (
    tri_facility_id TEXT NOT NULL,
    rmp_id TEXT NOT NULL,
    link_method TEXT,
    confidence REAL,
    PRIMARY KEY (tri_facility_id, rmp_id)
);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL UNIQUE,
    source TEXT NOT NULL,
    stage TEXT NOT NULL,
    status TEXT NOT NULL,
    records_processed INTEGER DEFAULT 0,
    records_created INTEGER DEFAULT 0,
    records_updated INTEGER DEFAULT 0,
    errors INTEGER DEFAULT 0,
    started_at TEXT DEFAULT (datetime('now')),
    completed_at TEXT,
    notes TEXT
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_facilities_tri_id ON tri_facilities(tri_facility_id);
CREATE INDEX IF NOT EXISTS idx_facilities_state ON tri_facilities(state);
CREATE INDEX IF NOT EXISTS idx_facilities_county ON tri_facilities(fips_county);
CREATE INDEX IF NOT EXISTS idx_facilities_sic ON tri_facilities(sic_code);
CREATE INDEX IF NOT EXISTS idx_facilities_industry ON tri_facilities(industry_sector);
CREATE INDEX IF NOT EXISTS idx_facilities_parent ON tri_facilities(parent_company_name);
CREATE INDEX IF NOT EXISTS idx_facilities_quality ON tri_facilities(quality_score);
CREATE INDEX IF NOT EXISTS idx_releases_facility ON tri_releases(tri_facility_id);
CREATE INDEX IF NOT EXISTS idx_releases_year ON tri_releases(reporting_year);
CREATE INDEX IF NOT EXISTS idx_releases_chemical ON tri_releases(chemical_name);
CREATE INDEX IF NOT EXISTS idx_releases_cas ON tri_releases(cas_number);
CREATE INDEX IF NOT EXISTS idx_releases_carcinogen ON tri_releases(carcinogen_flag);
CREATE INDEX IF NOT EXISTS idx_county_health_fips ON county_health(fips_county);
CREATE INDEX IF NOT EXISTS idx_county_health_year ON county_health(year);
CREATE INDEX IF NOT EXISTS idx_county_demo_fips ON county_demographics(fips_county);
CREATE INDEX IF NOT EXISTS idx_county_demo_year ON county_demographics(year);
CREATE INDEX IF NOT EXISTS idx_ej_county ON ej_indicators(fips_county);
CREATE INDEX IF NOT EXISTS idx_ej_tract ON ej_indicators(fips_tract);

-- Phase 1: Enforcement indexes
CREATE INDEX IF NOT EXISTS idx_frs_links_tri ON tri_frs_links(tri_facility_id);
CREATE INDEX IF NOT EXISTS idx_frs_links_registry ON tri_frs_links(registry_id);
CREATE INDEX IF NOT EXISTS idx_enforcement_registry ON enforcement_actions(registry_id);
CREATE INDEX IF NOT EXISTS idx_enforcement_type ON enforcement_actions(enforcement_type);
CREATE INDEX IF NOT EXISTS idx_enforcement_penalty ON enforcement_actions(penalty_amount);
CREATE INDEX IF NOT EXISTS idx_inspections_registry ON facility_inspections(registry_id);
CREATE INDEX IF NOT EXISTS idx_inspections_program ON facility_inspections(program);
CREATE INDEX IF NOT EXISTS idx_compliance_registry ON compliance_status(registry_id);

-- Phase 3: Superfund indexes
CREATE INDEX IF NOT EXISTS idx_superfund_state ON superfund_sites(state);
CREATE INDEX IF NOT EXISTS idx_superfund_proximity_tri ON tri_superfund_proximity(tri_facility_id);
CREATE INDEX IF NOT EXISTS idx_superfund_proximity_site ON tri_superfund_proximity(site_id);

-- Phase 5: RMP indexes
CREATE INDEX IF NOT EXISTS idx_rmp_frs ON rmp_facilities(frs_registry_id);
CREATE INDEX IF NOT EXISTS idx_rmp_state ON rmp_facilities(state);
CREATE INDEX IF NOT EXISTS idx_rmp_chem_facility ON rmp_chemicals(rmp_id);
CREATE INDEX IF NOT EXISTS idx_rmp_chem_cas ON rmp_chemicals(cas_number);
CREATE INDEX IF NOT EXISTS idx_rmp_acc_facility ON rmp_accidents(rmp_id);
CREATE INDEX IF NOT EXISTS idx_rmp_acc_date ON rmp_accidents(accident_date);
CREATE INDEX IF NOT EXISTS idx_tri_rmp_tri ON tri_rmp_links(tri_facility_id);
CREATE INDEX IF NOT EXISTS idx_tri_rmp_rmp ON tri_rmp_links(rmp_id);
""";


def get_connection(db_path: Optional[Path] = None) -> sqlite3.Connection:
    """Get a SQLite connection with WAL mode and row factory."""
    if db_path is None:
        db_path = DB_PATH
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def init_db(conn: Optional[sqlite3.Connection] = None) -> sqlite3.Connection:
    """Initialize the database schema."""
    if conn is None:
        conn = get_connection()
    conn.executescript(SCHEMA_SQL)
    conn.commit()
    return conn


# --- Facility Operations ---

def upsert_facility(data: dict[str, Any], conn: Optional[sqlite3.Connection] = None) -> int:
    """Insert or update a TRI facility record."""
    should_close = conn is None
    if conn is None:
        conn = get_connection()

    now = datetime.now(timezone.utc).isoformat()

    sql = """
        INSERT INTO tri_facilities (
            tri_facility_id, facility_name, canonical_name,
            street_address, city, county, state, zip_code,
            latitude, longitude, fips_state, fips_county,
            sic_code, sic_name, naics_code, industry_sector,
            parent_company_name, parent_company_db_num,
            quality_score, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(tri_facility_id) DO UPDATE SET
            facility_name = COALESCE(excluded.facility_name, tri_facilities.facility_name),
            canonical_name = COALESCE(excluded.canonical_name, tri_facilities.canonical_name),
            street_address = COALESCE(excluded.street_address, tri_facilities.street_address),
            city = COALESCE(excluded.city, tri_facilities.city),
            county = COALESCE(excluded.county, tri_facilities.county),
            state = COALESCE(excluded.state, tri_facilities.state),
            zip_code = COALESCE(excluded.zip_code, tri_facilities.zip_code),
            latitude = COALESCE(excluded.latitude, tri_facilities.latitude),
            longitude = COALESCE(excluded.longitude, tri_facilities.longitude),
            fips_state = COALESCE(excluded.fips_state, tri_facilities.fips_state),
            fips_county = COALESCE(excluded.fips_county, tri_facilities.fips_county),
            sic_code = COALESCE(excluded.sic_code, tri_facilities.sic_code),
            sic_name = COALESCE(excluded.sic_name, tri_facilities.sic_name),
            naics_code = COALESCE(excluded.naics_code, tri_facilities.naics_code),
            industry_sector = COALESCE(excluded.industry_sector, tri_facilities.industry_sector),
            parent_company_name = COALESCE(excluded.parent_company_name, tri_facilities.parent_company_name),
            parent_company_db_num = COALESCE(excluded.parent_company_db_num, tri_facilities.parent_company_db_num),
            quality_score = COALESCE(excluded.quality_score, tri_facilities.quality_score),
            updated_at = excluded.updated_at
    """

    cursor = conn.execute(sql, (
        data.get("tri_facility_id"),
        data.get("facility_name"),
        data.get("canonical_name"),
        data.get("street_address"),
        data.get("city"),
        data.get("county"),
        data.get("state"),
        data.get("zip_code"),
        data.get("latitude"),
        data.get("longitude"),
        data.get("fips_state"),
        data.get("fips_county"),
        data.get("sic_code"),
        data.get("sic_name"),
        data.get("naics_code"),
        data.get("industry_sector"),
        data.get("parent_company_name"),
        data.get("parent_company_db_num"),
        data.get("quality_score"),
        now, now,
    ))
    conn.commit()
    row_id = cursor.lastrowid
    if should_close:
        conn.close()
    return row_id


def upsert_release(data: dict[str, Any], conn: Optional[sqlite3.Connection] = None) -> int:
    """Insert or update a TRI release record."""
    should_close = conn is None
    if conn is None:
        conn = get_connection()

    now = datetime.now(timezone.utc).isoformat()

    sql = """
        INSERT INTO tri_releases (
            tri_facility_id, reporting_year, chemical_name, cas_number,
            carcinogen_flag, classification, unit_of_measure,
            total_releases_lbs, fugitive_air_lbs, stack_air_lbs,
            water_lbs, land_lbs, underground_injection_lbs,
            off_site_transfers_lbs, on_site_release_total, off_site_release_total,
            source, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(tri_facility_id, reporting_year, chemical_name) DO UPDATE SET
            cas_number = COALESCE(excluded.cas_number, tri_releases.cas_number),
            carcinogen_flag = COALESCE(excluded.carcinogen_flag, tri_releases.carcinogen_flag),
            classification = COALESCE(excluded.classification, tri_releases.classification),
            total_releases_lbs = COALESCE(excluded.total_releases_lbs, tri_releases.total_releases_lbs),
            fugitive_air_lbs = COALESCE(excluded.fugitive_air_lbs, tri_releases.fugitive_air_lbs),
            stack_air_lbs = COALESCE(excluded.stack_air_lbs, tri_releases.stack_air_lbs),
            water_lbs = COALESCE(excluded.water_lbs, tri_releases.water_lbs),
            land_lbs = COALESCE(excluded.land_lbs, tri_releases.land_lbs),
            underground_injection_lbs = COALESCE(excluded.underground_injection_lbs, tri_releases.underground_injection_lbs),
            off_site_transfers_lbs = COALESCE(excluded.off_site_transfers_lbs, tri_releases.off_site_transfers_lbs),
            on_site_release_total = COALESCE(excluded.on_site_release_total, tri_releases.on_site_release_total),
            off_site_release_total = COALESCE(excluded.off_site_release_total, tri_releases.off_site_release_total)
    """

    cursor = conn.execute(sql, (
        data.get("tri_facility_id"),
        data.get("reporting_year"),
        data.get("chemical_name"),
        data.get("cas_number"),
        data.get("carcinogen_flag"),
        data.get("classification"),
        data.get("unit_of_measure", "Pounds"),
        data.get("total_releases_lbs"),
        data.get("fugitive_air_lbs"),
        data.get("stack_air_lbs"),
        data.get("water_lbs"),
        data.get("land_lbs"),
        data.get("underground_injection_lbs"),
        data.get("off_site_transfers_lbs"),
        data.get("on_site_release_total"),
        data.get("off_site_release_total"),
        data.get("source", "epa_tri"),
        now,
    ))
    conn.commit()
    row_id = cursor.lastrowid
    if should_close:
        conn.close()
    return row_id


def upsert_county_health(data: dict[str, Any], conn: Optional[sqlite3.Connection] = None) -> int:
    """Insert or update a county health record."""
    should_close = conn is None
    if conn is None:
        conn = get_connection()

    now = datetime.now(timezone.utc).isoformat()

    sql = """
        INSERT INTO county_health (
            fips_county, year, state, county_name,
            premature_death_rate, poor_health_pct, poor_physical_health_days,
            poor_mental_health_days, low_birthweight_pct, adult_smoking_pct,
            adult_obesity_pct, physical_inactivity_pct, excessive_drinking_pct,
            uninsured_pct, preventable_hospital_stays, life_expectancy,
            child_mortality_rate, infant_mortality_rate, source, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(fips_county, year) DO UPDATE SET
            state = COALESCE(excluded.state, county_health.state),
            county_name = COALESCE(excluded.county_name, county_health.county_name),
            premature_death_rate = COALESCE(excluded.premature_death_rate, county_health.premature_death_rate),
            poor_health_pct = COALESCE(excluded.poor_health_pct, county_health.poor_health_pct),
            poor_physical_health_days = COALESCE(excluded.poor_physical_health_days, county_health.poor_physical_health_days),
            poor_mental_health_days = COALESCE(excluded.poor_mental_health_days, county_health.poor_mental_health_days),
            low_birthweight_pct = COALESCE(excluded.low_birthweight_pct, county_health.low_birthweight_pct),
            adult_smoking_pct = COALESCE(excluded.adult_smoking_pct, county_health.adult_smoking_pct),
            adult_obesity_pct = COALESCE(excluded.adult_obesity_pct, county_health.adult_obesity_pct),
            physical_inactivity_pct = COALESCE(excluded.physical_inactivity_pct, county_health.physical_inactivity_pct),
            excessive_drinking_pct = COALESCE(excluded.excessive_drinking_pct, county_health.excessive_drinking_pct),
            uninsured_pct = COALESCE(excluded.uninsured_pct, county_health.uninsured_pct),
            preventable_hospital_stays = COALESCE(excluded.preventable_hospital_stays, county_health.preventable_hospital_stays),
            life_expectancy = COALESCE(excluded.life_expectancy, county_health.life_expectancy),
            child_mortality_rate = COALESCE(excluded.child_mortality_rate, county_health.child_mortality_rate),
            infant_mortality_rate = COALESCE(excluded.infant_mortality_rate, county_health.infant_mortality_rate)
    """

    cursor = conn.execute(sql, (
        data.get("fips_county"),
        data.get("year"),
        data.get("state"),
        data.get("county_name"),
        data.get("premature_death_rate"),
        data.get("poor_health_pct"),
        data.get("poor_physical_health_days"),
        data.get("poor_mental_health_days"),
        data.get("low_birthweight_pct"),
        data.get("adult_smoking_pct"),
        data.get("adult_obesity_pct"),
        data.get("physical_inactivity_pct"),
        data.get("excessive_drinking_pct"),
        data.get("uninsured_pct"),
        data.get("preventable_hospital_stays"),
        data.get("life_expectancy"),
        data.get("child_mortality_rate"),
        data.get("infant_mortality_rate"),
        data.get("source", "county_health_rankings"),
        now,
    ))
    conn.commit()
    row_id = cursor.lastrowid
    if should_close:
        conn.close()
    return row_id


def upsert_county_demographics(data: dict[str, Any], conn: Optional[sqlite3.Connection] = None) -> int:
    """Insert or update a county demographics record."""
    should_close = conn is None
    if conn is None:
        conn = get_connection()

    now = datetime.now(timezone.utc).isoformat()

    sql = """
        INSERT INTO county_demographics (
            fips_county, year, state, county_name,
            total_population, median_household_income, poverty_pct,
            unemployment_pct, pct_white, pct_black, pct_hispanic,
            pct_asian, pct_native, pct_under_18, pct_over_65,
            pct_no_highschool, rural_pct, source, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(fips_county, year) DO UPDATE SET
            state = COALESCE(excluded.state, county_demographics.state),
            county_name = COALESCE(excluded.county_name, county_demographics.county_name),
            total_population = COALESCE(excluded.total_population, county_demographics.total_population),
            median_household_income = COALESCE(excluded.median_household_income, county_demographics.median_household_income),
            poverty_pct = COALESCE(excluded.poverty_pct, county_demographics.poverty_pct),
            unemployment_pct = COALESCE(excluded.unemployment_pct, county_demographics.unemployment_pct),
            pct_white = COALESCE(excluded.pct_white, county_demographics.pct_white),
            pct_black = COALESCE(excluded.pct_black, county_demographics.pct_black),
            pct_hispanic = COALESCE(excluded.pct_hispanic, county_demographics.pct_hispanic),
            pct_asian = COALESCE(excluded.pct_asian, county_demographics.pct_asian),
            pct_native = COALESCE(excluded.pct_native, county_demographics.pct_native),
            pct_under_18 = COALESCE(excluded.pct_under_18, county_demographics.pct_under_18),
            pct_over_65 = COALESCE(excluded.pct_over_65, county_demographics.pct_over_65),
            pct_no_highschool = COALESCE(excluded.pct_no_highschool, county_demographics.pct_no_highschool),
            rural_pct = COALESCE(excluded.rural_pct, county_demographics.rural_pct)
    """

    cursor = conn.execute(sql, (
        data.get("fips_county"),
        data.get("year"),
        data.get("state"),
        data.get("county_name"),
        data.get("total_population"),
        data.get("median_household_income"),
        data.get("poverty_pct"),
        data.get("unemployment_pct"),
        data.get("pct_white"),
        data.get("pct_black"),
        data.get("pct_hispanic"),
        data.get("pct_asian"),
        data.get("pct_native"),
        data.get("pct_under_18"),
        data.get("pct_over_65"),
        data.get("pct_no_highschool"),
        data.get("rural_pct"),
        data.get("source", "census_acs"),
        now,
    ))
    conn.commit()
    row_id = cursor.lastrowid
    if should_close:
        conn.close()
    return row_id


# --- Batch operations ---

def store_facilities_batch(
    facilities: list[dict], conn: Optional[sqlite3.Connection] = None,
) -> tuple[int, int]:
    """Store a batch of TRI facility records."""
    should_close = conn is None
    if conn is None:
        conn = get_connection()

    created, updated = 0, 0
    for f in facilities:
        existing = conn.execute(
            "SELECT id FROM tri_facilities WHERE tri_facility_id = ?",
            (f.get("tri_facility_id"),),
        ).fetchone()
        upsert_facility(f, conn=conn)
        if existing:
            updated += 1
        else:
            created += 1

    if should_close:
        conn.close()
    return created, updated


def store_releases_batch(
    releases: list[dict], conn: Optional[sqlite3.Connection] = None,
) -> tuple[int, int]:
    """Store a batch of TRI release records."""
    should_close = conn is None
    if conn is None:
        conn = get_connection()

    created, updated = 0, 0
    for r in releases:
        existing = conn.execute(
            "SELECT id FROM tri_releases WHERE tri_facility_id = ? AND reporting_year = ? AND chemical_name = ?",
            (r.get("tri_facility_id"), r.get("reporting_year"), r.get("chemical_name")),
        ).fetchone()
        upsert_release(r, conn=conn)
        if existing:
            updated += 1
        else:
            created += 1

    if should_close:
        conn.close()
    return created, updated


def store_county_health_batch(
    records: list[dict], conn: Optional[sqlite3.Connection] = None,
) -> tuple[int, int]:
    """Store a batch of county health records."""
    should_close = conn is None
    if conn is None:
        conn = get_connection()

    created, updated = 0, 0
    for r in records:
        existing = conn.execute(
            "SELECT id FROM county_health WHERE fips_county = ? AND year = ?",
            (r.get("fips_county"), r.get("year")),
        ).fetchone()
        upsert_county_health(r, conn=conn)
        if existing:
            updated += 1
        else:
            created += 1

    if should_close:
        conn.close()
    return created, updated


def store_county_demographics_batch(
    records: list[dict], conn: Optional[sqlite3.Connection] = None,
) -> tuple[int, int]:
    """Store a batch of county demographics records."""
    should_close = conn is None
    if conn is None:
        conn = get_connection()

    created, updated = 0, 0
    for r in records:
        existing = conn.execute(
            "SELECT id FROM county_demographics WHERE fips_county = ? AND year = ?",
            (r.get("fips_county"), r.get("year")),
        ).fetchone()
        upsert_county_demographics(r, conn=conn)
        if existing:
            updated += 1
        else:
            created += 1

    if should_close:
        conn.close()
    return created, updated


# --- Query Functions ---

def get_all_facilities(
    state: Optional[str] = None,
    industry_sector: Optional[str] = None,
    min_quality: Optional[float] = None,
    limit: int = 1000,
    offset: int = 0,
    conn: Optional[sqlite3.Connection] = None,
) -> list[dict]:
    """Query TRI facilities with optional filters."""
    should_close = conn is None
    if conn is None:
        conn = get_connection()

    conditions, params = [], []
    if state:
        conditions.append("state = ?")
        params.append(state.upper())
    if industry_sector:
        conditions.append("industry_sector = ?")
        params.append(industry_sector)
    if min_quality is not None:
        conditions.append("quality_score >= ?")
        params.append(min_quality)

    where = " WHERE " + " AND ".join(conditions) if conditions else ""
    sql = f"SELECT * FROM tri_facilities {where} ORDER BY facility_name LIMIT ? OFFSET ?"
    params.extend([limit, offset])

    rows = conn.execute(sql, params).fetchall()
    result = [dict(row) for row in rows]
    if should_close:
        conn.close()
    return result


def get_facility_releases(
    tri_facility_id: str,
    year: Optional[int] = None,
    conn: Optional[sqlite3.Connection] = None,
) -> list[dict]:
    """Get all releases for a specific facility."""
    should_close = conn is None
    if conn is None:
        conn = get_connection()

    conditions = ["tri_facility_id = ?"]
    params: list[Any] = [tri_facility_id]
    if year:
        conditions.append("reporting_year = ?")
        params.append(year)

    where = " WHERE " + " AND ".join(conditions)
    sql = f"SELECT * FROM tri_releases {where} ORDER BY reporting_year DESC, total_releases_lbs DESC"
    rows = conn.execute(sql, params).fetchall()
    result = [dict(row) for row in rows]
    if should_close:
        conn.close()
    return result


def get_county_context(
    fips_county: str,
    year: Optional[int] = None,
    conn: Optional[sqlite3.Connection] = None,
) -> dict:
    """Get health, demographics, and EJ data for a county."""
    should_close = conn is None
    if conn is None:
        conn = get_connection()

    result: dict[str, Any] = {"fips_county": fips_county}

    # Health data
    if year:
        row = conn.execute(
            "SELECT * FROM county_health WHERE fips_county = ? AND year = ?",
            (fips_county, year),
        ).fetchone()
    else:
        row = conn.execute(
            "SELECT * FROM county_health WHERE fips_county = ? ORDER BY year DESC LIMIT 1",
            (fips_county,),
        ).fetchone()
    if row:
        result["health"] = dict(row)

    # Demographics
    if year:
        row = conn.execute(
            "SELECT * FROM county_demographics WHERE fips_county = ? AND year = ?",
            (fips_county, year),
        ).fetchone()
    else:
        row = conn.execute(
            "SELECT * FROM county_demographics WHERE fips_county = ? ORDER BY year DESC LIMIT 1",
            (fips_county,),
        ).fetchone()
    if row:
        result["demographics"] = dict(row)

    # EJ indicators (aggregate across tracts in county)
    rows = conn.execute(
        "SELECT * FROM ej_indicators WHERE fips_county = ?",
        (fips_county,),
    ).fetchall()
    if rows:
        result["ej_tracts"] = [dict(r) for r in rows]

    if should_close:
        conn.close()
    return result


def update_facility_quality_scores(
    scores: dict[str, float], conn: Optional[sqlite3.Connection] = None,
) -> int:
    """Batch update quality scores for facilities."""
    should_close = conn is None
    if conn is None:
        conn = get_connection()

    count = 0
    now = datetime.now(timezone.utc).isoformat()
    for tri_facility_id, score in scores.items():
        conn.execute(
            "UPDATE tri_facilities SET quality_score = ?, updated_at = ? WHERE tri_facility_id = ?",
            (score, now, tri_facility_id),
        )
        count += 1
    conn.commit()
    if should_close:
        conn.close()
    return count


# --- Pipeline Run tracking ---

def start_pipeline_run(source: str, stage: str, conn: Optional[sqlite3.Connection] = None) -> str:
    """Record the start of a pipeline run."""
    should_close = conn is None
    if conn is None:
        conn = get_connection()
    run_id = str(uuid.uuid4())[:8]
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "INSERT INTO pipeline_runs (run_id, source, stage, status, started_at) VALUES (?, ?, ?, 'running', ?)",
        (run_id, source, stage, now),
    )
    conn.commit()
    if should_close:
        conn.close()
    return run_id


def complete_pipeline_run(
    run_id: str, records_processed: int = 0, records_created: int = 0,
    records_updated: int = 0, errors: int = 0, status: str = "completed",
    notes: Optional[str] = None, conn: Optional[sqlite3.Connection] = None,
) -> None:
    """Record the completion of a pipeline run."""
    should_close = conn is None
    if conn is None:
        conn = get_connection()
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        """UPDATE pipeline_runs SET status = ?, records_processed = ?, records_created = ?,
           records_updated = ?, errors = ?, completed_at = ?, notes = ? WHERE run_id = ?""",
        (status, records_processed, records_created, records_updated, errors, now, notes, run_id),
    )
    conn.commit()
    if should_close:
        conn.close()


# --- Enforcement & Compliance batch operations ---

def store_frs_links_batch(
    links: list[dict], conn: Optional[sqlite3.Connection] = None,
) -> int:
    """Store FRS linkage records. Returns count of rows inserted."""
    should_close = conn is None
    if conn is None:
        conn = get_connection()

    count = 0
    skipped = 0
    for link in links:
        tri_id = link.get("tri_facility_id")
        reg_id = link.get("registry_id")
        if not tri_id or not reg_id:
            skipped += 1
            continue
        try:
            cursor = conn.execute(
                "INSERT OR IGNORE INTO tri_frs_links (tri_facility_id, registry_id, program_system_acronym) VALUES (?, ?, ?)",
                (tri_id, reg_id, link.get("program_system_acronym")),
            )
            if cursor.rowcount > 0:
                count += 1
        except Exception as e:
            skipped += 1
            console.print(f"[yellow]FRS link error: {e}[/yellow]")
    conn.commit()
    if should_close:
        conn.close()
    return count


def store_enforcement_batch(
    records: list[dict], conn: Optional[sqlite3.Connection] = None,
) -> tuple[int, int]:
    """Store enforcement action records."""
    should_close = conn is None
    if conn is None:
        conn = get_connection()

    created, updated = 0, 0
    for r in records:
        case_num = r.get("case_number")
        if not case_num:
            continue
        existing = conn.execute(
            "SELECT case_number FROM enforcement_actions WHERE case_number = ?", (case_num,)
        ).fetchone()
        conn.execute("""
            INSERT INTO enforcement_actions (
                case_number, registry_id, case_name, activity_type, enforcement_type,
                lead_agency, case_status, settlement_date, penalty_amount,
                fed_penalty_assessed, state_local_penalty, compliance_action_cost,
                enforcement_outcome, quality_score
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(case_number) DO UPDATE SET
                case_name = COALESCE(excluded.case_name, enforcement_actions.case_name),
                activity_type = COALESCE(excluded.activity_type, enforcement_actions.activity_type),
                enforcement_type = COALESCE(excluded.enforcement_type, enforcement_actions.enforcement_type),
                lead_agency = COALESCE(excluded.lead_agency, enforcement_actions.lead_agency),
                case_status = COALESCE(excluded.case_status, enforcement_actions.case_status),
                settlement_date = COALESCE(excluded.settlement_date, enforcement_actions.settlement_date),
                penalty_amount = COALESCE(excluded.penalty_amount, enforcement_actions.penalty_amount),
                fed_penalty_assessed = COALESCE(excluded.fed_penalty_assessed, enforcement_actions.fed_penalty_assessed),
                state_local_penalty = COALESCE(excluded.state_local_penalty, enforcement_actions.state_local_penalty),
                compliance_action_cost = COALESCE(excluded.compliance_action_cost, enforcement_actions.compliance_action_cost),
                enforcement_outcome = COALESCE(excluded.enforcement_outcome, enforcement_actions.enforcement_outcome)
        """, (
            case_num, r.get("registry_id"), r.get("case_name"), r.get("activity_type"),
            r.get("enforcement_type"), r.get("lead_agency"), r.get("case_status"),
            r.get("settlement_date"), r.get("penalty_amount"), r.get("fed_penalty_assessed"),
            r.get("state_local_penalty"), r.get("compliance_action_cost"),
            r.get("enforcement_outcome"), r.get("quality_score"),
        ))
        if existing:
            updated += 1
        else:
            created += 1
    conn.commit()
    if should_close:
        conn.close()
    return created, updated


def store_inspections_batch(
    records: list[dict], conn: Optional[sqlite3.Connection] = None,
) -> tuple[int, int]:
    """Store inspection records."""
    should_close = conn is None
    if conn is None:
        conn = get_connection()

    created, updated = 0, 0
    for r in records:
        insp_id = r.get("inspection_id")
        if not insp_id:
            continue
        existing = conn.execute(
            "SELECT inspection_id FROM facility_inspections WHERE inspection_id = ?", (insp_id,)
        ).fetchone()
        conn.execute("""
            INSERT INTO facility_inspections (
                inspection_id, registry_id, program, inspection_type,
                start_date, end_date, lead_agency, found_violation, quality_score
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(inspection_id) DO UPDATE SET
                registry_id = COALESCE(excluded.registry_id, facility_inspections.registry_id),
                program = COALESCE(excluded.program, facility_inspections.program),
                inspection_type = COALESCE(excluded.inspection_type, facility_inspections.inspection_type),
                start_date = COALESCE(excluded.start_date, facility_inspections.start_date),
                end_date = COALESCE(excluded.end_date, facility_inspections.end_date),
                lead_agency = COALESCE(excluded.lead_agency, facility_inspections.lead_agency),
                found_violation = COALESCE(excluded.found_violation, facility_inspections.found_violation)
        """, (
            insp_id, r.get("registry_id"), r.get("program"), r.get("inspection_type"),
            r.get("start_date"), r.get("end_date"), r.get("lead_agency"),
            r.get("found_violation", 0), r.get("quality_score"),
        ))
        if existing:
            updated += 1
        else:
            created += 1
    conn.commit()
    if should_close:
        conn.close()
    return created, updated


def store_compliance_batch(
    records: list[dict], conn: Optional[sqlite3.Connection] = None,
) -> int:
    """Store compliance status records."""
    should_close = conn is None
    if conn is None:
        conn = get_connection()

    count = 0
    for r in records:
        reg_id = r.get("registry_id")
        program = r.get("program")
        if not reg_id or not program:
            continue
        conn.execute("""
            INSERT INTO compliance_status (registry_id, program, status, status_date, quarters_in_nc)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(registry_id, program) DO UPDATE SET
                status = COALESCE(excluded.status, compliance_status.status),
                status_date = COALESCE(excluded.status_date, compliance_status.status_date),
                quarters_in_nc = COALESCE(excluded.quarters_in_nc, compliance_status.quarters_in_nc)
        """, (reg_id, program, r.get("status"), r.get("status_date"), r.get("quarters_in_nc")))
        count += 1
    conn.commit()
    if should_close:
        conn.close()
    return count


# --- Superfund batch operations ---

def store_superfund_sites_batch(
    sites: list[dict], conn: Optional[sqlite3.Connection] = None,
) -> tuple[int, int]:
    """Store Superfund site records."""
    should_close = conn is None
    if conn is None:
        conn = get_connection()

    created, updated = 0, 0
    for s in sites:
        site_id = s.get("site_id")
        if not site_id:
            continue
        existing = conn.execute(
            "SELECT site_id FROM superfund_sites WHERE site_id = ?", (site_id,)
        ).fetchone()
        conn.execute("""
            INSERT INTO superfund_sites (site_id, site_name, address, city, state, zip_code,
                                         latitude, longitude, npl_status, federal_facility)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(site_id) DO UPDATE SET
                site_name = COALESCE(excluded.site_name, superfund_sites.site_name),
                npl_status = COALESCE(excluded.npl_status, superfund_sites.npl_status)
        """, (
            site_id, s.get("site_name"), s.get("address"), s.get("city"), s.get("state"),
            s.get("zip_code"), s.get("latitude"), s.get("longitude"),
            s.get("npl_status"), s.get("federal_facility", 0),
        ))
        if existing:
            updated += 1
        else:
            created += 1
    conn.commit()
    if should_close:
        conn.close()
    return created, updated


def store_superfund_proximity_batch(
    records: list[dict], conn: Optional[sqlite3.Connection] = None,
) -> int:
    """Store facility-superfund proximity records."""
    should_close = conn is None
    if conn is None:
        conn = get_connection()

    count = 0
    for r in records:
        conn.execute("""
            INSERT OR REPLACE INTO tri_superfund_proximity (tri_facility_id, site_id, distance_miles, same_county)
            VALUES (?, ?, ?, ?)
        """, (r["tri_facility_id"], r["site_id"], r.get("distance_miles"), r.get("same_county", 0)))
        count += 1
    conn.commit()
    if should_close:
        conn.close()
    return count


# --- EJ Indicators batch operations ---

def store_ej_indicators_batch(
    records: list[dict], conn: Optional[sqlite3.Connection] = None,
) -> tuple[int, int]:
    """Store EJ indicator records."""
    should_close = conn is None
    if conn is None:
        conn = get_connection()

    created, updated = 0, 0
    for r in records:
        tract = r.get("fips_tract")
        if not tract:
            continue
        existing = conn.execute(
            "SELECT id FROM ej_indicators WHERE fips_tract = ?", (tract,)
        ).fetchone()
        conn.execute("""
            INSERT INTO ej_indicators (
                fips_tract, fips_county, state, ej_index_pctl, pm25_pctl, ozone_pctl,
                diesel_pm_pctl, air_toxics_cancer_risk_pctl, respiratory_hazard_pctl,
                traffic_proximity_pctl, superfund_proximity_pctl, rmp_proximity_pctl,
                wastewater_pctl, low_income_pctl, people_of_color_pctl,
                linguistic_isolation_pctl, under_5_pctl, over_64_pctl, source
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(fips_tract) DO UPDATE SET
                fips_county = COALESCE(excluded.fips_county, ej_indicators.fips_county),
                state = COALESCE(excluded.state, ej_indicators.state),
                ej_index_pctl = COALESCE(excluded.ej_index_pctl, ej_indicators.ej_index_pctl),
                pm25_pctl = COALESCE(excluded.pm25_pctl, ej_indicators.pm25_pctl),
                ozone_pctl = COALESCE(excluded.ozone_pctl, ej_indicators.ozone_pctl),
                diesel_pm_pctl = COALESCE(excluded.diesel_pm_pctl, ej_indicators.diesel_pm_pctl),
                air_toxics_cancer_risk_pctl = COALESCE(excluded.air_toxics_cancer_risk_pctl, ej_indicators.air_toxics_cancer_risk_pctl),
                respiratory_hazard_pctl = COALESCE(excluded.respiratory_hazard_pctl, ej_indicators.respiratory_hazard_pctl),
                traffic_proximity_pctl = COALESCE(excluded.traffic_proximity_pctl, ej_indicators.traffic_proximity_pctl),
                superfund_proximity_pctl = COALESCE(excluded.superfund_proximity_pctl, ej_indicators.superfund_proximity_pctl),
                rmp_proximity_pctl = COALESCE(excluded.rmp_proximity_pctl, ej_indicators.rmp_proximity_pctl),
                wastewater_pctl = COALESCE(excluded.wastewater_pctl, ej_indicators.wastewater_pctl),
                low_income_pctl = COALESCE(excluded.low_income_pctl, ej_indicators.low_income_pctl),
                people_of_color_pctl = COALESCE(excluded.people_of_color_pctl, ej_indicators.people_of_color_pctl),
                linguistic_isolation_pctl = COALESCE(excluded.linguistic_isolation_pctl, ej_indicators.linguistic_isolation_pctl),
                under_5_pctl = COALESCE(excluded.under_5_pctl, ej_indicators.under_5_pctl),
                over_64_pctl = COALESCE(excluded.over_64_pctl, ej_indicators.over_64_pctl)
        """, (
            tract, r.get("fips_county"), r.get("state"), r.get("ej_index_pctl"),
            r.get("pm25_pctl"), r.get("ozone_pctl"), r.get("diesel_pm_pctl"),
            r.get("air_toxics_cancer_risk_pctl"), r.get("respiratory_hazard_pctl"),
            r.get("traffic_proximity_pctl"), r.get("superfund_proximity_pctl"),
            r.get("rmp_proximity_pctl"), r.get("wastewater_pctl"),
            r.get("low_income_pctl"), r.get("people_of_color_pctl"),
            r.get("linguistic_isolation_pctl"), r.get("under_5_pctl"),
            r.get("over_64_pctl"), r.get("source", "ejscreen"),
        ))
        if existing:
            updated += 1
        else:
            created += 1
    conn.commit()
    if should_close:
        conn.close()
    return created, updated


# --- RMP batch operations ---

def store_rmp_facilities_batch(
    facilities: list[dict], conn: Optional[sqlite3.Connection] = None,
) -> tuple[int, int]:
    """Store RMP facility records."""
    should_close = conn is None
    if conn is None:
        conn = get_connection()

    created, updated = 0, 0
    for r in facilities:
        rmp_id = r.get("rmp_id")
        if not rmp_id:
            continue
        existing = conn.execute(
            "SELECT rmp_id FROM rmp_facilities WHERE rmp_id = ?", (rmp_id,)
        ).fetchone()
        conn.execute("""
            INSERT INTO rmp_facilities (
                rmp_id, facility_name, street_address, city, state, zip_code,
                latitude, longitude, frs_registry_id, naics_code,
                num_processes, num_chemicals, last_submission_date,
                deregistration_date, quality_score
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(rmp_id) DO UPDATE SET
                facility_name = COALESCE(excluded.facility_name, rmp_facilities.facility_name),
                street_address = COALESCE(excluded.street_address, rmp_facilities.street_address),
                city = COALESCE(excluded.city, rmp_facilities.city),
                state = COALESCE(excluded.state, rmp_facilities.state),
                zip_code = COALESCE(excluded.zip_code, rmp_facilities.zip_code),
                latitude = COALESCE(excluded.latitude, rmp_facilities.latitude),
                longitude = COALESCE(excluded.longitude, rmp_facilities.longitude),
                frs_registry_id = COALESCE(excluded.frs_registry_id, rmp_facilities.frs_registry_id),
                naics_code = COALESCE(excluded.naics_code, rmp_facilities.naics_code),
                num_processes = COALESCE(excluded.num_processes, rmp_facilities.num_processes),
                num_chemicals = COALESCE(excluded.num_chemicals, rmp_facilities.num_chemicals),
                last_submission_date = COALESCE(excluded.last_submission_date, rmp_facilities.last_submission_date),
                deregistration_date = COALESCE(excluded.deregistration_date, rmp_facilities.deregistration_date),
                quality_score = COALESCE(excluded.quality_score, rmp_facilities.quality_score)
        """, (
            rmp_id, r.get("facility_name"), r.get("street_address"),
            r.get("city"), r.get("state"), r.get("zip_code"),
            r.get("latitude"), r.get("longitude"), r.get("frs_registry_id"),
            r.get("naics_code"), r.get("num_processes"), r.get("num_chemicals"),
            r.get("last_submission_date"), r.get("deregistration_date"),
            r.get("quality_score"),
        ))
        if existing:
            updated += 1
        else:
            created += 1
    conn.commit()
    if should_close:
        conn.close()
    return created, updated


def store_rmp_chemicals_batch(
    chemicals: list[dict], conn: Optional[sqlite3.Connection] = None,
) -> tuple[int, int]:
    """Store RMP chemical inventory records."""
    should_close = conn is None
    if conn is None:
        conn = get_connection()

    created, updated = 0, 0
    for r in chemicals:
        rmp_id = r.get("rmp_id")
        chem_name = r.get("chemical_name")
        if not rmp_id or not chem_name:
            continue
        existing = conn.execute(
            "SELECT id FROM rmp_chemicals WHERE rmp_id = ? AND chemical_name = ?",
            (rmp_id, chem_name),
        ).fetchone()
        conn.execute("""
            INSERT INTO rmp_chemicals (
                rmp_id, chemical_name, cas_number, quantity_lbs,
                is_toxic, is_flammable, worst_case_scenario,
                worst_case_distance_miles, alt_case_distance_miles, quality_score
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(rmp_id, chemical_name) DO UPDATE SET
                cas_number = COALESCE(excluded.cas_number, rmp_chemicals.cas_number),
                quantity_lbs = COALESCE(excluded.quantity_lbs, rmp_chemicals.quantity_lbs),
                is_toxic = COALESCE(excluded.is_toxic, rmp_chemicals.is_toxic),
                is_flammable = COALESCE(excluded.is_flammable, rmp_chemicals.is_flammable),
                worst_case_scenario = COALESCE(excluded.worst_case_scenario, rmp_chemicals.worst_case_scenario),
                worst_case_distance_miles = COALESCE(excluded.worst_case_distance_miles, rmp_chemicals.worst_case_distance_miles),
                alt_case_distance_miles = COALESCE(excluded.alt_case_distance_miles, rmp_chemicals.alt_case_distance_miles)
        """, (
            rmp_id, chem_name, r.get("cas_number"), r.get("quantity_lbs"),
            r.get("is_toxic", 0), r.get("is_flammable", 0),
            r.get("worst_case_scenario"), r.get("worst_case_distance_miles"),
            r.get("alt_case_distance_miles"), r.get("quality_score"),
        ))
        if existing:
            updated += 1
        else:
            created += 1
    conn.commit()
    if should_close:
        conn.close()
    return created, updated


def store_rmp_accidents_batch(
    accidents: list[dict], conn: Optional[sqlite3.Connection] = None,
) -> tuple[int, int]:
    """Store RMP accident history records."""
    should_close = conn is None
    if conn is None:
        conn = get_connection()

    created, updated = 0, 0
    for r in accidents:
        rmp_id = r.get("rmp_id")
        if not rmp_id:
            continue
        conn.execute("""
            INSERT INTO rmp_accidents (
                rmp_id, accident_date, chemical_name, cas_number,
                quantity_released_lbs, release_duration_hours, release_event,
                deaths_workers, deaths_public, injuries_workers, injuries_public,
                evacuations, property_damage_usd, quality_score
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            rmp_id, r.get("accident_date"), r.get("chemical_name"),
            r.get("cas_number"), r.get("quantity_released_lbs"),
            r.get("release_duration_hours"), r.get("release_event"),
            r.get("deaths_workers", 0), r.get("deaths_public", 0),
            r.get("injuries_workers", 0), r.get("injuries_public", 0),
            r.get("evacuations", 0), r.get("property_damage_usd"),
            r.get("quality_score"),
        ))
        created += 1
    conn.commit()
    if should_close:
        conn.close()
    return created, updated


def store_tri_rmp_links_batch(
    links: list[dict], conn: Optional[sqlite3.Connection] = None,
) -> int:
    """Store TRI→RMP cross-links."""
    should_close = conn is None
    if conn is None:
        conn = get_connection()

    count = 0
    for link in links:
        tri_id = link.get("tri_facility_id")
        rmp_id = link.get("rmp_id")
        if not tri_id or not rmp_id:
            continue
        try:
            before = conn.total_changes
            conn.execute("""
                INSERT OR IGNORE INTO tri_rmp_links (
                    tri_facility_id, rmp_id, link_method, confidence
                ) VALUES (?, ?, ?, ?)
            """, (tri_id, rmp_id, link.get("link_method"), link.get("confidence")))
            count += conn.total_changes - before
        except Exception:
            pass
    conn.commit()
    if should_close:
        conn.close()
    return count


def get_stats(conn: Optional[sqlite3.Connection] = None, print_output: bool = True) -> dict:
    """Get summary statistics about the database."""
    should_close = conn is None
    if conn is None:
        conn = get_connection()

    stats: dict[str, Any] = {}

    row = conn.execute("SELECT COUNT(*) as cnt FROM tri_facilities").fetchone()
    stats["total_facilities"] = row["cnt"]

    row = conn.execute("SELECT COUNT(*) as cnt FROM tri_releases").fetchone()
    stats["total_releases"] = row["cnt"]

    rows = conn.execute(
        "SELECT state, COUNT(*) as cnt FROM tri_facilities GROUP BY state ORDER BY cnt DESC"
    ).fetchall()
    stats["facilities_by_state"] = {r["state"]: r["cnt"] for r in rows}

    rows = conn.execute(
        "SELECT industry_sector, COUNT(*) as cnt FROM tri_facilities WHERE industry_sector IS NOT NULL GROUP BY industry_sector ORDER BY cnt DESC"
    ).fetchall()
    stats["by_industry"] = {r["industry_sector"]: r["cnt"] for r in rows}

    row = conn.execute("""
        SELECT COUNT(DISTINCT chemical_name) as chemicals,
               SUM(total_releases_lbs) as total_lbs,
               COUNT(DISTINCT reporting_year) as years
        FROM tri_releases
    """).fetchone()
    stats["chemicals_tracked"] = row["chemicals"]
    stats["total_release_lbs"] = row["total_lbs"] or 0
    stats["reporting_years"] = row["years"]

    row = conn.execute("""
        SELECT SUM(total_releases_lbs) as carcinogen_lbs
        FROM tri_releases WHERE carcinogen_flag = 'YES'
    """).fetchone()
    stats["carcinogen_release_lbs"] = row["carcinogen_lbs"] or 0

    row = conn.execute("SELECT COUNT(*) as cnt FROM county_health").fetchone()
    stats["county_health_records"] = row["cnt"]

    row = conn.execute("SELECT COUNT(*) as cnt FROM county_demographics").fetchone()
    stats["county_demo_records"] = row["cnt"]

    row = conn.execute("SELECT COUNT(*) as cnt FROM ej_indicators").fetchone()
    stats["ej_indicator_records"] = row["cnt"]

    # Enforcement stats
    row = conn.execute("SELECT COUNT(*) as cnt FROM tri_frs_links").fetchone()
    stats["frs_links"] = row["cnt"]

    row = conn.execute("SELECT COUNT(*) as cnt FROM enforcement_actions").fetchone()
    stats["enforcement_actions"] = row["cnt"]

    row = conn.execute("SELECT COUNT(*) as cnt FROM facility_inspections").fetchone()
    stats["facility_inspections"] = row["cnt"]

    row = conn.execute("SELECT COALESCE(SUM(penalty_amount), 0) as total FROM enforcement_actions").fetchone()
    stats["total_penalties"] = row["total"]

    # Superfund stats
    row = conn.execute("SELECT COUNT(*) as cnt FROM superfund_sites").fetchone()
    stats["superfund_sites"] = row["cnt"]

    row = conn.execute("SELECT COUNT(DISTINCT tri_facility_id) as cnt FROM tri_superfund_proximity").fetchone()
    stats["facilities_near_superfund"] = row["cnt"]

    row = conn.execute("""
        SELECT AVG(quality_score) as avg, MIN(quality_score) as min_q,
               MAX(quality_score) as max_q,
               COUNT(CASE WHEN quality_score >= 0.6 THEN 1 END) as above
        FROM tri_facilities WHERE quality_score IS NOT NULL
    """).fetchone()
    stats["quality"] = {
        "avg_score": round(row["avg"] or 0, 3),
        "min_score": round(row["min_q"] or 0, 3),
        "max_score": round(row["max_q"] or 0, 3),
        "above_threshold": row["above"] or 0,
    }

    if print_output:
        _print_stats(stats)

    if should_close:
        conn.close()
    return stats


def _print_stats(stats: dict) -> None:
    """Print formatted statistics table."""
    table = Table(title="EPA TRI Community Health & Demographics Tracker — Database Statistics")
    table.add_column("Metric", style="cyan")
    table.add_column("Value", style="green")

    table.add_row("Total TRI Facilities", f"{stats['total_facilities']:,}")
    table.add_row("Total Release Records", f"{stats['total_releases']:,}")
    table.add_row("Chemicals Tracked", f"{stats['chemicals_tracked']:,}")
    table.add_row("Reporting Years", str(stats["reporting_years"]))
    table.add_row("Total Releases (lbs)", f"{stats['total_release_lbs']:,.0f}")
    table.add_row("Carcinogen Releases (lbs)", f"{stats['carcinogen_release_lbs']:,.0f}")

    table.add_row("", "")
    table.add_row("[bold]Cross-Linked Data[/bold]", "")
    table.add_row("  County Health Records", f"{stats['county_health_records']:,}")
    table.add_row("  County Demographics Records", f"{stats['county_demo_records']:,}")
    table.add_row("  EJ Indicator Records", f"{stats['ej_indicator_records']:,}")

    table.add_row("", "")
    table.add_row("[bold]Enforcement & Compliance[/bold]", "")
    table.add_row("  FRS Linkages", f"{stats.get('frs_links', 0):,}")
    table.add_row("  Enforcement Actions", f"{stats.get('enforcement_actions', 0):,}")
    table.add_row("  Facility Inspections", f"{stats.get('facility_inspections', 0):,}")
    table.add_row("  Total Penalties ($)", f"${stats.get('total_penalties', 0):,.0f}")
    table.add_row("  Superfund NPL Sites", f"{stats.get('superfund_sites', 0):,}")
    table.add_row("  Facilities Near Superfund", f"{stats.get('facilities_near_superfund', 0):,}")

    table.add_row("", "")
    table.add_row("[bold]Top States by Facilities[/bold]", "")
    for state, count in list(stats.get("facilities_by_state", {}).items())[:10]:
        table.add_row(f"  {state}", f"{count:,}")

    table.add_row("", "")
    table.add_row("[bold]Quality[/bold]", "")
    q = stats["quality"]
    table.add_row("  Average Score", f"{q['avg_score']:.3f}")
    table.add_row("  Above Threshold (>=0.6)", str(q["above_threshold"]))

    console.print(table)
