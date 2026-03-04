# EPA TRI Tracker — Data Enrichment Plan V2

## Overview
This enrichment plan addresses two critical issues: (1) the original ENRICHMENT_PLAN.md enrichments were **scaffolded but never populated** — all enrichment tables exist with 0 records, and (2) a new high-value data source (EPA RMP) can be added. This plan completes the unfinished work and adds Risk Management Program data for chemical accident risk.

**Current state:** 22,936 facilities, 155,814 releases, 571 chemicals, 3,143 counties — enrichment database tables (enforcement_actions, facility_inspections, compliance_status, tri_frs_links, superfund_sites, tri_superfund_proximity, ej_indicators) ALL exist but contain **0 records**. The scrapers and CLI commands exist but were never executed against the production database.

**Target state:** Populated enforcement/compliance/EJ tables (completing V1 plan) PLUS new RMP hazardous chemical facility data for acute accident risk analysis.

---

## PART A: Complete V1 Enrichment Population (CRITICAL — Infrastructure Exists)

The following enrichments have complete infrastructure (scrapers, schema, CLI commands, dashboard tabs) but **0 records populated**. The work here is to execute the existing pipeline and fix any issues that arise.

### A1: EPA ECHO Enforcement Data Population

**Status:** Scraper exists (`src/scrapers/echo.py`), database tables exist, CLI `echo` command exists. Zero records.

**What needs to happen:**
1. Run the ECHO downloader to fetch ZIP files from `echo.epa.gov/files/echodownloads/`:
   - `case_downloads.zip` → enforcement_actions table
   - `npdes_downloads.zip` → facility_inspections (CWA)
   - `caa_downloads.zip` → facility_inspections (CAA)
   - `rcra_downloads.zip` → facility_inspections (RCRA)
2. Run the FRS downloader to build tri_frs_links:
   - `frs_downloads.zip` → extract TRI program records → build tri_frs_links table
   - This is the CRITICAL bridge: maps TRI_FACILITY_ID to FRS REGISTRY_ID
3. Cross-link enforcement records to TRI facilities via FRS registry_id
4. Populate compliance_status from ECHO data
5. Verify dashboard "Enforcement Dashboard" and "Compliance Tracker" tabs render with real data
6. Fix any issues that arise during data population

**Expected records:**
- enforcement_actions: ~100K+ records
- facility_inspections: ~500K+ records (across CWA, CAA, RCRA)
- compliance_status: ~50K+ records
- tri_frs_links: ~20K+ records

**Implementation steps:**
1. Test the existing `echo` CLI command: `python -m src.cli echo --force`
2. If it fails, debug the scraper (likely download URL changes or CSV format issues)
3. Verify FRS linkage is working (tri_frs_links populated)
4. Verify enforcement_actions, facility_inspections, compliance_status tables populated
5. Run quality scoring update to include enforcement data components
6. Verify dashboard tabs display real data
7. Fix any data quality issues in PROBLEMS.md

### A2: Superfund Site Data Population

**Status:** Database tables exist (superfund_sites, tri_superfund_proximity). Zero records.

**What needs to happen:**
1. Download Superfund/NPL site data from data.gov:
   - URL: `https://catalog.data.gov/dataset/epa-facility-registry-service-frs-sems_npl8`
   - Or: EPA Envirofacts SEMS search
2. Parse site data: site_id, site_name, address, city, state, zip, latitude, longitude, npl_status
3. Calculate haversine distances between TRI facilities and NPL sites
4. Flag facilities within 5-mile radius of any NPL site
5. Populate tri_superfund_proximity table
6. Verify "Superfund Proximity" dashboard tab

**Expected records:**
- superfund_sites: ~1,800 NPL sites
- tri_superfund_proximity: ~5,000-10,000 facility-site proximity pairs

### A3: EJScreen Environmental Justice Indicators

**Status:** Table exists (ej_indicators). Zero records. Marked DEFERRED (P6) because EPA removed EJScreen from website Feb 2025.

**What needs to happen:**
1. Download from Zenodo archive: `https://zenodo.org/records/14767363`
2. Select most recent year available
3. Parse CSV at census tract level
4. Populate ej_indicators table using existing schema
5. Link to TRI facilities via FIPS tract code or FIPS county code
6. Quality scores will jump from ~0.847 to ~0.95 (ej_indicators component goes from 0.0 to 1.0)

**Expected records:**
- ej_indicators: ~70,000+ census tract records

**Note:** File sizes may be large (5GB+/year). Consider downloading only the most recent year and filtering to tracts containing TRI facilities.

---

## PART B: New Enrichment — EPA Risk Management Program (RMP) Data

### What It Adds
- Facilities managing hazardous chemicals with potential for catastrophic accidents
- Chemical accident risk dimension (complements TRI's chronic exposure risk)
- Worst-case scenario and alternative release scenario data
- Emergency response planning information
- 5-year accident history per facility
- Enables: "Which communities face BOTH chronic pollution AND acute accident risk?"

### Why This Matters
- TRI tracks **routine releases** (chronic exposure)
- RMP tracks **catastrophic accident potential** (acute risk)
- A facility might have low TRI releases but high RMP risk (e.g., stores large quantities of chlorine)
- Combining both gives the most complete picture of chemical facility risk
- No other free tool connects TRI + RMP data

### Data Sources

**1. EPA RMP Public Data Sharing Portal**
- URL: `https://cdxapps.epa.gov/olem-rmp-pds/`
- Format: Downloadable datasets (CSV/Excel)
- Contains: Facility registration, chemical inventory, process information
- Updated: Regularly
- Join key: FRS Registry ID or facility name + address matching

**2. Data Liberation Project RMP Database (Alternative)**
- URL: `https://www.data-liberation-project.org/datasets/epa-risk-management-program-database/`
- Format: Structured database dump (updated through Dec 2025)
- More accessible than EPA portal for bulk download
- Contains: Complete RMP filing data including accident history

**3. RMP Facility Registration Data**
- Facility name, address, latitude/longitude, NAICS code
- Regulated substances and quantities
- Worst-case release scenario (toxic endpoint distance)
- Alternative release scenario
- 5-year accident history
- Emergency contact information

### Database Schema

```sql
-- RMP facility registrations
CREATE TABLE rmp_facilities (
    rmp_id TEXT PRIMARY KEY,
    facility_name TEXT,
    street_address TEXT,
    city TEXT,
    state TEXT,
    zip_code TEXT,
    latitude REAL,
    longitude REAL,
    frs_registry_id TEXT,              -- Join key to TRI via tri_frs_links
    naics_code TEXT,
    num_processes INTEGER,
    num_chemicals INTEGER,
    last_submission_date TEXT,
    deregistration_date TEXT,           -- NULL if still active
    quality_score REAL,
    created_at TEXT DEFAULT (datetime('now'))
);

-- Chemicals managed at RMP facilities
CREATE TABLE rmp_chemicals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    rmp_id TEXT NOT NULL,
    chemical_name TEXT,
    cas_number TEXT,
    quantity_lbs REAL,                  -- Maximum quantity on-site
    is_toxic INTEGER DEFAULT 0,
    is_flammable INTEGER DEFAULT 0,
    worst_case_scenario TEXT,           -- Brief description
    worst_case_distance_miles REAL,     -- Toxic endpoint distance
    alt_case_distance_miles REAL,       -- Alternative scenario distance
    quality_score REAL,
    created_at TEXT DEFAULT (datetime('now'))
);

-- 5-year accident history from RMP filings
CREATE TABLE rmp_accidents (
    accident_id INTEGER PRIMARY KEY AUTOINCREMENT,
    rmp_id TEXT NOT NULL,
    accident_date TEXT,
    chemical_name TEXT,
    cas_number TEXT,
    quantity_released_lbs REAL,
    release_duration_hours REAL,
    release_event TEXT,                 -- 'explosion', 'fire', 'toxic_release'
    deaths_workers INTEGER DEFAULT 0,
    deaths_public INTEGER DEFAULT 0,
    injuries_workers INTEGER DEFAULT 0,
    injuries_public INTEGER DEFAULT 0,
    evacuations INTEGER DEFAULT 0,
    property_damage_usd REAL,
    quality_score REAL,
    created_at TEXT DEFAULT (datetime('now'))
);

-- Cross-link: TRI facility to RMP facility
CREATE TABLE tri_rmp_links (
    tri_facility_id TEXT NOT NULL,
    rmp_id TEXT NOT NULL,
    link_method TEXT,                   -- 'frs_registry', 'address_match', 'name_match'
    confidence REAL,
    PRIMARY KEY (tri_facility_id, rmp_id)
);

CREATE INDEX idx_rmp_frs ON rmp_facilities(frs_registry_id);
CREATE INDEX idx_rmp_state ON rmp_facilities(state);
CREATE INDEX idx_rmp_chem_facility ON rmp_chemicals(rmp_id);
CREATE INDEX idx_rmp_chem_cas ON rmp_chemicals(cas_number);
CREATE INDEX idx_rmp_acc_facility ON rmp_accidents(rmp_id);
CREATE INDEX idx_rmp_acc_date ON rmp_accidents(accident_date);
CREATE INDEX idx_tri_rmp_tri ON tri_rmp_links(tri_facility_id);
CREATE INDEX idx_tri_rmp_rmp ON tri_rmp_links(rmp_id);
```

### Cross-Linking Strategy
```
TRI facility → tri_frs_links → FRS registry_id → rmp_facilities (frs_registry_id)
                                                  ↓
                                                  Also try: address matching, name matching
Result: tri_rmp_links table connecting TRI and RMP facilities
```

### Implementation Steps
1. Build `src/scrapers/rmp_downloader.py` — download RMP data from EPA portal or Data Liberation Project
2. Parse facility registration data into rmp_facilities
3. Parse chemical inventory into rmp_chemicals
4. Parse accident history into rmp_accidents
5. Build cross-linker: match RMP facilities to TRI facilities via:
   a. FRS registry ID (highest confidence)
   b. Address matching (medium confidence)
   c. Name + city + state matching (lower confidence)
6. Add "Chemical Accident Risk" dashboard tab:
   - Facilities with BOTH TRI releases AND RMP accident potential
   - Worst-case scenario distance mapping
   - Accident history timeline
   - Chemical inventory by facility
   - Community risk: population within worst-case toxic endpoint distance
7. Add "Dual Risk Analysis" section:
   - Scatter plot: TRI release volume vs RMP accident potential
   - Facilities in high-EJ-burden areas with high RMP risk
   - Industry sectors with highest combined TRI + RMP risk
8. Update quality scoring to include RMP linkage
9. Update exports
10. Add tests for RMP parsing, cross-linking, and accident data

### Expected Impact
- +12,000-15,000 RMP facility records
- +30,000-50,000 chemical inventory records
- +5,000-10,000 accident history records
- +8,000-12,000 TRI-RMP cross-links
- Unique analysis: "chronic pollution + acute accident risk" in one platform

---

## Dashboard Enhancements

### Verify Existing Enrichment Tabs Work With Data
After populating Part A tables:
- **Enforcement Dashboard** — Should now show real enforcement actions, penalties, compliance rates
- **Compliance Tracker** — Should now show significant non-compliance flags, inspection frequencies
- **Superfund Proximity** — Should now show TRI facilities near NPL sites
- **Environmental Justice** — Should now show EJ indicators with populated data

### New Tabs to Add
1. **Chemical Accident Risk** — RMP facilities, worst-case scenarios, accident history
2. **Dual Risk Analysis** — Combined TRI chronic + RMP acute risk assessment

### Enhanced Existing Tabs
- **Facility Explorer** — Add: RMP status, enforcement history, compliance status, EJ indicators
- **Community Impact** — Add: enforcement context, EJ percentiles, Superfund proximity, RMP accident risk
- **National Overview** — Add: total enforcement actions, penalties, compliance rates, RMP facilities

---

## Facility Risk Scoring (Enhanced)

Create a composite risk score per facility combining ALL data sources:
```python
FACILITY_RISK_WEIGHTS = {
    'release_volume': 0.20,            # Total pounds released, weighted by toxicity
    'carcinogen_concentration': 0.15,  # Proportion of carcinogenic releases
    'enforcement_history': 0.15,       # Violations, penalties, non-compliance quarters
    'community_vulnerability': 0.15,   # EJ indicators, demographics
    'trend_direction': 0.10,           # Increasing vs decreasing releases
    'rmp_accident_risk': 0.15,         # NEW: RMP worst-case + accident history
    'superfund_proximity': 0.10,       # NEW: Near legacy contamination sites
}
```

---

## Export Updates
- enforcement_actions.csv, facility_inspections.csv, compliance_status.csv (from Part A)
- superfund_proximity.csv (from Part A)
- rmp_facilities.csv, rmp_chemicals.csv, rmp_accidents.csv (from Part B)
- tri_rmp_links.csv (cross-links)
- Updated facility profiles with enforcement + RMP metrics
- Updated summary.md with enforcement and RMP statistics

---

## Test Requirements (Target: 50+ new tests)
- ECHO ZIP download and CSV parsing (enforcement, inspections, compliance)
- FRS registry linkage accuracy
- Superfund site distance calculations (haversine)
- EJScreen data parsing and tract-level aggregation
- RMP facility data parsing
- RMP chemical inventory parsing
- RMP accident history parsing
- TRI-to-RMP cross-linking (FRS, address, name matching)
- Facility risk score computation
- Dashboard data queries for all enrichment tabs
- Export validation

---

## Priority Order
1. **ECHO Enforcement Data** (Part A1) — Execute existing infrastructure, populate 0-record tables
2. **Superfund Sites** (Part A2) — Download and populate, enable proximity analysis
3. **EJScreen Indicators** (Part A3) — Download from Zenodo archive, populate empty table
4. **RMP Data** (Part B) — New source, adds unique chemical accident risk dimension

---

## Expected Outcome
- All enrichment tables populated with real data (currently all empty)
- Quality scores jump from ~0.847 to ~0.95+
- Dashboard enrichment tabs (Enforcement, Compliance, Superfund, EJ) now show actual data
- New RMP data creates "chronic + acute chemical risk" platform
- From "pollution inventory with empty enrichment tabs" to "comprehensive environmental risk platform"
- Unique cross-linking: TRI releases + ECHO enforcement + Superfund proximity + EJ indicators + RMP accident risk
- High-value for: environmental lawyers, EJ organizations, investigative journalists, EPA researchers, insurance companies
