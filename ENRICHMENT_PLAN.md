# EPA TRI Tracker — Data Enrichment Plan

## Overview
This enrichment transforms the EPA TRI Tracker from a "pollution inventory" into a comprehensive **environmental enforcement and community impact platform** by adding enforcement/compliance data, historical depth, and environmental justice indicators.

**Current state:** 22,936 facilities, 155,814 releases, 571 chemicals, 3,143 counties — but ZERO enforcement data, only 2 years of releases, and empty EJ indicators table.

**Target state:** 22,936+ facilities with enforcement histories, 10+ years of release trends, Superfund proximity flags, and populated EJ indicators.

---

## Enrichment 1: EPA ECHO Enforcement & Compliance Data (HIGHEST PRIORITY)

### What It Adds
- Facility-level enforcement actions (Clean Air Act, Clean Water Act, RCRA)
- Inspection history (dates, types, findings)
- Violation records with penalty amounts
- Compliance status (significant non-compliance flags)
- Permit information (NPDES, CAA, RCRA)

### Data Sources

**1. EPA FRS (Facility Registry Service) — The Join Key**
- Download: `https://echo.epa.gov/files/echodownloads/frs_downloads.zip`
- Format: CSV within ZIP (~50MB)
- Purpose: Maps TRI_FACILITY_ID to FRS_REGISTRY_ID (the universal EPA facility identifier)
- Join key: TRI facility ID appears in FRS program interest table
- This is the CRITICAL bridge between TRI and ECHO data

**2. ECHO CASE_ENFORCEMENTS (Enforcement Actions)**
- Download: `https://echo.epa.gov/files/echodownloads/case_downloads.zip`
- Format: CSV within ZIP
- Key fields: REGISTRY_ID, CASE_NUMBER, CASE_NAME, ACTIVITY_TYPE_DESC, ENF_OUTCOME_DESC, PENALTY_AMOUNT, SETTLEMENT_AMOUNT, FED_PENALTY_ASSESSED_AMT
- Volume: ~100K+ enforcement cases
- Updated weekly

**3. ECHO NPDES_INSPECTIONS (Clean Water Act Inspections)**
- Download: `https://echo.epa.gov/files/echodownloads/npdes_downloads.zip`
- Format: CSV within ZIP (~300MB)
- Key fields: REGISTRY_ID, NPDES_ID, ACTUAL_BEGIN_DATE, ACTUAL_END_DATE, COMP_MONITOR_TYPE_CODE, COMP_MONITOR_TYPE_DESC
- Volume: ~500K inspection records

**4. ECHO ICIS_FEC_EPA_INSPECTIONS (Clean Air Act Inspections)**
- Download: `https://echo.epa.gov/files/echodownloads/caa_downloads.zip`
- Format: CSV within ZIP
- Key fields: REGISTRY_ID, ACTIVITY_ID, COMP_MONITOR_TYPE, STATE_EPA_FLAG, ACTUAL_BEGIN_DATE

**5. ECHO RCRA_EVALUATIONS (Hazardous Waste Inspections)**
- Download: `https://echo.epa.gov/files/echodownloads/rcra_downloads.zip`
- Format: CSV within ZIP
- Key fields: REGISTRY_ID, EVALUATION_TYPE, EVALUATION_START_DATE, FOUND_VIOLATION_FLAG

### Database Schema Additions

```sql
-- Bridge table: TRI facility to FRS registry
CREATE TABLE tri_frs_links (
    tri_facility_id TEXT NOT NULL,
    registry_id TEXT NOT NULL,
    program_system_acronym TEXT,  -- 'TRIS'
    PRIMARY KEY (tri_facility_id, registry_id)
);

-- Enforcement actions from ECHO
CREATE TABLE enforcement_actions (
    case_number TEXT PRIMARY KEY,
    registry_id TEXT NOT NULL,
    case_name TEXT,
    activity_type TEXT,        -- 'FORMAL', 'INFORMAL'
    enforcement_type TEXT,     -- 'CAA', 'CWA', 'RCRA'
    lead_agency TEXT,          -- 'EPA', 'STATE'
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

-- Inspections from all programs
CREATE TABLE facility_inspections (
    inspection_id TEXT PRIMARY KEY,
    registry_id TEXT NOT NULL,
    program TEXT,              -- 'CWA', 'CAA', 'RCRA'
    inspection_type TEXT,
    start_date TEXT,
    end_date TEXT,
    lead_agency TEXT,
    found_violation INTEGER DEFAULT 0,
    quality_score REAL,
    created_at TEXT DEFAULT (datetime('now'))
);

-- Compliance status
CREATE TABLE compliance_status (
    registry_id TEXT NOT NULL,
    program TEXT NOT NULL,
    status TEXT,               -- 'In Compliance', 'Significant Non-Compliance', 'Violation'
    status_date TEXT,
    quarters_in_nc INTEGER,    -- quarters in non-compliance
    PRIMARY KEY (registry_id, program)
);

CREATE INDEX idx_enforcement_registry ON enforcement_actions(registry_id);
CREATE INDEX idx_enforcement_type ON enforcement_actions(enforcement_type);
CREATE INDEX idx_enforcement_penalty ON enforcement_actions(penalty_amount);
CREATE INDEX idx_inspections_registry ON facility_inspections(registry_id);
CREATE INDEX idx_inspections_program ON facility_inspections(program);
CREATE INDEX idx_compliance_registry ON compliance_status(registry_id);
```

### Cross-Linking Strategy
1. Download FRS data → extract TRI program records → build tri_frs_links table
2. Use registry_id to join enforcement_actions, facility_inspections, compliance_status to TRI facilities
3. Aggregate enforcement metrics per facility for the facility profile

### Implementation Steps
1. Build `src/scrapers/echo.py` — download all ECHO ZIP files, parse CSVs
2. Build `src/scrapers/frs.py` — download FRS data, extract TRI↔FRS mappings
3. Add new tables to `src/storage/database.py`
4. Build `src/normalization/enforcement_linker.py` — link ECHO records to TRI facilities via FRS_ID
5. Update quality scoring to include enforcement data (has_enforcement_linked component)
6. Add dashboard tabs: "Enforcement Analysis", "Compliance Status", "Penalty Tracker"
7. Update exports to include enforcement data
8. Add tests for all new functionality

---

## Enrichment 2: Historical TRI Data Expansion (2018-2021)

### What It Adds
- 4 additional years of release data (currently only 2022-2023)
- Trend analysis capability (5-year pollution trajectories per facility)
- Year-over-year change detection
- Facility pollution history

### Data Source
- Same EPA TRI Basic Data Files, just additional years
- URL pattern: `https://data.epa.gov/efservice/downloads/tri/mv_tri_basic_download/{year}_{state}/csv`
- Years to add: 2018, 2019, 2020, 2021
- Estimated additional records: ~300,000 release records

### Implementation
1. Update `src/scrapers/tri_downloader.py` to accept year range parameter
2. Run pipeline for years 2018-2021 across all states
3. Add trend calculation to facility profiles (5-year average, trend direction)
4. Add "Historical Trends" dashboard section showing per-facility trajectories
5. Calculate: year-over-year change %, 5-year trend direction, COVID impact analysis

---

## Enrichment 3: Superfund/NPL Site Proximity

### What It Adds
- Flag TRI facilities that are near or overlap with Superfund National Priorities List sites
- Proximity analysis: which communities face BOTH active pollution AND legacy contamination
- Superfund site status and cleanup stage

### Data Source
- EPA Superfund SEMS data via data.gov
- URL: `https://catalog.data.gov/dataset/epa-facility-registry-service-frs-sems_npl8`
- Alternative: EPA Envirofacts SEMS search
- Format: CSV with lat/lon coordinates
- Volume: ~1,800 NPL sites with coordinates

### Database Schema Addition
```sql
CREATE TABLE superfund_sites (
    site_id TEXT PRIMARY KEY,
    site_name TEXT,
    address TEXT,
    city TEXT,
    state TEXT,
    zip_code TEXT,
    latitude REAL,
    longitude REAL,
    npl_status TEXT,          -- 'Proposed', 'Final', 'Deleted'
    federal_facility INTEGER DEFAULT 0,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE tri_superfund_proximity (
    tri_facility_id TEXT NOT NULL,
    site_id TEXT NOT NULL,
    distance_miles REAL,
    same_county INTEGER DEFAULT 0,
    PRIMARY KEY (tri_facility_id, site_id)
);
```

### Implementation
1. Build `src/scrapers/superfund.py` — download NPL site data
2. Calculate haversine distances between TRI facilities and NPL sites
3. Flag facilities within 5-mile radius of any NPL site
4. Add "Legacy Contamination" dashboard section

---

## Enrichment 4: EJScreen Environmental Justice Indicators

### What It Adds
- Populates the EXISTING empty `ej_indicators` table
- Census tract-level environmental burden metrics
- Demographic vulnerability indicators
- Environmental justice composite scores

### Data Source
- Zenodo archive (since EPA removed official downloads): `https://zenodo.org/records/14767363`
- Format: CSV at census tract level
- Coverage: 2015-2024 data archived
- Key fields: PM2.5 percentile, ozone percentile, diesel PM, cancer risk, traffic proximity, superfund proximity, low income percentile, people of color percentile

### Impact on Quality Score
- Currently ej_indicators component = 0.0 for ALL facilities (10% weight)
- Populating this would raise average quality score from 0.847 to ~0.95
- Would also enable the "Environmental Justice" analysis that is the project's key differentiator

### Implementation
1. Download EJScreen CSV from Zenodo (most recent year available)
2. Parse tract-level data, aggregate to county FIPS where needed
3. Populate ej_indicators table using existing schema
4. Link to TRI facilities via FIPS tract code (if available) or FIPS county code
5. Update quality scoring (ej_indicators component will now be 1.0)
6. Enhance "Community Impact" dashboard with EJ percentile overlays

---

## Dashboard Enhancements

### New Tabs to Add
1. **Enforcement Dashboard** — Enforcement actions by facility/industry/state, penalty amounts, compliance rates
2. **Compliance Tracker** — Significant non-compliance flags, quarters in violation, inspection frequency
3. **Historical Trends** — 5-year release trajectories per facility, year-over-year changes
4. **Environmental Justice** — EJ indicators overlay on facility map, demographic burden scores
5. **Superfund Proximity** — Facilities near legacy contamination sites

### Enhanced Existing Tabs
- **Facility Explorer** — Add enforcement history, compliance status, EJ indicators
- **Community Impact** — Add enforcement context, EJ percentiles, Superfund proximity
- **National Overview** — Add total penalties, enforcement counts, compliance rates to KPI cards

---

## Updated Quality Scoring

Update the 9-component quality formula:
```python
QUALITY_WEIGHTS = {
    'has_facility_name': 0.08,
    'has_location_data': 0.08,
    'has_chemical_releases': 0.12,
    'has_release_quantities': 0.12,
    'has_health_data_linked': 0.12,
    'has_demographic_data': 0.08,
    'has_ej_indicators': 0.10,
    'has_industry_classification': 0.08,
    'has_enforcement_data': 0.12,    # NEW
    'has_source_url': 0.05,
    'has_historical_trend': 0.05,    # NEW
}
```

---

## Facility Risk Scoring (NEW)

Create a composite risk score per facility combining:
- Release volume (25%) — total pounds released, weighted by toxicity
- Carcinogen concentration (20%) — proportion of carcinogenic releases
- Enforcement history (20%) — violations, penalties, non-compliance quarters
- Community vulnerability (20%) — EJ indicators, demographics
- Trend direction (15%) — increasing vs decreasing releases over 5 years

Risk tiers: LOW (≥0.8), MEDIUM (≥0.5), HIGH (≥0.3), CRITICAL (<0.3)

---

## Export Updates
- Add enforcement_actions.csv, facility_inspections.csv, compliance_status.csv
- Add superfund_proximity.csv
- Update facility profiles to include enforcement metrics
- Update summary.md with enforcement statistics
- Update Excel workbook with new sheets

---

## Test Requirements (Target: 50+ new tests)
- ECHO download and parsing
- FRS linkage accuracy
- Enforcement-to-facility cross-linking
- Historical TRI trend calculations
- Superfund proximity calculations
- EJScreen data parsing and linking
- Risk scoring formula
- Dashboard data queries
- Export format validation

---

## Priority Order
1. ECHO enforcement data (highest value, creates "polluter accountability" narrative)
2. Historical TRI expansion (enables trend analysis, relatively easy)
3. EJScreen indicators (fills existing empty table, improves quality scores)
4. Superfund proximity (adds legacy contamination context)

---

## Expected Outcome
- From "pollution inventory" → "environmental enforcement and justice platform"
- Unique cross-linking: pollution data + enforcement + community demographics
- No other free public tool connects these 4 data streams
- High-value for: environmental lawyers, investigative journalists, EJ researchers, advocacy organizations
