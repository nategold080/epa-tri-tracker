# EPA Toxic Release Inventory — Community Health & Demographics Tracker

## Project Overview
Cross-linked database of EPA Toxic Release Inventory (TRI) facility-level toxic chemical releases with County Health Rankings, Census demographics, and EPA EJScreen environmental justice data. For any reporting facility, see not just what chemicals are released and in what quantities, but the health outcomes, demographics, and environmental justice indicators of the surrounding communities. No free, open, cross-linked database connecting facility-level toxic releases to community health and demographic context exists.

## Core Value Proposition
- **EPA TRI Explorer** shows facility releases but NOT community health/demographic context
- **County Health Rankings** shows county-level health but NOT facility-level pollution sources
- **EJScreen** shows environmental justice indicators but does NOT cross-link to actual TRI releases and health outcomes
- **Academic studies** have done individual analyses but no persistent, searchable database exists
- **Environmental justice organizations** need this data but lack technical capacity to build it
- **Nobody** has built a free, unified database connecting TRI facility releases → community health → demographics → EJ indicators at scale

## Data Sources

| Source | Format | Key Fields | URL |
|--------|--------|------------|-----|
| EPA TRI Basic Data | CSV download | Facility name, address, lat/lon, FIPS, chemicals, release quantities, SIC codes | data.epa.gov |
| County Health Rankings | CSV download | Health outcomes, behaviors, clinical care, social/economic factors by county | countyhealthrankings.org |
| Census ACS | REST API | Demographics, income, poverty, education by county/tract | api.census.gov |
| EPA EJScreen | Downloadable | Environmental justice indicators by block group | epa.gov/ejscreen |
| EPA IRIS | Reference | Chemical toxicity classifications, cancer hazard | epa.gov/iris |

## Technical Standards
- Python 3.12+, SQLite WAL mode, Click CLI, Streamlit + Plotly
- Zero LLM dependency for core pipeline
- Quality scoring on every record (weighted 0.0-1.0)
- All data from public government sources (CSV downloads + APIs)
- Dark theme dashboard: primaryColor="#0984E3", backgroundColor="#0E1117"
- Footer: "Built by Nathan Goldberg" + nathanmauricegoldberg@gmail.com + LinkedIn
- Contact in User-Agent headers: nathanmauricegoldberg@gmail.com

## Entity Resolution Strategy

### Primary Join Keys
- **TRI facility → County**: FIPS county code (direct join)
- **TRI facility → Census tract**: Lat/lon → Census tract geocoding OR facility FIPS codes
- **TRI chemical → Toxicity**: CAS number to EPA IRIS hazard classification
- **TRI facility → Industry**: SIC code to industry sector name

### Facility Name Normalization
1. Strip common suffixes (LLC, Inc., Corp., Plant, Facility)
2. Normalize abbreviations (Mfg → Manufacturing, Chem → Chemical)
3. Parent company linkage where identifiable from TRI data
4. DUNS number matching where available in TRI data

## Quality Scoring Formula
Each cross-linked facility record scored 0.0-1.0:
- has_facility_name: 0.10
- has_location_data: 0.10 (lat/lon + FIPS)
- has_chemical_releases: 0.15
- has_release_quantities: 0.15
- has_health_data_linked: 0.15 (county health rankings joined)
- has_demographic_data: 0.10 (Census data joined)
- has_ej_indicators: 0.10 (EJScreen data joined)
- has_industry_classification: 0.10
- has_source_url: 0.05

## Database Schema (Core Tables)

### tri_facilities
- facility_id (TRI_FACILITY_ID)
- facility_name, canonical_name
- street_address, city, county, state, zip
- latitude, longitude
- fips_state, fips_county
- sic_code, sic_name, naics_code
- industry_sector
- parent_company_name
- quality_score

### tri_releases
- facility_id (FK)
- reporting_year
- chemical_name, cas_number
- carcinogen_flag
- unit_of_measure
- total_releases_lbs
- fugitive_air_lbs, stack_air_lbs
- water_lbs, land_lbs
- underground_injection_lbs
- off_site_transfers_lbs

### county_health
- fips_county (PK for year)
- year
- county_name, state
- premature_death_rate
- poor_health_pct
- poor_mental_health_days
- low_birthweight_pct
- adult_smoking_pct
- adult_obesity_pct
- physical_inactivity_pct
- uninsured_pct
- preventable_hospital_stays
- life_expectancy

### county_demographics
- fips_county (PK for year)
- year
- total_population
- median_household_income
- poverty_pct
- unemployment_pct
- pct_white, pct_black, pct_hispanic, pct_asian
- pct_under_18, pct_over_65
- pct_no_highschool
- rural_pct

### ej_indicators
- fips_tract or fips_blockgroup
- fips_county
- ej_index_pctl
- pm25_pctl, ozone_pctl, diesel_pm_pctl
- air_toxics_cancer_risk_pctl
- traffic_proximity_pctl
- superfund_proximity_pctl
- wastewater_pctl
- low_income_pctl, people_of_color_pctl, linguistic_isolation_pctl

## Build Order
1. Config files (sources.yaml, chemicals.yaml, industry_sectors.yaml)
2. TRI data downloader — Fetch CSV basic data files by state and year
3. County Health Rankings downloader — Fetch annual CSV
4. Census ACS downloader — Fetch county demographics via API
5. EJScreen downloader — Fetch environmental justice indicator data
6. Extractors — Parse TRI CSV fields, normalize chemicals, classify industries
7. Cross-linker — Join facilities to counties (FIPS), overlay health + demographics
8. Normalization — Facility name resolution, parent company linkage
9. Validation — Quality scoring, deduplication
10. Storage — SQLite schema, CRUD operations
11. Pipeline — Wire download → extract → cross-link → normalize → validate → store
12. Run pipeline for ALL 50 states (start with top 10 by release volume)
13. Exports — CSV, JSON, Excel, Markdown
14. Dashboard — 7+ interactive sections
15. Methodology doc — 1-page PDF
16. Tests — 50+ covering all stages

## Dashboard Sections (planned)
1. **National Overview** — KPI cards: total facilities, states, chemicals tracked, total lbs released
2. **Facility Explorer** — Search/filter by state, company, chemical, industry, release amount
3. **Community Impact** — For any county: TRI facilities, total releases, health outcomes, demographics
4. **Environmental Justice Analysis** — Facilities in high-EJ-burden communities, disparity metrics
5. **Chemical Analysis** — Top chemicals by volume, carcinogen releases, trends over time
6. **Industry Sector Analysis** — Releases by SIC/NAICS sector, top polluting industries
7. **Geographic Map** — Interactive map with facility locations, sized by release volume, colored by EJ indicators
8. **Trends** — Year-over-year release trends at facility, county, state, national level

## Target Audiences
- **Environmental justice organizations** (Earthjustice, EDF, WE ACT, NAACP Environmental and Climate Justice Program)
- **Environmental law firms** (litigating pollution cases)
- **Academic researchers** (Dr. Robert Bullard, environmental health departments)
- **Investigative journalists** (ProPublica, The Guardian, Inside Climate News)
- **EPA and state environmental agencies**
- **Public health departments**
- **Bloomberg Philanthropies Environment Program**
- **Community groups** near TRI facilities
