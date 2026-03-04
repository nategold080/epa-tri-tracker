# EPA TRI Community Health & Demographics Tracker — Methodology

## Overview

The EPA TRI Community Health & Demographics Tracker is the first publicly available dataset that cross-links EPA Toxic Release Inventory facility-level chemical release data with county-level health outcomes and demographic profiles for all 50 US states. The dataset contains **22,936 TRI facilities**, **155,814 chemical release records** spanning **571 unique chemicals**, and **6.54 billion pounds** of total reported releases, joined to health and demographic data for **3,143 US counties**. All extraction is deterministic and rule-based with zero LLM dependency.

## Data Sources

| Source | Provider | Coverage | Records |
|--------|----------|----------|---------|
| TRI Basic Data Files | EPA (data.epa.gov) | All 50 states, 2022-2023 | 155,814 release records across 22,936 facilities |
| County Health Rankings & Roadmaps | countyhealthrankings.org | National, 2024 release | 3,143 county health outcome and health factor records |
| Census Demographics | Embedded in County Health Rankings | National | 3,143 county demographic profiles (income, poverty, race/ethnicity, age, education) |

## Extraction & Normalization

TRI data is downloaded as bulk CSV files from EPA's data.epa.gov, one file per state per reporting year. Records are parsed using deterministic rule-based extraction. Facility name normalization includes suffix stripping (LLC, Inc, Corp, Plant), abbreviation expansion, and direct parent company linkage from TRI reporting fields. Industry sectors are classified from SIC codes into 30 standardized categories. A total of 4,480 unique parent companies and 44,019 carcinogen release records (28.3% of all releases) are identified.

## County Cross-Linkage

Facilities are joined to county health and demographic data via FIPS county codes, achieving **100% resolution** across all records. The matching process handles several normalization challenges: county/parish/borough/census area suffix differences, collapsed spacing (DeKalb vs. De Kalb), Saint/St./St abbreviation variants, Virginia independent city formats, Alaska truncated borough names, and renamed census areas (e.g., Valdez-Cordova to Chugach). County Health Rankings data is parsed from a dual-header CSV format and joined on resolved FIPS codes.

## Quality Scoring

Every record receives a quality score on a 0.0--1.0 scale using a 9-component weighted formula covering: facility information completeness, location data, chemical identification, release quantities, county health data availability, demographic data availability, and industry classification. The dataset average quality score is **0.847**.

## Coverage

- **Geographic:** All 50 US states (territories not yet included)
- **Temporal:** Reporting years 2022 and 2023
- **Facility matching:** 100% of TRI facilities matched to county-level health and demographic data
- **Chemical scope:** 571 unique chemicals, including carcinogen classification

## Limitations

- **EJScreen data not integrated.** EPA removed EJScreen from its website in February 2025; archived copies on Zenodo exceed 5 GB per year and are not yet incorporated.
- **Two reporting years only.** The TRI program has data back to 1987; historical expansion is planned.
- **County-level granularity.** Census tract-level demographics are not included; all health and demographic data is at the county level.
- **No chemical toxicity scoring.** EPA IRIS toxicity classifications are not yet integrated.
- **Parent company linkage is direct.** Corporate subsidiary resolution beyond what TRI reporters disclose is not performed.

---

**Built by Nathan Goldberg** | nathanmauricegoldberg@gmail.com | [linkedin.com/in/nathangoldberg](https://linkedin.com/in/nathangoldberg)
