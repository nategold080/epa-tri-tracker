# EPA TRI Community Health & Demographics Tracker

A cross-linked database connecting EPA Toxic Release Inventory (TRI) facility-level toxic chemical releases to county-level health outcomes and demographics across all 50 US states.

## Key Statistics

- **22,936 facilities** reporting toxic releases across all 50 states
- **155,814 chemical release records** covering 571 distinct chemicals
- **6.5 billion pounds** of total toxic releases (2022-2023)
- **44,019 carcinogen release records** (657.5 million pounds)
- **3,143 counties** cross-linked with health outcomes and demographics
- **4,480 parent companies** across 30 industry sectors
- **0.847 average quality score** (100% above threshold)

## What Makes This Unique

No free, public database currently connects:
- EPA TRI facility releases (what pollutants, how much, from whom)
- County Health Rankings (premature death rates, poor health, obesity, etc.)
- Census demographics (income, poverty, race/ethnicity, education)

This database bridges those gaps, enabling queries like: *"What chemicals are released in my county, and what are the health outcomes here?"*

## Data Sources

| Source | Records | Coverage |
|--------|---------|----------|
| EPA Toxic Release Inventory | 22,936 facilities, 155,814 releases | All 50 states, 2022-2023 |
| County Health Rankings | 3,143 county records | Health outcomes, behaviors, clinical care |
| Census ACS (via CHR) | 3,143 county records | Income, poverty, demographics |

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Run the dashboard
streamlit run src/dashboard/app.py

# CLI commands
python -m src.cli scrape --source tri --state TX
python -m src.cli normalize
python -m src.cli validate
python -m src.cli export --format all
```

## Project Structure

```
src/
  scrapers/          # Data downloaders (TRI, County Health Rankings)
  normalization/     # FIPS resolution, facility name normalization
  validation/        # Quality scoring (9-component weighted formula)
  storage/           # SQLite database operations
  export/            # CSV, JSON, Excel, Markdown exporters
  dashboard/         # 7-section Streamlit dashboard
tests/               # 169 tests
data/exports/        # Generated data files
docs/                # Methodology documentation
outreach/            # 24 targets, email templates, pitch materials
```

## Dashboard Sections

1. **National Overview** - KPI cards, state rankings, chemical/industry breakdowns
2. **Facility Explorer** - Search/filter with county health context
3. **Community Impact** - County-level TRI + health + demographics
4. **Chemical Analysis** - Carcinogen breakdown, release pathways
5. **Industry Analysis** - Sector comparisons
6. **Geographic Map** - Interactive facility map
7. **Trends** - Year-over-year analysis

## Exports

Available in `data/exports/`:
- `tri_facilities.csv` / `.json` (22,936 records)
- `tri_releases.csv` / `.json` (155,814 records)
- `epa_tri_tracker.xlsx` (styled Excel workbook)
- `summary_stats.md` (summary with tables)

## Tests

```bash
python -m pytest tests/ -v
# 169 tests passing
```

## Built By

**Nathan Goldberg**
- Email: nathanmauricegoldberg@gmail.com
- LinkedIn: linkedin.com/in/nathangoldberg
