"""County Health Rankings data downloader.

Downloads analytic data CSV from countyhealthrankings.org and extracts
key health outcomes and health factors for cross-linking with TRI facilities.
"""

from __future__ import annotations

import csv
import io
from pathlib import Path
from typing import Optional

import httpx
from rich.console import Console

console = Console()

PROJECT_ROOT = Path(__file__).parent.parent.parent
CACHE_DIR = PROJECT_ROOT / "data" / "cache" / "health"

# County Health Rankings analytic data CSV URLs
CHR_URLS = {
    2024: "https://www.countyhealthrankings.org/sites/default/files/media/document/analytic_data2024.csv",
    2023: "https://www.countyhealthrankings.org/sites/default/files/media/document/analytic_data2023.csv",
}

# State FIPS codes for filtering
STATE_FIPS = {
    "01": "AL", "02": "AK", "04": "AZ", "05": "AR", "06": "CA",
    "08": "CO", "09": "CT", "10": "DE", "11": "DC", "12": "FL",
    "13": "GA", "15": "HI", "16": "ID", "17": "IL", "18": "IN",
    "19": "IA", "20": "KS", "21": "KY", "22": "LA", "23": "ME",
    "24": "MD", "25": "MA", "26": "MI", "27": "MN", "28": "MS",
    "29": "MO", "30": "MT", "31": "NE", "32": "NV", "33": "NH",
    "34": "NJ", "35": "NM", "36": "NY", "37": "NC", "38": "ND",
    "39": "OH", "40": "OK", "41": "OR", "42": "PA", "44": "RI",
    "45": "SC", "46": "SD", "47": "TN", "48": "TX", "49": "UT",
    "50": "VT", "51": "VA", "53": "WA", "54": "WV", "55": "WI",
    "56": "WY",
}

# Key column mappings from CHR analytic data
# These columns contain the actual measure values (v prefix) and CI bounds
CHR_COLUMNS = {
    "statecode": "fips_state",
    "countycode": "fips_county_part",
    "county": "county_name",
    "state": "state_abbr",
    # Health Outcomes
    "v001_rawvalue": "premature_death_rate",     # Years of Potential Life Lost Rate
    "v002_rawvalue": "poor_health_pct",          # % Fair or Poor Health
    "v036_rawvalue": "poor_physical_health_days", # Avg Physically Unhealthy Days
    "v042_rawvalue": "poor_mental_health_days",   # Avg Mentally Unhealthy Days
    "v037_rawvalue": "low_birthweight_pct",       # % Low Birthweight
    # Health Behaviors
    "v009_rawvalue": "adult_smoking_pct",         # % Adults Smoking
    "v011_rawvalue": "adult_obesity_pct",          # % Adults with Obesity
    "v070_rawvalue": "physical_inactivity_pct",   # % Physically Inactive
    "v049_rawvalue": "excessive_drinking_pct",    # % Excessive Drinking
    # Clinical Care
    "v085_rawvalue": "uninsured_pct",             # % Uninsured
    "v005_rawvalue": "preventable_hospital_stays", # Preventable Hospital Stays
    # Length of Life
    "v147_rawvalue": "life_expectancy",           # Life Expectancy
    # Mortality
    "v128_rawvalue": "child_mortality_rate",      # Child Mortality Rate
    "v129_rawvalue": "infant_mortality_rate",     # Infant Mortality Rate
    # Demographics (also included in CHR)
    "v058_rawvalue": "median_household_income",
    "v063_rawvalue": "unemployment_pct",
    "v052_rawvalue": "pct_under_18",
    "v053_rawvalue": "pct_over_65",
    "v069_rawvalue": "poverty_pct",
}


def download_chr_data(year: int = 2024, force: bool = False) -> Optional[Path]:
    """Download County Health Rankings analytic data CSV."""
    if year not in CHR_URLS:
        console.print(f"[red]No URL configured for year {year}[/red]")
        return None

    cache_file = CACHE_DIR / f"chr_analytic_{year}.csv"
    if cache_file.exists() and not force:
        console.print(f"[dim]Using cached CHR data: {cache_file}[/dim]")
        return cache_file

    cache_file.parent.mkdir(parents=True, exist_ok=True)

    url = CHR_URLS[year]
    console.print(f"[dim]Downloading County Health Rankings {year}...[/dim]")

    try:
        with httpx.Client(
            timeout=120,
            headers={"User-Agent": "EPA-TRI-Tracker/0.1 (nathanmauricegoldberg@gmail.com)"},
            follow_redirects=True,
        ) as client:
            resp = client.get(url)
            if resp.status_code == 200:
                cache_file.write_bytes(resp.content)
                console.print(f"[green]Downloaded {len(resp.content):,} bytes -> {cache_file}[/green]")
                return cache_file
            else:
                console.print(f"[red]HTTP {resp.status_code} downloading CHR data[/red]")
                return None
    except Exception as e:
        console.print(f"[red]Error downloading CHR data: {e}[/red]")
        return None


def parse_chr_csv(csv_path: Path, year: int = 2024) -> tuple[list[dict], list[dict]]:
    """Parse County Health Rankings CSV into health and demographic records.

    Returns (health_records, demographic_records).
    """
    health_records = []
    demo_records = []

    with open(csv_path, "r", encoding="utf-8", errors="replace") as f:
        content = f.read()

    # Remove BOM
    if content.startswith("\ufeff"):
        content = content[1:]

    lines = content.split("\n")
    if len(lines) < 3:
        return [], []

    # CHR CSV has two header rows:
    # Row 1: Human-readable names ("State FIPS Code", "Premature Death raw value", ...)
    # Row 2: Variable codes ("statecode", "countycode", "v001_rawvalue", ...)
    # Data starts at row 3
    # Use row 2 as headers (variable codes match our CHR_COLUMNS mapping)
    header_row = lines[1]  # Variable code row
    data_content = header_row + "\n" + "\n".join(lines[2:])

    reader = csv.DictReader(io.StringIO(data_content))

    for row in reader:
        # Get FIPS codes
        state_fips = None
        county_fips_part = None
        county_name = None
        state_abbr = None

        for key, val in row.items():
            key_lower = key.lower().strip()
            if key_lower in ("statecode", "state_fips_code"):
                state_fips = val.strip().zfill(2) if val else None
            elif key_lower in ("countycode", "county_fips_code"):
                county_fips_part = val.strip().zfill(3) if val else None
            elif key_lower == "county":
                county_name = val.strip() if val else None
            elif key_lower == "state":
                state_abbr = val.strip() if val else None

        # Skip state-level summaries (county code 000)
        if not county_fips_part or county_fips_part == "000":
            continue
        if not state_fips or state_fips == "00":
            continue

        fips_county = state_fips + county_fips_part

        # Look up state abbreviation
        if not state_abbr:
            state_abbr = STATE_FIPS.get(state_fips)

        # Extract health measures
        health = {
            "fips_county": fips_county,
            "year": year,
            "state": state_abbr,
            "county_name": county_name,
        }

        demo = {
            "fips_county": fips_county,
            "year": year,
            "state": state_abbr,
            "county_name": county_name,
        }

        for key, val in row.items():
            key_lower = key.lower().strip()
            if key_lower in CHR_COLUMNS:
                target = CHR_COLUMNS[key_lower]
                parsed = _safe_float(val)

                # Route to health or demographics
                if target in (
                    "premature_death_rate", "poor_health_pct",
                    "poor_physical_health_days", "poor_mental_health_days",
                    "low_birthweight_pct", "adult_smoking_pct",
                    "adult_obesity_pct", "physical_inactivity_pct",
                    "excessive_drinking_pct", "uninsured_pct",
                    "preventable_hospital_stays", "life_expectancy",
                    "child_mortality_rate", "infant_mortality_rate",
                ):
                    health[target] = parsed
                elif target in (
                    "median_household_income", "unemployment_pct",
                    "pct_under_18", "pct_over_65", "poverty_pct",
                ):
                    demo[target] = parsed

        # Only include records with some actual data
        health_fields = [v for k, v in health.items() if k not in ("fips_county", "year", "state", "county_name") and v is not None]
        if health_fields:
            health_records.append(health)

        demo_fields = [v for k, v in demo.items() if k not in ("fips_county", "year", "state", "county_name") and v is not None]
        if demo_fields:
            demo_records.append(demo)

    return health_records, demo_records


def _safe_float(val: str) -> Optional[float]:
    """Convert string to float, returning None for empty/invalid."""
    if not val or val.strip() == "" or val.strip() == ".":
        return None
    try:
        return float(val.replace(",", ""))
    except (ValueError, TypeError):
        return None


def ingest_chr_data(year: int = 2024, force: bool = False) -> tuple[int, int]:
    """Download, parse, and store County Health Rankings data.

    Returns (health_count, demo_count) of records stored.
    """
    from src.storage.database import (
        get_connection,
        store_county_health_batch,
        store_county_demographics_batch,
    )

    csv_path = download_chr_data(year=year, force=force)
    if not csv_path:
        return 0, 0

    health_records, demo_records = parse_chr_csv(csv_path, year=year)
    console.print(f"[dim]Parsed {len(health_records)} health records, {len(demo_records)} demographic records[/dim]")

    conn = get_connection()
    hc, hu = store_county_health_batch(health_records, conn=conn)
    dc, du = store_county_demographics_batch(demo_records, conn=conn)

    console.print(f"[green]Health: {hc} created, {hu} updated[/green]")
    console.print(f"[green]Demographics: {dc} created, {du} updated[/green]")

    conn.close()
    return hc + hu, dc + du
