"""EPA TRI Basic Data File downloader.

Downloads TRI basic data CSV files from EPA's data service.
URL pattern: https://data.epa.gov/efservice/downloads/tri/mv_tri_basic_download/{year}_{state}/csv

The TRI Basic Data File contains one row per facility-chemical-year combination
with columns for release quantities by media (air, water, land), off-site transfers,
and facility information.
"""

from __future__ import annotations

import csv
import io
import time
from pathlib import Path
from typing import Optional

import httpx
from rich.console import Console
from rich.progress import track

console = Console()

PROJECT_ROOT = Path(__file__).parent.parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw" / "tri"
CACHE_DIR = PROJECT_ROOT / "data" / "cache" / "tri"

BASE_URL = "https://data.epa.gov/efservice/downloads/tri"

# All US states + territories
ALL_STATES = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
    "DC", "PR", "VI", "GU", "AS", "MP",
]

# Top 10 states by TRI release volume — download first
PRIORITY_STATES = ["TX", "OH", "IN", "PA", "LA", "IL", "AL", "MI", "NC", "TN"]

# Key TRI CSV column mappings (TRI Basic Data File format — actual headers from CSV)
TRI_COLUMNS = {
    "1. YEAR": "reporting_year",
    "2. TRIFD": "tri_facility_id",
    "4. FACILITY NAME": "facility_name",
    "5. STREET ADDRESS": "street_address",
    "6. CITY": "city",
    "7. COUNTY": "county",
    "8. ST": "state",
    "9. ZIP": "zip_code",
    "12. LATITUDE": "latitude",
    "13. LONGITUDE": "longitude",
    "15. PARENT CO NAME": "parent_company_name",
    "16. PARENT CO DB NUM": "parent_company_db_num",
    "21. FEDERAL FACILITY": "federal_facility",
    "22. INDUSTRY SECTOR CODE": "industry_sector_code",
    "23. INDUSTRY SECTOR": "industry_sector",
    "24. PRIMARY SIC": "sic_code",
    "30. PRIMARY NAICS": "naics_code",
    "37. CHEMICAL": "chemical_name",
    "40. CAS#": "cas_number",
    "42. CLEAN AIR ACT CHEMICAL": "clean_air_act",
    "43. CLASSIFICATION": "classification",
    "44. METAL": "metal_flag",
    "45. METAL CATEGORY": "metal_category",
    "46. CARCINOGEN": "carcinogen_flag",
    "47. PBT": "pbt_flag",
    "48. PFAS": "pfas_flag",
    "50. UNIT OF MEASURE": "unit_of_measure",
    "51. 5.1 - FUGITIVE AIR": "fugitive_air_lbs",
    "52. 5.2 - STACK AIR": "stack_air_lbs",
    "53. 5.3 - WATER": "water_lbs",
    "54. 5.4 - UNDERGROUND": "underground_injection_lbs",
    "57. 5.5.1 - LANDFILLS": "landfill_lbs",
    "58. 5.5.1A - RCRA C LANDFILL": "rcra_landfill_lbs",
    "60. 5.5.2 - LAND TREATMENT": "land_treatment_lbs",
    "61. 5.5.3 - SURFACE IMPNDMNT": "surface_impoundment_lbs",
    "62. 5.5.3A - RCRA SURFACE IM": "rcra_surface_impoundment_lbs",
    "64. 5.5.4 - OTHER DISPOSAL": "other_disposal_lbs",
    "65. ON-SITE RELEASE TOTAL": "on_site_release_total",
    "68. POTW - TOTAL TRANSFERS": "potw_transfers_lbs",
    "88. OFF-SITE RELEASE TOTAL": "off_site_release_total",
    "107. TOTAL RELEASES": "total_releases_lbs",
}


def _safe_float(val: str) -> Optional[float]:
    """Convert string to float, returning None for empty/invalid."""
    if not val or val.strip() == "" or val.strip() == ".":
        return None
    try:
        return float(val.replace(",", ""))
    except (ValueError, TypeError):
        return None


def _safe_int(val: str) -> Optional[int]:
    """Convert string to int, returning None for empty/invalid."""
    if not val or val.strip() == "":
        return None
    try:
        return int(float(val.replace(",", "")))
    except (ValueError, TypeError):
        return None


def _map_row(row: dict) -> dict:
    """Map a TRI CSV row to our internal format."""
    mapped = {}
    for csv_col, our_col in TRI_COLUMNS.items():
        if csv_col in row:
            mapped[our_col] = row[csv_col]

    # If exact column names didn't match, try partial matching
    if not mapped.get("tri_facility_id"):
        for key, val in row.items():
            key_upper = key.upper().strip()
            if "TRIFD" in key_upper:
                mapped["tri_facility_id"] = val
            elif "FACILITY NAME" in key_upper and "facility_name" not in mapped:
                mapped["facility_name"] = val
            elif key_upper.endswith(". ST") or key_upper == "ST":
                mapped["state"] = val
            elif "CHEMICAL" in key_upper and "CLEAN AIR" not in key_upper and "chemical_name" not in mapped:
                mapped["chemical_name"] = val
            elif "TOTAL RELEASES" in key_upper and "total_releases_lbs" not in mapped:
                mapped["total_releases_lbs"] = val
            elif "CARCINOGEN" in key_upper and "carcinogen_flag" not in mapped:
                mapped["carcinogen_flag"] = val
            elif "YEAR" in key_upper and "reporting_year" not in mapped:
                mapped["reporting_year"] = val
            elif "LATITUDE" in key_upper and "latitude" not in mapped:
                mapped["latitude"] = val
            elif "LONGITUDE" in key_upper and "longitude" not in mapped:
                mapped["longitude"] = val
            elif "COUNTY" in key_upper and "FIPS" not in key_upper and "county" not in mapped:
                mapped["county"] = val
            elif "CITY" in key_upper and "city" not in mapped:
                mapped["city"] = val
            elif "ZIP" in key_upper and "zip_code" not in mapped:
                mapped["zip_code"] = val
            elif "STREET" in key_upper and "street_address" not in mapped:
                mapped["street_address"] = val
            elif "SIC" in key_upper and "PRIMARY" in key_upper and "sic_code" not in mapped:
                mapped["sic_code"] = val
            elif "NAICS" in key_upper and "PRIMARY" in key_upper and "naics_code" not in mapped:
                mapped["naics_code"] = val
            elif "INDUSTRY SECTOR" in key_upper and "CODE" not in key_upper and "industry_sector" not in mapped:
                mapped["industry_sector"] = val
            elif "PARENT CO NAME" in key_upper and "parent_company_name" not in mapped:
                mapped["parent_company_name"] = val
            elif "PARENT CO DB" in key_upper and "parent_company_db_num" not in mapped:
                mapped["parent_company_db_num"] = val
            elif "FUGITIVE AIR" in key_upper and "fugitive_air_lbs" not in mapped:
                mapped["fugitive_air_lbs"] = val
            elif "STACK AIR" in key_upper and "stack_air_lbs" not in mapped:
                mapped["stack_air_lbs"] = val
            elif "WATER" in key_upper and "5.3" in key_upper and "water_lbs" not in mapped:
                mapped["water_lbs"] = val
            elif "UNDERGROUND" in key_upper and "underground_injection_lbs" not in mapped:
                mapped["underground_injection_lbs"] = val
            elif "ON-SITE RELEASE TOTAL" in key_upper and "on_site_release_total" not in mapped:
                mapped["on_site_release_total"] = val
            elif "OFF-SITE RELEASE TOTAL" in key_upper and "off_site_release_total" not in mapped:
                mapped["off_site_release_total"] = val
            elif "FIPS STATE" in key_upper and "fips_state" not in mapped:
                mapped["fips_state"] = val
            elif "FIPS COUNTY" in key_upper and "fips_county" not in mapped:
                mapped["fips_county"] = val
            elif "CLASSIFICATION" in key_upper and "classification" not in mapped:
                mapped["classification"] = val
            elif "UNIT OF MEASURE" in key_upper and "unit_of_measure" not in mapped:
                mapped["unit_of_measure"] = val

    # Convert numeric fields
    for field in [
        "latitude", "longitude", "total_releases_lbs", "fugitive_air_lbs",
        "stack_air_lbs", "water_lbs", "underground_injection_lbs",
        "on_site_release_total", "off_site_release_total",
        "landfill_lbs", "land_treatment_lbs", "surface_impoundment_lbs",
        "other_disposal_lbs", "potw_transfers_lbs",
    ]:
        if field in mapped and isinstance(mapped[field], str):
            mapped[field] = _safe_float(mapped[field])

    if "reporting_year" in mapped and isinstance(mapped["reporting_year"], str):
        mapped["reporting_year"] = _safe_int(mapped["reporting_year"])

    # Convert grams to pounds for PBT chemicals (dioxins, lead, mercury, etc.)
    # TRI uses "Grams" for certain persistent bioaccumulative toxic chemicals
    unit = (mapped.get("unit_of_measure") or "").strip()
    if unit == "Grams":
        GRAMS_PER_POUND = 453.592
        for field in [
            "total_releases_lbs", "fugitive_air_lbs", "stack_air_lbs",
            "water_lbs", "underground_injection_lbs", "on_site_release_total",
            "off_site_release_total", "landfill_lbs", "land_treatment_lbs",
            "surface_impoundment_lbs", "other_disposal_lbs", "potw_transfers_lbs",
        ]:
            if field in mapped and mapped[field] is not None:
                mapped[field] = mapped[field] / GRAMS_PER_POUND
        mapped["unit_of_measure"] = "Pounds"  # Normalize to pounds after conversion

    # Compute land_lbs as sum of land disposal methods
    land_components = [
        mapped.get("landfill_lbs") or 0,
        mapped.get("land_treatment_lbs") or 0,
        mapped.get("surface_impoundment_lbs") or 0,
        mapped.get("other_disposal_lbs") or 0,
    ]
    if any(c is not None and c >= 0 for c in [
        mapped.get("landfill_lbs"), mapped.get("land_treatment_lbs"),
        mapped.get("surface_impoundment_lbs"), mapped.get("other_disposal_lbs"),
    ]):
        mapped["land_lbs"] = sum(land_components)

    # Build FIPS county code (state + county)
    fips_state = str(mapped.get("fips_state", "")).strip().zfill(2) if mapped.get("fips_state") else None
    fips_county = str(mapped.get("fips_county", "")).strip().zfill(3) if mapped.get("fips_county") else None
    if fips_state and fips_county and fips_state != "00":
        mapped["fips_county_full"] = fips_state + fips_county
    elif fips_state:
        mapped["fips_county_full"] = None
    else:
        mapped["fips_county_full"] = None

    # Clean string fields
    for field in ["facility_name", "city", "county", "state", "chemical_name", "parent_company_name", "industry_sector"]:
        if field in mapped and isinstance(mapped[field], str):
            mapped[field] = mapped[field].strip()

    return mapped


def download_tri_state_year(
    state: str,
    year: int,
    force: bool = False,
) -> Optional[Path]:
    """Download TRI basic data CSV for a specific state and year.

    Returns path to the cached CSV file, or None on failure.
    """
    cache_file = CACHE_DIR / f"tri_{state}_{year}.csv"
    if cache_file.exists() and not force:
        return cache_file

    cache_file.parent.mkdir(parents=True, exist_ok=True)

    url = f"{BASE_URL}/mv_tri_basic_download/{year}_{state}/csv"

    try:
        with httpx.Client(
            timeout=120,
            headers={"User-Agent": "EPA-TRI-Tracker/0.1 (nathanmauricegoldberg@gmail.com)"},
            follow_redirects=True,
        ) as client:
            resp = client.get(url)
            if resp.status_code == 200:
                content = resp.text
                if len(content.strip()) < 100:
                    # Empty or error response
                    return None
                cache_file.write_text(content)
                return cache_file
            else:
                console.print(f"[yellow]HTTP {resp.status_code} for {state} {year}[/yellow]")
                return None
    except Exception as e:
        console.print(f"[red]Error downloading {state} {year}: {e}[/red]")
        return None


def parse_tri_csv(csv_path: Path) -> list[dict]:
    """Parse a TRI basic data CSV file into structured records."""
    records = []
    with open(csv_path, "r", encoding="utf-8", errors="replace") as f:
        # TRI CSVs may have BOM or extra header rows
        content = f.read()

    # Remove BOM if present
    if content.startswith("\ufeff"):
        content = content[1:]

    reader = csv.DictReader(io.StringIO(content))
    for row in reader:
        mapped = _map_row(row)
        if mapped.get("tri_facility_id") and mapped.get("chemical_name"):
            records.append(mapped)

    return records


def extract_facilities(records: list[dict]) -> list[dict]:
    """Extract unique facility records from TRI release data."""
    facilities: dict[str, dict] = {}

    for r in records:
        fid = r.get("tri_facility_id")
        if not fid:
            continue

        if fid not in facilities:
            facilities[fid] = {
                "tri_facility_id": fid,
                "facility_name": r.get("facility_name"),
                "street_address": r.get("street_address"),
                "city": r.get("city"),
                "county": r.get("county"),
                "state": r.get("state"),
                "zip_code": r.get("zip_code"),
                "latitude": r.get("latitude"),
                "longitude": r.get("longitude"),
                "fips_state": r.get("fips_state"),
                "fips_county": r.get("fips_county_full") or r.get("fips_county"),
                "sic_code": r.get("sic_code"),
                "naics_code": r.get("naics_code"),
                "industry_sector": r.get("industry_sector"),
                "parent_company_name": r.get("parent_company_name"),
                "parent_company_db_num": r.get("parent_company_db_num"),
            }
        else:
            # Update with latest data (prefer non-None)
            existing = facilities[fid]
            for key in ["parent_company_name", "latitude", "longitude", "industry_sector"]:
                if not existing.get(key) and r.get(key):
                    existing[key] = r[key]

    return list(facilities.values())


def extract_releases(records: list[dict]) -> list[dict]:
    """Extract release records from parsed TRI data."""
    releases = []
    for r in records:
        if not r.get("tri_facility_id") or not r.get("chemical_name"):
            continue

        releases.append({
            "tri_facility_id": r["tri_facility_id"],
            "reporting_year": r.get("reporting_year"),
            "chemical_name": r.get("chemical_name"),
            "cas_number": r.get("cas_number"),
            "carcinogen_flag": r.get("carcinogen_flag"),
            "classification": r.get("classification"),
            "unit_of_measure": r.get("unit_of_measure", "Pounds"),
            "total_releases_lbs": r.get("total_releases_lbs"),
            "fugitive_air_lbs": r.get("fugitive_air_lbs"),
            "stack_air_lbs": r.get("stack_air_lbs"),
            "water_lbs": r.get("water_lbs"),
            "land_lbs": r.get("land_lbs"),
            "underground_injection_lbs": r.get("underground_injection_lbs"),
            "off_site_transfers_lbs": r.get("off_site_release_total"),
            "on_site_release_total": r.get("on_site_release_total"),
            "off_site_release_total": r.get("off_site_release_total"),
        })

    return releases


def download_tri_data(
    states: Optional[list[str]] = None,
    years: Optional[list[int]] = None,
    force: bool = False,
) -> dict[str, list[dict]]:
    """Download TRI data for specified states and years.

    Returns dict mapping state codes to lists of parsed records.
    """
    if states is None:
        states = PRIORITY_STATES
    if years is None:
        years = [2022, 2023]  # Most recent complete years

    all_records: dict[str, list[dict]] = {}

    total_tasks = len(states) * len(years)
    completed = 0

    for state in states:
        state_records = []
        for year in years:
            completed += 1
            console.print(f"[dim]({completed}/{total_tasks}) Downloading TRI data: {state} {year}...[/dim]")

            csv_path = download_tri_state_year(state, year, force=force)
            if csv_path:
                records = parse_tri_csv(csv_path)
                state_records.extend(records)
                console.print(f"[green]  {state} {year}: {len(records)} release records[/green]")
            else:
                console.print(f"[yellow]  {state} {year}: no data[/yellow]")

            time.sleep(2)  # Rate limit

        if state_records:
            all_records[state] = state_records

    return all_records
