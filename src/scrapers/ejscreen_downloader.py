"""EPA EJScreen environmental justice indicator data downloader.

Downloads EJScreen data from Zenodo archive (EPA removed official downloads in Feb 2025).
Parses census tract-level EJ indicators and aggregates to county FIPS level.
"""

from __future__ import annotations

import csv
import io
import os
import zipfile
from pathlib import Path
from typing import Optional

import httpx
from rich.console import Console

console = Console()

PROJECT_ROOT = Path(__file__).parent.parent.parent
CACHE_DIR = PROJECT_ROOT / "data" / "cache" / "ejscreen"

# Zenodo archive of EJScreen data (since EPA removed from website)
EJSCREEN_ZENODO_URL = "https://zenodo.org/records/14767363/files"

USER_AGENT = "EPA-TRI-Tracker/0.2 (nathanmauricegoldberg@gmail.com)"

# EJScreen column mapping (varies by year; these are common field names)
EJSCREEN_COLUMNS = {
    "ID": "fips_tract",
    "STATE_NAME": "state_name",
    "ST_ABBREV": "state",
    "CNTY_NAME": "county_name",
    # Environmental indicators (percentiles)
    "P_PM25": "pm25_pctl",
    "P_OZONE": "ozone_pctl",
    "P_DSLPM": "diesel_pm_pctl",
    "P_CANCER": "air_toxics_cancer_risk_pctl",
    "P_CANCR": "air_toxics_cancer_risk_pctl",
    "P_RESP": "respiratory_hazard_pctl",
    "P_PTRAF": "traffic_proximity_pctl",
    "P_PNPL": "superfund_proximity_pctl",
    "P_PRMP": "rmp_proximity_pctl",
    "P_PWDIS": "wastewater_pctl",
    # Demographic indicators (percentiles)
    "P_LWINCPCT": "low_income_pctl",
    "P_MINORPCT": "people_of_color_pctl",
    "P_LINGISOPCT": "linguistic_isolation_pctl",
    "P_LNGISPCT": "linguistic_isolation_pctl",
    "P_UNDER5PCT": "under_5_pctl",
    "P_UNDR5PCT": "under_5_pctl",
    "P_OVER64PCT": "over_64_pctl",
    "P_OVR64PCT": "over_64_pctl",
    # EJ Index
    "P_EJ_PM25": "ej_index_pctl",
    "P_PM25_D2": "ej_index_pctl",
}

# Alternate column names (different EJScreen versions)
ALT_COLUMNS = {
    "GEOID": "fips_tract",
    "GEOID10": "fips_tract",
    "FIPS": "fips_tract",
    "STABBR": "state",
    "STATE": "state",
    "P_D2_PM25": "ej_index_pctl",
    "P_D5_PM25": "ej_index_pctl",
    "PERCENTILE_PM25": "pm25_pctl",
    "PERCENTILE_OZONE": "ozone_pctl",
    "PERCENTILE_DIESEL": "diesel_pm_pctl",
    "PERCENTILE_CANCER": "air_toxics_cancer_risk_pctl",
    "PERCENTILE_RESP": "respiratory_hazard_pctl",
    "PERCENTILE_TRAFFIC": "traffic_proximity_pctl",
    "PERCENTILE_NPL": "superfund_proximity_pctl",
    "PERCENTILE_RMP": "rmp_proximity_pctl",
    "PERCENTILE_WATER": "wastewater_pctl",
    "LOWINCPCT": "low_income_pctl",
    "MINORPCT": "people_of_color_pctl",
    "LINGISOPCT": "linguistic_isolation_pctl",
    "UNDER5PCT": "under_5_pctl",
    "OVER64PCT": "over_64_pctl",
}


def _safe_float(val: str) -> Optional[float]:
    if not val or val.strip() in ("", ".", "N/A", "NA", "None"):
        return None
    try:
        return float(val.replace(",", ""))
    except (ValueError, TypeError):
        return None


def download_ejscreen(force: bool = False) -> list[dict]:
    """Download and parse EJScreen data.

    Tries Zenodo archive first, then falls back to EPA API if available.
    Returns list of tract-level EJ indicator records.
    """
    cache_file = CACHE_DIR / "ejscreen_data.csv"
    if cache_file.exists() and not force:
        console.print("[dim]Using cached ejscreen_data.csv[/dim]")
        return _parse_cached_ejscreen(cache_file)

    cache_file.parent.mkdir(parents=True, exist_ok=True)

    # Try EPA EJScreen API (may be restored)
    records = _try_epa_api()
    if records:
        _save_cache(records, cache_file)
        return records

    # Try Zenodo archive
    records = _try_zenodo(force)
    if records:
        _save_cache(records, cache_file)
        return records

    # NOTE: Previously had a fallback that generated synthetic county-level EJ
    # indicators from TRI release data. Removed because the synthetic values
    # (e.g., labeling TRI release rankings as "pm25_pctl") are scientifically
    # misleading and could produce false conclusions in journalistic use.
    # EJScreen data requires real EPA data — do not fabricate percentiles.

    console.print("[yellow]Could not download EJScreen data from any source.[/yellow]")
    console.print("[yellow]EJScreen was removed from EPA website Feb 2025. Try Zenodo archive manually.[/yellow]")
    return []


def _try_epa_api() -> list[dict]:
    """Try EPA's EJScreen API for tract-level data."""
    # EPA removed EJScreen in Feb 2025, but API may be restored
    url = "https://ejscreen.epa.gov/mapper/ejscreenRESTbroker1.aspx"
    console.print("[dim]Checking EPA EJScreen API availability...[/dim]")
    try:
        with httpx.Client(timeout=30, headers={"User-Agent": USER_AGENT}) as client:
            resp = client.get(url)
            if resp.status_code == 200:
                console.print("[dim]EPA EJScreen API appears available but requires per-tract queries[/dim]")
    except Exception:
        pass
    return []


def _try_zenodo(force: bool = False) -> list[dict]:
    """Try downloading from Zenodo archive."""
    # Zenodo organizes EJScreen by year as ZIP files
    # Use the Zenodo API download endpoint (the /files/ path gives 404)
    zenodo_csv_url = "https://zenodo.org/api/records/14767363/files/2020.zip/content"
    zip_path = CACHE_DIR / "ejscreen_zenodo.zip"

    if zip_path.exists() and not force:
        return _parse_zenodo_zip(zip_path)

    console.print(f"[dim]Downloading EJScreen from Zenodo archive...[/dim]")
    try:
        with httpx.Client(
            timeout=600,
            headers={"User-Agent": USER_AGENT},
            follow_redirects=True,
        ) as client:
            with client.stream("GET", zenodo_csv_url) as resp:
                if resp.status_code != 200:
                    console.print(f"[yellow]Zenodo HTTP {resp.status_code}[/yellow]")
                    return []
                zip_path.parent.mkdir(parents=True, exist_ok=True)
                with open(zip_path, "wb") as f:
                    for chunk in resp.iter_bytes(chunk_size=1024 * 1024):
                        f.write(chunk)
        return _parse_zenodo_zip(zip_path)
    except Exception as e:
        console.print(f"[yellow]Zenodo download failed: {e}[/yellow]")
        if zip_path.exists():
            zip_path.unlink()
        return []


def _parse_zenodo_zip(zip_path: Path) -> list[dict]:
    """Parse EJScreen CSV from Zenodo ZIP.

    Zenodo archives contain nested ZIPs (e.g., EJSCREEN_2020_USPR.csv.zip
    inside 2020.zip). We look for a .csv.zip file first (nested), then
    fall back to a direct .csv file.
    """
    records = []
    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            # First try: nested CSV ZIP (e.g., EJSCREEN_2020_USPR.csv.zip)
            csv_zips = [n for n in zf.namelist() if n.endswith(".csv.zip") and "USPR" in n]
            if not csv_zips:
                csv_zips = [n for n in zf.namelist() if n.endswith(".csv.zip")]

            if csv_zips:
                console.print(f"[dim]Found nested ZIP: {csv_zips[0]}[/dim]")
                with zf.open(csv_zips[0]) as nested_f:
                    nested_bytes = nested_f.read()
                    nested_zf = zipfile.ZipFile(io.BytesIO(nested_bytes))
                    inner_csvs = [n for n in nested_zf.namelist() if n.endswith(".csv")]
                    if inner_csvs:
                        with nested_zf.open(inner_csvs[0]) as cf:
                            content = cf.read().decode("utf-8", errors="replace")
                            if content.startswith("\ufeff"):
                                content = content[1:]
                            records = _parse_ejscreen_csv(content)
                    nested_zf.close()
                return records

            # Fallback: direct CSV in ZIP
            csv_files = [n for n in zf.namelist() if n.endswith(".csv")]
            if not csv_files:
                console.print("[yellow]No CSV in Zenodo ZIP[/yellow]")
                return []

            with zf.open(csv_files[0]) as f:
                text = io.TextIOWrapper(f, encoding="utf-8", errors="replace")
                content = text.read()
                if content.startswith("\ufeff"):
                    content = content[1:]
                records = _parse_ejscreen_csv(content)
    except Exception as e:
        console.print(f"[yellow]Error parsing Zenodo ZIP: {e}[/yellow]")
    return records


def _parse_ejscreen_csv(content: str) -> list[dict]:
    """Parse EJScreen CSV content into records."""
    records = []
    reader = csv.DictReader(io.StringIO(content))

    # Build column mapping from available headers
    col_map = {}
    if reader.fieldnames:
        for header in reader.fieldnames:
            h = header.strip().upper()
            if h in EJSCREEN_COLUMNS:
                col_map[header] = EJSCREEN_COLUMNS[h]
            elif h in ALT_COLUMNS:
                col_map[header] = ALT_COLUMNS[h]

    if not col_map:
        console.print("[yellow]Could not map EJScreen columns[/yellow]")
        return []

    for row in reader:
        mapped = {}
        for csv_col, our_col in col_map.items():
            val = row.get(csv_col, "").strip()
            if our_col == "fips_tract" or our_col == "state":
                mapped[our_col] = val
            else:
                mapped[our_col] = _safe_float(val)

        tract = mapped.get("fips_tract", "")
        if not tract or len(tract) < 5:
            continue

        # Derive county FIPS from tract (first 5 digits)
        mapped["fips_county"] = tract[:5]
        mapped["source"] = "ejscreen"
        records.append(mapped)

    console.print(f"[green]Parsed {len(records)} EJScreen tract records[/green]")
    return records


def _generate_county_level_ej(force: bool = False) -> list[dict]:
    """Generate synthetic county-level EJ indicators from EPA ECHO data.

    As a fallback when EJScreen download is unavailable, we use
    EPA ECHO facility-level data to generate county-level indicators.
    Each county gets a single record with its FIPS as a pseudo-tract.
    """
    console.print("[dim]Generating county-level EJ indicators from existing data...[/dim]")

    try:
        from src.storage.database import get_connection
        conn = get_connection()

        # Get counties with TRI facilities and their release data
        rows = conn.execute("""
            SELECT f.fips_county,
                   f.state,
                   COUNT(DISTINCT f.tri_facility_id) as facility_count,
                   COALESCE(SUM(r.total_releases_lbs), 0) as total_lbs,
                   COUNT(CASE WHEN r.carcinogen_flag = 'YES' THEN 1 END) as carc_count,
                   d.poverty_pct,
                   d.pct_black,
                   d.pct_hispanic,
                   d.pct_native,
                   d.median_household_income
            FROM tri_facilities f
            LEFT JOIN tri_releases r ON f.tri_facility_id = r.tri_facility_id
            LEFT JOIN county_demographics d ON f.fips_county = d.fips_county
            WHERE f.fips_county IS NOT NULL AND f.fips_county != ''
            GROUP BY f.fips_county
        """).fetchall()

        if not rows:
            conn.close()
            return []

        # Compute percentiles based on relative ranking
        counties = [dict(r) for r in rows]

        # Sort by release volume for pollution percentile
        counties.sort(key=lambda x: x.get("total_lbs", 0) or 0)
        total = len(counties)

        records = []
        for i, county in enumerate(counties):
            fips = county["fips_county"]
            pollution_pctl = round((i / max(total - 1, 1)) * 100, 1) if total > 1 else 50.0

            # POC percentile from demographics
            poc_pct = sum(filter(None, [
                county.get("pct_black"),
                county.get("pct_hispanic"),
                county.get("pct_native"),
            ]))
            poverty = county.get("poverty_pct") or 0

            # Simple EJ index: average of pollution burden and demographic vulnerability
            demo_score = min((poc_pct + poverty) / 2, 100) if poc_pct else 50.0
            ej_index = round((pollution_pctl + demo_score) / 2, 1)

            records.append({
                "fips_tract": f"{fips}000000",  # Pseudo-tract for county level
                "fips_county": fips,
                "state": county.get("state"),
                "ej_index_pctl": ej_index,
                "pm25_pctl": pollution_pctl,
                "ozone_pctl": None,
                "diesel_pm_pctl": None,
                "air_toxics_cancer_risk_pctl": round(
                    (county.get("carc_count", 0) / max(county.get("facility_count", 1), 1)) * 100, 1
                ) if county.get("carc_count") else None,
                "respiratory_hazard_pctl": None,
                "traffic_proximity_pctl": None,
                "superfund_proximity_pctl": None,
                "rmp_proximity_pctl": None,
                "wastewater_pctl": None,
                "low_income_pctl": round(poverty * 5, 1) if poverty else None,  # Scale poverty to percentile
                "people_of_color_pctl": round(poc_pct, 1) if poc_pct else None,
                "linguistic_isolation_pctl": None,
                "under_5_pctl": None,
                "over_64_pctl": None,
                "source": "derived_from_tri",
            })

        conn.close()
        console.print(f"[green]Generated {len(records)} county-level EJ indicators[/green]")
        return records

    except Exception as e:
        console.print(f"[yellow]Error generating county EJ data: {e}[/yellow]")
        return []


def _save_cache(records: list[dict], path: Path) -> None:
    """Save records to CSV cache."""
    if not records:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    keys = records[0].keys()
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(records)
    console.print(f"[dim]Cached {len(records)} records to {path}[/dim]")


def _parse_cached_ejscreen(path: Path) -> list[dict]:
    """Parse cached EJScreen CSV."""
    records = []
    with open(path, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            for key in row:
                if key not in ("fips_tract", "fips_county", "state", "source"):
                    row[key] = _safe_float(row.get(key, ""))
            records.append(row)
    return records
