"""EPA Superfund/NPL site data downloader and proximity calculator.

Downloads NPL site data and computes distances from TRI facilities
to nearby Superfund sites using the haversine formula.
"""

from __future__ import annotations

import csv
import io
import math
import time
import zipfile
from pathlib import Path
from typing import Optional

import httpx
from rich.console import Console

console = Console()

PROJECT_ROOT = Path(__file__).parent.parent.parent
CACHE_DIR = PROJECT_ROOT / "data" / "cache" / "superfund"

# Superfund NPL site data from EPA
SUPERFUND_URL = "https://echo.epa.gov/files/echodownloads/frs_downloads.zip"

USER_AGENT = "EPA-TRI-Tracker/0.2 (nathanmauricegoldberg@gmail.com)"

# Proximity radius in miles
PROXIMITY_RADIUS_MILES = 5.0


def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate distance in miles between two lat/lon points using haversine formula."""
    R = 3958.8  # Earth radius in miles

    lat1_r = math.radians(lat1)
    lat2_r = math.radians(lat2)
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)

    a = math.sin(dlat / 2) ** 2 + math.cos(lat1_r) * math.cos(lat2_r) * math.sin(dlon / 2) ** 2
    c = 2 * math.asin(math.sqrt(a))

    return R * c


def download_superfund_sites(force: bool = False) -> list[dict]:
    """Download NPL/Superfund site data from EPA FRS data.

    The FRS download includes SEMS program data which contains Superfund sites.
    We also try a direct NPL site list.
    """
    cache_file = CACHE_DIR / "superfund_sites.csv"
    if cache_file.exists() and not force:
        console.print("[dim]Using cached superfund_sites.csv[/dim]")
        return _parse_superfund_csv(cache_file)

    cache_file.parent.mkdir(parents=True, exist_ok=True)

    # Try EPA SEMS API for NPL sites
    sites = _download_from_envirofacts(force)
    if sites:
        _save_sites_csv(sites, cache_file)
        return sites

    # Fallback: extract from FRS data
    sites = _extract_from_frs(force)
    if sites:
        _save_sites_csv(sites, cache_file)
        return sites

    console.print("[yellow]Could not download Superfund site data[/yellow]")
    return []


def _download_from_envirofacts(force: bool = False) -> list[dict]:
    """Try downloading NPL sites from EPA Envirofacts SEMS API."""
    url = "https://data.epa.gov/efservice/SEMS_ACTIVE_SITES/SITE_TYPE/NPL/CSV"
    console.print(f"[dim]Trying Envirofacts SEMS API for NPL sites...[/dim]")

    try:
        with httpx.Client(
            timeout=120,
            headers={"User-Agent": USER_AGENT},
            follow_redirects=True,
        ) as client:
            resp = client.get(url)
            if resp.status_code == 200 and len(resp.text) > 200:
                sites = _parse_sems_csv(resp.text)
                if sites:
                    console.print(f"[green]Downloaded {len(sites)} NPL sites from Envirofacts[/green]")
                    return sites
    except Exception as e:
        console.print(f"[yellow]Envirofacts API failed: {e}[/yellow]")

    return []


def _parse_sems_csv(content: str) -> list[dict]:
    """Parse SEMS CSV content into site records."""
    if content.startswith("\ufeff"):
        content = content[1:]

    sites = []
    reader = csv.DictReader(io.StringIO(content))
    for row in reader:
        site_id = (row.get("SITE_EPA_ID", "") or row.get("EPA_ID", "") or row.get("SITE_ID", "")).strip()
        if not site_id:
            continue

        lat = _safe_float(row.get("LATITUDE", "") or row.get("LAT", ""))
        lon = _safe_float(row.get("LONGITUDE", "") or row.get("LON", ""))

        sites.append({
            "site_id": site_id,
            "site_name": (row.get("SITE_NAME", "") or row.get("NAME", "")).strip() or None,
            "address": (row.get("ADDRESS", "") or row.get("STREET_ADDRESS", "")).strip() or None,
            "city": (row.get("CITY", "") or row.get("CITY_NAME", "")).strip() or None,
            "state": (row.get("STATE", "") or row.get("STATE_CODE", "")).strip() or None,
            "zip_code": (row.get("ZIPCODE", "") or row.get("ZIP_CODE", "")).strip() or None,
            "latitude": lat,
            "longitude": lon,
            "npl_status": (row.get("NPL_STATUS", "") or row.get("SITE_TYPE", "") or "Final").strip(),
            "federal_facility": 1 if (row.get("FEDERAL_FACILITY_FLAG", "") or "").upper() in ("Y", "YES") else 0,
        })

    return sites


def _extract_from_frs(force: bool = False) -> list[dict]:
    """Extract Superfund sites from the FRS download as fallback."""
    frs_zip = CACHE_DIR.parent / "echo" / "frs.zip"
    if not frs_zip.exists():
        console.print("[yellow]FRS zip not found for Superfund extraction[/yellow]")
        return []

    console.print("[dim]Extracting Superfund sites from FRS data...[/dim]")
    sites = []

    try:
        with zipfile.ZipFile(frs_zip, "r") as zf:
            # Look for program interest records with SEMS
            matching = [n for n in zf.namelist() if "program" in n.lower() and n.endswith(".csv")]
            if not matching:
                return []

            with zf.open(matching[0]) as f:
                text = io.TextIOWrapper(f, encoding="utf-8", errors="replace")
                content = text.read()
                if content.startswith("\ufeff"):
                    content = content[1:]

                reader = csv.DictReader(io.StringIO(content))
                sems_reg_ids = set()
                for row in reader:
                    pgm = (row.get("PGM_SYS_ACRNM", "") or "").strip()
                    if pgm == "SEMS":
                        reg_id = (row.get("REGISTRY_ID", "") or "").strip()
                        pgm_id = (row.get("PGM_SYS_ID", "") or "").strip()
                        if reg_id:
                            sems_reg_ids.add(reg_id)

            # Now get facility details for these registry IDs
            fac_matching = [n for n in zf.namelist() if "facilit" in n.lower() and n.endswith(".csv")]
            if fac_matching:
                with zf.open(fac_matching[0]) as f:
                    text = io.TextIOWrapper(f, encoding="utf-8", errors="replace")
                    content = text.read()
                    if content.startswith("\ufeff"):
                        content = content[1:]

                    reader = csv.DictReader(io.StringIO(content))
                    for row in reader:
                        reg_id = (row.get("REGISTRY_ID", "") or "").strip()
                        if reg_id not in sems_reg_ids:
                            continue
                        # FRS_FACILITIES columns: FAC_NAME, FAC_STREET, FAC_CITY, FAC_STATE, FAC_ZIP,
                        # LATITUDE_MEASURE, LONGITUDE_MEASURE (or LATITUDE83, LONGITUDE83)
                        lat = _safe_float(
                            row.get("LATITUDE_MEASURE", "")
                            or row.get("LATITUDE83", "")
                            or row.get("LATITUDE", "")
                        )
                        lon = _safe_float(
                            row.get("LONGITUDE_MEASURE", "")
                            or row.get("LONGITUDE83", "")
                            or row.get("LONGITUDE", "")
                        )
                        sites.append({
                            "site_id": reg_id,
                            "site_name": (
                                row.get("FAC_NAME", "")
                                or row.get("PRIMARY_NAME", "")
                            ).strip() or None,
                            "address": (
                                row.get("FAC_STREET", "")
                                or row.get("LOCATION_ADDRESS", "")
                            ).strip() or None,
                            "city": (
                                row.get("FAC_CITY", "")
                                or row.get("CITY_NAME", "")
                            ).strip() or None,
                            "state": (
                                row.get("FAC_STATE", "")
                                or row.get("STATE_CODE", "")
                            ).strip() or None,
                            "zip_code": (
                                row.get("FAC_ZIP", "")
                                or row.get("POSTAL_CODE", "")
                            ).strip() or None,
                            "latitude": lat,
                            "longitude": lon,
                            "npl_status": "Final",
                            "federal_facility": 0,
                        })

        console.print(f"[green]Extracted {len(sites)} Superfund sites from FRS[/green]")
    except Exception as e:
        console.print(f"[red]Error extracting from FRS: {e}[/red]")

    return sites


def _safe_float(val: str) -> Optional[float]:
    if not val or val.strip() in ("", ".", "N/A"):
        return None
    try:
        return float(val.replace(",", ""))
    except (ValueError, TypeError):
        return None


def _save_sites_csv(sites: list[dict], path: Path) -> None:
    """Cache sites to CSV."""
    if not sites:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    keys = sites[0].keys()
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(sites)


def _parse_superfund_csv(path: Path) -> list[dict]:
    """Parse cached Superfund sites CSV."""
    sites = []
    with open(path, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            row["latitude"] = _safe_float(row.get("latitude", ""))
            row["longitude"] = _safe_float(row.get("longitude", ""))
            row["federal_facility"] = int(row.get("federal_facility", 0) or 0)
            sites.append(row)
    return sites


def compute_proximity(
    facilities: list[dict],
    superfund_sites: list[dict],
    radius_miles: float = PROXIMITY_RADIUS_MILES,
) -> list[dict]:
    """Compute proximity between TRI facilities and Superfund sites.

    For each facility, find all Superfund sites within radius_miles.
    Returns list of proximity records.
    """
    # Filter sites with coordinates
    sites_with_coords = [
        s for s in superfund_sites
        if s.get("latitude") is not None and s.get("longitude") is not None
    ]
    console.print(f"[dim]Computing proximity for {len(facilities)} facilities × {len(sites_with_coords)} sites...[/dim]")

    proximity_records = []

    for fac in facilities:
        fac_lat = fac.get("latitude")
        fac_lon = fac.get("longitude")
        fac_id = fac.get("tri_facility_id")
        fac_fips = fac.get("fips_county")

        if fac_lat is None or fac_lon is None or not fac_id:
            continue

        for site in sites_with_coords:
            site_lat = site["latitude"]
            site_lon = site["longitude"]

            # Quick bounding box filter (1 degree ~ 69 miles)
            if abs(fac_lat - site_lat) > radius_miles / 50.0:
                continue
            if abs(fac_lon - site_lon) > radius_miles / 40.0:
                continue

            dist = haversine_distance(fac_lat, fac_lon, site_lat, site_lon)
            if dist <= radius_miles:
                proximity_records.append({
                    "tri_facility_id": fac_id,
                    "site_id": site["site_id"],
                    "distance_miles": round(dist, 2),
                    "same_county": 0,  # Would need site FIPS to determine
                })

    console.print(f"[green]Found {len(proximity_records)} facility-site proximity pairs within {radius_miles} miles[/green]")
    return proximity_records
