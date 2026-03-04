"""EPA Risk Management Program (RMP) data downloader.

Downloads RMP facility, chemical, and accident data from the Data Liberation
Project's archive of EPA RMP public data. RMP tracks facilities handling
hazardous chemicals with potential for catastrophic accidents — complementing
TRI's routine release data with acute accident risk.

Data source: Data Liberation Project (via FOIA)
URL: https://dlp-cdn.muckrock.com/EPA%20RMP%20Data/Records%20Provided%202025-04/CSV%20Files%20Directly%20From%20EPA/
"""

from __future__ import annotations

import csv
import io
import re
import time
from pathlib import Path
from typing import Optional

import httpx
from rich.console import Console

console = Console()

PROJECT_ROOT = Path(__file__).parent.parent.parent
CACHE_DIR = PROJECT_ROOT / "data" / "cache" / "rmp"

BASE_URL = "https://dlp-cdn.muckrock.com/EPA%20RMP%20Data/Records%20Provided%202025-04/CSV%20Files%20Directly%20From%20EPA"

# Key CSV files we need
RMP_FILES = {
    "facilities": f"{BASE_URL}/tblS1Facilities_1.csv",
    "processes": f"{BASE_URL}/tblS1Processes_1.csv",
    "chemicals": f"{BASE_URL}/tblS1ProcessChemicals_1.csv",
    "accidents": f"{BASE_URL}/tblS6AccidentHistory_1.csv",
    "accident_chemicals": f"{BASE_URL}/tblS6AccidentChemicals_1.csv",
    "toxic_worst_case": f"{BASE_URL}/tblS2ToxicsWorstCase_1.csv",
    "toxic_alt_release": f"{BASE_URL}/tblS3ToxicsAltReleases_1.csv",
    "chemicals_lookup": f"{BASE_URL}/tlkpChemicals_1.csv",
    "rmp_track": f"{BASE_URL}/tblRMPTrack_1.csv",
}

USER_AGENT = "EPA-TRI-Tracker/0.2 (nathanmauricegoldberg@gmail.com)"


def _download_csv(url: str, name: str, force: bool = False) -> Optional[Path]:
    """Download a CSV file with caching."""
    csv_path = CACHE_DIR / f"{name}.csv"
    if csv_path.exists() and not force:
        console.print(f"[dim]Using cached {name}.csv[/dim]")
        return csv_path

    csv_path.parent.mkdir(parents=True, exist_ok=True)
    console.print(f"[dim]Downloading {name}...[/dim]")

    try:
        with httpx.Client(
            timeout=600,
            headers={"User-Agent": USER_AGENT},
            follow_redirects=True,
        ) as client:
            with client.stream("GET", url) as resp:
                if resp.status_code != 200:
                    console.print(f"[yellow]HTTP {resp.status_code} for {name}[/yellow]")
                    return None
                with open(csv_path, "wb") as f:
                    for chunk in resp.iter_bytes(chunk_size=1024 * 1024):
                        f.write(chunk)
        size_mb = csv_path.stat().st_size / 1024 / 1024
        console.print(f"[green]Downloaded {name}: {size_mb:.1f} MB[/green]")
        return csv_path
    except Exception as e:
        console.print(f"[red]Error downloading {name}: {e}[/red]")
        if csv_path.exists():
            csv_path.unlink()
        return None


def _read_csv(path: Path, max_rows: int = 0) -> list[dict]:
    """Read a CSV file into list of dicts, stripping whitespace from keys and values."""
    records = []
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        content = f.read()
        if content.startswith("\ufeff"):
            content = content[1:]
        reader = csv.DictReader(io.StringIO(content))
        for i, row in enumerate(reader):
            if max_rows and i >= max_rows:
                break
            # Strip whitespace from keys (RMP CSVs have trailing spaces in headers)
            cleaned = {k.strip(): v.strip() if isinstance(v, str) else v for k, v in row.items() if k is not None}
            records.append(cleaned)
    return records


def _safe_float(val) -> Optional[float]:
    if val is None:
        return None
    val = str(val).strip()
    if not val or val in ("", ".", "N/A", "NA", "None", "NULL"):
        return None
    try:
        return float(val.replace(",", "").replace("$", ""))
    except (ValueError, TypeError):
        return None


def _safe_int(val) -> Optional[int]:
    if val is None:
        return None
    val = str(val).strip()
    if not val or val in ("", ".", "N/A", "NA", "None", "NULL"):
        return None
    try:
        return int(float(val.replace(",", "")))
    except (ValueError, TypeError):
        return None


def _clean_str(val) -> Optional[str]:
    if val is None:
        return None
    val = str(val).strip()
    if not val or val in ("None", "NULL", "N/A"):
        return None
    return val


# --- Facility data ---

def download_rmp_facilities(force: bool = False) -> list[dict]:
    """Download and parse RMP facility registration data.

    Returns list of facility dicts ready for database storage.
    """
    # Download facility CSV
    fac_path = _download_csv(RMP_FILES["facilities"], "facilities", force=force)
    if not fac_path:
        return []
    time.sleep(2)

    # Download RMP tracking data for submission dates
    track_path = _download_csv(RMP_FILES["rmp_track"], "rmp_track", force=force)
    time.sleep(2)

    # Build EPAFacilityID → latest submission date mapping from rmp_track
    submission_dates_by_epa_id = {}
    if track_path:
        track_rows = _read_csv(track_path)
        for row in track_rows:
            epa_id = _clean_str(row.get("EPAFacilityID"))
            if not epa_id:
                continue
            sub_date = _clean_str(row.get("ReceiptDate")) or _clean_str(row.get("PostmarkDate"))
            if sub_date:
                if epa_id not in submission_dates_by_epa_id or sub_date > submission_dates_by_epa_id[epa_id]:
                    submission_dates_by_epa_id[epa_id] = sub_date

    console.print(f"[dim]RMP tracking: {len(submission_dates_by_epa_id):,} EPA IDs with submission dates[/dim]")

    # Parse facility data
    fac_rows = _read_csv(fac_path)
    console.print(f"[dim]RMP facility raw records: {len(fac_rows):,}[/dim]")

    facilities = []
    seen_ids = set()
    for row in fac_rows:
        fac_id = _clean_str(row.get("FacilityID"))
        if not fac_id or fac_id in seen_ids:
            continue
        seen_ids.add(fac_id)

        lat = _safe_float(row.get("FacilityLatDecDegs"))
        lon = _safe_float(row.get("FacilityLongDecDegs"))
        epa_id = _clean_str(row.get("EPAFacilityID"))

        # Get submission/deregistration from facilities file or track file
        sub_date = _clean_str(row.get("ReceiptDate")) or _clean_str(row.get("CompletionCheckDate"))
        if epa_id and epa_id in submission_dates_by_epa_id:
            sub_date = submission_dates_by_epa_id[epa_id]
        dereg_date = _clean_str(row.get("DeRegistrationDate")) or _clean_str(row.get("DeRegistrationEffectiveDate"))

        facilities.append({
            "rmp_id": fac_id,
            "facility_name": _clean_str(row.get("FacilityName")),
            "street_address": _clean_str(row.get("FacilityStr1")),
            "city": _clean_str(row.get("FacilityCity")),
            "state": _clean_str(row.get("FacilityState")),
            "zip_code": _clean_str(row.get("FacilityZipCode")),
            "latitude": lat,
            "longitude": lon,
            "frs_registry_id": epa_id,
            "naics_code": _clean_str(row.get("FacilityNAICS")),
            "num_processes": _safe_int(row.get("FTE")),  # FTE is available; processes need separate count
            "num_chemicals": None,  # Will be set from chemical inventory
            "last_submission_date": sub_date,
            "deregistration_date": dereg_date,
        })

    console.print(f"[green]Parsed {len(facilities):,} RMP facilities[/green]")
    return facilities


# --- Chemical inventory ---

def download_rmp_chemicals(force: bool = False) -> list[dict]:
    """Download and parse RMP chemical inventory data.

    Returns list of chemical inventory records with worst-case scenario data.
    """
    # Download chemicals
    chem_path = _download_csv(RMP_FILES["chemicals"], "chemicals", force=force)
    if not chem_path:
        return []
    time.sleep(2)

    # Download worst-case scenario data
    wc_path = _download_csv(RMP_FILES["toxic_worst_case"], "toxic_worst_case", force=force)
    time.sleep(2)

    # Download alternative release data
    alt_path = _download_csv(RMP_FILES["toxic_alt_release"], "toxic_alt_release", force=force)
    time.sleep(2)

    # Download chemical lookup for names/CAS
    lookup_path = _download_csv(RMP_FILES["chemicals_lookup"], "chemicals_lookup", force=force)
    time.sleep(2)

    # Download process data to map ProcessID → FacilityID
    proc_path = _download_csv(RMP_FILES["processes"], "processes", force=force)

    # Build ChemicalID → name/CAS mapping
    chem_names = {}
    if lookup_path:
        lookup_rows = _read_csv(lookup_path)
        for row in lookup_rows:
            cid = _clean_str(row.get("ChemicalID"))
            if cid:
                chem_names[cid] = {
                    "name": _clean_str(row.get("ChemicalName")),
                    "cas": _clean_str(row.get("CASNumber")),
                }

    # Build ProcessID → FacilityID mapping
    process_to_facility = {}
    if proc_path:
        proc_rows = _read_csv(proc_path)
        for row in proc_rows:
            pid = _clean_str(row.get("ProcessID"))
            fid = _clean_str(row.get("FacilityID"))
            if pid and fid:
                process_to_facility[pid] = fid

    # Build worst-case scenario lookup by ProcessChemicalID
    worst_cases: dict[str, dict] = {}
    if wc_path:
        wc_rows = _read_csv(wc_path)
        for row in wc_rows:
            pcid = _clean_str(row.get("ProcessChemicalID"))
            if pcid:
                dist = _safe_float(row.get("Distance2Endpoint"))
                scenario = _clean_str(row.get("Scenario"))
                # Keep the worst (largest distance)
                existing = worst_cases.get(pcid)
                if not existing or (dist and (existing.get("worst_case_distance_miles") or 0) < dist):
                    worst_cases[pcid] = {
                        "worst_case_distance_miles": dist,
                        "worst_case_scenario": scenario,
                    }

    # Build alt-release lookup by ProcessChemicalID
    alt_cases: dict[str, dict] = {}
    if alt_path:
        alt_rows = _read_csv(alt_path)
        for row in alt_rows:
            pcid = _clean_str(row.get("ProcessChemicalID"))
            if pcid:
                dist = _safe_float(row.get("Distance2Endpoint"))
                existing = alt_cases.get(pcid)
                if not existing or (dist and (existing.get("alt_case_distance_miles") or 0) < dist):
                    alt_cases[pcid] = {"alt_case_distance_miles": dist}

    # Parse chemical inventory
    chem_rows = _read_csv(chem_path)
    console.print(f"[dim]RMP chemical raw records: {len(chem_rows):,}[/dim]")

    chemicals = []
    seen_fac_chem = set()
    for row in chem_rows:
        pcid = _clean_str(row.get("ProcessChemicalID"))
        pid = _clean_str(row.get("ProcessID"))
        cid = _clean_str(row.get("ChemicalID"))
        fid = process_to_facility.get(pid, "") if pid else ""

        if not fid or not cid:
            continue

        chem_info = chem_names.get(cid, {})
        wc = worst_cases.get(pcid, {}) if pcid else {}
        alt = alt_cases.get(pcid, {}) if pcid else {}

        quantity = _safe_float(row.get("Quantity"))
        is_flam = 1 if str(row.get("CBI_Flag", "")).strip().upper() in ("Y", "YES") else 0

        # Deduplicate: keep one record per (facility, chemical) with highest quantity
        dedup_key = (fid, cid)
        chem_name = chem_info.get("name") or f"Chemical_{cid}"

        chemicals.append({
            "rmp_id": fid,
            "chemical_name": chem_name,
            "cas_number": chem_info.get("cas"),
            "quantity_lbs": quantity,
            "is_toxic": 1,  # All RMP chemicals are regulated hazardous substances
            "is_flammable": is_flam,
            "worst_case_scenario": wc.get("worst_case_scenario"),
            "worst_case_distance_miles": wc.get("worst_case_distance_miles"),
            "alt_case_distance_miles": alt.get("alt_case_distance_miles"),
        })

    console.print(f"[green]Parsed {len(chemicals):,} RMP chemical records[/green]")
    return chemicals


# --- Accident history ---

def download_rmp_accidents(force: bool = False) -> list[dict]:
    """Download and parse RMP 5-year accident history data.

    Returns list of accident records.
    """
    acc_path = _download_csv(RMP_FILES["accidents"], "accidents", force=force)
    if not acc_path:
        return []
    time.sleep(2)

    # Download accident chemical details
    acc_chem_path = _download_csv(RMP_FILES["accident_chemicals"], "accident_chemicals", force=force)

    # Build AccidentHistoryID → chemicals mapping
    acc_chemicals: dict[str, list[dict]] = {}
    if acc_chem_path:
        rows = _read_csv(acc_chem_path)
        for row in rows:
            ahid = _clean_str(row.get("AccidentHistoryID"))
            if ahid:
                acc_chemicals.setdefault(ahid, []).append({
                    "chemical_name": _clean_str(row.get("ChemicalName")),
                    "cas_number": _clean_str(row.get("CASNumber")),
                    "quantity_lbs": _safe_float(row.get("QuantityReleased")),
                })

    # Parse accident history
    acc_rows = _read_csv(acc_path)
    console.print(f"[dim]RMP accident raw records: {len(acc_rows):,}[/dim]")

    accidents = []
    for row in acc_rows:
        fid = _clean_str(row.get("FacilityID"))
        ahid = _clean_str(row.get("AccidentHistoryID"))
        if not fid:
            continue

        # Get chemicals for this accident
        chems = acc_chemicals.get(ahid, [])
        primary_chem = chems[0] if chems else {}

        # Determine release event type
        events = []
        if str(row.get("RE_Gas", "")).strip().upper() in ("Y", "1"):
            events.append("gas_release")
        if str(row.get("RE_Spill", "")).strip().upper() in ("Y", "1"):
            events.append("spill")
        if str(row.get("RE_Fire", "")).strip().upper() in ("Y", "1"):
            events.append("fire")
        if str(row.get("RE_Explosion", "")).strip().upper() in ("Y", "1"):
            events.append("explosion")
        if str(row.get("RE_ReactiveIncident", "")).strip().upper() in ("Y", "1"):
            events.append("reactive_incident")
        release_event = ",".join(events) if events else "unknown"

        accidents.append({
            "rmp_id": fid,
            "accident_date": _clean_str(row.get("AccidentDate")),
            "chemical_name": primary_chem.get("chemical_name"),
            "cas_number": primary_chem.get("cas_number"),
            "quantity_released_lbs": primary_chem.get("quantity_lbs") or _safe_float(row.get("QuantityReleased")),
            "release_duration_hours": _safe_float(row.get("ReleaseDuration")),
            "release_event": release_event,
            "deaths_workers": _safe_int(row.get("DeathsWorkers")) or 0,
            "deaths_public": _safe_int(row.get("DeathsPublicResponders")) or _safe_int(row.get("DeathsPublic")) or 0,
            "injuries_workers": _safe_int(row.get("InjuriesWorkers")) or 0,
            "injuries_public": _safe_int(row.get("InjuriesPublicResponders")) or _safe_int(row.get("InjuriesPublic")) or 0,
            "evacuations": _safe_int(row.get("Evacuated")) or 0,
            "property_damage_usd": _safe_float(row.get("OnsitePropertyDamage")),
        })

    console.print(f"[green]Parsed {len(accidents):,} RMP accident records[/green]")
    return accidents


# --- Main download function ---

def download_all_rmp(force: bool = False) -> dict:
    """Download all RMP data.

    Returns dict with keys: facilities, chemicals, accidents
    """
    console.print("[bold blue]Downloading EPA RMP chemical accident risk data...[/bold blue]")

    facilities = download_rmp_facilities(force=force)
    time.sleep(2)

    chemicals = download_rmp_chemicals(force=force)
    time.sleep(2)

    accidents = download_rmp_accidents(force=force)

    console.print(f"\n[bold green]RMP download complete:[/bold green]")
    console.print(f"  Facilities: {len(facilities):,}")
    console.print(f"  Chemical inventory: {len(chemicals):,}")
    console.print(f"  Accidents: {len(accidents):,}")

    return {
        "facilities": facilities,
        "chemicals": chemicals,
        "accidents": accidents,
    }
