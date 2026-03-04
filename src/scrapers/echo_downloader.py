"""EPA ECHO enforcement, inspection, and compliance data downloader.

Downloads ECHO bulk data files from EPA:
- FRS (Facility Registry Service) — maps TRI_FACILITY_ID to REGISTRY_ID
- CASE_ENFORCEMENTS + CASE_FACILITIES + CASE_PENALTIES — enforcement actions
- NPDES_INSPECTIONS — Clean Water Act inspections
- CAA (ICIS-Air) — Clean Air Act inspections/compliance
- RCRA_EVALUATIONS — Hazardous waste inspections

Data structure: ECHO ZIPs contain multiple CSVs. Enforcement data is normalized
across CASE_ENFORCEMENTS, CASE_FACILITIES, CASE_PENALTIES, and CASE_PROGRAMS.
REGISTRY_ID is on CASE_FACILITIES (not CASE_ENFORCEMENTS).
"""

from __future__ import annotations

import csv
import io
import os
import time
import zipfile
from pathlib import Path
from typing import Optional

import httpx
from rich.console import Console

console = Console()

PROJECT_ROOT = Path(__file__).parent.parent.parent
CACHE_DIR = PROJECT_ROOT / "data" / "cache" / "echo"

# ECHO bulk download URLs
ECHO_URLS = {
    "frs": "https://echo.epa.gov/files/echodownloads/frs_downloads.zip",
    "case_enforcements": "https://echo.epa.gov/files/echodownloads/case_downloads.zip",
    "npdes_inspections": "https://echo.epa.gov/files/echodownloads/npdes_downloads.zip",
    "caa_inspections": "https://echo.epa.gov/files/echodownloads/ICIS-AIR_downloads.zip",
    "rcra_evaluations": "https://echo.epa.gov/files/echodownloads/rcra_downloads.zip",
}

USER_AGENT = "EPA-TRI-Tracker/0.2 (nathanmauricegoldberg@gmail.com)"


def _download_zip(url: str, name: str, force: bool = False) -> Optional[Path]:
    """Download a ZIP file from EPA ECHO, with caching."""
    zip_path = CACHE_DIR / f"{name}.zip"
    if zip_path.exists() and not force:
        # Verify it's a valid zip
        try:
            with zipfile.ZipFile(zip_path, "r") as zf:
                zf.namelist()
            console.print(f"[dim]Using cached {name}.zip[/dim]")
            return zip_path
        except (zipfile.BadZipFile, Exception):
            console.print(f"[yellow]Cached {name}.zip is corrupt, re-downloading[/yellow]")
            zip_path.unlink()

    zip_path.parent.mkdir(parents=True, exist_ok=True)
    console.print(f"[dim]Downloading {name} from {url}...[/dim]")

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
                with open(zip_path, "wb") as f:
                    for chunk in resp.iter_bytes(chunk_size=1024 * 1024):
                        f.write(chunk)
        console.print(f"[green]Downloaded {name}: {zip_path.stat().st_size / 1024 / 1024:.1f} MB[/green]")
        return zip_path
    except Exception as e:
        console.print(f"[red]Error downloading {name}: {e}[/red]")
        if zip_path.exists():
            zip_path.unlink()
        return None


def _read_csv_from_zip(zip_path: Path, csv_pattern: str, max_rows: int = 0) -> list[dict]:
    """Read a CSV file from within a ZIP archive."""
    records = []
    with zipfile.ZipFile(zip_path, "r") as zf:
        matching = [n for n in zf.namelist() if csv_pattern.lower() in n.lower() and n.endswith(".csv")]
        if not matching:
            console.print(f"[yellow]No CSV matching '{csv_pattern}' in {zip_path.name}[/yellow]")
            console.print(f"[dim]Available: {zf.namelist()[:10]}[/dim]")
            return []

        csv_name = matching[0]
        console.print(f"[dim]Reading {csv_name} from {zip_path.name}...[/dim]")

        with zf.open(csv_name) as f:
            text = io.TextIOWrapper(f, encoding="utf-8", errors="replace")
            content = text.read()
            if content.startswith("\ufeff"):
                content = content[1:]

            reader = csv.DictReader(io.StringIO(content))
            for i, row in enumerate(reader):
                if max_rows and i >= max_rows:
                    break
                records.append(row)

    return records


def _safe_float(val: str) -> Optional[float]:
    """Convert string to float safely."""
    if not val or val.strip() in ("", ".", "N/A", "NA"):
        return None
    try:
        return float(val.replace(",", "").replace("$", ""))
    except (ValueError, TypeError):
        return None


def _safe_int(val: str) -> Optional[int]:
    """Convert string to int safely."""
    if not val or val.strip() in ("", ".", "N/A", "NA"):
        return None
    try:
        return int(float(val.replace(",", "")))
    except (ValueError, TypeError):
        return None


# Module-level cache for program ID → FRS REGISTRY_ID mappings
# Used to resolve RCRA ID_NUMBER and CAA PROGRAM_ID to REGISTRY_ID
_program_to_registry: dict[str, str] = {}


# --- FRS: Facility Registry Service ---

def download_frs_links(force: bool = False) -> list[dict]:
    """Download FRS data and extract TRI→FRS registry ID mappings.

    Also builds _program_to_registry mapping for RCRA/CAA/NPDES program ID resolution.
    Returns list of dicts with: tri_facility_id, registry_id, program_system_acronym
    """
    global _program_to_registry

    zip_path = _download_zip(ECHO_URLS["frs"], "frs", force=force)
    if not zip_path:
        return []

    # FRS program links table maps program IDs to registry IDs
    # EPA renamed the file: was FRS_PROGRAM_INTEREST, now FRS_PROGRAM_LINKS
    rows = _read_csv_from_zip(zip_path, "FRS_PROGRAM_LINKS")
    if not rows:
        rows = _read_csv_from_zip(zip_path, "FRS_PROGRAM_INTEREST")
    if not rows:
        rows = _read_csv_from_zip(zip_path, "PROGRAM")

    console.print(f"[dim]FRS raw records: {len(rows):,}[/dim]")

    links = []
    _program_to_registry = {}

    for row in rows:
        pgm = (row.get("PGM_SYS_ACRNM", "") or row.get("PROGRAM_SYSTEM_ACRONYM", "")).strip()
        pgm_id = (row.get("PGM_SYS_ID", "") or row.get("PROGRAM_SYSTEM_ID", "")).strip()
        reg_id = (row.get("REGISTRY_ID", "") or row.get("FRS_REGISTRY_ID", "")).strip()

        if not pgm_id or not reg_id:
            continue

        # Build program-to-registry mapping for all programs (RCRA, CAA, NPDES, etc.)
        _program_to_registry[pgm_id] = reg_id

        # Only return TRI links for the tri_frs_links table
        if pgm == "TRIS":
            links.append({
                "tri_facility_id": pgm_id,
                "registry_id": reg_id,
                "program_system_acronym": "TRIS",
            })

    console.print(f"[green]Extracted {len(links):,} TRI→FRS linkages[/green]")
    console.print(f"[dim]Built {len(_program_to_registry):,} program→registry mappings (for RCRA/CAA resolution)[/dim]")
    return links


# --- Enforcement Actions ---

def download_enforcement_actions(force: bool = False) -> list[dict]:
    """Download ECHO enforcement case data.

    ECHO case data is normalized across multiple CSVs within case_downloads.zip:
    - CASE_ENFORCEMENTS.csv — case metadata (no registry_id)
    - CASE_FACILITIES.csv — links cases to facilities via REGISTRY_ID
    - CASE_PENALTIES.csv — penalty amounts per case
    - CASE_PROGRAMS.csv — statute/program codes per case
    """
    zip_path = _download_zip(ECHO_URLS["case_enforcements"], "case_enforcements", force=force)
    if not zip_path:
        return []

    # Read case metadata
    case_rows = _read_csv_from_zip(zip_path, "CASE_ENFORCEMENTS")
    if not case_rows:
        case_rows = _read_csv_from_zip(zip_path, "CASE_ENFORCEMENT")
    console.print(f"[dim]Enforcement case records: {len(case_rows):,}[/dim]")

    # Build case lookup by ACTIVITY_ID
    cases_by_activity = {}
    for row in case_rows:
        aid = (row.get("ACTIVITY_ID", "") or "").strip()
        if aid:
            cases_by_activity[aid] = row

    # Read facility linkage (has REGISTRY_ID)
    fac_rows = _read_csv_from_zip(zip_path, "CASE_FACILITIES")
    console.print(f"[dim]Case-facility linkages: {len(fac_rows):,}[/dim]")

    # Read penalties
    pen_rows = _read_csv_from_zip(zip_path, "CASE_PENALTIES")
    console.print(f"[dim]Case penalty records: {len(pen_rows):,}[/dim]")
    penalties_by_activity = {}
    for row in pen_rows:
        aid = (row.get("ACTIVITY_ID", "") or "").strip()
        if aid:
            penalties_by_activity[aid] = row

    # Read program codes for enforcement type detection
    prog_rows = _read_csv_from_zip(zip_path, "CASE_PROGRAMS")
    console.print(f"[dim]Case program records: {len(prog_rows):,}[/dim]")
    programs_by_activity: dict[str, list[str]] = {}
    for row in prog_rows:
        aid = (row.get("ACTIVITY_ID", "") or "").strip()
        code = (row.get("PROGRAM_CODE", "") or row.get("PROGRAM_DESC", "")).strip()
        if aid and code:
            programs_by_activity.setdefault(aid, []).append(code)

    # Join: for each facility linkage, create an enforcement record
    records = []
    seen = set()
    for fac_row in fac_rows:
        reg_id = (fac_row.get("REGISTRY_ID", "") or "").strip()
        aid = (fac_row.get("ACTIVITY_ID", "") or "").strip()
        case_num = (fac_row.get("CASE_NUMBER", "") or "").strip()
        if not reg_id or not (aid or case_num):
            continue

        # Deduplicate by (case_number, registry_id)
        dedup_key = (case_num or aid, reg_id)
        if dedup_key in seen:
            continue
        seen.add(dedup_key)

        case = cases_by_activity.get(aid, {})
        penalty = penalties_by_activity.get(aid, {})
        programs = programs_by_activity.get(aid, [])

        # Detect enforcement type from programs
        enf_type = _detect_enforcement_type_from_programs(programs)

        # Calculate penalty
        fed_pen = _safe_float(penalty.get("FED_PENALTY", ""))
        st_pen = _safe_float(penalty.get("ST_LCL_PENALTY", ""))
        total_pen_from_case = _safe_float(case.get("TOTAL_PENALTY_ASSESSED_AMT", ""))
        comp_cost = _safe_float(penalty.get("COMPLIANCE_ACTION_COST", ""))

        if fed_pen is not None or st_pen is not None:
            total_penalty = (fed_pen or 0) + (st_pen or 0)
        elif total_pen_from_case is not None:
            total_penalty = total_pen_from_case
        else:
            total_penalty = None

        records.append({
            "case_number": case_num or f"ACT_{aid}",
            "registry_id": reg_id,
            "case_name": (case.get("CASE_NAME", "") or case.get("ACTIVITY_NAME", "")).strip() or None,
            "activity_type": (case.get("ACTIVITY_TYPE_DESC", "") or case.get("ACTIVITY_TYPE_CODE", "")).strip() or None,
            "enforcement_type": enf_type,
            "lead_agency": (case.get("LEAD", "") or case.get("ENF_AGENCY", "")).strip() or None,
            "case_status": (case.get("ACTIVITY_STATUS_DESC", "") or case.get("ACTIVITY_STATUS_CODE", "")).strip() or None,
            "settlement_date": (case.get("ACTIVITY_STATUS_DATE", "") or case.get("CASE_STATUS_DATE", "")).strip() or None,
            "penalty_amount": total_penalty,
            "fed_penalty_assessed": fed_pen,
            "state_local_penalty": st_pen,
            "compliance_action_cost": comp_cost,
            "enforcement_outcome": (case.get("ENF_OUTCOME_DESC", "") or "").strip() or None,
        })

    console.print(f"[green]Parsed {len(records):,} enforcement actions with facility linkage[/green]")
    return records


def _detect_enforcement_type_from_programs(programs: list[str]) -> Optional[str]:
    """Detect enforcement program type from CASE_PROGRAMS codes."""
    programs_str = " ".join(programs).upper()
    if "CWA" in programs_str or "NPDES" in programs_str:
        return "CWA"
    if "CAA" in programs_str or "CLEAN AIR" in programs_str:
        return "CAA"
    if "RCRA" in programs_str or "HAZARDOUS WASTE" in programs_str:
        return "RCRA"
    if "TSCA" in programs_str:
        return "TSCA"
    if "FIFRA" in programs_str:
        return "FIFRA"
    if "CERCLA" in programs_str or "SUPERFUND" in programs_str:
        return "CERCLA"
    if "EPCRA" in programs_str:
        return "EPCRA"
    return programs[0][:10].strip() if programs else None


def _detect_enforcement_type(row: dict) -> Optional[str]:
    """Detect enforcement program type (CAA, CWA, RCRA) from row data."""
    statutes = (row.get("ENF_STATUTE", "") or row.get("STATUTE_CODE", "") or row.get("PROGRAM_CODE", "")).upper()
    if "CAA" in statutes or "CLEAN AIR" in statutes:
        return "CAA"
    if "CWA" in statutes or "CLEAN WATER" in statutes or "NPDES" in statutes:
        return "CWA"
    if "RCRA" in statutes or "HAZARDOUS WASTE" in statutes:
        return "RCRA"
    if "TSCA" in statutes:
        return "TSCA"
    if "FIFRA" in statutes:
        return "FIFRA"
    if "CERCLA" in statutes or "SUPERFUND" in statutes:
        return "CERCLA"
    if "EPCRA" in statutes:
        return "EPCRA"
    return statutes.strip() or None


# --- Inspections ---

def download_inspections(force: bool = False) -> list[dict]:
    """Download inspection records from CWA, CAA, and RCRA programs."""
    all_inspections = []

    # CWA (NPDES) inspections — NPDES_INSPECTIONS.csv has REGISTRY_ID directly
    zip_path = _download_zip(ECHO_URLS["npdes_inspections"], "npdes_inspections", force=force)
    if zip_path:
        rows = _read_csv_from_zip(zip_path, "NPDES_INSPECTIONS")
        if not rows:
            rows = _read_csv_from_zip(zip_path, "NPDES_INSPECTION")
        console.print(f"[dim]CWA inspection raw records: {len(rows):,}[/dim]")
        for row in rows:
            insp = _parse_inspection(row, "CWA")
            if insp:
                all_inspections.append(insp)
        time.sleep(2)

    # CAA inspections — look for ICIS compliance evaluations
    zip_path = _download_zip(ECHO_URLS["caa_inspections"], "caa_inspections", force=force)
    if zip_path:
        # CAA zip may contain ICIS_FEC_EPA_INSPECTIONS or similar
        rows = _read_csv_from_zip(zip_path, "ICIS_FEC_EPA_INSPECTION")
        if not rows:
            rows = _read_csv_from_zip(zip_path, "ICIS-AIR_FCES_PCES")
        if not rows:
            rows = _read_csv_from_zip(zip_path, "CAA_EVALUATIONS")
        if not rows:
            rows = _read_csv_from_zip(zip_path, "CAA")
        console.print(f"[dim]CAA inspection raw records: {len(rows):,}[/dim]")
        for row in rows:
            insp = _parse_inspection(row, "CAA")
            if insp:
                all_inspections.append(insp)
        time.sleep(2)

    # RCRA evaluations
    zip_path = _download_zip(ECHO_URLS["rcra_evaluations"], "rcra_evaluations", force=force)
    if zip_path:
        rows = _read_csv_from_zip(zip_path, "RCRA_EVALUATIONS")
        if not rows:
            rows = _read_csv_from_zip(zip_path, "RCRA_EVALUATION")
        console.print(f"[dim]RCRA evaluation raw records: {len(rows):,}[/dim]")
        for row in rows:
            insp = _parse_inspection(row, "RCRA")
            if insp:
                all_inspections.append(insp)

    console.print(f"[green]Total inspections parsed: {len(all_inspections):,}[/green]")
    return all_inspections


def _parse_inspection(row: dict, program: str) -> Optional[dict]:
    """Parse an inspection row from any program.

    Handles different column conventions:
    - NPDES (CWA): has REGISTRY_ID directly, ACTIVITY_ID, ACTUAL_BEGIN_DATE
    - RCRA: has ID_NUMBER (handler ID, not registry), EVALUATION_IDENTIFIER, EVALUATION_START_DATE
    - CAA (ICIS-Air): has REGISTRY_ID or PGM_SYS_ID, ACTIVITY_ID
    """
    # Resolve REGISTRY_ID — RCRA uses ID_NUMBER which needs FRS mapping
    reg_id = (row.get("REGISTRY_ID", "") or row.get("FRS_REGISTRY_ID", "")).strip()
    if not reg_id:
        # For RCRA: ID_NUMBER is a handler ID, resolve via FRS
        handler_id = (row.get("ID_NUMBER", "") or row.get("PGM_SYS_ID", "")).strip()
        if handler_id and handler_id in _program_to_registry:
            reg_id = _program_to_registry[handler_id]
        elif handler_id:
            reg_id = handler_id  # Use handler ID as fallback
        else:
            return None

    if not reg_id:
        return None

    # Generate a unique inspection ID
    activity_id = (
        row.get("ACTIVITY_ID", "")
        or row.get("EVALUATION_IDENTIFIER", "")
    ).strip()
    start_date = (
        row.get("ACTUAL_BEGIN_DATE", "")
        or row.get("EVALUATION_START_DATE", "")
        or row.get("ACTUAL_DATE", "")
        or ""
    ).strip()

    if activity_id:
        insp_id = f"{program}_{reg_id}_{activity_id}"
    elif start_date:
        insp_id = f"{program}_{reg_id}_{start_date}"
    else:
        return None

    # Detect violations from multiple possible columns
    found_viol = 0
    # RCRA uses FOUND_VIOLATION (Y/N), NPDES uses ACTIVITY_OUTCOME_DESC
    viol_flag = (
        row.get("FOUND_VIOLATION", "")
        or row.get("FOUND_VIOLATION_FLAG", "")
        or row.get("SNC_FLAG", "")
    ).strip().upper()
    if viol_flag in ("Y", "YES", "1", "TRUE"):
        found_viol = 1
    # NPDES uses ACTIVITY_OUTCOME_DESC to indicate violations
    outcome = (row.get("ACTIVITY_OUTCOME_DESC", "") or "").upper()
    if "VIOLATION" in outcome or "NON-COMPLIANCE" in outcome or "NOT IMMEDIATELY CORRECTED" in outcome:
        found_viol = 1

    return {
        "inspection_id": insp_id,
        "registry_id": reg_id,
        "program": program,
        "inspection_type": (
            row.get("COMP_MONITOR_TYPE_DESC", "")
            or row.get("EVALUATION_DESC", "")
            or row.get("EVALUATION_TYPE", "")
            or row.get("COMP_MONITOR_TYPE_CODE", "")
            or ""
        ).strip() or None,
        "start_date": start_date or None,
        "end_date": (
            row.get("ACTUAL_END_DATE", "")
            or row.get("EVALUATION_END_DATE", "")
            or ""
        ).strip() or None,
        "lead_agency": (
            row.get("STATE_EPA_FLAG", "")
            or row.get("EVALUATION_AGENCY", "")
            or row.get("LEAD_AGENCY", "")
        ).strip() or None,
        "found_violation": found_viol,
    }


# --- Compliance Status ---

def extract_compliance_status(enforcement_records: list[dict], inspection_records: list[dict]) -> list[dict]:
    """Derive compliance status per facility-program from enforcement and inspection data.

    A facility is in 'Significant Non-Compliance' if it has recent violations or enforcement actions.
    """
    facility_programs: dict[tuple[str, str], dict] = {}

    for enf in enforcement_records:
        reg_id = enf.get("registry_id")
        program = enf.get("enforcement_type") or "UNKNOWN"
        if not reg_id:
            continue
        key = (reg_id, program)
        if key not in facility_programs:
            facility_programs[key] = {"violations": 0, "latest_date": None}
        facility_programs[key]["violations"] += 1
        date = enf.get("settlement_date")
        if date and (not facility_programs[key]["latest_date"] or date > facility_programs[key]["latest_date"]):
            facility_programs[key]["latest_date"] = date

    for insp in inspection_records:
        reg_id = insp.get("registry_id")
        program = insp.get("program") or "UNKNOWN"
        if not reg_id or not insp.get("found_violation"):
            continue
        key = (reg_id, program)
        if key not in facility_programs:
            facility_programs[key] = {"violations": 0, "latest_date": None}
        facility_programs[key]["violations"] += 1
        date = insp.get("start_date")
        if date and (not facility_programs[key]["latest_date"] or date > facility_programs[key]["latest_date"]):
            facility_programs[key]["latest_date"] = date

    records = []
    for (reg_id, program), info in facility_programs.items():
        status = "Significant Non-Compliance" if info["violations"] >= 3 else (
            "Violation" if info["violations"] > 0 else "In Compliance"
        )
        records.append({
            "registry_id": reg_id,
            "program": program,
            "status": status,
            "status_date": info["latest_date"],
            "quarters_in_nc": info["violations"],
        })

    return records


def download_all_echo(force: bool = False) -> dict:
    """Download all ECHO data. Returns dict with keys: frs_links, enforcement, inspections, compliance."""
    console.print("[bold blue]Downloading EPA ECHO enforcement & compliance data...[/bold blue]")

    frs_links = download_frs_links(force=force)
    time.sleep(2)

    enforcement = download_enforcement_actions(force=force)
    time.sleep(2)

    inspections = download_inspections(force=force)

    compliance = extract_compliance_status(enforcement, inspections)

    console.print(f"\n[bold green]ECHO download complete:[/bold green]")
    console.print(f"  FRS links: {len(frs_links):,}")
    console.print(f"  Enforcement actions: {len(enforcement):,}")
    console.print(f"  Inspections: {len(inspections):,}")
    console.print(f"  Compliance records: {len(compliance):,}")

    return {
        "frs_links": frs_links,
        "enforcement": enforcement,
        "inspections": inspections,
        "compliance": compliance,
    }
