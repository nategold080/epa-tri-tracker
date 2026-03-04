"""Cross-link ECHO enforcement data to TRI facilities via FRS registry IDs.

Flow:
1. tri_frs_links table maps TRI facility IDs to FRS registry IDs
2. enforcement_actions, facility_inspections use registry_id as key
3. This linker builds aggregate enforcement profiles per TRI facility
"""

from __future__ import annotations

from typing import Any, Optional

from rich.console import Console

console = Console()


def get_facility_enforcement_summary(
    tri_facility_id: str,
    conn=None,
) -> dict[str, Any]:
    """Get enforcement summary for a TRI facility.

    Returns dict with: enforcement_count, inspection_count, total_penalties,
    violation_count, compliance_programs, has_enforcement.
    """
    if conn is None:
        from src.storage.database import get_connection
        conn = get_connection()

    # Get registry IDs for this facility
    reg_ids = conn.execute(
        "SELECT registry_id FROM tri_frs_links WHERE tri_facility_id = ?",
        (tri_facility_id,),
    ).fetchall()

    if not reg_ids:
        return {
            "enforcement_count": 0,
            "inspection_count": 0,
            "total_penalties": 0.0,
            "violation_count": 0,
            "compliance_programs": [],
            "has_enforcement": False,
        }

    reg_id_list = [r["registry_id"] for r in reg_ids]
    placeholders = ",".join("?" * len(reg_id_list))

    # Enforcement actions
    enf_row = conn.execute(f"""
        SELECT COUNT(*) as cnt, COALESCE(SUM(penalty_amount), 0) as total_penalty
        FROM enforcement_actions WHERE registry_id IN ({placeholders})
    """, reg_id_list).fetchone()

    # Inspections
    insp_row = conn.execute(f"""
        SELECT COUNT(*) as cnt,
               SUM(CASE WHEN found_violation = 1 THEN 1 ELSE 0 END) as violations
        FROM facility_inspections WHERE registry_id IN ({placeholders})
    """, reg_id_list).fetchone()

    # Compliance status
    comp_rows = conn.execute(f"""
        SELECT program, status, quarters_in_nc
        FROM compliance_status WHERE registry_id IN ({placeholders})
    """, reg_id_list).fetchall()

    compliance_programs = [
        {"program": r["program"], "status": r["status"], "quarters_in_nc": r["quarters_in_nc"]}
        for r in comp_rows
    ]

    return {
        "enforcement_count": enf_row["cnt"],
        "inspection_count": insp_row["cnt"],
        "total_penalties": enf_row["total_penalty"],
        "violation_count": insp_row["violations"] or 0,
        "compliance_programs": compliance_programs,
        "has_enforcement": enf_row["cnt"] > 0 or insp_row["cnt"] > 0,
    }


def compute_facility_risk_score(
    facility: dict,
    enforcement_summary: dict,
    release_stats: dict,
    ej_data: Optional[dict] = None,
    trend_data: Optional[dict] = None,
) -> float:
    """Compute composite risk score for a facility.

    Components:
    - Release volume (25%) — total lbs, weighted by toxicity
    - Carcinogen concentration (20%) — proportion of carcinogenic releases
    - Enforcement history (20%) — violations, penalties, non-compliance
    - Community vulnerability (20%) — EJ indicators, demographics
    - Trend direction (15%) — increasing vs decreasing releases

    Returns score 0.0-1.0 where LOWER = HIGHER RISK.
    Risk tiers: LOW (>=0.8), MEDIUM (>=0.5), HIGH (>=0.3), CRITICAL (<0.3)
    """
    score = 0.0

    # Release volume component (25%)
    total_lbs = release_stats.get("total_releases_lbs", 0) or 0
    if total_lbs == 0:
        release_score = 1.0
    elif total_lbs < 1000:
        release_score = 0.9
    elif total_lbs < 10000:
        release_score = 0.7
    elif total_lbs < 100000:
        release_score = 0.5
    elif total_lbs < 1000000:
        release_score = 0.3
    else:
        release_score = 0.1
    score += release_score * 0.25

    # Carcinogen concentration (20%)
    carc_lbs = release_stats.get("carcinogen_lbs", 0) or 0
    if total_lbs > 0:
        carc_pct = carc_lbs / total_lbs
    else:
        carc_pct = 0
    if carc_pct == 0:
        carc_score = 1.0
    elif carc_pct < 0.01:
        carc_score = 0.8
    elif carc_pct < 0.1:
        carc_score = 0.5
    elif carc_pct < 0.5:
        carc_score = 0.3
    else:
        carc_score = 0.1
    score += carc_score * 0.20

    # Enforcement history (20%)
    enf_count = enforcement_summary.get("enforcement_count", 0)
    viol_count = enforcement_summary.get("violation_count", 0)
    penalties = enforcement_summary.get("total_penalties", 0) or 0
    enf_issues = enf_count + viol_count
    if enf_issues == 0 and penalties == 0:
        enf_score = 1.0
    elif enf_issues <= 2 and penalties < 10000:
        enf_score = 0.7
    elif enf_issues <= 5 and penalties < 100000:
        enf_score = 0.5
    elif enf_issues <= 10:
        enf_score = 0.3
    else:
        enf_score = 0.1
    score += enf_score * 0.20

    # Community vulnerability (20%)
    if ej_data and ej_data.get("ej_index_pctl") is not None:
        ej_pctl = ej_data["ej_index_pctl"]
        # Higher EJ percentile = more vulnerable = lower score
        comm_score = max(0.0, 1.0 - (ej_pctl / 100.0))
    else:
        comm_score = 0.5  # Unknown = medium risk
    score += comm_score * 0.20

    # Trend direction (15%)
    if trend_data and trend_data.get("trend_pct") is not None:
        trend_pct = trend_data["trend_pct"]
        if trend_pct <= -20:
            trend_score = 1.0  # Decreasing significantly
        elif trend_pct <= 0:
            trend_score = 0.8
        elif trend_pct <= 20:
            trend_score = 0.5
        else:
            trend_score = 0.2  # Increasing significantly
    else:
        trend_score = 0.5  # No trend data
    score += trend_score * 0.15

    return round(min(max(score, 0.0), 1.0), 3)


def get_risk_tier(score: float) -> str:
    """Convert risk score to tier label."""
    if score >= 0.8:
        return "LOW"
    elif score >= 0.5:
        return "MEDIUM"
    elif score >= 0.3:
        return "HIGH"
    else:
        return "CRITICAL"


def link_enforcement_to_facilities(conn=None) -> dict:
    """Run enforcement linkage for all facilities with FRS links.

    Returns summary stats dict.
    """
    if conn is None:
        from src.storage.database import get_connection
        conn = get_connection()

    # Count linked facilities
    row = conn.execute("""
        SELECT COUNT(DISTINCT tfl.tri_facility_id) as linked
        FROM tri_frs_links tfl
        JOIN enforcement_actions ea ON tfl.registry_id = ea.registry_id
    """).fetchone()
    facilities_with_enforcement = row["linked"]

    row = conn.execute("""
        SELECT COUNT(DISTINCT tfl.tri_facility_id) as linked
        FROM tri_frs_links tfl
        JOIN facility_inspections fi ON tfl.registry_id = fi.registry_id
    """).fetchone()
    facilities_with_inspections = row["linked"]

    row = conn.execute("""
        SELECT COUNT(DISTINCT tfl.tri_facility_id) as linked
        FROM tri_frs_links tfl
    """).fetchone()
    total_linked = row["linked"]

    console.print(f"\n[bold]Enforcement Linkage Summary:[/bold]")
    console.print(f"  TRI facilities linked to FRS: {total_linked:,}")
    console.print(f"  Facilities with enforcement actions: {facilities_with_enforcement:,}")
    console.print(f"  Facilities with inspections: {facilities_with_inspections:,}")

    return {
        "total_linked": total_linked,
        "facilities_with_enforcement": facilities_with_enforcement,
        "facilities_with_inspections": facilities_with_inspections,
    }
