"""Quality scoring for TRI facility records.

Each facility scored 0.0 to 1.0 based on data completeness
and cross-linkage with health, demographic, enforcement, and EJ data.

Updated Quality Score Weights (11 components):
  - has_facility_name: 0.08
  - has_location_data: 0.08
  - has_chemical_releases: 0.12
  - has_release_quantities: 0.12
  - has_health_data_linked: 0.12
  - has_demographic_data: 0.08
  - has_ej_indicators: 0.10
  - has_industry_classification: 0.08
  - has_enforcement_data: 0.12
  - has_source_url: 0.05
  - has_historical_trend: 0.05
"""

from __future__ import annotations

from typing import Any

from rich.console import Console

console = Console()

WEIGHTS = {
    "has_facility_name": 0.08,
    "has_location_data": 0.08,
    "has_chemical_releases": 0.12,
    "has_release_quantities": 0.12,
    "has_health_data_linked": 0.12,
    "has_demographic_data": 0.08,
    "has_ej_indicators": 0.10,
    "has_industry_classification": 0.08,
    "has_enforcement_data": 0.12,
    "has_source_url": 0.05,
    "has_historical_trend": 0.05,
}


def score_facility(
    facility: dict[str, Any],
    has_health_data: bool = False,
    has_demographics: bool = False,
    has_ej_data: bool = False,
    has_enforcement: bool = False,
    has_historical: bool = False,
    release_count: int = 0,
    total_releases_lbs: float = 0.0,
) -> dict:
    """Compute a quality score for a TRI facility record."""
    components = {}
    issues = []

    # Facility name
    if facility.get("canonical_name"):
        components["has_facility_name"] = 1.0
    elif facility.get("facility_name"):
        components["has_facility_name"] = 0.8
    else:
        components["has_facility_name"] = 0.0
        issues.append("Missing facility name")

    # Location data (lat/lon + FIPS)
    has_coords = (
        facility.get("latitude") is not None
        and facility.get("longitude") is not None
    )
    has_fips = bool(facility.get("fips_county"))
    if has_coords and has_fips:
        components["has_location_data"] = 1.0
    elif has_coords or has_fips:
        components["has_location_data"] = 0.6
    else:
        components["has_location_data"] = 0.0
        issues.append("Missing location data")

    # Chemical releases
    if release_count > 5:
        components["has_chemical_releases"] = 1.0
    elif release_count > 0:
        components["has_chemical_releases"] = 0.7
    else:
        components["has_chemical_releases"] = 0.0
        issues.append("No chemical release data")

    # Release quantities
    if total_releases_lbs > 0:
        components["has_release_quantities"] = 1.0
    elif release_count > 0:
        components["has_release_quantities"] = 0.5
    else:
        components["has_release_quantities"] = 0.0
        issues.append("No release quantity data")

    # Health data linked
    if has_health_data:
        components["has_health_data_linked"] = 1.0
    else:
        components["has_health_data_linked"] = 0.0

    # Demographic data
    if has_demographics:
        components["has_demographic_data"] = 1.0
    else:
        components["has_demographic_data"] = 0.0

    # EJ indicators
    if has_ej_data:
        components["has_ej_indicators"] = 1.0
    else:
        components["has_ej_indicators"] = 0.0

    # Industry classification
    if facility.get("industry_sector") and facility["industry_sector"] != "unknown":
        components["has_industry_classification"] = 1.0
    elif facility.get("sic_code"):
        components["has_industry_classification"] = 0.7
    else:
        components["has_industry_classification"] = 0.0
        issues.append("Missing industry classification")

    # Enforcement data (NEW)
    if has_enforcement:
        components["has_enforcement_data"] = 1.0
    else:
        components["has_enforcement_data"] = 0.0

    # Source URL (TRI data always has a known source)
    components["has_source_url"] = 1.0

    # Historical trend data (NEW)
    if has_historical:
        components["has_historical_trend"] = 1.0
    else:
        components["has_historical_trend"] = 0.0

    quality_score = sum(components[key] * WEIGHTS[key] for key in WEIGHTS)
    quality_score = round(min(max(quality_score, 0.0), 1.0), 3)

    return {
        "quality_score": quality_score,
        "component_scores": components,
        "issues": issues,
    }


def validate_facility(facility: dict[str, Any]) -> list[str]:
    """Run validation checks on a facility record."""
    errors = []

    if not facility.get("tri_facility_id"):
        errors.append("Missing tri_facility_id")
    if not facility.get("facility_name"):
        errors.append("Missing facility_name")
    if not facility.get("state"):
        errors.append("Missing state")

    state = facility.get("state", "")
    if state and len(state) != 2:
        errors.append(f"State code '{state}' should be 2 characters")

    lat = facility.get("latitude")
    lon = facility.get("longitude")
    if lat is not None and (lat < 17 or lat > 72):
        errors.append(f"Latitude {lat} outside US range")
    if lon is not None and (lon < -180 or lon > -60):
        errors.append(f"Longitude {lon} outside US range")

    return errors


def validate_release(release: dict[str, Any]) -> list[str]:
    """Run validation checks on a release record."""
    errors = []

    if not release.get("tri_facility_id"):
        errors.append("Missing tri_facility_id")
    if not release.get("chemical_name"):
        errors.append("Missing chemical_name")

    year = release.get("reporting_year")
    if year is not None and (year < 1987 or year > 2030):
        errors.append(f"Reporting year {year} outside valid range")

    total = release.get("total_releases_lbs")
    if total is not None and total < 0:
        errors.append(f"Negative total releases: {total}")

    return errors


def validate_all(conn=None) -> None:
    """Run quality validation on all facilities in the database."""
    from src.storage.database import (
        get_all_facilities,
        get_connection,
        update_facility_quality_scores,
    )

    should_close = conn is None
    if conn is None:
        conn = get_connection()

    facilities = get_all_facilities(limit=100000, conn=conn)
    console.print(f"[dim]Validating {len(facilities)} TRI facilities...[/dim]")

    scores = {}
    total_issues = 0
    error_count = 0

    for fac in facilities:
        fid = fac.get("tri_facility_id", "")
        errors = validate_facility(fac)
        if errors:
            error_count += 1

        # Check cross-linkage
        fips = fac.get("fips_county")
        has_health = False
        has_demo = False
        has_ej = False

        if fips:
            row = conn.execute(
                "SELECT COUNT(*) as cnt FROM county_health WHERE fips_county = ?", (fips,)
            ).fetchone()
            has_health = row["cnt"] > 0

            row = conn.execute(
                "SELECT COUNT(*) as cnt FROM county_demographics WHERE fips_county = ?", (fips,)
            ).fetchone()
            has_demo = row["cnt"] > 0

            row = conn.execute(
                "SELECT COUNT(*) as cnt FROM ej_indicators WHERE fips_county = ?", (fips,)
            ).fetchone()
            has_ej = row["cnt"] > 0

        # Check enforcement linkage — require actual enforcement/inspection data, not just FRS link
        has_enforcement = False
        row = conn.execute("""
            SELECT COUNT(*) as cnt FROM tri_frs_links l
            WHERE l.tri_facility_id = ?
            AND (
                EXISTS (SELECT 1 FROM enforcement_actions e WHERE e.registry_id = l.registry_id)
                OR EXISTS (SELECT 1 FROM facility_inspections i WHERE i.registry_id = l.registry_id)
            )
        """, (fid,)).fetchone()
        if row["cnt"] > 0:
            has_enforcement = True

        # Check historical data (multiple years)
        has_historical = False
        row = conn.execute(
            "SELECT COUNT(DISTINCT reporting_year) as yrs FROM tri_releases WHERE tri_facility_id = ?",
            (fid,),
        ).fetchone()
        if row["yrs"] >= 3:
            has_historical = True

        # Get release stats
        row = conn.execute(
            "SELECT COUNT(*) as cnt, COALESCE(SUM(total_releases_lbs), 0) as total FROM tri_releases WHERE tri_facility_id = ?",
            (fid,),
        ).fetchone()
        release_count = row["cnt"]
        total_lbs = row["total"]

        result = score_facility(
            fac,
            has_health_data=has_health,
            has_demographics=has_demo,
            has_ej_data=has_ej,
            has_enforcement=has_enforcement,
            has_historical=has_historical,
            release_count=release_count,
            total_releases_lbs=total_lbs,
        )
        scores[fid] = result["quality_score"]
        total_issues += len(result["issues"])

    update_facility_quality_scores(scores, conn=conn)

    all_scores = list(scores.values())
    if all_scores:
        avg_score = sum(all_scores) / len(all_scores)
        above_threshold = sum(1 for s in all_scores if s >= 0.6)
    else:
        avg_score = 0.0
        above_threshold = 0

    console.print(f"\n[bold]Quality Validation Results:[/bold]")
    console.print(f"  Facilities scored: {len(all_scores)}")
    console.print(f"  Average score: {avg_score:.3f}")
    console.print(f"  Above threshold (>=0.6): {above_threshold}/{len(all_scores)}")
    console.print(f"  Facilities with validation errors: {error_count}")
    console.print(f"  Total quality issues: {total_issues}")

    if should_close:
        conn.close()
