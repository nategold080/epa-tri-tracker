"""Facility entity resolution and name normalization.

Normalizes TRI facility names using:
1. Suffix stripping (LLC, Inc., Corp., Plant, Facility, etc.)
2. Abbreviation expansion (Mfg -> Manufacturing, Chem -> Chemical)
3. Parent company linkage (from TRI PARENT CO NAME field)
4. Industry sector classification from SIC codes
5. FIPS county code resolution from county name + state
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Optional

import yaml
from rich.console import Console
from thefuzz import fuzz

console = Console()

PROJECT_ROOT = Path(__file__).parent.parent.parent
CONFIG_DIR = PROJECT_ROOT / "config"

# State abbreviation to FIPS code mapping
_STATE_ABBR_TO_FIPS = {
    "AL": "01", "AK": "02", "AZ": "04", "AR": "05", "CA": "06",
    "CO": "08", "CT": "09", "DE": "10", "DC": "11", "FL": "12",
    "GA": "13", "HI": "15", "ID": "16", "IL": "17", "IN": "18",
    "IA": "19", "KS": "20", "KY": "21", "LA": "22", "ME": "23",
    "MD": "24", "MA": "25", "MI": "26", "MN": "27", "MS": "28",
    "MO": "29", "MT": "30", "NE": "31", "NV": "32", "NH": "33",
    "NJ": "34", "NM": "35", "NY": "36", "NC": "37", "ND": "38",
    "OH": "39", "OK": "40", "OR": "41", "PA": "42", "RI": "44",
    "SC": "45", "SD": "46", "TN": "47", "TX": "48", "UT": "49",
    "VT": "50", "VA": "51", "WA": "53", "WV": "54", "WI": "55",
    "WY": "56", "AS": "60", "GU": "66", "MP": "69", "PR": "72",
    "VI": "78",
}

# County name suffixes to strip for matching
_COUNTY_SUFFIXES = re.compile(
    r"\s+(County|Parish|Borough|Census Area|city|City and Borough|Municipality|Municipio)$",
    re.IGNORECASE,
)

# Common suffixes to strip for comparison
_SUFFIXES = [
    r",?\s+LLC\b",
    r",?\s+Inc\.?\b",
    r",?\s+Corp\.?\b",
    r",?\s+Corporation\b",
    r",?\s+Co\.?\b",
    r",?\s+Company\b",
    r",?\s+Ltd\.?\b",
    r",?\s+L\.?P\.?\b",
    r"\s+-\s+.+$",  # Strip trailing " - Plant Name" patterns
    r"\s+Plant\b",
    r"\s+Facility\b",
    r"\s+Operations?\b",
    r"\s+Division\b",
    r"\s+Refinery\b",
]

_SUFFIX_RE = re.compile("|".join(_SUFFIXES), re.IGNORECASE)

# Abbreviation expansions
_ABBREVIATIONS = {
    "&": "and",
    "Mfg": "Manufacturing",
    "Chem": "Chemical",
    "Natl": "National",
    "Amer": "American",
    "Intl": "International",
    "Corp": "Corporation",
    "Co": "Company",
    "Ind": "Industries",
    "Prod": "Products",
    "Svcs": "Services",
    "Tech": "Technology",
    "Engr": "Engineering",
    "Elec": "Electric",
    "Gen": "General",
    "Std": "Standard",
    "Petro": "Petroleum",
    "Pwr": "Power",
}

# Case-insensitive lookup: lowered key -> expansion value
_ABBREV_LOWER = {k.lower(): v for k, v in _ABBREVIATIONS.items()}

# SIC code to industry sector mapping
_SIC_SECTORS = {
    (100, 999): "Agriculture, Forestry & Fishing",
    (1000, 1499): "Mining",
    (1500, 1799): "Construction",
    (2000, 2099): "Food Processing",
    (2100, 2199): "Tobacco Products",
    (2200, 2299): "Textile Manufacturing",
    (2300, 2399): "Apparel Products",
    (2400, 2499): "Lumber & Wood Products",
    (2500, 2599): "Furniture & Fixtures",
    (2600, 2699): "Paper & Allied Products",
    (2700, 2799): "Printing & Publishing",
    (2800, 2899): "Chemical Manufacturing",
    (2900, 2999): "Petroleum Refining",
    (3000, 3099): "Rubber & Plastics",
    (3100, 3199): "Leather Products",
    (3200, 3299): "Stone, Clay, Glass & Concrete",
    (3300, 3399): "Primary Metals",
    (3400, 3499): "Fabricated Metal Products",
    (3500, 3599): "Industrial Machinery",
    (3600, 3699): "Electronic Equipment",
    (3700, 3799): "Transportation Equipment",
    (3800, 3899): "Instruments & Controls",
    (3900, 3999): "Miscellaneous Manufacturing",
    (4000, 4899): "Transportation & Communications",
    (4900, 4999): "Electric & Gas Utilities",
    (5000, 5199): "Wholesale Trade",
    (5200, 5999): "Retail Trade",
    (7000, 8999): "Services",
}


def _normalize_name(name: str) -> str:
    """Normalize a facility name for comparison."""
    name = name.strip()
    name = _SUFFIX_RE.sub("", name).strip()
    name = re.sub(r"\s+", " ", name)
    return name


def _expand_abbreviations(name: str) -> str:
    """Expand common abbreviations (case-insensitive).

    TRI facility names are often ALL CAPS (e.g., 'ABC MFG CO'),
    so we need case-insensitive lookup against abbreviation keys.
    """
    words = name.split()
    expanded = []
    for word in words:
        clean = word.rstrip(".")
        matched = _ABBREV_LOWER.get(clean.lower())
        if matched is not None:
            expanded.append(matched)
        else:
            expanded.append(word)
    return " ".join(expanded)


def classify_industry_sector(sic_code: Optional[str]) -> Optional[str]:
    """Map SIC code to industry sector name."""
    if not sic_code:
        return None
    try:
        sic_int = int(str(sic_code).strip()[:4])
    except (ValueError, TypeError):
        return None

    for (low, high), sector in _SIC_SECTORS.items():
        if low <= sic_int <= high:
            return sector
    return None


def resolve_facility_name(
    raw_name: str,
    known_canonicals: Optional[set[str]] = None,
) -> str:
    """Resolve a facility name to a canonical form.

    Resolution order:
    1. Normalized match against known canonical names
    2. Fuzzy match against known canonicals (threshold >= 85)
    3. Return cleaned name as-is
    """
    if not raw_name or not raw_name.strip():
        return raw_name

    raw_name = raw_name.strip()
    normalized = _normalize_name(raw_name)

    if known_canonicals:
        # Exact normalized match
        for canonical in known_canonicals:
            if _normalize_name(canonical) == normalized:
                return canonical

        # Fuzzy match
        best_match = None
        best_score = 0
        for canonical in known_canonicals:
            score = fuzz.token_sort_ratio(normalized, _normalize_name(canonical))
            if score > best_score and score >= 85:
                best_score = score
                best_match = canonical
        if best_match:
            return best_match

    return raw_name


def _normalize_county_name(name: str) -> str:
    """Normalize a county name for FIPS lookup matching.

    Strips suffixes like 'County', 'Parish', 'Borough' and normalizes case.
    """
    name = name.strip()
    name = _COUNTY_SUFFIXES.sub("", name)
    return name.upper().strip()


def build_fips_lookup(conn) -> dict[tuple[str, str], str]:
    """Build a (county_name_upper, state_abbr) -> fips_county lookup table.

    Uses county_health and county_demographics tables as the source of
    truth for FIPS codes, since those come from County Health Rankings
    which uses proper 5-digit FIPS codes.
    """
    lookup: dict[tuple[str, str], str] = {}

    rows = conn.execute(
        "SELECT DISTINCT fips_county, county_name, state FROM county_health WHERE county_name IS NOT NULL"
    ).fetchall()

    for row in rows:
        fips = row["fips_county"]
        name = row["county_name"]
        state = row["state"]

        if not fips or not name or not state:
            continue

        state_up = state.upper()

        # Normalize the county name for matching
        normalized = _normalize_county_name(name)
        lookup[(normalized, state_up)] = fips

        # Store the full uppercase name too (before suffix stripping) to avoid collisions
        # e.g., "Baltimore city" and "Baltimore County" both strip to "BALTIMORE"
        full_upper = name.strip().upper()
        if full_upper != normalized:
            lookup[(full_upper, state_up)] = fips

        # Also store common alternate forms
        # "DE WITT" vs "DEWITT", "ST. CLAIR" vs "ST CLAIR", etc.
        alt = normalized.replace(".", "").replace("'", "")
        if alt != normalized:
            lookup[(alt, state_up)] = fips

        # Handle "ST " vs "SAINT " and "STE " vs "SAINTE "
        if normalized.startswith("ST "):
            lookup[("SAINT " + normalized[3:], state_up)] = fips
        elif normalized.startswith("SAINT "):
            lookup[("ST " + normalized[6:], state_up)] = fips

    return lookup


# Hardcoded overrides for renamed/split/special counties
_COUNTY_FIPS_OVERRIDES = {
    # Valdez-Cordova Census Area split into Chugach + Copper River in 2019
    # Map to Chugach (02063) as it contains Valdez
    ("VALDEZ-CORDOVA CENSUS AREA", "AK"): "02063",
    ("VALDEZ-CORDOVA CENSU", "AK"): "02063",
    # Carson City is an independent city in Nevada (no "County" suffix in CHR)
    ("CARSON CITY", "NV"): "32510",
    # St. Croix Island, WI → St. Croix County
    ("ST. CROIX ISLAND", "WI"): "55109",
    ("ST CROIX ISLAND", "WI"): "55109",
}


def resolve_fips_codes(conn=None) -> int:
    """Populate fips_state and fips_county on TRI facilities.

    Uses county name + state abbreviation to look up FIPS codes from
    the county_health table. Returns count of facilities updated.
    """
    from src.storage.database import get_connection

    if conn is None:
        conn = get_connection()

    # Build lookup from county_health data
    lookup = build_fips_lookup(conn)
    console.print(f"[dim]Built FIPS lookup with {len(lookup)} county entries[/dim]")

    # Get all facilities missing FIPS codes
    rows = conn.execute(
        "SELECT tri_facility_id, county, state FROM tri_facilities "
        "WHERE (fips_county IS NULL OR fips_county = '') AND county IS NOT NULL"
    ).fetchall()

    console.print(f"[dim]Resolving FIPS codes for {len(rows)} facilities...[/dim]")

    updated = 0
    unmatched = set()
    now = __import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat()

    for row in rows:
        fid = row["tri_facility_id"]
        county = row["county"]
        state = row["state"]

        if not county or not state:
            continue

        # Normalize the TRI county name
        county_upper = county.strip().upper()
        state_upper = state.strip().upper()

        # Check hardcoded overrides first
        fips = _COUNTY_FIPS_OVERRIDES.get((county_upper, state_upper))

        # Strip county/parish suffixes from TRI names too
        county_clean = _COUNTY_SUFFIXES.sub("", county_upper).strip().upper()

        if not fips:
            fips = _COUNTY_FIPS_OVERRIDES.get((county_clean, state_upper))

        # Look up FIPS code — try cleaned name first, then raw
        if not fips:
            fips = lookup.get((county_clean, state_upper))
        if not fips:
            fips = lookup.get((county_upper, state_upper))

        # Try with dots/apostrophes removed
        if not fips:
            alt = county_clean.replace(".", "").replace("'", "")
            fips = lookup.get((alt, state_upper))

        # Try collapsing spaces ("DE KALB" -> "DEKALB", "LA PORTE" -> "LAPORTE")
        if not fips:
            collapsed = county_clean.replace(" ", "")
            for key, val in lookup.items():
                if key[1] == state_upper and key[0].replace(" ", "") == collapsed:
                    fips = val
                    break

        # Try "ST" <-> "SAINT" variants
        if not fips and county_clean.startswith("ST "):
            fips = lookup.get(("SAINT " + county_clean[3:], state_upper))
        if not fips and county_clean.startswith("SAINT "):
            fips = lookup.get(("ST " + county_clean[6:], state_upper))

        # Try "ST." variant
        if not fips and county_clean.startswith("ST. "):
            fips = lookup.get(("ST " + county_clean[4:], state_upper))
            if not fips:
                fips = lookup.get(("SAINT " + county_clean[4:], state_upper))

        # Handle "(CITY)" suffix — Virginia independent cities, Baltimore, St. Louis, Carson City
        # TRI has "CHESAPEAKE (CITY)" but CHR has "Chesapeake city" → stored as "CHESAPEAKE CITY" in lookup
        if not fips and "(CITY)" in county_upper:
            city_name = county_upper.replace("(CITY)", "").strip()
            city_name_nodots = city_name.replace(".", "")
            # Priority 1: Try "CITY_NAME CITY" forms (unambiguous — won't collide with counties)
            fips = lookup.get((city_name + " CITY", state_upper))
            if not fips:
                fips = lookup.get((city_name_nodots + " CITY", state_upper))
            # Priority 2: Search lookup for " CITY" suffix match with dot normalization
            if not fips:
                for key, val in lookup.items():
                    if key[1] == state_upper:
                        k = key[0].replace(".", "")
                        if k == city_name_nodots + " CITY":
                            fips = val
                            break
            # Priority 3: Try city name alone (only for cities with no same-name county)
            if not fips:
                fips = lookup.get((city_name, state_upper))
            if not fips:
                fips = lookup.get((city_name_nodots, state_upper))

        # Handle truncated Alaska borough/census area names
        # TRI truncates at ~25 chars: "FAIRBANKS NORTH STAR BORO" for "Fairbanks North Star Borough"
        if not fips and len(county_clean) >= 20:
            for key, val in lookup.items():
                if key[1] == state_upper and key[0].startswith(county_clean[:18]):
                    fips = val
                    break

        # Handle "ISLAND" suffix (e.g., "ST. CROIX ISLAND" → "ST. CROIX" or "ST CROIX")
        if not fips and county_clean.endswith(" ISLAND"):
            island_base = county_clean[:-7].strip()
            fips = lookup.get((island_base, state_upper))
            if not fips:
                alt_base = island_base.replace(".", "")
                fips = lookup.get((alt_base, state_upper))

        # Get state FIPS
        fips_state = _STATE_ABBR_TO_FIPS.get(state_upper)

        if fips:
            conn.execute(
                "UPDATE tri_facilities SET fips_county = ?, fips_state = ?, updated_at = ? "
                "WHERE tri_facility_id = ?",
                (fips, fips_state, now, fid),
            )
            updated += 1
        elif fips_state:
            # At least set state FIPS even if county not matched
            conn.execute(
                "UPDATE tri_facilities SET fips_state = ?, updated_at = ? "
                "WHERE tri_facility_id = ?",
                (fips_state, now, fid),
            )
            unmatched.add((county_upper, state_upper))
        else:
            unmatched.add((county_upper, state_upper))

    conn.commit()

    if unmatched:
        console.print(f"[yellow]Could not match {len(unmatched)} county names to FIPS codes:[/yellow]")
        for county, state in sorted(unmatched)[:20]:
            console.print(f"  [yellow]{county}, {state}[/yellow]")
        if len(unmatched) > 20:
            console.print(f"  [yellow]... and {len(unmatched) - 20} more[/yellow]")

    console.print(f"[green]Updated FIPS codes on {updated}/{len(rows)} facilities[/green]")
    return updated


def normalize_facilities() -> None:
    """Normalize all facility names and classify industries in the database."""
    from src.storage.database import get_all_facilities, get_connection

    conn = get_connection()
    facilities = get_all_facilities(limit=100000, conn=conn)
    console.print(f"[dim]Normalizing {len(facilities)} facilities...[/dim]")

    # Build canonical set from parent company names
    canonical_set: set[str] = set()
    for f in facilities:
        parent = f.get("parent_company_name")
        if parent and parent.strip():
            canonical_set.add(parent.strip())

    # Resolve each facility name
    name_mapping: dict[str, str] = {}
    industry_mapping: dict[str, str] = {}
    updated = 0

    for fac in facilities:
        fid = fac.get("tri_facility_id", "")
        name = fac.get("facility_name", "")

        if name and fid:
            canonical = resolve_facility_name(name, canonical_set)
            name_mapping[fid] = canonical
            canonical_set.add(canonical)

        # Classify industry from SIC code
        sic = fac.get("sic_code")
        existing_sector = fac.get("industry_sector")
        if sic and not existing_sector:
            sector = classify_industry_sector(sic)
            if sector:
                industry_mapping[fid] = sector

    # Update database
    now = __import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat()
    for fid, canonical in name_mapping.items():
        conn.execute(
            "UPDATE tri_facilities SET canonical_name = ?, updated_at = ? WHERE tri_facility_id = ?",
            (canonical, now, fid),
        )
        updated += 1

    for fid, sector in industry_mapping.items():
        conn.execute(
            "UPDATE tri_facilities SET industry_sector = ?, updated_at = ? WHERE tri_facility_id = ?",
            (sector, now, fid),
        )

    conn.commit()
    console.print(f"[green]Updated canonical names on {updated} facilities[/green]")
    console.print(f"[green]Classified industry sector for {len(industry_mapping)} facilities[/green]")

    # Resolve FIPS county codes
    fips_updated = resolve_fips_codes(conn=conn)

    # Stats
    row = conn.execute(
        "SELECT COUNT(DISTINCT parent_company_name) as cnt FROM tri_facilities WHERE parent_company_name IS NOT NULL"
    ).fetchone()
    console.print(f"[green]Unique parent companies: {row['cnt']}[/green]")

    row = conn.execute(
        "SELECT COUNT(*) as cnt FROM tri_facilities WHERE fips_county IS NOT NULL AND fips_county != ''"
    ).fetchone()
    console.print(f"[green]Facilities with FIPS county codes: {row['cnt']}[/green]")

    conn.close()
