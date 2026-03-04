"""Cross-link RMP facilities to TRI facilities.

Matching strategy (in priority order):
1. FRS Registry ID (highest confidence) — both TRI and RMP report FRS IDs
2. Address matching (medium confidence) — normalized street + city + state
3. Name + city + state matching (lower confidence) — facility name fuzzy match
"""

from __future__ import annotations

import re
from typing import Optional

from rich.console import Console

console = Console()


def _normalize_address(addr: str) -> str:
    """Normalize an address for matching."""
    if not addr:
        return ""
    addr = addr.upper().strip()
    # Common abbreviations
    addr = re.sub(r'\bSTREET\b', 'ST', addr)
    addr = re.sub(r'\bAVENUE\b', 'AVE', addr)
    addr = re.sub(r'\bBOULEVARD\b', 'BLVD', addr)
    addr = re.sub(r'\bDRIVE\b', 'DR', addr)
    addr = re.sub(r'\bROAD\b', 'RD', addr)
    addr = re.sub(r'\bLANE\b', 'LN', addr)
    addr = re.sub(r'\bHIGHWAY\b', 'HWY', addr)
    addr = re.sub(r'\bROUTE\b', 'RTE', addr)
    addr = re.sub(r'\bNORTH\b', 'N', addr)
    addr = re.sub(r'\bSOUTH\b', 'S', addr)
    addr = re.sub(r'\bEAST\b', 'E', addr)
    addr = re.sub(r'\bWEST\b', 'W', addr)
    # Remove punctuation
    addr = re.sub(r'[.,#\-/]', ' ', addr)
    addr = re.sub(r'\s+', ' ', addr).strip()
    return addr


def _normalize_name(name: str) -> str:
    """Normalize a facility name for matching."""
    if not name:
        return ""
    name = name.upper().strip()
    # Remove common suffixes
    for suffix in ('LLC', 'INC', 'CORP', 'CO', 'COMPANY', 'CORPORATION',
                   'LTD', 'LP', 'LLP', 'PLANT', 'FACILITY', 'OPERATIONS'):
        name = re.sub(rf'\b{suffix}\b\.?', '', name)
    name = re.sub(r'[.,\-/()]', ' ', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name


def build_tri_rmp_links(conn=None) -> list[dict]:
    """Build cross-links between TRI and RMP facilities.

    Uses three strategies:
    1. FRS registry ID match (confidence 1.0)
    2. Address + city + state match (confidence 0.8)
    3. Name + city + state match (confidence 0.6)
    """
    if conn is None:
        from src.storage.database import get_connection
        conn = get_connection()

    links = []
    linked_tri = set()
    linked_rmp = set()

    # Strategy 1: FRS Registry ID match via tri_frs_links
    console.print("[dim]RMP linking: Strategy 1 — FRS Registry ID match...[/dim]")
    rows = conn.execute("""
        SELECT DISTINCT tfl.tri_facility_id, rf.rmp_id
        FROM tri_frs_links tfl
        JOIN rmp_facilities rf ON tfl.registry_id = rf.frs_registry_id
        WHERE rf.frs_registry_id IS NOT NULL AND rf.frs_registry_id != ''
    """).fetchall()
    for row in rows:
        tri_id = row["tri_facility_id"]
        rmp_id = row["rmp_id"]
        links.append({
            "tri_facility_id": tri_id,
            "rmp_id": rmp_id,
            "link_method": "frs_registry",
            "confidence": 1.0,
        })
        linked_tri.add(tri_id)
        linked_rmp.add(rmp_id)
    console.print(f"[dim]  FRS match: {len(links)} links[/dim]")

    # Strategy 2: Address + city + state match
    console.print("[dim]RMP linking: Strategy 2 — Address match...[/dim]")
    # Get unlinked TRI facilities
    tri_facilities = conn.execute("""
        SELECT tri_facility_id, street_address, city, state
        FROM tri_facilities
        WHERE street_address IS NOT NULL AND city IS NOT NULL AND state IS NOT NULL
    """).fetchall()

    # Build TRI address hash
    tri_by_addr: dict[str, str] = {}
    for fac in tri_facilities:
        tid = fac["tri_facility_id"]
        if tid in linked_tri:
            continue
        key = f"{_normalize_address(fac['street_address'])}|{(fac['city'] or '').upper()}|{(fac['state'] or '').upper()}"
        tri_by_addr[key] = tid

    # Check RMP facilities
    rmp_facilities = conn.execute("""
        SELECT rmp_id, street_address, city, state
        FROM rmp_facilities
        WHERE street_address IS NOT NULL AND city IS NOT NULL AND state IS NOT NULL
    """).fetchall()

    addr_links = 0
    for rmp in rmp_facilities:
        rid = rmp["rmp_id"]
        if rid in linked_rmp:
            continue
        key = f"{_normalize_address(rmp['street_address'])}|{(rmp['city'] or '').upper()}|{(rmp['state'] or '').upper()}"
        if key in tri_by_addr:
            tri_id = tri_by_addr[key]
            links.append({
                "tri_facility_id": tri_id,
                "rmp_id": rid,
                "link_method": "address_match",
                "confidence": 0.8,
            })
            linked_tri.add(tri_id)
            linked_rmp.add(rid)
            addr_links += 1
    console.print(f"[dim]  Address match: {addr_links} links[/dim]")

    # Strategy 3: Name + city + state match
    console.print("[dim]RMP linking: Strategy 3 — Name match...[/dim]")
    tri_by_name: dict[str, str] = {}
    for fac in tri_facilities:
        tid = fac["tri_facility_id"]
        if tid in linked_tri:
            continue
        name = _normalize_name(conn.execute(
            "SELECT facility_name FROM tri_facilities WHERE tri_facility_id = ?", (tid,)
        ).fetchone()["facility_name"])
        city = (fac["city"] or "").upper()
        state = (fac["state"] or "").upper()
        if name and city and state:
            key = f"{name}|{city}|{state}"
            tri_by_name[key] = tid

    name_links = 0
    for rmp in rmp_facilities:
        rid = rmp["rmp_id"]
        if rid in linked_rmp:
            continue
        rmp_name_row = conn.execute(
            "SELECT facility_name FROM rmp_facilities WHERE rmp_id = ?", (rid,)
        ).fetchone()
        if not rmp_name_row:
            continue
        name = _normalize_name(rmp_name_row["facility_name"])
        city = (rmp["city"] or "").upper()
        state = (rmp["state"] or "").upper()
        if name and city and state:
            key = f"{name}|{city}|{state}"
            if key in tri_by_name:
                tri_id = tri_by_name[key]
                links.append({
                    "tri_facility_id": tri_id,
                    "rmp_id": rid,
                    "link_method": "name_match",
                    "confidence": 0.6,
                })
                linked_tri.add(tri_id)
                linked_rmp.add(rid)
                name_links += 1
    console.print(f"[dim]  Name match: {name_links} links[/dim]")

    console.print(f"[green]Total TRI→RMP links: {len(links):,}[/green]")
    return links
