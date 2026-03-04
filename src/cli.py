"""CLI interface for EPA TRI Community Health & Demographics Tracker.

Commands: download, ingest, normalize, validate, export, dashboard, stats, pipeline,
          echo, superfund, ejscreen, enrich
"""

import click
from pathlib import Path
from rich.console import Console

console = Console()
PROJECT_ROOT = Path(__file__).parent.parent


@click.group()
@click.version_option(version="0.2.0")
def cli():
    """EPA TRI Community Health & Demographics Tracker

    Cross-linked database of EPA Toxic Release Inventory facility data
    with county health outcomes, Census demographics, EPA ECHO enforcement,
    Superfund proximity, and EJScreen environmental justice indicators.
    """
    pass


@cli.command()
@click.option("--states", "-s", help="Comma-separated state codes (default: top 10)")
@click.option("--years", "-y", help="Comma-separated years (default: 2022,2023)")
@click.option("--force", "-f", is_flag=True, help="Re-download even if cached")
def download(states, years, force):
    """Download TRI data from EPA."""
    from src.scrapers.tri_downloader import download_tri_data, PRIORITY_STATES

    state_list = states.split(",") if states else PRIORITY_STATES
    year_list = [int(y) for y in years.split(",")] if years else [2022, 2023]

    console.print(f"[bold blue]Downloading TRI data for {len(state_list)} states, {len(year_list)} years...[/bold blue]")
    all_data = download_tri_data(states=state_list, years=year_list, force=force)

    total_records = sum(len(v) for v in all_data.values())
    console.print(f"\n[bold green]Downloaded {total_records:,} release records from {len(all_data)} states[/bold green]")


@cli.command()
@click.option("--states", "-s", help="Comma-separated state codes (default: top 10)")
@click.option("--years", "-y", help="Comma-separated years (default: 2022,2023)")
@click.option("--force", "-f", is_flag=True, help="Re-download even if cached")
def ingest(states, years, force):
    """Download TRI data and ingest into database."""
    from src.scrapers.tri_downloader import (
        download_tri_data,
        extract_facilities,
        extract_releases,
        PRIORITY_STATES,
    )
    from src.storage.database import (
        init_db,
        store_facilities_batch,
        store_releases_batch,
        start_pipeline_run,
        complete_pipeline_run,
    )

    state_list = states.split(",") if states else PRIORITY_STATES
    year_list = [int(y) for y in years.split(",")] if years else [2022, 2023]

    console.print(f"[bold blue]Ingesting TRI data for {len(state_list)} states, {len(year_list)} years...[/bold blue]")

    conn = init_db()
    run_id = start_pipeline_run("epa_tri", "ingest", conn=conn)

    all_data = download_tri_data(states=state_list, years=year_list, force=force)

    total_fac_created, total_fac_updated = 0, 0
    total_rel_created, total_rel_updated = 0, 0
    errors = 0

    for state_code, records in all_data.items():
        try:
            facilities = extract_facilities(records)
            releases = extract_releases(records)

            fc, fu = store_facilities_batch(facilities, conn=conn)
            rc, ru = store_releases_batch(releases, conn=conn)

            total_fac_created += fc
            total_fac_updated += fu
            total_rel_created += rc
            total_rel_updated += ru

            console.print(
                f"[green]{state_code}: {fc} new + {fu} updated facilities, "
                f"{rc} new + {ru} updated releases[/green]"
            )
        except Exception as e:
            errors += 1
            console.print(f"[red]Error processing {state_code}: {e}[/red]")

    total_records = sum(len(v) for v in all_data.values())
    complete_pipeline_run(
        run_id,
        records_processed=total_records,
        records_created=total_fac_created + total_rel_created,
        records_updated=total_fac_updated + total_rel_updated,
        errors=errors,
        conn=conn,
    )

    conn.close()
    console.print(f"\n[bold green]Ingestion complete:[/bold green]")
    console.print(f"  Facilities: {total_fac_created} created, {total_fac_updated} updated")
    console.print(f"  Releases: {total_rel_created} created, {total_rel_updated} updated")


@cli.command()
def normalize():
    """Normalize facility names and classify industries."""
    from src.normalization.facilities import normalize_facilities
    normalize_facilities()


@cli.command()
def validate():
    """Run data quality validation and compute quality scores."""
    from src.validation.quality import validate_all
    validate_all()


@cli.command()
@click.option("--format", "-f", "fmt",
              type=click.Choice(["csv", "json", "excel", "markdown", "all"]),
              default="all")
@click.option("--output-dir", "-o", default="data/exports")
def export(fmt, output_dir):
    """Export data in various formats."""
    from src.export.exporter import export_data
    export_data(fmt, output_dir)


@cli.command()
@click.option("--port", "-p", type=int, default=8503, help="Dashboard port")
def dashboard(port):
    """Launch the Streamlit dashboard."""
    import subprocess
    subprocess.run(["streamlit", "run", "src/dashboard/app.py", "--server.port", str(port)])


@cli.command()
def stats():
    """Show database statistics."""
    from src.storage.database import get_stats
    get_stats()


# --- Phase 1: ECHO Enforcement ---

@cli.command()
@click.option("--force", "-f", is_flag=True, help="Re-download even if cached")
def echo(force):
    """Download and ingest EPA ECHO enforcement & compliance data."""
    from src.scrapers.echo_downloader import download_all_echo
    from src.storage.database import (
        init_db,
        store_frs_links_batch,
        store_enforcement_batch,
        store_inspections_batch,
        store_compliance_batch,
        start_pipeline_run,
        complete_pipeline_run,
    )

    conn = init_db()
    run_id = start_pipeline_run("echo", "download_and_ingest", conn=conn)

    data = download_all_echo(force=force)

    frs_count = store_frs_links_batch(data["frs_links"], conn=conn)
    console.print(f"[green]Stored {frs_count:,} FRS links[/green]")

    enf_c, enf_u = store_enforcement_batch(data["enforcement"], conn=conn)
    console.print(f"[green]Enforcement: {enf_c:,} created, {enf_u:,} updated[/green]")

    insp_c, insp_u = store_inspections_batch(data["inspections"], conn=conn)
    console.print(f"[green]Inspections: {insp_c:,} created, {insp_u:,} updated[/green]")

    comp_count = store_compliance_batch(data["compliance"], conn=conn)
    console.print(f"[green]Compliance: {comp_count:,} records[/green]")

    total = frs_count + enf_c + enf_u + insp_c + insp_u + comp_count
    complete_pipeline_run(
        run_id,
        records_processed=sum(len(v) for v in data.values()),
        records_created=frs_count + enf_c + insp_c + comp_count,
        records_updated=enf_u + insp_u,
        conn=conn,
    )

    # Run enforcement linkage summary
    from src.normalization.enforcement_linker import link_enforcement_to_facilities
    link_enforcement_to_facilities(conn=conn)

    conn.close()
    console.print(f"\n[bold green]ECHO ingestion complete: {total:,} total records[/bold green]")


# --- Phase 3: Superfund ---

@cli.command()
@click.option("--force", "-f", is_flag=True, help="Re-download even if cached")
@click.option("--radius", "-r", type=float, default=5.0, help="Proximity radius in miles")
def superfund(force, radius):
    """Download Superfund sites and compute TRI facility proximity."""
    from src.scrapers.superfund_downloader import download_superfund_sites, compute_proximity
    from src.storage.database import (
        init_db,
        get_all_facilities,
        store_superfund_sites_batch,
        store_superfund_proximity_batch,
        start_pipeline_run,
        complete_pipeline_run,
    )

    conn = init_db()
    run_id = start_pipeline_run("superfund", "download_and_compute", conn=conn)

    sites = download_superfund_sites(force=force)
    if sites:
        sc, su = store_superfund_sites_batch(sites, conn=conn)
        console.print(f"[green]Superfund sites: {sc} created, {su} updated[/green]")
    else:
        console.print("[yellow]No Superfund sites downloaded[/yellow]")

    # Compute proximity
    facilities = get_all_facilities(limit=100000, conn=conn)
    proximity = compute_proximity(facilities, sites, radius_miles=radius)

    if proximity:
        pc = store_superfund_proximity_batch(proximity, conn=conn)
        console.print(f"[green]Proximity records: {pc:,}[/green]")

    complete_pipeline_run(
        run_id,
        records_processed=len(sites) + len(facilities),
        records_created=len(proximity),
        conn=conn,
    )
    conn.close()


# --- Phase 4: EJScreen ---

@cli.command()
@click.option("--force", "-f", is_flag=True, help="Re-download even if cached")
def ejscreen(force):
    """Download and ingest EJScreen environmental justice indicators."""
    from src.scrapers.ejscreen_downloader import download_ejscreen
    from src.storage.database import (
        init_db,
        store_ej_indicators_batch,
        start_pipeline_run,
        complete_pipeline_run,
    )

    conn = init_db()
    run_id = start_pipeline_run("ejscreen", "download_and_ingest", conn=conn)

    records = download_ejscreen(force=force)
    if records:
        ec, eu = store_ej_indicators_batch(records, conn=conn)
        console.print(f"[green]EJ indicators: {ec:,} created, {eu:,} updated[/green]")
    else:
        console.print("[yellow]No EJScreen data downloaded[/yellow]")

    complete_pipeline_run(
        run_id,
        records_processed=len(records),
        records_created=len(records),
        conn=conn,
    )
    conn.close()


# --- Phase 5: RMP (Risk Management Program) ---

@cli.command()
@click.option("--force", "-f", is_flag=True, help="Re-download even if cached")
def rmp(force):
    """Download and ingest EPA RMP chemical accident risk data."""
    from src.scrapers.rmp_downloader import download_all_rmp
    from src.normalization.rmp_linker import build_tri_rmp_links
    from src.storage.database import (
        init_db,
        store_rmp_facilities_batch,
        store_rmp_chemicals_batch,
        store_rmp_accidents_batch,
        store_tri_rmp_links_batch,
        start_pipeline_run,
        complete_pipeline_run,
    )

    conn = init_db()
    run_id = start_pipeline_run("rmp", "download_and_ingest", conn=conn)

    data = download_all_rmp(force=force)

    fc, fu = store_rmp_facilities_batch(data["facilities"], conn=conn)
    console.print(f"[green]RMP facilities: {fc:,} created, {fu:,} updated[/green]")

    cc, cu = store_rmp_chemicals_batch(data["chemicals"], conn=conn)
    console.print(f"[green]RMP chemicals: {cc:,} created, {cu:,} updated[/green]")

    ac, au = store_rmp_accidents_batch(data["accidents"], conn=conn)
    console.print(f"[green]RMP accidents: {ac:,} created, {au:,} updated[/green]")

    # Build cross-links
    console.print("\n[bold]Building TRI→RMP cross-links...[/bold]")
    links = build_tri_rmp_links(conn=conn)
    lc = store_tri_rmp_links_batch(links, conn=conn)
    console.print(f"[green]TRI→RMP links: {lc:,} stored[/green]")

    total = fc + fu + cc + cu + ac + au + lc
    complete_pipeline_run(
        run_id,
        records_processed=sum(len(v) for v in data.values()),
        records_created=fc + cc + ac + lc,
        records_updated=fu + cu + au,
        conn=conn,
    )

    conn.close()
    console.print(f"\n[bold green]RMP ingestion complete: {total:,} total records[/bold green]")


# --- Full Enrichment Pipeline ---

@cli.command()
@click.option("--states", "-s", help="Comma-separated state codes")
@click.option("--years", "-y", help="Comma-separated years")
@click.option("--skip-download", is_flag=True, help="Skip download step")
def pipeline(states, years, skip_download):
    """Run the full pipeline: download -> ingest -> normalize -> validate."""
    from src.storage.database import init_db, get_stats

    console.print("[bold blue]Starting full pipeline...[/bold blue]")
    conn = init_db()
    conn.close()

    if not skip_download:
        console.print("\n[bold]Step 1: Downloading and ingesting TRI data...[/bold]")
        from click.testing import CliRunner
        runner = CliRunner()
        args = []
        if states:
            args.extend(["--states", states])
        if years:
            args.extend(["--years", years])
        runner.invoke(ingest, args, catch_exceptions=False)

    console.print("\n[bold]Step 2: Normalizing facility entities...[/bold]")
    from src.normalization.facilities import normalize_facilities
    normalize_facilities()

    console.print("\n[bold]Step 3: Validating data quality...[/bold]")
    from src.validation.quality import validate_all
    validate_all()

    console.print("\n[bold]Step 4: Summary statistics...[/bold]")
    get_stats()

    console.print("\n[bold green]Pipeline complete![/bold green]")


@cli.command()
@click.option("--force", "-f", is_flag=True, help="Re-download even if cached")
@click.option("--skip-tri", is_flag=True, help="Skip TRI re-download")
def enrich(force, skip_tri):
    """Run full enrichment pipeline: ECHO + Superfund + EJScreen + validate + export."""
    from src.storage.database import init_db, get_stats
    from click.testing import CliRunner

    console.print("[bold blue]Starting enrichment pipeline...[/bold blue]")
    conn = init_db()
    conn.close()

    runner = CliRunner()

    console.print("\n[bold]Phase 1: ECHO Enforcement & Compliance...[/bold]")
    args = ["--force"] if force else []
    runner.invoke(echo, args, catch_exceptions=False)

    console.print("\n[bold]Phase 3: Superfund/NPL Proximity...[/bold]")
    runner.invoke(superfund, args, catch_exceptions=False)

    console.print("\n[bold]Phase 4: EJScreen Indicators...[/bold]")
    runner.invoke(ejscreen, args, catch_exceptions=False)

    console.print("\n[bold]Phase 5: RMP Chemical Accident Risk...[/bold]")
    runner.invoke(rmp, args, catch_exceptions=False)

    console.print("\n[bold]Revalidating quality scores...[/bold]")
    from src.validation.quality import validate_all
    validate_all()

    console.print("\n[bold]Exporting updated data...[/bold]")
    from src.export.exporter import export_data
    export_data("all", "data/exports")

    console.print("\n[bold]Final statistics...[/bold]")
    get_stats()

    console.print("\n[bold green]Enrichment pipeline complete![/bold green]")


if __name__ == "__main__":
    cli()
