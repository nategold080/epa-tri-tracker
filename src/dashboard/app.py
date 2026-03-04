"""EPA TRI Community Health & Demographics Tracker — Streamlit Dashboard.

Interactive dashboard for exploring toxic release data cross-linked
with county health outcomes, demographics, enforcement data,
Superfund proximity, and environmental justice indicators.
"""

import sqlite3
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# --- Config ---
PROJECT_ROOT = Path(__file__).parent.parent.parent
DB_PATH = PROJECT_ROOT / "data" / "epa_tri_tracker.db"

st.set_page_config(
    page_title="EPA TRI Community Health Tracker",
    page_icon="🏭",
    layout="wide",
    initial_sidebar_state="expanded",
)


@st.cache_resource
def get_db():
    conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA journal_mode=WAL")
    except Exception:
        pass
    return conn


@st.cache_data(ttl=300)
def query_df(sql, params=None):
    conn = get_db()
    try:
        return pd.read_sql_query(sql, conn, params=params or [])
    except Exception:
        return pd.DataFrame()


def _table_exists(name: str) -> bool:
    """Check if a table exists in the database."""
    try:
        conn = get_db()
        row = conn.execute(
            "SELECT COUNT(*) as cnt FROM sqlite_master WHERE type='table' AND name=?", (name,)
        ).fetchone()
        return row[0] > 0
    except Exception:
        return False


def main():
    # Sidebar
    st.sidebar.title("EPA TRI Tracker")
    st.sidebar.markdown("Community Health & Demographics")

    pages = [
        "National Overview",
        "Facility Explorer",
        "Community Impact",
        "Enforcement Dashboard",
        "Compliance Tracker",
        "Chemical Analysis",
        "Industry Analysis",
        "Environmental Justice",
        "Superfund Proximity",
        "Chemical Accident Risk (RMP)",
        "Dual Risk Analysis",
        "Geographic Map",
        "Trends",
    ]

    page = st.sidebar.radio("Navigate", pages)

    st.sidebar.markdown("---")
    st.sidebar.markdown(
        "Built by **Nathan Goldberg**  \n"
        "nathanmauricegoldberg@gmail.com  \n"
        "[LinkedIn](https://www.linkedin.com/in/nathan-goldberg-62a44522a/)"
    )

    if page == "National Overview":
        render_overview()
    elif page == "Facility Explorer":
        render_facility_explorer()
    elif page == "Community Impact":
        render_community_impact()
    elif page == "Enforcement Dashboard":
        render_enforcement()
    elif page == "Compliance Tracker":
        render_compliance()
    elif page == "Chemical Analysis":
        render_chemical_analysis()
    elif page == "Industry Analysis":
        render_industry_analysis()
    elif page == "Environmental Justice":
        render_ej_analysis()
    elif page == "Superfund Proximity":
        render_superfund()
    elif page == "Chemical Accident Risk (RMP)":
        render_rmp()
    elif page == "Dual Risk Analysis":
        render_dual_risk()
    elif page == "Geographic Map":
        render_map()
    elif page == "Trends":
        render_trends()


# ---- NATIONAL OVERVIEW ----

def render_overview():
    st.title("National Overview")
    st.markdown("EPA Toxic Release Inventory cross-linked with enforcement, health, demographics, and environmental justice data.")

    if not _table_exists("tri_facilities") or not _table_exists("tri_releases"):
        st.warning("No data loaded. Run the pipeline first: `python -m src.cli pipeline`")
        return

    stats_df = query_df("""
        SELECT
            (SELECT COUNT(*) FROM tri_facilities) as total_facilities,
            (SELECT COUNT(DISTINCT state) FROM tri_facilities) as total_states,
            (SELECT COUNT(DISTINCT chemical_name) FROM tri_releases) as total_chemicals,
            (SELECT COALESCE(SUM(total_releases_lbs), 0) FROM tri_releases) as total_lbs,
            (SELECT COUNT(*) FROM tri_releases) as total_releases,
            (SELECT COUNT(*) FROM tri_releases WHERE carcinogen_flag = 'YES') as carcinogen_releases,
            (SELECT AVG(quality_score) FROM tri_facilities WHERE quality_score IS NOT NULL) as avg_quality
    """)

    if stats_df.empty:
        st.warning("No data found. Please run the pipeline first.")
        return

    stats = stats_df.iloc[0]

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("TRI Facilities", f"{int(stats['total_facilities'] or 0):,}")
    c2.metric("States Covered", int(stats['total_states'] or 0))
    c3.metric("Chemicals Tracked", int(stats['total_chemicals'] or 0))
    c4.metric("Total Releases (lbs)", f"{(stats['total_lbs'] or 0):,.0f}")

    # Enforcement KPIs
    if _table_exists("enforcement_actions") and _table_exists("facility_inspections") and _table_exists("tri_frs_links"):
        enf_stats = query_df("""
            SELECT
                (SELECT COUNT(*) FROM enforcement_actions) as enf_count,
                (SELECT COALESCE(SUM(penalty_amount), 0) FROM enforcement_actions) as total_penalties,
                (SELECT COUNT(*) FROM facility_inspections) as insp_count,
                (SELECT COUNT(DISTINCT tri_facility_id) FROM tri_frs_links) as linked_facilities
        """)
        if not enf_stats.empty:
            es = enf_stats.iloc[0]
            c5, c6, c7, c8 = st.columns(4)
            c5.metric("Enforcement Actions", f"{int(es['enf_count'] or 0):,}")
            c6.metric("Total Penalties ($)", f"${(es['total_penalties'] or 0):,.0f}")
            c7.metric("Inspections", f"{int(es['insp_count'] or 0):,}")
            c8.metric("Avg Quality Score", f"{(stats['avg_quality'] or 0):.3f}")

    st.markdown("---")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Facilities by State")
        state_df = query_df("""
            SELECT state, COUNT(*) as facilities
            FROM tri_facilities GROUP BY state ORDER BY facilities DESC LIMIT 20
        """)
        if not state_df.empty:
            fig = px.bar(state_df, x="state", y="facilities", color="facilities",
                         color_continuous_scale="Blues")
            fig.update_layout(template="plotly_dark", height=400)
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Top Chemicals by Release Volume")
        chem_df = query_df("""
            SELECT chemical_name, SUM(total_releases_lbs) as total_lbs
            FROM tri_releases
            GROUP BY chemical_name
            ORDER BY total_lbs DESC LIMIT 15
        """)
        if not chem_df.empty:
            fig = px.bar(chem_df, x="total_lbs", y="chemical_name", orientation="h",
                         color="total_lbs", color_continuous_scale="Reds")
            fig.update_layout(template="plotly_dark", height=400, yaxis=dict(autorange="reversed"))
            st.plotly_chart(fig, use_container_width=True)

    st.subheader("Facilities by Industry Sector")
    ind_df = query_df("""
        SELECT COALESCE(industry_sector, 'Unknown') as sector, COUNT(*) as count
        FROM tri_facilities GROUP BY sector ORDER BY count DESC
    """)
    if not ind_df.empty:
        fig = px.pie(ind_df, names="sector", values="count", hole=0.4)
        fig.update_layout(template="plotly_dark", height=400)
        st.plotly_chart(fig, use_container_width=True)


# ---- FACILITY EXPLORER ----

def render_facility_explorer():
    st.title("Facility Explorer")

    if not _table_exists("tri_facilities"):
        st.warning("No data loaded. Run the pipeline first: `python -m src.cli pipeline`")
        return

    col1, col2, col3 = st.columns(3)
    with col1:
        states = query_df("SELECT DISTINCT state FROM tri_facilities ORDER BY state")
        state_list = states["state"].tolist() if not states.empty else []
        selected_state = st.selectbox("State", ["All"] + state_list)
    with col2:
        search = st.text_input("Search facility name")
    with col3:
        industries = query_df(
            "SELECT DISTINCT industry_sector FROM tri_facilities WHERE industry_sector IS NOT NULL ORDER BY industry_sector"
        )
        industry_list = industries["industry_sector"].tolist() if not industries.empty else []
        selected_industry = st.selectbox("Industry", ["All"] + industry_list)

    conditions = []
    params = []
    if selected_state != "All":
        conditions.append("f.state = ?")
        params.append(selected_state)
    if search:
        conditions.append("f.facility_name LIKE ?")
        params.append(f"%{search}%")
    if selected_industry != "All":
        conditions.append("f.industry_sector = ?")
        params.append(selected_industry)

    where = " WHERE " + " AND ".join(conditions) if conditions else ""

    fac_df = query_df(f"""
        SELECT f.tri_facility_id, f.facility_name, f.city, f.county, f.state,
               f.industry_sector, f.parent_company_name, f.quality_score,
               f.latitude, f.longitude, f.fips_county,
               COUNT(r.id) as release_count,
               COALESCE(SUM(r.total_releases_lbs), 0) as total_lbs
        FROM tri_facilities f
        LEFT JOIN tri_releases r ON f.tri_facility_id = r.tri_facility_id
        {where}
        GROUP BY f.tri_facility_id
        ORDER BY total_lbs DESC
        LIMIT 500
    """, params)

    if len(fac_df) >= 500:
        st.markdown(f"**{len(fac_df)} facilities** (showing top 500 by release volume)")
    else:
        st.markdown(f"**{len(fac_df)} facilities** (by release volume)")

    st.dataframe(
        fac_df[["facility_name", "city", "state", "industry_sector",
                "parent_company_name", "release_count", "total_lbs", "quality_score"]],
        use_container_width=True,
        height=400,
    )

    if len(fac_df) > 0:
        # Use TRI ID + name for unique selection (facility names can be duplicated)
        fac_df["_display"] = fac_df["facility_name"] + " (" + fac_df["tri_facility_id"] + ")"
        selected = st.selectbox("Select facility for details", fac_df["_display"].tolist())
        if selected:
            fac = fac_df[fac_df["_display"] == selected].iloc[0]
            fid = fac["tri_facility_id"]

            st.subheader(f"Detail: {selected}")

            col1, col2 = st.columns(2)
            with col1:
                st.markdown(f"""
                - **TRI ID:** {fid}
                - **Location:** {fac.get('city', 'N/A')}, {fac['state']}
                - **County:** {fac.get('county', 'N/A')}
                - **Industry:** {fac.get('industry_sector', 'N/A')}
                - **Parent Company:** {fac.get('parent_company_name', 'N/A')}
                - **Quality Score:** {fac.get('quality_score', 'N/A')}
                """)

            with col2:
                rel_df = query_df("""
                    SELECT chemical_name, reporting_year, total_releases_lbs,
                           carcinogen_flag, fugitive_air_lbs, stack_air_lbs, water_lbs
                    FROM tri_releases WHERE tri_facility_id = ?
                    ORDER BY reporting_year DESC, total_releases_lbs DESC
                """, [fid])
                st.dataframe(rel_df, use_container_width=True, height=300)

            # Enforcement history
            if _table_exists("tri_frs_links"):
                enf_df = query_df("""
                    SELECT ea.case_name, ea.enforcement_type, ea.penalty_amount,
                           ea.settlement_date, ea.enforcement_outcome
                    FROM enforcement_actions ea
                    JOIN tri_frs_links tfl ON ea.registry_id = tfl.registry_id
                    WHERE tfl.tri_facility_id = ?
                    ORDER BY ea.penalty_amount DESC NULLS LAST
                """, [fid])
                if len(enf_df) > 0:
                    st.subheader("Enforcement History")
                    st.dataframe(enf_df, use_container_width=True)

            # County health context
            fips = fac.get("fips_county")
            if fips:
                health = query_df(
                    "SELECT * FROM county_health WHERE fips_county = ? ORDER BY year DESC LIMIT 1",
                    [fips],
                )
                if len(health) > 0:
                    st.subheader("County Health Context")
                    h = health.iloc[0]
                    hc1, hc2, hc3, hc4 = st.columns(4)
                    le = h.get('life_expectancy')
                    hc1.metric("Life Expectancy", f"{le:.1f} yrs" if pd.notna(le) else "N/A")
                    ph = h.get('poor_health_pct')
                    hc2.metric("Poor Health %", f"{ph * 100:.1f}%" if pd.notna(ph) else "N/A")
                    ob = h.get('adult_obesity_pct')
                    hc3.metric("Obesity %", f"{ob * 100:.1f}%" if pd.notna(ob) else "N/A")
                    lb = h.get('low_birthweight_pct')
                    hc4.metric("Low Birthweight %", f"{lb * 100:.1f}%" if pd.notna(lb) else "N/A")


# ---- COMMUNITY IMPACT ----

def render_community_impact():
    st.title("Community Impact")
    st.markdown("Explore the relationship between TRI facilities and community health.")

    if not _table_exists("tri_facilities"):
        st.warning("No data loaded. Run the pipeline first: `python -m src.cli pipeline`")
        return

    counties = query_df("""
        SELECT DISTINCT f.fips_county, f.county || ', ' || f.state as label
        FROM tri_facilities f
        WHERE f.fips_county IS NOT NULL
        ORDER BY label
    """)

    if counties.empty:
        st.warning("No counties with FIPS codes found.")
        return

    selected_county = st.selectbox("Select County", counties["label"].tolist())
    if not selected_county:
        return

    matched = counties[counties["label"] == selected_county]
    if matched.empty:
        return
    fips = matched["fips_county"].iloc[0]

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("TRI Facilities in County")
        fac_df = query_df("""
            SELECT f.facility_name, f.industry_sector,
                   COALESCE(SUM(r.total_releases_lbs), 0) as total_lbs,
                   COUNT(DISTINCT r.chemical_name) as chemicals
            FROM tri_facilities f
            LEFT JOIN tri_releases r ON f.tri_facility_id = r.tri_facility_id
            WHERE f.fips_county = ?
            GROUP BY f.tri_facility_id
            ORDER BY total_lbs DESC
        """, [fips])
        st.dataframe(fac_df, use_container_width=True)
        st.metric("Total Facilities", len(fac_df))

    with col2:
        st.subheader("County Health Indicators")
        health = query_df(
            "SELECT * FROM county_health WHERE fips_county = ? ORDER BY year DESC LIMIT 1",
            [fips],
        )
        if len(health) > 0:
            h = health.iloc[0]
            metrics = {
                "Life Expectancy": h.get("life_expectancy"),
                "Premature Death Rate": h.get("premature_death_rate"),
                "Poor Health %": h.get("poor_health_pct"),
                "Poor Mental Health Days": h.get("poor_mental_health_days"),
                "Adult Obesity %": h.get("adult_obesity_pct"),
                "Low Birthweight %": h.get("low_birthweight_pct"),
                "Uninsured %": h.get("uninsured_pct"),
                "Adult Smoking %": h.get("adult_smoking_pct"),
            }
            for label, val in metrics.items():
                if val is not None and pd.notna(val):
                    st.metric(label, f"{val:.1f}")
        else:
            st.info("No health data available for this county.")

    # Demographics
    st.subheader("Demographics")
    demo = query_df(
        "SELECT * FROM county_demographics WHERE fips_county = ? ORDER BY year DESC LIMIT 1",
        [fips],
    )
    if len(demo) > 0:
        d = demo.iloc[0]
        dc1, dc2, dc3, dc4 = st.columns(4)
        if pd.notna(d.get("median_household_income")):
            dc1.metric("Median Income", f"${d['median_household_income']:,.0f}")
        if pd.notna(d.get("poverty_pct")):
            dc2.metric("Poverty %", f"{d['poverty_pct']:.1f}%")
        if pd.notna(d.get("unemployment_pct")):
            dc3.metric("Unemployment %", f"{d['unemployment_pct']:.1f}%")
        if pd.notna(d.get("pct_over_65")):
            dc4.metric("Over 65 %", f"{d['pct_over_65']:.1f}%")

    # EJ Indicators for county
    if _table_exists("ej_indicators"):
        ej_df = query_df(
            "SELECT * FROM ej_indicators WHERE fips_county = ? LIMIT 1", [fips]
        )
        if len(ej_df) > 0:
            st.subheader("Environmental Justice Indicators")
            ej = ej_df.iloc[0]
            ec1, ec2, ec3, ec4 = st.columns(4)
            if pd.notna(ej.get("ej_index_pctl")):
                ec1.metric("EJ Index Percentile", f"{ej['ej_index_pctl']:.1f}")
            if pd.notna(ej.get("pm25_pctl")):
                ec2.metric("PM2.5 Percentile", f"{ej['pm25_pctl']:.1f}")
            if pd.notna(ej.get("low_income_pctl")):
                ec3.metric("Low Income Pctl", f"{ej['low_income_pctl']:.1f}")
            if pd.notna(ej.get("people_of_color_pctl")):
                ec4.metric("POC Pctl", f"{ej['people_of_color_pctl']:.1f}")

    # Chemicals released
    st.subheader("Top Chemicals Released in County")
    chem_df = query_df("""
        SELECT r.chemical_name, r.carcinogen_flag,
               SUM(r.total_releases_lbs) as total_lbs,
               COUNT(DISTINCT r.tri_facility_id) as facility_count
        FROM tri_releases r
        JOIN tri_facilities f ON r.tri_facility_id = f.tri_facility_id
        WHERE f.fips_county = ?
        GROUP BY r.chemical_name
        ORDER BY total_lbs DESC
        LIMIT 20
    """, [fips])
    if len(chem_df) > 0:
        fig = px.bar(chem_df, x="total_lbs", y="chemical_name", orientation="h",
                     color="carcinogen_flag",
                     color_discrete_map={"YES": "#e74c3c", "NO": "#3498db"})
        fig.update_layout(template="plotly_dark", height=400, yaxis=dict(autorange="reversed"))
        st.plotly_chart(fig, use_container_width=True)


# ---- ENFORCEMENT DASHBOARD ----

def render_enforcement():
    st.title("Enforcement Dashboard")
    st.markdown("EPA enforcement actions, penalties, and compliance across TRI facilities.")

    enf_tables = ["enforcement_actions", "facility_inspections", "tri_frs_links"]
    if not all(_table_exists(t) for t in enf_tables):
        st.info("No enforcement data available. Run `python -m src.cli echo` to download.")
        return

    # KPI cards
    stats_df = query_df("""
        SELECT
            (SELECT COUNT(*) FROM enforcement_actions) as total_actions,
            (SELECT COALESCE(SUM(penalty_amount), 0) FROM enforcement_actions) as total_penalties,
            (SELECT COUNT(*) FROM enforcement_actions WHERE penalty_amount > 0) as with_penalties,
            (SELECT MAX(penalty_amount) FROM enforcement_actions) as max_penalty,
            (SELECT COUNT(*) FROM facility_inspections) as total_inspections,
            (SELECT SUM(CASE WHEN found_violation = 1 THEN 1 ELSE 0 END) FROM facility_inspections) as violation_inspections
    """)

    if not stats_df.empty:
        s = stats_df.iloc[0]
        c1, c2, c3 = st.columns(3)
        c1.metric("Enforcement Actions", f"{int(s['total_actions'] or 0):,}")
        c2.metric("Total Penalties", f"${(s['total_penalties'] or 0):,.0f}")
        c3.metric("Max Single Penalty", f"${(s['max_penalty'] or 0):,.0f}")

        c4, c5, c6 = st.columns(3)
        c4.metric("Actions with Penalties", f"{int(s['with_penalties'] or 0):,}")
        c5.metric("Total Inspections", f"{int(s['total_inspections'] or 0):,}")
        c6.metric("Inspections w/ Violations", f"{int(s['violation_inspections'] or 0):,}")

    st.markdown("---")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Enforcement by Program Type")
        type_df = query_df("""
            SELECT COALESCE(enforcement_type, 'Unknown') as program,
                   COUNT(*) as actions,
                   COALESCE(SUM(penalty_amount), 0) as total_penalty
            FROM enforcement_actions
            GROUP BY enforcement_type
            ORDER BY total_penalty DESC
        """)
        if not type_df.empty:
            fig = px.bar(type_df, x="program", y="total_penalty", color="actions",
                         text="actions", color_continuous_scale="Reds")
            fig.update_layout(template="plotly_dark", height=400)
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Top Penalties")
        top_df = query_df("""
            SELECT ea.case_name, ea.enforcement_type, ea.penalty_amount,
                   ea.settlement_date, ea.enforcement_outcome
            FROM enforcement_actions ea
            WHERE ea.penalty_amount > 0
            ORDER BY ea.penalty_amount DESC
            LIMIT 20
        """)
        if not top_df.empty:
            st.dataframe(top_df, use_container_width=True)

    # Enforcement by state (via FRS links)
    st.subheader("Enforcement Actions by State")
    state_enf = query_df("""
        SELECT state, COUNT(*) as actions,
               COALESCE(SUM(penalty_amount), 0) as total_penalty
        FROM (
            SELECT DISTINCT ea.case_number, ea.penalty_amount, f.state
            FROM enforcement_actions ea
            JOIN tri_frs_links tfl ON ea.registry_id = tfl.registry_id
            JOIN tri_facilities f ON tfl.tri_facility_id = f.tri_facility_id
        )
        GROUP BY state
        ORDER BY total_penalty DESC
        LIMIT 20
    """)
    if not state_enf.empty:
        fig = px.bar(state_enf, x="state", y="total_penalty", color="actions",
                     color_continuous_scale="YlOrRd")
        fig.update_layout(template="plotly_dark", height=400)
        st.plotly_chart(fig, use_container_width=True)


# ---- COMPLIANCE TRACKER ----

def render_compliance():
    st.title("Compliance Tracker")
    st.markdown("Track significant non-compliance across TRI facilities.")

    if not _table_exists("compliance_status"):
        st.info("No compliance data available. Run `python -m src.cli echo` to download.")
        return

    # Overview
    comp_df = query_df("""
        SELECT status, COUNT(*) as count
        FROM compliance_status
        GROUP BY status
        ORDER BY count DESC
    """)
    if not comp_df.empty:
        c1, c2 = st.columns(2)
        with c1:
            st.subheader("Compliance Status Distribution")
            fig = px.pie(comp_df, names="status", values="count", hole=0.4,
                         color_discrete_map={
                             "In Compliance": "#2ecc71",
                             "Violation": "#f39c12",
                             "Significant Non-Compliance": "#e74c3c",
                         })
            fig.update_layout(template="plotly_dark", height=400)
            st.plotly_chart(fig, use_container_width=True)

        with c2:
            st.subheader("Compliance by Program")
            prog_df = query_df("""
                SELECT program,
                       COUNT(*) as total,
                       SUM(CASE WHEN status = 'Significant Non-Compliance' THEN 1 ELSE 0 END) as snc,
                       SUM(CASE WHEN status = 'Violation' THEN 1 ELSE 0 END) as violations
                FROM compliance_status
                GROUP BY program
                ORDER BY snc DESC
            """)
            if not prog_df.empty:
                st.dataframe(prog_df, use_container_width=True)

    # Worst offenders
    st.subheader("Facilities with Most Violation Events")
    worst_df = query_df("""
        SELECT f.facility_name, f.state, f.industry_sector,
               cs.program, cs.status, cs.quarters_in_nc as violation_event_count
        FROM compliance_status cs
        JOIN tri_frs_links tfl ON cs.registry_id = tfl.registry_id
        JOIN tri_facilities f ON tfl.tri_facility_id = f.tri_facility_id
        WHERE cs.quarters_in_nc > 0
        ORDER BY cs.quarters_in_nc DESC
        LIMIT 50
    """)
    if not worst_df.empty:
        st.dataframe(worst_df, use_container_width=True)


# ---- CHEMICAL ANALYSIS ----

def render_chemical_analysis():
    st.title("Chemical Analysis")

    if not _table_exists("tri_releases"):
        st.warning("No data loaded. Run the pipeline first: `python -m src.cli pipeline`")
        return

    stats_df = query_df("""
        SELECT
            COUNT(DISTINCT chemical_name) as unique_chemicals,
            SUM(total_releases_lbs) as total_lbs,
            SUM(CASE WHEN carcinogen_flag = 'YES' THEN total_releases_lbs ELSE 0 END) as carcinogen_lbs,
            COUNT(CASE WHEN carcinogen_flag = 'YES' THEN 1 END) as carcinogen_records
        FROM tri_releases
    """)

    if stats_df.empty:
        st.warning("No release data found.")
        return

    stats = stats_df.iloc[0]

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Unique Chemicals", int(stats["unique_chemicals"] or 0))
    c2.metric("Total Released (lbs)", f"{(stats['total_lbs'] or 0):,.0f}")
    c3.metric("Carcinogen Released (lbs)", f"{(stats['carcinogen_lbs'] or 0):,.0f}")
    c4.metric("Carcinogen Records", f"{int(stats['carcinogen_records'] or 0):,}")

    st.markdown("---")

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Top 20 Chemicals by Volume")
        top_chem = query_df("""
            SELECT chemical_name, carcinogen_flag,
                   SUM(total_releases_lbs) as total_lbs,
                   COUNT(DISTINCT tri_facility_id) as facilities
            FROM tri_releases
            GROUP BY chemical_name
            ORDER BY total_lbs DESC LIMIT 20
        """)
        if not top_chem.empty:
            fig = px.bar(top_chem, x="total_lbs", y="chemical_name", orientation="h",
                         color="carcinogen_flag",
                         color_discrete_map={"YES": "#e74c3c", "NO": "#3498db"})
            fig.update_layout(template="plotly_dark", height=500, yaxis=dict(autorange="reversed"))
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Top 20 Carcinogens by Volume")
        top_carc = query_df("""
            SELECT chemical_name,
                   SUM(total_releases_lbs) as total_lbs,
                   COUNT(DISTINCT tri_facility_id) as facilities
            FROM tri_releases
            WHERE carcinogen_flag = 'YES'
            GROUP BY chemical_name
            ORDER BY total_lbs DESC LIMIT 20
        """)
        if not top_carc.empty:
            fig = px.bar(top_carc, x="total_lbs", y="chemical_name", orientation="h",
                         color="total_lbs", color_continuous_scale="Reds")
            fig.update_layout(template="plotly_dark", height=500, yaxis=dict(autorange="reversed"))
            st.plotly_chart(fig, use_container_width=True)

    st.subheader("Release Pathways")
    pathway_df = query_df("""
        SELECT
            COALESCE(SUM(fugitive_air_lbs), 0) as 'Fugitive Air',
            COALESCE(SUM(stack_air_lbs), 0) as 'Stack Air',
            COALESCE(SUM(water_lbs), 0) as 'Water',
            COALESCE(SUM(land_lbs), 0) as 'Land',
            COALESCE(SUM(underground_injection_lbs), 0) as 'Underground Injection',
            COALESCE(SUM(off_site_release_total), 0) as 'Off-site Releases'
        FROM tri_releases
    """)
    if not pathway_df.empty:
        pathways = pathway_df.iloc[0].to_dict()
        fig = px.pie(names=list(pathways.keys()), values=list(pathways.values()), hole=0.4)
        fig.update_layout(template="plotly_dark", height=400)
        st.plotly_chart(fig, use_container_width=True)


# ---- INDUSTRY ANALYSIS ----

def render_industry_analysis():
    st.title("Industry Sector Analysis")

    if not _table_exists("tri_facilities"):
        st.warning("No data loaded. Run the pipeline first: `python -m src.cli pipeline`")
        return

    ind_df = query_df("""
        SELECT COALESCE(f.industry_sector, 'Unknown') as sector,
               COUNT(DISTINCT f.tri_facility_id) as facilities,
               COALESCE(SUM(r.total_releases_lbs), 0) as total_lbs,
               COUNT(DISTINCT r.chemical_name) as chemicals,
               COUNT(CASE WHEN r.carcinogen_flag = 'YES' THEN 1 END) as carcinogen_records
        FROM tri_facilities f
        LEFT JOIN tri_releases r ON f.tri_facility_id = r.tri_facility_id
        GROUP BY sector
        ORDER BY total_lbs DESC
    """)

    if ind_df.empty:
        st.warning("No industry data found.")
        return

    st.dataframe(ind_df, use_container_width=True)

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Release Volume by Sector")
        fig = px.bar(ind_df, x="sector", y="total_lbs", color="total_lbs",
                     color_continuous_scale="Reds")
        fig.update_layout(template="plotly_dark", height=400, xaxis_tickangle=45)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Facilities by Sector")
        fig = px.bar(ind_df, x="sector", y="facilities", color="facilities",
                     color_continuous_scale="Blues")
        fig.update_layout(template="plotly_dark", height=400, xaxis_tickangle=45)
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("Average Releases per Facility by Sector")
    ind_df["avg_lbs"] = ind_df["total_lbs"] / ind_df["facilities"].replace(0, 1)
    fig = px.bar(ind_df.sort_values("avg_lbs", ascending=False),
                 x="sector", y="avg_lbs", color="avg_lbs",
                 color_continuous_scale="YlOrRd")
    fig.update_layout(template="plotly_dark", height=400, xaxis_tickangle=45)
    st.plotly_chart(fig, use_container_width=True)


# ---- ENVIRONMENTAL JUSTICE ----

def render_ej_analysis():
    st.title("Environmental Justice Analysis")
    st.markdown("EPA EJScreen indicators overlay on TRI facility pollution data. "
                "EJ Index shown is the PM2.5-based EJ index percentile from EJScreen.")

    if not _table_exists("ej_indicators"):
        st.info("No EJ data available. Run `python -m src.cli ejscreen` to download.")
        return

    ej_count = query_df("SELECT COUNT(*) as cnt FROM ej_indicators")
    if ej_count.empty or ej_count.iloc[0]["cnt"] == 0:
        st.info("EJ indicators table is empty. Run `python -m src.cli ejscreen` to populate.")
        return

    # Counties with highest EJ burden + most pollution
    # NOTE: Use subqueries to avoid inflating release totals via many-to-many EJ tract join
    st.subheader("Counties with Highest Environmental Justice Burden")
    ej_df = query_df("""
        SELECT ej_agg.fips_county, ej_agg.state, ej_agg.ej_index_pctl,
               ej_agg.pm25_pctl, ej_agg.low_income_pctl, ej_agg.people_of_color_pctl,
               COALESCE(county_stats.facilities, 0) as facilities,
               COALESCE(county_stats.total_lbs, 0) as total_lbs
        FROM (
            SELECT fips_county, state,
                   AVG(ej_index_pctl) as ej_index_pctl,
                   AVG(pm25_pctl) as pm25_pctl,
                   AVG(low_income_pctl) as low_income_pctl,
                   AVG(people_of_color_pctl) as people_of_color_pctl
            FROM ej_indicators
            WHERE ej_index_pctl IS NOT NULL
            GROUP BY fips_county
        ) ej_agg
        LEFT JOIN (
            SELECT f.fips_county,
                   COUNT(DISTINCT f.tri_facility_id) as facilities,
                   COALESCE(SUM(r.total_releases_lbs), 0) as total_lbs
            FROM tri_facilities f
            LEFT JOIN tri_releases r ON f.tri_facility_id = r.tri_facility_id
            WHERE f.fips_county IS NOT NULL
            GROUP BY f.fips_county
        ) county_stats ON ej_agg.fips_county = county_stats.fips_county
        WHERE county_stats.facilities > 0
        ORDER BY ej_agg.ej_index_pctl DESC
        LIMIT 50
    """)

    if not ej_df.empty:
        st.dataframe(ej_df, use_container_width=True)

        col1, col2 = st.columns(2)
        with col1:
            st.subheader("EJ Index vs Total Releases")
            fig = px.scatter(ej_df, x="ej_index_pctl", y="total_lbs",
                             size="facilities", color="ej_index_pctl",
                             color_continuous_scale="RdYlGn_r",
                             hover_data=["fips_county", "state"])
            fig.update_layout(template="plotly_dark", height=400)
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.subheader("Low Income vs People of Color %")
            ej_filtered = ej_df.dropna(subset=["low_income_pctl", "people_of_color_pctl"])
            if not ej_filtered.empty:
                fig = px.scatter(ej_filtered, x="low_income_pctl", y="people_of_color_pctl",
                                 size="total_lbs", color="facilities",
                                 color_continuous_scale="Viridis",
                                 hover_data=["fips_county", "state"])
                fig.update_layout(template="plotly_dark", height=400)
                st.plotly_chart(fig, use_container_width=True)


# ---- SUPERFUND PROXIMITY ----

def render_superfund():
    st.title("Superfund/NPL Site Proximity")
    st.markdown("TRI facilities near legacy contamination Superfund sites.")

    if not _table_exists("superfund_sites") or not _table_exists("tri_superfund_proximity"):
        st.info("No Superfund data available. Run `python -m src.cli superfund` to download.")
        return

    sf_count = query_df("SELECT COUNT(*) as cnt FROM superfund_sites")
    prox_count = query_df("SELECT COUNT(*) as cnt FROM tri_superfund_proximity")

    c1, c2, c3 = st.columns(3)
    c1.metric("Superfund NPL Sites", int(sf_count.iloc[0]["cnt"] or 0))
    c2.metric("Proximity Records", int(prox_count.iloc[0]["cnt"] or 0))

    fac_near = query_df("SELECT COUNT(DISTINCT tri_facility_id) as cnt FROM tri_superfund_proximity")
    c3.metric("Facilities Near Superfund", int(fac_near.iloc[0]["cnt"] or 0))

    st.markdown("---")

    # Closest facilities to superfund sites
    st.subheader("TRI Facilities Closest to Superfund Sites")
    prox_df = query_df("""
        SELECT f.facility_name, f.state, f.industry_sector,
               ss.site_name, ss.npl_status,
               tsp.distance_miles,
               COALESCE(rel.total_lbs, 0) as total_lbs
        FROM tri_superfund_proximity tsp
        JOIN tri_facilities f ON tsp.tri_facility_id = f.tri_facility_id
        JOIN superfund_sites ss ON tsp.site_id = ss.site_id
        LEFT JOIN (
            SELECT tri_facility_id, SUM(total_releases_lbs) as total_lbs
            FROM tri_releases GROUP BY tri_facility_id
        ) rel ON f.tri_facility_id = rel.tri_facility_id
        ORDER BY tsp.distance_miles
        LIMIT 100
    """)
    if not prox_df.empty:
        st.dataframe(prox_df, use_container_width=True)

        st.subheader("Distance Distribution")
        fig = px.histogram(prox_df, x="distance_miles", nbins=20,
                           color_discrete_sequence=["#e74c3c"])
        fig.update_layout(template="plotly_dark", height=300)
        st.plotly_chart(fig, use_container_width=True)

    # States with most facilities near superfund
    st.subheader("States with Most Facilities Near Superfund Sites")
    state_sf = query_df("""
        SELECT f.state, COUNT(DISTINCT f.tri_facility_id) as facilities,
               COUNT(DISTINCT tsp.site_id) as nearby_sites
        FROM tri_superfund_proximity tsp
        JOIN tri_facilities f ON tsp.tri_facility_id = f.tri_facility_id
        GROUP BY f.state
        ORDER BY facilities DESC
        LIMIT 20
    """)
    if not state_sf.empty:
        fig = px.bar(state_sf, x="state", y="facilities", color="nearby_sites",
                     color_continuous_scale="Reds")
        fig.update_layout(template="plotly_dark", height=400)
        st.plotly_chart(fig, use_container_width=True)


# ---- CHEMICAL ACCIDENT RISK (RMP) ----

def render_rmp():
    st.title("Chemical Accident Risk (RMP)")
    st.markdown(
        "EPA Risk Management Program data: facilities handling hazardous chemicals, "
        "worst-case scenarios, and historical accident records."
    )

    rmp_tables = ["rmp_facilities", "rmp_chemicals", "rmp_accidents", "tri_rmp_links"]
    if not all(_table_exists(t) for t in rmp_tables):
        st.info("No RMP data available. Run `python -m src.cli rmp` to download.")
        return

    rmp_count = query_df("SELECT COUNT(*) as cnt FROM rmp_facilities")
    if rmp_count.empty or rmp_count.iloc[0]["cnt"] == 0:
        st.info("RMP facilities table is empty. Run `python -m src.cli rmp` to populate.")
        return

    # KPI cards
    stats_df = query_df("""
        SELECT
            (SELECT COUNT(*) FROM rmp_facilities) as total_rmp,
            (SELECT COUNT(*) FROM rmp_chemicals) as total_chemicals,
            (SELECT COUNT(*) FROM rmp_accidents) as total_accidents,
            (SELECT COUNT(*) FROM tri_rmp_links) as total_links,
            (SELECT COALESCE(SUM(deaths_workers + deaths_public), 0) FROM rmp_accidents) as total_deaths,
            (SELECT COALESCE(SUM(injuries_workers + injuries_public), 0) FROM rmp_accidents) as total_injuries,
            (SELECT COALESCE(SUM(property_damage_usd), 0) FROM rmp_accidents) as total_damage
    """)

    if not stats_df.empty:
        s = stats_df.iloc[0]
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("RMP Facilities", f"{int(s['total_rmp'] or 0):,}")
        c2.metric("Regulated Chemicals", f"{int(s['total_chemicals'] or 0):,}")
        c3.metric("Historical Accidents", f"{int(s['total_accidents'] or 0):,}")
        c4.metric("TRI-RMP Cross-Links", f"{int(s['total_links'] or 0):,}")

        c5, c6, c7 = st.columns(3)
        c5.metric("Total Deaths", f"{int(s['total_deaths'] or 0):,}")
        c6.metric("Total Injuries", f"{int(s['total_injuries'] or 0):,}")
        c7.metric("Property Damage ($)", f"${(s['total_damage'] or 0):,.0f}")

    st.markdown("---")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Top Chemicals by Facility Count")
        chem_df = query_df("""
            SELECT chemical_name, COUNT(DISTINCT rmp_id) as facilities,
                   COALESCE(SUM(quantity_lbs), 0) as total_quantity_lbs,
                   SUM(is_toxic) as toxic_count, SUM(is_flammable) as flammable_count
            FROM rmp_chemicals
            GROUP BY chemical_name
            ORDER BY facilities DESC
            LIMIT 20
        """)
        if not chem_df.empty:
            fig = px.bar(chem_df, x="facilities", y="chemical_name", orientation="h",
                         color="total_quantity_lbs", color_continuous_scale="YlOrRd")
            fig.update_layout(template="plotly_dark", height=500, yaxis=dict(autorange="reversed"))
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Worst Accidents by Impact")
        acc_df = query_df("""
            SELECT a.accident_date, a.chemical_name,
                   rf.facility_name, rf.state,
                   (a.deaths_workers + a.deaths_public) as deaths,
                   (a.injuries_workers + a.injuries_public) as injuries,
                   a.evacuations, a.property_damage_usd
            FROM rmp_accidents a
            JOIN rmp_facilities rf ON a.rmp_id = rf.rmp_id
            ORDER BY deaths DESC, injuries DESC, a.property_damage_usd DESC
            LIMIT 20
        """)
        if not acc_df.empty:
            st.dataframe(acc_df, use_container_width=True)

    # RMP facilities by state
    st.subheader("RMP Facilities by State")
    state_df = query_df("""
        SELECT state, COUNT(*) as facilities,
               COUNT(DISTINCT (SELECT chemical_name FROM rmp_chemicals WHERE rmp_chemicals.rmp_id = rmp_facilities.rmp_id LIMIT 1)) as unique_chems
        FROM rmp_facilities
        WHERE state IS NOT NULL
        GROUP BY state
        ORDER BY facilities DESC
        LIMIT 25
    """)
    if not state_df.empty:
        fig = px.bar(state_df, x="state", y="facilities", color="facilities",
                     color_continuous_scale="Blues")
        fig.update_layout(template="plotly_dark", height=400)
        st.plotly_chart(fig, use_container_width=True)

    # Worst-case scenario analysis
    st.subheader("Worst-Case Scenario Distances")
    wc_df = query_df("""
        SELECT chemical_name,
               AVG(worst_case_distance_miles) as avg_worst_case_mi,
               MAX(worst_case_distance_miles) as max_worst_case_mi,
               COUNT(*) as scenarios
        FROM rmp_chemicals
        WHERE worst_case_distance_miles IS NOT NULL AND worst_case_distance_miles > 0
        GROUP BY chemical_name
        ORDER BY avg_worst_case_mi DESC
        LIMIT 20
    """)
    if not wc_df.empty:
        fig = px.bar(wc_df, x="avg_worst_case_mi", y="chemical_name", orientation="h",
                     color="max_worst_case_mi", color_continuous_scale="Reds",
                     hover_data=["scenarios", "max_worst_case_mi"])
        fig.update_layout(template="plotly_dark", height=500,
                         yaxis=dict(autorange="reversed"),
                         xaxis_title="Average Worst-Case Distance (miles)")
        st.plotly_chart(fig, use_container_width=True)


# ---- DUAL RISK ANALYSIS ----

def render_dual_risk():
    st.title("Dual Risk Analysis: TRI Chronic + RMP Acute")
    st.markdown(
        "Facilities that appear in both TRI (chronic toxic releases) and RMP "
        "(acute chemical accident risk) — the highest concern facilities."
    )

    if not _table_exists("tri_rmp_links"):
        st.info("No TRI-RMP cross-links available. Run `python -m src.cli rmp` to build.")
        return

    link_count = query_df("SELECT COUNT(*) as cnt FROM tri_rmp_links")
    if link_count.empty or link_count.iloc[0]["cnt"] == 0:
        st.info("No TRI-RMP cross-links found. Run `python -m src.cli rmp` to build links.")
        return

    # KPIs
    stats_df = query_df("""
        SELECT
            COUNT(DISTINCT tri_facility_id) as linked_tri,
            COUNT(DISTINCT rmp_id) as linked_rmp,
            COUNT(*) as total_links,
            SUM(CASE WHEN link_method = 'frs_registry' THEN 1 ELSE 0 END) as frs_links,
            SUM(CASE WHEN link_method = 'address_match' THEN 1 ELSE 0 END) as addr_links,
            SUM(CASE WHEN link_method = 'name_match' THEN 1 ELSE 0 END) as name_links
        FROM tri_rmp_links
    """)

    if not stats_df.empty:
        s = stats_df.iloc[0]
        c1, c2, c3 = st.columns(3)
        c1.metric("TRI Facilities with RMP", f"{int(s['linked_tri'] or 0):,}")
        c2.metric("RMP Facilities Linked", f"{int(s['linked_rmp'] or 0):,}")
        c3.metric("Total Cross-Links", f"{int(s['total_links'] or 0):,}")

        c4, c5, c6 = st.columns(3)
        c4.metric("FRS Registry Links", f"{int(s['frs_links'] or 0):,}")
        c5.metric("Address Match Links", f"{int(s['addr_links'] or 0):,}")
        c6.metric("Name Match Links", f"{int(s['name_links'] or 0):,}")

    st.markdown("---")

    # Dual-risk facilities
    st.subheader("Highest-Concern Dual Risk Facilities")
    dual_df = query_df("""
        SELECT f.facility_name, f.state, f.industry_sector,
               COALESCE(rel.total_lbs, 0) as chronic_releases_lbs,
               COALESCE(rel.carcinogen_count, 0) as carcinogen_chemicals,
               COALESCE(rmp_stats.chemicals, 0) as rmp_chemicals,
               COALESCE(rmp_stats.accidents, 0) as rmp_accidents,
               COALESCE(rmp_stats.max_worst_case_mi, 0) as worst_case_miles,
               lnk.link_method, lnk.confidence
        FROM tri_rmp_links lnk
        JOIN tri_facilities f ON lnk.tri_facility_id = f.tri_facility_id
        LEFT JOIN (
            SELECT tri_facility_id,
                   SUM(total_releases_lbs) as total_lbs,
                   COUNT(CASE WHEN carcinogen_flag = 'YES' THEN 1 END) as carcinogen_count
            FROM tri_releases GROUP BY tri_facility_id
        ) rel ON f.tri_facility_id = rel.tri_facility_id
        LEFT JOIN (
            SELECT rmp_id,
                   COUNT(DISTINCT chemical_name) as chemicals,
                   (SELECT COUNT(*) FROM rmp_accidents ra WHERE ra.rmp_id = rmp_chemicals.rmp_id) as accidents,
                   MAX(worst_case_distance_miles) as max_worst_case_mi
            FROM rmp_chemicals GROUP BY rmp_id
        ) rmp_stats ON lnk.rmp_id = rmp_stats.rmp_id
        ORDER BY chronic_releases_lbs DESC
        LIMIT 100
    """)
    if not dual_df.empty:
        st.dataframe(dual_df, use_container_width=True)

        col1, col2 = st.columns(2)
        with col1:
            st.subheader("Chronic Releases vs Acute Worst-Case")
            fig = px.scatter(
                dual_df[dual_df["worst_case_miles"] > 0],
                x="chronic_releases_lbs", y="worst_case_miles",
                color="rmp_accidents", size="rmp_chemicals",
                color_continuous_scale="YlOrRd",
                hover_data=["facility_name", "state"],
            )
            fig.update_layout(template="plotly_dark", height=400,
                             xaxis_title="Chronic TRI Releases (lbs)",
                             yaxis_title="Worst-Case Scenario Distance (mi)")
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.subheader("Cross-Link Method Distribution")
            method_df = query_df("""
                SELECT link_method, COUNT(*) as count
                FROM tri_rmp_links
                GROUP BY link_method
            """)
            if not method_df.empty:
                fig = px.pie(method_df, names="link_method", values="count", hole=0.4,
                             color_discrete_map={
                                 "frs_registry": "#2ecc71",
                                 "address_match": "#3498db",
                                 "name_match": "#f39c12",
                             })
                fig.update_layout(template="plotly_dark", height=400)
                st.plotly_chart(fig, use_container_width=True)

    # States with most dual-risk facilities
    st.subheader("States with Most Dual-Risk Facilities")
    state_dual = query_df("""
        SELECT f.state,
               COUNT(DISTINCT lnk.tri_facility_id) as dual_risk_facilities
        FROM tri_rmp_links lnk
        JOIN tri_facilities f ON lnk.tri_facility_id = f.tri_facility_id
        GROUP BY f.state
        ORDER BY dual_risk_facilities DESC
        LIMIT 20
    """)
    if not state_dual.empty:
        fig = px.bar(state_dual, x="state", y="dual_risk_facilities",
                     color="dual_risk_facilities", color_continuous_scale="Reds")
        fig.update_layout(template="plotly_dark", height=400)
        st.plotly_chart(fig, use_container_width=True)


# ---- GEOGRAPHIC MAP ----

def render_map():
    st.title("Geographic Map")
    st.markdown("Facility locations sized by release volume.")

    if not _table_exists("tri_facilities"):
        st.warning("No data loaded. Run the pipeline first: `python -m src.cli pipeline`")
        return

    states = query_df("SELECT DISTINCT state FROM tri_facilities ORDER BY state")
    state_options = states["state"].tolist() if not states.empty else []
    selected = st.multiselect("Filter states", state_options, default=[])

    where = ""
    params = []
    if selected:
        placeholders = ",".join("?" * len(selected))
        where = f"WHERE f.state IN ({placeholders})"
        params = selected

    map_df = query_df(f"""
        SELECT f.facility_name, f.state, f.latitude, f.longitude,
               f.industry_sector, f.county,
               COALESCE(SUM(r.total_releases_lbs), 0) as total_lbs,
               COUNT(DISTINCT r.chemical_name) as chemicals
        FROM tri_facilities f
        LEFT JOIN tri_releases r ON f.tri_facility_id = r.tri_facility_id
        {where}
        GROUP BY f.tri_facility_id
        HAVING f.latitude IS NOT NULL AND f.longitude IS NOT NULL
        ORDER BY total_lbs DESC
        LIMIT 5000
    """, params)

    if len(map_df) == 0:
        st.warning("No facilities with coordinates found.")
        return

    map_df["size"] = map_df["total_lbs"].clip(lower=100).apply(lambda x: max(x ** 0.3, 2))

    fig = px.scatter_mapbox(
        map_df,
        lat="latitude",
        lon="longitude",
        size="size",
        color="total_lbs",
        color_continuous_scale="YlOrRd",
        hover_name="facility_name",
        hover_data={"state": True, "county": True, "total_lbs": ":,.0f",
                    "chemicals": True, "industry_sector": True,
                    "size": False, "latitude": False, "longitude": False},
        mapbox_style="carto-darkmatter",
        zoom=3,
        center={"lat": 39.5, "lon": -98.5},
        height=700,
    )
    fig.update_layout(template="plotly_dark", margin=dict(l=0, r=0, t=0, b=0))
    st.plotly_chart(fig, use_container_width=True)

    st.caption(f"Showing {len(map_df):,} facilities (max 5,000). Bubble size = release volume.")


# ---- TRENDS ----

def render_trends():
    st.title("Year-over-Year Trends")

    if not _table_exists("tri_releases"):
        st.warning("No data loaded. Run the pipeline first: `python -m src.cli pipeline`")
        return

    yearly = query_df("""
        SELECT reporting_year as year,
               COUNT(DISTINCT tri_facility_id) as facilities,
               COUNT(*) as release_records,
               COALESCE(SUM(total_releases_lbs), 0) as total_lbs,
               COALESCE(SUM(CASE WHEN carcinogen_flag = 'YES' THEN total_releases_lbs ELSE 0 END), 0) as carcinogen_lbs,
               COUNT(DISTINCT chemical_name) as chemicals
        FROM tri_releases
        GROUP BY reporting_year
        ORDER BY reporting_year
    """)

    if yearly.empty:
        st.warning("No release data found for trend analysis.")
        return

    if len(yearly) < 2:
        st.info("Need 2+ years of data for trend analysis. Currently have data for: " +
                ", ".join(str(y) for y in yearly["year"].tolist()))
        st.dataframe(yearly, use_container_width=True)
        return

    # Summary metrics
    years_covered = yearly["year"].tolist()
    st.markdown(f"**Data covers {len(years_covered)} years:** {min(years_covered)} - {max(years_covered)}")

    if len(yearly) >= 2:
        latest = yearly.iloc[-1]["total_lbs"]
        previous = yearly.iloc[-2]["total_lbs"]
        if previous > 0:
            change_pct = ((latest - previous) / previous) * 100
            st.metric("Year-over-Year Change", f"{change_pct:+.1f}%")

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Total Releases by Year")
        fig = px.bar(yearly, x="year", y="total_lbs", text="total_lbs")
        fig.update_traces(texttemplate="%{text:,.0f}", textposition="outside")
        fig.update_layout(template="plotly_dark", height=400)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Carcinogen Releases by Year")
        fig = px.bar(yearly, x="year", y="carcinogen_lbs", text="carcinogen_lbs",
                     color_discrete_sequence=["#e74c3c"])
        fig.update_traces(texttemplate="%{text:,.0f}", textposition="outside")
        fig.update_layout(template="plotly_dark", height=400)
        st.plotly_chart(fig, use_container_width=True)

    # Multi-year trend lines
    if len(yearly) >= 3:
        st.subheader("Release Trends Over Time")
        fig = px.line(yearly, x="year", y=["total_lbs", "carcinogen_lbs"],
                      markers=True)
        fig.update_layout(template="plotly_dark", height=400)
        st.plotly_chart(fig, use_container_width=True)

    # State-level trends
    st.subheader("Trends by State")
    state_yearly = query_df("""
        SELECT r.reporting_year as year, f.state,
               SUM(r.total_releases_lbs) as total_lbs
        FROM tri_releases r
        JOIN tri_facilities f ON r.tri_facility_id = f.tri_facility_id
        GROUP BY r.reporting_year, f.state
        ORDER BY r.reporting_year, total_lbs DESC
    """)

    if len(state_yearly) > 0:
        top_states = state_yearly.groupby("state")["total_lbs"].sum().nlargest(10).index.tolist()
        filtered = state_yearly[state_yearly["state"].isin(top_states)]
        fig = px.line(filtered, x="year", y="total_lbs", color="state",
                      markers=True)
        fig.update_layout(template="plotly_dark", height=400)
        st.plotly_chart(fig, use_container_width=True)


if __name__ == "__main__":
    main()
