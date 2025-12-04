import streamlit as st
import duckdb
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# Page config
st.set_page_config(
    page_title="Orphan Version Analysis - Rust Crates",
    page_icon="🔍",
    layout="wide"
)

# Connect to DuckDB
@st.cache_resource
def get_db():
    return duckdb.connect('data/crates.duckdb', read_only=True)

con = get_db()

# Title and description
st.title("🔍 Orphan Version Analysis Dashboard")
st.markdown("""
This dashboard analyzes **orphan version IDs** - download records that reference versions no longer present in the `stg_versions` table.
These orphans may indicate deleted/yanked versions, data sync issues, or historical data inconsistencies.
""")
st.markdown("---")

# SUMMARY STATISTICS
st.header("📊 Summary Statistics")

col1, col2, col3, col4, col5 = st.columns(5)

with col1:
    total_crates = con.execute("SELECT COUNT(*) FROM staging.stg_crates").fetchone()[0]
    st.metric("Total Crates", f"{total_crates:,}")

with col2:
    total_versions = con.execute("SELECT COUNT(*) FROM staging.stg_versions").fetchone()[0]
    st.metric("Total Versions", f"{total_versions:,}")

with col3:
    orphan_versions_query = """
        SELECT COUNT(DISTINCT vd.version_id)
        FROM staging.stg_version_downloads vd
        LEFT JOIN staging.stg_versions v ON vd.version_id = v.id
        WHERE v.id IS NULL
    """
    total_orphan_versions = con.execute(orphan_versions_query).fetchone()[0]
    st.metric(
        "Total Orphan Versions",
        f"{total_orphan_versions:,}",
        delta=f"{(total_orphan_versions / total_versions * 100):.2f}% of versions" if total_versions > 0 else "0%",
        delta_color="inverse"
    )

with col4:
    total_downloads = con.execute("SELECT SUM(downloads) FROM staging.stg_version_downloads").fetchone()[0]
    st.metric("Total Downloads", f"{total_downloads/1e9:.2f}B")

with col5:
    orphan_downloads_query = """
        SELECT SUM(vd.downloads)
        FROM staging.stg_version_downloads vd
        LEFT JOIN staging.stg_versions v ON vd.version_id = v.id
        WHERE v.id IS NULL
    """
    total_orphan_downloads = con.execute(orphan_downloads_query).fetchone()[0]

    if total_orphan_downloads:
        orphan_pct = (total_orphan_downloads / total_downloads * 100) if total_downloads > 0 else 0
        st.metric(
            "Total Orphan Downloads",
            f"{total_orphan_downloads/1e6:.2f}M",
            delta=f"{orphan_pct:.3f}% of total",
            delta_color="inverse"
        )
    else:
        st.metric("Total Orphan Downloads", "0")

st.markdown("---")

# Detailed orphan metrics
st.header("🔢 Orphan Metrics Breakdown")

col1, col2, col3 = st.columns(3)

with col1:
    orphan_records_query = """
        SELECT COUNT(*)
        FROM staging.stg_version_downloads vd
        LEFT JOIN staging.stg_versions v ON vd.version_id = v.id
        WHERE v.id IS NULL
    """
    orphan_records = con.execute(orphan_records_query).fetchone()[0]
    total_records = con.execute("SELECT COUNT(*) FROM staging.stg_version_downloads").fetchone()[0]

    st.metric(
        "Orphan Download Records",
        f"{orphan_records:,}",
        delta=f"{(orphan_records / total_records * 100):.2f}% of records" if total_records > 0 else "0%",
        delta_color="inverse"
    )

with col2:
    date_range_query = """
        SELECT
            MIN(vd.date) as earliest,
            MAX(vd.date) as latest
        FROM staging.stg_version_downloads vd
        LEFT JOIN staging.stg_versions v ON vd.version_id = v.id
        WHERE v.id IS NULL
    """
    earliest, latest = con.execute(date_range_query).fetchone()

    st.metric("Orphan Date Range", f"{(latest - earliest).days if earliest and latest else 0} days")
    if earliest and latest:
        st.caption(f"From {earliest} to {latest}")

with col3:
    avg_orphan_downloads_query = """
        SELECT AVG(vd.downloads)
        FROM staging.stg_version_downloads vd
        LEFT JOIN staging.stg_versions v ON vd.version_id = v.id
        WHERE v.id IS NULL
    """
    avg_orphan = con.execute(avg_orphan_downloads_query).fetchone()[0]
    avg_all = con.execute("SELECT AVG(downloads) FROM staging.stg_version_downloads").fetchone()[0]

    if avg_orphan:
        st.metric(
            "Avg Downloads per Orphan Record",
            f"{avg_orphan:.1f}",
            delta=f"{((avg_orphan - avg_all) / avg_all * 100):.1f}% vs all records" if avg_all > 0 else "N/A"
        )
    else:
        st.metric("Avg Downloads per Orphan Record", "0")

st.markdown("---")

# MONTHLY DOWNLOADS COMPARISON
st.header("📈 Monthly Downloads: Valid vs Orphan")

monthly_downloads_query = """
    SELECT
        DATE_TRUNC('month', vd.date) as month,
        SUM(CASE WHEN v.id IS NULL THEN vd.downloads ELSE 0 END) as orphan_downloads,
        SUM(CASE WHEN v.id IS NOT NULL THEN vd.downloads ELSE 0 END) as valid_downloads,
        SUM(vd.downloads) as total_downloads
    FROM staging.stg_version_downloads vd
    LEFT JOIN staging.stg_versions v ON vd.version_id = v.id
    GROUP BY month
    ORDER BY month
"""

df_monthly = con.execute(monthly_downloads_query).fetchdf()

if not df_monthly.empty:
    # Create two separate line charts
    fig_monthly = make_subplots(
        rows=2, cols=1,
        subplot_titles=('Valid Downloads per Month', 'Orphan Downloads per Month'),
        vertical_spacing=0.12,
        specs=[[{"type": "scatter"}], [{"type": "scatter"}]]
    )

    # Add valid downloads chart
    fig_monthly.add_trace(
        go.Scatter(
            x=df_monthly['month'],
            y=df_monthly['valid_downloads'],
            name='Valid Downloads',
            fill='tozeroy',
            line=dict(color='#2ecc71', width=2),
            hovertemplate='%{y:,.0f}<extra></extra>'
        ),
        row=1, col=1
    )

    # Add orphan downloads chart
    fig_monthly.add_trace(
        go.Scatter(
            x=df_monthly['month'],
            y=df_monthly['orphan_downloads'],
            name='Orphan Downloads',
            fill='tozeroy',
            line=dict(color='#e74c3c', width=2),
            hovertemplate='%{y:,.0f}<extra></extra>'
        ),
        row=2, col=1
    )

    fig_monthly.update_xaxes(title_text="Month", row=2, col=1)
    fig_monthly.update_yaxes(title_text="Downloads", row=1, col=1)
    fig_monthly.update_yaxes(title_text="Downloads", row=2, col=1)

    fig_monthly.update_layout(
        height=700,
        showlegend=True,
        hovermode='x unified'
    )

    st.plotly_chart(fig_monthly, use_container_width=True)

    with st.expander("📋 View Monthly Data Table"):
        df_monthly['orphan_pct'] = (df_monthly['orphan_downloads'] / df_monthly['total_downloads'] * 100).round(3)
        st.dataframe(df_monthly, use_container_width=True)
else:
    st.info("No monthly download data available.")

st.markdown("---")

# MONTHLY ORPHAN VERSION IDS
st.header("🆔 Monthly Distinct Orphan Version IDs")

monthly_orphan_ids_query = """
    SELECT
        DATE_TRUNC('month', vd.date) as month,
        COUNT(DISTINCT vd.version_id) as distinct_orphan_ids,
        COUNT(*) as orphan_records,
        SUM(vd.downloads) as orphan_downloads
    FROM staging.stg_version_downloads vd
    LEFT JOIN staging.stg_versions v ON vd.version_id = v.id
    WHERE v.id IS NULL
    GROUP BY month
    ORDER BY month
"""

df_monthly_orphans = con.execute(monthly_orphan_ids_query).fetchdf()

if not df_monthly_orphans.empty:
    # Create figure with secondary axis
    fig_orphan_ids = make_subplots(
        rows=2, cols=1,
        subplot_titles=('Distinct Orphan Version IDs per Month', 'Orphan Download Records per Month'),
        vertical_spacing=0.15,
        specs=[[{"secondary_y": False}], [{"secondary_y": False}]]
    )

    # Distinct IDs
    fig_orphan_ids.add_trace(
        go.Bar(
            x=df_monthly_orphans['month'],
            y=df_monthly_orphans['distinct_orphan_ids'],
            name='Distinct Orphan IDs',
            marker_color='#e67e22',
            hovertemplate='%{y:,.0f} orphan IDs'
        ),
        row=1, col=1
    )

    # Orphan records
    fig_orphan_ids.add_trace(
        go.Bar(
            x=df_monthly_orphans['month'],
            y=df_monthly_orphans['orphan_records'],
            name='Orphan Records',
            marker_color='#9b59b6',
            hovertemplate='%{y:,.0f} records'
        ),
        row=2, col=1
    )

    fig_orphan_ids.update_xaxes(title_text="Month", row=2, col=1)
    fig_orphan_ids.update_yaxes(title_text="Count", row=1, col=1)
    fig_orphan_ids.update_yaxes(title_text="Count", row=2, col=1)

    fig_orphan_ids.update_layout(
        height=700,
        showlegend=True,
        hovermode='x unified'
    )

    st.plotly_chart(fig_orphan_ids, use_container_width=True)

    # Summary metrics
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Peak Orphan IDs (Single Month)", f"{df_monthly_orphans['distinct_orphan_ids'].max():,}")
    with col2:
        st.metric("Avg Orphan IDs per Month", f"{df_monthly_orphans['distinct_orphan_ids'].mean():.0f}")
    with col3:
        avg_records_per_id = (df_monthly_orphans['orphan_records'].sum() / df_monthly_orphans['distinct_orphan_ids'].sum())
        st.metric("Avg Records per Orphan ID", f"{avg_records_per_id:.1f}")

    with st.expander("📋 View Orphan IDs Data Table"):
        st.dataframe(df_monthly_orphans, use_container_width=True)
else:
    st.info("No orphan version ID data available.")

st.markdown("---")

# TOP ORPHAN VERSION IDS
st.header("🔝 Top Orphan Version IDs by Downloads")

top_orphans_query = """
    SELECT
        vd.version_id,
        COUNT(*) as record_count,
        SUM(vd.downloads) as total_downloads,
        MIN(vd.date) as first_seen,
        MAX(vd.date) as last_seen,
        MAX(vd.date) - MIN(vd.date) as days_active
    FROM staging.stg_version_downloads vd
    LEFT JOIN staging.stg_versions v ON vd.version_id = v.id
    WHERE v.id IS NULL
    GROUP BY vd.version_id
    ORDER BY total_downloads DESC
    LIMIT 30
"""

df_top_orphans = con.execute(top_orphans_query).fetchdf()

if not df_top_orphans.empty:
    col1, col2 = st.columns([2, 1])

    with col1:
        fig_top_orphans = px.bar(
            df_top_orphans.head(20),
            x='version_id',
            y='total_downloads',
            title='Top 20 Orphan Version IDs by Total Downloads',
            labels={'version_id': 'Version ID', 'total_downloads': 'Total Downloads'},
            color='total_downloads',
            color_continuous_scale='Reds',
            hover_data=['record_count', 'days_active']
        )
        fig_top_orphans.update_layout(xaxis_tickangle=-45)
        st.plotly_chart(fig_top_orphans, use_container_width=True)

    with col2:
        st.subheader("Top 10 Details")
        for idx, row in df_top_orphans.head(10).iterrows():
            with st.container():
                st.markdown(f"**Version ID:** `{row['version_id']}`")
                # days_active is already an integer representing days
                days = int(row['days_active']) if row['days_active'] is not None else 0
                st.caption(f"Downloads: {row['total_downloads']:,} | Records: {row['record_count']} | Active: {days} days")
                st.divider()

    with st.expander("📋 View Top 30 Orphan Version IDs"):
        st.dataframe(df_top_orphans, use_container_width=True)
else:
    st.info("No orphan version data available.")

st.markdown("---")

# ORPHAN PERCENTAGE OVER TIME
st.header("📉 Orphan Rate Over Time")

orphan_rate_query = """
    SELECT
        DATE_TRUNC('month', vd.date) as month,
        COUNT(*) as total_records,
        SUM(CASE WHEN v.id IS NULL THEN 1 ELSE 0 END) as orphan_records,
        (SUM(CASE WHEN v.id IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) as orphan_percentage
    FROM staging.stg_version_downloads vd
    LEFT JOIN staging.stg_versions v ON vd.version_id = v.id
    GROUP BY month
    ORDER BY month
"""

df_orphan_rate = con.execute(orphan_rate_query).fetchdf()

if not df_orphan_rate.empty:
    fig_rate = go.Figure()

    fig_rate.add_trace(go.Scatter(
        x=df_orphan_rate['month'],
        y=df_orphan_rate['orphan_percentage'],
        mode='lines+markers',
        name='Orphan Rate',
        line=dict(color='#e74c3c', width=3),
        fill='tozeroy',
        fillcolor='rgba(231, 76, 60, 0.1)'
    ))

    fig_rate.update_layout(
        title='Orphan Rate (%) Over Time',
        xaxis_title='Month',
        yaxis_title='Orphan Percentage (%)',
        hovermode='x unified',
        height=400
    )

    st.plotly_chart(fig_rate, use_container_width=True)

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Current Month Orphan Rate", f"{df_orphan_rate.iloc[-1]['orphan_percentage']:.3f}%")
    with col2:
        st.metric("Average Orphan Rate", f"{df_orphan_rate['orphan_percentage'].mean():.3f}%")
    with col3:
        st.metric("Peak Orphan Rate", f"{df_orphan_rate['orphan_percentage'].max():.3f}%")
else:
    st.info("No orphan rate data available.")

st.markdown("---")

# YEARLY COMPARISON
st.header("📅 Yearly Orphan Comparison")

yearly_query = """
    SELECT
        EXTRACT(YEAR FROM vd.date) as year,
        COUNT(DISTINCT vd.version_id) as total_version_ids,
        SUM(CASE WHEN v.id IS NULL THEN 1 ELSE 0 END) as orphan_records,
        COUNT(DISTINCT CASE WHEN v.id IS NULL THEN vd.version_id END) as orphan_version_ids,
        SUM(CASE WHEN v.id IS NULL THEN vd.downloads ELSE 0 END) as orphan_downloads,
        SUM(vd.downloads) as total_downloads
    FROM staging.stg_version_downloads vd
    LEFT JOIN staging.stg_versions v ON vd.version_id = v.id
    GROUP BY year
    ORDER BY year
"""

df_yearly = con.execute(yearly_query).fetchdf()

if not df_yearly.empty:
    df_yearly['orphan_id_pct'] = (df_yearly['orphan_version_ids'] / df_yearly['total_version_ids'] * 100).round(2)
    df_yearly['orphan_download_pct'] = (df_yearly['orphan_downloads'] / df_yearly['total_downloads'] * 100).round(4)

    fig_yearly = px.bar(
        df_yearly,
        x='year',
        y=['orphan_version_ids', 'total_version_ids'],
        title='Orphan vs Total Version IDs by Year',
        labels={'value': 'Count', 'variable': 'Type'},
        barmode='group',
        color_discrete_map={'orphan_version_ids': '#e74c3c', 'total_version_ids': '#3498db'}
    )

    st.plotly_chart(fig_yearly, use_container_width=True)

    with st.expander("📋 View Yearly Breakdown"):
        st.dataframe(df_yearly, use_container_width=True)
else:
    st.info("No yearly data available.")

st.markdown("---")

# Footer
st.markdown("---")
st.markdown("""
**Data Source:** crates.io database dump | **Database:** DuckDB | **Framework:** Streamlit
""")