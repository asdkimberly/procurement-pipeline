"""
dashboard.py — Procurement Intelligence Pipeline
=================================================
Interactive Streamlit dashboard for exploring Peru Compras
public procurement data loaded into the SQLite database.

Pages:
    1. Overview    — KPIs (orders, spend, avg value, suppliers),
                     monthly spend trend, order status breakdown,
                     order size distribution by value tier
    2. Suppliers   — Top 15 suppliers by contract value, market
                     concentration analysis, full supplier table
    3. Entities    — Top 15 buying entities by spend, spend by
                     department, full entity table
    4. Products    — Processor brand share (AMD vs Intel), top 15
                     processor SKUs by order count, top 15 models
                     by spend, RAM and storage distribution
    5. Data Quality — Flag KPI cards, flag trend over time,
                      pipeline run log, anomaly explorer

Sidebar filters:
    Year, Quarter, Department, Framework Code,
    Product Category, Exclude cancelled, Exclude zero-value

Usage:
    streamlit run dashboard.py

Requirements:
    pip install streamlit pandas plotly

Author: Your Name
"""

import sqlite3
import pandas as pd
import plotly.express as px
import streamlit as st
from pathlib import Path

# ─── Configuration ────────────────────────────────────────────────────────────

DB_PATH   = Path("data/procurement.db")
APP_TITLE = "Peru Compras — Procurement Intelligence"

COLOR_PRIMARY   = "#C8102E"   # Peru flag red
COLOR_SECONDARY = "#D4AF37"   # Gold
COLOR_NEUTRAL   = "#2C3E50"   # Dark slate
COLOR_WARNING   = "#E67E22"
COLOR_LIGHT     = "#ECF0F1"

PLOTLY_TEMPLATE = "plotly_white"

# ─── Page Config ──────────────────────────────────────────────────────────────

st.set_page_config(
    page_title=APP_TITLE,
    page_icon="flag-pe",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─── Custom CSS ───────────────────────────────────────────────────────────────

st.markdown("""
<style>
    .block-container { padding-top: 1.5rem; }
    .kpi-card {
        background: white;
        border-radius: 10px;
        padding: 1.2rem 1.5rem;
        border-left: 5px solid #C8102E;
        box-shadow: 0 2px 8px rgba(0,0,0,0.07);
        margin-bottom: 0.5rem;
    }
    .kpi-label { font-size: 0.78rem; color: #888; text-transform: uppercase; letter-spacing: 0.05em; }
    .kpi-value { font-size: 1.9rem; font-weight: 700; color: #2C3E50; line-height: 1.2; }
    .kpi-sub   { font-size: 0.82rem; color: #aaa; margin-top: 2px; }
    .section-header {
        font-size: 1.05rem;
        font-weight: 600;
        color: #2C3E50;
        border-bottom: 2px solid #C8102E;
        padding-bottom: 4px;
        margin-bottom: 1rem;
        margin-top: 1rem;
    }
</style>
""", unsafe_allow_html=True)


# ─── Database Helpers ─────────────────────────────────────────────────────────

@st.cache_resource
def get_connection():
    """Cached SQLite connection shared across all queries."""
    if not DB_PATH.exists():
        st.error(
            f"Database not found at `{DB_PATH}`.\n\n"
            "Run the pipeline first:\n"
            "```\npython extract.py\npython transform.py\npython load.py\n```"
        )
        st.stop()
    return sqlite3.connect(DB_PATH, check_same_thread=False)


@st.cache_data(ttl=300)
def run_query(_conn, sql: str) -> pd.DataFrame:
    """Execute SQL and cache results for 5 minutes."""
    return pd.read_sql_query(sql, _conn)


def load_filtered(_conn, filters: dict) -> pd.DataFrame:
    """Load contracts table with sidebar filters applied."""
    clauses = ["1=1"]

    if filters.get("years"):
        years_str = ", ".join(str(y) for y in filters["years"])
        clauses.append(f"order_year IN ({years_str})")

    if filters.get("quarters"):
        quarters_str = ", ".join(str(q) for q in filters["quarters"])
        clauses.append(f"order_quarter IN ({quarters_str})")

    if filters.get("departments"):
        depts = "', '".join(filters["departments"])
        clauses.append(f"delivery_department IN ('{depts}')")

    if filters.get("frameworks"):
        fws = "', '".join(filters["frameworks"])
        clauses.append(f"framework_code IN ('{fws}')")

    if filters.get("categories"):
        cats = "', '".join(filters["categories"])
        clauses.append(f"product_category IN ('{cats}')")

    if filters.get("exclude_cancelled"):
        clauses.append("is_cancelled = 0")

    if filters.get("exclude_zero"):
        clauses.append("flag_zero_value = 0")

    where = " AND ".join(clauses)
    return run_query(_conn, f"SELECT * FROM contracts WHERE {where}")


# ─── UI Components ────────────────────────────────────────────────────────────

def kpi_card(label: str, value: str, sub: str = "") -> None:
    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-label">{label}</div>
        <div class="kpi-value">{value}</div>
        <div class="kpi-sub">{sub}</div>
    </div>
    """, unsafe_allow_html=True)


def section(title: str) -> None:
    st.markdown(f"<div class='section-header'>{title}</div>", unsafe_allow_html=True)


# ─── Sidebar ──────────────────────────────────────────────────────────────────

def render_sidebar(conn) -> dict:
    st.sidebar.title("Peru Compras")
    st.sidebar.caption("Procurement Intelligence Dashboard")
    st.sidebar.markdown("---")

    page = st.sidebar.radio(
        "Page",
        ["Overview", "Suppliers", "Entities", "Products", "Data Quality"],
        label_visibility="collapsed"
    )

    st.sidebar.markdown("### Filters")

    # Year
    years_df  = run_query(conn, "SELECT DISTINCT order_year FROM contracts WHERE order_year IS NOT NULL ORDER BY order_year")
    all_years = sorted(years_df["order_year"].dropna().astype(int).tolist())
    sel_years = st.sidebar.multiselect("Year", all_years, default=all_years)

    # Quarter — filtered to selected years
    years_str   = ", ".join(str(y) for y in (sel_years or all_years))
    quarter_df  = run_query(conn, f"SELECT DISTINCT order_quarter FROM contracts WHERE order_year IN ({years_str}) AND order_quarter IS NOT NULL ORDER BY order_quarter")
    all_quarters = sorted(quarter_df["order_quarter"].dropna().astype(int).tolist())
    quarter_labels = {1: "Q1", 2: "Q2", 3: "Q3", 4: "Q4"}
    sel_quarters = st.sidebar.multiselect(
        "Quarter",
        options=all_quarters,
        default=all_quarters,
        format_func=lambda q: quarter_labels.get(q, f"Q{q}")
    )

    # Department
    dept_df   = run_query(conn, "SELECT DISTINCT delivery_department FROM contracts WHERE delivery_department IS NOT NULL ORDER BY delivery_department")
    all_depts = dept_df["delivery_department"].tolist()
    sel_depts = st.sidebar.multiselect("Department", all_depts, default=[])

    # Framework code
    fw_df    = run_query(conn, "SELECT DISTINCT framework_code FROM contracts WHERE framework_code IS NOT NULL ORDER BY framework_code")
    all_fws  = fw_df["framework_code"].tolist()
    sel_fws  = st.sidebar.multiselect("Framework Code", all_fws, default=[])

    # Product category
    cat_df    = run_query(conn, "SELECT DISTINCT product_category FROM contracts WHERE product_category IS NOT NULL ORDER BY product_category")
    all_cats  = cat_df["product_category"].tolist()
    sel_cats  = st.sidebar.multiselect("Product Category", all_cats, default=[])

    excl_canc = st.sidebar.checkbox("Exclude cancelled orders", value=True)
    excl_zero = st.sidebar.checkbox("Exclude zero-value orders", value=True)

    st.sidebar.markdown("---")
    st.sidebar.caption("Data: Peru Compras Open Data Portal")

    return {
        "page":              page,
        "years":             sel_years   if sel_years   else all_years,
        "quarters":          sel_quarters if sel_quarters else all_quarters,
        "departments":       sel_depts   if sel_depts   else all_depts,
        "frameworks":        sel_fws     if sel_fws     else all_fws,
        "categories":        sel_cats    if sel_cats    else all_cats,
        "exclude_cancelled": excl_canc,
        "exclude_zero":      excl_zero,
    }


# ─── Page: Overview ───────────────────────────────────────────────────────────

def page_overview(df: pd.DataFrame) -> None:
    section("Key Metrics")

    c1, c2, c3, c4 = st.columns(4)
    with c1: kpi_card("Total Orders",       f"{len(df):,}",                           "purchase orders")
    with c2: kpi_card("Total Spend",        f"S/. {df['subtotal_pen'].sum():,.0f}",   "Peruvian Soles")
    with c3: kpi_card("Avg Order Value",    f"S/. {df['subtotal_pen'].mean():,.0f}",  "per order")
    with c4: kpi_card("Unique Suppliers",   f"{df['supplier_name'].nunique():,}",      "active vendors")

    st.markdown("<br>", unsafe_allow_html=True)
    col_l, col_r = st.columns([2, 1])

    with col_l:
        section("Monthly Spend Trend")
        if "order_ym" in df.columns and df["order_ym"].notna().any():
            monthly = (
                df.groupby("order_ym")
                .agg(total_spend=("subtotal_pen", "sum"))
                .reset_index().sort_values("order_ym")
            )
            fig = px.area(
                monthly, x="order_ym", y="total_spend",
                labels={"order_ym": "Month", "total_spend": "Spend (PEN)"},
                template=PLOTLY_TEMPLATE,
                color_discrete_sequence=[COLOR_PRIMARY],
            )
            fig.update_traces(line_color=COLOR_PRIMARY, fillcolor="rgba(200,16,46,0.12)")
            fig.update_layout(height=300, margin=dict(t=10, b=10))
            st.plotly_chart(fig, width='stretch')
        else:
            st.info("No date data available.")

    with col_r:
        section("Order Status")
        if "order_status" in df.columns:
            status_df = df["order_status"].value_counts().reset_index()
            status_df.columns = ["status", "count"]
            fig2 = px.pie(
                status_df, values="count", names="status",
                template=PLOTLY_TEMPLATE,
                hole=0.45,
                color_discrete_sequence=[COLOR_PRIMARY, COLOR_SECONDARY, COLOR_NEUTRAL, "#95A5A6"],
            )
            fig2.update_layout(height=300, margin=dict(t=10, b=10))
            st.plotly_chart(fig2, width='stretch')

    section("Order Size Distribution")
    if "value_tier" in df.columns:
        tier_order = ["micro", "small", "medium", "large", "mega"]
        tier_df = df["value_tier"].value_counts().reindex(tier_order).dropna().reset_index()
        tier_df.columns = ["tier", "count"]
        fig3 = px.bar(
            tier_df, x="tier", y="count",
            labels={"tier": "Value Tier", "count": "Orders"},
            template=PLOTLY_TEMPLATE,
            color="tier",
            color_discrete_sequence=[COLOR_LIGHT, COLOR_SECONDARY, COLOR_WARNING, COLOR_PRIMARY, COLOR_NEUTRAL],
        )
        fig3.update_layout(height=240, margin=dict(t=10, b=10), showlegend=False)
        st.plotly_chart(fig3, width='stretch')
        st.caption("micro < S/.10K  |  small S/.10K–100K  |  medium S/.100K–500K  |  large S/.500K–1M  |  mega > S/.1M")


# ─── Page: Suppliers ──────────────────────────────────────────────────────────

def page_suppliers(df: pd.DataFrame) -> None:
    section("Supplier Analysis")

    if df.empty or "supplier_name" not in df.columns:
        st.warning("No supplier data available.")
        return

    supplier_df = (
        df.groupby("supplier_name")
        .agg(order_count=("order_id", "count"), total_value=("subtotal_pen", "sum"), avg_value=("subtotal_pen", "mean"))
        .reset_index().sort_values("total_value", ascending=False)
    )

    col1, col2 = st.columns([3, 2])

    with col1:
        section("Top 15 Suppliers by Contract Value")
        top15 = supplier_df.head(15).sort_values("total_value")
        fig = px.bar(
            top15, x="total_value", y="supplier_name", orientation="h",
            labels={"total_value": "Total Value (PEN)", "supplier_name": ""},
            template=PLOTLY_TEMPLATE, color_discrete_sequence=[COLOR_PRIMARY],
        )
        fig.update_layout(height=450, margin=dict(t=10, b=10))
        st.plotly_chart(fig, width='stretch')

    with col2:
        section("Market Concentration")
        total   = supplier_df["total_value"].sum()
        top5    = supplier_df.head(5)["total_value"].sum()
        top5pct = top5 / total * 100 if total else 0
        kpi_card("Top 5 Suppliers", f"{top5pct:.1f}%", "of total procurement spend")
        st.markdown("<br>", unsafe_allow_html=True)

        conc_df = pd.DataFrame({
            "group": ["Top 5 Suppliers", "All Others"],
            "value": [top5, total - top5]
        })
        fig2 = px.pie(
            conc_df, values="value", names="group",
            template=PLOTLY_TEMPLATE, hole=0.5,
            color_discrete_sequence=[COLOR_PRIMARY, COLOR_LIGHT],
        )
        fig2.update_layout(height=250, margin=dict(t=10, b=10))
        st.plotly_chart(fig2, width='stretch')

    section("Full Supplier Table")
    disp = supplier_df.copy()
    disp["total_value"] = disp["total_value"].map("S/. {:,.0f}".format)
    disp["avg_value"]   = disp["avg_value"].map("S/. {:,.0f}".format)
    disp.columns        = ["Supplier", "Orders", "Total Value (PEN)", "Avg Order (PEN)"]
    st.dataframe(disp, width='stretch', height=300)


# ─── Page: Entities ───────────────────────────────────────────────────────────

def page_entities(df: pd.DataFrame) -> None:
    section("Buying Entity Analysis")

    if df.empty or "entity_name" not in df.columns:
        st.warning("No entity data available.")
        return

    entity_df = (
        df.groupby("entity_name")
        .agg(
            order_count=("order_id", "count"),
            total_spend=("subtotal_pen", "sum"),
            avg_value=("subtotal_pen", "mean"),
            n_suppliers=("supplier_name", "nunique"),
        )
        .reset_index().sort_values("total_spend", ascending=False)
    )

    col1, col2 = st.columns([3, 2])

    with col1:
        section("Top 15 Entities by Spend")
        top15 = entity_df.head(15).sort_values("total_spend")
        fig = px.bar(
            top15, x="total_spend", y="entity_name", orientation="h",
            labels={"total_spend": "Total Spend (PEN)", "entity_name": ""},
            template=PLOTLY_TEMPLATE, color_discrete_sequence=[COLOR_NEUTRAL],
        )
        fig.update_layout(height=450, margin=dict(t=10, b=10))
        st.plotly_chart(fig, width='stretch')

    with col2:
        section("Spend by Department")
        if "delivery_department" in df.columns:
            dept_df = (
                df.groupby("delivery_department")["subtotal_pen"]
                .sum().reset_index().sort_values("subtotal_pen", ascending=False).head(10)
            )
            fig2 = px.pie(
                dept_df, values="subtotal_pen", names="delivery_department",
                template=PLOTLY_TEMPLATE, hole=0.35,
                color_discrete_sequence=px.colors.qualitative.Set3,
            )
            fig2.update_layout(height=380, margin=dict(t=10, b=10))
            st.plotly_chart(fig2, width='stretch')

    section("Full Entity Table")
    disp = entity_df.copy()
    disp["total_spend"] = disp["total_spend"].map("S/. {:,.0f}".format)
    disp["avg_value"]   = disp["avg_value"].map("S/. {:,.0f}".format)
    disp.columns        = ["Entity", "Orders", "Total Spend (PEN)", "Avg Order (PEN)", "Unique Suppliers"]
    st.dataframe(disp, width='stretch', height=300)



# ─── Page: Products ───────────────────────────────────────────────────────────

def page_products(df: pd.DataFrame) -> None:
    section("Product & Spec Analysis")

    if df.empty:
        st.warning("No product data available.")
        return

    spec_df = df[df["spec_processor_brand"].notna()].copy()

    # ── Row 1: Processor brand share + Top processor SKUs ──
    col1, col2 = st.columns([1, 2])

    with col1:
        section("Processor Brand Share")
        brand_df = (
            spec_df.groupby("spec_processor_brand")
            .agg(order_count=("order_id", "count"), total_spend=("subtotal_pen", "sum"))
            .reset_index().sort_values("order_count", ascending=False)
        )
        fig = px.pie(
            brand_df, values="order_count", names="spec_processor_brand",
            template=PLOTLY_TEMPLATE, hole=0.45,
            color_discrete_sequence=[COLOR_PRIMARY, COLOR_SECONDARY, COLOR_NEUTRAL],
        )
        fig.update_layout(height=300, margin=dict(t=10, b=10))
        st.plotly_chart(fig, width='stretch')

        # Brand KPIs
        for _, row in brand_df.iterrows():
            pct = row["order_count"] / brand_df["order_count"].sum() * 100
            kpi_card(
                row["spec_processor_brand"],
                f"{row['order_count']:,} orders",
                f"S/. {row['total_spend']:,.0f}  ·  {pct:.1f}% of orders"
            )

    with col2:
        section("Top 15 Processor SKUs by Unit Count")
        sku_df = (
            spec_df.groupby(["spec_processor_brand", "spec_processor_sku"])
            .agg(unit_count=("delivery_quantity", "sum"), total_spend=("subtotal_pen", "sum"))
            .reset_index().sort_values("unit_count", ascending=False).head(15)
        )
        sku_df["label"] = sku_df["spec_processor_brand"] + " " + sku_df["spec_processor_sku"]
        sku_df_sorted = sku_df.sort_values("unit_count")
        fig2 = px.bar(
            sku_df_sorted, x="unit_count", y="label", orientation="h",
            labels={"unit_count": "Unit Count", "label": ""},
            color="spec_processor_brand",
            color_discrete_map={"INTEL": COLOR_PRIMARY, "AMD": COLOR_SECONDARY},
            template=PLOTLY_TEMPLATE,
        )
        fig2.update_layout(height=450, margin=dict(t=10, b=10), showlegend=True)
        st.plotly_chart(fig2, width='stretch')

    # ── Row 1b: Product brand vs Processor SKU ──
    section("Product Brand vs Processor SKU")
    brand_sku_df = (
        spec_df[spec_df["product_brand"].notna() & spec_df["spec_processor_sku"].notna()]
        .groupby(["product_brand", "spec_processor_sku"])
        .agg(unit_count=("delivery_quantity", "sum"))
        .reset_index()
    )
    # Keep only top 10 processor SKUs and top 10 product brands by unit count to keep chart readable
    top_skus   = brand_sku_df.groupby("spec_processor_sku")["unit_count"].sum().nlargest(10).index
    top_brands = brand_sku_df.groupby("product_brand")["unit_count"].sum().nlargest(10).index
    brand_sku_df = brand_sku_df[
        brand_sku_df["spec_processor_sku"].isin(top_skus) &
        brand_sku_df["product_brand"].isin(top_brands)
    ]
    if not brand_sku_df.empty:
        pivot = brand_sku_df.pivot(index="spec_processor_sku", columns="product_brand", values="unit_count").fillna(0)
        fig_heat = px.imshow(
            pivot,
            labels={"x": "Product Brand", "y": "Processor SKU", "color": "Unit Count"},
            color_continuous_scale=[[0, "#ECF0F1"], [1, COLOR_PRIMARY]],
            template=PLOTLY_TEMPLATE,
            aspect="auto",
        )
        fig_heat.update_layout(height=350, margin=dict(t=10, b=10))
        fig_heat.update_xaxes(tickangle=-35)
        st.plotly_chart(fig_heat, width='stretch')
        st.caption("Top 10 product brands × top 10 processor SKUs by unit count")
    else:
        st.info("Not enough data to render brand vs SKU chart.")

    # ── Row 2: Top models + RAM distribution ──
    col3, col4 = st.columns([2, 1])

    with col3:
        section("Top 15 Models by Spend")
        model_df = (
            df[df["spec_model"].notna()]
            .groupby(["product_category", "spec_model"])
            .agg(order_count=("order_id", "count"), total_spend=("subtotal_pen", "sum"))
            .reset_index().sort_values("total_spend", ascending=False).head(15)
        )
        model_df_sorted = model_df.sort_values("total_spend")
        fig3 = px.bar(
            model_df_sorted, x="total_spend", y="spec_model", orientation="h",
            labels={"total_spend": "Total Spend (PEN)", "spec_model": ""},
            color="product_category",
            template=PLOTLY_TEMPLATE,
        )
        fig3.update_layout(height=450, margin=dict(t=10, b=10))
        st.plotly_chart(fig3, width='stretch')

    with col4:
        section("RAM Distribution")
        ram_df = (
            df[df["spec_ram"].notna()]
            .groupby("spec_ram")
            .agg(order_count=("order_id", "count"))
            .reset_index().sort_values("order_count", ascending=False).head(10)
        )
        fig4 = px.pie(
            ram_df, values="order_count", names="spec_ram",
            template=PLOTLY_TEMPLATE, hole=0.4,
            color_discrete_sequence=px.colors.qualitative.Set3,
        )
        fig4.update_layout(height=220, margin=dict(t=10, b=5))
        st.plotly_chart(fig4, width='stretch')

        section("Storage Distribution")
        storage_df = (
            df[df["spec_storage"].notna()]
            .groupby("spec_storage")
            .agg(order_count=("order_id", "count"))
            .reset_index().sort_values("order_count", ascending=False).head(10)
        )
        fig5 = px.pie(
            storage_df, values="order_count", names="spec_storage",
            template=PLOTLY_TEMPLATE, hole=0.4,
            color_discrete_sequence=px.colors.qualitative.Pastel,
        )
        fig5.update_layout(height=220, margin=dict(t=10, b=5))
        st.plotly_chart(fig5, width='stretch')

# ─── Page: Data Quality ───────────────────────────────────────────────────────

def page_data_quality(conn, df: pd.DataFrame) -> None:
    section("Data Quality Report")

    flag_cols = [c for c in df.columns if c.startswith("flag_")]

    if flag_cols:
        cols = st.columns(len(flag_cols))
        for i, flag in enumerate(flag_cols):
            count = int(df[flag].sum()) if flag in df.columns else 0
            pct   = count / len(df) * 100 if len(df) else 0
            label = flag.replace("flag_", "").replace("_", " ").title()
            icon  = "🔴" if count > 0 else "🟢"
            with cols[i]:
                kpi_card(f"{icon} {label}", f"{count:,}", f"{pct:.1f}% of records")

    st.markdown("<br>", unsafe_allow_html=True)

    if flag_cols and "order_ym" in df.columns:
        section("Flag Volume Over Time")
        trend = df.groupby("order_ym")[flag_cols].sum().reset_index()
        fig = px.line(
            trend.melt(id_vars="order_ym", value_vars=flag_cols),
            x="order_ym", y="value", color="variable",
            labels={"order_ym": "Month", "value": "Flagged Records", "variable": "Flag"},
            template=PLOTLY_TEMPLATE,
        )
        fig.update_layout(height=280, margin=dict(t=10, b=10))
        st.plotly_chart(fig, width='stretch')

    section("Pipeline Run Log")
    try:
        run_log = run_query(conn, "SELECT * FROM load_log ORDER BY loaded_at DESC LIMIT 20")
        st.dataframe(run_log, width='stretch')
    except Exception:
        st.info("No pipeline run log available yet.")

    section("Anomaly Explorer")
    if flag_cols:
        selected_flag = st.selectbox("Show records flagged by:", flag_cols)
        if selected_flag and selected_flag in df.columns:
            anomalies = df[df[selected_flag] == 1][
                ["order_id", "entity_name", "supplier_name", "subtotal_pen", "published_date", selected_flag]
            ].head(100)
            st.dataframe(anomalies, width='stretch', height=300)
            st.caption(f"Showing up to 100 records where {selected_flag} = True")


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    conn    = get_connection()
    filters = render_sidebar(conn)
    page    = filters["page"]

    st.markdown(f"# Peru Compras — Procurement Intelligence")
    st.markdown(
        "Exploring public procurement data from the "
        "[Peru Compras open data portal](https://www.catalogos.perucompras.gob.pe/ConsultaOrdenesPub)."
    )
    st.markdown("---")

    with st.spinner("Loading data ..."):
        df = load_filtered(conn, filters)

    if df.empty:
        st.warning("No records match your current filters. Try adjusting the sidebar.")
        return

    if page == "Overview":
        page_overview(df)
    elif page == "Suppliers":
        page_suppliers(df)
    elif page == "Entities":
        page_entities(df)
    elif page == "Products":
        page_products(df)
    elif page == "Data Quality":
        page_data_quality(conn, df)

    st.markdown("---")
    st.caption("Built with Python · pandas · SQLite · Streamlit · Plotly  |  Data: Peru Compras")


if __name__ == "__main__":
    main()
