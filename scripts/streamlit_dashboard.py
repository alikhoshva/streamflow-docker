import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path

# --- PAGE SETUP ---
st.set_page_config(
    page_title="Sales & inventory insights",
    page_icon=":material/analytics:",
    layout="wide"
)

BASE_DIR = Path(__file__).resolve().parents[1]
CURATED_DIR = BASE_DIR / "data" / "curated" / "daily_summary"
RAW_DIR = BASE_DIR / "data" / "raw"
CATALOG_PATH = BASE_DIR / "data" / "metadata" / "catalog.csv"
TOP_ITEMS_DIR = CURATED_DIR / "top_items"
LOW_STOCK_DIR = CURATED_DIR / "low_stock_alerts"

# --- CACHED DATA LOADERS ---
@st.cache_data(ttl=30)
def load_data(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_parquet(path)
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=30)
def load_catalog(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=["sku_id", "product_name", "category"])
    try:
        return pd.read_csv(path, dtype=str)
    except Exception:
        return pd.DataFrame(columns=["sku_id", "product_name", "category"])

@st.cache_data(ttl=30)
def load_raw_data(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    files = sorted(path.glob("*.parquet"))
    if not files:
        return pd.DataFrame()
    dfs = []
    for file in files:
        try:
            dfs.append(pd.read_parquet(file))
        except Exception:
            continue
    if not dfs:
        return pd.DataFrame()
    return pd.concat(dfs, ignore_index=True)

# Load underlying data
catalog_df = load_catalog(CATALOG_PATH)
df_top = load_data(TOP_ITEMS_DIR)
df_low = load_data(LOW_STOCK_DIR)
raw_df = load_raw_data(RAW_DIR)

# Merge catalog definitions into curated summaries
if not df_top.empty and "entity_id" in df_top.columns:
    df_top = df_top.merge(catalog_df, left_on="entity_id", right_on="sku_id", how="left")
    if "sku_id" in df_top.columns:
        df_top = df_top.drop(columns=["sku_id"])

if not df_low.empty and "entity_id" in df_low.columns:
    df_low = df_low.merge(catalog_df, left_on="entity_id", right_on="sku_id", how="left")
    if "sku_id" in df_low.columns:
        df_low = df_low.drop(columns=["sku_id"])

# Extract raw payload structures if present
if not raw_df.empty:
    raw_df = raw_df.copy()
    if "payload" in raw_df.columns:
        try:
            payload_df = pd.json_normalize(raw_df["payload"])
            raw_df = pd.concat([raw_df.drop(columns=["payload"]), payload_df], axis=1)
        except Exception:
            pass

# --- SIDEBAR CONTROLS ---
with st.sidebar:
    st.header(":material/filter_alt: Dashboard filters")

    # Collect available categories
    available_categories = sorted(list(set(
        catalog_df["category"].dropna().tolist() +
        (df_top["category"].dropna().tolist() if "category" in df_top.columns else [])
    )))
    if not available_categories:
        available_categories = ["Home & Kitchen", "Office Supplies", "Electronics", "Sports & Outdoors", "Beauty & Personal Care"]

    selected_categories = st.multiselect(
        "Category filter",
        options=available_categories,
        default=available_categories,
        help="Select product categories to display across metrics and charts."
    )

    # Event types filter
    available_event_types = sorted(raw_df["event_type"].dropna().unique().tolist()) if "event_type" in raw_df.columns and not raw_df.empty else ["checkout", "restock", "return"]
    selected_event_types = st.multiselect(
        "Event type filter",
        options=available_event_types,
        default=available_event_types,
        help="Filter transactions by event type."
    )

    st.markdown("---")
    if st.button("Refresh data", icon=":material/refresh:", width="stretch"):
        st.cache_data.clear()
        st.rerun()

# --- FILTER DATASETS ---
filtered_df_top = df_top.copy()
if "category" in filtered_df_top.columns and selected_categories:
    filtered_df_top = filtered_df_top[filtered_df_top["category"].isin(selected_categories)]

filtered_df_low = df_low.copy()
if "category" in filtered_df_low.columns and selected_categories:
    filtered_df_low = filtered_df_low[filtered_df_low["category"].isin(selected_categories)]

filtered_raw_df = raw_df.copy()
if not filtered_raw_df.empty and "entity_id" in filtered_raw_df.columns:
    filtered_raw_df = filtered_raw_df.merge(catalog_df, left_on="entity_id", right_on="sku_id", how="left")
    if "category" in filtered_raw_df.columns and selected_categories:
        filtered_raw_df = filtered_raw_df[filtered_raw_df["category"].isin(selected_categories)]
    if "event_type" in filtered_raw_df.columns and selected_event_types:
        filtered_raw_df = filtered_raw_df[filtered_raw_df["event_type"].isin(selected_event_types)]

# --- HEADER SECTION ---
st.title(":material/analytics: Sales and inventory insights")
st.caption("Real-time telemetry and retail event stream processing analytics")

# --- TOP KPI METRIC ROW ---
total_events = len(filtered_df_top) if not filtered_df_top.empty else (len(filtered_raw_df) if not filtered_raw_df.empty else 0)
unique_skus = filtered_df_top["entity_id"].nunique() if not filtered_df_top.empty and "entity_id" in filtered_df_top.columns else (filtered_raw_df["entity_id"].nunique() if not filtered_raw_df.empty and "entity_id" in filtered_raw_df.columns else 0)
low_stock_count = len(filtered_df_low) if not filtered_df_low.empty else 0

with st.container(horizontal=True):
    st.metric(
        label="Total transactions",
        value=f"{total_events:,}",
        border=True
    )
    st.metric(
        label="Active SKUs",
        value=f"{unique_skus:,}",
        border=True
    )
    st.metric(
        label="Low-stock alerts",
        value=f"{low_stock_count:,}",
        delta=f"{low_stock_count} item(s) critical" if low_stock_count > 0 else "Inventory healthy",
        delta_color="inverse" if low_stock_count > 0 else "normal",
        border=True
    )

# --- AI INSIGHTS CARD ---
with st.container(border=True):
    st.subheader(":material/auto_awesome: AI inventory insights")

    if filtered_raw_df.empty and filtered_df_low.empty:
        st.info("No active event telemetry available for pattern analysis.")
    else:
        # Stock risk analysis
        if "current_stock_level" in filtered_raw_df.columns:
            low_threshold = 9
            low_stock_items = filtered_raw_df[filtered_raw_df["current_stock_level"] < low_threshold].copy()
            if not low_stock_items.empty:
                if "event_ts" in low_stock_items.columns:
                    low_stock_items["event_ts"] = pd.to_datetime(low_stock_items["event_ts"], errors="coerce")
                    latest_low_stock = low_stock_items.sort_values("event_ts").groupby("entity_id", as_index=False).last()
                else:
                    latest_low_stock = low_stock_items.drop_duplicates(subset=["entity_id"])

                latest_low_stock["display_name"] = latest_low_stock["product_name"].fillna(latest_low_stock["entity_id"])
                st.warning(
                    f"**Attention Required:** {len(latest_low_stock)} SKU(s) dropped below the low-stock threshold of {low_threshold} units."
                )

                bullet_points = []
                for _, row in latest_low_stock.sort_values("current_stock_level").head(4).iterrows():
                    sku = row.get("entity_id", "N/A")
                    name = row.get("display_name", sku)
                    stock = int(row.get("current_stock_level", 0))
                    bullet_points.append(f"- **{name}** (`{sku}`): {stock} unit(s) remaining")
                st.markdown("\n".join(bullet_points))
            else:
                st.success("No immediate stock risk detected across selected categories.")

        # Top SKU & transaction type summary
        summary_notes = []
        if "event_type" in filtered_raw_df.columns and not filtered_raw_df["event_type"].empty:
            top_event = filtered_raw_df["event_type"].value_counts().idxmax()
            summary_notes.append(f"Most frequent activity type: **{top_event}**")

        if "entity_id" in filtered_raw_df.columns and not filtered_raw_df["entity_id"].empty:
            top_sku = filtered_raw_df["entity_id"].value_counts().idxmax()
            matching_name = catalog_df.loc[catalog_df["sku_id"] == top_sku, "product_name"]
            top_name = matching_name.iloc[0] if not matching_name.empty else top_sku
            summary_notes.append(f"Highest activity product: **{top_name}** (`{top_sku}`)")

        if summary_notes:
            st.markdown("  \n".join([f"- {note}" for note in summary_notes]))

# --- MAIN DASHBOARD VISUALS GRID ---
col_left, col_right = st.columns(2)

with col_left:
    with st.container(border=True):
        st.subheader(":material/bar_chart: Top selling categories")
        if not filtered_df_top.empty and "category" in filtered_df_top.columns:
            cat_counts = filtered_df_top["category"].fillna("Uncategorized").value_counts().reset_index()
            cat_counts.columns = ["category", "transactions"]
            cat_counts = cat_counts.head(10)

            fig = px.bar(
                cat_counts,
                x="category",
                y="transactions",
                color="category",
                color_discrete_sequence=px.colors.qualitative.Blues_r,
                template="plotly_dark",
            )
            fig.update_layout(
                showlegend=False,
                margin=dict(l=20, r=20, t=20, b=20),
                xaxis_title=None,
                yaxis_title="Transactions",
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
            )
            st.plotly_chart(fig, width="stretch")
        elif not filtered_raw_df.empty and "category" in filtered_raw_df.columns:
            cat_counts = filtered_raw_df["category"].fillna("Uncategorized").value_counts().reset_index()
            cat_counts.columns = ["category", "transactions"]
            cat_counts = cat_counts.head(10)

            fig = px.bar(
                cat_counts,
                x="category",
                y="transactions",
                color="category",
                color_discrete_sequence=px.colors.qualitative.Blues_r,
                template="plotly_dark",
            )
            fig.update_layout(
                showlegend=False,
                margin=dict(l=20, r=20, t=20, b=20),
                xaxis_title=None,
                yaxis_title="Transactions",
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
            )
            st.plotly_chart(fig, width="stretch")
        else:
            st.info("No category sales data available for selected filters.")

with col_right:
    with st.container(border=True):
        st.subheader(":material/warning: Low-stock watchlist")
        if not filtered_df_low.empty:
            display_low = filtered_df_low.copy()
            if "product_name" not in display_low.columns:
                display_low["product_name"] = display_low["entity_id"]

            cols_to_show = [col for col in ["product_name", "category", "current_stock_level", "entity_id"] if col in display_low.columns]
            table_df = display_low[cols_to_show].drop_duplicates().head(10)

            st.dataframe(
                table_df,
                column_config={
                    "product_name": st.column_config.TextColumn("Product name"),
                    "category": st.column_config.TextColumn("Category"),
                    "current_stock_level": st.column_config.ProgressColumn(
                        "Stock level",
                        min_value=0,
                        max_value=20,
                        format="%d units"
                    ),
                    "entity_id": st.column_config.TextColumn("SKU ID")
                },
                hide_index=True,
                width="stretch"
            )
        elif not filtered_raw_df.empty and "current_stock_level" in filtered_raw_df.columns:
            low_df = filtered_raw_df[filtered_raw_df["current_stock_level"] < 15].copy()
            if not low_df.empty:
                low_df["product_name"] = low_df["product_name"].fillna(low_df["entity_id"])
                cols_to_show = [col for col in ["product_name", "category", "current_stock_level", "entity_id"] if col in low_df.columns]
                table_df = low_df[cols_to_show].drop_duplicates(subset=["entity_id"]).sort_values("current_stock_level").head(10)

                st.dataframe(
                    table_df,
                    column_config={
                        "product_name": st.column_config.TextColumn("Product name"),
                        "category": st.column_config.TextColumn("Category"),
                        "current_stock_level": st.column_config.ProgressColumn(
                            "Stock level",
                            min_value=0,
                            max_value=20,
                            format="%d units"
                        ),
                        "entity_id": st.column_config.TextColumn("SKU ID")
                    },
                    hide_index=True,
                    width="stretch"
                )
            else:
                st.success("No items currently below low-stock thresholds.")
        else:
            st.info("No low-stock alerts recorded for selected filters.")
