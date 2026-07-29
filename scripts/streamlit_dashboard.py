import datetime
import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path

# --- PAGE SETUP ---
st.set_page_config(page_title="Sales Insights", layout="wide", page_icon="📦")

BASE_DIR = Path(__file__).resolve().parents[1]
CURATED_DIR = BASE_DIR / "data" / "curated" / "daily_summary"
RAW_DIR = BASE_DIR / "data" / "raw"
REJECTS_DIR = BASE_DIR / "data" / "rejects"
CATALOG_PATH = BASE_DIR / "data" / "metadata" / "catalog.csv"
TOP_ITEMS_DIR = CURATED_DIR / "top_items"
LOW_STOCK_DIR = CURATED_DIR / "low_stock_alerts"

@st.cache_data(ttl=600)
def load_data(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_parquet(path)
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=600)
def load_catalog(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=["sku_id", "product_name", "category"])
    return pd.read_csv(path, dtype=str)

@st.cache_data(ttl=600)
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

catalog_df = load_catalog(CATALOG_PATH)
df_top = load_data(TOP_ITEMS_DIR)
df_low = load_data(LOW_STOCK_DIR)
raw_df = load_raw_data(RAW_DIR)
rejects_df = load_raw_data(REJECTS_DIR)

if not df_top.empty and 'entity_id' in df_top.columns:
    df_top = df_top.merge(catalog_df, left_on='entity_id', right_on='sku_id', how='left')
    if 'sku_id' in df_top.columns:
        df_top = df_top.drop(columns=['sku_id'])

if not df_low.empty and 'entity_id' in df_low.columns:
    df_low = df_low.merge(catalog_df, left_on='entity_id', right_on='sku_id', how='left')
    if 'sku_id' in df_low.columns:
        df_low = df_low.drop(columns=['sku_id'])

# --- HEADER & REFRESH CONTROL ---
header_col1, header_col2 = st.columns([4, 1])
with header_col1:
    st.title(":material/inventory_2: Sales & Inventory Insights")
    st.markdown("Monitor sales activity and inventory health across top-selling categories.")

with header_col2:
    if st.button(":material/refresh: Refresh Data", type="secondary"):
        st.cache_data.clear()
        st.rerun()
    st.caption(f"Last updated: {datetime.datetime.now().strftime('%H:%M:%S')}")

# --- Summary metrics ---
metric_col1, metric_col2, metric_col3, metric_col4 = st.columns(4)

total_events = len(df_top)
unique_skus = df_top['entity_id'].nunique() if 'entity_id' in df_top.columns else 0
low_stock_count = len(df_low)
rejects_count = len(rejects_df)

metric_col1.metric("Total Transactions", f"{total_events:,}", border=True)
metric_col2.metric("Active SKUs", f"{unique_skus:,}", border=True)
metric_col3.metric(
    "Critical Low Stock Items",
    f"{low_stock_count:,}",
    delta=f"{low_stock_count} items requiring action" if low_stock_count > 0 else "No action needed",
    delta_color="off",
    border=True,
)
metric_col4.metric(
    "Quarantined Events",
    f"{rejects_count:,}",
    delta=f"{rejects_count} pending recovery" if rejects_count > 0 else "Pipeline healthy",
    delta_color="off",
    border=True,
)

# --- AI-style insights ---
with st.expander(":material/smart_toy: AI Insights", expanded=True):
    if df_top.empty and raw_df.empty and df_low.empty and rejects_df.empty:
        st.info("No event or summary data available to generate insights.")
    else:
        if rejects_count > 0:
            st.info(f"ℹ️ **{rejects_count}** event(s) quarantined in `data/rejects/`. Trigger Airflow recovery workflow to heal and reprocess.")

        # Check low stock items from curated df_low first, then raw_df fallback
        if not df_low.empty:
            low_threshold = 9
            st.warning(
                f"{len(df_low)} SKU(s) identified below low-stock threshold of {low_threshold}. Review replenishment priorities."
            )
            for _, row in df_low.sort_values('current_stock_level', ascending=True if 'current_stock_level' in df_low.columns else False).head(5).iterrows():
                display_name = row.get('product_name') if pd.notna(row.get('product_name')) else row.get('entity_id', 'Unknown')
                stock_val = int(row['current_stock_level']) if 'current_stock_level' in row and pd.notna(row['current_stock_level']) else 'Low'
                st.write(f"- ⚠️ **{display_name}** ({row.get('entity_id')}): **{stock_val}** units remaining → *Action: Schedule reorder*")
        elif not raw_df.empty:
            if 'event_ts' in raw_df.columns:
                raw_copy = raw_df.copy()
                raw_copy['event_ts'] = pd.to_datetime(raw_copy['event_ts'], errors='coerce')
                latest_stock = raw_copy.sort_values('event_ts').groupby('entity_id', as_index=False).last()
            else:
                latest_stock = raw_df.copy()

            if 'payload' in latest_stock.columns:
                payload_df = pd.json_normalize(latest_stock['payload'])
                latest_stock = pd.concat([latest_stock.drop(columns=['payload']), payload_df], axis=1)

            if 'current_stock_level' in latest_stock.columns:
                low_threshold = 9
                low_stock = latest_stock[latest_stock['current_stock_level'] < low_threshold].copy()
                low_stock = low_stock.merge(catalog_df, left_on='entity_id', right_on='sku_id', how='left')
                low_stock['display_name'] = low_stock['product_name'].fillna(low_stock['entity_id'])
                if not low_stock.empty:
                    st.warning(
                        f"{len(low_stock)} SKU(s) are below the low-stock threshold of {low_threshold}. Review replenishment priorities."
                    )
                    for _, row in low_stock.sort_values('current_stock_level').head(5).iterrows():
                        st.write(f"- ⚠️ **{row['display_name']}** ({row['entity_id']}): **{int(row['current_stock_level'])}** units remaining → *Action: Schedule reorder*")
                else:
                    st.success("No immediate low-stock risk detected.")
            else:
                st.success("No immediate low-stock risk detected.")
        else:
            st.success("No immediate low-stock risk detected.")

        if not raw_df.empty and 'event_type' in raw_df.columns and not raw_df['event_type'].empty:
            top_event = raw_df['event_type'].value_counts().idxmax()
            st.write(f"- Most common transaction type: **{top_event}**")

        if not raw_df.empty and 'entity_id' in raw_df.columns and not raw_df['entity_id'].empty:
            top_sku = raw_df['entity_id'].value_counts().idxmax()
            top_name = catalog_df.loc[catalog_df['sku_id'] == top_sku, 'product_name']
            display_name = top_name.iloc[0] if not top_name.empty else top_sku
            st.write(f"- Top active SKU: **{display_name}** ({top_sku})")

with st.container(border=True):
    st.subheader(":material/sell: Top Selling Categories")
    if 'category' in df_top.columns and not df_top.empty:
        category_counts = df_top['category'].fillna('Unknown').value_counts().reset_index()
        category_counts.columns = ['category', 'transactions']
        category_counts = category_counts.head(10)
        fig = px.bar(category_counts, x='category', y='transactions', title='Top Selling Categories', template='plotly_white')
        fig.update_layout(showlegend=False, xaxis_title='Category', yaxis_title='Transactions', margin=dict(l=20, r=20, t=40, b=20))
        st.plotly_chart(fig, width="stretch")
        
        st.dataframe(
            category_counts,
            hide_index=True,
            width="stretch",
            column_config={
                "category": st.column_config.TextColumn("Category"),
                "transactions": st.column_config.NumberColumn(
                    "Transactions",
                    format="%d",
                ),
            },
        )
    else:
        st.info("No category sales data available.")


