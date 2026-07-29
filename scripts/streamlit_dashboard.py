import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path

# --- PAGE SETUP ---
st.set_page_config(page_title="Sales Insights", layout="wide", page_icon="📦")

st.markdown(
    """
    <style>
    .metric-container .stMetric {
        background-color: #f8fafc;
        border: 1px solid #e6ecf0;
        border-radius: 12px;
        padding: 18px;
    }
    .stMarkdown h1, .stMarkdown h2, .stMarkdown h3 {
        color: #0f172a;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

BASE_DIR = Path(__file__).resolve().parents[1]
CURATED_DIR = BASE_DIR / "data" / "curated" / "daily_summary"
RAW_DIR = BASE_DIR / "data" / "raw"
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

if not df_top.empty and 'entity_id' in df_top.columns:
    df_top = df_top.merge(catalog_df, left_on='entity_id', right_on='sku_id', how='left')
    if 'sku_id' in df_top.columns:
        df_top = df_top.drop(columns=['sku_id'])

if not df_low.empty and 'entity_id' in df_low.columns:
    df_low = df_low.merge(catalog_df, left_on='entity_id', right_on='sku_id', how='left')
    if 'sku_id' in df_low.columns:
        df_low = df_low.drop(columns=['sku_id'])

st.title("📦 Sales & Inventory Insights")
st.markdown("Monitor sales activity and inventory health across top-selling categories.")

# --- Summary metrics ---
metric_col1, metric_col2, metric_col3 = st.columns(3)

total_events = len(df_top)
unique_skus = df_top['entity_id'].nunique() if 'entity_id' in df_top.columns else 0
low_stock_count = len(df_low)

metric_col1.metric("Total Transactions", f"{total_events:,}")
metric_col2.metric("Active SKUs", f"{unique_skus:,}")
metric_col3.metric(
    "Critical Low Stock Items",
    f"{low_stock_count:,}",
    delta=f"{low_stock_count} items" if low_stock_count > 0 else "No action needed",
    delta_color="inverse",
)

st.markdown("---")

# --- AI-style insights ---
with st.expander("🤖 AI Insights", expanded=True):
    if raw_df.empty:
        st.info("No raw event data available to generate insights.")
    else:
        insight_rows = []
        if 'event_ts' in raw_df.columns:
            raw_df = raw_df.copy()
            raw_df['event_ts'] = pd.to_datetime(raw_df['event_ts'], errors='coerce')
            latest_stock = raw_df.sort_values('event_ts').groupby('entity_id', as_index=False).last()
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
                    st.write(f"- **{row['display_name']}** ({row['entity_id']}): {int(row['current_stock_level'])} units remaining")
            else:
                st.success("No immediate low-stock risk detected in current raw data.")
        else:
            st.warning("Current stock levels are not available in the raw payloads.")

        if 'event_type' in raw_df.columns and not raw_df['event_type'].empty:
            top_event = raw_df['event_type'].value_counts().idxmax()
            st.write(f"- Most common transaction type: **{top_event}**")

        if 'entity_id' in raw_df.columns and not raw_df['entity_id'].empty:
            top_sku = raw_df['entity_id'].value_counts().idxmax()
            top_name = catalog_df.loc[catalog_df['sku_id'] == top_sku, 'product_name']
            display_name = top_name.iloc[0] if not top_name.empty else top_sku
            st.write(f"- Top active SKU: **{display_name}** ({top_sku})")

st.markdown("---")

st.subheader("🏷️ Top Selling Categories")
if 'category' in df_top.columns and not df_top.empty:
    category_counts = df_top['category'].fillna('Unknown').value_counts().reset_index()
    category_counts.columns = ['category', 'transactions']
    category_counts = category_counts.head(10)
    fig = px.bar(category_counts, x='category', y='transactions', title='Top Selling Categories', template='plotly_white')
    fig.update_layout(showlegend=False, xaxis_title='Category', yaxis_title='Transactions')
    st.plotly_chart(fig, use_container_width=True)
    st.dataframe(category_counts, use_container_width=True)
else:
    st.info("No category sales data available.")
