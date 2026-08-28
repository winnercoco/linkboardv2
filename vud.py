from pathlib import Path
import pandas as pd
import streamlit as st


# ============================================================
# PAGE CONFIGURATION
# ============================================================

st.set_page_config(
    page_title="Verify Uniformity of Data",
    page_icon="🔎",
    layout="wide"
)


# ============================================================
# LOAD DATA
# ============================================================

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
FILE_NAME = "master_links.xlsx"
LINKS_PATH = DATA_DIR / FILE_NAME


@st.cache_data
def load_data():
    return pd.read_excel(LINKS_PATH)


try:
    df = load_data()
except Exception as e:
    st.error(f"Could not load {FILE_NAME}")
    st.exception(e)
    st.stop()


# ============================================================
# HEADER
# ============================================================

st.title("🔎 Verify Uniformity of Data")

st.caption(
    f"Checking data consistency in **{FILE_NAME}**"
)

st.divider()


# ============================================================
# DATASET OVERVIEW
# ============================================================

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric(
        "Total Rows",
        len(df)
    )

with col2:
    st.metric(
        "Total Columns",
        len(df.columns)
    )

with col3:
    st.metric(
        "Missing Values",
        int(df.isna().sum().sum())
    )

with col4:
    st.metric(
        "Duplicate Rows",
        int(df.duplicated().sum())
    )


st.divider()


# ============================================================
# SELECT COLUMNS
# ============================================================

# Exclude columns that were excluded in the original program
excluded_columns = [
    "main_link",
    "duration",
    "rate"
]

header = [
    column
    for column in df.columns
    if column not in excluded_columns
]


# ============================================================
# SIDEBAR
# ============================================================

st.sidebar.header("Uniformity Check")

# Refresh Excel file
if st.sidebar.button("🔄 Refresh Excel"):
    load_data.clear()
    st.rerun()

selected_column = st.sidebar.selectbox(
    "Select a column",
    header
)

search_value = st.sidebar.text_input(
    "Search value",
    placeholder="e.g. English"
)

top_n = st.sidebar.slider(
    "Number of values to display",
    min_value=5,
    max_value=50,
    value=20
)


# ============================================================
# PROCESS SELECTED COLUMN
# ============================================================

selected_data = df[selected_column]

# Split comma-separated values
words = (
    selected_data
    .dropna()
    .astype(str)
    .str.split(",")
    .explode()
    .str.strip()
)

# Remove empty values
words = words[words != ""]

# Frequency
freq = (
    words
    .value_counts()
    .sort_values(ascending=False)
)


# ============================================================
# COLUMN HEADER
# ============================================================

st.subheader(
    f"📊 Uniformity Analysis — {selected_column}"
)


# ============================================================
# METRICS
# ============================================================

metric1, metric2, metric3 = st.columns(3)

with metric1:
    st.metric(
        "Total Rows",
        len(selected_data)
    )

with metric2:
    st.metric(
        "Unique Values",
        len(freq)
    )

with metric3:
    st.metric(
        "Non-empty Entries",
        len(words)
    )


# ============================================================
# SEARCH
# ============================================================

if search_value:

    search_results = freq[
        freq.index.str.contains(
            search_value,
            case=False,
            na=False
        )
    ]

    st.subheader("🔍 Search Results")

    if len(search_results) == 0:
        st.warning(
            f"No values found containing '{search_value}'."
        )

    else:

        search_df = (
            search_results
            .rename("Count")
            .reset_index()
            .rename(columns={"index": selected_column})
        )

        st.dataframe(
            search_df,
            use_container_width=True,
            hide_index=True
        )


# ============================================================
# FREQUENCY TABLE + CHART
# ============================================================

left, right = st.columns([1, 1])


# ------------------------------------------------------------
# TABLE
# ------------------------------------------------------------

with left:

    st.subheader("📋 Value Frequency")

    display_df = (
        freq
        .head(top_n)
        .rename("Count")
        .reset_index()
        .rename(columns={"index": selected_column})
    )

    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True
    )


# ------------------------------------------------------------
# CHART
# ------------------------------------------------------------

with right:

    st.subheader("📈 Frequency Distribution")

    chart_df = (
        freq
        .head(top_n)
        .rename("Count")
    )

    st.bar_chart(
        chart_df,
        horizontal=True
    )


# ============================================================
# ALL VALUES
# ============================================================

with st.expander("View all unique values"):

    all_values_df = (
        freq
        .rename("Count")
        .reset_index()
        .rename(columns={"index": selected_column})
    )

    st.dataframe(
        all_values_df,
        use_container_width=True,
        hide_index=True
    )


# ============================================================
# DATA PREVIEW
# ============================================================

with st.expander("Preview original data"):

    st.dataframe(
        df[[selected_column]].head(100),
        use_container_width=True,
        hide_index=True
    )


# ============================================================
# FOOTER
# ============================================================

st.divider()

st.caption(
    "Uniformity Checker • Comma-separated values are split, "
    "trimmed and analyzed for frequency."
)