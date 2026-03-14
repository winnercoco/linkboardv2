import re
import streamlit as st
import json
import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DATA_PATH = BASE_DIR / "data" / "links.json"

# =============================
# Data Load & Save Functions
# =============================
def load_data():
    if DATA_PATH.exists():
        with open(DATA_PATH, "r") as f:
            return json.load(f)
    return []

def save_data(data):
    with open(DATA_PATH, "w") as f:
        json.dump(data, f, indent=2)

def extract_unique(data, fields):
    values = set()
    for item in data:
        for field in fields:
            val = str(item.get(field, "")).strip()
            if val:
                values.add(val)
    return sorted(values)

# Load initial data
data = load_data()
df = pd.DataFrame(data)

for pos_field in ["pos_1", "pos_2", "pos_3"]:
    df[pos_field] = df[pos_field].astype(str)

# Also update data list to ensure filtering works consistently
for movie in data:
    for pos_field in ["pos_1", "pos_2", "pos_3"]:
        movie[pos_field] = str(movie.get(pos_field, ""))
        
# =============================
# Streamlit Config
# =============================
st.set_page_config(page_title="Links Explorer", layout="wide")
st.markdown(
    """
    <style>
    .block-container {
        padding-top: 1rem;
        padding-bottom: 1rem;
    }
    h1 {
        margin-bottom: 0.5rem;
    }

    /* Base table styling */
    table {
        table-layout: auto;
        width: 100%;
        border-collapse: collapse;
    }
    th, td {
        white-space: normal !important;
        word-wrap: break-word;
        vertical-align: top;
        padding: 4px 6px;
        border-bottom: 1px solid #ddd;
    }

    /* Column-specific widths */
    .small-col { max-width: 40px; text-align: center; }
    .link-col { max-width: 80px; word-break: break-word; }
    .cat-col { max-width: 260px; word-break: break-word; }
    .pos-col { max-width: 180px; word-break: break-word; }
    .tag-col { max-width: 300px; word-break: break-word; }

    </style>
    """,
    unsafe_allow_html=True
)
st.title("🎬 Links Explorer")

# =============================
# 🔍 FILTERS
# =============================
st.markdown("## 🔍 Filters")

cat_fields = ["cat_1", "cat_2", "cat_3", "cat_4", "cat_5", "cat_6"]

filter_cols = st.columns([2, 2, 2, 2, 2])
with filter_cols[0]:
    duration_range = st.slider("Duration (minutes)", 0, 300, (60, 150))
with filter_cols[1]:
    rating_range = st.slider("Rating Range", 1, 10, (1, 10))
with filter_cols[2]:
    core_cats = extract_unique(data, ["core_cat"])
    core_cat_selected = st.multiselect("Core Categories", core_cats)
with filter_cols[3]:
    all_cats = extract_unique(data, cat_fields)
    cats_selected = st.multiselect("Other Categories", all_cats)
with filter_cols[4]:
    tag_search = st.text_input("Tags (comma-separated)")

row2 = st.columns([2, 2, 2])
with row2[0]:
    all_actors = extract_unique(data, ["star_1", "star_2", "star_3"])
    actors_selected = st.multiselect("Actors", all_actors)
with row2[1]:
    all_studios = extract_unique(data, ["studio"])
    studio_selected = st.multiselect("Studios", all_studios)
with row2[2]:
    all_positions = extract_unique(data, ["pos_1", "pos_2", "pos_3"])
    positions_selected = st.multiselect("Positions", all_positions)

# =============================
# 🎯 Filter Logic
# =============================
def matches_filters(movie):
    # Duration check
    try:
        duration = int(movie.get("duration", 0))
    except ValueError:
        duration = 0
    if not (duration_range[0] <= duration <= duration_range[1]):
        return False

    # Rating range check
    try:
        rate = float(movie.get("rate", 0))
    except ValueError:
        rate = 0
    if not (rating_range[0] <= rate <= rating_range[1]):
        return False

    # Core category check (multi-select)
    if core_cat_selected and movie.get("core_cat", "") not in core_cat_selected:
        return False

    # Other categories
    movie_cats = [str(movie.get(cat, "")).strip().lower() for cat in cat_fields]
    if cats_selected and not any(cat.lower() in movie_cats for cat in cats_selected):
        return False

    # Actor check
    movie_actors = [movie.get("star_1", ""), movie.get("star_2", ""), movie.get("star_3", "")]
    if actors_selected and not all(actor in movie_actors for actor in actors_selected):
        return False

    # Position check
    movie_positions = [movie.get("pos_1", ""), movie.get("pos_2", ""), movie.get("pos_3", "")]
    if positions_selected and not any(pos in movie_positions for pos in positions_selected):
        return False

    # Studio check
    if studio_selected and movie.get("studio", "") not in studio_selected:
        return False

    # Tags check
    if tag_search:
        tags = [t.strip().lower() for t in tag_search.split(",")]
        movie_tags = str(movie.get("general_tags", "")).lower()
        if not all(tag in movie_tags for tag in tags):
            return False

    return True

# Filter the data
filtered = list(filter(matches_filters, data))

# =============================
# 📋 Display Filtered Results (HTML Wrapped Table)
# =============================
st.markdown("## 🎞️ Filtered Links")
st.write(f"Showing **{len(filtered)}** result(s):")

if len(filtered) > 0:
    filtered_df = pd.DataFrame(filtered)

    # Merge fields safely
    def merge_fields(row, fields):
        return ", ".join(
            [str(row.get(f, "")).strip() for f in fields if str(row.get(f, "")).strip() not in ["", "-"]]
        )

    # Create merged columns
    filtered_df["Categories"] = filtered_df.apply(
        lambda x: merge_fields(x, cat_fields), axis=1
    )
    filtered_df["Stars"] = filtered_df.apply(
        lambda x: merge_fields(x, ["star_1", "star_2", "star_3"]), axis=1
    )
    filtered_df["Positions"] = filtered_df.apply(
        lambda x: merge_fields(x, ["pos_1", "pos_2", "pos_3"]), axis=1
    )

    # Prepare display DataFrame
    display_df = filtered_df[
        ["main_link", "duration", "rate", "studio", "core_cat", "Stars", "Categories", "Positions", "general_tags"]
    ].copy()

    display_df.rename(columns={
        "main_link": "Link",
        "duration": "Duration",
        "rate": "Rating",
        "studio": "Studio",
        "core_cat": "Core Category",
        "general_tags": "General Tags"
    }, inplace=True)

    # Apply Column-specific Wrapping
    display_df["Link"] = display_df["Link"].apply(lambda x: f"<div class='link-col'><a href='{x}' target='_blank'>🔗 Link</a></div>")
    display_df["Duration"] = display_df["Duration"].apply(lambda x: f"<div class='small-col'>{x}</div>")
    display_df["Rating"] = display_df["Rating"].apply(lambda x: f"<div class='small-col'>{x}</div>")
    display_df["Categories"] = display_df["Categories"].apply(lambda x: f"<div class='cat-col'>{x}</div>")
    display_df["Positions"] = display_df["Positions"].apply(lambda x: f"<div class='pos-col'>{x}</div>")
    display_df["General Tags"] = display_df["General Tags"].apply(lambda x: f"<div class='tag-col'>{x}</div>")

    # Render table
    st.markdown(display_df.to_html(escape=False, index=False), unsafe_allow_html=True)

else:
    st.info("No movies match your filters.")

# =============================
# ➕ Add New Movie Form
# =============================
st.markdown("---")
st.markdown("## ➕ Add New Link")

with st.form("add_movie_form"):
    c1, c2, c3 = st.columns(3)

    with c1:
        main_link = st.text_input("Main Link")
        match = re.search(r"https?://(?:www\.)?([^/]+)", main_link)
        website_autofill = match.group(1) if match else ""
        st.text_input("Website (autofilled from link)", value=website_autofill, disabled=True)

        duration = st.number_input("Duration (min)", min_value=1)
        rate = st.slider("Rating", 1, 10)
        studio = st.text_input("Studio")

    with c2:
        core_cat = st.text_input("Core Category")
        cats = [st.text_input(f"Category {i+1}") for i in range(6)]
        general_tags = st.text_area("General Tags")

    with c3:
        stars = [st.text_input(f"Star {i+1}") for i in range(3)]
        positions = [st.text_input(f"Position {i+1}") for i in range(3)]

    submitted = st.form_submit_button("Add Movie")
    if submitted:
        new_movie = {
            "main_link": main_link,
            "duration": duration,
            "rate": rate,
            "studio": studio,
            "website": website_autofill,
            "core_cat": core_cat,
            "cat_1": cats[0],
            "cat_2": cats[1],
            "cat_3": cats[2],
            "cat_4": cats[3],
            "cat_5": cats[4],
            "cat_6": cats[5],
            "general_tags": general_tags,
            "star_1": stars[0],
            "star_2": stars[1],
            "star_3": stars[2],
            "pos_1": positions[0],
            "pos_2": positions[1],
            "pos_3": positions[2],
        }
        data.append(new_movie)
        save_data(data)
        st.success("✅ Link added successfully! Refresh to see it in results.")
