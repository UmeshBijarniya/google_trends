# MAIN.PY — GOOGLE TRENDS DASHBOARD (INDIA • YOUTUBE)
import os
from datetime import date, datetime
import numpy as np
import pandas as pd
import streamlit as st
import plotly.graph_objs as go

# =====================================================
# CONFIG
# =====================================================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "Data")

TOP_VISIBLE = 50
HOVER_TOP = 20

# =====================================================
# DATA LOADING LOGIC
# =====================================================

@st.cache_data
def load_and_merge_data() -> pd.DataFrame:
    """
    1. Scans 'Data' folder for {ExamName}.csv
    2. Reads files regardless of header name (e.g. date,NEET or date,Jaipur)
    3. Merges them into one big table keyed by Date
    """
    if not os.path.exists(DATA_DIR):
        return pd.DataFrame(columns=["Date"])

    files = [f for f in os.listdir(DATA_DIR) if f.lower().endswith(".csv")]
    
    if not files:
        return pd.DataFrame(columns=["Date"])

    merged_df = pd.DataFrame()

    for f in files:
        file_path = os.path.join(DATA_DIR, f)
        # Use filename as the official Exam Name (e.g. "NEET.csv" -> "NEET")
        exam_name = os.path.splitext(f)[0]

        try:
            df = pd.read_csv(file_path)
            
            # 1. Identify DATE column (case-insensitive)
            date_col = next((c for c in df.columns if c.lower().strip() == "date"), None)
            
            if not date_col:
                print(f"Skipping {f}: No 'date' column found.")
                continue

            # 2. Identify VALUE column (The column that is NOT the date column)
            # This handles 'NEET', 'Jaipur', or any other header name dynamically
            value_cols = [c for c in df.columns if c != date_col]
            if not value_cols:
                print(f"Skipping {f}: No value column found.")
                continue
            
            original_val_col = value_cols[0]

            # 3. Standardize & Rename
            # We rename the value column to the File Name to ensure consistency
            df = df.rename(columns={date_col: "Date", original_val_col: exam_name})
            df = df[["Date", exam_name]]

            # 4. Clean Date
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
            df = df.dropna(subset=["Date"]).sort_values("Date")

            # 5. Merge
            if merged_df.empty:
                merged_df = df
            else:
                merged_df = pd.merge(merged_df, df, on="Date", how="outer")

        except Exception as e:
            print(f"Error loading {f}: {e}")
            continue

    if merged_df.empty:
        return pd.DataFrame(columns=["Date"])

    merged_df.sort_values("Date", inplace=True)
    merged_df.fillna(0, inplace=True)
    
    return merged_df

# =====================================================
# UI SETUP
# =====================================================

st.set_page_config(page_title="📊 Google Trends Dashboard", layout="wide")
st.title("📊 Google Trends Dashboard (India • YouTube)")

# 1. Load Data
master_df = load_and_merge_data()

if master_df.empty:
    st.error(f"No valid data found in {DATA_DIR}. Please check that your CSV files have a 'date' column.")
    st.stop()

# 2. Get Exam Names (columns excluding Date)
all_exams = [c for c in master_df.columns if c != "Date"]

# 3. Sidebar Selection
exam_choice = st.sidebar.multiselect(
    "🏷️ Exam Name",
    ["All Exams"] + all_exams,
    default=["All Exams"],
)

if not exam_choice:
    st.warning("⚠ No exam selected.")
    st.stop()

# Helper for case-insensitive comparison
def norm_key(x): return str(x).lower().strip().replace(" ", "")

has_all_exams = any(norm_key(e) == "allexams" for e in exam_choice)

# 4. Filter Columns based on selection
cols_to_plot = ["Date"]
if has_all_exams:
    cols_to_plot = master_df.columns.tolist()
else:
    # Match user selection to dataframe columns
    selected_norms = [norm_key(e) for e in exam_choice]
    for c in master_df.columns:
        if c != "Date" and norm_key(c) in selected_norms:
            cols_to_plot.append(c)

df_filtered = master_df[cols_to_plot].copy()

# =====================================================
# DATE RANGE
# =====================================================

min_dt = df_filtered["Date"].min().date()
max_dt = df_filtered["Date"].max().date()

date_range = st.sidebar.date_input(
    "📅 Date Range",
    value=(min_dt, max_dt),
    min_value=min_dt,
    max_value=max_dt,
)

if isinstance(date_range, tuple):
    if len(date_range) == 2:
        start_date, end_date = date_range
    elif len(date_range) == 1:
        start_date = end_date = date_range[0]
    else:
        start_date, end_date = min_dt, max_dt
else:
    start_date = end_date = date_range

# Apply Date Filter
mask = (df_filtered["Date"].dt.date >= start_date) & (df_filtered["Date"].dt.date <= end_date)
df_final = df_filtered.loc[mask]

# =====================================================
# PLOT FUNCTION
# =====================================================

def plot_line(df: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    
    cols = [c for c in df.columns if c != "Date"]
    
    colors = [
        "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728",
        "#9467bd", "#8c564b", "#e377c2", "#7f7f7f",
        "#bcbd22", "#17becf",
    ]

    for i, c in enumerate(cols):
        fig.add_trace(go.Scatter(
            x=df["Date"],
            y=df[c],
            mode="lines",
            name=c,
            line=dict(width=2.0, color=colors[i % len(colors)]),
            hoverinfo="skip"
        ))

    # --- Hover Logic ---
    hover_texts = []
    dates = pd.to_datetime(df["Date"]).dt.strftime("%b %d, %Y").tolist()
    vals = df[cols].values

    def bar(color):
        return f"<span style='display:inline-block;width:14px;height:4px;background:{color};margin-right:6px;'></span>"

    for i in range(len(df)):
        pairs = list(zip(cols, vals[i]))
        # Sort by value descending
        top = sorted(pairs, key=lambda x: x[1], reverse=True)[:HOVER_TOP]

        lines = [
            f"{bar(colors[cols.index(name) % len(colors)])} <b>{name}</b>: {val:.2f}"
            for name, val in top
        ]
        hover_texts.append(f"<b>{dates[i]}</b><br>" + "<br>".join(lines))

    fig.add_trace(go.Scatter(
        x=df["Date"],
        y=[0] * len(df),
        mode="markers",
        marker=dict(size=0, opacity=0),
        customdata=hover_texts,
        hovertemplate="%{customdata}<extra></extra>",
        showlegend=False,
    ))

    fig.update_layout(
        height=500,
        template="plotly_white",
        hovermode="closest",
        legend=dict(orientation="v", y=0.5, x=1.02),
        xaxis=dict(title="Date"),
        yaxis=dict(title="Search Interest"),
    )
    return fig

# =====================================================
# RENDER CHART & TABLE
# =====================================================

st.header("1️⃣ Search Interest Over Time")

if not df_final.empty:
    st.plotly_chart(plot_line(df_final), use_container_width=True)
    
    with st.expander("📄 View Raw Data"):
        table_df = df_final.copy()
        table_df["Date"] = table_df["Date"].apply(lambda x: x.strftime("%Y-%m-%d"))
        st.dataframe(table_df, use_container_width=True)
else:
    st.warning("No data available for the selected range.")

# =====================================================
# AUC (Area Under Curve)
# =====================================================

st.header("📀 AUC Comparison (6-Month Intervals)")

if not df_final.empty:
    cols = [c for c in df_final.columns if c != "Date"]
    
    min_d_auc = df_final["Date"].min()
    max_d_auc = df_final["Date"].max()

    rows = []
    cur = min_d_auc

    while cur < max_d_auc:
        nxt = cur + pd.DateOffset(months=6)
        
        window_start = cur.date()
        window_end = min(nxt, max_d_auc).date()
        
        row = {"Start": window_start, "End": window_end}
        
        mask_auc = (df_final["Date"] >= cur) & (df_final["Date"] < nxt)
        slice_df = df_final.loc[mask_auc]

        has_data = False
        for col in cols:
            # Need at least 2 points to calculate area
            if slice_df.shape[0] > 1:
                x_secs = slice_df["Date"].astype("int64") / 1e9
                y_vals = slice_df[col]
                
                # --- FIX: Use np.trapezoid for NumPy 2.0+ ---
                area = np.trapezoid(y_vals, x_secs) 
                
                row[col] = round(float(area), 2)
                has_data = True
            else:
                row[col] = 0.0

        if has_data:
            rows.append(row)
        
        cur = nxt

    if rows:
        st.dataframe(pd.DataFrame(rows), use_container_width=True)
    else:
        st.info("Not enough data points to calculate AUC.")