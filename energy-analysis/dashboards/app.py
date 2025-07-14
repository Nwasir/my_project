import glob
import os
import re
from datetime import datetime

import numpy as np
import pandas as pd
import pydeck as pdk
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
import yaml
import json

# --- Configuration & Setup ---
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROCESSED_DATA_DIR = os.path.join(BASE_DIR, 'data', 'processed')
CONFIG_PATH = os.path.join(BASE_DIR, 'config', 'config.yaml')

import sys
sys.path.append(os.path.join(BASE_DIR, 'src'))
from analysis import check_missing_values, check_outliers, check_freshness

st.set_page_config(
    page_title="Energy and Weather Analysis Dashboard",
    layout="wide"
)

# --- Data Loading ---
@st.cache_data
def load_latest_data():
    try:
        files = glob.glob(os.path.join(PROCESSED_DATA_DIR, 'merged_data_*.csv'))
        if not files:
            return None, None, None
        latest_file = max(files, key=os.path.getctime)
        match = re.search(r'(\d{8}_\d{6})', latest_file)
        timestamp_str = match.group(1) if match else None
        last_updated = datetime.strptime(timestamp_str, '%Y%m%d_%H%M%S') if timestamp_str else None
        df = pd.read_csv(latest_file)
        df['date'] = pd.to_datetime(df['date'])
        with open(CONFIG_PATH, 'r') as f:
            config = yaml.safe_load(f)
        city_info = {city['name']: {'lat': city['latitude'], 'lon': city['longitude']} for city in config['cities']}
        return df, last_updated, city_info
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return None, None, None

def get_color_by_value(value, min_val, max_val):
    if max_val == min_val:
        return [0, 255, 0, 160]
    normalized = (value - min_val) / (max_val - min_val)
    red = int(255 * normalized)
    green = int(255 * (1 - normalized))
    return [red, green, 0, 160]

# --- Main App ---
df, last_updated, city_info = load_latest_data()

if df is None:
    st.warning("No processed data found. Please run the pipeline first.")
    st.stop()

# --- Sidebar Filters ---
st.sidebar.header("Filters")

# Date range selector
min_date = df['date'].min()
max_date = df['date'].max()
date_range = st.sidebar.date_input(
    "Select Date Range",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date
)

# City filter
all_cities = sorted(df['city'].unique())
selected_cities = st.sidebar.multiselect(
    "Select Cities",
    options=all_cities,
    default=all_cities
)

# Apply filters
filtered_df = df[
    (df['date'] >= pd.to_datetime(date_range[0])) &
    (df['date'] <= pd.to_datetime(date_range[1])) &
    (df['city'].isin(selected_cities))
]

if filtered_df.empty:
    st.warning("No data for the selected filters. Please adjust your selection.")
    st.stop()

df = filtered_df  # Use the filtered DataFrame for all visualizations

st.title("Energy & Weather Analysis Dashboard")
if last_updated:
    st.caption(f"Data last updated: {last_updated.strftime('%Y-%m-%d %H:%M:%S')} UTC")

# ...after filtering df...

st.header("Data Quality Report")
st.markdown("""
**Checks performed:**
- Missing values per column
- Outliers: extreme temperatures or negative energy
- Data freshness (days since latest record)
""")

missing = check_missing_values(df)
st.subheader("Missing Values")
st.write(missing)

temp_outliers, energy_outliers = check_outliers(df)
st.subheader("Temperature Outliers")
st.write(temp_outliers)
st.subheader("Negative Energy Outliers")
st.write(energy_outliers)

days_old = check_freshness(df)
st.subheader("Data Freshness")
if days_old > 3:
    st.warning(f"Latest data is {days_old} days old! (older than 3 days)")
else:
    st.success(f"Latest data is {days_old} days old.")

# --- Data Quality Checks ---
@st.cache_data

def load_quality_reports():
    """Load all available quality reports and return as a DataFrame."""
    report_files = sorted(glob.glob(os.path.join(PROCESSED_DATA_DIR, "quality_report_*.json")))
    records = []
    for f in report_files:
        with open(f, "r") as fp:
            report = json.load(fp)
        # Extract timestamp from filename
        match = re.search(r'(\d{8}_\d{6})', f)
        ts = match.group(1) if match else "unknown"
        record = {"timestamp": ts}
        # Flatten missing values
        for key in ["missing_values_weather", "missing_values_energy"]:
            mv = report.get(key, {})
            for col, val in mv.items():
                record[f"{key}_{col}"] = val if isinstance(val, int) else str(val)
        # Outliers
        outliers = report.get("outliers", {})
        for k, v in outliers.items():
            record[f"outliers_{k}"] = v
        # Freshness
        for key in ["freshness_weather", "freshness_energy"]:
            fr = report.get(key, {})
            for k, v in fr.items():
                record[f"{key}_{k}"] = v
        records.append(record)
    if records:
        return pd.DataFrame(records)
    else:
        return pd.DataFrame()

# --- Data Quality Report Section ---
st.header("Data Quality Report")
st.markdown("""
This section provides automated checks on your data:
- **Missing Values:** Shows where data is missing. Missing data can skew analysis or cause errors.
- **Outliers:** Flags extreme temperature values (above 130°F/55°C or below -50°F/-45°C) and negative energy usage. Outliers may indicate errors or rare events.
- **Data Freshness:** Warns if the latest data is more than 3 days old. Stale data can lead to incorrect analysis.
- **History:** See how data quality has changed over time.
""")

qdf = load_quality_reports()
if qdf.empty:
    st.info("No data quality reports found. Run the pipeline to generate reports.")
else:
    # Show latest report
    latest = qdf.sort_values("timestamp").iloc[-1]
    st.subheader("Latest Data Quality Metrics")
    cols = [c for c in qdf.columns if c != "timestamp"]
    st.table(latest[cols])

    # Show freshness warnings
    freshness_cols = [c for c in qdf.columns if "freshness" in c and ("is_stale" in c or "days_since_latest_date" in c)]
    for col in freshness_cols:
        if "is_stale" in col and latest[col] == True:
            st.warning(f"⚠️ {col.replace('_', ' ').title()}: Data is stale!")
        if "days_since_latest_date" in col:
            st.info(f"{col.replace('_', ' ').title()}: {latest[col]} days since last data point.")

    # Show history table
    st.subheader("Data Quality History")
    st.dataframe(qdf.set_index("timestamp").sort_index(ascending=False))

# --- Visualization 1: Geographic Overview ---
st.header("1. Geographic Overview")
st.markdown("Interactive map of all cities showing current temperature, today's energy usage, and % change from yesterday.")

latest_date = df['date'].max()
map_data = df[df['date'] == latest_date].copy()
df_sorted = df.sort_values(['city', 'date'])
df_sorted['energy_pct_change'] = df_sorted.groupby('city')['energy_consumption_mwh'].pct_change() * 100
latest_pct_change = df_sorted[df_sorted['date'] == latest_date][['city', 'energy_pct_change']]
map_data = pd.merge(map_data, latest_pct_change, on='city', how='left')
map_data['lat'] = map_data['city'].apply(lambda c: city_info.get(c, {}).get('lat'))
map_data['lon'] = map_data['city'].apply(lambda c: city_info.get(c, {}).get('lon'))
min_energy = map_data['energy_consumption_mwh'].min()
max_energy = map_data['energy_consumption_mwh'].max()
map_data['color'] = map_data['energy_consumption_mwh'].apply(
    lambda x: get_color_by_value(x, min_energy, max_energy)
)

layer = pdk.Layer(
    "ScatterplotLayer",
    data=map_data,
    get_position='[lon, lat]',
    get_color='color',
    get_radius=80000,
    pickable=True,
    auto_highlight=True,
)
tooltip = {
    "html": """
    <b>{city}</b><br/>
    Temp: {temp_avg_c:.1f}°C<br/>
    Usage: {energy_consumption_mwh:,.0f} MWh<br/>
    % Change: {energy_pct_change:.2f}%
    """,
    "style": {"backgroundColor": "steelblue", "color": "white"},
}
view_state = pdk.ViewState(
    latitude=39.8283, longitude=-98.5795, zoom=3, pitch=0,
)
st.pydeck_chart(pdk.Deck(
    map_style='mapbox://styles/mapbox/light-v9',
    initial_view_state=view_state,
    layers=[layer],
    tooltip=tooltip
))
st.markdown("---")

# --- Visualization 2: Time Series Analysis ---
st.header("2. Time Series Analysis")
st.markdown("Dual-axis line chart of temperature and energy consumption over the last 90 days. Select a city or view all.")

city_options = ["All Cities"] + sorted(df['city'].unique())
selected_city = st.selectbox("Select City", city_options, index=0)
if selected_city == "All Cities":
    plot_df = df.groupby('date').agg(
        temp_avg_c=('temp_avg_c', 'mean'),
        energy_consumption_mwh=('energy_consumption_mwh', 'sum')
    ).reset_index()
    chart_title = "All Cities"
else:
    plot_df = df[df['city'] == selected_city].sort_values('date')
    chart_title = selected_city

fig = go.Figure()
fig.add_trace(go.Scatter(
    x=plot_df['date'],
    y=plot_df['temp_avg_c'],
    name='Avg Temperature (°C)',
    line=dict(color='royalblue', width=2),
    yaxis='y1'
))
fig.add_trace(go.Scatter(
    x=plot_df['date'],
    y=plot_df['energy_consumption_mwh'],
    name='Energy (MWh)',
    line=dict(color='darkorange', width=2, dash='dot'),
    yaxis='y2'
))
for date in plot_df['date']:
    if date.weekday() == 5:
        fig.add_vrect(
            x0=date, x1=date + pd.Timedelta(days=2),
            fillcolor="rgba(200, 200, 200, 0.2)",
            layer="below", line_width=0,
        )
fig.update_layout(
    title=f"Temperature and Energy Consumption: {chart_title}",
    xaxis_title="Date",
    yaxis=dict(title="Avg Temperature (°C)", color="royalblue"),
    yaxis2=dict(title="Energy Consumption (MWh)", overlaying='y', side='right', color="darkorange"),
    legend=dict(x=0.01, y=0.99, yanchor="top", xanchor="left"),
    hovermode="x unified"
)
st.plotly_chart(fig, use_container_width=True)
st.markdown("---")

# --- Visualization 3: Correlation Analysis ---
st.header("3. Correlation Analysis")
st.markdown("Scatter plot of temperature vs. energy consumption for all cities, with regression line and stats.")

scatter_df = df.copy()
fig_scatter = px.scatter(
    scatter_df,
    x="temp_avg_c",
    y="energy_consumption_mwh",
    color="city",
    trendline="ols",
    trendline_scope="overall",
    labels={
        "temp_avg_c": "Average Temperature (°C)",
        "energy_consumption_mwh": "Energy Consumption (MWh)"
    },
    hover_data=['city', 'date']
)
try:
    results = px.get_trendline_results(fig_scatter)
    fit = results.px_fit_results.iloc[0]
    r_squared = fit.rsquared
    params = fit.params
    slope = params[1]
    intercept = params[0]
    equation = f"y = {slope:.2f}x + {intercept:.2f}"
    correlation = scatter_df['temp_avg_c'].corr(scatter_df['energy_consumption_mwh'])
    st.markdown(f"**Regression Equation:** {equation} &nbsp;&nbsp; **R²:** {r_squared:.4f} &nbsp;&nbsp; **Correlation:** {correlation:.4f}")
except Exception:
    st.warning("Could not calculate regression statistics.")
st.plotly_chart(fig_scatter, use_container_width=True)
st.markdown("---")

# --- Visualization 4: Usage Patterns Heatmap ---
st.header("4. Usage Patterns Heatmap")
st.markdown("Average energy usage by temperature range and day of week. Filter by city.")

heatmap_cities = st.multiselect(
    "Select Cities for Heatmap",
    options=sorted(df['city'].unique()),
    default=sorted(df['city'].unique())
)
heatmap_df = df[df['city'].isin(heatmap_cities)].copy()
heatmap_df['temp_avg_f'] = heatmap_df['temp_avg_c'] * 9/5 + 32
temp_bins = [-float('inf'), 50, 60, 70, 80, 90, float('inf')]
temp_labels = ["<50°F", "50-60°F", "60-70°F", "70-80°F", "80-90°F", ">90°F"]
heatmap_df['temp_bin'] = pd.cut(heatmap_df['temp_avg_f'], bins=temp_bins, labels=temp_labels, right=False)
heatmap_df['day_of_week'] = heatmap_df['date'].dt.day_name()
if heatmap_df.dropna(subset=['temp_bin', 'day_of_week']).empty:
    st.warning("Not enough data for the selected filters to generate a heatmap.")
else:
    pivot_table = heatmap_df.groupby(['temp_bin', 'day_of_week'])['energy_consumption_mwh'].mean().unstack()
    day_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    pivot_table = pivot_table.reindex(columns=day_order)
    fig_heatmap = go.Figure(data=go.Heatmap(
        z=pivot_table.values,
        x=pivot_table.columns,
        y=pivot_table.index,
        colorscale='RdBu_r',
        colorbar=dict(title="Avg MWh"),
        text=np.round(pivot_table.values, 0),
        texttemplate="%{text:,.0f}",
        hoverongaps=False
    ))
    fig_heatmap.update_layout(
        title="Average Energy Usage by Temperature Range and Day of Week",
        xaxis_title="Day of the Week",
        yaxis_title="Temperature Range (°F)"
    )
    st.plotly_chart(fig_heatmap, use_container_width=True)
st.markdown("---")