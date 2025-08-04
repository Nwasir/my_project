import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from sklearn.linear_model import LinearRegression
from sklearn.metrics import r2_score
import numpy as np
from datetime import datetime
import os

# Load processed data
@st.cache_data
def load_data():
    # Get absolute path to this file's directory
    base_dir = os.path.dirname(os.path.abspath(__file__))
    # Construct path to CSV relative to this file: ../data/processed/merged_2025-05-05_to_2025-08-03.csv
    csv_path = os.path.join(base_dir, "..", "data", "processed", "merged_2025-05-05_to_2025-08-03.csv")

    df = pd.read_csv(csv_path, parse_dates=["date"])
    df["dow"] = df["date"].dt.day_name()
    # Use the correct TMAX_F and TMIN_F columns
    tmax_col = next((col for col in df.columns if col.startswith("TMAX_F")), None)
    tmin_col = next((col for col in df.columns if col.startswith("TMIN_F")), None)
    if tmax_col and tmin_col:
        df["temp_avg"] = (df[tmax_col] + df[tmin_col]) / 2
    else:
        df["temp_avg"] = None
    df["temp_bin"] = pd.cut(
        df["temp_avg"],
        bins=[-100, 50, 60, 70, 80, 90, 150],
        labels=["<50°F", "50-60°F", "60-70°F", "70-80°F", "80-90°F", ">90°F"]
    )
    return df

df = load_data()
df["energy_usage"] = df["energy_usage"].abs()

# Sidebar filters
st.sidebar.title("Filter Options")
cities = df["city"].unique().tolist()
selected_cities = st.sidebar.multiselect("Select Cities", cities, default=cities)
date_range = st.sidebar.date_input("Date Range", [df["date"].min(), df["date"].max()])

filtered_df = df[(df["city"].isin(selected_cities)) & 
                 (df["date"].between(pd.to_datetime(date_range[0]), pd.to_datetime(date_range[1])))]

# Title
st.title("Energy + Weather Dashboard")
st.caption(f"Last updated: {df['date'].max().date()}")

# Show raw data section
st.sidebar.markdown("---")
show_raw_data = st.sidebar.checkbox("📊 Show Raw Data")

if show_raw_data:
    st.header("📋 Raw Data Explorer")

    st.subheader("✅ Processed Combined Data")
    st.dataframe(df, use_container_width=True)

    def try_load_csv(path):
        try:
            return pd.read_csv(path)
        except Exception:
            return pd.DataFrame()

    # Construct absolute paths for raw data CSVs too
    raw_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data", "raw")
    weather_raw_path = os.path.join(raw_dir, "weather_2025-05-06_to_2025-08-04.csv")
    energy_raw_path = os.path.join(raw_dir, "energy_2025-07-28_to_2025-08-04.csv")

    weather_raw = try_load_csv(weather_raw_path)
    energy_raw = try_load_csv(energy_raw_path)

    # Add temperature column if TMAX_F and TMIN_F exist
    if not weather_raw.empty and all(col in weather_raw.columns for col in ["TMAX_F", "TMIN_F"]):
        weather_raw["temperature"] = ((weather_raw["TMAX_F"] + weather_raw["TMIN_F"]) / 2).round(2)

    if not energy_raw.empty and all(col in energy_raw.columns for col in ["TMAX_F", "TMIN_F"]):
        energy_raw["temperature"] = ((energy_raw["TMAX_F"] + energy_raw["TMIN_F"]) / 2).round(2)

    with st.expander("📅 Weather Daily"):
        if not weather_raw.empty:
            st.dataframe(weather_raw, use_container_width=True)
        else:
            st.warning("Weather daily data not found.")

    with st.expander("⚡ Energy Daily", expanded=True):
        if not energy_raw.empty:
            st.dataframe(energy_raw, use_container_width=True)
        else:
            st.warning("Energy daily data not found.")


# Visualization 1 – Geographic Overview
st.header("📍 Geographic Overview")

# Define all target cities and their lat/lon
city_info = pd.DataFrame([
    {"city": "New York", "latitude": 40.7128, "longitude": -74.0060},
    {"city": "Chicago", "latitude": 41.8781, "longitude": -87.6298},
    {"city": "Houston", "latitude": 29.7604, "longitude": -95.3698},
    {"city": "Phoenix", "latitude": 33.4484, "longitude": -112.0740},
    {"city": "Seattle", "latitude": 47.6062, "longitude": -122.3321},
])

# Get today's and yesterday's data
latest_date = filtered_df["date"].max()
yesterday = latest_date - pd.Timedelta(days=1)

today_df = filtered_df[filtered_df["date"] == latest_date].copy()
yesterday_df = filtered_df[filtered_df["date"] == yesterday][["city", "energy_usage"]].rename(columns={"energy_usage": "prev_energy"})

# Merge today's data with yesterday's to get % change
merged_df = city_info.merge(today_df, on="city", how="left").merge(yesterday_df, on="city", how="left")

# Fill missing values
merged_df["energy_usage"] = merged_df["energy_usage"].fillna(0)
merged_df["temp_avg"] = ((merged_df.get("TMAX_F", 0) + merged_df.get("TMIN_F", 0)) / 2).round(1)
merged_df["energy_change_pct"] = (
    ((merged_df["energy_usage"] - merged_df["prev_energy"]) / merged_df["prev_energy"]) * 100
).round(1).replace([np.inf, -np.inf], 0).fillna(0)

# Rename latitude and longitude properly after merging
merged_df["latitude"] = merged_df["latitude_x"]
merged_df["longitude"] = merged_df["longitude_x"]

# Build the geo scatter plot
fig_map = px.scatter_geo(
    merged_df,
    lat="latitude",
    lon="longitude",
    text="city",
    color="energy_usage",
    size="energy_usage",
    hover_name="city",
    hover_data={
        "temp_avg": True,
        "energy_usage": ":,.0f",
        "energy_change_pct": True,
        "latitude": False,
        "longitude": False
    },
    projection="albers usa",
    color_continuous_scale="RdYlGn_r",
)

fig_map.update_traces(marker=dict(line=dict(width=1, color="black")))
fig_map.update_layout(
    title="Current Day Energy Usage by City (MWh)",
    margin={"r": 0, "t": 40, "l": 0, "b": 0},
)

# Show visualization
st.subheader("Interactive Map of City Energy & Weather")
st.plotly_chart(fig_map, use_container_width=True)
st.caption(f"Data last updated: {latest_date.strftime('%Y-%m-%d')}")
st.markdown("🔴 High Energy Usage  🟢 Low Energy Usage")


# Visualization 2 – Time Series Analysis
st.header("📈 Time Series Analysis")

city_opt = st.selectbox("Select city for time series", ["All Cities"] + cities)
plot_df = filtered_df if city_opt == "All Cities" else filtered_df[filtered_df["city"] == city_opt]

# Create dual-axis figure
fig_ts = make_subplots(specs=[[{"secondary_y": True}]])

# Add temperature trace (left axis)
fig_ts.add_trace(
    go.Scatter(x=plot_df["date"], y=plot_df["temp_avg"], name="Avg Temp (°F)", line=dict(color="orange")),
    secondary_y=False
)

# Add energy usage trace (right axis)
fig_ts.add_trace(
    go.Scatter(x=plot_df["date"], y=plot_df["energy_usage"], name="Energy Usage (MWh)", line=dict(dash="dot", color="blue")),
    secondary_y=True
)

# Highlight weekends
for weekend in plot_df[plot_df["date"].dt.dayofweek >= 5]["date"].unique():
    fig_ts.add_vrect(x0=weekend, x1=weekend + pd.Timedelta(days=1), fillcolor="gray", opacity=0.1, line_width=0)

# Update layout
fig_ts.update_layout(
    title="Temperature vs Energy Over Time for",
    xaxis_title="Date",
    legend_title="Metrics"
)

# Y-axis labels
fig_ts.update_yaxes(title_text="Avg Temp (°F)", secondary_y=False)
fig_ts.update_yaxes(title_text="Energy Usage (MWh)", secondary_y=True)

st.plotly_chart(fig_ts, use_container_width=True)

# Visualization 3 – Correlation Analysis
st.header("🔍 Correlation Analysis")

# Prepare data
corr_df = filtered_df.dropna(subset=["temp_avg", "energy_usage"])

# Linear regression
X = corr_df["temp_avg"].values.reshape(-1, 1)
y = corr_df["energy_usage"].values
model = LinearRegression().fit(X, y)
slope = model.coef_[0]
intercept = model.intercept_
r2 = r2_score(y, model.predict(X))
corr_coef = np.corrcoef(corr_df["temp_avg"], corr_df["energy_usage"])[0, 1]

# Scatter plot with regression line
fig_corr = px.scatter(
    corr_df,
    x="temp_avg",
    y="energy_usage",
    color="city",
    hover_data=["date", "city", "temp_avg", "energy_usage"],
    labels={"temp_avg": "Avg Temp (°F)", "energy_usage": "Energy Usage (MWh)"},
    title=f"Temp vs Energy Correlation<br><sup>Correlation Coefficient: {corr_coef:.2f}, R²: {r2:.2f}</sup>"
)

# Add regression line
x_range = np.linspace(corr_df["temp_avg"].min(), corr_df["temp_avg"].max(), 100)
y_range = slope * x_range + intercept
fig_corr.add_trace(go.Scatter(
    x=x_range, y=y_range,
    mode="lines",
    name=f"y={slope:.2f}x+{intercept:.2f} (R²={r2:.2f})",
    line=dict(color="black", dash="dash")
))

fig_corr.update_layout(
    xaxis_title="Average Temp (°F)",
    yaxis_title="Energy Usage (MWh)",
    legend_title="City"
)

st.plotly_chart(fig_corr, use_container_width=True)

# Visualization 4 – Usage Heatmap
st.header("🔥 Usage Patterns Heatmap (All Cities Combined)")

# Create temperature bins
temp_bins = [-float("inf"), 50, 60, 70, 80, 90, float("inf")]
temp_labels = ["<50°F", "50–60°F", "60–70°F", "70–80°F", "80–90°F", ">90°F"]

# Bin temperature
filtered_df["temp_bin"] = pd.cut(filtered_df["temp_avg"], bins=temp_bins, labels=temp_labels, include_lowest=True)

# Ensure 'dow' is correctly assigned (0 = Monday, 6 = Sunday)
filtered_df["dow"] = filtered_df["date"].dt.dayofweek
dow_labels = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]

# Group by temp_bin and day of week
heat_df = (
    filtered_df.groupby(["temp_bin", "dow"], observed=False)["energy_usage"]
    .mean()
    .reset_index()
    .pivot(index="temp_bin", columns="dow", values="energy_usage")
    .reindex(index=temp_labels, columns=range(7))  # Ensures full heatmap shape
)

# Create heatmap
fig_heat = px.imshow(
    heat_df,
    labels=dict(x="Day of Week", y="Temperature Range", color="Avg Energy Usage (MWh)"),
    x=dow_labels,
    y=temp_labels,
    text_auto=True,
    color_continuous_scale="RdBu_r",
)

fig_heat.update_layout(
    title="All Cities – Energy Usage by Temp & Day",
    xaxis_title="Day of Week",
    yaxis_title="Temperature Range",
    coloraxis_colorbar_title="Energy(MWh)"
)

st.plotly_chart(fig_heat, use_container_width=True)



   
