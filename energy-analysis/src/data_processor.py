import pandas as pd
import yaml
from pathlib import Path
import logging

# Setup logging
logging.basicConfig(
    filename="logs/pipeline.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Load config
def load_config():
    with open("config/config.yaml", "r") as f:
        return yaml.safe_load(f)

config = load_config()
CITIES = config["cities"]

# Optional: Add fixed coordinates per city (could also be added in config)
CITY_COORDS = {
    "New York": (40.7128, -74.0060),
    "Chicago": (41.8781, -87.6298),
    "Houston": (29.7604, -95.3698),
    "Phoenix": (33.4484, -112.0740),
    "Seattle": (47.6062, -122.3321),
}

def process_and_merge_data(weather_path, energy_path, output_path="data/processed/merged_2025-05-05_to_2025-08-03.csv"):
    try:
        weather_df = pd.read_csv(weather_path)
        energy_df = pd.read_csv(energy_path)

        # Ensure date columns are in datetime format
        weather_df["date"] = pd.to_datetime(weather_df["date"])
        energy_df["date"] = pd.to_datetime(energy_df["date"])

        # Pivot NOAA weather data (TMAX_F, TMIN_F already present)
        if "datatype" in weather_df.columns and "value" in weather_df.columns:
            weather_df = weather_df.pivot_table(
                index=["city", "date"],
                columns="datatype",
                values="value",
                aggfunc="first"
            ).reset_index()
            weather_df.columns.name = None  # remove group name from columns

        # Merge datasets on city and date
        df = pd.merge(weather_df, energy_df, on=["city", "date"], how="inner")

        # Merge city metadata (state, EIA region code)
        city_meta = pd.DataFrame(CITIES).rename(columns={"name": "city"})
        df = df.merge(city_meta[["city", "state", "eia_region_code"]], on="city", how="left")

        # Add fixed coordinates
        df["latitude"] = df["city"].map(lambda c: CITY_COORDS.get(c, (None, None))[0])
        df["longitude"] = df["city"].map(lambda c: CITY_COORDS.get(c, (None, None))[1])

        # Validate required columns
        required_cols = ["TMAX_F", "TMIN_F", "energy_usage"]
        missing = [col for col in required_cols if col not in df.columns]
        if missing:
            logging.warning(f"Missing expected columns: {missing}. Some records may be dropped.")

        # Compute average temperature (F)
        if "TMAX_F" in df.columns and "TMIN_F" in df.columns:
            df["temperature"] = df[["TMAX_F", "TMIN_F"]].mean(axis=1)

        # Drop rows with missing critical data
        present_required = [col for col in required_cols if col in df.columns]
        df.dropna(subset=present_required, inplace=True)

        # Drop any outdated columns
        for col in ["TMAX", "TMIN", "Avg_temp"]:
            if col in df.columns:
                df.drop(columns=col, inplace=True)

        # Ensure there's data left
        if df.empty:
            logging.error("No valid merged data to save after dropping missing values.")
            return pd.DataFrame()

        # Save to output CSV
        Path("data/processed").mkdir(parents=True, exist_ok=True)
        df.to_csv(output_path, index=False)
        logging.info(f"Processed data saved to {output_path}")

        return df

    except Exception as e:
        logging.error(f"Error during data processing: {e}")
        return pd.DataFrame()

