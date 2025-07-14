import json
import logging
import os
from datetime import datetime, timezone

import pandas as pd

from data_fetcher import load_config
# Define project directories
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DATA_DIR = os.path.join(BASE_DIR, 'data', 'raw')
PROCESSED_DATA_DIR = os.path.join(BASE_DIR, 'data', 'processed')


class DataProcessor:
    """
    Handles cleaning, validation, merging, and processing of raw data.
    Generates a data quality report.
    """

    def __init__(self, run_timestamp: str):
        self.run_timestamp = run_timestamp
        config = load_config()
        self.quality_params = config.get('data_quality', {})
        self.quality_report = {}
        os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)

    def _load_raw_data(self, data_prefix: str) -> pd.DataFrame:
        """Loads the raw data file for the current run into a DataFrame."""
        filename = f"{data_prefix}{self.run_timestamp}.json"
        filepath = os.path.join(RAW_DATA_DIR, filename)

        if not os.path.exists(filepath):
            logging.warning(f"Raw data file not found: {filepath}")
            return pd.DataFrame()

        try:
            logging.info(f"Loading raw data from {filepath}")
            return pd.read_json(filepath)
        except Exception as e:
            logging.error(f"Failed to load or parse {filepath}: {e}")
            return pd.DataFrame()

    def _check_missing_values(self, df: pd.DataFrame, df_name: str, columns: list):
        """
        Checks for and documents missing values in specified columns.
        Why it matters: Missing data can skew analysis or cause errors in models.
        Identifying them is the first step to deciding on a handling strategy.
        """
        summary = {}
        if df.empty:
            summary['error'] = "DataFrame is empty."
        else:
            for col in columns:
                missing_count = df[col].isnull().sum()
                if missing_count > 0:
                    summary[col] = int(missing_count)
        
        self.quality_report[f"missing_values_{df_name}"] = summary
        logging.info(f"Missing value check for {df_name}: {summary if summary else 'OK'}")

    def _check_outliers(self, weather_df: pd.DataFrame, energy_df: pd.DataFrame):
        """
        Checks for outliers in temperature and energy consumption data.
        Why it matters: Outliers can be errors or extreme events. Flagging them is
        crucial for understanding data distributions and preventing model bias.
        """
        summary = {}
        if not weather_df.empty:
            temp_thresholds = self.quality_params.get('outlier_thresholds', {}).get('temp_celsius', {})
            max_temp = temp_thresholds.get('max', 55)
            min_temp = temp_thresholds.get('min', -45)
            # Temp in Celsius: > 55°C (131°F) or < -45°C (-49°F)
            temp_outliers = weather_df[
                (weather_df['temp_high_c'] > max_temp) | (weather_df['temp_low_c'] < min_temp)
            ]
            if not temp_outliers.empty:
                summary['temperature_outliers_found'] = len(temp_outliers)

        if not energy_df.empty:
            energy_thresholds = self.quality_params.get('outlier_thresholds', {}).get('energy_mwh', {})
            min_energy = energy_thresholds.get('min', 0)
            energy_outliers = energy_df[energy_df['energy_consumption_mwh'] < min_energy]
            if not energy_outliers.empty:
                summary['energy_outliers_found'] = len(energy_outliers)

        self.quality_report["outliers"] = summary
        logging.info(f"Outlier check: {summary if summary else 'OK'}")

    def _check_data_freshness(self, df: pd.DataFrame, df_name: str, max_days_old: int = 3):
        """
        Checks if the most recent data point is within an acceptable time window.
        Why it matters: Stale data can lead to incorrect analysis. This check
        ensures the pipeline is receiving up-to-date information.
        """
        summary = {}
        if df.empty or 'date' not in df.columns:
            summary['error'] = "DataFrame is empty or missing 'date' column."
        else:
            # Assumes 'date' column is already datetime
            latest_date = df['date'].max().tz_localize(timezone.utc)
            if pd.isna(latest_date):
                summary['error'] = "Could not determine latest date."
            else:
                days_diff = (datetime.now(timezone.utc) - latest_date).days
                summary['latest_date_found'] = latest_date.strftime('%Y-%m-%d')
                summary['days_since_latest_date'] = days_diff
                summary['is_stale'] = days_diff > max_days_old

                if summary['is_stale']:
                    logging.warning(f"Data freshness for {df_name}: STALE. Latest data is {days_diff} days old.")

        self.quality_report[f"freshness_{df_name}"] = summary
        logging.info(f"Data freshness check for {df_name}: {summary.get('is_stale', 'OK')}")

    def process_and_validate(self, weather_prefix: str, energy_prefix: str):
        """Orchestrates the full data processing and validation pipeline."""
        logging.info("--- Starting data processing and validation ---")
        
        weather_df = self._load_raw_data(weather_prefix)
        energy_df = self._load_raw_data(energy_prefix)

        # --- 1. Clean and Standardize Data Types ---
        if not weather_df.empty:
            weather_df['date'] = pd.to_datetime(weather_df['date'])
        if not energy_df.empty:
            # EIA data can have mixed hourly/daily formats, so normalize to date
            energy_df['date'] = pd.to_datetime(energy_df['date']).dt.normalize()

            # Aggregate hourly energy data to daily totals if needed
            if 'energy_consumption_mwh' in energy_df.columns:
                energy_df = energy_df.groupby(['city', 'date'], as_index=False).agg(
                    {'energy_consumption_mwh': 'sum'}
                )

        # Perform all quality checks
        self._check_missing_values(weather_df, 'weather', ['temp_high_c', 'temp_low_c'])
        self._check_missing_values(energy_df, 'energy', ['energy_consumption_mwh'])
        self._check_outliers(weather_df, energy_df)
        self._check_data_freshness(weather_df, 'weather')
        self._check_data_freshness(energy_df, 'energy')

        # Log and save the quality report
        logging.info("--- Data Quality Report ---")
        logging.info(json.dumps(self.quality_report, indent=2))
        report_path = os.path.join(PROCESSED_DATA_DIR, f"quality_report_{self.run_timestamp}.json")
        with open(report_path, 'w') as f:
            json.dump(self.quality_report, f, indent=4)
        logging.info(f"Data quality report saved to {report_path}")

        # Merge and save clean data
        if weather_df.empty or energy_df.empty:
            logging.warning("One or both dataframes are empty, skipping merge.")
            logging.info("--- Data processing finished with warnings ---")
            return

        # --- 3. Merge Data and Log Statistics ---
        logging.info(f"Pre-merge record counts: Weather={len(weather_df)}, Energy={len(energy_df)}")
        merged_df = pd.merge(weather_df, energy_df, on=['city', 'date'], how='inner')
        logging.info(f"Post-merge record count: {len(merged_df)}")
        
        if merged_df.empty:
            logging.warning("Merged dataframe is empty. Check for date or city mismatches.")
            logging.info("--- Data processing finished with warnings ---")
            return None  # <--- Explicitly return None if merge failed
        else:
            # Final cleaning: calculate temp_avg
            merged_df['temp_avg_c'] = (merged_df['temp_high_c'] + merged_df['temp_low_c']) / 2

            # Save processed data
            output_filename = f"merged_data_{self.run_timestamp}.csv"
            output_path = os.path.join(PROCESSED_DATA_DIR, output_filename)
            merged_df.to_csv(output_path, index=False)
            logging.info(f"Successfully saved processed data to {output_path}")

        logging.info("--- Data processing and validation finished ---")
        return merged_df  # <--- Return the clean, merged DataFrame