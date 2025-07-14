import os
import sys
import logging
from datetime import datetime, timedelta, timezone

import pandas as pd

from data_fetcher import DataFetcher
from data_processor import DataProcessor

# --- Setup logging ---
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOGS_DIR = os.path.join(BASE_DIR, 'logs')
os.makedirs(LOGS_DIR, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOGS_DIR, 'pipeline.log')),
        logging.StreamHandler()
    ]
)

def run_daily_pipeline():
    fetcher = DataFetcher()
    run_timestamp = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
    all_weather_data = []
    all_energy_data = []

    # Fetch data for each city (example, adapt as needed)
    for city in fetcher.cities:
        try:
            weather = fetcher.fetch_weather_data(city, run_timestamp, run_timestamp)
            if weather:
                all_weather_data.extend(weather)
        except Exception as e:
            logging.error(f"Weather fetch failed for {city['name']}: {e}")
        try:
            energy = fetcher.fetch_energy_data(city, run_timestamp, run_timestamp)
            if energy:
                all_energy_data.extend(energy)
        except Exception as e:
            logging.error(f"Energy fetch failed for {city['name']}: {e}")

    # Save raw responses as CSV in data/raw/response.csv
    raw_dir = os.path.join(BASE_DIR, 'data', 'raw')
    os.makedirs(raw_dir, exist_ok=True)
    if all_weather_data:
        weather_df = pd.DataFrame(all_weather_data)
        weather_df.to_csv(os.path.join(raw_dir, 'weather_response.csv'), index=False)
    if all_energy_data:
        energy_df = pd.DataFrame(all_energy_data)
        energy_df.to_csv(os.path.join(raw_dir, 'energy_response.csv'), index=False)

    processor = DataProcessor(run_timestamp)
    processed_df = processor.process_and_validate('weather_data_', 'energy_data_')

    # Save processed data as CSV in data/processed
    # Save processed data as CSV in data/processed/analysis-data.csv (overwrite each run)
    if processed_df is not None and not processed_df.empty:
        processed_path = os.path.join(BASE_DIR, 'data', 'processed', 'analysis-data.csv')
        processed_df.to_csv(processed_path, index=False)
        logging.info(f"Processed data saved to {processed_path}")
    else:
        logging.warning("No processed data to save.")

    logging.info("--- Daily pipeline run finished ---")

def run_historical_pipeline(days=90):
    # Set the run timestamp for historical pipeline
    run_timestamp = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
    processor = DataProcessor(run_timestamp)
    processed_df = processor.process_and_validate('weather_data_', 'energy_data_')

    # Save processed data as CSV in data/processed
    if processed_df is not None and not processed_df.empty:
        processed_path = os.path.join(BASE_DIR, 'data', 'processed', 'analysis-data.csv')
        processed_df.to_csv(processed_path, index=False)
        logging.info(f"Processed data saved to {processed_path}")
    else:
        logging.warning("No processed data to save.")

    logging.info("--- Historical pipeline run finished ---")

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Run the energy/weather data pipeline.")
    parser.add_argument('--historical', action='store_true', help='Fetch 90 days of historical data')
    parser.add_argument('--days', type=int, default=90, help='Number of days for historical fetch')
    args = parser.parse_args()

    if args.historical:
        run_historical_pipeline(days=args.days)
    else:
        run_daily_pipeline()

if __name__ == "__main__":
    main()
    