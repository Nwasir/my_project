import logging
from datetime import date, timedelta
from pathlib import Path

from src.data_fetcher import fetch_noaa_weather, fetch_eia_energy
from src.data_processor import process_and_merge_data
from src.analysis import analyze_data 

# Setup logging
Path("logs").mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    filename="logs/pipeline.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def run_pipeline():
    try:
        logging.info("🚀 Starting data pipeline...")

        # Define date range (last 90 days)
        end = date.today()
        start = end - timedelta(days=90)
        date_range = f"{start.isoformat()}_to_{end.isoformat()}"

        # Ensure raw data directories exist
        Path("data/raw").mkdir(parents=True, exist_ok=True)
        Path("data/processed").mkdir(parents=True, exist_ok=True)

        weather_file = f"data/raw/weather_{date_range}.csv"
        energy_file = f"data/raw/energy_{date_range}.csv"

        # Step 1: Fetch weather data noaa
        logging.info("📡 Fetching NOAA weather data...")
        fetch_noaa_weather(start.isoformat(), end.isoformat(), save_csv=True)

        # Step 2: Fetch energy data
        logging.info("⚡ Fetching EIA energy data...")
        fetch_eia_energy(start.isoformat(), end.isoformat(), save_csv=True)

        # Step 3: Process and merge
        logging.info("🧹 Processing and merging data...")
        processed_df = process_and_merge_data(weather_file, energy_file)

        if not processed_df.empty:
            # Save processed data
            processed_file = f"data/processed/merged_{date_range}.csv"
            processed_df.to_csv(processed_file, index=False)
            logging.info(f"✅ Pipeline completed. Data saved to {processed_file}")
            
            # ✅ Step 4: Run data quality checks
            logging.info("🔍 Running data quality analysis...")
            report = analyze_data(processed_file)
            logging.info("📋 Data quality report:\n" + report)
        
        else:
            logging.warning("⚠️ Pipeline ran, but processed data is empty.")

    except Exception as e:
        logging.error(f"💥 Pipeline failed: {e}")

if __name__ == "__main__":
    run_pipeline()
