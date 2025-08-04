import pandas as pd
import logging
from pathlib import Path

def analyze_data(file_path="data/processed/combined_weather_energy.csv"):
    """
    Perform data quality checks and summary statistics.
    Logs the results and saves a brief report to logs/.
    """
    report_lines = []

    try:
        df = pd.read_csv(file_path)
        report_lines.append(f"📊 Rows: {len(df)}, Columns: {len(df.columns)}\n")

        # Check for nulls
        null_counts = df.isnull().sum()
        if null_counts.any():
            report_lines.append("❗ Missing Values:")
            report_lines.extend([f" - {col}: {count}" for col, count in null_counts.items() if count > 0])
        else:
            report_lines.append("✅ No missing values detected.")

        # Check value ranges
        if "temperature" in df.columns:
            t_min, t_max = df["temperature"].min(), df["temperature"].max()
            report_lines.append(f"🌡️ Temperature range: {t_min:.1f}°C to {t_max:.1f}°C")
            if t_min < -50 or t_max > 60:
                report_lines.append("⚠️ Temperature outliers detected.")

        if "energy_demand" in df.columns:
            e_min, e_max = df["energy_demand"].min(), df["energy_demand"].max()
            report_lines.append(f"⚡ Energy demand range: {e_min:.1f} MWh to {e_max:.1f} MWh")
            if e_min < 0:
                report_lines.append("⚠️ Negative energy demand values found.")

        # Freshness check      
        datetime_col = None
        for col in ['datetime', 'date', 'timestamp']:
            if col in df.columns:
                datetime_col = col
                break

        if not datetime_col:
            raise ValueError("No datetime-like column found (e.g., 'datetime', 'date', or 'timestamp').")

        df[datetime_col] = pd.to_datetime(df[datetime_col])
        latest_date = df[datetime_col].max().date()
        report_lines.append(f"🕒 Latest timestamp: {latest_date}")


        # Save report
        Path("logs").mkdir(parents=True, exist_ok=True)
        report_path = "logs/analysis_report.txt"
        with open(report_path, "w") as f:
            for line in report_lines:
                f.write(line + "\n")

        logging.info("✅ Data analysis completed.")
        return "\n".join(report_lines)

    except Exception as e:
        logging.error(f"❌ Data analysis failed: {e}")
        return f"Data analysis failed: {e}"
