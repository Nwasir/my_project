import logging
import os
import time
from datetime import datetime, timedelta, timezone
from functools import wraps
from typing import Tuple

import pandas as pd
import requests
import yaml
from pathlib import Path

# Load configuration from config.yaml
config_path = Path(__file__).resolve().parent.parent / "config" / "config.yaml"
with open(config_path, "r") as f:
    config = yaml.safe_load(f)

# Define CITIES from config or as a constant list
CITIES = config.get("cities", [
    # Example:
    # {"city": "New York", "eia_region_code": "NYIS"},
    # {"city": "Chicago", "eia_region_code": "MISO"},
])

def fetch_noaa_weather(start_date, end_date, save_csv=True):
    token = config["api_keys"]["noaa"]
    datasetid = "GHCND"
    url = "https://www.ncei.noaa.gov/cdo-web/api/v2/data"
    all_data = []

    for city_info in CITIES:
        city = city_info["name"]
        station_id = city_info["noaa_station_id"]
        retries = 5
        backoff = 3

        logging.info(f"Fetching NOAA weather for {city} from {start_date} to {end_date}")
        
        for attempt in range(retries):
            try:
                params = {
                    "datasetid": datasetid,
                    "stationid": station_id,
                    "startdate": start_date,
                    "enddate": end_date,
                    "limit": 1000,
                    "units": "metric"
                }
                headers = {"token": token}
                
                response = requests.get(url, params=params, headers=headers, timeout=30)

                if response.status_code == 429:
                    logging.warning(f"[{city}] NOAA rate limit hit. Backing off {backoff}s...")
                    time.sleep(backoff)
                    backoff *= 2
                    continue

                response.raise_for_status()

                records = response.json().get("results", [])
                if not records:
                    logging.warning(f"[{city}] No weather records returned.")
                    break

                for r in records:
                    if r["datatype"] in ["TMAX", "TMIN"]:
                        all_data.append({
                            "city": city,
                            "date": r["date"][:10],
                            "datatype": r["datatype"],
                            "value": r["value"] / 10  # tenths of °C
                        })

                logging.info(f"[{city}] ✅ Weather data fetched successfully.")
                break  # success, break out of retry loop

            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as net_err:
                logging.warning(f"[{city}] Network error: {net_err}. Retrying in {backoff}s...")
                time.sleep(backoff)
                backoff *= 2
            except requests.exceptions.HTTPError as http_err:
                logging.error(f"[{city}] HTTP error: {http_err}")
                break
            except Exception as e:
                logging.error(f"[{city}] Unexpected error: {e}")
                break
        else:
            logging.error(f"[{city}] ❌ All retry attempts failed.")

    df = pd.DataFrame(all_data)
    if df.empty:
        logging.warning("⚠️ No NOAA weather data fetched at all.")
        return df

    pivot_df = df.pivot_table(index=["city", "date"], columns="datatype", values="value", aggfunc="first").reset_index()

    if "TMAX" in pivot_df.columns:
        pivot_df["TMAX_F"] = (pivot_df["TMAX"] * 0.18 + 32).round(2)
        pivot_df.drop(columns="TMAX", inplace=True)
    if "TMIN" in pivot_df.columns:
        pivot_df["TMIN_F"] = (pivot_df["TMIN"] * 0.18 + 32).round(2)
        pivot_df.drop(columns="TMIN", inplace=True)

    if "Avg_temp" in pivot_df.columns:
        pivot_df.drop(columns="Avg_temp", inplace=True)

    if save_csv:
        out_dir = Path("data/raw/")
        out_dir.mkdir(parents=True, exist_ok=True)
        filename = f"weather_{start_date}_to_{end_date}.csv"
        filepath = out_dir / filename
        pivot_df.to_csv(filepath, index=False)
        logging.info(f"✅ NOAA weather data saved to {filepath}")

    return pivot_df



def fetch_eia_energy(start_date, end_date, save_csv=True):
    token = config["api_keys"]["eia"]
    all_data = []

    for city_info in CITIES:
        city = city_info["name"]
        retries = 5
        backoff = 1

        if city.lower() == "houston":
            logging.info("🔁 Using monthly retail-sales endpoint for Houston.")
            base_url = "https://api.eia.gov/v2/electricity/retail-sales/data/"
            sector = "ALL"  # You may specify 'RES', 'COM', 'IND', etc. if needed

            params = {
                "api_key": token,
                "frequency": "monthly",
                "data[0]": "sales",
                "facets[stateid][]": "TX",
                "facets[sectorid][]": sector,
                "start": start_date[:7],  # yyyy-mm
                "end": end_date[:7]
            }
        else:
            base_url = "https://api.eia.gov/v2/electricity/rto/daily-region-data/data/"
            region_code = city_info["eia_region_code"]

            params = {
                "api_key": token,
                "frequency": "daily",
                "data[0]": "value",
                "facets[respondent][]": region_code,
                "start": start_date,
                "end": end_date
            }

        while retries > 0:
            try:
                response = requests.get(base_url, params=params, timeout=30)

                if response.status_code == 429:
                    logging.warning(f"EIA rate limit hit for {city}. Retrying in {backoff} seconds...")
                    time.sleep(backoff)
                    retries -= 1
                    backoff *= 2
                    continue

                response.raise_for_status()
                records = response.json().get("response", {}).get("data", [])

                if not records:
                    logging.warning(f"No EIA data returned for {city} from {start_date} to {end_date}")

                for r in records:
                    period = r.get("period", "")[:10]
                    value = r.get("value", r.get("sales"))

                    all_data.append({
                        "city": city,
                        "date": period,
                        "energy_usage": value
                    })

                break  # success

            except Exception as e:
                logging.error(f"Failed to fetch EIA data for {city}: {e}")
                break

    df = pd.DataFrame(all_data)

    if save_csv:
        out_dir = Path("data/raw/")
        out_dir.mkdir(parents=True, exist_ok=True)
        filename = f"energy_{start_date}_to_{end_date}.csv"
        filepath = out_dir / filename

        if not df.empty:
            df = df[["city", "date", "energy_usage"]]
            df.to_csv(filepath, index=False)
            logging.info(f"✅ Saved EIA energy data to {filepath}")
        else:
            logging.warning("⚠️ EIA energy DataFrame is empty. Nothing was saved.")

    if not df.empty:
        successful_cities = df["city"].unique()
        logging.info(f"✅ Successfully fetched data for: {list(successful_cities)}")
    else:
        logging.warning("⚠️ No data fetched for any city.")

    return df


if __name__ == "__main__":
    from datetime import date, timedelta

    # Example: last 21 days
    end_date = date.today()
    start_date = end_date - timedelta(days=30)

    df = fetch_noaa_weather(start_date=start_date.isoformat(), end_date=end_date.isoformat(), save_csv=True)
    df = fetch_eia_energy(start_date=start_date.isoformat(), end_date=end_date.isoformat(), save_csv=True)
    print(df.head())  # Optional: preview the data



