import logging
import os
import time
from datetime import datetime, timedelta, timezone
from functools import wraps
from typing import Tuple

import pandas as pd
import requests
import yaml


def retry_with_backoff(retries=5, initial_delay=1, backoff_factor=2):
    """
    A decorator for retrying a function with exponential backoff.
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            delay = initial_delay
            for i in range(retries):
                try:
                    return func(*args, **kwargs)
                except requests.exceptions.RequestException as e:
                    logging.warning(f"API request failed: {e}. Retrying in {delay}s... ({i+1}/{retries})")
                    time.sleep(delay)
                    delay *= backoff_factor
            logging.error(f"Function {func.__name__} failed after {retries} retries.")
            # Return an empty list to allow the pipeline to continue
            return []
        return wrapper
    return decorator

def load_config():
    """Loads the configuration from config/config.yaml."""
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.yaml')
    try:
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        logging.error(f"Configuration file not found at {config_path}")
        raise
    except yaml.YAMLError as e:
        logging.error(f"Error parsing YAML file: {e}")
        raise


class DataFetcher:
    """
    A class to fetch data from weather and energy APIs.
    """
    def __init__(self):
        self.config = load_config()
        self.noaa_api_key = self.config['api_keys']['noaa_cdo']
        self.eia_api_key = self.config['api_keys']['eia']
        self.noaa_url = self.config['api_urls']['noaa_cdo']
        self.eia_url = self.config['api_urls']['eia']
        self.cities = self.config['cities']
        self.session = requests.Session() # Use a session for connection pooling
        # Set the NOAA token in the header for all requests in this session
        self.session.headers.update({'token': self.noaa_api_key})

    @retry_with_backoff()
    def fetch_weather_data(self, city: dict, start_date: str, end_date: str) -> list:
        """
        Fetches daily high/low temperature from NOAA CDO for a specific city and date range.
        """
        city_name = city['name']
        station_id = city['noaa_station_id']
        logging.info(f"Fetching weather for {city_name} ({station_id}) from {start_date} to {end_date}...")
        params = {
            'datasetid': 'GHCND',
            'stationid': station_id,
            'startdate': start_date,
            'enddate': end_date,
            'datatypeid': 'TMAX,TMIN',
            'units': 'metric',
            'limit': 1000  # Max limit per request
        }

        try:
            response = self.session.get(self.noaa_url, params=params)
            response.raise_for_status()
            data = response.json().get('results', [])
            if not data:
                logging.warning(f"No weather data returned for {city_name} for the specified period.")
                return []

            # Process the results, which come as a flat list, into a per-day structure
            daily_temps = {}
            for item in data:
                day = item['date'].split('T')[0]
                if day not in daily_temps:
                    daily_temps[day] = {'city': city_name, 'date': day}
                
                if item['datatype'] == 'TMAX':
                    daily_temps[day]['temp_high_c'] = item['value']
                elif item['datatype'] == 'TMIN':
                    daily_temps[day]['temp_low_c'] = item['value']
            
            # Filter out any days that don't have both TMIN and TMAX
            processed_data = [
                temps for temps in daily_temps.values() 
                if 'temp_high_c' in temps and 'temp_low_c' in temps
            ]
            return processed_data

        except requests.exceptions.RequestException as e:
            logging.error(f"API request failed for weather in {city_name}: {e}")
        except (KeyError, IndexError) as e:
            logging.error(f"Failed to parse weather data for {city_name}: {e}")
        return []

    @retry_with_backoff()
    def fetch_energy_data(self, city: dict, start_date: str, end_date: str) -> list:
        """
        Fetches daily energy consumption data for a given region and date range from EIA.
        """
        series_id = city['eia_series_id']
        city_name = city['name']
        logging.info(f"Fetching energy data for {city_name} ({series_id}) from {start_date} to {end_date}...")

        params = {
            "api_key": self.eia_api_key,
            "data[0]": "value",
            "facets[seriesId][]": series_id,
            "start": start_date,
            "end": end_date,
            "sort[0][column]": "period",
            "sort[0][direction]": "asc",
        }

        try:
            response = self.session.get(self.eia_url, params=params)
            response.raise_for_status()
            data = response.json().get('response', {}).get('data', [])
            if not data:
                logging.warning(f"No energy data returned for {city_name} for the specified period.")
                return []

            # The API returns hourly data, so we format it
            processed_data = [
                {
                    "city": city_name,
                    "region_series_id": item['series-id'],
                    "date": item['period'],
                    "energy_consumption_mwh": item['value']
                }
                for item in data if item.get('value') is not None
            ]
            return processed_data
        except requests.exceptions.RequestException as e:
            logging.error(f"API request failed for energy data in {city_name}: {e}")
        except (KeyError, IndexError) as e:
            logging.error(f"Failed to parse energy data for {city_name}: {e}")
        return []

    def fetch_historical_data(self, days: int = 90) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Fetches the last N days of historical data for all cities."""
        logging.info(f"--- Starting historical data fetch for the last {days} days ---")
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=days)

        # Format for APIs
        end_date_str = end_date.strftime('%Y-%m-%d')
        start_date_str = start_date.strftime('%Y-%m-%d')

        all_weather_data = []
        all_energy_data = []

        for city in self.cities:
            weather_data = self.fetch_weather_data(city, start_date_str, end_date_str)
            if weather_data:
                all_weather_data.extend(weather_data)

            energy_data = self.fetch_energy_data(city, start_date_str, end_date_str)
            if energy_data:
                all_energy_data.extend(energy_data)
            
            time.sleep(0.2) # Add a small delay to be respectful of API rate limits (e.g., 5 req/sec)

        logging.info("--- Historical data fetch complete ---")
        
        weather_df = pd.DataFrame(all_weather_data)
        energy_df = pd.DataFrame(all_energy_data)

        return weather_df, energy_df