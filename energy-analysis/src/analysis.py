import pandas as pd

def check_missing_values(df: pd.DataFrame):
    """Returns a Series with count of missing values per column."""
    return df.isnull().sum()

def check_outliers(df: pd.DataFrame):
    """
    Returns two DataFrames:
    - temp_outliers: rows where temp_avg_c > 54.4°C (130°F) or < -45.6°C (-50°F)
    - energy_outliers: rows where energy_consumption_mwh < 0
    """
    temp_outliers = df[(df['temp_avg_c'] > 54.4) | (df['temp_avg_c'] < -45.6)]
    energy_outliers = df[df['energy_consumption_mwh'] < 0]
    return temp_outliers, energy_outliers

def check_freshness(df: pd.DataFrame):
    """Returns days since latest date in the DataFrame."""
    latest_date = df['date'].max()
    days_old = (pd.Timestamp.now() - latest_date).days
    return days_old