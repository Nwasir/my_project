# ⚡ Weather-Energy Demand Analysis Pipeline

## 📈 Business Context

Energy companies lose **millions of dollars annually** due to inaccurate demand forecasting. Overproduction leads to wasted energy and high operational costs, while underproduction risks blackouts and customer dissatisfaction. One key driver of energy demand is **weather**—especially temperature extremes.

By combining **NOAA climate data** with **EIA energy usage data**, this project builds a **production-ready data pipeline** that enables more accurate demand analysis across major U.S. cities. It helps utility companies, grid operators, and energy analysts better understand how temperature fluctuations affect electricity consumption — paving the way for smarter forecasting, optimized generation, and reduced environmental impact.

---

## 🚀 Project Overview

This project automates the entire process of:

- 🔍 **Collecting weather data** from NOAA Climate Data Online
- ⚡ **Collecting electricity usage data** from the EIA (U.S. Energy Information Administration)
- 🧹 **Cleaning, transforming, and merging** datasets into a unified format
- 📊 **Analyzing temperature-energy relationships**
- ✅ **Exporting production-ready CSV files** for downstream modeling or reporting

---

## 🔧 Technologies Used

| Tool | Purpose |
|------|---------|
| **Python 3.10+** | Core programming language |
| **pandas** | Data loading, cleaning, and manipulation |
| **requests** | API calls to NOAA and EIA |
| **Streamlit** | Web-based interactive dashboard |
| **Plotly** | Interactive charts and visualizations |
| **NumPy** | Numerical operations and arrays |
| **scikit-learn (LinearRegression)** | Simple trend analysis and modeling |
| **datetime** | Date parsing and range handling |
| **logging** | Monitoring and debugging the pipeline |
| **unittest** | Pipeline testing and reliability checks |

---

## 🧪 Features

- ✅ Robust data collection with **retry logic** and error handling
- ✅ Support for multiple cities and flexible date ranges
- ✅ On-the-fly **unit conversion** (tenths of Celsius to Fahrenheit)
- ✅ Data quality checks with summary statistics
- ✅ Modular and testable pipeline design

---

## 🌍 Cities Covered

The pipeline currently tracks weather and energy usage for:

- New York, NY
- Chicago, IL
- Houston, TX
- Phoenix, AZ
- Seattle, WA

---

## 📁 Output Files

- weather data: `data/raw/weather_YYYY-MM-DD_to_YYYY-MM-DD.csv`
- Raw energy data: `data/raw/ergy_YYYY-MM-DD_to_YYYY-MM-DD.csv`
- Cleaned energy data: `data/raw/ergy_YYYY-MM-DD_to_YYYY-MM-DD.csv`
- Merged dataset: `data/processed/merged_YYYY-MM-DD_to_YYYY-MM-DD.csv`
- Quality analysis: Console log summary of rows, columns, and missing values

---

## 📌 How to Run

1. Set your API keys:
   - NOAA: `NOAA_API_TOKEN`
   - EIA: `EIA_API_KEY`

2. Run the pipeline:

```bash
python -m src.pipeline

3. Run the unit test:

```bash
python -m unittest tests/test_pipeline.py

4. Launch the Dashboard
```bash
streamlit run dashboards/app.py

##Suggested Repository Structure

project1-energy-analysis/
├── README.md                 # Business-focused project summary
├── AI_USAGE.md              # AI assistance documentation
├── pyproject.toml           # Dependencies (using uv)
├── config/
│   └── config.yaml          # API keys, cities list
├── src/
│   ├── data_fetcher.py      # API interaction module
│   ├── data_processor.py    # Cleaning and transformation
│   ├── analysis.py          # Statistical analysis
│   └── pipeline.py          # Main orchestration
├── dashboards/
│   └── app.py               # Streamlit application
├── logs/
│   └── pipeline.log         # Execution logs
├── data/
│   ├── raw/                 # Original API responses
│   └── processed/           # Clean, analysis-ready data
├── notebooks/
│   └── exploration.ipynb    # Initial analysis (optional)
├── tests/
│   └── test_pipeline.py     # Basic unit tests
└── video_link.md            # Link to your presentation

