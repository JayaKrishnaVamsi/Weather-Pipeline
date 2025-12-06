# 🌤️ Weather Data Pipeline (Python + Supabase + Power BI)

A fully automated **ETL pipeline** that fetches live weather data for multiple cities, cleans and transforms it, stores it in **Parquet files** and in **Supabase cloud storage**, and visualizes insights using a **Power BI dashboard**.

This project demonstrates real-world **data engineering skills**: API ingestion, batch processing, data modeling, anomaly detection, cloud integration, and dashboard automation.

---

## 🚀 Features

### ✔ 1. Automated Weather Fetching
- Uses **Open-Meteo API** to extract hourly & daily weather data.
- Supports **multiple cities** (Delhi, Mumbai, Bangalore, Hyderabad, etc.).
- Handles retries and network/API failures gracefully.

### ✔ 2. Data Cleaning & Transformation
- Converts timestamps into local date/time fields.
- Creates **temperature buckets** (Cold / Normal / Hot).
- Adds derived fields:
  - `is_temp_anomaly`
  - `temp_bucket`
  - `humidity_pct`
  - `time_of_day` (Morning / Afternoon / Evening / Night)

### ✔ 3. Parquet-Based Data Lake
The pipeline maintains two optimized analytic tables:
- `master_weather_hourly.parquet`
- `master_weather_daily.parquet`

They update incrementally on each run—no duplication.

### ✔ 4. Cloud Storage via Supabase
Each run:
- Uploads cleaned CSV output to a Supabase Storage bucket.
- Organizes files using timestamped folder names.
- Ensures successful uploads with error-handling & retries.

### ✔ 5. Power BI Dashboard
An interactive dashboard built on top of the Parquet files:
- Hourly & daily temperature trends  
- Humidity trends  
- Temperature bucket visualization  
- Anomaly detection graph  
- KPI cards for min/max temperature  
- Slicers for Cities / Dates / Hours  

### ✔ 6. Optional Automation
Automate the pipeline using:
- **Windows Task Scheduler**, or  
- **GitHub Actions** (if cloud billing is enabled)

---

## 🧱 Architecture Overview
```
        ┌────────── Weather API (Open-Meteo) ──────────┐
        │                                               │
        ▼                                               │
  weather_pipeline.py  (ETL Script)                     │
 ┌──────────────────────────────────────────────────────┘
 │
 ├── Extract  → Fetch weather data for all cities
 ├── Transform → Clean, enrich & detect anomalies
 ├── Load      → Save Parquet & upload CSV to Supabase
 │
 └── Trigger   → Manual run / GitHub Actions / Task Scheduler
```

---

## ⚙️ Setup Instructions


```bash
git clone https://github.com/JayaKrishnaVamsi/Weather-Pipeline.git
cd Weather-Pipeline

python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt

SUPABASE_URL=<your_supabase_url>
SUPABASE_SERVICE_KEY=<your_service_role_key>

python weather_pipeline.py
```

