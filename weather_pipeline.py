import os
import logging
from datetime import datetime

import pandas as pd
import requests
from dotenv import load_dotenv
from supabase import create_client, Client

# --------------------------------------------------------------------
# Setup & configuration
# --------------------------------------------------------------------
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
BUCKET_NAME = os.getenv("SUPABASE_BUCKET", "weather-pipeline")
CITIES_CSV = os.getenv("CITIES_CSV", "cities.csv")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY not set in .env")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

BASE_URL = "https://api.open-meteo.com/v1/forecast"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("weather_pipeline")


# --------------------------------------------------------------------
# Core functions
# --------------------------------------------------------------------
def fetch_weather_for_city(city: str, lat: float, lon: float) -> dict:
    """Call Open-Meteo API for a single city and return JSON."""
    log.info("Fetching weather for %s (lat=%.4f, lon=%.4f)...", city, lat, lon)

    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "temperature_2m,relativehumidity_2m,windspeed_10m",
        "timezone": "auto",
    }

    resp = requests.get(BASE_URL, params=params)
    resp.raise_for_status()
    data = resp.json()

    log.info("API call successful for %s", city)
    return data


def transform(data: dict, city: str) -> pd.DataFrame:
    """Transform raw JSON for one city into a tidy DataFrame."""
    hourly = data["hourly"]

    df = pd.DataFrame(
        {
            "timestamp": hourly["time"],
            "temperature_c": hourly["temperature_2m"],
            "humidity_pct": hourly["relativehumidity_2m"],
            "windspeed_ms": hourly["windspeed_10m"],
        }
    )

    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["city"] = city

    df["date"] = df["timestamp"].dt.date
    df["hour"] = df["timestamp"].dt.hour

    # Temperature bucket: Cold / Moderate / Hot
    df["temp_bucket"] = pd.cut(
        df["temperature_c"],
        bins=[-100, 15, 30, 100],
        labels=["Cold", "Moderate", "Hot"],
    )

    # Time-of-day bucket
    df["time_of_day"] = pd.cut(
        df["hour"],
        bins=[-1, 5, 11, 17, 21, 24],
        labels=["Night", "Morning", "Afternoon", "Evening", "Late Night"],
    )

    log.info("Transformed data for %s – %d rows", city, len(df))
    return df


def add_temperature_anomaly_flags(df: pd.DataFrame) -> pd.DataFrame:
    """Add simple z-score based anomaly flag per city."""
    df = df.copy()

    def _anomaly_for_city(group: pd.DataFrame) -> pd.DataFrame:
        mean = group["temperature_c"].mean()
        std = group["temperature_c"].std(ddof=0) or 1.0
        group["temp_z"] = (group["temperature_c"] - mean) / std
        group["is_temp_anomaly"] = group["temp_z"].abs() > 2.5
        return group

    df = df.groupby("city", group_keys=False).apply(_anomaly_for_city)
    return df


def make_daily_aggregates(df: pd.DataFrame) -> pd.DataFrame:
    """Create daily aggregates (min/max/avg temp & avg humidity) per city."""
    daily = (
        df.groupby(["city", "date"])
        .agg(
            avg_temp=("temperature_c", "mean"),
            min_temp=("temperature_c", "min"),
            max_temp=("temperature_c", "max"),
            avg_humidity=("humidity_pct", "mean"),
            hours_hot=("temp_bucket", lambda x: (x == "Hot").sum()),
            hours_cold=("temp_bucket", lambda x: (x == "Cold").sum()),
        )
        .reset_index()
    )

    log.info("Created daily aggregates – %d rows", len(daily))
    return daily


def save_run_files(df_hourly: pd.DataFrame, df_daily: pd.DataFrame) -> tuple[str, str]:
    """Save cleaned hourly data and daily aggregates for this run."""
    os.makedirs("output", exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    hourly_path = os.path.join("output", f"weather_hourly_run_{ts}.csv")
    daily_path = os.path.join("output", f"weather_daily_run_{ts}.csv")

    df_hourly.to_csv(hourly_path, index=False)
    df_daily.to_csv(daily_path, index=False)

    log.info("Saved hourly CSV at %s", hourly_path)
    log.info("Saved daily CSV at %s", daily_path)

    return hourly_path, daily_path


def update_master_parquet(df_hourly: pd.DataFrame, df_daily: pd.DataFrame) -> None:
    """Append to / update master Parquet history (hourly & daily)."""
    os.makedirs("data", exist_ok=True)

    hourly_master_path = os.path.join("data", "master_weather_hourly.parquet")
    daily_master_path = os.path.join("data", "master_weather_daily.parquet")

    # Hourly
    if os.path.exists(hourly_master_path):
        old_hourly = pd.read_parquet(hourly_master_path)
        combined_hourly = (
            pd.concat([old_hourly, df_hourly], ignore_index=True)
            .drop_duplicates(subset=["city", "timestamp"])
            .sort_values(["city", "timestamp"])
        )
    else:
        combined_hourly = df_hourly.sort_values(["city", "timestamp"])

    combined_hourly.to_parquet(hourly_master_path, index=False)
    log.info("Updated master hourly Parquet at %s", hourly_master_path)

    # Daily
    if os.path.exists(daily_master_path):
        old_daily = pd.read_parquet(daily_master_path)
        combined_daily = (
            pd.concat([old_daily, df_daily], ignore_index=True)
            .drop_duplicates(subset=["city", "date"])
            .sort_values(["city", "date"])
        )
    else:
        combined_daily = df_daily.sort_values(["city", "date"])

    combined_daily.to_parquet(daily_master_path, index=False)
    log.info("Updated master daily Parquet at %s", daily_master_path)


def upload_to_supabase(local_path: str, remote_prefix: str = "weather/") -> None:
    """Upload a local file to Supabase Storage under remote_prefix."""
    log.info("Uploading %s to Supabase bucket '%s' ...", local_path, BUCKET_NAME)

    with open(local_path, "rb") as f:
        file_bytes = f.read()

    remote_path = remote_prefix + os.path.basename(local_path)

    res = supabase.storage.from_(BUCKET_NAME).upload(remote_path, file_bytes)
    log.info("Upload complete. Remote path: %s; response: %s", remote_path, res)


# --------------------------------------------------------------------
# Main pipeline
# --------------------------------------------------------------------
def main():
    log.info("Starting weather ETL pipeline")

    if not os.path.exists(CITIES_CSV):
        raise FileNotFoundError(f"{CITIES_CSV} not found. Create cities.csv with city,lat,lon.")

    cities_df = pd.read_csv(CITIES_CSV)

    all_hourly_frames: list[pd.DataFrame] = []

    # Fetch + transform per city
    for _, row in cities_df.iterrows():
        city = row["city"]
        lat = float(row["lat"])
        lon = float(row["lon"])

        try:
            raw = fetch_weather_for_city(city, lat, lon)
            df_city = transform(raw, city)
            all_hourly_frames.append(df_city)
        except Exception as e:
            log.exception("Failed processing city %s: %s", city, e)

    if not all_hourly_frames:
        log.error("No city data processed. Exiting.")
        return

    # Combine all cities into one DataFrame
    df_hourly = pd.concat(all_hourly_frames, ignore_index=True)
    df_hourly = add_temperature_anomaly_flags(df_hourly)

    # Daily aggregates across cities
    df_daily = make_daily_aggregates(df_hourly)

    # Save run-level CSVs
    hourly_path, daily_path = save_run_files(df_hourly, df_daily)

    # Update master Parquet histories
    update_master_parquet(df_hourly, df_daily)

    # Upload latest run CSVs to Supabase
    upload_to_supabase(hourly_path, remote_prefix="weather/hourly/")
    upload_to_supabase(daily_path, remote_prefix="weather/daily/")

    log.info("Weather ETL pipeline completed successfully.")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        log.exception("Pipeline failed: %s", exc)
        raise
