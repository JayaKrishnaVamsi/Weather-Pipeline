import pandas as pd

hourly = pd.read_parquet("data/master_weather_hourly.parquet")
daily = pd.read_parquet("data/master_weather_daily.parquet")

print("Cities:", hourly["city"].unique())
print("Date range:", hourly["timestamp"].min(), "→", hourly["timestamp"].max())

# How many anomalies per city?
anomalies = hourly[hourly["is_temp_anomaly"]]
print("\nTemperature anomalies per city:")
print(anomalies.groupby("city")["timestamp"].count())

# Average temperature per city
print("\nAverage temperature per city:")
print(daily.groupby("city")["avg_temp"].mean())
