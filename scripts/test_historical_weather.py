import requests
import json

# Перевіряємо реальну температуру в NYC 23 січня 2025
url = "https://archive-api.open-meteo.com/v1/archive"
params = {
    "latitude":   40.7128,
    "longitude":  -74.0060,
    "start_date": "2025-01-23",
    "end_date":   "2025-01-23",
    "daily":      "temperature_2m_max,temperature_2m_min,precipitation_sum",
    "timezone":   "America/New_York"
}

response = requests.get(url, params=params)
data = response.json()
print(json.dumps(data, indent=2))

import requests
import json

# London 23 лютого 2025 (один з наших ринків)
url = "https://archive-api.open-meteo.com/v1/archive"
params = {
    "latitude":   51.5074,
    "longitude":  -0.1278,
    "start_date": "2025-02-02",
    "end_date":   "2025-02-02",
    "daily":      "temperature_2m_max,temperature_2m_min,precipitation_sum",
    "timezone":   "Europe/London"
}

response = requests.get(url, params=params)
data = response.json()

daily = data["daily"]
temp_max_c = daily["temperature_2m_max"][0]
temp_max_f = round(temp_max_c * 9/5 + 32, 1)

print(f"London Feb 2, 2025:")
print(f"  Max temp: {temp_max_c}°C = {temp_max_f}°F")
print(f"  Min temp: {daily['temperature_2m_min'][0]}°C")
print(f"  Precipitation: {daily['precipitation_sum'][0]}mm")
print()
print("Market question:")
print("  'Will highest temp in London be between 41-42°F on Feb 2?'")
print(f"  Threshold: 41-42°F")
print(f"  Actual: {temp_max_f}°F")
result = 41 <= temp_max_f <= 42
print(f"  ACTUAL_OUTCOME: {1 if result else 0} ({'Yes' if result else 'No'})")