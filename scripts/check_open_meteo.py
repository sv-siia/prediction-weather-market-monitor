import requests
import json
from datetime import datetime, timezone

# Міста з наших synthetic даних
cities = [
    {"name": "Seattle",  "lat": 47.6062,  "lon": -122.3321},
    {"name": "Warsaw",   "lat": 52.2297,  "lon":  21.0122},
    {"name": "London",   "lat": 51.5074,  "lon":  -0.1278},
    {"name": "New York", "lat": 40.7128,  "lon": -74.0060},
    {"name": "Tokyo",    "lat": 35.6762,  "lon": 139.6503},
]

results = []

for city in cities:
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude":  city["lat"],
        "longitude": city["lon"],
        "current":   "temperature_2m,precipitation,rain,weather_code,wind_speed_10m,relative_humidity_2m"
    }
    
    response = requests.get(url, params=params)
    data = response.json()
    
    # Додаємо поля які додає наш producer
    data["LOCATION_NAME"]   = city["name"]
    data["POLL_TIMESTAMP"]  = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    
    results.append(data)
    
    # Виводимо красиво
    current = data["current"]
    code = current["weather_code"]
    is_raining = code in [51,53,55,61,63,65,80,81,82]
    is_snowing = code in [71,73,75,77]
    
    condition = "☔ ДОЩ" if is_raining else ("❄️ СНІГ" if is_snowing else "☀️ СУХО")
    
    print(f"🌍 {city['name']}")
    print(f"   Температура:  {current['temperature_2m']}°C")
    print(f"   Опади:        {current['precipitation']} mm")
    print(f"   Weather code: {code} → {condition}")
    print(f"   Вітер:        {current['wind_speed_10m']} km/h")
    print(f"   Вологість:    {current['relative_humidity_2m']}%")
    print(f"   Час:          {current['time']}")
    print()

# Зберігаємо в файл
with open("weather_actuals_sample.json", "w", encoding="utf-8") as f:
    json.dump(results, f, indent=2, ensure_ascii=False)

print(f"✅ Збережено в weather_actuals_sample.json")
print(f"   Це точно той формат що потрібен нашому weather producer!")

