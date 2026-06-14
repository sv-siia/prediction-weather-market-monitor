# # import requests
# # from collections import Counter

# # print("Завантажуємо ВСІ ринки з Polymarket...")
# # print("(це може зайняти хвилину)\n")

# # all_markets = []
# # cursor = None
# # page = 1

# # while True:
# #     params = {}
# #     if cursor:
# #         params["next_cursor"] = cursor
    
# #     response = requests.get("https://clob.polymarket.com/markets", params=params)
# #     data = response.json()
    
# #     markets = data.get("data", [])
# #     all_markets.extend(markets)
    
# #     next_cursor = data.get("next_cursor")
# #     print(f"Сторінка {page}: +{len(markets)} ринків (всього: {len(all_markets)}) | next_cursor: {next_cursor}")
    
# #     # Зупиняємось якщо немає наступної сторінки
# #     if not next_cursor or next_cursor == "LTE=" or len(markets) == 0:
# #         break
    
# #     cursor = next_cursor
# #     page += 1
    
# #     # Захист від нескінченного циклу
# #     if page > 50:
# #         print("Зупинка — більше 50 сторінок")
# #         break

# # print(f"\n✅ Завантажено всього: {len(all_markets)} ринків")
# # print()

# # # Шукаємо weather ринки
# # weather_tag = [m for m in all_markets if "Weather" in (m.get("tags") or [])]
# # precip_tag  = [m for m in all_markets if "Precipitation" in (m.get("tags") or [])]

# # weather_keywords = []
# # for m in all_markets:
# #     q = m.get("question", "").lower()
# #     if any(w in q for w in ["rain", "snow", "temperature", "precipitation", "wind", "weather", "forecast"]):
# #         weather_keywords.append(m)

# # print(f"Ринків з тегом 'Weather': {len(weather_tag)}")
# # print(f"Ринків з тегом 'Precipitation': {len(precip_tag)}")
# # print(f"Ринків з weather словами в питанні: {len(weather_keywords)}")
# # print()

# # # Показуємо всі weather ринки
# # all_weather = {m["condition_id"]: m for m in weather_tag + precip_tag + weather_keywords}.values()
# # print(f"=== ВСЬОГО УНІКАЛЬНИХ WEATHER РИНКІВ: {len(list(all_weather))} ===")
# # print()

# # all_weather = {m["condition_id"]: m for m in weather_tag + precip_tag + weather_keywords}.values()
# # for m in all_weather:
# #     print(f"Питання:  {m['question']}")
# #     print(f"Активний: {m['active']} | Закритий: {m['closed']}")
# #     print(f"Теги:     {m.get('tags')}")
# #     print(f"Дата:     {m.get('game_start_time')}")
# #     for t in m.get("tokens", []):
# #         print(f"  {t['outcome']}: price={t['price']}, winner={t['winner']}")
# #     print()


# # import json

# # # Зберігаємо всі знайдені weather ринки в JSON
# # all_weather_list = list({m["condition_id"]: m for m in weather_tag + precip_tag + weather_keywords}.values())

# # with open("weather_markets_found.json", "w", encoding="utf-8") as f:
# #     json.dump(all_weather_list, f, indent=2, ensure_ascii=False)

# # print(f"✅ Збережено в weather_markets_found.json")
# # print(f"   Файл містить {len(all_weather_list)} ринків")

# # # Також зберігаємо простий текстовий звіт
# # with open("weather_markets_report.txt", "w", encoding="utf-8") as f:
# #     f.write(f"Всього ринків на Polymarket: {len(all_markets)}\n")
# #     f.write(f"Weather ринків знайдено: {len(all_weather_list)}\n\n")
    
# #     for m in all_weather_list:
# #         f.write(f"Питання:  {m['question']}\n")
# #         f.write(f"Активний: {m['active']} | Закритий: {m['closed']}\n")
# #         f.write(f"Теги:     {m.get('tags')}\n")
# #         f.write(f"Дата:     {m.get('game_start_time')}\n")
# #         for t in m.get("tokens", []):
# #             f.write(f"  {t['outcome']}: price={t['price']}, winner={t['winner']}\n")
# #         f.write("\n")

# # print(f"✅ Звіт збережено в weather_markets_report.txt")

# ###############################################################################################################################################################
# import requests
# import json
# from datetime import datetime

# print("Шукаємо weather ринки з конкретними містами і датами...")
# print("Завантажуємо всі 50,000 ринків...\n")

# all_markets = []
# cursor = None
# page = 1

# while True:
#     params = {}
#     if cursor:
#         params["next_cursor"] = cursor
    
#     response = requests.get("https://clob.polymarket.com/markets", params=params)
#     data = response.json()
#     markets = data.get("data", [])
#     all_markets.extend(markets)
    
#     next_cursor = data.get("next_cursor")
#     print(f"Сторінка {page}: всього завантажено {len(all_markets)} ринків")
    
#     if not next_cursor or next_cursor == "LTE=" or len(markets) == 0:
#         break
#     cursor = next_cursor
#     page += 1
#     if page > 50:
#         break

# print(f"\n✅ Всього завантажено: {len(all_markets)} ринків")
# print("\nШукаємо ринки з назвами міст...\n")

# # Список міст які шукаємо
# city_names = [
#     "seattle", "warsaw", "london", "new york", "tokyo",
#     "paris", "berlin", "sydney", "toronto", "miami",
#     "chicago", "los angeles", "houston", "boston",
#     "amsterdam", "madrid", "rome", "vienna", "prague"
# ]

# # Шукаємо ринки де є назва міста І дата
# city_markets = []
# for market in all_markets:
#     question = market.get("question", "").lower()
#     game_start = market.get("game_start_time")
    
#     for city in city_names:
#         if city in question:
#             city_markets.append({
#                 "city_found": city,
#                 "question": market["question"],
#                 "active": market["active"],
#                 "closed": market["closed"],
#                 "game_start_time": game_start,
#                 "tags": market.get("tags"),
#                 "tokens": market.get("tokens", []),
#                 "end_date_iso": market.get("end_date_iso"),
#             })
#             break

# print(f"Ринків з назвами міст: {len(city_markets)}")
# print()

# # Фільтруємо тільки активні або з датою
# active_city = [m for m in city_markets if m["active"] and not m["closed"]]
# with_date   = [m for m in city_markets if m["game_start_time"]]
# today       = datetime.now().strftime("%Y-%m-%d")
# today_markets = [m for m in city_markets if m.get("game_start_time", "") and today in m.get("game_start_time", "")]

# print(f"З яких активні (не закриті): {len(active_city)}")
# print(f"З яких мають game_start_time: {len(with_date)}")
# print(f"З яких на сьогодні ({today}): {len(today_markets)}")
# print()

# # Показуємо всі активні
# print("=== АКТИВНІ РИНКИ З МІСТАМИ ===")
# for m in active_city[:20]:
#     print(f"Місто:    {m['city_found'].upper()}")
#     print(f"Питання:  {m['question']}")
#     print(f"Дата:     {m['game_start_time']}")
#     print(f"Теги:     {m['tags']}")
#     for t in m["tokens"]:
#         print(f"  {t['outcome']}: price={t['price']}")
#     print()

# # Зберігаємо
# with open("city_weather_markets.json", "w", encoding="utf-8") as f:
#     json.dump(city_markets, f, indent=2, ensure_ascii=False)
# print(f"✅ Збережено в city_weather_markets.json")


############################################################################################################

import requests
import json

print("Завантажуємо всі ринки...")
all_markets = []
cursor = None
page = 1

while True:
    params = {}
    if cursor:
        params["next_cursor"] = cursor
    response = requests.get("https://clob.polymarket.com/markets", params=params)
    data = response.json()
    markets = data.get("data", [])
    all_markets.extend(markets)
    next_cursor = data.get("next_cursor")
    print(f"Сторінка {page}: всього {len(all_markets)}")
    if not next_cursor or next_cursor == "LTE=" or len(markets) == 0:
        break
    cursor = next_cursor
    page += 1
    if page > 50:
        break

print(f"\n✅ Всього: {len(all_markets)} ринків\n")

# Шукаємо weather ринки з містами які НЕ закриті
city_names = ["london", "new york", "seattle", "tokyo", "paris",
              "berlin", "miami", "chicago", "boston", "sydney"]

active_weather = []
for market in all_markets:
    question = market.get("question", "").lower()
    tags = market.get("tags") or []
    
    has_city    = any(city in question for city in city_names)
    has_weather = ("Weather" in tags or
                   any(w in question for w in
                       ["temperature", "rain", "snow", "precipitation", "wind"]))
    is_active   = not market.get("closed", True)
    
    if has_city and has_weather:
        active_weather.append(market)

print(f"Weather ринків з містами (всі): {len(active_weather)}")
active_only = [m for m in active_weather if not m["closed"]]
print(f"З яких НЕ закриті: {len(active_only)}")
print()

print("=== ВСІ АКТИВНІ WEATHER РИНКИ З МІСТАМИ ===\n")
for m in active_only:
    print(f"Питання:  {m['question']}")
    print(f"Теги:     {m.get('tags')}")
    print(f"Дата:     {m.get('end_date_iso')}")
    print(f"active={m['active']}, closed={m['closed']}")
    for t in m.get("tokens", []):
        print(f"  {t['outcome']}: price={t['price']}, winner={t['winner']}")
    print()

with open("active_weather_cities.json", "w", encoding="utf-8") as f:
    json.dump(active_only, f, indent=2, ensure_ascii=False)
print(f"✅ Збережено в active_weather_cities.json")