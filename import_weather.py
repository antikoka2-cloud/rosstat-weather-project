# generate_weather_csv.py
import pandas as pd
import requests
import time
import json
import os
from datetime import datetime
try:
    import matplotlib.pyplot as plt
    import seaborn as sns
    PLOTTING_AVAILABLE = True
except ImportError:
    PLOTTING_AVAILABLE = False
    print("⚠️ Установите для графиков: pip install matplotlib seaborn")

# ========== НАСТРОЙКИ ==========
START_YEAR = 2008
END_YEAR = 2025
OUTPUT_FILE = 'weather_value_2008_2025.csv'
CACHE_DIR = 'cache'

# 🔥 КРИТИЧЕСКИ ВАЖНЫЕ НАСТРОЙКИ ДЛЯ ИЗБЕЖАНИЯ 429:
REQUEST_DELAY = 3.0        # Задержка между запросами (сек) — НЕ МЕНЬШЕ 2.0!
TIMEOUT = 45               # Таймаут запроса (сек)
MAX_RETRIES = 5            # Попыток при ошибке
RATE_LIMIT_WAIT = 30       # Пауза при 429 ошибке (сек)

# Индикаторы
INDICATOR_TEMP = 6
INDICATOR_HUMIDITY = 7
INDICATOR_PRECIP = 8

# ========== ЗАГРУЗКА РАЙОНОВ ==========
def load_districts(csv_file='district.csv'):
    df = pd.read_csv(csv_file, encoding='cp1251', header=None, names=['id', 'region_id', 'name'])
    return df

# ========== КООРДИНАТЫ ==========
COORDINATES = {
    # Саратовская область
    1: (48.25, 47.35),   2: (52.05, 43.50),   3: (51.88, 45.00),   4: (52.15, 46.40),
    5: (52.03, 47.80),   6: (51.53, 43.23),   7: (51.95, 46.25),   8: (52.05, 47.38),
    9: (51.90, 48.40),   10: (51.35, 49.40),  11: (52.25, 48.90),  12: (51.82, 42.50),
    13: (51.37, 48.27),  14: (51.85, 49.25),  15: (51.62, 44.50),  16: (50.95, 46.95),
    17: (50.95, 48.12),  18: (51.05, 49.55),  19: (52.17, 46.05),  20: (51.70, 46.75),
    21: (52.15, 45.40),  22: (50.47, 49.93),  23: (50.92, 50.63),  24: (51.72, 50.52),
    25: (51.82, 45.85),  26: (51.10, 47.65),  27: (52.02, 48.80),  28: (50.58, 47.72),
    29: (51.43, 44.25),  30: (52.25, 43.80),  31: (51.35, 43.25),  32: (51.55, 46.00),
    33: (50.88, 47.55),  34: (52.10, 45.73),  35: (51.77, 42.85),  36: (51.25, 48.50),
    37: (52.48, 48.10),  38: (51.48, 46.12),  39: (51.55, 46.00),  40: (51.55, 46.00),
    # Ростовская область
    41: (47.25, 39.85),  42: (47.15, 40.30),  43: (48.17, 40.80),  44: (49.25, 41.73),
    45: (49.85, 41.50),  46: (47.05, 41.55),  47: (47.52, 42.15),  48: (47.08, 42.67),
    49: (46.90, 40.35),  50: (47.08, 43.73),  51: (46.85, 40.30),  52: (47.60, 42.65),
    53: (47.03, 39.18),  54: (48.32, 40.27),  55: (49.30, 40.58),  56: (47.58, 41.10),
    57: (48.30, 40.07),  58: (47.90, 38.78),  59: (47.30, 41.50),  60: (47.58, 38.48),
    61: (48.92, 40.65),  62: (48.85, 41.25),  63: (48.35, 41.80),  64: (47.30, 39.50),
    65: (47.20, 38.75),  66: (48.95, 41.25),  67: (47.35, 40.15),  68: (46.70, 42.15),
    69: (46.40, 40.90),  70: (46.60, 41.70),  71: (47.12, 43.20),  72: (48.05, 39.75),
    73: (46.57, 41.55),  74: (47.52, 40.82),  75: (46.65, 41.00),  76: (49.15, 40.42),
    77: (48.42, 41.17),  78: (47.63, 40.35),  79: (46.57, 41.00),  80: (47.67, 42.15),
    81: (49.50, 40.17),  82: (49.65, 41.27),  83: (47.10, 39.42),  84: (47.15, 39.75),
    85: (47.52, 42.15),  86: (48.05, 39.95),  87: (48.33, 40.12),  88: (48.03, 40.18),
    89: (48.32, 40.27),  90: (47.42, 40.10),  91: (47.75, 39.93),  92: (47.23, 39.72),
    93: (47.23, 38.93),  94: (47.70, 40.22),
}

# ========== ЗАПРОС ДАННЫХ С ЗАЩИТОЙ ОТ 429 ==========
def fetch_year_data(lat, lon, year):
    url = "https://archive-api.open-meteo.com/v1/era5"
    params = {
        'latitude': lat,
        'longitude': lon,
        'start_date': f'{year}-01-01',
        'end_date': f'{year}-12-31',
        'daily': ['temperature_2m_mean', 'relative_humidity_2m_mean', 'precipitation_sum'],
        'timezone': 'Europe/Moscow'
    }

    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(url, params=params, timeout=TIMEOUT)

            # 🔥 Обработка 429 ошибки
            if response.status_code == 429:
                wait = RATE_LIMIT_WAIT * (attempt + 1)
                print(f"\n⚠️ 429! Пауза {wait}сек (попытка {attempt+1}/{MAX_RETRIES})...", end=" ")
                time.sleep(wait)
                continue

            response.raise_for_status()
            return response.json()

        except requests.exceptions.Timeout:
            if attempt < MAX_RETRIES - 1:
                wait = 2 ** attempt
                print(f"\n⏱ Таймаут, пауза {wait}сек...", end=" ")
                time.sleep(wait)
                continue
            return None
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(2)
                continue
            return None

    return None

# ========== ОБРАБОТКА ДАННЫХ ==========
def process_year_data(data):
    if not data or 'daily' not in data:
        return None

    temps = [v for v in data['daily'].get('temperature_2m_mean', []) if v is not None]
    hums = [v for v in data['daily'].get('relative_humidity_2m_mean', []) if v is not None]
    precs = [v for v in data['daily'].get('precipitation_sum', []) if v is not None]

    if not temps:
        return None

    return {
        'avg_temperature': round(sum(temps) / len(temps), 4),
        'avg_humidity': round(sum(hums) / len(hums), 4) if hums else None,
        'total_precipitation': round(sum(precs), 4) if precs else None
    }

# ========== КЭШИРОВАНИЕ (НЕ МЕНЯТЬ!) ==========
def get_cache_path(district_id):
    return os.path.join(CACHE_DIR, f'district_{district_id}.json')

def load_from_cache(district_id):
    path = get_cache_path(district_id)
    if os.path.exists(path):
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    return None

def save_to_cache(district_id, data):
    os.makedirs(CACHE_DIR, exist_ok=True)
    path = get_cache_path(district_id)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

# ========== ГЛАВНАЯ ФУНКЦИЯ ==========
def generate_csv():
    print("=" * 70)
    print("🌤  ГЕНЕРАЦИЯ КЛИМАТИЧЕСКИХ ДАННЫХ 2008-2025")
    print("=" * 70)

    # 🔥 Пауза для сброса rate limit сервера
    print("⏳ Пауза 30 сек для сброса лимитов API...")
    time.sleep(30)

    districts = load_districts()
    print(f"📋 Загружено районов: {len(districts)}")
    print(f"📁 Кэш-папка: {CACHE_DIR}")
    print("=" * 70)

    all_records = []
    processed = 0
    failed = 0

    for _, row in districts.iterrows():
        district_id = int(row['id'])
        district_name = row['name']

        if district_id not in COORDINATES:
            print(f"[{district_id:3d}] ️Нет координат: {district_name}")
            failed += 1
            continue

        lat, lon = COORDINATES[district_id]

        # 🔥 ПРОВЕРКА КЭША — ГЛАВНОЕ: НЕ ПЕРЕЗАПИСЫВАЕМ!
        cached = load_from_cache(district_id)
        if cached:
            print(f"[{district_id:3d}] ️  Кэш: {district_name[:35]}")
            for year, values in cached.items():
                year = int(year)
                if values.get('avg_temperature') is not None:
                    all_records.append({'district_id': district_id, 'indicator_id': INDICATOR_TEMP, 'year': year, 'value': values['avg_temperature']})
                if values.get('avg_humidity') is not None:
                    all_records.append({'district_id': district_id, 'indicator_id': INDICATOR_HUMIDITY, 'year': year, 'value': values['avg_humidity']})
                if values.get('total_precipitation') is not None:
                    all_records.append({'district_id': district_id, 'indicator_id': INDICATOR_PRECIP, 'year': year, 'value': values['total_precipitation']})
            processed += 1
            continue

        # Запрос новых данных
        print(f"[{district_id:3d}]  {district_name[:35]}...", end=" ")
        yearly_results = {}

        for year in range(START_YEAR, END_YEAR + 1):
            data = fetch_year_data(lat, lon, year)
            if data:
                processed_data = process_year_data(data)
                if processed_data:
                    yearly_results[year] = processed_data
                    print(f"{year}✓", end=" ")
                else:
                    print(f"{year}✗", end=" ")
            else:
                print(f"{year}!", end=" ")
            time.sleep(REQUEST_DELAY)  # 🔥 Задержка между запросами

        print()

        # Сохранение в кэш ТОЛЬКО если есть новые данные
        if yearly_results:
            save_to_cache(district_id, yearly_results)
            for year, values in yearly_results.items():
                if values.get('avg_temperature') is not None:
                    all_records.append({'district_id': district_id, 'indicator_id': INDICATOR_TEMP, 'year': year, 'value': values['avg_temperature']})
                if values.get('avg_humidity') is not None:
                    all_records.append({'district_id': district_id, 'indicator_id': INDICATOR_HUMIDITY, 'year': year, 'value': values['avg_humidity']})
                if values.get('total_precipitation') is not None:
                    all_records.append({'district_id': district_id, 'indicator_id': INDICATOR_PRECIP, 'year': year, 'value': values['total_precipitation']})
            processed += 1
        else:
            print(f"    ❌ Нет данных")
            failed += 1

        # Промежуточное сохранение
        if processed % 10 == 0 and all_records:
            df_temp = pd.DataFrame(all_records, columns=['district_id', 'indicator_id', 'year', 'value'])
            df_temp.to_csv(OUTPUT_FILE, index=False, encoding='utf-8-sig')
            print(f"     Сохранено: {len(all_records)} записей")

    # Финальное сохранение
    df = pd.DataFrame(all_records, columns=['district_id', 'indicator_id', 'year', 'value'])
    df = df.sort_values(['district_id', 'indicator_id', 'year'])
    df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8-sig')

    print("\n" + "=" * 70)
    print("✅ ГОТОВО!")
    print("=" * 70)
    print(f"📁 Файл: {OUTPUT_FILE}")
    print(f"📊 Записей: {len(df)}")
    print(f"✅ Успешно: {processed}")
    print(f"❌ Ошибки: {failed}")
    if len(df) > 0:
        print(f"\n📋 Пример:")
        print(df.head(12).to_string(index=False))

    return df

# ========== ЗАПУСК ==========
if __name__ == "__main__":
    try:
        generate_csv()
    except KeyboardInterrupt:
        print("\n⚠️ Прервано. Данные сохранены в кэше и CSV.")
    except Exception as e:
        print(f"\n❌ Ошибка: {e}")
        raise


def print_statistics(df):
    """Вывод статистики по данным"""
    print("\n" + "="*70)
    print("📊 СТАТИСТИКА ПО ДАННЫМ")
    print("="*70)

    # Общая информация
    print(f" Всего записей: {len(df):,}")
    print(f"Районов: {df['district_id'].nunique()}")
    print(f"Период: {df['year'].min()} - {df['year'].max()}")
    print(f" Индикаторов: {df['indicator_id'].nunique()}")

    # Статистика по индикаторам
    print(f" Статистика по показателям:")
    indicators = {6: 'Температура (°C)', 7: 'Влажность (%)', 8: 'Осадки (мм)'}

    for ind_id, ind_name in indicators.items():
        data = df[df['indicator_id'] == ind_id]['value']
        if len(data) > 0:
            print(f"\n{ind_name}:")
            print(f"   Среднее: {data.mean():.2f}")
            print(f"   Медиана: {data.median():.2f}")
            print(f"   Мин: {data.min():.2f}")
            print(f"   Макс: {data.max():.2f}")
            print(f"   Std: {data.std():.2f}")

    # Топ-5 самых тёплых/холодных районов (по температуре)
    temp_data = df[df['indicator_id'] == 6].groupby('district_id')['value'].mean()
    print(f"\n🌡️ Топ-5 самых тёплых районов (средняя температура):")
    for district_id, avg_temp in temp_data.nlargest(5).items():
        print(f"   ID {district_id}: {avg_temp:.2f}°C")

    print(f"\n❄️ Топ-5 самых холодных районов:")
    for district_id, avg_temp in temp_data.nsmallest(5).items():
        print(f"   ID {district_id}: {avg_temp:.2f}°C")

    # Статистика по годам (тренд температуры)
    yearly_temp = df[(df['indicator_id'] == 6)].groupby('year')['value'].mean()
    print(f"\n Тренд средней температуры по годам:")
    for year, temp in yearly_temp.items():
        change = "↑" if year > yearly_temp.index.min() and temp > yearly_temp[year-1] else "↓" if year > yearly_temp.index.min() else ""
        print(f"   {year}: {temp:.2f}°C {change}")

print_statistics()
