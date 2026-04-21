# -*- coding: utf-8 -*-
import sqlite3
import pandas as pd
from pathlib import Path

print("="*70)
print("📋 ПОЭТАПНЫЙ ИМПОРТ ДАННЫХ")
print("="*70)

DB_FILE = Path(__file__).parent / 'main.db'
YEARLY_CSV = Path(__file__).parent / 'weather_value_2008_2025.csv'
MONTHLY_CSV = Path(__file__).parent / 'monthly_temperature_2008_2025.csv'

conn = sqlite3.connect(DB_FILE)
cursor = conn.cursor()

# ========== ЭТАП 1: ОЧИСТКА ТАБЛИЦЫ ==========
print("\n" + "="*70)
print("ЭТАП 1: ОЧИСТКА ТАБЛИЦЫ weather_value")
print("="*70)

cursor.execute("SELECT COUNT(*) FROM weather_value")
count_before = cursor.fetchone()[0]
print(f"📊 Записей до очистки: {count_before:,}")

cursor.execute("DELETE FROM weather_value")
conn.commit()

cursor.execute("SELECT COUNT(*) FROM weather_value")
count_after = cursor.fetchone()[0]
print(f"✅ Таблица очищена (осталось записей: {count_after})")

# ========== ЭТАП 2: ВСТАВКА ГОДОВЫХ ДАННЫХ ==========
print("\n" + "="*70)
print("ЭТАП 2: ВСТАВКА ГОДОВЫХ ДАННЫХ (weather_value_2008_2025.csv)")
print("="*70)

df_yearly = pd.read_csv(YEARLY_CSV, encoding='utf-8-sig')
print(f"📊 Прочитано строк из CSV: {len(df_yearly):,}")
print(f"📋 Колонки: {list(df_yearly.columns)}")

# Добавляем month = NULL для годовых данных
df_yearly['month'] = None

# Вставляем данные
records_yearly = [tuple(row) for row in df_yearly[['district_id', 'indicator_id', 'year', 'month', 'value']].values]

cursor.executemany('''
    INSERT INTO weather_value (district_id, indicator_id, year, month, value)
    VALUES (?, ?, ?, ?, ?)
''', records_yearly)

conn.commit()

cursor.execute("SELECT COUNT(*) FROM weather_value")
total = cursor.fetchone()[0]
print(f"✅ Импортировано {len(records_yearly):,} записей")
print(f"📊 Всего в таблице: {total:,} записей")

# Проверка по индикаторам
cursor.execute("""
    SELECT indicator_id, COUNT(*) 
    FROM weather_value 
    GROUP BY indicator_id
""")
print("\n📈 По индикаторам:")
for row in cursor.fetchall():
    ind_names = {6: 'Температура (годовая)', 7: 'Влажность', 8: 'Осадки'}
    print(f"   ID {row[0]} ({ind_names.get(row[0], '')}): {row[1]:,}")

# ========== ЭТАП 3: УДАЛЕНИЕ ГОДОВОЙ ТЕМПЕРАТУРЫ (indicator_id = 6) ==========
print("\n" + "="*70)
print("ЭТАП 3: УДАЛЕНИЕ ГОДОВОЙ ТЕМПЕРАТУРЫ (indicator_id = 6)")
print("="*70)

cursor.execute("SELECT COUNT(*) FROM weather_value WHERE indicator_id = 6")
count_temp = cursor.fetchone()[0]
print(f"📊 Записей с indicator_id = 6: {count_temp:,}")

cursor.execute("DELETE FROM weather_value WHERE indicator_id = 6")
conn.commit()

cursor.execute("SELECT COUNT(*) FROM weather_value")
total_after = cursor.fetchone()[0]
print(f"✅ Удалено {count_temp:,} записей")
print(f"📊 Осталось в таблице: {total_after:,} записей")

# ========== ЭТАП 4: ВСТАВКА МЕСЯЧНОЙ ТЕМПЕРАТУРЫ ==========
print("\n" + "="*70)
print("ЭТАП 4: ВСТАВКА МЕСЯЧНОЙ ТЕМПЕРАТУРЫ (monthly_temperature_2008_2025.csv)")
print("="*70)

df_monthly = pd.read_csv(MONTHLY_CSV, encoding='utf-8-sig')
print(f"📊 Прочитано строк из CSV: {len(df_monthly):,}")
print(f"📋 Колонки: {list(df_monthly.columns)}")

# Вставляем данные
records_monthly = [tuple(row) for row in df_monthly[['district_id', 'indicator_id', 'year', 'month', 'value']].values]

cursor.executemany('''
    INSERT INTO weather_value (district_id, indicator_id, year, month, value)
    VALUES (?, ?, ?, ?, ?)
''', records_monthly)

conn.commit()

cursor.execute("SELECT COUNT(*) FROM weather_value")
total_final = cursor.fetchone()[0]
print(f"✅ Импортировано {len(records_monthly):,} записей")
print(f"📊 Всего в таблице: {total_final:,} записей")

# ========== ФИНАЛЬНАЯ ПРОВЕРКА ==========
print("\n" + "="*70)
print("📊 ФИНАЛЬНАЯ ПРОВЕРКА")
print("="*70)

# По индикаторам
cursor.execute("""
    SELECT indicator_id, COUNT(*) 
    FROM weather_value 
    GROUP BY indicator_id
    ORDER BY indicator_id
""")
print("\n📈 По индикаторам:")
ind_names = {6: 'Температура (месячная)', 7: 'Влажность (годовая)', 8: 'Осадки (годовые)'}
for row in cursor.fetchall():
    print(f"   ID {row[0]} {ind_names.get(row[0], '')}: {row[1]:,}")

# По наличию month
cursor.execute("""
    SELECT 
        CASE WHEN month IS NULL THEN 'Годовые данные' ELSE 'Месячные данные' END,
        COUNT(*)
    FROM weather_value
    GROUP BY CASE WHEN month IS NULL THEN 'Годовые данные' ELSE 'Месячные данные' END
""")
print("\n📅 По типу данных:")
for row in cursor.fetchall():
    print(f"   {row[0]}: {row[1]:,}")

# Пример данных
print("\n📄 Пример данных (10 строк):")
cursor.execute("""
    SELECT district_id, indicator_id, year, month, value
    FROM weather_value
    LIMIT 10
""")
for row in cursor.fetchall():
    print(f"   {row}")

conn.close()

print("\n" + "="*70)
print("✅ ВСЕ ЭТАПЫ ЗАВЕРШЕНЫ!")
print("="*70)
print("\n📋 ИТОГ:")
print(f"   - Годовые данные (влажность, осадки): month = NULL")
print(f"   - Месячная температура: month = 1-12")
print(f"   - Всего записей: {total_final:,}")
