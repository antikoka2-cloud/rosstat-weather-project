# -*- coding: utf-8 -*-
# import_to_weather_value.py — Импорт данных В ТАБЛИЦУ weather_value

import sqlite3
import pandas as pd
from pathlib import Path

print("="*70)
print("🗄️  ИМПОРТ ДАННЫХ В ТАБЛИЦУ weather_value")
print("="*70)

# Пути
BASE_DIR = Path(__file__).parent
DB_FILE = BASE_DIR / 'main.db'
YEARLY_CSV = BASE_DIR / 'weather_value_2008_2025.csv'
MONTHLY_CSV = BASE_DIR / 'monthly_temperature_2008_2025.csv'

print(f"\n📁 База данных: {DB_FILE}")
print(f"📁 Годовые данные: {YEARLY_CSV}")
print(f"📁 Месячные данные: {MONTHLY_CSV}")

# Проверка файлов
for file in [DB_FILE, YEARLY_CSV]:
    if file.exists():
        print(f"   ✅ {file.name} существует")
    else:
        print(f"   ❌ {file.name} НЕ НАЙДЕН!")
        exit()

# Подключение к БД
conn = sqlite3.connect(DB_FILE)
cursor = conn.cursor()

# ========== 1. ПРОВЕРКА ТАБЛИЦЫ weather_value ==========
print("\n" + "="*70)
print("1️⃣ ПРОВЕРКА ТАБЛИЦЫ weather_value")
print("="*70)

cursor.execute("""
    SELECT name FROM sqlite_master 
    WHERE type='table' AND name='weather_value'
""")
if cursor.fetchone():
    print("   ✅ Таблица weather_value существует")
else:
    print("   ❌ Таблица weather_value НЕ СУЩЕСТВУЕТ!")
    print("   Создаю таблицу...")
    cursor.execute("""
        CREATE TABLE weather_value (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            district_id INTEGER NOT NULL,
            indicator_id INTEGER NOT NULL,
            year INTEGER NOT NULL,
            month INTEGER,
            value NUMERIC(18,4) NOT NULL
        )
    """)
    conn.commit()
    print("   ✅ Таблица создана")

# ========== 2. ДОБАВЛЕНИЕ КОЛОНКИ month ==========
print("\n" + "="*70)
print("2️⃣ ДОБАВЛЕНИЕ КОЛОНКИ month")
print("="*70)

try:
    cursor.execute("ALTER TABLE weather_value ADD COLUMN month INTEGER")
    print("   ✅ Колонка month добавлена")
    conn.commit()
except sqlite3.OperationalError as e:
    if 'duplicate column name' in str(e):
        print("   ⚠️ Колонка month уже существует")
    else:
        print(f"   ❌ Ошибка: {e}")

# ========== 3. ОЧИСТКА ТАБЛИЦЫ ==========
print("\n" + "="*70)
print("3️⃣ ОЧИСТКА ТАБЛИЦЫ weather_value")
print("="*70)

cursor.execute("SELECT COUNT(*) FROM weather_value")
old_count = cursor.fetchone()[0]
print(f"   📊 Записей до очистки: {old_count:,}")

cursor.execute("DELETE FROM weather_value")
conn.commit()
print(f"   🗑️ Таблица очищена")

# ========== 4. ИМПОРТ ГОДОВЫХ ДАННЫХ ==========
print("\n" + "="*70)
print("4️⃣ ИМПОРТ ГОДОВЫХ ДАННЫХ (month = NULL)")
print("="*70)

if YEARLY_CSV.exists():
    df = pd.read_csv(YEARLY_CSV, encoding='utf-8-sig')
    print(f"   📊 Прочитано строк из CSV: {len(df):,}")
    print(f"   📋 Колонки: {list(df.columns)}")

    # Добавить month = NULL для годовых данных
    df['month'] = None

    # Подготовка данных
    records = []
    for _, row in df.iterrows():
        records.append((
            int(row['district_id']),
            int(row['indicator_id']),
            int(row['year']),
            row['month'],
            float(row['value'])
        ))

    # Вставка в таблицу weather_value
    cursor.executemany('''
        INSERT INTO weather_value (district_id, indicator_id, year, month, value)
        VALUES (?, ?, ?, ?, ?)
    ''', records)

    conn.commit()
    print(f"   ✅ Импортировано {len(records):,} записей В weather_value")
else:
    print(f"   ❌ Файл не найден!")

# ========== 5. ИМПОРТ МЕСЯЧНЫХ ДАННЫХ ==========
print("\n" + "="*70)
print("5️⃣ ИМПОРТ МЕСЯЧНЫХ ДАННЫХ (month = 1-12)")
print("="*70)

if MONTHLY_CSV.exists():
    df = pd.read_csv(MONTHLY_CSV, encoding='utf-8-sig')
    print(f"   📊 Прочитано строк из CSV: {len(df):,}")

    records = []
    for _, row in df.iterrows():
        records.append((
            int(row['district_id']),
            int(row['indicator_id']),
            int(row['year']),
            int(row['month']),
            float(row['value'])
        ))

    cursor.executemany('''
        INSERT INTO weather_value (district_id, indicator_id, year, month, value)
        VALUES (?, ?, ?, ?, ?)
    ''', records)

    conn.commit()
    print(f"   ✅ Импортировано {len(records):,} записей В weather_value")
else:
    print(f"   ⚠️ Файл не найден (пропущено)")

# ========== 6. СОЗДАНИЕ ИНДЕКСОВ ==========
print("\n" + "="*70)
print("6️⃣ СОЗДАНИЕ ИНДЕКСОВ")
print("="*70)

indexes = [
    'CREATE INDEX IF NOT EXISTS idx_weather_district ON weather_value(district_id)',
    'CREATE INDEX IF NOT EXISTS idx_weather_year ON weather_value(year)',
    'CREATE INDEX IF NOT EXISTS idx_weather_indicator ON weather_value(indicator_id)',
    'CREATE INDEX IF NOT EXISTS idx_weather_month ON weather_value(month)',
    'CREATE INDEX IF NOT EXISTS idx_weather_full ON weather_value(district_id, indicator_id, year, month)'
]

for idx_sql in indexes:
    cursor.execute(idx_sql)

conn.commit()
print("   ✅ Индексы созданы")

# ========== 7. ПРОВЕРКА РЕЗУЛЬТАТА ==========
print("\n" + "="*70)
print("7️⃣ ПРОВЕРКА РЕЗУЛЬТАТА")
print("="*70)

# Всего записей
cursor.execute("SELECT COUNT(*) FROM weather_value")
total = cursor.fetchone()[0]
print(f"📋 Всего записей в weather_value: {total:,}")

# По типам данных
cursor.execute("""
    SELECT 
        CASE WHEN month IS NULL THEN 'Годовые' ELSE 'Месячные' END as type,
        COUNT(*) as count
    FROM weather_value
    GROUP BY type
""")
print(f"\n📈 По типу данных:")
for row in cursor.fetchall():
    print(f"   {row[0]}: {row[1]:,}")

# Период
cursor.execute("SELECT MIN(year), MAX(year) FROM weather_value")
min_y, max_y = cursor.fetchone()
print(f"\n📅 Период: {min_y} - {max_y}")

# Районы
cursor.execute("SELECT COUNT(DISTINCT district_id) FROM weather_value")
print(f"🏘️ Районов: {cursor.fetchone()[0]}")

# Индикаторы
cursor.execute("SELECT COUNT(DISTINCT indicator_id) FROM weather_value")
print(f"📈 Индикаторов: {cursor.fetchone()[0]}")

# Пример данных
print(f"\n📄 Пример данных (10 строк) ИЗ weather_value:")
cursor.execute("SELECT * FROM weather_value LIMIT 10")
for row in cursor.fetchall():
    print(f"   {row}")

# ========== 8. ПРОВЕРКА ЧТО НЕ ПОПАЛО В ДРУГИЕ ТАБЛИЦЫ ==========
print("\n" + "="*70)
print("8️⃣ ПРОВЕРКА ДРУГИХ ТАБЛИЦ")
print("="*70)

cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
tables = cursor.fetchall()
print(f"\n📋 Все таблицы в БД:")
for table in tables:
    table_name = table[0]
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    count = cursor.fetchone()[0]
    marker = " ← weather_value" if table_name == 'weather_value' else ""
    print(f"   {table_name}: {count:,} записей{marker}")

conn.close()

print("\n" + "="*70)
print("✅ ИМПОРТ В weather_value ЗАВЕРШЁН!")
print("="*70)
