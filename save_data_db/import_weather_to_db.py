# -*- coding: utf-8 -*-
import sqlite3
import pandas as pd
from pathlib import Path

# Пути
DB_FILE = Path(__file__).parent / 'main.db'
YEARLY_CSV = Path(__file__).parent / 'weather_value_2008_2025.csv'
MONTHLY_CSV = Path(__file__).parent / 'monthly_temperature_2008_2025.csv'

def main():
    print("="*70)
    print("🗄️  ИМПОРТ ДАННЫХ ПОГОДЫ В SQLITE")
    print("="*70)
    
    # Подключение к БД
    conn = sqlite3.connect(DB_FILE)
    print(f"✅ Подключение: {DB_FILE}")
    
    try:
        # 1. Добавить колонку month
        add_month_column(conn)
        
        # 2. Импорт годовых данных
        import_yearly_data(conn, YEARLY_CSV)
        
        # 3. Импорт месячных данных (если есть)
        if MONTHLY_CSV.exists():
            import_monthly_data(conn, MONTHLY_CSV)
        
        # 4. Создать индексы
        create_indexes(conn)
        
        # 5. Проверка
        verify_data(conn)
        
        print("\n✅ ИМПОРТ ЗАВЕРШЁН!")
        
    except Exception as e:
        print(f"\n❌ Ошибка: {e}")
        raise
    finally:
        conn.close()

def add_month_column(conn):
    """Добавление колонки month"""
    cursor = conn.cursor()
    try:
        cursor.execute('ALTER TABLE weather_value ADD COLUMN month INTEGER')
        print("✅ Колонка 'month' добавлена")
    except sqlite3.OperationalError as e:
        if 'duplicate column name' in str(e):
            print("⚠️ Колонка 'month' уже существует")
        else:
            raise
    conn.commit()

def import_yearly_data(conn, csv_file):
    """Импорт годовых данных (month = NULL)"""
    if not csv_file.exists():
        print(f"❌ Файл {csv_file} не найден")
        return
    
    print(f"\n📥 Импорт из {csv_file.name}...")
    df = pd.read_csv(csv_file, encoding='utf-8-sig')
    
    # Добавить колонку month = NULL для годовых данных
    df['month'] = None
    
    cursor = conn.cursor()
    records = []
    
    for _, row in df.iterrows():
        records.append((
            int(row['district_id']),
            int(row['indicator_id']),
            int(row['year']),
            row['month'],  # NULL
            float(row['value'])
        ))
    
    # Очистить таблицу перед импортом (если нужно)
    # cursor.execute('DELETE FROM weather_value')
    
    cursor.executemany('''
        INSERT INTO weather_value (district_id, indicator_id, year, month, value)
        VALUES (?, ?, ?, ?, ?)
    ''', records)
    
    conn.commit()
    print(f"✅ Импортировано {len(records):,} записей (годовые данные)")

def import_monthly_data(conn, csv_file):
    """Импорт месячных данных"""
    if not csv_file.exists():
        print(f"❌ Файл {csv_file} не найден")
        return
    
    print(f"\n📥 Импорт из {csv_file.name}...")
    df = pd.read_csv(csv_file, encoding='utf-8-sig')
    
    cursor = conn.cursor()
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
    print(f"✅ Импортировано {len(records):,} записей (месячные данные)")

def create_indexes(conn):
    """Создание индексов для ускорения"""
    print("\n📊 Создание индексов...")
    cursor = conn.cursor()
    
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
    print("✅ Индексы созданы")

def verify_data(conn):
    """Проверка данных"""
    print("\n" + "="*70)
    print("📊 ПРОВЕРКА ДАННЫХ")
    print("="*70)
    
    cursor = conn.cursor()
    
    # Всего записей
    cursor.execute('SELECT COUNT(*) FROM weather_value')
    print(f"📋 Всего записей: {cursor.fetchone()[0]:,}")
    
    # По типам
    cursor.execute('''
        SELECT 
            CASE WHEN month IS NULL THEN 'Годовые' ELSE 'Месячные' END as type,
            COUNT(*) as count
        FROM weather_value
        GROUP BY type
    ''')
    print("\n📈 По типу данных:")
    for row in cursor.fetchall():
        print(f"   {row[0]}: {row[1]:,}")
    
    # Период
    cursor.execute('SELECT MIN(year), MAX(year) FROM weather_value')
    min_year, max_year = cursor.fetchone()
    print(f"\n📅 Период: {min_year} - {max_year}")
    
    # Районы
    cursor.execute('SELECT COUNT(DISTINCT district_id) FROM weather_value')
    print(f"🏘️ Районов: {cursor.fetchone()[0]}")
    
    # Пример
    print("\n📋 Пример данных (5 строк):")
    cursor.execute('''
        SELECT district_id, indicator_id, year, month, value
        FROM weather_value
        LIMIT 5
    ''')
    for row in cursor.fetchall():
        print(f"   {row}")
    
    print("="*70)

if __name__ == "__main__":
    main()