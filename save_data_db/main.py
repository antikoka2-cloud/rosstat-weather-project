import sqlite3
import pandas as pd
import numpy as np
from pathlib import Path


DB_PATH = "main.db"
FILE_PATH = r"data\Саратовская область\Посевные площади.csv"


def parse_agri_file(path: str) -> pd.DataFrame:
    rows = []

    with open(path, "r", encoding="cp1251", errors="replace") as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(line.split(";"))

    if not rows:
        raise ValueError("Файл пустой")

    # выравнивание строк
    max_len = max(len(r) for r in rows)
    rows = [r + [""] * (max_len - len(r)) for r in rows]

    # 2-я строка обычно содержит года
    years = [y.strip() for y in rows[1][1:] if y.strip()]

    data = []
    district = None
    farm_type = None

    for row in rows[2:]:
        label = (row[0] or "").strip()
        values = row[1:1 + len(years)]

        if not label:
            continue

        has_values = any(v.strip() for v in values)

        # определяем уровни иерархии
        if not has_values:
            low = label.lower()

            if "муниципальный район" in low or "городской округ" in low or low.startswith("город "):
                district = label
                farm_type = None
            elif "хозяйства" in low or "организации" in low:
                farm_type = label

            continue

        # строки с данными
        for year, val in zip(years, values):
            val = val.strip()
            if not val:
                continue

            try:
                num = float(val.replace(" ", "").replace(",", "."))
            except ValueError:
                continue

            if num == 999999999:
                num = np.nan

            data.append({
                "district": district,
                "farm_type": farm_type,
                "crop": label,
                "year": int(year),
                "value": num
            })

    df = pd.DataFrame(data)
    df = df.dropna(subset=["value"])

    return df


def detect_indicator_name(file_path: str) -> str:
    """
    Подбираем indicator.name по имени файла.
    Должно совпадать со значением из таблицы indicator.name
    """
    filename = Path(file_path).stem.lower()

    mapping = {
        "посевные площади": "Посевные площади сельскохозяйственных культур, гектар",
        "валовые сборы": "Валовые сборы сельскохозяйственных культур, центнер",
        "урожайность": "Урожайность сельскохозяйственных культур, центнер с гектара",
        "внесено минеральных удобрений": "Внесено минеральных удобрений, центнер",
        "внесено органических удобрений": "Внесено органических удобрений, тонна",
    }

    for key, value in mapping.items():
        if key in filename:
            return value

    raise ValueError(
        f"Не удалось определить indicator.name по имени файла: {file_path}\n"
        f"Добавь соответствие в mapping."
    )


def load_reference_dicts(conn: sqlite3.Connection):
    district_map = {
        name.strip(): id_
        for id_, _, name in conn.execute("SELECT id, region_id, name FROM district")
    }

    farm_type_map = {
        name.strip(): id_
        for id_, name in conn.execute("SELECT id, name FROM farm_type")
    }

    crop_map = {
        name.strip(): id_
        for id_, name in conn.execute("SELECT id, name FROM crop")
    }

    indicator_map = {
        name.strip(): id_
        for id_, name in conn.execute("SELECT id, name FROM indicator")
    }

    return district_map, farm_type_map, crop_map, indicator_map


def save_agri_crop_values_to_db(df: pd.DataFrame, db_path: str, indicator_name: str):
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    cur = conn.cursor()

    district_map, farm_type_map, crop_map, indicator_map = load_reference_dicts(conn)

    if indicator_name not in indicator_map:
        raise ValueError(f"Индикатор не найден в таблице indicator: {indicator_name}")

    indicator_id = indicator_map[indicator_name]

    inserted = 0
    updated = 0
    skipped = 0

    for row in df.itertuples(index=False):
        district_name = (row.district or "").strip()
        farm_type_name = (row.farm_type or "").strip()
        crop_name = (row.crop or "").strip()
        year = int(row.year)
        value = float(row.value)

        district_id = district_map.get(district_name)
        farm_type_id = farm_type_map.get(farm_type_name) if farm_type_name else None
        crop_id = crop_map.get(crop_name)

        if district_id is None:
            print(f"[SKIP] district не найден: {district_name}")
            skipped += 1
            continue

        if farm_type_name and farm_type_id is None:
            print(f"[SKIP] farm_type не найден: {farm_type_name}")
            skipped += 1
            continue

        if crop_id is None:
            print(f"[SKIP] crop не найден: {crop_name}")
            skipped += 1
            continue

        # Ищем существующую запись
        cur.execute("""
            SELECT id
            FROM agri_crop_value
            WHERE district_id = ?
              AND indicator_id = ?
              AND (
                    (farm_type_id = ?)
                    OR (farm_type_id IS NULL AND ? IS NULL)
                  )
              AND crop_id = ?
              AND year = ?
        """, (district_id, indicator_id, farm_type_id, farm_type_id, crop_id, year))

        existing = cur.fetchone()

        if existing:
            cur.execute("""
                UPDATE agri_crop_value
                SET value = ?
                WHERE id = ?
            """, (value, existing[0]))
            updated += 1
        else:
            cur.execute("""
                INSERT INTO agri_crop_value (
                    district_id,
                    indicator_id,
                    farm_type_id,
                    crop_id,
                    year,
                    value
                )
                VALUES (?, ?, ?, ?, ?, ?)
            """, (district_id, indicator_id, farm_type_id, crop_id, year, value))
            inserted += 1

    conn.commit()
    conn.close()

    print(f"Готово. inserted={inserted}, updated={updated}, skipped={skipped}")


if __name__ == "__main__":
    df = parse_agri_file(FILE_PATH)
    print(df.head(20))

    indicator_name = detect_indicator_name(FILE_PATH)
    print(f"Определён indicator: {indicator_name}")

    save_agri_crop_values_to_db(df, DB_PATH, indicator_name)