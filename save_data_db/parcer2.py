# для валовых сборов
import sqlite3
import pandas as pd
import numpy as np


DB_PATH = "main.db"
FILE_PATH = r"data\Ростовская область\Валовые сборы.csv"
INDICATOR_ID = 2


def parse_gross_harvest_csv(path: str) -> pd.DataFrame:
    rows = []

    with open(path, "r", encoding="cp1251", errors="replace") as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(line.split(";"))

    if not rows:
        raise ValueError("Файл пустой")

    max_len = max(len(r) for r in rows)
    rows = [r + [""] * (max_len - len(r)) for r in rows]

    # Ищем строку с годами
    header_idx = None
    for i, row in enumerate(rows):
        first = (row[0] or "").strip().lower()
        tail = [x.strip() for x in row[1:] if x.strip()]

        if first == "" and tail and all(x.isdigit() for x in tail[: min(5, len(tail))]):
            header_idx = i
            break

    if header_idx is None:
        raise ValueError("Не удалось найти строку с годами")

    years = [int(y.strip()) for y in rows[header_idx][1:] if y.strip()]

    data = []
    district = None
    farm_type = None

    for row in rows[header_idx + 1:]:
        label = (row[0] or "").strip()
        values = row[1:1 + len(years)]

        if not label:
            continue

        has_values = any(v.strip() for v in values)
        low = label.lower()

        # Служебные/иерархические строки
        if not has_values:
            # служебный заголовок внутри блока
            if low in {"муниципальный район", "городской округ"}:
                continue

            # район / округ / город
            if (
                "муниципальный район" in low
                or "городской округ" in low
                or low.startswith("город ")
            ):
                district = label
                farm_type = None
                continue

            # тип хозяйства
            if (
                "хозяйства" in low
                or "организации" in low
                or "сельхозорганизации" in low
            ):
                farm_type = label
                continue

            continue

        # Строка с культурой и значениями
        for year, raw_val in zip(years, values):
            raw_val = raw_val.strip()
            if not raw_val:
                continue

            try:
                num = float(raw_val.replace(" ", "").replace(",", "."))
            except ValueError:
                continue

            if num == 999999999:
                num = np.nan

            if pd.isna(num):
                continue

            data.append({
                "district": district,
                "farm_type": farm_type,
                "crop": label,
                "year": year,
                "value": float(num),
            })

    df = pd.DataFrame(data)

    # защита от битых строк
    df = df.dropna(subset=["district", "farm_type", "crop", "year", "value"])

    return df


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

    return district_map, farm_type_map, crop_map


def save_to_db(df: pd.DataFrame, db_path: str, indicator_id: int):
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    cur = conn.cursor()

    district_map, farm_type_map, crop_map = load_reference_dicts(conn)

    inserted = 0
    updated = 0
    skipped = 0

    for row in df.itertuples(index=False):
        district_name = row.district.strip()
        farm_type_name = row.farm_type.strip()
        crop_name = row.crop.strip()
        year = int(row.year)
        value = float(row.value)

        district_id = district_map.get(district_name)
        farm_type_id = farm_type_map.get(farm_type_name)
        crop_id = crop_map.get(crop_name)

        if district_id is None:
            print(f"[SKIP] district не найден: {district_name}")
            skipped += 1
            continue

        if farm_type_id is None:
            print(f"[SKIP] farm_type не найден: {farm_type_name}")
            skipped += 1
            continue

        if crop_id is None:
            print(f"[SKIP] crop не найден: {crop_name}")
            skipped += 1
            continue

        # Проверяем, есть ли уже такая запись
        cur.execute("""
            SELECT id
            FROM agri_crop_value
            WHERE district_id = ?
              AND indicator_id = ?
              AND farm_type_id = ?
              AND crop_id = ?
              AND year = ?
        """, (district_id, indicator_id, farm_type_id, crop_id, year))

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

    print(f"Готово: inserted={inserted}, updated={updated}, skipped={skipped}")


if __name__ == "__main__":
    df = parse_gross_harvest_csv(FILE_PATH)
    print(df.head(30))
    print(f"Всего строк после парсинга: {len(df)}")

    save_to_db(df, DB_PATH, INDICATOR_ID)