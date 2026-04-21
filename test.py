import sqlite3
import pandas as pd


DB_PATH = "main.db"
FILE_PATH = r"data\Ростовская область\Минеральные удобрения.csv"

INDICATOR_ID = 4
FARM_TYPE_ID = 2  # Сельскохозяйственные организации


def parse_mineral_fertilizers_csv(path: str) -> pd.DataFrame:
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

    # строка с годами
    header_idx = None
    for i, row in enumerate(rows):
        first = (row[0] or "").strip()
        tail = [x.strip() for x in row[1:] if x.strip()]
        if first == "" and tail:
            sample = tail[: min(5, len(tail))]
            if all(x.isdigit() and len(x) == 4 for x in sample):
                header_idx = i
                break

    if header_idx is None:
        raise ValueError("Не удалось найти строку с годами")

    years = [int(x.strip()) for x in rows[header_idx][1:] if x.strip()]

    data = []
    current_district = None

    service_labels = {
        "муниципальный район",
        "городской округ, городской округ с внутригородским делением",
        "городской округ",
    }

    skip_labels = {
        "городские округа ростовской области/",
    }

    for row in rows[header_idx + 1:]:
        label = (row[0] or "").strip()
        values = row[1:1 + len(years)]

        if not label:
            continue

        low = label.lower()
        has_values = any(v.strip() for v in values)

        # мусорные заголовки секций
        if low in skip_labels:
            continue

        # заголовок района/города без значений
        if not has_values:
            if low not in service_labels:
                current_district = label
            continue

        # строка с данными: district берём из предыдущего заголовка
        if low in service_labels:
            if not current_district:
                print(f"[SKIP] нет current_district для строки: {label}")
                continue

            district_name = current_district

            for year, raw_val in zip(years, values):
                raw_val = raw_val.strip()
                if not raw_val or raw_val == "-":
                    continue

                try:
                    num = float(raw_val.replace(" ", "").replace(",", "."))
                except ValueError:
                    continue

                if num == 999999999:
                    continue

                data.append({
                    "district": district_name,
                    "year": int(year),
                    "value": float(num),
                })

    df = pd.DataFrame(data)

    if df.empty:
        raise ValueError("После парсинга DataFrame пуст")

    df = df.drop_duplicates(subset=["district", "year"], keep="last").reset_index(drop=True)
    return df


def load_district_map(conn: sqlite3.Connection):
    return {
        str(name).strip(): id_
        for id_, _, name in conn.execute("SELECT id, region_id, name FROM district")
    }


def save_to_agri_district_value(df: pd.DataFrame, db_path: str, indicator_id: int, farm_type_id: int):
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    cur = conn.cursor()

    district_map = load_district_map(conn)

    inserted = 0
    updated = 0
    skipped = 0

    for row in df.itertuples(index=False):
        district_name = row.district.strip()
        year = int(row.year)
        value = float(row.value)

        district_id = district_map.get(district_name)

        if district_id is None:
            print(f"[SKIP] district не найден: {district_name}")
            skipped += 1
            continue

        cur.execute("""
            SELECT id
            FROM agri_district_value
            WHERE district_id = ?
              AND indicator_id = ?
              AND farm_type_id = ?
              AND year = ?
        """, (district_id, indicator_id, farm_type_id, year))

        existing = cur.fetchone()

        if existing:
            cur.execute("""
                UPDATE agri_district_value
                SET value = ?
                WHERE id = ?
            """, (value, existing[0]))
            updated += 1
        else:
            cur.execute("""
                INSERT INTO agri_district_value (
                    district_id,
                    indicator_id,
                    farm_type_id,
                    year,
                    value
                )
                VALUES (?, ?, ?, ?, ?)
            """, (district_id, indicator_id, farm_type_id, year, value))
            inserted += 1

    conn.commit()
    conn.close()

    print(f"Готово: inserted={inserted}, updated={updated}, skipped={skipped}")


if __name__ == "__main__":
    df = parse_mineral_fertilizers_csv(FILE_PATH)
    print(df.head(20))
    print(f"Всего строк: {len(df)}")

    save_to_agri_district_value(df, DB_PATH, INDICATOR_ID, FARM_TYPE_ID)