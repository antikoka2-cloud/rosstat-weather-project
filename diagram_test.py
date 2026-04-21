import sqlite3
import tkinter as tk
from tkinter import ttk, messagebox
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg


DB_PATH = "main.db"


class AgroAnalyticsApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Agro Analytics")
        self.root.geometry("1500x900")

        self.conn = sqlite3.connect(DB_PATH)

        self.region_map = {}
        self.district_map = {}
        self.indicator_map = {}
        self.farm_type_map = {}
        self.crop_map = {}

        self._build_ui()
        self._load_reference_data()
        self._bind_events()
        self._refresh_filters()

    def _build_ui(self):
        main = ttk.Frame(self.root, padding=10)
        main.pack(fill="both", expand=True)

        # ---------- СКРОЛЛИРУЕМАЯ ЛЕВАЯ ПАНЕЛЬ ----------

        left_container = ttk.Frame(main)
        left_container.pack(side="left", fill="y", padx=(0, 10))

        canvas = tk.Canvas(left_container, width=380)
        scrollbar = ttk.Scrollbar(left_container, orient="vertical", command=canvas.yview)

        scrollable_frame = ttk.Frame(canvas)

        scrollable_frame.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )

        canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")

        canvas.configure(yscrollcommand=scrollbar.set)

        canvas.pack(side="left", fill="y", expand=False)
        scrollbar.pack(side="right", fill="y")

        left = scrollable_frame

        right = ttk.Frame(main)
        right.pack(side="right", fill="both", expand=True)

        # ---------- Источник данных ----------
        source_frame = ttk.LabelFrame(left, text="Источник данных", padding=10)
        source_frame.pack(fill="x", pady=5)

        self.table_var = tk.StringVar(value="agri_crop_value")
        ttk.Label(source_frame, text="Таблица:").pack(anchor="w")
        self.table_combo = ttk.Combobox(
            source_frame,
            textvariable=self.table_var,
            state="readonly",
            values=["agri_crop_value", "agri_district_value", "weather_value"]
        )
        self.table_combo.pack(fill="x", pady=3)

        # ---------- Регион ----------
        region_frame = ttk.LabelFrame(left, text="Регион", padding=10)
        region_frame.pack(fill="x", pady=5)

        self.region_var = tk.StringVar()
        self.region_combo = ttk.Combobox(region_frame, textvariable=self.region_var, state="readonly")
        self.region_combo.pack(fill="x", pady=3)

        # ---------- Районы ----------
        district_frame = ttk.LabelFrame(left, text="Районы", padding=10)
        district_frame.pack(fill="both", pady=5)

        district_btns = ttk.Frame(district_frame)
        district_btns.pack(fill="x", pady=(0, 5))

        ttk.Button(district_btns, text="Все", command=self._select_all_districts).pack(side="left", padx=2)
        ttk.Button(district_btns, text="Снять", command=self._clear_districts).pack(side="left", padx=2)

        self.district_listbox = tk.Listbox(district_frame, selectmode=tk.MULTIPLE, height=12, exportselection=False)
        self.district_listbox.pack(fill="both", expand=True)

        # ---------- Показатель ----------
        indicator_frame = ttk.LabelFrame(left, text="Показатель", padding=10)
        indicator_frame.pack(fill="x", pady=5)

        self.indicator_var = tk.StringVar()
        self.indicator_combo = ttk.Combobox(indicator_frame, textvariable=self.indicator_var, state="readonly")
        self.indicator_combo.pack(fill="x", pady=3)

        # ---------- Тип хозяйства ----------
        farm_frame = ttk.LabelFrame(left, text="Тип хозяйства", padding=10)
        farm_frame.pack(fill="x", pady=5)

        self.farm_type_var = tk.StringVar(value="Все")
        self.farm_type_combo = ttk.Combobox(farm_frame, textvariable=self.farm_type_var, state="readonly")
        self.farm_type_combo.pack(fill="x", pady=3)

        # ---------- Культура ----------
        crop_frame = ttk.LabelFrame(left, text="Культура", padding=10)
        crop_frame.pack(fill="x", pady=5)

        self.crop_var = tk.StringVar(value="Все")
        self.crop_combo = ttk.Combobox(crop_frame, textvariable=self.crop_var, state="readonly")
        self.crop_combo.pack(fill="x", pady=3)

        # ---------- Годы ----------
        years_frame = ttk.LabelFrame(left, text="Годы", padding=10)
        years_frame.pack(fill="x", pady=5)

        year_row = ttk.Frame(years_frame)
        year_row.pack(fill="x")

        ttk.Label(year_row, text="С:").grid(row=0, column=0, sticky="w")
        self.year_from_var = tk.StringVar()
        self.year_from_combo = ttk.Combobox(year_row, textvariable=self.year_from_var, state="readonly", width=12)
        self.year_from_combo.grid(row=0, column=1, padx=5, pady=2)

        ttk.Label(year_row, text="По:").grid(row=0, column=2, sticky="w")
        self.year_to_var = tk.StringVar()
        self.year_to_combo = ttk.Combobox(year_row, textvariable=self.year_to_var, state="readonly", width=12)
        self.year_to_combo.grid(row=0, column=3, padx=5, pady=2)

        # ---------- Тип графика ----------
        chart_frame = ttk.LabelFrame(left, text="График", padding=10)
        chart_frame.pack(fill="x", pady=5)

        self.chart_type_var = tk.StringVar(value="Линии")
        self.chart_type_combo = ttk.Combobox(
            chart_frame,
            textvariable=self.chart_type_var,
            state="readonly",
            values=["Линии", "Столбцы", "Среднее по годам", "Boxplot по годам"]
        )
        self.chart_type_combo.pack(fill="x", pady=3)

        # ---------- Кнопки ----------
        actions = ttk.Frame(left)
        actions.pack(fill="x", pady=10)

        ttk.Button(actions, text="Построить", command=self.plot_chart).pack(side="left", fill="x", expand=True, padx=2)
        ttk.Button(actions, text="Очистить график", command=self.clear_chart).pack(side="left", fill="x", expand=True, padx=2)

        # ---------- Область вывода ----------
        self.figure = plt.Figure(figsize=(10, 7), dpi=100)
        self.ax = self.figure.add_subplot(111)
        self.canvas = FigureCanvasTkAgg(self.figure, master=right)
        self.canvas.get_tk_widget().pack(fill="both", expand=True)

        self.status_var = tk.StringVar(value="Готово")
        status_bar = ttk.Label(self.root, textvariable=self.status_var, relief="sunken", anchor="w")
        status_bar.pack(fill="x", side="bottom")

    def _bind_events(self):
        self.table_combo.bind("<<ComboboxSelected>>", lambda e: self._refresh_filters())
        self.region_combo.bind("<<ComboboxSelected>>", lambda e: self._refresh_districts())
        self.indicator_combo.bind("<<ComboboxSelected>>", lambda e: self._refresh_years())
        self.farm_type_combo.bind("<<ComboboxSelected>>", lambda e: self._refresh_years())
        self.crop_combo.bind("<<ComboboxSelected>>", lambda e: self._refresh_years())

    def _load_reference_data(self):
        self.region_map = self._load_map("SELECT id, name FROM region ORDER BY name")
        self.indicator_map = self._load_map("SELECT id, name FROM indicator ORDER BY id")
        self.farm_type_map = self._load_map("SELECT id, name FROM farm_type ORDER BY id")
        self.crop_map = self._load_map("SELECT id, name FROM crop ORDER BY name")

        self.region_combo["values"] = list(self.region_map.keys())
        self.indicator_combo["values"] = list(self.indicator_map.keys())
        self.farm_type_combo["values"] = ["Все"] + list(self.farm_type_map.keys())
        self.crop_combo["values"] = ["Все"] + list(self.crop_map.keys())

        if self.region_combo["values"]:
            self.region_combo.current(0)
        if self.indicator_combo["values"]:
            self.indicator_combo.current(0)
        self.farm_type_combo.current(0)
        self.crop_combo.current(0)

    def _load_map(self, query):
        cur = self.conn.cursor()
        cur.execute(query)
        rows = cur.fetchall()
        return {name: id_ for id_, name in rows}

    def _refresh_filters(self):
        self._refresh_districts()
        self._refresh_years()
        self._toggle_controls_by_table()

    def _toggle_controls_by_table(self):
        table = self.table_var.get()

        if table == "agri_crop_value":
            self.farm_type_combo.configure(state="readonly")
            self.crop_combo.configure(state="readonly")
        elif table == "agri_district_value":
            self.farm_type_combo.configure(state="readonly")
            self.crop_var.set("Все")
            self.crop_combo.configure(state="disabled")
        else:  # weather_value
            self.farm_type_var.set("Все")
            self.crop_var.set("Все")
            self.farm_type_combo.configure(state="disabled")
            self.crop_combo.configure(state="disabled")

    def _refresh_districts(self):
        region_name = self.region_var.get()
        if not region_name:
            return

        region_id = self.region_map.get(region_name)
        if region_id is None:
            return

        query = """
        SELECT id, name
        FROM district
        WHERE region_id = ?
        ORDER BY name
        """
        cur = self.conn.cursor()
        cur.execute(query, (region_id,))
        rows = cur.fetchall()

        self.district_map = {name: id_ for id_, name in rows}

        self.district_listbox.delete(0, tk.END)
        for name in self.district_map.keys():
            self.district_listbox.insert(tk.END, name)

    def _refresh_years(self):
        try:
            years = self._get_available_years()
        except Exception:
            years = []

        year_values = [str(y) for y in years]
        self.year_from_combo["values"] = year_values
        self.year_to_combo["values"] = year_values

        if year_values:
            self.year_from_var.set(year_values[0])
            self.year_to_var.set(year_values[-1])
        else:
            self.year_from_var.set("")
            self.year_to_var.set("")

    def _get_selected_district_ids(self):
        selected_indices = self.district_listbox.curselection()
        selected_names = [self.district_listbox.get(i) for i in selected_indices]
        return [self.district_map[name] for name in selected_names if name in self.district_map]

    def _get_available_years(self):
        table = self.table_var.get()
        indicator_name = self.indicator_var.get()
        indicator_id = self.indicator_map.get(indicator_name)

        if indicator_id is None:
            return []

        region_name = self.region_var.get()
        region_id = self.region_map.get(region_name)

        params = []
        where = []

        if table in ("agri_crop_value", "agri_district_value"):
            where.append("t.indicator_id = ?")
            params.append(indicator_id)

        if region_id is not None:
            where.append("d.region_id = ?")
            params.append(region_id)

        if table == "agri_crop_value":
            if self.farm_type_var.get() != "Все":
                farm_type_id = self.farm_type_map.get(self.farm_type_var.get())
                if farm_type_id is not None:
                    where.append("t.farm_type_id = ?")
                    params.append(farm_type_id)

            if self.crop_var.get() != "Все":
                crop_id = self.crop_map.get(self.crop_var.get())
                if crop_id is not None:
                    where.append("t.crop_id = ?")
                    params.append(crop_id)

        elif table == "agri_district_value":
            if self.farm_type_var.get() != "Все":
                farm_type_id = self.farm_type_map.get(self.farm_type_var.get())
                if farm_type_id is not None:
                    where.append("t.farm_type_id = ?")
                    params.append(farm_type_id)

        join_sql = "JOIN district d ON d.id = t.district_id"
        sql = f"SELECT DISTINCT t.year FROM {table} t {join_sql}"

        if where:
            sql += " WHERE " + " AND ".join(where)

        sql += " ORDER BY t.year"

        cur = self.conn.cursor()
        cur.execute(sql, params)
        return [row[0] for row in cur.fetchall()]

    def _select_all_districts(self):
        self.district_listbox.select_set(0, tk.END)

    def _clear_districts(self):
        self.district_listbox.selection_clear(0, tk.END)

    def _build_query(self):
        table = self.table_var.get()
        indicator_name = self.indicator_var.get()
        indicator_id = self.indicator_map.get(indicator_name)

        if table in ("agri_crop_value", "agri_district_value") and indicator_id is None:
            raise ValueError("Не выбран показатель")

        year_from = self.year_from_var.get()
        year_to = self.year_to_var.get()

        if not year_from or not year_to:
            raise ValueError("Не выбраны годы")

        selected_district_ids = self._get_selected_district_ids()
        region_name = self.region_var.get()
        region_id = self.region_map.get(region_name)

        params = []
        where = []

        select_value = "t.value"

        if table == "agri_crop_value":
            sql = f"""
            SELECT
                d.name AS district,
                t.year,
                {select_value} AS value
            FROM {table} t
            JOIN district d ON d.id = t.district_id
            """

            where.append("t.indicator_id = ?")
            params.append(indicator_id)

            if self.farm_type_var.get() != "Все":
                farm_type_id = self.farm_type_map.get(self.farm_type_var.get())
                if farm_type_id is not None:
                    where.append("t.farm_type_id = ?")
                    params.append(farm_type_id)

            if self.crop_var.get() != "Все":
                crop_id = self.crop_map.get(self.crop_var.get())
                if crop_id is not None:
                    where.append("t.crop_id = ?")
                    params.append(crop_id)

        elif table == "agri_district_value":
            sql = f"""
            SELECT
                d.name AS district,
                t.year,
                {select_value} AS value
            FROM {table} t
            JOIN district d ON d.id = t.district_id
            """

            where.append("t.indicator_id = ?")
            params.append(indicator_id)

            if self.farm_type_var.get() != "Все":
                farm_type_id = self.farm_type_map.get(self.farm_type_var.get())
                if farm_type_id is not None:
                    where.append("t.farm_type_id = ?")
                    params.append(farm_type_id)

        else:  # weather_value
            sql = f"""
            SELECT
                d.name AS district,
                t.year,
                {select_value} AS value
            FROM {table} t
            JOIN district d ON d.id = t.district_id
            """
            # weather_value может не иметь indicator_id/farm_type_id/crop_id

        if region_id is not None:
            where.append("d.region_id = ?")
            params.append(region_id)

        if selected_district_ids:
            placeholders = ",".join("?" * len(selected_district_ids))
            where.append(f"t.district_id IN ({placeholders})")
            params.extend(selected_district_ids)

        where.append("t.year BETWEEN ? AND ?")
        params.extend([int(year_from), int(year_to)])

        if where:
            sql += " WHERE " + " AND ".join(where)

        sql += " ORDER BY d.name, t.year"

        return sql, params

    def plot_chart(self):
        try:
            sql, params = self._build_query()
            df = pd.read_sql_query(sql, self.conn, params=params)

            if df.empty:
                messagebox.showwarning("Нет данных", "По выбранным параметрам данные не найдены")
                return

            chart_type = self.chart_type_var.get()

            self.ax.clear()

            if chart_type == "Линии":
                pivot = pd.pivot_table(
                    df,
                    index="year",
                    columns="district",
                    values="value",
                    aggfunc="mean"
                ).sort_index()

                for district in pivot.columns:
                    self.ax.plot(pivot.index, pivot[district], linewidth=1, label=district)

                if len(pivot.columns) <= 15:
                    self.ax.legend(fontsize=8)

            elif chart_type == "Столбцы":
                grouped = df.groupby(["district", "year"], as_index=False)["value"].mean()
                pivot = grouped.pivot(index="year", columns="district", values="value").sort_index()
                pivot.plot(kind="bar", ax=self.ax)

            elif chart_type == "Среднее по годам":
                grouped = df.groupby("year", as_index=False)["value"].mean()
                self.ax.plot(grouped["year"], grouped["value"], marker="o")
                self.ax.set_title("Среднее значение по годам")

            elif chart_type == "Boxplot по годам":
                years = sorted(df["year"].unique())
                data = [df.loc[df["year"] == y, "value"].dropna().values for y in years]
                self.ax.boxplot(data, tick_labels=years)

            self.ax.set_xlabel("Год")
            self.ax.set_ylabel("Значение")

            title_parts = [
                self.table_var.get(),
                self.region_var.get(),
                self.indicator_var.get()
            ]
            if self.farm_type_var.get() != "Все":
                title_parts.append(self.farm_type_var.get())
            if self.crop_var.get() != "Все":
                title_parts.append(self.crop_var.get())

            self.ax.set_title(" | ".join([x for x in title_parts if x]))
            self.ax.grid(True, alpha=0.3)
            self.figure.tight_layout()
            self.canvas.draw()

            self.status_var.set(f"Построено: {len(df)} строк")

        except Exception as e:
            messagebox.showerror("Ошибка", str(e))
            self.status_var.set(f"Ошибка: {e}")

    def clear_chart(self):
        self.ax.clear()
        self.canvas.draw()
        self.status_var.set("График очищен")

    def close(self):
        try:
            self.conn.close()
        except Exception:
            pass
        self.root.destroy()


if __name__ == "__main__":
    root = tk.Tk()
    app = AgroAnalyticsApp(root)
    root.protocol("WM_DELETE_WINDOW", app.close)
    root.mainloop()