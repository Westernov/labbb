import tkinter as tk
from tkinter import ttk, messagebox
import psycopg2
from contextlib import closing

# Импортируем функцию добавления записи из zapis.py
from zapis import show_add_dialog

# Импортируйте свои вспомогательные модули, если они нужны
import Фильмы
import Страна
import Владельцы
import ТипыСобственности
import ВидеоПрокаты
import ВидеоКассеты
import ЦенаУслуг
import услуги
import Квитанции
import ПозицииКвитанции

class DatabaseManager:
    def __init__(self):
        self.db_params = {
            "host": "localhost",
            "database": "postgres",
            "user": "postgres",
            "password": "W3st3rn228"
        }

        self.auto_generated = {
            'Цены_услуг': ['ID_цены'],
            'Фильмы': ['ID_фильма'],
            'Владельцы': ['ID_владельца'],
            'Позиции_квитанции': ['ID_позиции'],
            'Квитанции': ['ID_квитанции'],
            'Видеопрокаты': ['ID_проката'],
            'Видеокассеты': ['ID_кассеты'],
            'Страны': ['ID_страны'],
            'Услуги': ['ID_услуги'],
            'ТипыСобственности': ['ID_типа_собственности']
        }

        self.foreign_keys = {
            'Видеокассеты': ['ID_фильма'],
            'Позиции_квитанции': ['ID_квитанции', 'ID_услуги', 'ID_кассеты'],
            'Квитанции': ['ID_проката'],
            'Цены_услуг': ['ID_проката', 'ID_услуги'],
            'Видеопрокаты': ['ID_владельца', 'ID_типа_собственности'],
            'Фильмы': ['ID_страны']
        }

    def get_connection(self):
        return psycopg2.connect(**self.db_params)

    def get_hidden_columns(self, table_name):
        hidden = self.auto_generated.get(table_name, []).copy()
        hidden.extend(self.foreign_keys.get(table_name, []))
        return hidden

    def delete_row(self, table_name, id_column, row_id):
        try:
            with closing(self.get_connection()) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        f"DELETE FROM {table_name} WHERE {id_column} = %s",
                        (row_id,)
                    )
                    conn.commit()
            return True
        except Exception as e:
            print(f"Ошибка удаления: {e}")
            return False

    def validate_data_types(self, table_name, columns, values):
        type_map = {
            'integer': int,
            'numeric': float,
            'text': str,
            'date': str,
            'boolean': bool,
            'time without time zone': str
        }
        try:
            with closing(self.get_connection()) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(f"""
                        SELECT column_name, data_type
                        FROM information_schema.columns
                        WHERE table_name = '{table_name}'
                    """)
                    schema = {row[0]: row[1] for row in cursor.fetchall()}
                    for col, val in zip(columns, values):
                        if val == "":
                            if schema[col] in ['integer', 'numeric']:
                                raise ValueError(f"Поле {col} не может быть пустым")
                        else:
                            try:
                                type_map[schema[col]](val)
                            except (ValueError, KeyError):
                                raise TypeError(f"Некорректный тип для {col}. Ожидается {schema[col]}")
            return True
        except Exception as e:
            raise ValueError(str(e))

    def insert_row(self, table_name, columns, values):
        try:
            with closing(self.get_connection()) as conn:
                with conn.cursor() as cursor:
                    hidden_columns = self.get_hidden_columns(table_name)
                    filtered_cols = [col for col in columns if col not in hidden_columns]
                    filtered_vals = [val for col, val in zip(columns, values) if col not in hidden_columns]
                    auto_cols = self.auto_generated.get(table_name, [])
                    for col in auto_cols:
                        filtered_cols.append(col)
                        filtered_vals.append(self.find_missing_id(table_name, col))
                    placeholders = ', '.join(['%s'] * len(filtered_vals))
                    query = f"""
                        INSERT INTO {table_name} ({', '.join(filtered_cols)})
                        VALUES ({placeholders})
                    """
                    cursor.execute(query, filtered_vals)
                    conn.commit()
            return True
        except Exception as e:
            print(f"Ошибка вставки: {e}")
            return False

    def get_tables(self):
        try:
            with closing(self.get_connection()) as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT table_name
                        FROM information_schema.tables
                        WHERE table_schema = 'public'
                    """)
                    return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            print(f"Ошибка: {e}")
            return []

    def get_table_data(self, table_name):
        try:
            with closing(self.get_connection()) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(f"SELECT * FROM {table_name}")
                    data = cursor.fetchall()
                    cursor.execute(f"""
                        SELECT column_name
                        FROM information_schema.columns
                        WHERE table_name = '{table_name}'
                    """)
                    columns = [row[0] for row in cursor.fetchall()]
                    return columns, data
        except Exception as e:
            print(f"Ошибка: {e}")
            return [], []

    def is_numeric_column(self, table_name, column_name):
        try:
            with closing(self.get_connection()) as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT data_type
                        FROM information_schema.columns
                        WHERE table_name = %s
                        AND column_name = %s
                    """, (table_name, column_name))
                    result = cursor.fetchone()
                    if result:
                        return result[0] in ('integer', 'numeric', 'bigint', 'smallint')
            return False
        except Exception as e:
            print(f"Ошибка проверки типа колонки: {e}")
            return False

    def find_missing_id(self, table_name, id_column):
        try:
            with closing(self.get_connection()) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(f"SELECT {id_column} FROM {table_name} ORDER BY {id_column}")
                    existing_ids = [row[0] for row in cursor.fetchall()]
                    next_id = 1
                    while next_id in existing_ids:
                        next_id += 1
                    return next_id
        except Exception as e:
            print(f"Ошибка поиска ID: {e}")
            return 1

    def generate_random_data(self):
        try:
            with closing(self.get_connection()) as conn:
                cursor = conn.cursor()
                # Удаляем только записи с указанными ID
                tables_to_clear = {
                    'Видеокассеты': 'ID_кассеты',
                    'Фильмы': 'ID_фильма',
                    'Владельцы': 'ID_владельца',
                    'Позиции_квитанции': 'ID_позиции',
                    'Квитанции': 'ID_квитанции',
                    'Цены_услуг': 'ID_цены',
                    'Видеопрокаты': 'ID_проката'
                }
                for table, id_column in tables_to_clear.items():
                    cursor.execute(f"DELETE FROM {table}")
                    print(f"Удалены записи из таблицы {table}")

                # Страны (ручная вставка)
                countries = Страна.get_countries()
                for country in countries:
                    cursor.execute("""
                        INSERT INTO Страны (Название)
                        VALUES (%s)
                    """, (country["Название"],))

                # Типы собственности
                property_types = ТипыСобственности.get_property_types()
                for prop_type in property_types:
                    cursor.execute("""
                        INSERT INTO ТипыСобственности (Наименование)
                        VALUES (%s)
                    """, (prop_type["Наименование"],))

                # Владельцы (автоинкремент)
                owners = Владельцы.generate_owners(1000)
                for owner in owners:
                    cursor.execute("""
                        INSERT INTO Владельцы (
                            Фамилия, Имя, Отчество,
                            Контактный_телефон, Номер_лицензии
                        ) VALUES (%s, %s, %s, %s, %s)
                    """, (
                        owner["Фамилия"], owner["Имя"],
                        owner["Отчество"], owner["Контактный_телефон"], owner["Номер_лицензии"]
                    ))

                # Видеопрокаты
                video_rentals = ВидеоПрокаты.generate_video_rentals(100)
                rental_ids = []
                for rental in video_rentals:
                    cursor.execute("""
                        INSERT INTO Видеопрокаты (
                            Название, Район, Адрес,
                            ID_типа_собственности, Телефон,
                            Время_открытия, Время_закрытия,
                            Количество_сотрудников, ID_владельца
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        rental["Название"], rental["Район"],
                        rental["Адрес"], rental["ID_типа_собственности"], rental["Телефон"],
                        rental["Время_открытия"], rental["Время_закрытия"],
                        rental["Количество_сотрудников"], rental["ID_владельца"]
                    ))
                    rental_ids.append(cursor.lastrowid)

                # Фильмы
                movies = Фильмы.generate_random_movies(1000)
                for movie in movies:
                    cursor.execute("""
                        INSERT INTO Фильмы (
                            Название, Режиссер, Студия,
                            ID_страны, Год_выпуска, Продолжительность,
                            Информация, Популярный
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        movie["Название"], movie["Режиссер"],
                        movie["Студия"], movie["ID_страны"], movie["Год_выпуска"],
                        movie["Продолжительность"], movie["Информация"], movie["Популярный"]
                    ))

                # Видеокассеты
                cassettes = ВидеоКассеты.generate_video_cassettes(1000)
                cassette_ids = []
                for cassette in cassettes:
                    cursor.execute("""
                        INSERT INTO Видеокассеты (
                            ID_фильма, Текущее_местоположение,
                            Качество, Фото, Цена
                        ) VALUES (%s, %s, %s, %s, %s)
                    """, (
                        cassette["ID_фильма"],
                        cassette["Текущее_местоположение"], cassette["Качество"],
                        cassette["Фото"], cassette["Цена"]
                    ))
                    cassette_ids.append(cursor.lastrowid)

                # Услуги
                services = услуги.get_services()
                service_ids = []
                for service in services:
                    cursor.execute("""
                        INSERT INTO Услуги (Тип_услуги, Описание)
                        VALUES (%s, %s)
                    """, (service["Тип_услуги"], service["Описание"]))
                    service_ids.append(cursor.lastrowid)

                # Цены услуг
                service_prices = ЦенаУслуг.generate_service_prices(500, rental_ids, service_ids)
                for price in service_prices:
                    cursor.execute("""
                        INSERT INTO Цены_услуг (
                            ID_проката, ID_услуги,
                            Цена, Действует_с, Действует_по
                        ) VALUES (%s, %s, %s, %s, %s)
                    """, (
                        price["ID_проката"], price["ID_услуги"],
                        price["Цена"], price["Действует_с"], price["Действует_по"]
                    ))

                # Квитанции
                receipts = Квитанции.generate_receipts(500, rental_ids)
                receipt_ids = []
                for receipt in receipts:
                    cursor.execute("""
                        INSERT INTO Квитанции (
                            ID_проката,
                            Информация_о_клиенте, Дата, Общая_сумма
                        ) VALUES (%s, %s, %s, %s)
                    """, (
                        receipt["ID_проката"],
                        receipt["Информация_о_клиенте"], receipt["Дата"],
                        receipt["Общая_сумма"]
                    ))
                    receipt_ids.append(cursor.lastrowid)

                # Позиции квитанций
                receipt_items = ПозицииКвитанции.generate_receipt_items(
                    2000, receipt_ids, service_ids, cassette_ids
                )
                for item in receipt_items:
                    cursor.execute("""
                        INSERT INTO Позиции_квитанции (
                            ID_квитанции, ID_услуги, ID_кассеты,
                            Количество, Цена, Сумма, Примечания
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """, (
                        item["ID_квитанции"],
                        item["ID_услуги"], item["ID_кассеты"],
                        item["Количество"], item["Цена"],
                        item["Сумма"], item["Примечания"]
                    ))

                conn.commit()
            return True
        except Exception as e:
            print(f"Ошибка при генерации данных: {e}")
            if 'conn' in locals() and not conn.closed:
                conn.rollback()
            return False

class Application:
    def __init__(self, root):
        self.visible_columns = visible_columns
        self.root = root
        self.db = DatabaseManager()
        self.current_table = None
        self.setup_ui()
        self.load_tables()

    def setup_ui(self):
        self.root.title("Кинотека")
        self.root.geometry("1200x800")

        main_frame = ttk.Frame(self.root)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        left_panel = ttk.Frame(main_frame, width=200)
        left_panel.pack(side=tk.LEFT, fill=tk.Y)

        ttk.Label(left_panel, text="Таблицы").pack()
        self.tables_listbox = tk.Listbox(left_panel)
        self.tables_listbox.pack(fill=tk.BOTH, expand=True)
        self.tables_listbox.bind('<<ListboxSelect>>', self.on_table_select)

        right_panel = ttk.Frame(main_frame)
        right_panel.pack(side=tk.RIGHT, fill=tk.BOTH, expand=True)

        counter_frame = ttk.Frame(right_panel)
        counter_frame.pack(fill=tk.X, pady=5)
        ttk.Label(counter_frame, text="Количество строк:").pack(side=tk.LEFT)
        self.counter_var = tk.StringVar()
        ttk.Entry(counter_frame, textvariable=self.counter_var, width=10, state='readonly').pack(side=tk.LEFT)

        self.table = ttk.Treeview(right_panel)
        v_scroll = ttk.Scrollbar(right_panel, orient="vertical", command=self.table.yview)
        h_scroll = ttk.Scrollbar(right_panel, orient="horizontal", command=self.table.xview)
        self.table.configure(yscrollcommand=v_scroll.set, xscrollcommand=h_scroll.set)
        self.table.pack(fill=tk.BOTH, expand=True)
        v_scroll.pack(side=tk.RIGHT, fill=tk.Y)
        h_scroll.pack(side=tk.BOTTOM, fill=tk.X)

        btn_frame = ttk.Frame(right_panel)
        btn_frame.pack(fill=tk.X, pady=5)
        ttk.Button(btn_frame, text="Сгенерировать", command=self.generate_random_data).pack(side=tk.LEFT)
        ttk.Button(btn_frame, text="Добавить", command=self.open_add_dialog).pack(side=tk.LEFT)
        ttk.Button(btn_frame, text="Удалить", command=self.delete_selected_row).pack(side=tk.LEFT)
        ttk.Button(btn_frame, text="Обновить", command=self.refresh_data).pack(side=tk.LEFT)
        ttk.Button(btn_frame, text="Выход", command=self.root.quit).pack(side=tk.RIGHT)

    def load_tables(self):
        self.tables_listbox.delete(0, tk.END)
        for table in self.db.get_tables():
            self.tables_listbox.insert(tk.END, table)

    def on_table_select(self, event):
        selection = self.tables_listbox.curselection()
        if selection:
            self.current_table = self.tables_listbox.get(selection[0])
            self.load_table_data()

    def load_table_data(self):
        self.table.delete(*self.table.get_children())
        columns, data = self.db.get_table_data(self.current_table)
        self.table["columns"] = columns
        self.table.heading("#0", text="ID", anchor=tk.W)
        for col in columns:
            self.table.heading(col, text=col)
            self.table.column(col, width=10, minwidth=150, stretch=tk.YES)
        for row in data:
            self.table.insert("", tk.END, values=row)
        self.counter_var.set(str(len(data)))

    def show_add_dialog(self):
        # ...

        # Выводим определенные ID в диалоге "Добавить запись"
        if self.current_table:
            columns, _ = self.db.get_table_data(self.current_table)
            hidden_columns = self.db.get_hidden_columns(self.current_table)
            id_visible = [col for col in columns if col.startswith('id_') and col not in hidden_columns]
            print("Определенные ID:", id_visible)

    def open_add_dialog(self):
        show_add_dialog(
            parent=self.root,
            db=self.db,
            current_table=self.current_table,
            refresh_callback=self.load_table_data
        )

    def generate_random_data(self):
        if messagebox.askyesno("Подтвердите", "Сгенерировать новые данные для указанных ID?"):
            if self.db.generate_random_data():
                self.load_table_data()
                messagebox.showinfo("Успех", "Данные сгенерированы")
            else:
                messagebox.showerror("Ошибка", "Ошибка генерации")

    def delete_selected_row(self):
        if not self.current_table:
            return
        selection = self.table.selection()
        if selection:
            item = self.table.item(selection[0])
            if messagebox.askyesno("Подтвердите", "Удалить запись?"):
                if self.db.delete_row(
                    self.current_table,
                    self.table["columns"][0],  # Первая колонка как ID
                    item["values"][0]          # Значение первой колонки
                ):
                    self.load_table_data()

    def refresh_data(self):
        if self.current_table:
            self.load_table_data()

if __name__ == "__main__":
    root = tk.Tk()
    app = Application(root)
    root.mainloop()
