from datetime import datetime, timedelta
from tapi_yandex_metrika import YandexMetrikaLogsapi
import pandas as pd
from time import sleep
import os
import configparser
import psycopg2
from psycopg2.extras import execute_batch

dirname = os.path.dirname(__file__)
# Чтение конфигурации
config = configparser.ConfigParser()
config.read(os.path.join(dirname, "config.ini"))

# Параметры подключения к БД
DB_CONFIG = {
    'host': config['Database']['HOST'],
    'database': config['Database']['DATABASE'],
    'user': config['Database']['USER'],
    'password': config['Database']['PASSWORD']
}

# Параметры Яндекс.Метрики
ACCESS_TOKEN = config['YandexMetrika']['ACCESS_TOKEN']
COUNTER_ID = config['YandexMetrika']['COUNTER_ID']

# Поля для выгрузки hits (без pageViewID)
FIELDS = [
    'ym:pv:watchID',
    'ym:pv:clientID',
    'ym:pv:dateTime',
    'ym:pv:title',
    'ym:pv:URL',
    'ym:pv:isPageView'
]

def get_week_ranges(start_date, end_date):
    """Генерирует список диапазонов дат по неделям"""
    current_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")
    
    week_ranges = []
    
    while current_date <= end_date:
        week_start = current_date
        week_end = current_date + timedelta(days=6)
        if week_end > end_date:
            week_end = end_date
        
        week_ranges.append((
            week_start.strftime("%Y-%m-%d"),
            week_end.strftime("%Y-%m-%d")
        ))
        
        current_date = week_end + timedelta(days=1)
    
    return week_ranges

def wait_for_report_processing(client, request_id, check_interval=30):
    """Ожидает завершения обработки отчета"""
    while True:
        info = client.info(requestId=request_id).get()
        status = info["log_request"]["status"]
        
        if status == "processed":
            return info
        elif status == "created" or status == "pending":
            print(f"Отчет в обработке, статус: {status}. Ожидаю {check_interval} секунд...")
            sleep(check_interval)
        else:
            raise Exception(f"Ошибка обработки отчета. Статус: {status}")

def download_report_parts(client, request_id, parts_count):
    """Загружает все части отчета и объединяет их в один DataFrame"""
    all_data = []
    
    for part_number in range(parts_count):
        print(f"Загрузка части {part_number + 1} из {parts_count}...")
        part = client.download(requestId=request_id, partNumber=part_number).get()
        part_data = part().to_dicts()
        all_data.extend(part_data)
    
    return pd.DataFrame(all_data, columns=FIELDS)

def create_connection():
    """Создает подключение к PostgreSQL"""
    return psycopg2.connect(**DB_CONFIG)

def create_table(conn):
    """Создает таблицу для хранения hits с watchID в качестве ключа"""
    with conn.cursor() as cursor:
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS yandex_metrika_hits (
            watch_id TEXT PRIMARY KEY,
            client_id TEXT,
            date_time TIMESTAMP,
            title TEXT,
            url TEXT,
            is_page_view TEXT
        )
        """)
    conn.commit()

def insert_data(conn, df):
    """Вставляет данные hits в PostgreSQL"""
    with conn.cursor() as cursor:
        data = []
        for _, row in df.iterrows():
            data.append((
                row.get('ym:pv:watchID'),
                row.get('ym:pv:clientID'),
                pd.to_datetime(row.get('ym:pv:dateTime')),
                row.get('ym:pv:title'),
                row.get('ym:pv:URL'),
                row.get('ym:pv:isPageView')
            ))

        execute_batch(cursor, """
            INSERT INTO yandex_metrika_hits (
                watch_id, client_id, date_time,
                title, url, is_page_view
            ) VALUES (
                %s, %s, %s,
                %s, %s, %s
            )
            ON CONFLICT (watch_id) DO NOTHING
        """, data)
    conn.commit()

def process_week(client, conn, date1, date2):
    """Обрабатывает данные hits за одну неделю"""
    try:
        # Запрос данных
        params = {
            "fields": ",".join(FIELDS),
            "source": "hits",
            "date1": date1,
            "date2": date2
        }

        # Создание и обработка запроса
        print(f"Запрашиваю данные hits с {date1} по {date2}...")
        request = client.create().post(params=params)
        request_id = request["log_request"]["request_id"]
        print(f"Запрос создан, ID: {request_id}")
        
        # Ожидание обработки
        info = wait_for_report_processing(client, request_id)
        parts_count = len(info["log_request"]["parts"])
        print(f"Отчет обработан, количество частей: {parts_count}")
        
        # Загрузка данных
        df = download_report_parts(client, request_id, parts_count)
        
        # Сохранение в БД
        insert_data(conn, df)
        print(f"Успешно сохранено {len(df)} записей hits с {date1} по {date2}")
        
    except Exception as e:
        print(f"Ошибка при обработке недели с {date1} по {date2}: {e}")
        raise

def cleanup_temp_files():
    """Удаление всех временных файлов"""
    temp_files = [
        "yandex_metrika_pageviews.csv",
        "yandex_metrika_pageviews.json",
        "yandex_metrika_pageviews.xlsx"
    ]
    
    for file in temp_files:
        try:
            if os.path.exists(file):
                os.remove(file)
                print(f"Временный файл {file} удалён")
        except Exception as e:
            print(f"Ошибка при удалении файла {file}: {e}")

def main():
    try:
        # Инициализация клиента Яндекс.Метрики
        client = YandexMetrikaLogsapi(
            access_token=ACCESS_TOKEN,
            default_url_params={'counterId': COUNTER_ID}
        )

        # Получаем диапазоны дат по неделям
        week_ranges = get_week_ranges("2025-06-15", "2025-06-16")
        
        # Подключение к БД
        conn = create_connection()
        create_table(conn)
        
        # Обработка каждой недели
        for i, (date1, date2) in enumerate(week_ranges, 1):
            print(f"\nОбработка недели {i} из {len(week_ranges)}: {date1} - {date2}")
            try:
                process_week(client, conn, date1, date2)
            except Exception as e:
                print(f"Прерывание обработки недели {date1}-{date2} из-за ошибки: {e}")
                sleep(60)  # Задержка перед повторной попыткой
                continue
                
    except Exception as e:
        print(f"Произошла критическая ошибка: {e}")
    finally:
        if 'conn' in locals() and conn is not None:
            conn.close()
        cleanup_temp_files()

if __name__ == "__main__":
    main()
