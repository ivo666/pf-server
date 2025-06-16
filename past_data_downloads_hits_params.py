from datetime import datetime, timedelta
from tapi_yandex_metrika import YandexMetrikaLogsapi
import pandas as pd
from time import sleep
import os
import configparser
import psycopg2
from psycopg2.extras import execute_batch
import json
import re

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

# Поля для выгрузки hits с параметрами
FIELDS = [
    'ym:pv:watchID',
    'ym:pv:pageViewID',
    'ym:pv:clientID',
    'ym:pv:dateTime',
    'ym:pv:title',
    'ym:pv:URL',
    'ym:pv:isPageView',
    'ym:pv:artificial',
    'ym:pv:params',
    'ym:pv:parsedParamsKey1',
    'ym:pv:parsedParamsKey2',
    'ym:pv:parsedParamsKey3'
]

def get_date_range():
    """Генерирует диапазон дат с 1 июня по вчера"""
    start_date = datetime(2025, 6, 1).date()
    end_date = datetime.now().date() - timedelta(days=1)
    
    dates = []
    current_date = start_date
    
    while current_date <= end_date:
        date_str = current_date.strftime("%Y-%m-%d")
        dates.append(date_str)
        current_date += timedelta(days=1)
    
    return dates

def parse_event_params(params_str):
    """Парсит параметры событий из строки"""
    if not params_str or pd.isna(params_str) or params_str == '{}':
        return {}
    
    # Если данные уже являются словарем
    if isinstance(params_str, dict):
        return params_str
    
    try:
        # Обработка строк вида "{""key"":""value""}"
        if params_str.startswith('"{') and params_str.endswith('}"'):
            fixed_str = params_str[1:-1].replace('""', '"')
            return json.loads(fixed_str)
        
        # Обработка строк вида {key:value}
        if not ('"' in params_str or "'" in params_str):
            fixed_str = re.sub(r'([{,]\s*)(\w+)(\s*:)', r'\1"\2"\3', params_str)
            fixed_str = re.sub(r':\s*([^"\s][^,}]*)([,}])', r':"\1"\2', fixed_str)
            return json.loads(fixed_str)
        
        # Стандартный JSON
        return json.loads(params_str)
    
    except json.JSONDecodeError as e:
        print(f"Не удалось разобрать параметры: {params_str}")
        print(f"Ошибка: {e}")
        return {}

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
    """Создает таблицу для хранения hits с параметрами"""
    with conn.cursor() as cursor:
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS yandex_metrika_hits_with_params (
            watch_id TEXT PRIMARY KEY,
            page_view_id TEXT,
            client_id TEXT,
            date_time TIMESTAMP,
            title TEXT,
            url TEXT,
            is_page_view TEXT,
            artificial TEXT,
            event_category TEXT,
            event_action TEXT,
            event_label TEXT,
            button_location TEXT,
            event_content TEXT,
            event_context TEXT,
            action_group TEXT,
            page_path TEXT
        )
        """)
    conn.commit()

def insert_data(conn, df):
    """Вставляет данные hits с параметрами в PostgreSQL"""
    with conn.cursor() as cursor:
        data = []
        for _, row in df.iterrows():
            params = parse_event_params(row.get('ym:pv:params'))
            
            data.append((
                str(row.get('ym:pv:watchID')),
                str(row.get('ym:pv:pageViewID')) if pd.notna(row.get('ym:pv:pageViewID')) else None,
                str(row.get('ym:pv:clientID')) if pd.notna(row.get('ym:pv:clientID')) else None,
                pd.to_datetime(row.get('ym:pv:dateTime')),
                row.get('ym:pv:title'),
                row.get('ym:pv:URL'),
                str(row.get('ym:pv:isPageView')) if pd.notna(row.get('ym:pv:isPageView')) else None,
                str(row.get('ym:pv:artificial')) if pd.notna(row.get('ym:pv:artificial')) else None,
                params.get('eventCategory'),
                params.get('eventAction'),
                params.get('eventLabel'),
                params.get('buttonLocation'),
                params.get('eventContent'),
                params.get('eventContext'),
                params.get('actionGroup'),
                params.get('pagePath')
            ))

        execute_batch(cursor, """
            INSERT INTO yandex_metrika_hits_with_params (
                watch_id, page_view_id, client_id, date_time,
                title, url, is_page_view, artificial,
                event_category, event_action, event_label, button_location,
                event_content, event_context, action_group, page_path
            ) VALUES (
                %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s
            )
            ON CONFLICT (watch_id) DO NOTHING
        """, data)
    conn.commit()

def process_day(client, conn, date):
    """Обрабатывает данные hits за один день"""
    try:
        # Запрос данных
        params = {
            "fields": ",".join(FIELDS),
            "source": "hits",
            "date1": date,
            "date2": date
        }

        # Создание и обработка запроса
        print(f"\nЗапрашиваю данные hits за {date}...")
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
        print(f"Успешно сохранено {len(df)} записей hits за {date}")
        
        return True
        
    except Exception as e:
        print(f"Ошибка при обработке дня {date}: {e}")
        return False

def main():
    try:
        # Инициализация клиента Яндекс.Метрики
        client = YandexMetrikaLogsapi(
            access_token=ACCESS_TOKEN,
            default_url_params={'counterId': COUNTER_ID}
        )

        # Получаем список дат для обработки
        dates = get_date_range()
        
        # Подключение к БД
        conn = create_connection()
        create_table(conn)
        
        # Обработка каждого дня
        for date in dates:
            print(f"\nОбработка данных за {date}")
            success = False
            attempts = 0
            max_attempts = 3
            
            while not success and attempts < max_attempts:
                attempts += 1
                success = process_day(client, conn, date)
                
                if not success and attempts < max_attempts:
                    wait_time = attempts * 30
                    print(f"Повторная попытка через {wait_time} секунд...")
                    sleep(wait_time)
                
            if not success:
                print(f"Не удалось обработать данные за {date} после {max_attempts} попыток")
                
    except Exception as e:
        print(f"Произошла критическая ошибка: {e}")
    finally:
        if 'conn' in locals() and conn is not None:
            conn.close()
        print("Обработка завершена")

if __name__ == "__main__":
    main()

if __name__ == "__main__":
    main()
