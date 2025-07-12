import requests
import time
import psycopg2
import configparser
from datetime import datetime, timedelta

# Чтение конфигурации из файла
config = configparser.ConfigParser()
config.read('config.ini')

# Параметры подключения к БД
DB_PARAMS = {
    'host': config['Database']['HOST'],
    'database': config['Database']['DATABASE'],
    'user': config['Database']['USER'],
    'password': config['Database']['PASSWORD'],
    'port': config['Database']['PORT']
}

# Токен Яндекс.Директ
YANDEX_TOKEN = config['YandexDirect']['ACCESS_TOKEN']

# Настройки запросов
MAX_RETRIES = 3
RETRY_DELAY = 15  # Секунды между попытками
REQUEST_DELAY = 20  # Пауза между запросами разных дней

def get_campaign_stats(token, date_from, date_to):
    """Получает статистику из Яндекс.Директ за указанный период"""
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept-Language": "ru",
        "Content-Type": "application/json"
    }

    body = {
        "params": {
            "SelectionCriteria": {
                "DateFrom": date_from,
                "DateTo": date_to
            },
            "FieldNames": [
                "Date",
                "CampaignId",
                "CampaignName",
                "AdId",
                "Impressions",
                "Clicks",
                "Cost",
                "AvgClickPosition",
                "Device",
                "LocationOfPresenceId",
                "MatchType",
                "Slot"
            ],
            "ReportName": "API_Report_Extended222",
            "ReportType": "AD_PERFORMANCE_REPORT",
            "DateRangeType": "CUSTOM_DATE",
            "Format": "TSV",
            "IncludeVAT": "YES",
            "IncludeDiscount": "NO"
        }
    }

    for attempt in range(MAX_RETRIES):
        try:
            response = requests.post(
                "https://api.direct.yandex.com/json/v5/reports",
                headers=headers,
                json=body,
                timeout=30
            )

            if response.status_code == 200:
                return response.text
            elif response.status_code == 201:
                print(f"Отчет формируется... (попытка {attempt + 1})")
                time.sleep(RETRY_DELAY)
            else:
                print(f"Ошибка {response.status_code}: {response.text}")
                return None

        except Exception as e:
            print(f"Ошибка соединения: {str(e)}")
            return None

    print("Достигнуто максимальное число попыток.")
    return None

def create_table(conn):
    """Создает таблицу в схеме rdl в PostgreSQL"""
    create_table_query = """
    CREATE TABLE IF NOT EXISTS rdl.ad_performance_report (
        date DATE,
        campaign_id BIGINT,
        campaign_name TEXT,
        ad_id BIGINT,
        impressions INTEGER,
        clicks INTEGER,
        cost DECIMAL(15, 2),  
        avg_click_position TEXT,
        device TEXT,
        location_of_presence_id INTEGER,
        match_type TEXT,
        slot TEXT
    )
    """
    with conn.cursor() as cursor:
        cursor.execute(create_table_query)
    conn.commit()

def process_and_load_data(conn, tsv_data, date):
    """Обрабатывает TSV данные и загружает в PostgreSQL"""
    if not tsv_data:
        print(f"{date}: нет данных для загрузки")
        return 0
    
    tsv_data = tsv_data.strip()
    lines = tsv_data.split('\n')[2:]  # Пропускаем первые две строки
    
    data_to_insert = []
    for line in lines:
        if not line or line.startswith('Total rows:'):
            continue
            
        row = line.split('\t')
        if len(row) != 12:
            continue
            
        avg_click_pos = None if row[7] == '--' else row[7]
        
        try:
            data_to_insert.append((
                datetime.strptime(row[0], '%Y-%m-%d').date(),
                int(row[1]),
                row[2],
                int(row[3]),
                int(row[4]),
                int(row[5]),
                float(row[6]),
                avg_click_pos,
                row[8],
                int(row[9]),
                row[10],
                row[11]
            ))
        except ValueError as e:
            print(f"Ошибка преобразования данных в строке: {row}. Ошибка: {e}")
            continue
    
    if not data_to_insert:
        print(f"{date}: нет валидных данных для вставки")
        return 0
    
    insert_query = """
    INSERT INTO rdl.ad_performance_report (
        date, campaign_id, campaign_name, ad_id, impressions, 
        clicks, cost, avg_click_position, device, 
        location_of_presence_id, match_type, slot
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    with conn.cursor() as cursor:
        cursor.executemany(insert_query, data_to_insert)
    conn.commit()
    print(f"{date}: успешно загружено {len(data_to_insert)} строк")
    return len(data_to_insert)

def main():
    # Определяем диапазон дат
    start_date = datetime(2025, 7, 1).date()
    end_date = (datetime.now() - timedelta(days=1)).date()
    
    # Подключаемся к PostgreSQL
    conn = None
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        create_table(conn)
        
        current_date = start_date
        while current_date <= end_date:
            date_str = current_date.strftime("%Y-%m-%d")
            data = get_campaign_stats(YANDEX_TOKEN, date_str, date_str)
            
            if data:
                process_and_load_data(conn, data, date_str)
            else:
                print(f"{date_str}: данные не получены")
            
            current_date += timedelta(days=1)
            if current_date <= end_date:
                time.sleep(REQUEST_DELAY)
                
    except Exception as e:
        print(f"Критическая ошибка: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == '__main__':
    main()
