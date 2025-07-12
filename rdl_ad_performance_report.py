import requests
import time
import psycopg2
import configparser
from datetime import datetime, timedelta
from pathlib import Path

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
MAX_RETRIES = 5
RETRY_DELAY = 10  # Секунды между попытками
REQUEST_DELAY = 2  # Секунды между запросами разных дней

def get_campaign_stats(token, date_from, date_to):
    """Получает статистику из Яндекс.Директ"""
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
            "ReportName": "API_Report_Extended",
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
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY)
                    continue
                else:
                    return None
            else:
                return None

        except Exception:
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY)
                continue
            else:
                return None

    return None

def create_table(conn):
    """Создает таблицу в схеме rdl в PostgreSQL"""
    create_table_query = """
    CREATE TABLE IF NOT EXISTS rdl.yd_ad_performance_report (
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
        except ValueError:
            continue
    
    if not data_to_insert:
        return 0
    
    insert_query = """
    INSERT INTO rdl.yd_ad_performance_report (
        date, campaign_id, campaign_name, ad_id, impressions, 
        clicks, cost, avg_click_position, device, 
        location_of_presence_id, match_type, slot
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    with conn.cursor() as cursor:
        cursor.executemany(insert_query, data_to_insert)
    conn.commit()
    return len(data_to_insert)

def main():
    # Определяем диапазон дат
    start_date = datetime(2025, 5, 1).date()
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
                count = process_and_load_data(conn, data, current_date)
                print(f"{date_str}: загружено {count} строк")
            else:
                print(f"{date_str}: данные не получены")
            
            current_date += timedelta(days=1)
            if current_date <= end_date:
                time.sleep(REQUEST_DELAY)
                
    except Exception:
        pass
    finally:
        if conn:
            conn.close()

if __name__ == '__main__':
    main()
