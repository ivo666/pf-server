import requests
import time
import psycopg2
import configparser
from datetime import datetime, timedelta

# Чтение конфигурации
config = configparser.ConfigParser()
config.read('config.ini')

# Настройки подключения
DB_PARAMS = {
    'host': config['Database']['HOST'],
    'database': config['Database']['DATABASE'],
    'user': config['Database']['USER'],
    'password': config['Database']['PASSWORD'],
    'port': config['Database']['PORT']
}
YANDEX_TOKEN = config['YandexDirect']['ACCESS_TOKEN']

# Параметры запросов
MAX_RETRIES = 5  # Увеличим количество попыток
RETRY_DELAY = 15  # Увеличим задержку между попытками
REQUEST_DELAY = 3  # Пауза между днями

def get_campaign_stats(token, date):
    """Получает статистику за день с повторными попытками"""
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept-Language": "ru",
        "Content-Type": "application/json"
    }

    body = {
        "params": {
            "SelectionCriteria": {"DateFrom": date, "DateTo": date},
            "FieldNames": [
                "Date", "CampaignId", "CampaignName", "AdId",
                "Impressions", "Clicks", "Cost", "AvgClickPosition",
                "Device", "LocationOfPresenceId", "MatchType", "Slot"
            ],
            "ReportName": f"API_Report_{date.replace('-', '')}",
            "ReportType": "AD_PERFORMANCE_REPORT",
            "DateRangeType": "CUSTOM_DATE",
            "Format": "TSV",
            "IncludeVAT": "YES",
            "IncludeDiscount": "NO"
        }
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.post(
                "https://api.direct.yandex.com/json/v5/reports",
                headers=headers,
                json=body,
                timeout=60
            )

            if response.status_code == 200:
                print(f"{date}: данные получены (попытка {attempt})")
                return response.text
            elif response.status_code == 201:
                print(f"{date}: отчет формируется (попытка {attempt}), ждем {RETRY_DELAY} сек...")
                time.sleep(RETRY_DELAY)
                continue
            else:
                print(f"{date}: ошибка {response.status_code} (попытка {attempt})")
                time.sleep(RETRY_DELAY)
                continue

        except Exception as e:
            print(f"{date}: ошибка соединения (попытка {attempt}): {str(e)}")
            time.sleep(RETRY_DELAY)
            continue

    print(f"{date}: не удалось получить данные после {MAX_RETRIES} попыток")
    return None

def create_table(conn):
    """Создает таблицу в PostgreSQL"""
    with conn.cursor() as cursor:
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS rdl.ad_performance_report (
            date DATE,
            campaign_id BIGINT,
            campaign_name TEXT,
            ad_id BIGINT,
            impressions INTEGER,
            clicks INTEGER,
            cost DECIMAL(15,2),
            avg_click_position TEXT,
            device TEXT,
            location_of_presence_id INTEGER,
            match_type TEXT,
            slot TEXT
        )
        """)
    conn.commit()

def process_and_load_data(conn, tsv_data, date):
    """Обрабатывает и загружает данные"""
    if not tsv_data:
        print(f"{date}: нет данных для загрузки")
        return 0
    
    lines = tsv_data.strip().split('\n')[2:]  # Пропускаем заголовки
    
    data_to_insert = []
    for line in lines:
        if not line or line.startswith('Total rows:'):
            continue
            
        row = line.split('\t')
        if len(row) != 12:
            continue
            
        try:
            data_to_insert.append((
                datetime.strptime(row[0], '%Y-%m-%d').date(),
                int(row[1]),
                row[2],
                int(row[3]),
                int(row[4]),
                int(row[5]),
                float(row[6]),
                None if row[7] == '--' else row[7],
                row[8],
                int(row[9]),
                row[10],
                row[11]
            ))
        except ValueError as e:
            print(f"{date}: ошибка в строке данных - {str(e)}")
            continue
    
    if not data_to_insert:
        print(f"{date}: нет валидных данных")
        return 0
    
    try:
        with conn.cursor() as cursor:
            cursor.executemany("""
            INSERT INTO rdl.ad_performance_report (
                date, campaign_id, campaign_name, ad_id, impressions, 
                clicks, cost, avg_click_position, device, 
                location_of_presence_id, match_type, slot
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, data_to_insert)
        conn.commit()
        
        print(f"{date}: успешно загружено {len(data_to_insert)} строк")
        return len(data_to_insert)
        
    except Exception as e:
        print(f"{date}: ошибка при загрузке в БД - {str(e)}")
        conn.rollback()
        return 0

def main():
    start_date = datetime(2025, 7, 1).date()
    end_date = (datetime.now() - timedelta(days=1)).date()
    
    print(f"Начало загрузки данных с {start_date} по {end_date}")
    
    conn = None
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        create_table(conn)
        
        current_date = start_date
        while current_date <= end_date:
            date_str = current_date.strftime('%Y-%m-%d')
            print(f"\n--- Обработка {date_str} ---")
            
            data = get_campaign_stats(YANDEX_TOKEN, date_str)
            
            if data:
                process_and_load_data(conn, data, date_str)
            else:
                print(f"{date_str}: пропуск из-за ошибки")
            
            current_date += timedelta(days=1)
            if current_date <= end_date:
                time.sleep(REQUEST_DELAY)
                
    except Exception as e:
        print(f"\nКритическая ошибка: {str(e)}")
    finally:
        if conn:
            conn.close()
        print("\nЗавершение работы")

if __name__ == '__main__':
    main()
