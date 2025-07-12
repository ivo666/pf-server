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
MAX_RETRIES = 5
RETRY_DELAY = 15
REQUEST_DELAY = 3

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
    """Создает таблицу в PostgreSQL с уникальным ограничением"""
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
            slot TEXT,
            UNIQUE(date, campaign_id, ad_id, device, location_of_presence_id, match_type, slot)
        )
        """)
    conn.commit()

def check_date_exists(conn, date):
    """Проверяет, есть ли уже данные за указанную дату"""
    with conn.cursor() as cursor:
        cursor.execute("""
        SELECT EXISTS(
            SELECT 1 FROM rdl.ad_performance_report 
            WHERE date = %s 
            LIMIT 1
        )
        """, (date,))
        return cursor.fetchone()[0]

def process_and_load_data(conn, tsv_data, date):
    """Обрабатывает и загружает данные с проверкой на дубликаты"""
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
    
    loaded_count = 0
    for data in data_to_insert:
        try:
            with conn.cursor() as cursor:
                # Используем ON CONFLICT DO NOTHING для пропуска дубликатов
                cursor.execute("""
                INSERT INTO rdl.ad_performance_report (
                    date, campaign_id, campaign_name, ad_id, impressions, 
                    clicks, cost, avg_click_position, device, 
                    location_of_presence_id, match_type, slot
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (date, campaign_id, ad_id, device, location_of_presence_id, match_type, slot) DO NOTHING
                """, data)
                if cursor.rowcount > 0:
                    loaded_count += 1
            conn.commit()
        except Exception as e:
            print(f"{date}: ошибка при загрузке строки - {str(e)}")
            conn.rollback()
            continue
    
    print(f"{date}: загружено {loaded_count} новых строк (из {len(data_to_insert)})")
    return loaded_count

def main():
    start_date = datetime(2025, 5, 1).date()
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
            
            # Проверяем, есть ли уже данные за эту дату
            if check_date_exists(conn, current_date):
                print(f"{date_str}: данные уже существуют, пропускаем")
                current_date += timedelta(days=1)
                continue
            
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
