import requests
import time
import csv
import psycopg2
from datetime import datetime, timedelta

# Конфигурация Яндекс.Директ
YANDEX_TOKEN = "y0__xCfm56NBhi4uzgg2IHdxxMB-11ibEFeXtYCgMHlML7g5RHDNA"  
DATE = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")  # Данные за позавчера
MAX_RETRIES = 3
RETRY_DELAY = 5  # Секунды между попытками

# Параметры подключения к PostgreSQL
DB_PARAMS = {
    'host': 'localhost',
    'database': 'pfserver',
    'user': 'postgres',
    'password': 'pfserverivo',
    'port': '5432'
}

def get_campaign_stats(token, date):
    """Получает статистику из Яндекс.Директ"""
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept-Language": "ru",
        "Content-Type": "application/json"
    }

    body = {
        "params": {
            "SelectionCriteria": {
                "DateFrom": date,
                "DateTo": date
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
    """Создает таблицу в PostgreSQL"""
    create_table_query = """
    CREATE TABLE IF NOT EXISTS campaign_data (
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

def process_and_load_data(conn, tsv_data):
    """Обрабатывает TSV данные и загружает в PostgreSQL"""
    # Удаляем возможные пустые строки в конце
    tsv_data = tsv_data.strip()
    
    # Разделяем строки
    lines = tsv_data.split('\n')
    
    # Пропускаем первые две строки (заголовок и пустую строку)
    lines = lines[2:]
    
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
    
    insert_query = """
    INSERT INTO campaign_data (
        date, campaign_id, campaign_name, ad_id, impressions, 
        clicks, cost, avg_click_position, device, 
        location_of_presence_id, match_type, slot
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    with conn.cursor() as cursor:
        cursor.executemany(insert_query, data_to_insert)
    conn.commit()
    print(f"Успешно импортировано {len(data_to_insert)} строк")

def main():
    print(f"Запрос данных из Яндекс.Директ за {DATE}")
    
    # Получаем данные из API
    data = get_campaign_stats(YANDEX_TOKEN, DATE)
    
    if not data:
        print("Не удалось получить данные из Яндекс.Директ")
        return
    
    print("Успешно получены данные (первые 100 символов):")
    print(data[:100] + "...")
    
    # Подключаемся к PostgreSQL
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        
        # Создаем таблицу (если не существует)
        create_table(conn)
        
        # Обрабатываем и загружаем данные
        process_and_load_data(conn, data)
        
    except Exception as e:
        print(f"Ошибка при работе с PostgreSQL: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == '__main__':
    main()
