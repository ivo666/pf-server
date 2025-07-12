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

def get_campaign_stats(token, date):
    """Получает статистику за указанную дату"""
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
            "ReportName": f"YD_Report_{date.replace('-', '')}",
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
                print(f"Данные за {date} успешно получены (попытка {attempt})")
                return response.text
            elif response.status_code == 201:
                print(f"Отчет за {date} формируется (попытка {attempt}), ждем {RETRY_DELAY} сек...")
                time.sleep(RETRY_DELAY)
                continue
            else:
                print(f"Ошибка {response.status_code} при запросе за {date} (попытка {attempt})")
                time.sleep(RETRY_DELAY)
                continue

        except Exception as e:
            print(f"Ошибка соединения при запросе за {date} (попытка {attempt}): {str(e)}")
            time.sleep(RETRY_DELAY)
            continue

    print(f"Не удалось получить данные за {date} после {MAX_RETRIES} попыток")
    return None

def create_table(conn):
    """Создает целевую таблицу, если она не существует"""
    with conn.cursor() as cursor:
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS rdl.yd_ad_performance_report (
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
    """Проверяет, есть ли данные за указанную дату"""
    with conn.cursor() as cursor:
        cursor.execute("""
        SELECT EXISTS(
            SELECT 1 FROM rdl.yd_ad_performance_report
            WHERE date = %s 
            LIMIT 1
        )
        """, (date,))
        return cursor.fetchone()[0]

def process_and_load_data(conn, tsv_data, date):
    """Обрабатывает и загружает данные, пропуская дубликаты"""
    if not tsv_data:
        print(f"Нет данных для загрузки за {date}")
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
            print(f"Ошибка в строке данных за {date}: {str(e)}")
            continue
    
    if not data_to_insert:
        print(f"Нет валидных данных для загрузки за {date}")
        return 0
    
    loaded_count = 0
    for data in data_to_insert:
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                INSERT INTO rdl.yd_ad_performance_report (
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
            print(f"Ошибка при загрузке данных за {date}: {str(e)}")
            conn.rollback()
            continue
    
    print(f"Загружено {loaded_count} новых строк за {date} (из {len(data_to_insert)})")
    return loaded_count

def main():
    # Определяем вчерашнюю дату
    target_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    print(f"Загрузка данных за {target_date}")
    
    conn = None
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        create_table(conn)
        
        # Проверяем, есть ли уже данные за эту дату
        if check_date_exists(conn, datetime.strptime(target_date, '%Y-%m-%d').date()):
            print(f"Данные за {target_date} уже существуют, загрузка не требуется")
            return
        
        # Получаем и загружаем данные
        data = get_campaign_stats(YANDEX_TOKEN, target_date)
        if data:
            process_and_load_data(conn, data, target_date)
        else:
            print(f"Не удалось получить данные за {target_date}")
                
    except Exception as e:
        print(f"Критическая ошибка: {str(e)}")
    finally:
        if conn:
            conn.close()
        print("Завершение работы")

if __name__ == '__main__':
    main()
