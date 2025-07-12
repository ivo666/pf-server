import requests
import time
import psycopg2
import configparser
from datetime import datetime, timedelta
import random
from yandex_direct import Client  # Импортируем клиент Яндекс.Директ

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

# Инициализация клиента Яндекс.Директ
client = Client(YANDEX_TOKEN)

# Параметры запросов
MAX_WAIT_MINUTES = 30  # Максимальное время ожидания отчета (минуты)
CHECK_DELAY = 30      # Задержка между проверками статуса (секунды)
REQUEST_DELAY = 3     # Задержка между запросами для разных дней (секунды)

def get_campaign_stats(token, date_from, date_to):
    """Получает статистику с проверкой статуса отчета"""
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept-Language": "ru",
        "Content-Type": "application/json"
    }

    # Уникальное имя отчета
    report_name = f"API_Report_{date_from}_{random.randint(1000, 9999)}"
    
    body = {
        "params": {
            "SelectionCriteria": {"DateFrom": date_from, "DateTo": date_to},
            "FieldNames": [
                "Date", "CampaignId", "CampaignName", "AdId",
                "Impressions", "Clicks", "Cost", "AvgClickPosition",
                "Device", "LocationOfPresenceId", "MatchType", "Slot"
            ],
            "ReportName": report_name,
            "ReportType": "AD_PERFORMANCE_REPORT",
            "DateRangeType": "CUSTOM_DATE",
            "Format": "TSV",
            "IncludeVAT": "YES",
            "IncludeDiscount": "NO"
        }
    }

    try:
        # Отправляем запрос на формирование отчета
        response = requests.post(
            "https://api.direct.yandex.com/json/v5/reports",
            headers=headers,
            json=body,
            timeout=60
        )

        if response.status_code == 200:
            print(f"{date_from}: отчет готов сразу")
            return response.text
        elif response.status_code == 201:
            request_id = response.headers.get('Request-Id')
            print(f"{date_from}: отчет формируется (ID: {request_id})")
            return wait_for_report(request_id, date_from)
        else:
            print(f"{date_from}: ошибка запроса (код {response.status_code}): {response.text}")
            return None

    except Exception as e:
        print(f"{date_from}: ошибка соединения: {str(e)}")
        return None

def wait_for_report(request_id, date_str):
    """Ожидает готовности отчета с выводом статуса"""
    start_time = time.time()
    
    while True:
        # Проверяем статус отчета
        try:
            info = client.info(requestId=request_id).get()
            status = info["log_request"]["status"]
            print(f"{date_str}: статус обработки - {status}")
            
            if status == "processed":
                print(f"{date_str}: отчет успешно сформирован")
                return download_report(request_id)
            elif status == "created" or status == "pending":
                if time.time() - start_time > MAX_WAIT_MINUTES * 60:
                    print(f"{date_str}: превышено время ожидания ({MAX_WAIT_MINUTES} минут)")
                    return None
                time.sleep(CHECK_DELAY)
            else:
                print(f"{date_str}: ошибка обработки отчета - {status}")
                return None
                
        except Exception as e:
            print(f"{date_str}: ошибка при проверке статуса: {str(e)}")
            return None

def download_report(request_id):
    """Загружает готовый отчет"""
    try:
        response = client.download_report(requestId=request_id)
        return response.text
    except Exception as e:
        print(f"Ошибка при загрузке отчета: {str(e)}")
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

def process_and_load_data(conn, tsv_data, date_str):
    """Обрабатывает и загружает данные"""
    if not tsv_data:
        print(f"{date_str}: нет данных для загрузки")
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
            print(f"{date_str}: ошибка в строке данных - {str(e)}")
            continue
    
    if not data_to_insert:
        print(f"{date_str}: нет валидных данных для загрузки")
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
        
        print(f"{date_str}: успешно загружено {len(data_to_insert)} строк")
        return len(data_to_insert)
        
    except Exception as e:
        print(f"{date_str}: ошибка при загрузке в БД - {str(e)}")
        conn.rollback()
        return 0

def main():
    start_date = datetime(2025, 7, 1).date()
    end_date = (datetime.now() - timedelta(days=1)).date()
    
    print(f"Начало загрузки данных с {start_date} по {end_date}")
    
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        create_table(conn)
        
        current_date = start_date
        while current_date <= end_date:
            date_str = current_date.strftime('%Y-%m-%d')
            print(f"\nОбработка данных за {date_str}")
            
            data = get_campaign_stats(YANDEX_TOKEN, date_str, date_str)
            
            if data:
                process_and_load_data(conn, data, date_str)
            else:
                print(f"{date_str}: не удалось получить данные")
            
            current_date += timedelta(days=1)
            if current_date <= end_date:
                time.sleep(REQUEST_DELAY)
                
    except Exception as e:
        print(f"\nКритическая ошибка: {str(e)}")
    finally:
        if conn:
            conn.close()
        print("\nЗавершение работы скрипта")

if __name__ == '__main__':
    main()
