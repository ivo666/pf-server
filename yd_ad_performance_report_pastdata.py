import requests
import logging
import time
import uuid
import configparser
from datetime import datetime, timedelta
import psycopg2
import os
from psycopg2.extras import execute_batch

# Настройка логирования
log_file = '/var/log/yandex_direct_loader.log'
os.makedirs(os.path.dirname(log_file), exist_ok=True)

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler(log_file)],
    force=True
)

console_logger = logging.getLogger('console')
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(message)s')
console_handler.setFormatter(formatter)
console_logger.addHandler(console_handler)
console_logger.propagate = False

logger = logging.getLogger(__name__)

# Константы
REQUEST_DELAY = 15
MAX_RETRIES = 3
RETRY_DELAY = 30

def log_console(message):
    console_logger.info(message)

def load_config():
    config = configparser.ConfigParser()
    config.read('config.ini')
    return {
        'db': {
            'host': config['Database']['HOST'],
            'database': config['Database']['DATABASE'],
            'user': config['Database']['USER'],
            'password': config['Database']['PASSWORD'],
            'port': config['Database']['PORT']
        },
        'yandex': {
            'token': config['YandexDirect']['ACCESS_TOKEN']
        }
    }

def create_table_if_not_exists(conn):
    with conn.cursor() as cursor:
        cursor.execute("CREATE SCHEMA IF NOT EXISTS rdl;")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS rdl.yd_ad_performance_report (
                date DATE NOT NULL,
                campaign_id BIGINT NOT NULL,
                campaign_name TEXT,
                ad_id BIGINT NOT NULL,
                impressions INTEGER,
                clicks INTEGER,
                cost DECIMAL(18, 2),
                avg_click_position DECIMAL(10, 2),
                device TEXT,
                location_of_presence_id INTEGER,
                match_type TEXT,
                slot TEXT,
                PRIMARY KEY (date, campaign_id, ad_id)
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS rdl.yd_raw_data (
                id SERIAL PRIMARY KEY,
                date DATE NOT NULL,
                raw_data TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
        conn.commit()

def save_raw_data_to_db(conn, date, raw_data):
    with conn.cursor() as cursor:
        cursor.execute(
            "INSERT INTO rdl.yd_raw_data (date, raw_data) VALUES (%s, %s)",
            (date, raw_data)
        )
        conn.commit()

def get_campaign_stats(token, date):
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
            "ReportName": str(uuid.uuid4()),
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
                timeout=120
            )

            if response.status_code == 200:
                return response.text
            elif response.status_code == 201:
                time.sleep(RETRY_DELAY * (attempt + 1))
                continue
            else:
                logger.error(f"API Error: {response.status_code} - {response.text}")
                return None
        except Exception as e:
            logger.error(f"Request failed: {str(e)}")
            time.sleep(RETRY_DELAY * (attempt + 1))
    
    return None

def process_data(conn, raw_data, date):
    """Корректная обработка TSV данных"""
    if not raw_data:
        return []
    
    lines = [line for line in raw_data.split('\n') if line.strip()]
    if not lines:
        return []
    
    header = []
    data_lines = []
    
    for line in lines:
        if line.startswith('Дата\t'):
            header = line.strip().split('\t')
        else:
            data_lines.append(line)
    
    if not header:
        logger.error("Не найден заголовок в TSV данных")
        return []
    
    expected_columns = 12  # Количество ожидаемых столбцов
    
    data = []
    for line in data_lines:
        parts = line.strip().split('\t')
        if len(parts) != expected_columns:
            logger.warning(f"Пропущена строка (несоответствие столбцов): {line[:200]}...")
            continue
        
        try:
            record = {
                'Date': parts[0],
                'CampaignId': int(parts[1]) if parts[1] else 0,
                'CampaignName': parts[2] if parts[2] else '',
                'AdId': int(parts[3]) if parts[3] else 0,
                'Impressions': int(parts[4]) if parts[4] else 0,
                'Clicks': int(parts[5]) if parts[5] else 0,
                'Cost': float(parts[6].replace(',', '.')) / 1000000 if parts[6] and parts[6] != '--' else 0.0,
                'AvgClickPosition': float(parts[7].replace(',', '.')) if parts[7] and parts[7] != '--' else None,
                'Device': parts[8] if parts[8] else '',
                'LocationOfPresenceId': int(parts[9]) if parts[9] else None,
                'MatchType': parts[10] if parts[10] else '',
                'Slot': parts[11] if parts[11] else ''
            }
            data.append(record)
        except Exception as e:
            logger.error(f"Ошибка обработки строки: {line[:200]}... Ошибка: {str(e)}")
            continue
    
    return data

def main():
    try:
        config = load_config()
        log_console("Конфигурация загружена")

        conn = psycopg2.connect(**config['db'])
        create_table_if_not_exists(conn)

        end_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        start_date = "2025-07-01"

        current_date = datetime.strptime(start_date, "%Y-%m-%d").date()
        end_date = datetime.strptime(end_date, "%Y-%m-%d").date()

        while current_date <= end_date:
            date_str = current_date.strftime("%Y-%m-%d")
            log_console(f"Обработка даты: {date_str}")

            raw_data = get_campaign_stats(config['yandex']['token'], date_str)
            if not raw_data:
                log_console(f"Нет данных за {date_str}")
                current_date += timedelta(days=1)
                continue

            save_raw_data_to_db(conn, current_date, raw_data)

            data = process_data(conn, raw_data, date_str)
            if data:
                with conn.cursor() as cursor:
                    execute_batch(cursor, """
                        INSERT INTO rdl.yd_ad_performance_report (
                            date, campaign_id, campaign_name, ad_id, impressions, clicks,
                            cost, avg_click_position, device, location_of_presence_id,
                            match_type, slot
                        ) VALUES (
                            %(Date)s, %(CampaignId)s, %(CampaignName)s, %(AdId)s,
                            %(Impressions)s, %(Clicks)s, %(Cost)s, %(AvgClickPosition)s,
                            %(Device)s, %(LocationOfPresenceId)s, %(MatchType)s, %(Slot)s
                        ) ON CONFLICT (date, campaign_id, ad_id) DO UPDATE SET
                            impressions = EXCLUDED.impressions,
                            clicks = EXCLUDED.clicks,
                            cost = EXCLUDED.cost,
                            avg_click_position = EXCLUDED.avg_click_position,
                            device = EXCLUDED.device,
                            location_of_presence_id = EXCLUDED.location_of_presence_id,
                            match_type = EXCLUDED.match_type,
                            slot = EXCLUDED.slot
                    """, data)
                    conn.commit()
                    log_console(f"Сохранено {len(data)} записей за {date_str}")

            current_date += timedelta(days=1)
            time.sleep(REQUEST_DELAY)

        log_console("Выгрузка завершена")

    except Exception as e:
        logger.error(f"Fatal error: {str(e)}", exc_info=True)
        log_console(f"Ошибка: {str(e)}")
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()
