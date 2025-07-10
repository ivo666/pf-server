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
    """Полностью переработанная функция обработки данных"""
    if not raw_data:
        logger.error("Получены пустые данные")
        return []

    # Разделяем строки и удаляем пустые
    lines = [line.strip() for line in raw_data.split('\n') if line.strip()]
    if not lines:
        logger.error("Нет данных для обработки после разделения строк")
        return []

    # Находим строку с заголовками
    header_line = None
    for line in lines:
        if line.startswith('Date\t') or line.startswith('Дата\t'):
            header_line = line
            break

    if not header_line:
        logger.error("Не найдена строка с заголовками")
        return []

    # Определяем индексы столбцов
    headers = header_line.split('\t')
    try:
        date_idx = headers.index('Date') if 'Date' in headers else headers.index('Дата')
        campaign_id_idx = headers.index('CampaignId')
        campaign_name_idx = headers.index('CampaignName')
        ad_id_idx = headers.index('AdId')
        impressions_idx = headers.index('Impressions')
        clicks_idx = headers.index('Clicks')
        cost_idx = headers.index('Cost')
        avg_pos_idx = headers.index('AvgClickPosition')
        device_idx = headers.index('Device')
        location_idx = headers.index('LocationOfPresenceId')
        match_type_idx = headers.index('MatchType')
        slot_idx = headers.index('Slot')
    except ValueError as e:
        logger.error(f"Не найден ожидаемый столбец: {str(e)}")
        return []

    data = []
    for line in lines:
        # Пропускаем строку заголовков и служебные строки
        if line == header_line or not line or line.startswith('ReportName'):
            continue

        parts = line.split('\t')
        if len(parts) != len(headers):
            logger.warning(f"Пропущена строка (несоответствие столбцов): {line[:200]}...")
            continue

        try:
            # Обработка каждой записи
            record = {
                'Date': parts[date_idx],
                'CampaignId': int(parts[campaign_id_idx]),
                'CampaignName': parts[campaign_name_idx],
                'AdId': int(parts[ad_id_idx]),
                'Impressions': int(parts[impressions_idx]) if parts[impressions_idx] else 0,
                'Clicks': int(parts[clicks_idx]) if parts[clicks_idx] else 0,
                'Cost': float(parts[cost_idx].replace(',', '.')) / 1000000 if parts[cost_idx] and parts[cost_idx] != '--' else 0.0,
                'AvgClickPosition': float(parts[avg_pos_idx].replace(',', '.')) if parts[avg_pos_idx] and parts[avg_pos_idx] != '--' else None,
                'Device': parts[device_idx],
                'LocationOfPresenceId': int(parts[location_idx]) if parts[location_idx] else None,
                'MatchType': parts[match_type_idx],
                'Slot': parts[slot_idx]
            }
            data.append(record)
        except Exception as e:
            logger.error(f"Ошибка обработки строки: {line[:200]}... Ошибка: {str(e)}")
            continue

    # Дополнительная проверка данных
    if data:
        logger.info(f"Успешно обработано {len(data)} записей за {date}")
        logger.info("Пример первой записи:")
        logger.info(f"CampaignId: {data[0]['CampaignId']}")
        logger.info(f"AdId: {data[0]['AdId']}")
        logger.info(f"Impressions: {data[0]['Impressions']}")
        logger.info(f"Clicks: {data[0]['Clicks']}")
    else:
        logger.error("Нет данных после обработки")

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
