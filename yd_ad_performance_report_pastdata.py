import requests
import logging
import time
import uuid
import configparser
from datetime import datetime, timedelta
import psycopg2
import os
from psycopg2.extras import execute_batch

# Настройка логирования ТОЛЬКО в файл
log_file = '/var/log/yandex_direct_loader.log'
os.makedirs(os.path.dirname(log_file), exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler(log_file)]  # Только файловый обработчик
)

# Дополнительный обработчик для вывода логов в терминал (только INFO и выше)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)

logger = logging.getLogger(__name__)
logger.addHandler(console_handler)  # Добавляем обработчик для терминала

# Константы
REQUEST_DELAY = 15
MAX_RETRIES = 3
RETRY_DELAY = 30
WEEKLY_REPORT = True

def load_config():
    """Загружает конфигурацию из config.ini"""
    config = configparser.ConfigParser()
    config.read('config.ini')
    
    required_sections = ['Database', 'YandexDirect']
    for section in required_sections:
        if not config.has_section(section):
            raise ValueError(f"Section '{section}' not found in config.ini")
    
    return {
        'db': {
            'host': config['Database'].get('HOST', 'localhost'),
            'database': config['Database'].get('DATABASE', 'pfserver'),
            'user': config['Database'].get('USER', 'postgres'),
            'password': config['Database'].get('PASSWORD', ''),
            'port': config['Database'].get('PORT', '5432')
        },
        'yandex': {
            'token': config['YandexDirect'].get('ACCESS_TOKEN')
        }
    }

def create_table_if_not_exists(conn):
    """Создает целевую таблицу, если она не существует"""
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS rdl.yd_ad_performance_report (
        date DATE NOT NULL,
        campaign_id BIGINT NOT NULL,
        campaign_name TEXT,
        ad_id BIGINT NOT NULL,
        clicks INTEGER,
        impressions INTEGER,
        cost DECIMAL(18, 2),
        avg_click_position DECIMAL(10, 2),
        device TEXT,
        location_of_presence_id INTEGER,
        match_type TEXT,
        slot TEXT,
        CONSTRAINT pk_yd_ad_performance PRIMARY KEY (date, campaign_id, ad_id)
    )
    """
    
    try:
        with conn.cursor() as cursor:
            cursor.execute("CREATE SCHEMA IF NOT EXISTS rdl;")
            cursor.execute(create_table_sql)
            conn.commit()
            logger.info("Table checked/created successfully")
    except Exception as e:
        logger.error(f"Error creating table: {str(e)}")
        conn.rollback()
        raise

def check_existing_data(conn, date_range):
    """Проверяет наличие данных за указанный период"""
    check_sql = """
    SELECT COUNT(*) FROM rdl.yd_ad_performance_report 
    WHERE date BETWEEN %s AND %s
    """
    
    try:
        with conn.cursor() as cursor:
            cursor.execute(check_sql, date_range)
            return cursor.fetchone()[0] > 0
    except Exception as e:
        logger.error(f"Error checking existing data: {str(e)}")
        raise

def save_to_database(data, db_config, date_range):
    """Сохраняет данные в PostgreSQL"""
    conn = None
    try:
        conn = psycopg2.connect(**db_config)
        create_table_if_not_exists(conn)
        
        if check_existing_data(conn, date_range):
            logger.info(f"Data for {date_range[0]} to {date_range[1]} already exists")
            return False
        
        insert_sql = """
        INSERT INTO rdl.yd_ad_performance_report (
            date, campaign_id, campaign_name, ad_id, clicks, impressions, cost, 
            avg_click_position, device, location_of_presence_id, match_type, slot
        ) VALUES (
            %(Date)s, %(CampaignId)s, %(CampaignName)s, %(AdId)s, %(Clicks)s, 
            %(Impressions)s, %(Cost)s, %(AvgClickPosition)s, %(Device)s, 
            %(LocationOfPresenceId)s, %(MatchType)s, %(Slot)s
        ) ON CONFLICT DO NOTHING
        """
        
        with conn.cursor() as cursor:
            execute_batch(cursor, insert_sql, data)
            conn.commit()
            logger.info(f"Saved {len(data)} records for {date_range[0]} to {date_range[1]}")
            return True
            
    except Exception as e:
        logger.error(f"Database error: {str(e)}")
        raise
    finally:
        if conn:
            conn.close()

def parse_tsv_data(tsv_data):
    """Парсит TSV данные из API"""
    if not tsv_data or not tsv_data.strip():
        logger.error("Empty TSV data received")
        return None
        
    data = []
    for line in [line for line in tsv_data.split('\n') if line.strip() and not line.startswith('Date\t')]:
        try:
            parts = line.strip().split('\t')
            if len(parts) >= 12:
                cost = float(parts[6]) / 1000000 if parts[6] else None
                if cost and cost > 100000:
                    logger.warning(f"High cost value detected: {cost}")
                    
                data.append({
                    'Date': parts[0],
                    'CampaignId': int(parts[1]) if parts[1] else None,
                    'CampaignName': parts[2],
                    'AdId': int(parts[3]) if parts[3] else None,
                    'Clicks': int(parts[4]) if parts[4] else None,
                    'Impressions': int(parts[5]) if parts[5] else None,
                    'Cost': cost,
                    'AvgClickPosition': float(parts[7]) if parts[7] else None,
                    'Device': parts[8],
                    'LocationOfPresenceId': int(parts[9]) if parts[9] else None,
                    'MatchType': parts[10],
                    'Slot': parts[11]
                })
        except Exception as e:
            logger.error(f"Error parsing line: {line}. Error: {str(e)}")
    
    return data if data else None

def get_campaign_stats(token, date_from, date_to):
    """Получает статистику из API Яндекс.Директ"""
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept-Language": "en",
        "Content-Type": "application/json"
    }

    body = {
        "params": {
            "SelectionCriteria": {"DateFrom": date_from, "DateTo": date_to},
            "FieldNames": ["Date", "CampaignId", "CampaignName", "AdId", "Clicks", 
                          "Impressions", "Cost", "AvgClickPosition", "Device", 
                          "LocationOfPresenceId", "MatchType", "Slot"],
            "ReportName": f"report_{uuid.uuid4().hex[:8]}",
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
                wait_time = RETRY_DELAY * (attempt + 1)
                logger.info(f"Report is being generated, waiting {wait_time} seconds...")
                time.sleep(wait_time)
                continue
            
            logger.error(f"API error: {response.status_code} - {response.text}")
            return None
            
        except Exception as e:
            logger.error(f"Request failed (attempt {attempt + 1}): {str(e)}")
            time.sleep(RETRY_DELAY * (attempt + 1))
    
    logger.error(f"Max retries ({MAX_RETRIES}) reached")
    return None

def get_date_ranges(start_date, end_date):
    """Генерирует диапазоны дат"""
    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()
    
    if not WEEKLY_REPORT:
        return [(start_date, end_date)]
    
    dates = []
    current = start
    while current <= end:
        week_end = min(current + timedelta(days=6), end)
        dates.append((current.strftime("%Y-%m-%d"), week_end.strftime("%Y-%m-%d")))
        current = week_end + timedelta(days=1)
    return dates

if __name__ == "__main__":
    try:
        config = load_config()
        logger.info("Configuration loaded")
        
        end_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        date_ranges = get_date_ranges("2025-05-01", end_date)
        
        for range_start, range_end in date_ranges:
            logger.info(f"Processing {range_start} to {range_end}")
            time.sleep(REQUEST_DELAY)
            
            data = get_campaign_stats(config['yandex']['token'], range_start, range_end)
            if not data:
                continue
                
            parsed = parse_tsv_data(data)
            if parsed:
                save_to_database(parsed, config['db'], (range_start, range_end))
        
        logger.info("Script completed successfully")
        
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}", exc_info=True)
