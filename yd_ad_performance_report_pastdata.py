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
WEEKLY_REPORT = True

def log_console(message):
    console_logger.info(message)

def load_config():
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
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS rdl.yd_ad_performance_report (
        date DATE NOT NULL,
        ad_id VARCHAR(20) NOT NULL,
        ad_format TEXT,
        impressions INTEGER DEFAULT 0,
        clicks INTEGER DEFAULT 0,
        ctr DECIMAL(5,2),
        cost DECIMAL(18,2) DEFAULT 0.0,
        avg_cpc DECIMAL(18,2),
        avg_pageviews DECIMAL(5,2),
        avg_traffic_volume DECIMAL(5,2),
        conversions INTEGER DEFAULT 0,
        conversion_rate DECIMAL(5,2),
        cost_per_conversion DECIMAL(18,2),
        roi DECIMAL(5,2),
        revenue DECIMAL(18,2) DEFAULT 0.0,
        profit DECIMAL(18,2) DEFAULT 0.0,
        PRIMARY KEY (date, ad_id)
    )
    """
    
    try:
        with conn.cursor() as cursor:
            cursor.execute("CREATE SCHEMA IF NOT EXISTS rdl;")
            cursor.execute(create_table_sql)
            conn.commit()
            log_console("Таблица проверена/создана")
    except Exception as e:
        logger.error(f"Error creating table: {str(e)}")
        log_console("Ошибка при создании таблицы")
        conn.rollback()
        raise

def check_existing_data(conn, date_range):
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
    conn = None
    try:
        conn = psycopg2.connect(**db_config)
        create_table_if_not_exists(conn)
        
        if check_existing_data(conn, date_range):
            log_console(f"Данные за период {date_range[0]} - {date_range[1]} уже существуют")
            return False
        
        insert_sql = """
        INSERT INTO rdl.yd_ad_performance_report (
            date, ad_id, ad_format, impressions, clicks, ctr, cost,
            avg_cpc, avg_pageviews, avg_traffic_volume, conversions,
            conversion_rate, cost_per_conversion, roi, revenue, profit
        ) VALUES (
            %(Date)s, %(AdId)s, %(AdFormat)s, %(Impressions)s, %(Clicks)s,
            %(Ctr)s, %(Cost)s, %(AvgCpc)s, %(AvgPageviews)s,
            %(AvgTrafficVolume)s, %(Conversions)s, %(ConversionRate)s,
            %(CostPerConversion)s, %(Roi)s, %(Revenue)s, %(Profit)s
        ) ON CONFLICT DO NOTHING
        """
        
        with conn.cursor() as cursor:
            execute_batch(cursor, insert_sql, data)
            conn.commit()
            log_console(f"Сохранено {len(data)} записей за {date_range[0]} - {date_range[1]}")
            return True
            
    except Exception as e:
        logger.error(f"Database error: {str(e)}")
        log_console("Ошибка при сохранении в базу данных")
        raise
    finally:
        if conn:
            conn.close()

def parse_tsv_data(tsv_data):
    if not tsv_data or not tsv_data.strip():
        logger.error("Получены пустые данные")
        return None
        
    lines = [line for line in tsv_data.split('\n') 
             if line.strip() and not line.startswith('Дата\t')]
    
    data = []
    for line in lines:
        try:
            parts = line.strip().split('\t')
            if len(parts) >= 16:
                record = {
                    'Date': parts[0],
                    'AdId': f"M-{parts[1]}" if parts[1] else None,
                    'AdFormat': parts[2],
                    'Impressions': int(parts[3]) if parts[3] else 0,
                    'Clicks': int(parts[4]) if parts[4] else 0,
                    'Ctr': float(parts[5].replace(',', '.')) if parts[5] not in ('--', '') else None,
                    'Cost': float(parts[6].replace(',', '.')) if parts[6] not in ('--', '') else 0.0,
                    'AvgCpc': float(parts[7].replace(',', '.')) if parts[7] not in ('--', '') else None,
                    'AvgPageviews': float(parts[8].replace(',', '.')) if parts[8] not in ('--', '') else None,
                    'AvgTrafficVolume': float(parts[9].replace(',', '.')) if parts[9] not in ('--', '') else None,
                    'Conversions': int(parts[10]) if parts[10] else 0,
                    'ConversionRate': float(parts[11].replace(',', '.')) if parts[11] not in ('--', '') else None,
                    'CostPerConversion': float(parts[12].replace(',', '.')) if parts[12] not in ('--', '') else None,
                    'Roi': float(parts[13].replace(',', '.')) if parts[13] not in ('--', '') else None,
                    'Revenue': float(parts[14].replace(',', '.')) if parts[14] not in ('--', '') else 0.0,
                    'Profit': float(parts[15].replace(',', '.')) if parts[15] not in ('--', '') else 0.0
                }
                data.append(record)
        except Exception as e:
            logger.warning(f"Ошибка обработки строки: {line[:100]}... Ошибка: {str(e)}")
            continue
    
    if data:
        logger.debug(f"Первые 3 строки данных: {data[:3]}")
    return data if data else None

def get_campaign_stats(token, date_from, date_to):
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept-Language": "ru",
        "Content-Type": "application/json"
    }

    body = {
        "params": {
            "SelectionCriteria": {"DateFrom": date_from, "DateTo": date_to},
            "FieldNames": [
                "Date",
                "AdId",
                "AdFormat",
                "Impressions",
                "Clicks",
                "Ctr",
                "Cost",
                "AvgCpc",
                "AvgPageviews",
                "AvgTrafficVolume",
                "Conversions",
                "ConversionRate",
                "CostPerConversion",
                "Roi",
                "Revenue",
                "Profit"
            ],
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
            log_console(f"Запрос данных за {date_from} - {date_to} (попытка {attempt + 1})")
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
                log_console(f"Отчет формируется, ожидание {wait_time} секунд...")
                time.sleep(wait_time)
                continue
            
            logger.error(f"API error: {response.status_code} - {response.text}")
            log_console("Ошибка API Яндекс.Директ")
            return None
            
        except Exception as e:
            logger.error(f"Request failed (attempt {attempt + 1}): {str(e)}")
            time.sleep(RETRY_DELAY * (attempt + 1))
    
    log_console("Превышено максимальное количество попыток")
    return None

def get_date_ranges(start_date, end_date):
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
        log_console("Конфигурация загружена")
        
        end_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        date_ranges = get_date_ranges("2025-05-01", end_date)
        
        for range_start, range_end in date_ranges:
            log_console(f"Обработка периода {range_start} - {range_end}")
            time.sleep(REQUEST_DELAY)
            
            data = get_campaign_stats(config['yandex']['token'], range_start, range_end)
            if not data:
                continue
                
            parsed = parse_tsv_data(data)
            if parsed:
                save_to_database(parsed, config['db'], (range_start, range_end))
        
        log_console("Скрипт успешно завершен")
        
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}", exc_info=True)
        log_console("Критическая ошибка выполнения скрипта")
