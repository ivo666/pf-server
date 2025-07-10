import requests
import logging
import time
import uuid
import configparser
from datetime import datetime, timedelta
from io import StringIO
import psycopg2
import os
from psycopg2 import sql
from psycopg2.extras import execute_batch

# Настройка логирования
log_file = '/var/log/yandex_direct_loader.log'
os.makedirs(os.path.dirname(log_file), exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Константы
TOKEN = "y0__xCfm56NBhi4uzgg2IHdxxMB-11ibEFeXtYCgMHlML7g5RHDNA"
REQUEST_DELAY = 15  # Пауза между запросами в секундах
MAX_RETRIES = 3
RETRY_DELAY = 30  # Начальная задержка между повторными попытками в секундах
WEEKLY_REPORT = True  # Флаг для формирования недельных отчетов

def load_db_config():
    """Загружает конфигурацию БД из config.ini"""
    config = configparser.ConfigParser()
    config.read('config.ini')
    
    if not config.has_section('Database'):
        raise ValueError("Section 'Database' not found in config.ini")
    
    return {
        'host': config['Database'].get('HOST', 'localhost'),
        'database': config['Database'].get('DATABASE', 'pfserver'),
        'user': config['Database'].get('USER', 'postgres'),
        'password': config['Database'].get('PASSWORD', ''),
        'port': config['Database'].get('PORT', '5432')
    }

def create_table_if_not_exists(conn):
    """Создает целевую таблицу, если она не существует"""
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS rdl.ya_ad_performance_report (
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
        week_start_date DATE,
        CONSTRAINT pk_ya_ad_performance PRIMARY KEY (date, campaign_id, ad_id)
    );
    
    CREATE INDEX IF NOT EXISTS idx_ya_ad_performance_week ON rdl.ya_ad_performance_report (week_start_date);
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

def save_to_database(data, db_config):
    """Сохраняет данные в PostgreSQL"""
    try:
        conn = psycopg2.connect(**db_config)
        create_table_if_not_exists(conn)
        
        insert_sql = """
        INSERT INTO rdl.ya_ad_performance_report (
            date, campaign_id, campaign_name, ad_id, clicks, impressions, cost, 
            avg_click_position, device, location_of_presence_id, match_type, slot, week_start_date
        ) VALUES (
            %(Date)s, %(CampaignId)s, %(CampaignName)s, %(AdId)s, %(Clicks)s, %(Impressions)s, %(Cost)s,
            %(AvgClickPosition)s, %(Device)s, %(LocationOfPresenceId)s, %(MatchType)s, %(Slot)s, %(WeekStartDate)s
        )
        ON CONFLICT (date, campaign_id, ad_id) 
        DO UPDATE SET
            campaign_name = EXCLUDED.campaign_name,
            clicks = EXCLUDED.clicks,
            impressions = EXCLUDED.impressions,
            cost = EXCLUDED.cost,
            avg_click_position = EXCLUDED.avg_click_position,
            device = EXCLUDED.device,
            location_of_presence_id = EXCLUDED.location_of_presence_id,
            match_type = EXCLUDED.match_type,
            slot = EXCLUDED.slot,
            week_start_date = EXCLUDED.week_start_date
        """
        
        # Добавляем недельную дату для группировки
        for record in data:
            record_date = datetime.strptime(record['Date'], '%Y-%m-%d').date()
            if WEEKLY_REPORT:
                record['WeekStartDate'] = record_date - timedelta(days=record_date.weekday())
            else:
                record['WeekStartDate'] = record_date
        
        with conn.cursor() as cursor:
            execute_batch(cursor, insert_sql, data)
            conn.commit()
            logger.info(f"Successfully saved {len(data)} records to database")
            
    except Exception as e:
        logger.error(f"Database error: {str(e)}")
        raise
    finally:
        if conn:
            conn.close()

def parse_tsv_data(tsv_data):
    """Парсит TSV данные из API и возвращает список словарей"""
    try:
        if not tsv_data or not tsv_data.strip():
            logger.error("Empty TSV data received")
            return None
            
        lines = [line for line in tsv_data.split('\n') if line.strip() and not line.startswith('Date\t')]
        
        data = []
        for line in lines:
            try:
                parts = line.strip().split('\t')
                if len(parts) >= 12:
                    record = {
                        'Date': parts[0],
                        'CampaignId': int(parts[1]) if parts[1] else None,
                        'CampaignName': parts[2],
                        'AdId': int(parts[3]) if parts[3] else None,
                        'Clicks': int(parts[4]) if parts[4] else None,
                        'Impressions': int(parts[5]) if parts[5] else None,
                        'Cost': float(parts[6]) if parts[6] else None,
                        'AvgClickPosition': float(parts[7]) if parts[7] else None,
                        'Device': parts[8],
                        'LocationOfPresenceId': int(parts[9]) if parts[9] else None,
                        'MatchType': parts[10],
                        'Slot': parts[11]
                    }
                    data.append(record)
            except Exception as line_error:
                logger.error(f"Error parsing line: {line}. Error: {str(line_error)}")
                continue
        
        if not data:
            logger.error("No valid data found in TSV")
            return None
            
        return data
    except Exception as e:
        logger.error(f"Failed to parse TSV data: {str(e)}")
        return None

def get_campaign_stats(token, date_from, date_to, max_retries=3):
    url = "https://api.direct.yandex.com/json/v5/reports"
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept-Language": "en",
        "Content-Type": "application/json"
    }

    report_name = f"campaign_stats_{uuid.uuid4().hex[:8]}"

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
                "Clicks",
                "Impressions",
                "Cost",
                "AvgClickPosition",
                "Device",
                "LocationOfPresenceId",
                "MatchType",
                "Slot"
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
        logger.info(f"Requesting Campaign stats from {date_from} to {date_to}")
        response = requests.post(
            url,
            headers=headers,
            json=body,
            timeout=120
        )
        
        if response.status_code == 200:
            return response.text
        elif response.status_code == 201:
            logger.info("Report is being generated, waiting...")
            retry_count = 0
            while retry_count < max_retries:
                wait_time = RETRY_DELAY * (retry_count + 1)
                logger.info(f"Waiting {wait_time} seconds before retry...")
                time.sleep(wait_time)
                
                if 'Retry-After' in response.headers:
                    wait_time = int(response.headers['Retry-After'])
                    logger.info(f"Server requested to wait {wait_time} seconds")
                    time.sleep(wait_time)
                
                download_url = response.headers.get('Location')
                if download_url:
                    logger.info(f"Trying to download report (attempt {retry_count + 1})")
                    download_response = requests.get(download_url, headers=headers, timeout=120)
                    if download_response.status_code == 200:
                        return download_response.text
                    else:
                        logger.warning(f"Download failed: {download_response.status_code}")
                else:
                    logger.warning("Download URL not found, trying to request report again")
                    response = requests.post(
                        url,
                        headers=headers,
                        json=body,
                        timeout=120
                    )
                    if response.status_code == 200:
                        return response.text
                
                retry_count += 1
            logger.error(f"Max retries ({max_retries}) reached. Report is not ready.")
            return None
        else:
            logger.error(f"API error: {response.status_code} - {response.text}")
            return None
    except Exception as e:
        logger.error(f"Request failed: {str(e)}")
        return None

def get_weekly_date_ranges(start_date, end_date):
    """Генерирует список недельных диапазонов дат"""
    date_ranges = []
    current_date = datetime.strptime(start_date, "%Y-%m-%d").date()
    end_date = datetime.strptime(end_date, "%Y-%m-%d").date()
    
    while current_date <= end_date:
        week_start = current_date
        week_end = min(week_start + timedelta(days=6), end_date)
        date_ranges.append((week_start.strftime("%Y-%m-%d"), week_end.strftime("%Y-%m-%d")))
        current_date = week_end + timedelta(days=1)
    
    return date_ranges

if __name__ == "__main__":
    try:
        # Загружаем конфигурацию БД
        db_config = load_db_config()
        logger.info("Database configuration loaded")
        
        # Устанавливаем даты (с 01.05.2025 по вчерашний день)
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        start_date = "2025-05-01"
        end_date = yesterday
        
        logger.info(f"Processing data from {start_date} to {end_date}")
        
        # Получаем диапазоны дат (по неделям или один диапазон)
        if WEEKLY_REPORT:
            date_ranges = get_weekly_date_ranges(start_date, end_date)
        else:
            date_ranges = [(start_date, end_date)]
        
        # Обрабатываем каждый диапазон дат
        for range_start, range_end in date_ranges:
            logger.info(f"Processing date range: {range_start} - {range_end}")
            
            # Добавляем паузу перед запросом
            logger.info(f"Waiting {REQUEST_DELAY} seconds before request...")
            time.sleep(REQUEST_DELAY)
            
            # Получаем данные
            data = get_campaign_stats(TOKEN, range_start, range_end)
            
            if data:
                parsed_data = parse_tsv_data(data)
                if parsed_data:
                    # Сохраняем в БД
                    save_to_database(parsed_data, db_config)
                    logger.info(f"Successfully processed range {range_start} - {range_end}")
                else:
                    logger.error(f"Failed to parse data for range {range_start} - {range_end}")
            else:
                logger.error(f"Failed to get data for range {range_start} - {range_end}")
        
        logger.info("Script finished successfully")
    except Exception as e:
        logger.error(f"Script failed: {str(e)}", exc_info=True)
        raise
