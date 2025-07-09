import requests
import logging
import time
import hashlib
import configparser
import psycopg2
from datetime import datetime, timedelta
from io import StringIO

# Константы
REQUEST_DELAY = 15  # Пауза между запросами в секундах
MAX_RETRIES = 3
RETRY_DELAY = 30  # Начальная задержка между повторными попытками в секундах

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

def get_db_connection(config_file='config.ini'):
    """Создает соединение с PostgreSQL на основе конфигурационного файла"""
    config = configparser.ConfigParser()
    config.read(config_file)
    
    db_config = config['Database']
    
    try:
        conn = psycopg2.connect(
            host=db_config['HOST'],
            database=db_config['DATABASE'],
            user=db_config['USER'],
            password=db_config['PASSWORD'],
            port=db_config['PORT']
        )
        logger.info("Successfully connected to PostgreSQL database")
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to database: {str(e)}")
        return None

def check_existing_dates(conn, start_date, end_date):
    """Проверяет, за какие даты в указанном диапазоне уже есть данные"""
    query = """
    SELECT DISTINCT date 
    FROM rdl.yandex_direct_stats 
    WHERE date BETWEEN %s AND %s
    """
    
    try:
        with conn.cursor() as cursor:
            cursor.execute(query, (start_date, end_date))
            existing_dates = {row[0] for row in cursor.fetchall()}
            logger.info(f"Found {len(existing_dates)} existing dates in the range")
            return existing_dates
    except Exception as e:
        logger.error(f"Failed to check existing dates: {str(e)}")
        return set()

def generate_week_ranges(start_date, end_date):
    """Генерирует список недельных диапазонов"""
    current_date = start_date
    week_ranges = []
    
    while current_date <= end_date:
        week_end = current_date + timedelta(days=6)
        if week_end > end_date:
            week_end = end_date
        week_ranges.append((current_date, week_end))
        current_date = week_end + timedelta(days=1)
    
    return week_ranges

def parse_tsv_data(tsv_data):
    """Парсит TSV данные из API и возвращает список кортежей для вставки"""
    try:
        if not tsv_data or not tsv_data.strip():
            logger.error("Empty TSV data received")
            return None
            
        lines = [line for line in tsv_data.split('\n') if line.strip() and not line.startswith('Date\t')]
        
        data = []
        for line in lines:
            try:
                parts = line.strip().split('\t')
                if len(parts) >= 7:
                    date = parts[0]
                    campaign_id = int(parts[1])
                    campaign_name = parts[2]
                    ad_id = int(parts[3])
                    clicks = int(parts[4]) if parts[4] else 0
                    impressions = int(parts[5]) if parts[5] else 0
                    cost = float(parts[6]) / 1000000 if parts[6] else 0.0
                    
                    data.append((date, campaign_id, campaign_name, ad_id, clicks, impressions, cost))
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

def save_to_database(conn, data):
    """Сохраняет данные в таблицу rdl.yandex_direct_stats"""
    if not data:
        logger.warning("No data to save")
        return False
    
    insert_query = """
    INSERT INTO rdl.yandex_direct_stats (date, campaign_id, campaign_name, ad_id, clicks, impressions, cost)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (date, campaign_id, ad_id) DO UPDATE SET
        campaign_name = EXCLUDED.campaign_name,
        clicks = EXCLUDED.clicks,
        impressions = EXCLUDED.impressions,
        cost = EXCLUDED.cost;
    """
    
    try:
        with conn.cursor() as cursor:
            cursor.executemany(insert_query, data)
            conn.commit()
            logger.info(f"Successfully inserted/updated {len(data)} records")
            return True
    except Exception as e:
        logger.error(f"Failed to insert data: {str(e)}")
        conn.rollback()
        return False

def get_campaign_stats(token, date_from, date_to, max_retries=3):
    url = "https://api.direct.yandex.com/json/v5/reports"
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept-Language": "en",
        "Content-Type": "application/json"
    }

    report_hash = hashlib.md5(f"{date_from}_{date_to}".encode()).hexdigest()[:8]
    report_name = f"campaign_stats_{report_hash}"

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
                "Cost"
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
                wait_time = 30 * (retry_count + 1)
                logger.info(f"Waiting {wait_time} seconds before retry...")
                time.sleep(wait_time)
                
                # Добавляем проверку заголовка Retry-After
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

def process_week(conn, token, week_start, week_end, existing_dates):
    """Обрабатывает данные за неделю, пропуская уже существующие даты"""
    # Формируем список дат в неделе, которых еще нет в БД
    dates_to_process = []
    current_date = week_start
    while current_date <= week_end:
        if current_date not in existing_dates:
            dates_to_process.append(current_date)
        current_date += timedelta(days=1)
    
    if not dates_to_process:
        logger.info(f"No new dates to process for week {week_start} - {week_end}")
        return True
    
    # Получаем данные за всю неделю
    date_from_str = week_start.strftime("%Y-%m-%d")
    date_to_str = week_end.strftime("%Y-%m-%d")
    
    logger.info(f"Processing dates: {', '.join(d.strftime('%Y-%m-%d') for d in dates_to_process)}")
    
    data = get_campaign_stats(token, date_from_str, date_to_str)
    
    if data:
        parsed_data = parse_tsv_data(data)
        if parsed_data:
            # Фильтруем данные, оставляя только те, которых нет в БД
            filtered_data = [
                row for row in parsed_data 
                if datetime.strptime(row[0], "%Y-%m-%d").date() not in existing_dates
            ]
            
            if filtered_data:
                return save_to_database(conn, filtered_data)
            else:
                logger.info("All data for this week already exists in database")
                return True
        else:
            logger.error("Failed to parse data")
            return False
    else:
        logger.error("Failed to get data")
        return False

if __name__ == "__main__":
    TOKEN = "y0__xCfm56NBhi4uzgg2IHdxxMB-11ibEFeXtYCgMHlML7g5RHDNA"  
    
    # Устанавливаем даты начала (01.01.2025) и конца (вчерашний день)
    start_date = datetime(2025, 1, 1).date()
    end_date = datetime.now().date() - timedelta(days=1)
    
    logger.info(f"Starting report from {start_date} to {end_date}")
    
    # Подключаемся к БД
    conn = get_db_connection()
    if not conn:
        logger.error("Cannot proceed without database connection")
        exit(1)
    
    try:
        # Проверяем, за какие даты уже есть данные
        existing_dates = check_existing_dates(conn, start_date, end_date)
        
        # Генерируем недельные диапазоны
        week_ranges = generate_week_ranges(start_date, end_date)
        
        # Обрабатываем каждую неделю
        for i, (week_start, week_end) in enumerate(week_ranges):
            logger.info(f"\n{'='*50}")
            logger.info(f"Processing week {week_start} - {week_end}")
            
            # Добавляем паузу перед каждым запросом, кроме первого
            if i > 0:
                logger.info(f"Waiting {REQUEST_DELAY} seconds before next request...")
                time.sleep(REQUEST_DELAY)
            
            if not process_week(conn, TOKEN, week_start, week_end, existing_dates):
                logger.error(f"Failed to process week {week_start} - {week_end}")
                # Продолжаем обработку следующих недель даже при ошибке
                continue
    except KeyboardInterrupt:
        logger.info("Script interrupted by user")
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
    finally:
        conn.close()
        logger.info("Database connection closed")
    
    logger.info("Script finished")
