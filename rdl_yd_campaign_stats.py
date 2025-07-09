import requests
import logging
import time
import hashlib
import configparser
import psycopg2
from datetime import datetime, timedelta
from io import StringIO

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
    """Проверяет, какие даты уже есть в базе данных"""
    query = """
    SELECT DISTINCT date 
    FROM rdl.yandex_direct_stats
    WHERE date BETWEEN %s AND %s
    """
    
    try:
        with conn.cursor() as cursor:
            cursor.execute(query, (start_date, end_date))
            existing_dates = {row[0] for row in cursor.fetchall()}
            return existing_dates
    except Exception as e:
        logger.error(f"Failed to check existing dates: {str(e)}")
        return set()

def generate_weekly_periods(start_date_str, end_date_str=None):
    """Генерирует список недельных периодов с заданной даты"""
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d").date() if end_date_str else datetime.now().date()
    
    current_date = start_date
    periods = []
    
    while current_date <= end_date:
        period_end = min(current_date + timedelta(days=6), end_date)
        periods.append((current_date, period_end))
        current_date = period_end + timedelta(days=1)
    
    return periods

def get_campaign_stats(token, date_from, date_to, max_retries=3):
    """Получает статистику кампаний за указанный период"""
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
            timeout=60
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
                
                download_url = response.headers.get('Location')
                if download_url:
                    logger.info(f"Trying to download report (attempt {retry_count + 1})")
                    download_response = requests.get(download_url, headers=headers, timeout=60)
                    if download_response.status_code == 200:
                        return download_response.text
                    else:
                        logger.warning(f"Download failed: {download_response.status_code}")
                else:
                    logger.warning("Download URL not found.")
                    break
                retry_count += 1
            logger.error(f"Max retries ({max_retries}) reached. Report is not ready.")
            return None
        else:
            logger.error(f"API error: {response.status_code} - {response.text}")
            return None
    except Exception as e:
        logger.error(f"Request failed: {str(e)}")
        return None

def parse_tsv_data(tsv_data):
    """Парсит TSV данные из API и возвращает список кортежей для вставки"""
    try:
        lines = [line for line in tsv_data.split('\n') if line.strip() and not line.startswith('Date\t')]
        
        data = []
        for line in lines:
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
    ON CONFLICT (date, campaign_id, ad_id) DO NOTHING;
    """
    
    try:
        with conn.cursor() as cursor:
            cursor.executemany(insert_query, data)
            conn.commit()
            logger.info(f"Successfully inserted {cursor.rowcount} new records")
            return True
    except Exception as e:
        logger.error(f"Failed to insert data: {str(e)}")
        conn.rollback()
        return False

def main():
    TOKEN = "y0__xCfm56NBhi4uzgg2IHdxxMB-11ibEFeXtYCgMHlML7g5RHDNA"
    START_DATE = "2025-01-01"
    
    conn = get_db_connection()
    if not conn:
        return
    
    try:
        # Генерируем недельные периоды с 01.01.2025 по текущую дату
        weekly_periods = generate_weekly_periods(START_DATE)
        
        for period_start, period_end in weekly_periods:
            period_start_str = period_start.strftime("%Y-%m-%d")
            period_end_str = period_end.strftime("%Y-%m-%d")
            
            logger.info(f"Processing period: {period_start_str} - {period_end_str}")
            
            # Проверяем, какие даты из этого периода уже есть в БД
            existing_dates = check_existing_dates(conn, period_start, period_end)
            
            # Если все даты периода уже есть в БД, пропускаем
            if existing_dates and all(
                period_start + timedelta(days=i) in existing_dates
                for i in range((period_end - period_start).days + 1)
            ):
                logger.info(f"All data for period {period_start_str} - {period_end_str} already exists, skipping")
                continue
            
            # Загружаем данные за период
            data = get_campaign_stats(TOKEN, period_start_str, period_end_str)
            
            if data:
                parsed_data = parse_tsv_data(data)
                
                if parsed_data:
                    save_to_database(conn, parsed_data)
                else:
                    logger.error(f"Failed to parse data for {period_start_str} - {period_end_str}")
            else:
                logger.error(f"Failed to get data for {period_start_str} - {period_end_str}")
            
            # Небольшая пауза между запросами
            time.sleep(5)
            
    finally:
        conn.close()
    
    logger.info("Script finished")

if __name__ == "__main__":
    main()
