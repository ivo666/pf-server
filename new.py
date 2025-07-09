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

def create_table_if_not_exists(conn):
    """Создает таблицу в схеме rdl для хранения статистики кампаний, если она не существует"""
    create_table_query = """
    CREATE SCHEMA IF NOT EXISTS rdl;
    
    CREATE TABLE IF NOT EXISTS rdl.yandex_direct_stats (
        date DATE NOT NULL,
        campaign_id BIGINT NOT NULL,
        campaign_name TEXT,
        ad_id BIGINT NOT NULL,
        clicks INTEGER,
        impressions INTEGER,
        cost DECIMAL(15, 2),
        PRIMARY KEY (date, campaign_id, ad_id)
    );
    
    COMMENT ON TABLE rdl.yandex_direct_stats IS 'Статистика кампаний Яндекс.Директ';
    COMMENT ON COLUMN rdl.yandex_direct_stats.date IS 'Дата сбора статистики';
    COMMENT ON COLUMN rdl.yandex_direct_stats.campaign_id IS 'ID кампании';
    COMMENT ON COLUMN rdl.yandex_direct_stats.campaign_name IS 'Название кампании';
    COMMENT ON COLUMN rdl.yandex_direct_stats.ad_id IS 'ID объявления';
    COMMENT ON COLUMN rdl.yandex_direct_stats.clicks IS 'Количество кликов';
    COMMENT ON COLUMN rdl.yandex_direct_stats.impressions IS 'Количество показов';
    COMMENT ON COLUMN rdl.yandex_direct_stats.cost IS 'Стоимость (в валюте кампании)';
    """
    
    try:
        with conn.cursor() as cursor:
            cursor.execute(create_table_query)
            conn.commit()
            logger.info("Table created or already exists in rdl schema")
    except Exception as e:
        logger.error(f"Failed to create table: {str(e)}")
        conn.rollback()

def parse_tsv_data(tsv_data):
    """Парсит TSV данные из API и возвращает список кортежей для вставки"""
    try:
        # Пропускаем заголовок и пустые строки
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
                cost = float(parts[6]) if parts[6] else 0.0
                
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

def get_campaign_stats(token, date, max_retries=3):
    url = "https://api.direct.yandex.com/json/v5/reports"
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept-Language": "en",
        "Content-Type": "application/json"
    }

    # Генерируем уникальное имя отчета
    report_hash = hashlib.md5(date.encode()).hexdigest()[:8]
    report_name = f"campaign_stats_{report_hash}"

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
        logger.info(f"Requesting Campaign stats for {date}")
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

if __name__ == "__main__":
    TOKEN = "y0__xCfm56NBhi4uzgg2IHdxxMB-11ibEFeXtYCgMHlML7g5RHDNA"  
    report_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    
    logger.info(f"Starting report for {report_date}")
    data = get_campaign_stats(TOKEN, report_date)
    
    if data:
        # Подключаемся к БД
        conn = get_db_connection()
        if conn:
            try:
                # Создаем таблицу в схеме rdl (если не существует)
                create_table_if_not_exists(conn)
                
                # Парсим данные
                parsed_data = parse_tsv_data(data)
                
                if parsed_data:
                    # Сохраняем в БД
                    save_to_database(conn, parsed_data)
                else:
                    logger.error("Failed to parse data")
            finally:
                conn.close()
    else:
        logger.error("Failed to get data")
    
    logger.info("Script finished")
