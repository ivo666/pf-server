import requests
import psycopg2
from datetime import datetime, timedelta
import configparser
from pathlib import Path
import sys
import time
import logging

# Настройка логирования
logging.basicConfig(
    level=logging.DEBUG,  # Временный DEBUG для диагностики
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/var/log/yandex_direct_stats.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def load_config():
    config = configparser.ConfigParser()
    config_path = Path(__file__).parent / 'config.ini'
    config.read(config_path)
    return config

def get_direct_report(token, date_from, date_to):
    url = "https://api.direct.yandex.com/json/v5/reports"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept-Language": "ru",
        "Content-Type": "application/json",
        "processingMode": "auto",
        "skipReportHeader": "true",
        "skipColumnHeader": "true",
        "skipReportSummary": "true"
    }

    report_body = {
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
                "Cost",
                "Ctr",
                "Impressions"
            ],
            "ReportName": "CustomReport",
            "ReportType": "AD_PERFORMANCE_REPORT",
            "DateRangeType": "CUSTOM_DATE",
            "Format": "TSV",
            "IncludeVAT": "YES",
            "IncludeDiscount": "NO"
        }
    }

    try:
        logger.info(f"Запрос данных за {date_from} — {date_to}")
        logger.debug(f"Тело запроса: {report_body}")  # Логируем тело запроса
        
        response = requests.post(
            url,
            headers=headers,
            json=report_body,
            timeout=60
        )
        
        logger.debug(f"Статус ответа: {response.status_code}")
        logger.debug(f"Тело ответа: {response.text[:500]}")  # Логируем часть ответа
        
        response.raise_for_status()
        
        if not response.text.strip():
            logger.error("Пустой ответ от API")
            return None
            
        return response.text
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Ошибка запроса: {str(e)}")
        if hasattr(e, 'response') and e.response:
            logger.error(f"Код ошибки: {e.response.status_code}")
            logger.error(f"Тело ошибки: {e.response.text}")
        return None

def save_to_postgres(data, db_config):
    conn = None
    try:
        conn = psycopg2.connect(
            host=db_config['HOST'],
            database=db_config['DATABASE'],
            user=db_config['USER'],
            password=db_config['PASSWORD'],
            port=db_config['PORT']
        )
        cur = conn.cursor()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS rdl.yandex_direct_stats (
                date DATE,
                campaign_id BIGINT,
                campaign_name TEXT,
                ad_id BIGINT,
                clicks INTEGER,
                cost DECIMAL(15, 2),
                ctr DECIMAL(5, 2),
                impressions INTEGER,
                PRIMARY KEY (date, campaign_id, ad_id)
            )
        """)

        lines = data.strip().split('\n')
        processed_rows = 0
        
        for line in lines:
            if not line.strip() or line.startswith(('"', 'Date\t', 'Total rows:')):
                continue
                
            values = line.split('\t')
            if len(values) != 8:
                logger.warning(f"Неверное количество полей ({len(values)}): {line}")
                continue
                
            try:
                cur.execute("""
                    INSERT INTO rdl.yandex_direct_stats VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (date, campaign_id, ad_id) DO NOTHING
                """, (
                    values[0].strip(),          # Date
                    int(values[1]),             # CampaignId
                    values[2].strip(),          # CampaignName
                    int(values[3]),             # AdId
                    int(values[4]),             # Clicks
                    float(values[5]) / 1000000, # Cost
                    float(values[6]),           # Ctr
                    int(values[7])              # Impressions
                ))
                processed_rows += 1
            except (ValueError, IndexError) as e:
                logger.warning(f"Ошибка обработки строки: {line} | {str(e)}")
                continue

        conn.commit()
        logger.info(f"Загружено строк: {processed_rows}")
        
    except Exception as e:
        logger.error(f"Ошибка БД: {str(e)}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()

def generate_date_ranges(start_date, end_date):
    current = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    ranges = []
    
    while current <= end:
        next_date = min(current + timedelta(days=6), end)
        ranges.append((
            current.strftime("%Y-%m-%d"),
            next_date.strftime("%Y-%m-%d")
        ))
        current = next_date + timedelta(days=1)
    
    return ranges

def check_existing_data(db_config, date_from, date_to):
    conn = None
    try:
        conn = psycopg2.connect(
            host=db_config['HOST'],
            database=db_config['DATABASE'],
            user=db_config['USER'],
            password=db_config['PASSWORD'],
            port=db_config['PORT']
        )
        cur = conn.cursor()
        cur.execute("""
            SELECT EXISTS (
                SELECT 1 FROM rdl.yandex_direct_stats
                WHERE date BETWEEN %s AND %s
                LIMIT 1
            )
        """, (date_from, date_to))
        return cur.fetchone()[0]
    except Exception as e:
        logger.error(f"Ошибка проверки данных: {str(e)}")
        return False
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    try:
        config = load_config()
        token = config['YandexDirect']['ACCESS_TOKEN']
        db_config = config['Database']

        # Тестовый период (уменьшен для диагностики)
        start_date = "2025-06-10"
        end_date = "2025-06-11"  # Всего 1 день для теста

        logger.info(f"Начало выгрузки с {start_date} по {end_date}")
        
        for date_from, date_to in generate_date_ranges(start_date, end_date):
            logger.info(f"\nПериод: {date_from} — {date_to}")
            
            if check_existing_data(db_config, date_from, date_to):
                logger.info("Данные уже есть, пропускаем")
                continue
                
            data = get_direct_report(token, date_from, date_to)
            if data:
                save_to_postgres(data, db_config)
            else:
                logger.error("Не удалось получить данные")
                
            time.sleep(5)  # Уменьшенная пауза для теста

    except Exception as e:
        logger.critical(f"Критическая ошибка: {str(e)}")
        sys.exit(1)
    finally:
        logger.info("Выгрузка завершена")
