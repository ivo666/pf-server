import requests
import psycopg2
from datetime import datetime, timedelta
import configparser
from pathlib import Path
import sys
import time
import logging
import json

# Настройка логирования
logging.basicConfig(
    level=logging.DEBUG,
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
        "returnMoneyInMicros": "false"
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
            "ReportName": "AD_PERFORMANCE_REPORT",  # Добавлено обязательное поле
            "ReportType": "AD_PERFORMANCE_REPORT",
            "DateRangeType": "CUSTOM_DATE",
            "Format": "TSV",
            "IncludeVAT": "YES",
            "IncludeDiscount": "NO"
        }
    }

    try:
        logger.info(f"Формирование отчета за {date_from} — {date_to}")
        logger.debug(f"Запрос: {json.dumps(report_body, indent=2)}")
        
        response = requests.post(
            url,
            headers=headers,
            json=report_body,
            timeout=60
        )
        
        logger.debug(f"Статус: {response.status_code}")
        logger.debug(f"Ответ: {response.text[:500]}...")
        
        if response.status_code == 200:
            return response.text
        elif response.status_code == 201:
            download_url = response.headers.get('Location')
            if download_url:
                logger.info("Отчет формируется, ожидаем...")
                time.sleep(30)
                return download_report(download_url, headers)
            logger.error("Не получен URL для скачивания")
            return None
        else:
            response.raise_for_status()
            return None
            
    except requests.exceptions.RequestException as e:
        logger.error(f"Ошибка запроса: {str(e)}")
        if hasattr(e, 'response') and e.response:
            logger.error(f"Детали ошибки: {e.response.text}")
        return None

def download_report(url, headers):
    try:
        response = requests.get(url, headers=headers, timeout=60)
        response.raise_for_status()
        return response.text
    except requests.exceptions.RequestException as e:
        logger.error(f"Ошибка загрузки отчета: {str(e)}")
        return None

def save_to_postgres(data, db_config):
    if not data:
        return

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
                logger.warning(f"Пропущена строка: {line}")
                continue
                
            try:
                cur.execute("""
                    INSERT INTO rdl.yandex_direct_stats VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (date, campaign_id, ad_id) DO NOTHING
                """, (
                    values[0].strip(),
                    int(values[1]),
                    values[2].strip(),
                    int(values[3]),
                    int(values[4]),
                    float(values[5]) / 1000000,
                    float(values[6]),
                    int(values[7])
                ))
                processed_rows += 1
            except (ValueError, IndexError) as e:
                logger.warning(f"Ошибка строки {line}: {str(e)}")
                continue

        conn.commit()
        logger.info(f"Загружено {processed_rows} строк")
        
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
        next_date = min(current + timedelta(days=1), end)
        ranges.append((
            current.strftime("%Y-%m-%d"),
            next_date.strftime("%Y-%m-%d")
        ))
        current = next_date + timedelta(days=1)
    
    return ranges

if __name__ == "__main__":
    try:
        config = load_config()
        token = config['YandexDirect']['ACCESS_TOKEN']
        db_config = config['Database']

        # Тестовый период - 1 день
        start_date = "2025-06-10"
        end_date = "2025-06-10"

        logger.info(f"Старт выгрузки с {start_date} по {end_date}")
        
        for date_from, date_to in generate_date_ranges(start_date, end_date):
            logger.info(f"\nОбработка: {date_from} — {date_to}")
            
            data = get_direct_report(token, date_from, date_to)
            if data:
                save_to_postgres(data, db_config)
            else:
                logger.error("Не удалось получить данные")
                
            time.sleep(5)

    except Exception as e:
        logger.critical(f"Фатальная ошибка: {str(e)}")
        sys.exit(1)
    finally:
        logger.info("Процесс завершен")
