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
    level=logging.INFO,
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
        "Content-Type": "application/json"
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
            "ReportName": "AD_PERFORMANCE_REPORT",
            "ReportType": "AD_PERFORMANCE_REPORT",
            "DateRangeType": "CUSTOM_DATE",
            "Format": "TSV",
            "IncludeVAT": "YES"
        }
    }

    try:
        logger.info(f"Загрузка данных за {date_from} — {date_to}...")
        response = requests.post(url, headers=headers, json=report_body, timeout=60)
        response.raise_for_status()
        
        # Дополнительная проверка ответа
        if not response.text.strip():
            logger.error("Пустой ответ от API")
            return None
            
        return response.text
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Ошибка API: {e}")
        if hasattr(e, 'response') and e.response:
            logger.error(f"Детали ошибки: {e.response.text}")
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

        # Создаем таблицу (только AdId без AdName)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS rdl.yandex_direct_ad_stats (
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
            if not line.strip() or line.startswith('"') or line.startswith('Date\t') or line.startswith('Total rows:'):
                continue
                
            values = line.split('\t')
            if len(values) != 8:  # Теперь 8 полей вместо 9
                logger.warning(f"Пропущена строка (ожидалось 8 полей): {line}")
                continue
                
            try:
                cur.execute("""
                    INSERT INTO rdl.yandex_direct_ad_stats VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (date, campaign_id, ad_id) DO NOTHING
                """, (
                    values[0].strip(),          # Date
                    int(values[1]),             # CampaignId
                    values[2].strip(),         # CampaignName
                    int(values[3]),            # AdId
                    int(values[4]),             # Clicks
                    float(values[5]) / 1000000, # Cost
                    float(values[6]),          # Ctr
                    int(values[7])              # Impressions
                ))
                processed_rows += 1
            except (ValueError, IndexError) as e:
                logger.warning(f"Ошибка обработки строки: {e}")
                continue

        conn.commit()
        logger.info(f"Успешно загружено строк: {processed_rows}")
        
    except Exception as e:
        logger.error(f"Ошибка БД: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()

def generate_date_ranges(start_date, end_date):
    """Генерирует недельные интервалы"""
    ranges = []
    current = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    
    while current <= end:
        next_date = min(current + timedelta(days=6), end)
        ranges.append((
            current.strftime("%Y-%m-%d"),
            next_date.strftime("%Y-%m-%d")
        ))
        current = next_date + timedelta(days=1)
    
    return ranges

def check_existing_data(db_config, date_from, date_to):
    """Проверяет наличие данных"""
    conn = None
    try:
        conn = psycopg2.connect(**db_config)
        cur = conn.cursor()
        cur.execute("""
            SELECT EXISTS (
                SELECT 1 FROM rdl.yandex_direct_ad_stats
                WHERE date BETWEEN %s AND %s
                LIMIT 1
            )
        """, (date_from, date_to))
        return cur.fetchone()[0]
    except Exception as e:
        logger.error(f"Ошибка проверки данных: {e}")
        return False
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    try:
        config = load_config()
        token = config['YandexDirect']['ACCESS_TOKEN']
        db_config = {
            'HOST': config['Database']['HOST'],
            'DATABASE': config['Database']['DATABASE'],
            'USER': config['Database']['USER'],
            'PASSWORD': config['Database']['PASSWORD'],
            'PORT': config['Database']['PORT']
        }

        # Период выгрузки
        start_date = "2025-06-10"
        end_date = "2025-06-24"

        for date_from, date_to in generate_date_ranges(start_date, end_date):
            logger.info(f"\nОбработка периода: {date_from} — {date_to}")
            
            if check_existing_data(db_config, date_from, date_to):
                logger.info("Данные уже существуют, пропускаем")
                continue
                
            data = get_direct_report(token, date_from, date_to)
            if data:
                save_to_postgres(data, db_config)
            else:
                logger.error("Не удалось получить данные")
                
            time.sleep(10)  # Пауза между запросами

    except Exception as e:
        logger.critical(f"Критическая ошибка: {e}")
        sys.exit(1)
    finally:
        logger.info("Работа скрипта завершена")
