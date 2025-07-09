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
        "method": "get",
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
                "AdName",
                "Clicks",
                "Cost",
                "Ctr",
                "Impressions"
            ],
            "ReportName": "AdPerformanceReport",
            "ReportType": "AD_PERFORMANCE_REPORT",
            "DateRangeType": "CUSTOM_DATE",
            "Format": "TSV",
            "IncludeVAT": "YES"
        }
    }

    try:
        logger.info(f"🔄 Загрузка данных по объявлениям за {date_from} — {date_to}...")
        response = requests.post(url, headers=headers, json=report_body, timeout=60)
        response.raise_for_status()
        return response.text
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Ошибка API: {e}")
        if hasattr(e, 'response') and e.response:
            logger.error(f"Тело ответа: {e.response.text}")
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

        # Создаем таблицу для статистики по объявлениям
        cur.execute("""
            CREATE TABLE IF NOT EXISTS rdl.yandex_direct_ad_stats (
                date DATE,
                campaign_id BIGINT,
                campaign_name TEXT,
                ad_id BIGINT,
                ad_name TEXT,
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
            if len(values) != 9:
                logger.warning(f"⚠ Пропущена строка (ожидалось 9 полей, получено {len(values)}): {line}")
                continue
                
            try:
                cur.execute("""
                    INSERT INTO rdl.yandex_direct_ad_stats VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (date, campaign_id, ad_id) DO NOTHING
                """, (
                    values[0].strip(),          # Date
                    int(values[1]),             # CampaignId
                    values[2].strip(),          # CampaignName
                    int(values[3]),             # AdId
                    values[4].strip(),          # AdName
                    int(values[5]),             # Clicks
                    float(values[6]) / 1000000, # Cost (микроединицы → рубли)
                    float(values[7]),           # Ctr
                    int(values[8])              # Impressions
                ))
                processed_rows += 1
            except (ValueError, IndexError) as e:
                logger.warning(f"⚠ Ошибка в строке: {line} | Ошибка: {e}")
                continue

        conn.commit()
        logger.info(f"✅ Успешно загружено {processed_rows} строк")
        
    except Exception as e:
        logger.error(f"❌ Ошибка БД: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()

def generate_date_ranges(start_date, end_date):
    """Разбивает период на недельные интервалы (7 дней)"""
    current = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    ranges = []
    
    while current < end:
        next_date = min(current + timedelta(days=6), end)
        ranges.append((
            current.strftime("%Y-%m-%d"),
            next_date.strftime("%Y-%m-%d")
        ))
        current = next_date + timedelta(days=1)
    
    return ranges

def check_existing_data(db_config, date_from, date_to):
    """Проверяет, есть ли данные за указанный период"""
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
                SELECT 1 FROM rdl.yandex_direct_ad_stats 
                WHERE date BETWEEN %s AND %s
                LIMIT 1
            )
        """, (date_from, date_to))
        return cur.fetchone()[0]
    except Exception as e:
        logger.error(f"⚠ Ошибка при проверке данных: {e}")
        return False
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    try:
        config = load_config()
        token = config['YandexDirect']['ACCESS_TOKEN']
        db_config = config['Database']
        
        # Период для выгрузки
        start_date = "2025-06-10"
        end_date = "2025-06-24"

        for date_from, date_to in generate_date_ranges(start_date, end_date):
            logger.info(f"\n📅 Обработка периода {date_from} — {date_to}")
            
            if check_existing_data(db_config, date_from, date_to):
                logger.info("⏩ Данные уже существуют, пропускаем")
                continue
            
            data = get_direct_report(token, date_from, date_to)
            if data:
                save_to_postgres(data, db_config)
            
            time.sleep(10)  # Пауза между запросами

    except Exception as e:
        logger.critical(f"🔥 Критическая ошибка: {e}")
        sys.exit(1)
    finally:
        logger.info("✅ Скрипт завершил работу")
