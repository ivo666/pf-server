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
        "params": {  # Убрали "method": "get" - в API v5 это не используется
            "SelectionCriteria": {
                "DateFrom": date_from,
                "DateTo": date_to
            },
            "FieldNames": [
                "Date",
                "CampaignId", 
                "CampaignName",
                "AdId",  # Перенесли AdId выше, чтобы соответствовать порядку полей в ответе
                "Clicks",
                "Cost",
                "Ctr",
                "Impressions"
            ],
            "ReportName": "AD_PERFORMANCE_REPORT",  # Исправлено название
            "ReportType": "AD_PERFORMANCE_REPORT",
            "DateRangeType": "CUSTOM_DATE",
            "Format": "TSV",
            "IncludeVAT": "YES",
            "IncludeDiscount": "NO"  # Добавили обязательный параметр
        }
    }

    try:
        logger.info(f"🔄 Загрузка данных за период {date_from} — {date_to}...")
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

        # Обновили структуру таблицы
        cur.execute("""
            CREATE TABLE IF NOT EXISTS rdl.yandex_direct_stats (
                date DATE,
                campaign_id BIGINT,
                campaign_name TEXT,
                ad_id BIGINT,  # Перенесли ad_id выше
                clicks INTEGER,
                cost DECIMAL(15, 2),
                ctr DECIMAL(5, 2),
                impressions INTEGER,
                PRIMARY KEY (date, campaign_id, ad_id)  # Добавили ad_id в первичный ключ
            )
        """)

        lines = data.strip().split('\n')
        processed_rows = 0
        
        for line in lines:
            if not line.strip() or line.startswith('"') or line.startswith('Date\t') or line.startswith('Total rows:'):
                continue
                
            values = line.split('\t')
            if len(values) != 8:  # Теперь ожидаем 8 полей
                logger.warning(f"⚠ Пропущена строка (неверное кол-во полей): {line}")
                continue
                
            try:
                cur.execute("""
                    INSERT INTO rdl.yandex_direct_stats VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (date, campaign_id, ad_id) DO NOTHING  # Обновили условие конфликта
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
                if cur.rowcount > 0:
                    processed_rows += 1
            except (ValueError, IndexError) as e:
                logger.warning(f"⚠ Пропущена строка: {line} | Ошибка: {str(e)}")
                continue

        conn.commit()
        logger.info(f"✅ Успешно загружено {processed_rows} строк")
        
    except Exception as e:
        logger.error(f"❌ Ошибка БД: {str(e)}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()

def generate_weekly_ranges(start_date, end_date):
    """Разбивает период на недельные интервалы"""
    current_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")
    date_ranges = []
    
    while current_date < end_date:
        next_date = current_date + timedelta(days=6)  # Неделя = 7 дней (от current_date до next_date)
        if next_date > end_date:
            next_date = end_date
        date_ranges.append((
            current_date.strftime("%Y-%m-%d"),
            next_date.strftime("%Y-%m-%d")
        ))
        current_date = next_date + timedelta(days=1)  # Следующая неделя начинается со следующего дня
    
    return date_ranges

def check_existing_data(db_config, date_from, date_to):
    """Проверяет, есть ли уже данные за указанный период"""
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
        
        exists = cur.fetchone()[0]
        return exists
        
    except Exception as e:
        logger.warning(f"⚠ Ошибка при проверке данных: {str(e)}")
        return False
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    try:
        config = load_config()
        token = config['YandexDirect']['ACCESS_TOKEN']
        db_config = config['Database']

        # Указываем период за 2025 год (или любой другой)
        start_date = "2025-06-10"
        end_date = "2025-06-24"

        # Разбиваем на недельные интервалы
        date_ranges = generate_weekly_ranges(start_date, end_date)

        for date_from, date_to in date_ranges:
            logger.info(f"\n📅 Проверка данных за {date_from} — {date_to}...")
            
            # Проверяем, есть ли уже данные за этот период
            if check_existing_data(db_config, date_from, date_to):
                logger.info(f"⏩ Данные уже загружены, пропускаем...")
                continue
            
            # Загружаем данные, если их нет
            report_data = get_direct_report(token, date_from, date_to)
            
            if report_data:
                save_to_postgres(report_data, db_config)
            else:
                logger.warning(f"⚠ Не удалось получить данные за {date_from} — {date_to}")
            
            # Пауза 10 секунд между запросами (чтобы не превысить лимиты API)
            logger.info("⏳ Ожидание 10 секунд...")
            time.sleep(10)

    except Exception as e:
        logger.error(f"🔥 Критическая ошибка: {str(e)}")
        sys.exit(1)
    finally:
        logger.info("\n✅ Выгрузка завершена")
