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
    """Загрузка конфигурации из файла"""
    config = configparser.ConfigParser()
    config_path = Path(__file__).parent / 'config.ini'
    config.read(config_path)
    return config

def get_direct_report(token, date_from, date_to):
    """Получение отчета из Яндекс.Директ"""
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
            "ReportType": "AD_PERFORMANCE_REPORT",
            "DateRangeType": "CUSTOM_DATE",
            "Format": "TSV",
            "IncludeVAT": "YES",
            "IncludeDiscount": "NO"
        }
    }

    try:
        logger.info(f"Загрузка данных за {date_from} — {date_to}...")
        response = requests.post(
            url,
            headers=headers,
            json=report_body,
            timeout=60
        )
        
        # Логирование ответа для отладки
        logger.debug(f"Статус ответа: {response.status_code}")
        logger.debug(f"Тело ответа: {response.text[:200]}...")  # Логируем первые 200 символов
        
        response.raise_for_status()
        
        if not response.text.strip():
            logger.error("Получен пустой ответ от API")
            return None
            
        return response.text
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Ошибка при запросе к API: {str(e)}")
        if hasattr(e, 'response') and e.response:
            logger.error(f"Код ошибки: {e.response.status_code}")
            logger.error(f"Тело ошибки: {e.response.text}")
        return None

def save_to_postgres(data, db_config):
    """Сохранение данных в PostgreSQL"""
    conn = None
    try:
        conn = psycopg2.connect(
            host=db_config['host'],
            database=db_config['database'],
            user=db_config['user'],
            password=db_config['password'],
            port=db_config['port']
        )
        cur = conn.cursor()

        # Создание таблицы (если не существует)
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
            # Пропускаем служебные строки
            if not line.strip() or line.startswith('"') or line.startswith('Date\t') or line.startswith('Total rows:'):
                continue
                
            values = line.split('\t')
            if len(values) != 8:
                logger.warning(f"Пропущена строка (неверное количество полей): {line}")
                continue
                
            try:
                # Вставка данных
                cur.execute("""
                    INSERT INTO rdl.yandex_direct_ad_stats VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (date, campaign_id, ad_id) DO NOTHING
                """, (
                    values[0].strip(),          # Date
                    int(values[1]),             # CampaignId
                    values[2].strip(),          # CampaignName
                    int(values[3]),             # AdId
                    int(values[4]),             # Clicks
                    float(values[5]) / 1000000, # Cost (перевод из микроединиц)
                    float(values[6]),           # Ctr
                    int(values[7])              # Impressions
                ))
                processed_rows += 1
            except (ValueError, IndexError) as e:
                logger.warning(f"Ошибка обработки строки: {line} | Ошибка: {str(e)}")
                continue

        conn.commit()
        logger.info(f"Успешно загружено строк: {processed_rows}")
        
    except Exception as e:
        logger.error(f"Ошибка при работе с БД: {str(e)}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()

def generate_date_ranges(start_date, end_date):
    """Генерация недельных интервалов дат"""
    date_ranges = []
    current_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")
    
    while current_date <= end_date:
        next_date = min(current_date + timedelta(days=6), end_date)
        date_ranges.append((
            current_date.strftime("%Y-%m-%d"),
            next_date.strftime("%Y-%m-%d")
        ))
        current_date = next_date + timedelta(days=1)
    
    return date_ranges

def check_existing_data(db_config, date_from, date_to):
    """Проверка существующих данных в БД"""
    conn = None
    try:
        conn = psycopg2.connect(
            host=db_config['host'],
            database=db_config['database'],
            user=db_config['user'],
            password=db_config['password'],
            port=db_config['port']
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
        logger.error(f"Ошибка при проверке данных: {str(e)}")
        return False
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    try:
        # Загрузка конфигурации
        config = load_config()
        token = config['YandexDirect']['ACCESS_TOKEN']
        db_config = {
            'host': config['Database']['HOST'],
            'database': config['Database']['DATABASE'],
            'user': config['Database']['USER'],
            'password': config['Database']['PASSWORD'],
            'port': config['Database']['PORT']
        }

        # Параметры выгрузки
        start_date = "2025-06-10"
        end_date = "2025-06-24"

        logger.info("Начало выгрузки данных")
        
        # Обработка каждого временного интервала
        for date_from, date_to in generate_date_ranges(start_date, end_date):
            logger.info(f"\nОбработка периода: {date_from} — {date_to}")
            
            # Проверка существующих данных
            if check_existing_data(db_config, date_from, date_to):
                logger.info("Данные уже существуют, пропускаем")
                continue
                
            # Получение данных из API
            data = get_direct_report(token, date_from, date_to)
            if data:
                save_to_postgres(data, db_config)
            else:
                logger.error("Не удалось получить данные")
                
            time.sleep(10)  # Задержка между запросами

    except KeyboardInterrupt:
        logger.info("Выполнение прервано пользователем")
    except Exception as e:
        logger.critical(f"Критическая ошибка: {str(e)}")
        sys.exit(1)
    finally:
        logger.info("Работа скрипта завершена")
