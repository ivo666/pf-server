import requests
import logging
import time
import uuid
import configparser
from datetime import datetime, timedelta
import psycopg2
import os
from psycopg2.extras import execute_batch

# Настройка логирования
log_file = '/var/log/yandex_direct_loader.log'
os.makedirs(os.path.dirname(log_file), exist_ok=True)

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler(log_file)],
    force=True
)

console_logger = logging.getLogger('console')
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(message)s')
console_handler.setFormatter(formatter)
console_logger.addHandler(console_handler)
console_logger.propagate = False

logger = logging.getLogger(__name__)

# Константы
REQUEST_DELAY = 15
MAX_RETRIES = 3
RETRY_DELAY = 30
DAILY_REPORT = True  # Изменили на дневные отчеты

def log_console(message):
    console_logger.info(message)

def load_config():
    config = configparser.ConfigParser()
    config.read('config.ini')
    
    required_sections = ['Database', 'YandexDirect']
    for section in required_sections:
        if not config.has_section(section):
            raise ValueError(f"Section '{section}' not found in config.ini")
    
    return {
        'db': {
            'host': config['Database'].get('HOST', 'localhost'),
            'database': config['Database'].get('DATABASE', 'pfserver'),
            'user': config['Database'].get('USER', 'postgres'),
            'password': config['Database'].get('PASSWORD', ''),
            'port': config['Database'].get('PORT', '5432')
        },
        'yandex': {
            'token': config['YandexDirect'].get('ACCESS_TOKEN')
        }
    }

def create_table_if_not_exists(conn):
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS rdl.yd_ad_performance_report (
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
        CONSTRAINT pk_yd_ad_performance PRIMARY KEY (date, campaign_id, ad_id)
    )
    """
    
    try:
        with conn.cursor() as cursor:
            cursor.execute("CREATE SCHEMA IF NOT EXISTS rdl;")
            cursor.execute(create_table_sql)
            conn.commit()
            log_console("Таблица проверена/создана")
    except Exception as e:
        logger.error(f"Error creating table: {str(e)}")
        log_console("Ошибка при создании таблицы")
        conn.rollback()
        raise

def check_existing_data(conn, date):
    """Проверяет наличие данных за указанную дату"""
    check_sql = """
    SELECT COUNT(*) FROM rdl.yd_ad_performance_report 
    WHERE date = %s
    """
    
    try:
        with conn.cursor() as cursor:
            cursor.execute(check_sql, (date,))
            return cursor.fetchone()[0] > 0
    except Exception as e:
        logger.error(f"Error checking existing data: {str(e)}")
        raise

def save_to_database(data, db_config, date):
    conn = None
    try:
        conn = psycopg2.connect(**db_config)
        create_table_if_not_exists(conn)
        
        if check_existing_data(conn, date):
            log_console(f"Данные за {date} уже существуют")
            return False
        
        insert_sql = """
        INSERT INTO rdl.yd_ad_performance_report (
            date, campaign_id, campaign_name, ad_id, clicks, impressions, cost, 
            avg_click_position, device, location_of_presence_id, match_type, slot
        ) VALUES (
            %(Date)s, %(CampaignId)s, %(CampaignName)s, %(AdId)s, %(Clicks)s, 
            %(Impressions)s, %(Cost)s, %(AvgClickPosition)s, %(Device)s, 
            %(LocationOfPresenceId)s, %(MatchType)s, %(Slot)s
        ) ON CONFLICT DO NOTHING
        """
        
        with conn.cursor() as cursor:
            execute_batch(cursor, insert_sql, data)
            conn.commit()
            log_console(f"Сохранено {len(data)} записей за {date}")
            return True
            
    except Exception as e:
        logger.error(f"Database error: {str(e)}")
        log_console("Ошибка при сохранении в базу данных")
        raise
    finally:
        if conn:
            conn.close()

def parse_tsv_data(tsv_data):
    """Парсит TSV данные с нужными полями"""
    if not tsv_data or not tsv_data.strip():
        logger.error("Получены пустые данные")
        return None
        
    lines = [line for line in tsv_data.split('\n') 
             if line.strip() and not line.startswith('Date\t')]
    
    data = []
    for line in lines:
        try:
            parts = line.strip().split('\t')
            if len(parts) >= 12:
                # Обработка специальных значений
                avg_pos = None if parts[7] in ('--', '') else float(parts[7].replace(',', '.'))
                cost = None if parts[6] in ('--', '') else float(parts[6].replace(',', '.')) / 1000000
                
                data.append({
                    'Date': parts[0],
                    'CampaignId': int(parts[1]) if parts[1] else None,
                    'CampaignName': parts[2],
                    'AdId': int(parts[3]) if parts[3] else None,
                    'Clicks': int(parts[4]) if parts[4] else 0,
                    'Impressions': int(parts[5]) if parts[5] else 0,
                    'Cost': cost,
                    'AvgClickPosition': avg_pos,
                    'Device': parts[8],
                    'LocationOfPresenceId': int(parts[9]) if parts[9] else None,
                    'MatchType': parts[10],
                    'Slot': parts[11]
                })
        except Exception as e:
            logger.warning(f"Ошибка обработки строки: {line[:100]}... Ошибка: {str(e)}")
            continue
    
    if data:
        logger.debug(f"Первые 3 строки данных: {data[:3]}")
    return data if data else None

def get_campaign_stats(token, date):
    """Запрашивает данные за один день"""
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept-Language": "ru",
        "Content-Type": "application/json"
    }

    body = {
        "params": {
            "SelectionCriteria": {"DateFrom": date, "DateTo": date},
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
            "ReportName": f"report_{uuid.uuid4().hex[:8]}",
            "ReportType": "AD_PERFORMANCE_REPORT",
            "DateRangeType": "CUSTOM_DATE",
            "Format": "TSV",
            "IncludeVAT": "YES",
            "IncludeDiscount": "NO"
        }
    }

    for attempt in range(MAX_RETRIES):
        try:
            log_console(f"Запрос данных за {date} (попытка {attempt + 1})")
            response = requests.post(
                "https://api.direct.yandex.com/json/v5/reports",
                headers=headers,
                json=body,
                timeout=120
            )
            
            if response.status_code == 200:
                return response.text
            elif response.status_code == 201:
                wait_time = RETRY_DELAY * (attempt + 1)
                log_console(f"Отчет формируется, ожидание {wait_time} секунд...")
                time.sleep(wait_time)
                continue
            elif response.status_code == 400:
                error_data = response.json()
                error_msg = error_data.get('error', {}).get('error_string', 'Неизвестная ошибка')
                error_detail = error_data.get('error', {}).get('error_detail', 'Нет деталей')
                logger.error(f"API error 400: {error_msg} - {error_detail}")
                log_console(f"Ошибка в запросе: {error_msg}")
                return None
            elif response.status_code == 500:
                logger.error(f"API error 500: {response.text}")
                log_console("Внутренняя ошибка сервера Яндекс.Директ")
                return None
            elif response.status_code == 403:
                logger.error(f"API error 403: {response.text}")
                log_console("Ошибка авторизации. Проверьте токен доступа")
                return None
            else:
                logger.error(f"API error: {response.status_code} - {response.text}")
                log_console(f"Ошибка API Яндекс.Директ (код {response.status_code})")
                return None
            
        except Exception as e:
            logger.error(f"Request failed (attempt {attempt + 1}): {str(e)}")
            time.sleep(RETRY_DELAY * (attempt + 1))
    
    log_console("Превышено максимальное количество попыток")
    return None

def get_dates(start_date, end_date):
    """Генерирует список дней для запроса"""
    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()
    
    dates = []
    current = start
    while current <= end:
        dates.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)
    return dates

def check_token(token):
    """Проверяет валидность токена"""
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept-Language": "ru"
    }
    
    try:
        response = requests.get(
            "https://api.direct.yandex.com/json/v5/agencyclients",
            headers=headers,
            timeout=10
        )
        if response.status_code == 200:
            return True
        logger.error(f"Token check failed: {response.status_code} - {response.text}")
        return False
    except Exception as e:
        logger.error(f"Token check error: {str(e)}")
        return False

if __name__ == "__main__":
    try:
        config = load_config()
        log_console("Конфигурация загружена")
        
        if not check_token(config['yandex']['token']):
            log_console("Ошибка проверки токена. Проверьте его корректность и права доступа")
            exit(1)
            
        end_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        start_date = "2025-07-01"
        
        dates = get_dates(start_date, end_date)
        
        for date in dates:
            log_console(f"Обработка даты {date}")
            time.sleep(REQUEST_DELAY)
            
            data = get_campaign_stats(config['yandex']['token'], date)
            if not data:
                log_console(f"Пропускаем дату {date} из-за ошибки")
                continue
                
            parsed = parse_tsv_data(data)
            if parsed:
                if not save_to_database(parsed, config['db'], date):
                    log_console(f"Данные за {date} не были сохранены (возможно уже существуют)")
            else:
                log_console(f"Нет данных для сохранения за {date}")
        
        log_console("Скрипт успешно завершен")
        
    except KeyboardInterrupt:
        log_console("Скрипт остановлен пользователем")
        exit(0)
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}", exc_info=True)
        log_console(f"Критическая ошибка: {str(e)}")
        exit(1)
