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
REQUEST_DELAY = 15  # Задержка между запросами
MAX_RETRIES = 3     # Максимальное количество попыток
RETRY_DELAY = 30    # Задержка между попытками

def log_console(message):
    """Вывод сообщений в консоль"""
    console_logger.info(message)

def load_config():
    """Загрузка конфигурации из файла"""
    config = configparser.ConfigParser()
    config.read('config.ini')
    
    required_sections = ['Database', 'YandexDirect']
    for section in required_sections:
        if not config.has_section(section):
            raise ValueError(f"Раздел '{section}' не найден в config.ini")
    
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
    """Создание таблицы в БД если она не существует"""
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
        logger.error(f"Ошибка создания таблицы: {str(e)}")
        raise

def check_existing_data(conn, date):
    """Проверка наличия данных за указанную дату"""
    check_sql = """
    SELECT COUNT(*) FROM rdl.yd_ad_performance_report 
    WHERE date = %s
    """
    
    try:
        with conn.cursor() as cursor:
            cursor.execute(check_sql, (date,))
            return cursor.fetchone()[0] > 0
    except Exception as e:
        logger.error(f"Ошибка проверки данных: {str(e)}")
        raise

def save_to_database(data, db_config, date):
    """Сохранение данных в PostgreSQL"""
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
        logger.error(f"Ошибка базы данных: {str(e)}")
        raise
    finally:
        if conn:
            conn.close()

def parse_tsv_data(tsv_data):
    """Парсинг TSV данных из API"""
    if not tsv_data or not tsv_data.strip():
        logger.error("Получены пустые данные")
        return None
        
    lines = [line for line in tsv_data.split('\n') 
             if line.strip() and not line.startswith('Дата\t')]
    
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

def check_token(token):
    """Проверка валидности токена"""
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept-Language": "ru",
        "Content-Type": "application/json"
    }
    
    try:
        # Тестовый запрос минимального отчета
        test_body = {
            "params": {
                "SelectionCriteria": {"DateFrom": "2000-01-01", "DateTo": "2000-01-01"},
                "FieldNames": ["Date"],
                "ReportName": "token_check",
                "ReportType": "AD_PERFORMANCE_REPORT",
                "DateRangeType": "CUSTOM_DATE",
                "Format": "TSV",
                "IncludeVAT": "YES",
                "IncludeDiscount": "NO"
            }
        }
        
        response = requests.post(
            "https://api.direct.yandex.com/json/v5/reports",
            headers=headers,
            json=test_body,
            timeout=10
        )
        
        # Любой ответ кроме 403 означает, что токен работает
        if response.status_code != 403:
            return True
            
        logger.error(f"Токен недействителен: {response.status_code} - {response.text}")
        return False
        
    except Exception as e:
        logger.error(f"Ошибка проверки токена: {str(e)}")
        return False

def get_campaign_stats(token, date):
    """Получение статистики из API Яндекс.Директ"""
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
                log_console(f"Отчет формируется, ожидание {wait_time} сек...")
                time.sleep(wait_time)
                continue
            elif response.status_code == 400:
                error_data = response.json()
                error_detail = error_data.get('error', {}).get('error_detail', '')
                log_console(f"Ошибка в запросе: {error_detail}")
                return None
            else:
                log_console(f"Ошибка API (код {response.status_code})")
                return None
                
        except Exception as e:
            logger.error(f"Ошибка соединения: {str(e)}")
            time.sleep(RETRY_DELAY * (attempt + 1))
    
    log_console("Превышено максимальное количество попыток")
    return None

def get_dates(start_date, end_date):
    """Генерация списка дат для обработки"""
    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()
    
    dates = []
    current = start
    while current <= end:
        dates.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)
    return dates

if __name__ == "__main__":
    try:
        # Загрузка конфигурации
        config = load_config()
        log_console("Конфигурация загружена")
        
        # Проверка токена
        if not check_token(config['yandex']['token']):
            log_console("Ошибка: токен не работает с API отчетов")
            log_console("Убедитесь что:")
            log_console("1. Токен действителен и не истек")
            log_console("2. Имеет право direct:reports")
            log_console("3. Активен в нужном кабинете Яндекс.Директ")
            exit(1)
            
        # Определение дат для обработки
        end_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        start_date = "2025-07-01"  # Начальная дата выгрузки
        
        dates = get_dates(start_date, end_date)
        
        # Обработка данных по дням
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
        logger.error(f"Критическая ошибка: {str(e)}", exc_info=True)
        log_console(f"Скрипт завершился с ошибкой: {str(e)}")
        exit(1)
