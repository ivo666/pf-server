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
                "AdName",
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
        logger.info(f"🔄 Загрузка данных по объявлениям за {date_from} — {date_to}...")
        response = requests.post(
            url,
            headers=headers,
            json=report_body,
            timeout=60
        )
        response.raise_for_status()
        
        # Проверяем, что ответ содержит данные
        if not response.text.strip():
            logger.error("❌ Пустой ответ от API")
            return None
            
        return response.text
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Ошибка API: {e}")
        if hasattr(e, 'response') and e.response:
            logger.error(f"Тело ошибки: {e.response.text}")
        return None

# ... (остальные функции save_to_postgres, generate_date_ranges, check_existing_data остаются без изменений)

if __name__ == "__main__":
    try:
        config = load_config()
        token = config['YandexDirect']['ACCESS_TOKEN']
        db_config = config['Database']
        
        # Период для выгрузки (пример)
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
            else:
                logger.error("⚠ Не удалось получить данные")
            
            time.sleep(10)  # Пауза между запросами

    except Exception as e:
        logger.critical(f"🔥 Критическая ошибка: {e}")
        sys.exit(1)
    finally:
        logger.info("✅ Скрипт завершил работу")
