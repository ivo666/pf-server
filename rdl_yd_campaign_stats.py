import requests
import logging
import time
import hashlib
import configparser
import psycopg2
from datetime import datetime, timedelta

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)  # Исправлено на __name__

class YandexDirectWeeklyLoader:
    def __init__(self, config_file='config.ini'):  # Исправлено на __init__
        self.config_file = config_file
        self.token = "y0__xCfm56NBhi4uzgg2IHdxxMB-11ibEFeXtYCgMHlML7g5RHDNA"
        self.conn = None
        self.start_date = datetime(2025, 1, 1).date()
        self.end_date = (datetime.now() - timedelta(days=1)).date()

    def get_db_connection(self):
        """Устанавливает соединение с PostgreSQL"""
        config = configparser.ConfigParser()
        config.read(self.config_file)
        
        try:
            self.conn = psycopg2.connect(
                host=config['Database']['HOST'],
                database=config['Database']['DATABASE'],
                user=config['Database']['USER'],
                password=config['Database']['PASSWORD'],
                port=config['Database']['PORT']
            )
            logger.info("Успешное подключение к PostgreSQL")
            return True
        except Exception as e:
            logger.error(f"Ошибка подключения к БД: {str(e)}")
            return False

    def check_table_exists(self):
        """Проверяет существование целевой таблицы"""
        check_query = """
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables 
            WHERE table_schema = 'rdl' 
            AND table_name = 'yandex_direct_stats'
        );
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(check_query)
                exists = cursor.fetchone()[0]
                if not exists:
                    logger.error("Таблица rdl.yandex_direct_stats не существует!")
                return exists
        except Exception as e:
            logger.error(f"Ошибка проверки таблицы: {str(e)}")
            return False

    def get_weekly_report(self, week_start, week_end):
        """Получает отчет за неделю"""
        url = "https://api.direct.yandex.com/json/v5/reports"
        
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Accept-Language": "ru",
            "Content-Type": "application/json"
        }

        report_name = f"weekly_{week_start.strftime('%Y%m%d')}_to_{week_end.strftime('%Y%m%d')}"

        body = {
            "params": {
                "SelectionCriteria": {
                    "DateFrom": week_start.strftime("%Y-%m-%d"),
                    "DateTo": week_end.strftime("%Y-%m-%d")
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
            response = requests.post(url, json=body, headers=headers)
            response.raise_for_status()  # Проверка на ошибки HTTP
            logger.info(f"Отчет за неделю с {week_start} по {week_end} успешно получен")
            return response.text  # Возвращаем текст отчета
        except requests.exceptions.RequestException as e:
            logger.error(f"Ошибка при получении отчета: {str(e)}")
            return None

# Пример использования класса
if __name__ == "__main__":
    loader = YandexDirectWeeklyLoader()
    if loader.get_db_connection() and loader.check_table_exists():
        # Получаем отчеты по неделям
        current_date = loader.start_date
        while current_date <= loader.end_date:
            week_end_date = current_date + timedelta(days=6)
            report_data = loader.get_weekly_report(current_date, week_end_date)
            if report_data:
                # Обработайте данные отчета здесь (например, вставьте в БД)
                pass
            current_date += timedelta(days=7)
