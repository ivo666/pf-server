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
logger = logging.getLogger(__name__)

class YandexDirectWeeklyLoader:
    def __init__(self, config_file='config.ini'):
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
            logger.info(f"Запрос данных за период: {week_start} - {week_end}")
            response = requests.post(
                url,
                headers=headers,
                json=body,
                timeout=60
            )
            
            if response.status_code == 200:
                return response.text
            elif response.status_code == 201:
                logger.info("Отчет формируется, ожидание...")
                time.sleep(30)
                return self.get_weekly_report(week_start, week_end)
            else:
                logger.error(f"Ошибка API: {response.status_code} - {response.text}")
                return None
        except Exception as e:
            logger.error(f"Ошибка запроса: {str(e)}")
            return None

    def parse_tsv_data(self, tsv_data):
        """Парсит TSV данные"""
        try:
            lines = [line for line in tsv_data.split('\n') if line.strip() and not line.startswith('Date\t')]
            
            data = []
            for line in lines:
                parts = line.strip().split('\t')
                if len(parts) >= 7:
                    try:
                        record = (
                            parts[0],  # Date
                            int(parts[1]),  # CampaignId
                            parts[2],  # CampaignName
                            int(parts[3]),  # AdId
                            int(parts[4]) if parts[4] else 0,  # Clicks
                            int(parts[5]) if parts[5] else 0,  # Impressions
                            float(parts[6]) / 1000000 if parts[6] else 0.0  # Cost (convert to RUB)
                        )
                        data.append(record)
                    except (ValueError, IndexError) as e:
                        logger.warning(f"Пропуск строки: {line}. Ошибка: {str(e)}")
                        continue
            
            return data
        except Exception as e:
            logger.error(f"Ошибка парсинга данных: {str(e)}")
            return None

    def save_weekly_data(self, data):
        """Сохраняет недельные данные в БД"""
        if not data:
            logger.warning("Нет данных для сохранения")
            return False
        
        insert_query = """
        INSERT INTO rdl.yandex_direct_stats 
        (date, campaign_id, campaign_name, ad_id, clicks, impressions, cost)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (date, campaign_id, ad_id) DO UPDATE SET
            campaign_name = EXCLUDED.campaign_name,
            clicks = EXCLUDED.clicks,
            impressions = EXCLUDED.impressions,
            cost = EXCLUDED.cost;
        """
        
        try:
            with self.conn.cursor() as cursor:
                cursor.executemany(insert_query, data)
                self.conn.commit()
                logger.info(f"Успешно загружено {len(data)} записей за неделю")
                print(f"Успешно загружен пакет данных: {len(data)} записей")  # Явное сообщение в терминал
                return True
        except Exception as e:
            logger.error(f"Ошибка сохранения данных: {str(e)}")
            self.conn.rollback()
            return False

    def process_weekly_data(self):
        """Обрабатывает данные по неделям"""
        current_week_start = self.start_date
        total_records = 0
        
        while current_week_start <= self.end_date:
            week_end = min(current_week_start + timedelta(days=6), self.end_date)
            
            logger.info(f"Обработка недели: {current_week_start} - {week_end}")
            
            # Получаем данные за неделю
            tsv_data = self.get_weekly_report(current_week_start, week_end)
            if not tsv_data:
                logger.warning(f"Нет данных за неделю {current_week_start} - {week_end}")
                current_week_start += timedelta(days=7)
                continue
            
            # Парсим данные
            parsed_data = self.parse_tsv_data(tsv_data)
            if not parsed_data:
                logger.warning(f"Не удалось распарсить данные за неделю {current_week_start} - {week_end}")
                current_week_start += timedelta(days=7)
                continue
            
            # Сохраняем в БД
            if self.save_weekly_data(parsed_data):
                total_records += len(parsed_data)
            
            current_week_start += timedelta(days=7)
        
        return total_records

    def run(self):
        """Основной метод запуска загрузки"""
        try:
            if not self.get_db_connection():
                return False
            
            if not self.check_table_exists():
                return False
            
            total = self.process_weekly_data()
            
            logger.info(f"Загрузка завершена. Всего загружено записей: {total}")
            print(f"Загрузка завершена. Всего загружено: {total} записей")  # Финал
            
            return True
        except Exception as e:
            logger.error(f"Ошибка в процессе загрузки: {str(e)}")
            return False
        finally:
            if self.conn:
                self.conn.close()

if __name__ == "__main__":
    loader = YandexDirectWeeklyLoader()
    loader.run()
