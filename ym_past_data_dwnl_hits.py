#!/usr/bin/env python3
"""
Yandex Metrika Hits Historical Downloader
Загружает исторические данные хитов в таблицу row.yandex_metrika_hits
с проверкой дубликатов по watch_id
"""

import os
import sys
import logging
from datetime import datetime, timedelta
from time import sleep
import configparser
import psycopg2
from psycopg2.extras import execute_batch
import pandas as pd
from tapi_yandex_metrika import YandexMetrikaLogsapi

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/var/log/ym_hits_download.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class YMHitsDownloader:
    def __init__(self):
        # Загрузка конфигурации
        self.config = configparser.ConfigParser()
        self.config.read('/home/pf-server/config.ini')
        
        # Параметры подключения
        self.db_params = {
            'host': self.config['Database']['HOST'],
            'database': self.config['Database']['DATABASE'],
            'user': self.config['Database']['USER'],
            'password': self.config['Database']['PASSWORD']
        }
        
        # Параметры API
        self.ym_token = self.config['YandexMetrika']['ACCESS_TOKEN']
        self.counter_id = self.config['YandexMetrika']['COUNTER_ID']
        
        # Поля для выгрузки
        self.fields = [
            'ym:pv:watchID',
            'ym:pv:clientID',
            'ym:pv:dateTime',
            'ym:pv:title',
            'ym:pv:URL',
            'ym:pv:isPageView'
        ]

    def get_ym_client(self):
        """Инициализация клиента Яндекс.Метрики"""
        try:
            return YandexMetrikaLogsapi(
                access_token=self.ym_token,
                default_url_params={'counterId': self.counter_id}
            )
        except Exception as e:
            logger.error(f"Ошибка инициализации клиента API: {str(e)}")
            raise

    def wait_for_report(self, client, request_id, max_attempts=30):
        """Ожидание готовности отчета"""
        attempt = 0
        while attempt < max_attempts:
            try:
                info = client.info(requestId=request_id).get()
                status = info["log_request"]["status"]
                
                if status == "processed":
                    return info
                elif status in ("created", "pending"):
                    logger.info(f"Отчет в обработке, статус: {status}. Ждем...")
                    sleep(30)
                    attempt += 1
                else:
                    raise Exception(f"Ошибка обработки отчета. Статус: {status}")
            except Exception as e:
                logger.error(f"Ошибка проверки статуса отчета: {str(e)}")
                raise
        raise Exception("Достигнуто максимальное количество попыток")

    def load_data_to_db(self, data):
        """Загрузка данных в PostgreSQL с проверкой дубликатов"""
        if not data:
            logger.warning("Нет данных для загрузки")
            return False

        conn = None
        try:
            conn = psycopg2.connect(**self.db_params)
            with conn.cursor() as cur:
                # Создаем временную таблицу для новых данных
                cur.execute("""
                    CREATE TEMP TABLE temp_hits_data (
                        LIKE row.yandex_metrika_hits
                    ) ON COMMIT DROP
                """)
                
                # Вставляем данные во временную таблицу
                sql_temp = """
                    INSERT INTO temp_hits_data (
                        watch_id, client_id, date_time,
                        title, url, is_page_view
                    ) VALUES (
                        %s, %s, %s,
                        %s, %s, %s
                    )
                """
                execute_batch(cur, sql_temp, data)
                
                # Вставляем только новые данные
                sql_final = """
                    INSERT INTO row.yandex_metrika_hits
                    SELECT * FROM temp_hits_data t
                    WHERE NOT EXISTS (
                        SELECT 1 FROM row.yandex_metrika_hits m
                        WHERE m.watch_id = t.watch_id
                    )
                """
                cur.execute(sql_final)
                inserted_count = cur.rowcount
                
            conn.commit()
            logger.info(f"Успешно загружено {inserted_count} новых записей (пропущено {len(data) - inserted_count} дубликатов)")
            return True
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Ошибка при загрузке данных в БД: {str(e)}")
            return False
        finally:
            if conn:
                conn.close()

    def get_weekly_periods(self, start_date, end_date=None):
        """Генерация недельных периодов"""
        if end_date is None:
            # Используем вчерашний день как конечную дату
            end_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        
        start = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')
        
        # Если начальная дата в будущем - возвращаем пустой список
        if start > end:
            return []
        
        current = start
        periods = []
        
        while current < end:
            period_end = current + timedelta(days=6)
            if period_end > end:
                period_end = end
            
            periods.append((
                current.strftime('%Y-%m-%d'),
                period_end.strftime('%Y-%m-%d')
            ))
            
            current = period_end + timedelta(days=1)
        
        return periods

    def process_period(self, ym_client, date1, date2):
        """Обработка данных за указанный период"""
        try:
            # Проверяем, что date2 не является сегодняшней или будущей датой
            today = datetime.now().strftime('%Y-%m-%d')
            if date2 >= today:
                date2 = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
                logger.info(f"Скорректирован диапазон дат на {date1}-{date2} для избежания будущих дат")
            
            logger.info(f"Обработка периода {date1} - {date2}")
            
            # Создание запроса
            params = {
                "fields": ",".join(self.fields),
                "source": "hits",
                "date1": date1,
                "date2": date2
            }
            
            # Отправка запроса
            request = ym_client.create().post(params=params)
            request_id = request["log_request"]["request_id"]
            logger.info(f"Запрос создан, ID: {request_id}")
            
            # Ожидание обработки
            report_info = self.wait_for_report(ym_client, request_id)
            parts_count = len(report_info["log_request"]["parts"])
            logger.info(f"Отчет готов. Частей: {parts_count}")
            
            # Загрузка и обработка данных
            all_data = []
            for part_number in range(parts_count):
                logger.info(f"Обработка части {part_number + 1}/{parts_count}")
                part_data = ym_client.download(requestId=request_id, partNumber=part_number).get()().to_dicts()
                all_data.extend(part_data)
            
            # Подготовка данных для вставки
            prepared_data = []
            for row in all_data:
                prepared_data.append((
                    row.get('ym:pv:watchID'),
                    row.get('ym:pv:clientID'),
                    pd.to_datetime(row.get('ym:pv:dateTime')),
                    row.get('ym:pv:title'),
                    row.get('ym:pv:URL'),
                    row.get('ym:pv:isPageView')
                ))
            
            # Загрузка в БД
            if prepared_data:
                return self.load_data_to_db(prepared_data)
            else:
                logger.warning("Нет данных для загрузки")
                return True
                
        except Exception as e:
            logger.error(f"Ошибка при обработке периода {date1}-{date2}: {str(e)}")
            return False

    def run_historical(self, start_date='2024-01-01'):
        """Основной процесс исторической выгрузки"""
        logger.info(f"Начало исторической выгрузки данных хитов с {start_date}")
        
        try:
            # Инициализация клиента API
            ym_client = self.get_ym_client()
            
            # Получение списка периодов
            periods = self.get_weekly_periods(start_date)
            logger.info(f"Всего периодов для обработки: {len(periods)}")
            
            if not periods:
                logger.warning("Нет периодов для обработки (начальная дата может быть в будущем)")
                return False
            
            # Обработка каждого периода
            for i, (date1, date2) in enumerate(periods, 1):
                logger.info(f"Обработка периода {i}/{len(periods)}: {date1} - {date2}")
                if not self.process_period(ym_client, date1, date2):
                    logger.error(f"Не удалось обработать период {date1} - {date2}")
                    continue
                
                # Пауза между запросами
                if i < len(periods):
                    sleep(10)
            
            logger.info("Историческая выгрузка завершена")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка исторической выгрузки: {str(e)}")
            return False

    def run_daily(self):
        """Ежедневная выгрузка (оригинальная функциональность)"""
        report_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        logger.info(f"Начало ежедневной выгрузки данных хитов за {report_date}")
        return self.process_period(self.get_ym_client(), report_date, report_date)

if __name__ == "__main__":
    downloader = YMHitsDownloader()
    
    # Для исторической выгрузки (рекомендуется для первого запуска)
    if not downloader.run_historical(start_date='2024-01-01'):
        sys.exit(1)
    
    # ИЛИ для ежедневной выгрузки (как было в оригинале)
    # if not downloader.run_daily():
    #     sys.exit(1)
