#!/usr/bin/env python3
"""
Yandex Metrika Hits with Params Historical Downloader
Выгружает исторические данные хитов с параметрами событий с 2025-06-01 по вчерашний день
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
import json
import re

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/var/log/ym_hits_params_download.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class YMHitsParamsDownloader:
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
        
        # Поля для выгрузки hits с параметрами
        self.fields = [
            'ym:pv:watchID',
            'ym:pv:pageViewID',
            'ym:pv:clientID',
            'ym:pv:dateTime',
            'ym:pv:title',
            'ym:pv:URL',
            'ym:pv:isPageView',
            'ym:pv:artificial',
            'ym:pv:params',
            'ym:pv:parsedParamsKey1',
            'ym:pv:parsedParamsKey2',
            'ym:pv:parsedParamsKey3'
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

    def parse_event_params(self, params_str):
        """Парсит параметры событий из строки"""
        if not params_str or pd.isna(params_str) or params_str == '{}':
            return {}
        
        if isinstance(params_str, dict):
            return params_str
        
        try:
            if params_str.startswith('"{') and params_str.endswith('}"'):
                fixed_str = params_str[1:-1].replace('""', '"')
                return json.loads(fixed_str)
            
            if not ('"' in params_str or "'" in params_str):
                fixed_str = re.sub(r'([{,]\s*)(\w+)(\s*:)', r'\1"\2"\3', params_str)
                fixed_str = re.sub(r':\s*([^"\s][^,}]*)([,}])', r':"\1"\2', fixed_str)
                return json.loads(fixed_str)
            
            return json.loads(params_str)
        
        except json.JSONDecodeError as e:
            logger.error(f"Не удалось разобрать параметры: {params_str}. Ошибка: {e}")
            return {}

    def load_data_to_db(self, data):
        """Загрузка данных в PostgreSQL"""
        if not data:
            logger.warning("Нет данных для загрузки")
            return False

        conn = None
        try:
            conn = psycopg2.connect(**self.db_params)
            with conn.cursor() as cur:
                execute_batch(cur, """
                    INSERT INTO rdl.ym_hits_params (
                        watch_id, page_view_id, client_id, date_time,
                        title, url, is_page_view, artificial,
                        event_category, event_action, event_label, button_location,
                        event_content, event_context, action_group, page_path
                    ) VALUES (
                        %s, %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s, %s, %s
                    )
                    ON CONFLICT (watch_id) DO NOTHING
                """, data)
            conn.commit()
            logger.info(f"Успешно загружено {len(data)} записей")
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
            end_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        
        start = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')
        
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
            # Проверка и корректировка даты
            today = datetime.now().strftime('%Y-%m-%d')
            if date2 >= today:
                date2 = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
                logger.info(f"Скорректирован диапазон дат на {date1}-{date2}")
            
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
                params = self.parse_event_params(row.get('ym:pv:params'))
                
                prepared_data.append((
                    str(row.get('ym:pv:watchID')),
                    str(row.get('ym:pv:pageViewID')) if pd.notna(row.get('ym:pv:pageViewID')) else None,
                    str(row.get('ym:pv:clientID')) if pd.notna(row.get('ym:pv:clientID')) else None,
                    pd.to_datetime(row.get('ym:pv:dateTime')),
                    row.get('ym:pv:title'),
                    row.get('ym:pv:URL'),
                    str(row.get('ym:pv:isPageView')) if pd.notna(row.get('ym:pv:isPageView')) else None,
                    str(row.get('ym:pv:artificial')) if pd.notna(row.get('ym:pv:artificial')) else None,
                    params.get('eventCategory'),
                    params.get('eventAction'),
                    params.get('eventLabel'),
                    params.get('buttonLocation'),
                    params.get('eventContent'),
                    params.get('eventContext'),
                    params.get('actionGroup'),
                    params.get('pagePath')
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

    def run_historical(self, start_date='2025-06-01'):
        """Основной процесс исторической выгрузки"""
        logger.info(f"Начало исторической выгрузки данных хитов с параметрами с {start_date}")
        
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
                    sleep(15)  # Увеличенная пауза для избежания лимитов API
            
            logger.info("Историческая выгрузка завершена")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка исторической выгрузки: {str(e)}")
            return False

if __name__ == "__main__":
    downloader = YMHitsParamsDownloader()
    
    # Для исторической выгрузки с 2025-06-01 по вчерашний день
    if not downloader.run_historical(start_date='2025-06-01'):
        sys.exit(1)
