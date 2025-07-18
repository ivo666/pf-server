#!/usr/bin/env python3
"""
Yandex Metrika Daily Hits with Params Downloader
Выгружает данные хитов с параметрами событий за вчерашний день
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
        logging.FileHandler('/var/log/ym_hits_params_daily.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class YMHitsParamsDailyDownloader:
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

    def get_yesterday_date(self):
        """Возвращает вчерашнюю дату в формате YYYY-MM-DD"""
        yesterday = datetime.now() - timedelta(days=1)
        return yesterday.strftime("%Y-%m-%d")

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
            logger.info(f"Успешно загружено {len(data)} записей за вчерашний день")
            return True
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Ошибка при загрузке данных в БД: {str(e)}")
            return False
        finally:
            if conn:
                conn.close()

    def process_yesterday(self):
        """Обработка данных за вчерашний день"""
        try:
            # Получаем вчерашнюю дату
            yesterday = self.get_yesterday_date()
            logger.info(f"Начало обработки данных за {yesterday}")
            
            # Инициализация клиента API
            ym_client = self.get_ym_client()
            
            # Создание запроса
            params = {
                "fields": ",".join(self.fields),
                "source": "hits",
                "date1": yesterday,
                "date2": yesterday
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
                logger.warning("Нет данных для загрузки за вчерашний день")
                return True
                
        except Exception as e:
            logger.error(f"Ошибка при обработке данных за вчерашний день: {str(e)}")
            return False

if __name__ == "__main__":
    downloader = YMHitsParamsDailyDownloader()
    
    # Попробуем выполнить обработку до 3 раз при ошибках
    max_attempts = 3
    attempt = 0
    success = False
    
    while attempt < max_attempts and not success:
        attempt += 1
        logger.info(f"Попытка {attempt} из {max_attempts}")
        success = downloader.process_yesterday()
        
        if not success and attempt < max_attempts:
            wait_time = attempt * 30  # Увеличиваем паузу с каждой попыткой
            logger.info(f"Ожидание {wait_time} секунд перед повторной попыткой...")
            sleep(wait_time)
    
    if not success:
        logger.error("Не удалось обработать данные за вчерашний день после всех попыток")
        sys.exit(1)
