#!/usr/bin/env python3
"""
Yandex Metrika Hits Daily Downloader
Загружает данные хитов за вчерашний день в таблицу row.yandex_metrika_hits
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
        
        # Дата за которую выгружаем данные (вчера)
        self.report_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        
        # Поля для выгрузки (как в оригинальном run_hits.py)
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
        return YandexMetrikaLogsapi(
            access_token=self.ym_token,
            default_url_params={'counterId': self.counter_id}
        )

    def wait_for_report(self, client, request_id):
        """Ожидание готовности отчета"""
        while True:
            status = client.info(requestId=request_id).get()["log_request"]["status"]
            if status == "processed":
                return True
            elif status in ("created", "pending"):
                sleep(30)
            else:
                raise Exception(f"Ошибка обработки отчета. Статус: {status}")

    def load_data_to_db(self, data):
        """Загрузка данных в PostgreSQL"""
        conn = psycopg2.connect(**self.db_params)
        try:
            with conn.cursor() as cur:
                execute_batch(cur, """
                    INSERT INTO row.yandex_metrika_hits (
                        watch_id, client_id, date_time,
                        title, url, is_page_view
                    ) VALUES (
                        %s, %s, %s,
                        %s, %s, %s
                    )
                """, data)
            conn.commit()
        finally:
            conn.close()

    def run(self):
        """Основной процесс выгрузки"""
        logger.info(f"Начало выгрузки данных хитов за {self.report_date}")
        
        try:
            # Инициализация клиента API
            ym_client = self.get_ym_client()
            
            # Создание запроса
            params = {
                "fields": ",".join(self.fields),
                "source": "hits",
                "date1": self.report_date,
                "date2": self.report_date
            }
            
            # Отправка запроса
            request = ym_client.create().post(params=params)
            request_id = request["log_request"]["request_id"]
            logger.info(f"Запрос создан, ID: {request_id}")
            
            # Ожидание обработки
            self.wait_for_report(ym_client, request_id)
            
            # Получение информации о частях отчета
            report_info = ym_client.info(requestId=request_id).get()
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
                self.load_data_to_db(prepared_data)
                logger.info(f"Успешно загружено {len(prepared_data)} записей")
            else:
                logger.warning("Нет данных для загрузки")
            
            return True
            
        except Exception as e:
            logger.error(f"Ошибка при выгрузке данных: {str(e)}")
            return False

if __name__ == "__main__":
    downloader = YMHitsDownloader()
    if not downloader.run():
        sys.exit(1)
