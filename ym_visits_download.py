#!/usr/bin/env python3
"""
Yandex Metrika Visits Daily Downloader
Загружает данные визитов за вчерашний день в таблицу row.yandex_metrika_visits
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
        logging.FileHandler('/var/log/ym_visits_download.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class YMVisitsDownloader:
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
        
        # Поля для выгрузки (полный список из оригинального run.py)
        self.fields = [
            'ym:s:clientID', 'ym:s:visitID', 'ym:s:watchIDs', 'ym:s:date', 'ym:s:dateTime',
            'ym:s:isNewUser', 'ym:s:startURL', 'ym:s:endURL', 'ym:s:pageViews', 'ym:s:visitDuration',
            'ym:s:regionCountry', 'ym:s:regionCity', 'ym:s:<attribution>TrafficSource',
            'ym:s:<attribution>AdvEngine', 'ym:s:<attribution>ReferalSource',
            'ym:s:<attribution>SearchEngineRoot', 'ym:s:<attribution>SearchEngine',
            'ym:s:<attribution>SocialNetwork', 'ym:s:referer', 'ym:s:<attribution>DirectClickOrder',
            'ym:s:<attribution>DirectBannerGroup', 'ym:s:<attribution>DirectClickBanner',
            'ym:s:<attribution>DirectClickOrderName', 'ym:s:<attribution>ClickBannerGroupName',
            'ym:s:<attribution>DirectClickBannerName', 'ym:s:<attribution>DirectPlatformType',
            'ym:s:<attribution>DirectPlatform', 'ym:s:<attribution>DirectConditionType',
            'ym:s:<attribution>UTMCampaign', 'ym:s:<attribution>UTMContent',
            'ym:s:<attribution>UTMMedium', 'ym:s:<attribution>UTMSource', 'ym:s:<attribution>UTMTerm',
            'ym:s:deviceCategory', 'ym:s:mobilePhone', 'ym:s:mobilePhoneModel', 'ym:s:browser',
            'ym:s:screenFormat', 'ym:s:screenOrientation', 'ym:s:physicalScreenWidth',
            'ym:s:physicalScreenHeight', 'ym:s:<attribution>Messenger',
            'ym:s:<attribution>RecommendationSystem'
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
                    INSERT INTO row.yandex_metrika_visits (
                        client_id, visit_id, watch_ids, date, date_time, is_new_user,
                        start_url, end_url, page_views, visit_duration, region_country,
                        region_city, traffic_source, adv_engine, referal_source,
                        search_engine_root, search_engine, social_network, referer,
                        direct_click_order, direct_banner_group, direct_click_banner,
                        direct_click_order_name, click_banner_group_name, direct_click_banner_name,
                        direct_platform_type, direct_platform, direct_condition_type,
                        utm_campaign, utm_content, utm_medium, utm_source, utm_term,
                        device_category, mobile_phone, mobile_phone_model, browser,
                        screen_format, screen_orientation, physical_screen_width,
                        physical_screen_height, messenger, recommendation_system
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s
                    )
                """, data)
            conn.commit()
        finally:
            conn.close()

    def run(self):
        """Основной процесс выгрузки"""
        logger.info(f"Начало выгрузки данных визитов за {self.report_date}")
        
        try:
            # Инициализация клиента API
            ym_client = self.get_ym_client()
            
            # Создание запроса
            params = {
                "fields": ",".join(self.fields),
                "source": "visits",
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
                watch_ids = []
                if isinstance(row['ym:s:watchIDs'], str):
                    watch_ids = [x.strip(' "\'') for x in row['ym:s:watchIDs'].strip('[]').split(',') if x.strip()]
                
                prepared_data.append((
                    row.get('ym:s:clientID'),
                    row.get('ym:s:visitID'),
                    watch_ids or None,
                    row.get('ym:s:date'),
                    pd.to_datetime(row.get('ym:s:dateTime')),
                    str(row.get('ym:s:isNewUser', '')),
                    row.get('ym:s:startURL'),
                    row.get('ym:s:endURL'),
                    int(row.get('ym:s:pageViews', 0)),
                    int(row.get('ym:s:visitDuration', 0)),
                    row.get('ym:s:regionCountry'),
                    row.get('ym:s:regionCity'),
                    row.get('ym:s:<attribution>TrafficSource'),
                    row.get('ym:s:<attribution>AdvEngine'),
                    row.get('ym:s:<attribution>ReferalSource'),
                    row.get('ym:s:<attribution>SearchEngineRoot'),
                    row.get('ym:s:<attribution>SearchEngine'),
                    row.get('ym:s:<attribution>SocialNetwork'),
                    row.get('ym:s:referer'),
                    row.get('ym:s:<attribution>DirectClickOrder'),
                    row.get('ym:s:<attribution>DirectBannerGroup'),
                    row.get('ym:s:<attribution>DirectClickBanner'),
                    row.get('ym:s:<attribution>DirectClickOrderName'),
                    row.get('ym:s:<attribution>ClickBannerGroupName'),
                    row.get('ym:s:<attribution>DirectClickBannerName'),
                    row.get('ym:s:<attribution>DirectPlatformType'),
                    row.get('ym:s:<attribution>DirectPlatform'),
                    row.get('ym:s:<attribution>DirectConditionType'),
                    row.get('ym:s:<attribution>UTMCampaign'),
                    row.get('ym:s:<attribution>UTMContent'),
                    row.get('ym:s:<attribution>UTMMedium'),
                    row.get('ym:s:<attribution>UTMSource'),
                    row.get('ym:s:<attribution>UTMTerm'),
                    row.get('ym:s:deviceCategory'),
                    row.get('ym:s:mobilePhone'),
                    row.get('ym:s:mobilePhoneModel'),
                    row.get('ym:s:browser'),
                    row.get('ym:s:screenFormat'),
                    row.get('ym:s:screenOrientation'),
                    int(row.get('ym:s:physicalScreenWidth', 0)),
                    int(row.get('ym:s:physicalScreenHeight', 0)),
                    row.get('ym:s:<attribution>Messenger'),
                    row.get('ym:s:<attribution>RecommendationSystem')
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
    downloader = YMVisitsDownloader()
    if not downloader.run():
        sys.exit(1)
