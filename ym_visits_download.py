#!/usr/bin/env python3
"""
Yandex Metrika Visits Daily Downloader - FINAL FIXED VERSION
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

# Configure logging
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
        # Load configuration
        self.config = configparser.ConfigParser()
        self.config.read('/home/pf-server/config.ini')
        
        # Database connection parameters
        self.db_params = {
            'host': self.config['Database']['HOST'],
            'database': self.config['Database']['DATABASE'],
            'user': self.config['Database']['USER'],
            'password': self.config['Database']['PASSWORD']
        }
        
        # Yandex Metrika API credentials
        self.ym_token = self.config['YandexMetrika']['ACCESS_TOKEN']
        self.counter_id = self.config['YandexMetrika']['COUNTER_ID']
        
        # Report date (yesterday)
        self.report_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        
        # Fields to request from API
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
        """Initialize Yandex Metrika API client"""
        try:
            client = YandexMetrikaLogsapi(
                access_token=self.ym_token,
                default_url_params={'counterId': self.counter_id}
            )
            # Test connection
            client.logs().get(params={"date1": self.report_date, "date2": self.report_date})
            return client
        except Exception as e:
            logger.error(f"Failed to initialize Yandex Metrika client: {str(e)}")
            raise

    def wait_for_report(self, client, request_id, max_attempts=20):
        """Wait for report processing to complete"""
        attempt = 0
        while attempt < max_attempts:
            try:
                info = client.info(requestId=request_id).get()
                status = info["log_request"]["status"]
                
                if status == "processed":
                    return info
                elif status in ("created", "pending"):
                    logger.info(f"Report processing, status: {status}. Waiting...")
                    sleep(30)
                    attempt += 1
                else:
                    raise Exception(f"Report processing failed. Status: {status}")
            except Exception as e:
                logger.error(f"Error checking report status: {str(e)}")
                raise
        raise Exception("Max attempts reached while waiting for report")

    def prepare_data(self, raw_data):
        """Prepare data for database insertion (43 fields)"""
        prepared = []
        for row in raw_data:
            try:
                watch_ids = []
                if isinstance(row['ym:s:watchIDs'], str):
                    watch_ids = [x.strip(' "\'') for x in row['ym:s:watchIDs'].strip('[]').split(',') if x.strip()]
                
                prepared.append((
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
                    # loaded_at will be set automatically by DEFAULT
                ))
            except Exception as e:
                logger.error(f"Error processing data row: {str(e)}")
                continue
        return prepared

    def load_data_to_db(self, data):
        """Load data to PostgreSQL (44 fields including loaded_at)"""
        if not data:
            logger.warning("No data to load")
            return False

        # Verify data structure
        if len(data[0]) != 43:
            logger.error(f"Data structure mismatch: expected 43 fields, got {len(data[0])}")
            return False

        conn = None
        try:
            conn = psycopg2.connect(**self.db_params)
            with conn.cursor() as cur:
                sql = """
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
                        physical_screen_height, messenger, recommendation_system,
                        loaded_at
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, DEFAULT
                    )
                """
                execute_batch(cur, sql, data)
            conn.commit()
            logger.info(f"Successfully loaded {len(data)} records")
            return True
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Database error: {str(e)}")
            return False
        finally:
            if conn:
                conn.close()

    def run(self):
        """Main execution flow"""
        logger.info(f"Starting Yandex Metrika visits download for {self.report_date}")
        
        try:
            # Initialize API client
            ym_client = self.get_ym_client()
            
            # Create API request
            params = {
                "fields": ",".join(self.fields),
                "source": "visits",
                "date1": self.report_date,
                "date2": self.report_date
            }
            
            request = ym_client.create().post(params=params)
            request_id = request["log_request"]["request_id"]
            logger.info(f"Request created, ID: {request_id}")
            
            # Wait for report processing
            report_info = self.wait_for_report(ym_client, request_id)
            parts_count = len(report_info["log_request"]["parts"])
            logger.info(f"Report processed, parts: {parts_count}")
            
            # Download and process data
            all_data = []
            for part_number in range(parts_count):
                logger.info(f"Processing part {part_number + 1}/{parts_count}")
                part_data = ym_client.download(requestId=request_id, partNumber=part_number).get()().to_dicts()
                all_data.extend(part_data)
            
            # Prepare data for database
            prepared_data = self.prepare_data(all_data)
            
            if not prepared_data:
                logger.warning("No valid data to load")
                return False
                
            # Load to database
            return self.load_data_to_db(prepared_data)
            
        except Exception as e:
            logger.error(f"Processing failed: {str(e)}")
            return False

if __name__ == "__main__":
    downloader = YMVisitsDownloader()
    if not downloader.run():
        sys.exit(1)
