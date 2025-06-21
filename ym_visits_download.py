#!/usr/bin/env python3
"""
Yandex Metrika Visits Daily Downloader - FINAL WORKING VERSION
"""

import os
import sys
import logging
from datetime import datetime, timedelta
import configparser
import psycopg2
from psycopg2.extras import execute_batch
import pandas as pd
from tapi_yandex_metrika import YandexMetrikaStats

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
        
        # Dimensions for the report (all fields from your table)
        self.dimensions = [
            'ym:s:clientID',
            'ym:s:visitID',
            'ym:s:watchIDs',
            'ym:s:date',
            'ym:s:dateTime',
            'ym:s:isNewUser',
            'ym:s:startURL',
            'ym:s:endURL',
            'ym:s:regionCountry',
            'ym:s:regionCity',
            'ym:s:trafficSource',
            'ym:s:advEngine',
            'ym:s:referalSource',
            'ym:s:searchEngineRoot',
            'ym:s:searchEngine',
            'ym:s:socialNetwork',
            'ym:s:referer',
            'ym:s:directClickOrder',
            'ym:s:directBannerGroup',
            'ym:s:directClickBanner',
            'ym:s:directClickOrderName',
            'ym:s:clickBannerGroupName',
            'ym:s:directClickBannerName',
            'ym:s:directPlatformType',
            'ym:s:directPlatform',
            'ym:s:directConditionType',
            'ym:s:UTMCampaign',
            'ym:s:UTMContent',
            'ym:s:UTMMedium',
            'ym:s:UTMSource',
            'ym:s:UTMTerm',
            'ym:s:deviceCategory',
            'ym:s:mobilePhone',
            'ym:s:mobilePhoneModel',
            'ym:s:browser',
            'ym:s:screenFormat',
            'ym:s:screenOrientation',
            'ym:s:messenger',
            'ym:s:recommendationSystem'
        ]
        
        # Metrics for the report
        self.metrics = [
            'ym:s:pageViews',
            'ym:s:visitDuration',
            'ym:s:physicalScreenWidth',
            'ym:s:physicalScreenHeight'
        ]

    def get_ym_client(self):
        """Initialize Yandex Metrika API client"""
        try:
            client = YandexMetrikaStats(
                access_token=self.ym_token,
                default_url_params={'counter_id': self.counter_id}
            )
            return client
        except Exception as e:
            logger.error(f"Failed to initialize Yandex Metrika client: {str(e)}")
            raise

    def get_visits_report(self, client):
        """Get visits report from API"""
        try:
            logger.info("Requesting visits report from API")
            
            report = client.stats().get(params={
                'ids': self.counter_id,
                'date1': self.report_date,
                'date2': self.report_date,
                'metrics': ",".join(self.metrics),
                'dimensions': ",".join(self.dimensions),
                'limit': 10000,
                'accuracy': "full"
            })
            
            return report().to_dicts()
        except Exception as e:
            logger.error(f"Error getting visits report: {str(e)}")
            raise

    def prepare_data(self, raw_data):
        """Prepare data for database insertion (all 43 fields)"""
        prepared = []
        for row in raw_data:
            try:
                watch_ids = []
                if isinstance(row.get('ym:s:watchIDs'), str):
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
                    row.get('ym:s:trafficSource'),
                    row.get('ym:s:advEngine'),
                    row.get('ym:s:referalSource'),
                    row.get('ym:s:searchEngineRoot'),
                    row.get('ym:s:searchEngine'),
                    row.get('ym:s:socialNetwork'),
                    row.get('ym:s:referer'),
                    row.get('ym:s:directClickOrder'),
                    row.get('ym:s:directBannerGroup'),
                    row.get('ym:s:directClickBanner'),
                    row.get('ym:s:directClickOrderName'),
                    row.get('ym:s:clickBannerGroupName'),
                    row.get('ym:s:directClickBannerName'),
                    row.get('ym:s:directPlatformType'),
                    row.get('ym:s:directPlatform'),
                    row.get('ym:s:directConditionType'),
                    row.get('ym:s:UTMCampaign'),
                    row.get('ym:s:UTMContent'),
                    row.get('ym:s:UTMMedium'),
                    row.get('ym:s:UTMSource'),
                    row.get('ym:s:UTMTerm'),
                    row.get('ym:s:deviceCategory'),
                    row.get('ym:s:mobilePhone'),
                    row.get('ym:s:mobilePhoneModel'),
                    row.get('ym:s:browser'),
                    row.get('ym:s:screenFormat'),
                    row.get('ym:s:screenOrientation'),
                    int(row.get('ym:s:physicalScreenWidth', 0)),
                    int(row.get('ym:s:physicalScreenHeight', 0)),
                    row.get('ym:s:messenger'),
                    row.get('ym:s:recommendationSystem')
                ))
            except Exception as e:
                logger.error(f"Error processing data row: {str(e)}")
                continue
        return prepared

    def load_data_to_db(self, data):
        """Load data to PostgreSQL (all 44 fields including loaded_at)"""
        if not data:
            logger.warning("No data to load")
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
            
            # Get visits report
            raw_data = self.get_visits_report(ym_client)
            
            if not raw_data:
                logger.warning("No data received from API")
                return False
                
            # Prepare data for database
            prepared_data = self.prepare_data(raw_data)
            
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
