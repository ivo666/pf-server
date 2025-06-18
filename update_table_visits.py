import os
import logging
import configparser
import psycopg2
from psycopg2 import sql
from datetime import datetime

# Настройка логгирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('update_cdm_visits.log'),
        logging.StreamHandler()
    ]
)

def get_db_config():
    """Чтение конфигурации из config.ini"""
    dirname = os.path.dirname(__file__)
    config = configparser.ConfigParser()
    config.read(os.path.join(dirname, "config.ini"))
    
    if not config.has_section('Database'):
        raise ValueError("В config.ini отсутствует секция [Database]")
    
    return {
        'host': config['Database']['HOST'],
        'database': config['Database']['DATABASE'],
        'user': config['Database']['USER'],
        'password': config['Database']['PASSWORD'],
        'port': config.get('Database', 'PORT', fallback='5432')
    }

def update_cdm_visits():
    try:
        # Получаем параметры подключения
        db_config = get_db_config()
        
        # Подключение к БД
        conn = psycopg2.connect(**db_config)
        conn.autocommit = False
        cursor = conn.cursor()
        
        logging.info("Successfully connected to the database")
        
        # SQL для создания таблицы в слое CDM
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS cdm.table_visits (
            visit_id text PRIMARY KEY,
            client_id text,
            visit_date date,
            visit_ts timestamp,
            visit_duration integer,
            is_new_user text,
            start_url text,
            end_url text,
            page_views integer,
            utm_campaign text,
            utm_content text,
            utm_medium text,
            utm_source text,
            utm_term text,
            source_ text,
            medium_ text,
            FOREIGN KEY (client_id) REFERENCES cdm.table_clients(client_id) ON DELETE CASCADE
        )
        """
        
        # SQL для заполнения таблицы
        update_data_sql = """
        INSERT INTO cdm.table_visits
        SELECT
            ymv.visit_id,
            ymv.client_id,
            ymv.date AS visit_date,
            ymv.date_time AS visit_ts,
            ymv.visit_duration,
            ymv.is_new_user,
            split_part(ymv.start_url, '?', 1) AS start_url,
            split_part(ymv.end_url, '?', 1) AS end_url,
            cpv.cnt_page_views,
            ymv.utm_campaign,
            ymv.utm_content,
            ymv.utm_medium,
            ymv.utm_source,
            ymv.utm_term,
            CASE
                WHEN ymv.traffic_source = 'internal' THEN ymv.traffic_source
                WHEN ymv.referal_source LIKE '%yandex.ru%' AND ymv.referer LIKE '%yandex.ru/images/search%' THEN 'yandex_images'
                WHEN ymv.referal_source <> '' THEN 'referal source'
                WHEN ymv.utm_medium = 'cpc' THEN ymv.utm_source
                WHEN ymv.adv_engine = 'ya_direct' AND ymv.traffic_source = 'ad' THEN 'yandex_poisk'
                WHEN ymv.social_network <> '' THEN ymv.social_network
                WHEN ymv.traffic_source = 'direct' THEN ymv.traffic_source
                WHEN ymv.utm_source = 'spravker' THEN ymv.utm_source
                WHEN ymv.traffic_source <> '' THEN ymv.search_engine_root
            END AS source_,
            CASE
                WHEN ymv.traffic_source = 'internal' THEN ymv.traffic_source
                WHEN ymv.referal_source LIKE '%yandex.ru%' AND ymv.referer LIKE '%yandex.ru/images/search%' THEN 'organic'
                WHEN ymv.referal_source <> '' THEN ymv.traffic_source
                WHEN ymv.utm_medium = 'cpc' THEN ymv.utm_medium
                WHEN ymv.social_network <> '' THEN ymv.traffic_source
                WHEN ymv.adv_engine = 'ya_direct' AND ymv.traffic_source = 'ad' THEN 'cpc'
                WHEN ymv.traffic_source = 'direct' THEN 'none'
                WHEN ymv.traffic_source <> '' THEN ymv.traffic_source
                WHEN ymv.utm_source = 'spravker' THEN 'referral'
            END AS medium_
        FROM yandex_metrika_visits ymv
        JOIN (
            SELECT 
                visit_id,
                COUNT(DISTINCT url) AS cnt_page_views
            FROM cdm.table_page_views
            GROUP BY visit_id
        ) AS cpv ON cpv.visit_id = ymv.visit_id
        WHERE ymv.client_id IN (SELECT client_id FROM cdm.table_clients)
        ON CONFLICT (visit_id) DO UPDATE SET
            client_id = EXCLUDED.client_id,
            visit_date = EXCLUDED.visit_date,
            visit_ts = EXCLUDED.visit_ts,
            visit_duration = EXCLUDED.visit_duration,
            is_new_user = EXCLUDED.is_new_user,
            start_url = EXCLUDED.start_url,
            end_url = EXCLUDED.end_url,
            page_views = EXCLUDED.page_views,
            utm_campaign = EXCLUDED.utm_campaign,
            utm_content = EXCLUDED.utm_content,
            utm_medium = EXCLUDED.utm_medium,
            utm_source = EXCLUDED.utm_source,
            utm_term = EXCLUDED.utm_term,
            source_ = EXCLUDED.source_,
            medium_ = EXCLUDED.medium_
        """
        
        # Выполнение SQL-запросов
        cursor.execute(create_table_sql)
        logging.info("CDM table cdm.table_visits created or verified")
        
        cursor.execute(update_data_sql)
        conn.commit()
        
        # Получаем статистику
        cursor.execute("SELECT COUNT(*) FROM cdm.table_visits")
        total_visits = cursor.fetchone()[0]
        
        cursor.execute("""
            SELECT 
                COUNT(DISTINCT client_id),
                SUM(page_views),
                AVG(visit_duration)
            FROM cdm.table_visits
        """)
        stats = cursor.fetchone()
        
        logging.info(
            f"Data successfully loaded to CDM layer. "
            f"Total visits: {total_visits}, "
            f"Unique clients: {stats[0]}, "
            f"Total page views: {stats[1]}, "
            f"Avg duration: {stats[2]:.1f} sec"
        )
        
    except Exception as e:
        logging.error(f"Error loading data to CDM: {str(e)}", exc_info=True)
        if 'conn' in locals() and conn:
            conn.rollback()
        raise
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()
        logging.info("Database connection closed")

if __name__ == "__main__":
    logging.info("CDM visits table update started")
    try:
        update_cdm_visits()
    except Exception as e:
        logging.critical(f"CDM update failed: {str(e)}", exc_info=True)
        exit(1)
    logging.info("CDM visits table update completed successfully")
