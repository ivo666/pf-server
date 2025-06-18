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
        logging.FileHandler('update_visits_table.log'),
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

def create_visits_table():
    try:
        # Получаем параметры подключения
        db_config = get_db_config()
        
        # Подключение к БД
        conn = psycopg2.connect(**db_config)
        conn.autocommit = False
        cursor = conn.cursor()
        
        logging.info("Successfully connected to the database")
        
        # Создаем схему cdm если не существует
        cursor.execute("CREATE SCHEMA IF NOT EXISTS cdm")
        conn.commit()

        # SQL для создания таблицы
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS cdm.table_visits_svr (
            visit_id text PRIMARY KEY,
            client_id text,
            visit_date date,
            visit_ts timestamp,
            visit_duration integer,
            is_new_user integer,
            start_url text,
            end_url text,
            page_views integer,
            source_ text,
            medium_ text,
            FOREIGN KEY (client_id) REFERENCES cdm.table_clients_svr(client_id) ON DELETE CASCADE
        )
        """
        
        # SQL для заполнения таблицы
        insert_data_sql = """
        INSERT INTO cdm.table_visits_svr
        SELECT 
            rv.visit_id,
            rv.client_id,
            rv.date,
            rv.date_time,
            rv.visit_duration,
            CASE WHEN rv.is_new_user THEN 1 ELSE 0 END,
            split_part(rv.start_url, '?', 1) AS start_url,
            split_part(rv.end_url, '?', 1) AS end_url,
            cpv.cnt_page_views,
            CASE
                WHEN rv.traffic_source = 'internal' THEN rv.traffic_source
                WHEN rv.referal_source LIKE '%yandex.ru%' AND rv.referer LIKE '%yandex.ru/images/search%' THEN 'yandex_images'
                WHEN rv.referal_source <> '' THEN 'referal source'
                WHEN rv.utm_medium = 'cpc' THEN rv.utm_source
                WHEN rv.adv_engine = 'ya_direct' AND rv.traffic_source = 'ad' THEN 'yandex_poisk'
                WHEN rv.social_network <> '' THEN rv.social_network
                WHEN rv.traffic_source = 'direct' THEN rv.traffic_source
                WHEN rv.utm_source = 'spravker' THEN rv.utm_source
                WHEN rv.traffic_source <> '' THEN rv.search_engine_root
            END AS source_,
            CASE
                WHEN rv.traffic_source = 'internal' THEN rv.traffic_source
                WHEN rv.referal_source LIKE '%yandex.ru%' AND rv.referer LIKE '%yandex.ru/images/search%' THEN 'organic'
                WHEN rv.referal_source <> '' THEN rv.traffic_source
                WHEN rv.utm_medium = 'cpc' THEN rv.utm_medium
                WHEN rv.social_network <> '' THEN rv.traffic_source
                WHEN rv.adv_engine = 'ya_direct' AND rv.traffic_source = 'ad' THEN 'cpc'
                WHEN rv.traffic_source = 'direct' THEN 'none'
                WHEN rv.traffic_source <> '' THEN rv.traffic_source
                WHEN rv.utm_source = 'spravker' THEN 'referral'
            END AS medium_
        FROM yandex_metrika_visits rv
        JOIN (
            SELECT 
                visit_id,
                COUNT(DISTINCT url) AS cnt_page_views
            FROM cdm.table_page_views_svr
            GROUP BY visit_id
        ) AS cpv ON cpv.visit_id = rv.visit_id
        WHERE rv.client_id IN (SELECT client_id FROM cdm.table_clients_svr)
        ON CONFLICT (visit_id) DO UPDATE SET
            client_id = EXCLUDED.client_id,
            visit_date = EXCLUDED.visit_date,
            visit_ts = EXCLUDED.visit_ts,
            visit_duration = EXCLUDED.visit_duration,
            is_new_user = EXCLUDED.is_new_user,
            start_url = EXCLUDED.start_url,
            end_url = EXCLUDED.end_url,
            page_views = EXCLUDED.page_views,
            source_ = EXCLUDED.source_,
            medium_ = EXCLUDED.medium_
        """
        
        # Выполнение SQL-запросов
        cursor.execute(create_table_sql)
        logging.info("Table cdm.table_visits_svr created or already exists")
        
        cursor.execute(insert_data_sql)
        conn.commit()
        
        # Получаем количество записей
        cursor.execute("SELECT COUNT(*) FROM cdm.table_visits_svr")
        count = cursor.fetchone()[0]
        logging.info(f"Data successfully inserted. Total visits: {count}")
        
    except Exception as e:
        logging.error(f"Error occurred: {str(e)}", exc_info=True)
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
    logging.info("Script started")
    try:
        create_visits_table()
    except Exception as e:
        logging.critical(f"Script failed: {str(e)}", exc_info=True)
    logging.info("Script finished")
