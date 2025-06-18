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
        logging.FileHandler('update_page_views.log'),
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

def create_page_views_table():
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
        CREATE TABLE IF NOT EXISTS cdm.table_page_views_svr (
            watch_id text PRIMARY KEY,
            client_id text,
            visit_id text,
            hit_ts timestamp,
            title_page text,
            URL text,
            web_search_query text,
            type_parametres text[],
            primenenie_parametres text[],
            material_parametres text[],
            cleartype_parametres text[],
            FOREIGN KEY (client_id) REFERENCES cdm.table_clients_svr(client_id) ON DELETE CASCADE
        )
        """
        
        # SQL для заполнения таблицы
        insert_data_sql = """
        INSERT INTO cdm.table_page_views_svr
        SELECT 
            rh.watch_id,
            vw.client_id,
            vw.visit_id,
            rh.date_time,
            rh.title,
            CASE 
                WHEN rh.url LIKE '%allfilters%' OR rh.url LIKE '%searchresult%' 
                THEN split_part(rh.url, '?', 1) 
                ELSE rh.url 
            END AS url,
            substring(rh.url FROM '&text=([^&]+)') AS web_search_query,
            regexp_matches(rh.url, 'type\[\]=([^&]+)') AS type_parametres,
            regexp_matches(rh.url, 'primenenie\[\]=([^&]+)') AS primenenie_parametres,
            regexp_matches(rh.url, 'material\[\]=([^&]+)') AS material_parametres,
            regexp_matches(rh.url, 'cleartype\[\]=([^&]+)') AS cleartype_parametres
        FROM (
            SELECT 
                visit_id,
                client_id,
                unnest(watch_ids) AS watch_id
            FROM yandex_metrika_visits
            WHERE client_id IN (SELECT client_id FROM cdm.table_clients_svr)
        ) vw
        JOIN yandex_metrika_hits rh ON vw.watch_id = rh.watch_id
        WHERE rh.is_page_view = 1
        ON CONFLICT (watch_id) DO UPDATE SET
            client_id = EXCLUDED.client_id,
            visit_id = EXCLUDED.visit_id,
            hit_ts = EXCLUDED.hit_ts,
            title_page = EXCLUDED.title_page,
            URL = EXCLUDED.URL,
            web_search_query = EXCLUDED.web_search_query,
            type_parametres = EXCLUDED.type_parametres,
            primenenie_parametres = EXCLUDED.primenenie_parametres,
            material_parametres = EXCLUDED.material_parametres,
            cleartype_parametres = EXCLUDED.cleartype_parametres
        """
        
        # Выполнение SQL-запросов
        cursor.execute(create_table_sql)
        logging.info("Table cdm.table_page_views_svr created or already exists")
        
        cursor.execute(insert_data_sql)
        conn.commit()
        
        # Получаем количество записей
        cursor.execute("SELECT COUNT(*) FROM cdm.table_page_views_svr")
        count = cursor.fetchone()[0]
        logging.info(f"Data successfully inserted. Total page views: {count}")
        
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
        create_page_views_table()
    except Exception as e:
        logging.critical(f"Script failed: {str(e)}", exc_info=True)
    logging.info("Script finished")
