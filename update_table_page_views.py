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
        logging.FileHandler('update_cdm_page_views.log'),
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

def update_cdm_page_views():
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

        # SQL для создания таблицы в слое CDM
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS cdm.table_page_views (
            watch_id text PRIMARY KEY,
            client_id text,
            visit_id text,
            hit_ts timestamp,
            title_page text,
            url text,
            web_search_query text,
            type_parametres text[],
            primenenie_parametres text[],
            material_parametres text[],
            cleartype_parametres text[],
            FOREIGN KEY (client_id) REFERENCES cdm.table_clients(client_id) ON DELETE CASCADE
        )
        """
        
        # SQL для заполнения таблицы
        update_data_sql = r"""
        INSERT INTO cdm.table_page_views
        SELECT 
            ymh.watch_id,
            vw.client_id,
            vw.visit_id,
            ymh.date_time AS hit_ts,
            ymh.title AS title_page,
            CASE 
                WHEN ymh.url LIKE '%allfilters%' 
                    OR ymh.url LIKE '%searchresult%'
                    OR ymh.url LIKE '%?etext=%'
                THEN split_part(ymh.url, '?', 1)
                ELSE ymh.url
            END AS url,
            substring(ymh.url FROM '&text=([^&]+)') AS web_search_query,
            ARRAY(SELECT regexp_matches(ymh.url, 'type\[\]=([^&]+)', 'g')) AS type_parametres,
            ARRAY(SELECT regexp_matches(ymh.url, 'primenenie\[\]=([^&]+)', 'g')) AS primenenie_parametres,
            ARRAY(SELECT regexp_matches(ymh.url, 'material\[\]=([^&]+)', 'g')) AS material_parametres,
            ARRAY(SELECT regexp_matches(ymh.url, 'cleartype\[\]=([^&]+)', 'g')) AS cleartype_parametres
        FROM (
            SELECT 
                visit_id,
                client_id,
                unnest(watch_ids) AS watch_id
            FROM yandex_metrika_visits
            WHERE client_id IN (SELECT client_id FROM cdm.table_clients)
        ) vw
        JOIN yandex_metrika_hits ymh ON vw.watch_id = ymh.watch_id
        WHERE ymh.is_page_view = '1'
        ON CONFLICT (watch_id) DO UPDATE SET
            client_id = EXCLUDED.client_id,
            visit_id = EXCLUDED.visit_id,
            hit_ts = EXCLUDED.hit_ts,
            title_page = EXCLUDED.title_page,
            url = EXCLUDED.url,
            web_search_query = EXCLUDED.web_search_query,
            type_parametres = EXCLUDED.type_parametres,
            primenenie_parametres = EXCLUDED.primenenie_parametres,
            material_parametres = EXCLUDED.material_parametres,
            cleartype_parametres = EXCLUDED.cleartype_parametres
        """
        
        # Выполнение SQL-запросов
        cursor.execute(create_table_sql)
        logging.info("CDM table cdm.table_page_views created or verified")
        
        cursor.execute(update_data_sql)
        conn.commit()
        
        # Получаем статистику
        cursor.execute("SELECT COUNT(*) FROM cdm.table_page_views")
        total_views = cursor.fetchone()[0]
        
        cursor.execute("""
            SELECT COUNT(DISTINCT visit_id) 
            FROM cdm.table_page_views
        """)
        visits_count = cursor.fetchone()[0]
        
        logging.info(
            f"Data successfully loaded to CDM layer. "
            f"Total page views: {total_views}, "
            f"Unique visits: {visits_count}"
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
    logging.info("CDM page views table update started")
    try:
        update_cdm_page_views()
    except Exception as e:
        logging.critical(f"CDM update failed: {str(e)}", exc_info=True)
        exit(1)
    logging.info("CDM page views table update completed successfully")
