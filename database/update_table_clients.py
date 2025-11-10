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
        logging.FileHandler('update_cdm_clients.log'),
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

def update_cdm_clients():
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
        CREATE TABLE IF NOT EXISTS cdm.table_clients (
            client_id text PRIMARY KEY,
            city text,
            browser text,
            screen_width text,
            screen_height text
        )
        """
        
        # SQL для заполнения таблицы (без русскоязычных комментариев)
        # ... (остальной код остается без изменений)

        update_data_sql = """
        INSERT INTO cdm.table_clients
        SELECT 
            client_id,
            region_city AS city,
            browser,
            physical_screen_width AS screen_width,
            physical_screen_height AS screen_height
        FROM (
            SELECT 
                client_id,
                region_city,
                browser,
                physical_screen_width,
                physical_screen_height,
                date_time,
                ROW_NUMBER() OVER(
                    PARTITION BY client_id 
                    ORDER BY date_time DESC
                ) AS last_visit
            FROM yandex_metrika_visits
            WHERE 
                date = CURRENT_DATE - INTERVAL '1 day'  -- ДОБАВЛЕНО: фильтр по дате
                AND region_country = 'Russia'
                AND referal_source NOT IN ('metrika.yandex.ru', 'klaue.cloudbpm.ru')
                AND client_id NOT IN (
                    '1742907795159016963', '1690275728585926726', '1745571831246112348',
                    '1660561659524790881', '171759016385815372', '1739452086606602606',
                    '1744210585372274818', '1745570119620709361', '1745570221463237118',
                    '1745571778695559054', '1745571831246112348',
                    '1749632059707122060', '174854765848914695',
                    '1749814059318463188', '1660561659524790881'
                )
                AND device_category = '1'
                AND screen_orientation = '2'
                AND browser NOT IN (
                    'miui', 'headlesschrome', 
                    'samsungbrowser', 'sputnik', 
                    'maxthonbrowser'
                )
        ) AS last_client_visits
        WHERE last_visit = 1
        ON CONFLICT (client_id) DO UPDATE SET
            city = EXCLUDED.city,
            browser = EXCLUDED.browser,
            screen_width = EXCLUDED.screen_width,
            screen_height = EXCLUDED.screen_height
        """
        
        # Выполнение SQL-запросов
        cursor.execute(create_table_sql)
        logging.info("CDM table cdm.table_clients created or verified")
        
        cursor.execute(update_data_sql)
        conn.commit()
        
        # Получаем статистику
        cursor.execute("SELECT COUNT(*) FROM cdm.table_clients")
        total_clients = cursor.fetchone()[0]
        
        cursor.execute("""
            SELECT COUNT(DISTINCT city) 
            FROM cdm.table_clients 
            WHERE city IS NOT NULL
        """)
        cities_count = cursor.fetchone()[0]
        
        logging.info(
            f"Data successfully loaded to CDM layer. "
            f"Total clients: {total_clients}, "
            f"Unique cities: {cities_count}"
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
    logging.info("CDM clients table update started")
    try:
        update_cdm_clients()
    except Exception as e:
        logging.critical(f"CDM update failed: {str(e)}", exc_info=True)
        exit(1)
    logging.info("CDM clients table update completed successfully")
