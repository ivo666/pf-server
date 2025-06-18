import os
import logging
import configparser
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_batch
from datetime import datetime

# Настройка логгирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='update_clients_table.log'
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
        'port': config.get('Database', 'PORT', fallback='5432')  # Порт по умолчанию для PostgreSQL
    }

def update_clients_table():
    try:
        # Получаем параметры подключения
        db_config = get_db_config()
        
        # Подключение к БД
        conn = psycopg2.connect(**db_config)
        conn.autocommit = False  # Для явного управления транзакциями
        cursor = conn.cursor()
        
        logging.info("Successfully connected to the database")
        
        # SQL для создания таблицы, если она не существует
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS cdm.table_clients_svr (
            client_id text PRIMARY KEY,
            city text,
            browser text,
            screen_width text,
            screen_height text
        )
        """
        
        # SQL для обновления данных в таблице
        update_data_sql = """
        INSERT INTO cdm.table_clients_svr
        SELECT "ym:s:clientID",
            "ym:s:regionCity",
            "ym:s:browser",
            "ym:s:physicalScreenWidth",
            "ym:s:physicalScreenHeight"
        FROM (
            SELECT "ym:s:clientID",
                "ym:s:regionCity",
                "ym:s:deviceCategory",
                "ym:s:browser",
                "ym:s:physicalScreenWidth",
                "ym:s:physicalScreenHeight",
                ROW_NUMBER() OVER(PARTITION BY "ym:s:clientID" ORDER BY "ym:s:dateTime" DESC) AS last_visit
            FROM row_visits rv
            WHERE TRUE
                AND "ym:s:regionCountry" = 'Russia'
                AND "ym:s:<attribution>ReferalSource" NOT IN ('metrika.yandex.ru', 'klaue.cloudbpm.ru')
                AND "ym:s:clientID" NOT IN (
                    '1742907795159016963', '1690275728585926726', '1745571831246112348',
                    '1660561659524790881', '171759016385815372', '1739452086606602606',
                    '1744210585372274818', '1745570119620709361', '1745570221463237118',
                    '1745571778695559054', '1745571831246112348'
                )
                AND "ym:s:deviceCategory" = '1'
                AND "ym:s:screenOrientation" = '2'
                AND "ym:s:browser" NOT IN ('miui', 'headlesschrome', 'samsungbrowser', 'sputnik', 'maxthonbrowser')
        ) query_last_visit
        WHERE last_visit = 1
        ON CONFLICT (client_id) DO UPDATE SET
            city = EXCLUDED.city,
            browser = EXCLUDED.browser,
            screen_width = EXCLUDED.screen_width,
            screen_height = EXCLUDED.screen_height
        """
        
        # Выполнение SQL-запросов
        cursor.execute(create_table_sql)
        logging.info("Table checked/created successfully")
        
        cursor.execute(update_data_sql)
        conn.commit()
        
        # Получаем количество записей для логгирования
        cursor.execute("SELECT COUNT(*) FROM cdm.table_clients_svr")
        count = cursor.fetchone()[0]
        logging.info(f"Data successfully updated. Total clients: {count}")
        
    except Exception as e:
        logging.error(f"Error occurred: {str(e)}", exc_info=True)
        if 'conn' in locals() and conn:
            conn.rollback()
        raise  # Можно убрать raise, если не хотите прерывать выполнение cron
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()
        logging.info("Database connection closed")

if __name__ == "__main__":
    logging.info("Script started")
    try:
        update_clients_table()
    except Exception as e:
        logging.critical(f"Script failed: {str(e)}", exc_info=True)
    logging.info("Script finished")
