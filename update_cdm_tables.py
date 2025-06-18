import os
import logging
from datetime import datetime

# Настройка логгирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/var/log/cdm_update.log'),
        logging.StreamHandler()
    ]
)

def run_script(script_name):
    """Запуск скрипта с логированием"""
    logging.info(f"Starting {script_name}")
    try:
        exit_code = os.system(f"python {script_name}")
        if exit_code != 0:
            raise RuntimeError(f"Script {script_name} failed with code {exit_code}")
        logging.info(f"Successfully completed {script_name}")
    except Exception as e:
        logging.error(f"Error executing {script_name}: {str(e)}")
        raise

def clear_tables():
    """Очистка таблиц в правильном порядке"""
    import psycopg2
    from psycopg2 import sql
    
    db_config = {
        'host': 'your_host',
        'database': 'your_db',
        'user': 'your_user',
        'password': 'your_password'
    }
    
    tables = ['table_visits', 'table_page_views', 'table_clients']
    
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        
        for table in tables:
            logging.info(f"Clearing table cdm.{table}")
            cursor.execute(sql.SQL("TRUNCATE TABLE cdm.{} CASCADE").format(
                sql.Identifier(table)
            ))
            conn.commit()
            
        cursor.close()
    except Exception as e:
        logging.error(f"Error clearing tables: {str(e)}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()

def main():
    try:
        start_time = datetime.now()
        logging.info("=== Starting CDM tables update ===")
        
        # Очистка таблиц (опционально, раскомментируйте если нужно)
        # clear_tables()
        
        # Запуск скриптов в правильном порядке
        scripts = [
            'update_table_clients.py',
            'update_table_page_views.py',
            'update_table_visits.py'
        ]
        
        for script in scripts:
            run_script(script)
            
        logging.info(f"=== CDM update completed in {datetime.now() - start_time} ===")
    except Exception as e:
        logging.critical(f"CDM update failed: {str(e)}")
        exit(1)

if __name__ == "__main__":
    main()
