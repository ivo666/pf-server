import os
import logging
from datetime import datetime
import sys
import subprocess  # Используем вместо os.system

# Настройка логгирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/var/log/cdm_update.log'),
        logging.StreamHandler()
    ]
)

def get_python_path():
    """Получаем путь к python из текущего окружения"""
    return sys.executable

def run_script(script_name):
    """Запуск скрипта с логированием"""
    script_path = os.path.join(os.path.dirname(__file__), script_name)
    python_path = get_python_path()
    
    logging.info(f"Starting {script_name} with {python_path}")
    
    try:
        # Используем subprocess вместо os.system для лучшего контроля
        result = subprocess.run(
            [python_path, script_path],
            check=True,
            capture_output=True,
            text=True
        )
        
        # Логируем вывод скрипта
        if result.stdout:
            logging.info(f"Output from {script_name}:\n{result.stdout}")
        if result.stderr:
            logging.warning(f"Errors from {script_name}:\n{result.stderr}")
            
        logging.info(f"Successfully completed {script_name}")
        
    except subprocess.CalledProcessError as e:
        error_msg = f"Script {script_name} failed with code {e.returncode}\n"
        error_msg += f"Output:\n{e.stdout}\n" if e.stdout else ""
        error_msg += f"Errors:\n{e.stderr}\n" if e.stderr else ""
        logging.error(error_msg)
        raise RuntimeError(error_msg)
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
