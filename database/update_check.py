import subprocess
import logging
from datetime import datetime

# Настройка логгирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

def check_data():
    try:
        # Команда для выполнения в psql
        query = """
        SELECT 
            'table_clients' as table_name, 
            NULL as max_date,  -- В этой таблице нет даты
            COUNT(*) as total_rows 
        FROM cdm.table_clients
        UNION ALL
        SELECT 
            'table_page_views', 
            MAX(hit_ts::date), 
            COUNT(*) 
        FROM cdm.table_page_views
        UNION ALL
        SELECT 
            'table_visits', 
            MAX(visit_date), 
            COUNT(*) 
        FROM cdm.table_visits;
        """
        
        # Выполнение команды psql
        cmd = [
            'psql',
            '-h', 'localhost',
            '-U', 'postgres',
            '-d', 'pfserver',
            '-c', query
        ]
        
        # Добавляем пароль из переменной окружения
        env = {'PGPASSWORD': 'pfserverivo'}
        
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            env=env
        )
        
        if result.returncode == 0:
            logging.info("Data check results:\n" + result.stdout)
        else:
            logging.error("Error checking data:\n" + result.stderr)
            
    except Exception as e:
        logging.error(f"Error in check_data: {str(e)}")

if __name__ == "__main__":
    logging.info("Starting CDM data check")
    check_data()
    logging.info("Check completed")
