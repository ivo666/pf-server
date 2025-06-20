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
         'yandex_metrika_visits' as table, 
         MAX(date) as max_date, 
         COUNT(*) filter (WHERE date = CURRENT_DATE - INTERVAL '1 day') as yesterday_count
       FROM yandex_metrika_visits
       UNION ALL
       SELECT 
        'yandex_metrika_hits', 
         MAX(date_time::date), 
         COUNT(*) filter (WHERE date_time::date = CURRENT_DATE - INTERVAL '1 day')
       FROM yandex_metrika_hits;
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
