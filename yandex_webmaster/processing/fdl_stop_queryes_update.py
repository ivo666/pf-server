import pandas as pd
import gspread
from sqlalchemy import create_engine, text
import logging
import sys
import os
from dotenv import load_dotenv

# Загрузка переменных окружения из .env файла
env_path = '/home/pf-server/yandex_webmaster/config/.env'
load_dotenv(env_path)

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

def load_config():
    """Загрузка конфигурации из переменных окружения"""
    # Проверяем, что переменные загружены
    required_vars = [
        'DB_HOST', 'DB_PORT', 'DB_NAME', 'DB_USER', 'DB_PASSWORD',
        'GOOGLE_CREDENTIALS_PATH', 'GOOGLE_SPREADSHEET_NAME', 'GOOGLE_WORKSHEET_NAME'
    ]
    
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        logging.error(f"Отсутствуют обязательные переменные окружения: {missing_vars}")
        raise ValueError(f"Отсутствуют переменные в .env файле: {missing_vars}")
    
    logging.info("Все обязательные переменные окружения найдены")
    
    return {
        'db': {
            'host': os.getenv('DB_HOST'),
            'port': os.getenv('DB_PORT', '5432'),
            'database': os.getenv('DB_NAME'),
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD')
        },
        'gsheets': {
            'creds_path': os.getenv('GOOGLE_CREDENTIALS_PATH'),
            'spreadsheet': os.getenv('GOOGLE_SPREADSHEET_NAME'),
            'worksheet': os.getenv('GOOGLE_WORKSHEET_NAME')
        }
    }

def get_stop_queries_from_sheets(cfg):
    """Получение стоп-запросов из Google Sheets"""
    try:
        logging.info(f"Используем credentials: {cfg['gsheets']['creds_path']}")
        logging.info(f"Ищем таблицу: {cfg['gsheets']['spreadsheet']}")
        logging.info(f"Ищем лист: {cfg['gsheets']['worksheet']}")
        
        gc = gspread.service_account(filename=cfg['gsheets']['creds_path'])
        
        # Получаем список всех таблиц для диагностики
        all_spreadsheets = gc.openall()
        logging.info(f"Доступные таблицы: {[sh.title for sh in all_spreadsheets]}")
        
        spreadsheet = gc.open(cfg['gsheets']['spreadsheet'])
        worksheet = spreadsheet.worksheet(cfg['gsheets']['worksheet'])
        
        # Получаем все значения из первой колонки
        all_values = worksheet.col_values(1)
        logging.info(f"Сырые данные из колонки: {all_values}")
        
        # Очистка данных
        stop_queries = []
        for query in all_values:
            if query and str(query).strip():
                cleaned_query = str(query).strip()
                if cleaned_query.lower() != 'query':
                    stop_queries.append(cleaned_query)
            
        logging.info(f"После очистки: {stop_queries}")
        return stop_queries
        
    except Exception as e:
        logging.error(f"Ошибка при получении данных из Google Sheets: {str(e)}")
        raise

def prepare_stop_queries_df(stop_queries_list):
    """Подготовка DataFrame со стоп-запросами"""
    if not stop_queries_list:
        return pd.DataFrame(columns=['query'])
    
    df = pd.DataFrame({'query': stop_queries_list})
    
    # Удаляем дубликаты и пустые строки
    df = df.drop_duplicates().dropna()
    df['query'] = df['query'].str.strip()
    df = df[df['query'] != '']
    
    if not df.empty:
        logging.info(f"Примеры стоп-запросов: {df['query'].head().tolist()}")
    return df

def replace_stop_queries_in_db(engine, df):
    """Полная замена стоп-запросов в базе данных"""
    try:
        with engine.begin() as conn:
            conn.execute(text("TRUNCATE TABLE fdl.webm_stop_queryes"))
            
            if not df.empty:
                df.to_sql('webm_stop_queryes', conn, schema='fdl', if_exists='append', index=False)
                logging.info(f"Успешно загружено {len(df)} стоп-запросов в БД")
            else:
                logging.warning("Нет данных для загрузки в БД")
                
        return len(df)
    except Exception as e:
        logging.error(f"Ошибка при работе с БД: {str(e)}")
        raise

def main():
    try:
        # Загрузка конфигурации
        cfg = load_config()
        logging.info("Конфигурация загружена")

        # Получение данных из Google Sheets
        stop_queries_list = get_stop_queries_from_sheets(cfg)
        logging.info(f"Получено стоп-запросов из Google Sheets: {len(stop_queries_list)}")

        # Подготовка данных
        df = prepare_stop_queries_df(stop_queries_list)
        logging.info(f"После очистки осталось стоп-запросов: {len(df)}")

        if df.empty:
            logging.warning("Нет данных для загрузки")
            return

        # Подключение к БД и загрузка данных
        engine = create_engine(
            f"postgresql+psycopg2://{cfg['db']['user']}:{cfg['db']['password']}@"
            f"{cfg['db']['host']}:{cfg['db']['port']}/{cfg['db']['database']}"
        )
        
        # Проверка подключения
        with engine.connect() as conn:
            logging.info("Подключение к PostgreSQL успешно")
        
        # Загрузка в БД
        loaded_count = replace_stop_queries_in_db(engine, df)
        logging.info(f"Итог: загружено {loaded_count} стоп-запросов")

    except Exception as e:
        logging.error(f"Ошибка: {str(e)}")
        sys.exit(1)
    finally:
        if 'engine' in locals():
            engine.dispose()

if __name__ == "__main__":
    logging.info("=== Начало загрузки стоп-запросов ===")
    main()
    logging.info("=== Скрипт завершен ===")
