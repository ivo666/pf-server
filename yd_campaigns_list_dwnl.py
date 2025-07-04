import pandas as pd
import gspread
from sqlalchemy import create_engine, types
from pathlib import Path
import datetime
import configparser
import logging

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('yd_campaigns_list.log'),
        logging.StreamHandler()
    ]
)

def load_config():
    """Загрузка конфигурации из config.ini"""
    config = configparser.ConfigParser()
    config.read('config.ini')
    
    if not all(section in config for section in ['Database', 'GoogleSheets']):
        raise ValueError("Неверная структура config.ini")
    
    return {
        'db': {
            'host': config['Database']['HOST'],
            'database': config['Database']['DATABASE'],
            'user': config['Database']['USER'],
            'password': config['Database']['PASSWORD'],
            'port': config['Database']['PORT']
        },
        'gsheets': {
            'creds_path': config['GoogleSheets']['CREDENTIALS_PATH'],
            'spreadsheet': "ProfiFilter_cpc_fvkart",
            'worksheet': "Campaigns"
        }
    }

def main():
    try:
        # Загрузка конфигурации
        cfg = load_config()
        logging.info("Конфигурация загружена")

        # 1. Получение данных из Google Sheets
        try:
            gc = gspread.service_account(filename=cfg['gsheets']['creds_path'])
            sh = gc.open(cfg['gsheets']['spreadsheet'])
            worksheet = sh.worksheet(cfg['gsheets']['worksheet'])
            data = worksheet.get_all_records()
            df = pd.DataFrame(data)
            logging.info(f"Данные получены. Записей: {len(df)}")
        except Exception as e:
            raise Exception(f"Ошибка Google Sheets: {str(e)}")

        # 2. Подготовка данных
        df.columns = df.columns.str.replace('.', '_').str.lower()

        # Преобразование только даты
        if 'start_date' in df.columns:
            df['start_date'] = pd.to_datetime(df['start_date'], format='%d.%m.%Y', errors='coerce')
        else:
            logging.warning("Столбец start_date отсутствует")

        # 3. Подключение к PostgreSQL
        try:
            engine = create_engine(
                f"postgresql+psycopg2://{cfg['db']['user']}:{cfg['db']['password']}@"
                f"{cfg['db']['host']}:{cfg['db']['port']}/{cfg['db']['database']}"
            )
            
            # Проверка подключения
            with engine.connect() as conn:
                logging.info("Подключение к PostgreSQL успешно")
        except Exception as e:
            raise Exception(f"Ошибка PostgreSQL: {str(e)}")

        # 4. Загрузка данных
        try:
            with engine.begin() as connection:
                df.to_sql(
                    'yd_campaigns_list',
                    connection,
                    if_exists='append',
                    index=False,
                    dtype={
                        'campaign': types.String(),
                        'utm_campaign': types.String(),
                        'content_id': types.String(),
                        'content_profit': types.String(),
                        'start_date': types.Date(),
                        'comments_date_17_06_2025': types.String(),
                        'comments_date_24_06_2025': types.String(),
                        'comments_date_30_06_25': types.String(),
                        'comments_date_02_07_2025': types.String()
                    },
                    method='multi',
                    chunksize=100
                )
            
            # Проверка загруженных данных (без сортировки по id)
            with engine.connect() as conn:
                result = pd.read_sql("SELECT * FROM yd_campaigns_list LIMIT 5", conn)
                logging.info(f"Первые 5 записей:\n{result.to_string()}")
                count = pd.read_sql("SELECT COUNT(*) as count FROM yd_campaigns_list", conn)['count'].iloc[0]
                logging.info(f"Всего записей в таблице: {count}")
                
        except Exception as e:
            raise Exception(f"Ошибка загрузки: {str(e)}")
            
    except Exception as e:
        logging.error(f"Критическая ошибка: {str(e)}")
        raise
    finally:
        if 'engine' in locals():
            engine.dispose()
            logging.info("Соединение с PostgreSQL закрыто")

if __name__ == "__main__":
    main()
