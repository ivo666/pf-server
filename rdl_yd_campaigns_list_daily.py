import pandas as pd
import gspread
from sqlalchemy import create_engine, types, text
from pathlib import Path
import datetime
import configparser
import logging
import sys

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

def create_table_if_not_exists(engine):
    """Создает таблицу, если она не существует"""
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS rdl.yd_campaigns_list (
        campaign VARCHAR,
        utm_campaign VARCHAR,
        content_id VARCHAR PRIMARY KEY,
        content_profit VARCHAR,
        start_date DATE,
        last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """
    with engine.begin() as conn:
        conn.execute(text(create_table_sql))
        logging.info("Проверка/создание таблицы выполнена")

def update_data_in_db(engine, df):
    """Обновляет данные в таблице, используя content_id как ключ"""
    # Добавляем метку времени обновления
    df['last_update'] = datetime.datetime.now()
    
    # Получаем список существующих content_id
    with engine.connect() as conn:
        existing_ids = pd.read_sql('SELECT content_id FROM rdl.yd_campaigns_list', conn)['content_id'].tolist()
    
    # Разделяем данные на новые и обновляемые
    new_records = df[~df['content_id'].isin(existing_ids)]
    update_records = df[df['content_id'].isin(existing_ids)]
    
    logging.info(f"Новых записей для добавления: {len(new_records)}")
    logging.info(f"Записей для обновления: {len(update_records)}")
    
    # Загружаем новые записи
    if not new_records.empty:
        with engine.begin() as connection:
            new_records.to_sql(
                'yd_campaigns_list',
                connection,
                schema='rdl',
                if_exists='append',
                index=False,
                dtype={
                    'campaign': types.String(),
                    'utm_campaign': types.String(),
                    'content_id': types.String(),
                    'content_profit': types.String(),
                    'start_date': types.Date(),
                    'last_update': types.TIMESTAMP()
                },
                method='multi',
                chunksize=100
            )
        logging.info("Новые записи успешно добавлены")
    
    # Обновляем существующие записи
    if not update_records.empty:
        with engine.begin() as connection:
            for _, row in update_records.iterrows():
                update_sql = """
                UPDATE rdl.yd_campaigns_list
                SET 
                    campaign = :campaign,
                    utm_campaign = :utm_campaign,
                    content_profit = :content_profit,
                    start_date = :start_date,
                    last_update = :last_update
                WHERE content_id = :content_id
                """
                params = {
                    'campaign': row['campaign'],
                    'utm_campaign': row['utm_campaign'],
                    'content_profit': row['content_profit'],
                    'start_date': row['start_date'],
                    'last_update': row['last_update'],
                    'content_id': row['content_id']
                }
                connection.execute(text(update_sql), params)
        logging.info("Существующие записи успешно обновлены")

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
        
        # Оставляем только колонки, которые есть в целевой таблице
        target_columns = ['campaign', 'utm_campaign', 'content_id', 'content_profit', 'start_date']
        df = df[[col for col in df.columns if col in target_columns]]

        # Проверка наличия обязательного поля content_id
        if 'content_id' not in df.columns:
            raise Exception("Обязательное поле content_id отсутствует в данных")
        
        # Удаление дубликатов по content_id (оставляем последнюю запись)
        df = df.drop_duplicates(subset=['content_id'], keep='last')

        # Преобразование даты
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
            
            # Проверка подключения и создание таблицы если нужно
            with engine.connect() as conn:
                logging.info("Подключение к PostgreSQL успешно")
                create_table_if_not_exists(engine)
                
        except Exception as e:
            raise Exception(f"Ошибка PostgreSQL: {str(e)}")

        # 4. Обновление данных в таблице
        update_data_in_db(engine, df)
            
        # 5. Проверка результатов
        with engine.connect() as conn:
            result = pd.read_sql('SELECT * FROM rdl.yd_campaigns_list ORDER BY last_update DESC LIMIT 5', conn)
            logging.info(f"Последние 5 записей:\n{result.to_string()}")
            count = pd.read_sql('SELECT COUNT(*) as count FROM rdl.yd_campaigns_list', conn)['count'].iloc[0]
            logging.info(f"Всего записей в таблице: {count}")
            updated_count = pd.read_sql(
                "SELECT COUNT(*) as count FROM rdl.yd_campaigns_list WHERE last_update >= NOW() - INTERVAL '1 hour'", 
                conn
            )['count'].iloc[0]
            logging.info(f"Записей обновлено/добавлено в текущем запуске: {updated_count}")
                
    except Exception as e:
        logging.error(f"Критическая ошибка: {str(e)}")
        sys.exit(1)
    finally:
        if 'engine' in locals():
            engine.dispose()
            logging.info("Соединение с PostgreSQL закрыто")

if __name__ == "__main__":
    logging.info("=== Начало выполнения скрипта ===")
    main()
    logging.info("=== Скрипт успешно завершен ===")
