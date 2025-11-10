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

def get_existing_ids(engine):
    """Получает список существующих content_id из базы"""
    with engine.connect() as conn:
        try:
            existing_ids = pd.read_sql('SELECT content_id FROM rdl.yd_campaigns_list', conn)['content_id'].tolist()
            # Преобразуем все ID в строки для единообразия сравнения
            return [str(id) for id in existing_ids]
        except Exception as e:
            logging.error(f"Ошибка при получении существующих ID: {str(e)}")
            return []

def update_data_in_db(engine, df):
    """Обновляет данные в таблице, используя content_id как ключ"""
    try:
        # Преобразуем content_id в строки для сравнения
        df['content_id'] = df['content_id'].astype(str)
        
        # Добавляем метку времени обновления
        df['last_update'] = datetime.datetime.now()
        
        # Получаем список существующих content_id
        existing_ids = get_existing_ids(engine)
        
        if existing_ids is None:
            existing_ids = []
        
        # Разделяем данные на новые и обновляемые
        new_records = df[~df['content_id'].isin(existing_ids)]
        update_records = df[df['content_id'].isin(existing_ids)]
        
        logging.info(f"Новых записей для добавления: {len(new_records)}")
        logging.info(f"Записей для обновления: {len(update_records)}")
        
        # Загружаем новые записи по одной с обработкой возможных дубликатов
        if not new_records.empty:
            with engine.begin() as connection:
                for _, row in new_records.iterrows():
                    try:
                        insert_sql = """
                        INSERT INTO rdl.yd_campaigns_list 
                            (campaign, utm_campaign, content_id, content_profit, start_date, last_update)
                        VALUES 
                            (:campaign, :utm_campaign, :content_id, :content_profit, :start_date, :last_update)
                        ON CONFLICT (content_id) DO NOTHING
                        """
                        params = {
                            'campaign': row['campaign'],
                            'utm_campaign': row['utm_campaign'],
                            'content_id': row['content_id'],
                            'content_profit': row['content_profit'],
                            'start_date': row['start_date'],
                            'last_update': row['last_update']
                        }
                        connection.execute(text(insert_sql), params)
                    except Exception as e:
                        logging.warning(f"Не удалось вставить запись {row['content_id']}: {str(e)}")
                        continue
            logging.info("Новые записи успешно обработаны")
        
        # Обновляем существующие записи
        if not update_records.empty:
            with engine.begin() as connection:
                for _, row in update_records.iterrows():
                    try:
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
                    except Exception as e:
                        logging.warning(f"Не удалось обновить запись {row['content_id']}: {str(e)}")
                        continue
            logging.info("Существующие записи успешно обновлены")
            
        return True
    except Exception as e:
        logging.error(f"Ошибка при обновлении данных: {str(e)}")
        return False

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
        update_success = update_data_in_db(engine, df)
            
        # 5. Проверка результатов
        if update_success:
            with engine.connect() as conn:
                # Получаем статистику по последним изменениям
                stats = pd.read_sql("""
                    SELECT 
                        COUNT(*) as total_count,
                        SUM(CASE WHEN last_update >= NOW() - INTERVAL '1 hour' THEN 1 ELSE 0 END) as updated_count
                    FROM rdl.yd_campaigns_list
                """, conn)
                
                logging.info(f"Всего записей в таблице: {stats['total_count'].iloc[0]}")
                logging.info(f"Записей обновлено/добавлено в текущем запуске: {stats['updated_count'].iloc[0]}")
                
                # Выводим примеры измененных записей
                changed_records = pd.read_sql("""
                    SELECT content_id, campaign, last_update 
                    FROM rdl.yd_campaigns_list 
                    WHERE last_update >= NOW() - INTERVAL '1 hour'
                    ORDER BY last_update DESC 
                    LIMIT 5
                """, conn)
                
                if not changed_records.empty:
                    logging.info("Примеры измененных записей:\n" + changed_records.to_string(index=False))
                else:
                    logging.info("Нет измененных записей в этом запуске")
                
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
