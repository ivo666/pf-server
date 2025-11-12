import pandas as pd
import gspread
from sqlalchemy import create_engine, text
import logging
import sys

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

def load_config():
    """Загрузка конфигурации из config.ini"""
    config = configparser.ConfigParser()
    config.read('config.ini')
    return {
        'db': dict(config['Database']),
        'gsheets': {
            'creds_path': config['GoogleSheets']['CREDENTIALS_PATH'],
            'spreadsheet': "ProfiFiltr_webmaster_stop_words",
            'worksheet': "stop_words"
        }
    }

def get_stop_words_from_sheets(cfg):
    """Получение стоп-слов из Google Sheets"""
    gc = gspread.service_account(filename=cfg['gsheets']['creds_path'])
    worksheet = gc.open(cfg['gsheets']['spreadsheet']).worksheet(cfg['gsheets']['worksheet'])
    
    # Получаем все значения из первой колонки
    stop_words = worksheet.col_values(1)
    
    # Удаляем заголовок если есть и пустые значения
    stop_words = [word.strip() for word in stop_words if word.strip()]
    if stop_words and stop_words[0].lower() == 'stop_word':
        stop_words = stop_words[1:]
    
    return stop_words

def prepare_stop_words_df(stop_words_list):
    """Подготовка DataFrame со стоп-словами"""
    df = pd.DataFrame({'stop_word': stop_words_list})
    
    # Удаляем дубликаты и пустые строки
    df = df.drop_duplicates().dropna()
    df['stop_word'] = df['stop_word'].str.strip()
    df = df[df['stop_word'] != '']
    
    return df

def replace_stop_words_in_db(engine, df):
    """Полная замена стоп-слов в базе данных"""
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE fdl.stop_words"))
        
        if not df.empty:
            df.to_sql('stop_words', conn, schema='fdl', if_exists='append', index=False)
        
    return len(df)

def main():
    try:
        # Загрузка конфигурации
        cfg = load_config()
        logging.info("Конфигурация загружена")

        # Получение данных из Google Sheets
        stop_words_list = get_stop_words_from_sheets(cfg)
        logging.info(f"Получено стоп-слов из Google Sheets: {len(stop_words_list)}")

        # Подготовка данных
        df = prepare_stop_words_df(stop_words_list)
        logging.info(f"После очистки осталось стоп-слов: {len(df)}")

        if df.empty:
            logging.warning("Нет данных для загрузки")
            return

        # Подключение к БД и загрузка данных
        engine = create_engine(
            f"postgresql+psycopg2://{cfg['db']['user']}:{cfg['db']['password']}@"
            f"{cfg['db']['host']}:{cfg['db']['port']}/{cfg['db']['database']}"
        )
        
        with engine.connect() as conn:
            logging.info("Подключение к PostgreSQL успешно")
        
        # Загрузка в БД
        loaded_count = replace_stop_words_in_db(engine, df)
        logging.info(f"Успешно загружено стоп-слов в БД: {loaded_count}")

    except Exception as e:
        logging.error(f"Ошибка: {str(e)}")
        sys.exit(1)
    finally:
        if 'engine' in locals():
            engine.dispose()

if __name__ == "__main__":
    logging.info("=== Начало загрузки стоп-слов ===")
    main()
    logging.info("=== Скрипт завершен ===")
