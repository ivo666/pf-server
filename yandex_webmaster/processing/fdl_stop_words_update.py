import pandas as pd
import gspread
from sqlalchemy import create_engine, text
import logging
import sys
import configparser

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
    try:
        gc = gspread.service_account(filename=cfg['gsheets']['creds_path'])
        spreadsheet = gc.open(cfg['gsheets']['spreadsheet'])
        worksheet = spreadsheet.worksheet(cfg['gsheets']['worksheet'])
        
        # Получаем все значения из первой колонки
        all_values = worksheet.col_values(1)
        
        # Удаляем заголовок если есть и пустые значения
        stop_words = []
        for word in all_values:
            if word and str(word).strip():
                cleaned_word = str(word).strip()
                # Пропускаем заголовок
                if cleaned_word.lower() != 'stop_word':
                    stop_words.append(cleaned_word)
            
        logging.info(f"Получено значений из Google Sheets: {len(all_values)}, после очистки: {len(stop_words)}")
        return stop_words
        
    except Exception as e:
        logging.error(f"Ошибка при получении данных из Google Sheets: {str(e)}")
        raise

def prepare_stop_words_df(stop_words_list):
    """Подготовка DataFrame со стоп-словами"""
    if not stop_words_list:
        return pd.DataFrame(columns=['stop_word'])
    
    df = pd.DataFrame({'stop_word': stop_words_list})
    
    # Удаляем дубликаты и пустые строки
    df = df.drop_duplicates().dropna()
    df['stop_word'] = df['stop_word'].str.strip()
    df = df[df['stop_word'] != '']
    
    if not df.empty:
        logging.info(f"Примеры стоп-слов: {df['stop_word'].head().tolist()}")
    return df

def replace_stop_words_in_db(engine, df):
    """Полная замена стоп-слов в базе данных"""
    try:
        with engine.begin() as conn:
            conn.execute(text("TRUNCATE TABLE fdl.stop_words"))
            
            if not df.empty:
                df.to_sql('stop_words', conn, schema='fdl', if_exists='append', index=False)
                logging.info(f"Успешно загружено {len(df)} стоп-слов в БД")
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
        
        # Проверка подключения
        with engine.connect() as conn:
            logging.info("Подключение к PostgreSQL успешно")
        
        # Загрузка в БД
        loaded_count = replace_stop_words_in_db(engine, df)
        logging.info(f"Итог: загружено {loaded_count} стоп-слов")

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
