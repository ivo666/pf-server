import psycopg2
from psycopg2 import sql
import configparser
from datetime import datetime, timedelta

def load_config():
    """Загружает конфигурацию из config.ini"""
    config = configparser.ConfigParser()
    config.read('config.ini')
    return config

def get_db_connection(config):
    """Создает подключение к PostgreSQL"""
    try:
        conn = psycopg2.connect(
            host=config['Database']['HOST'],
            database=config['Database']['DATABASE'],
            user=config['Database']['USER'],
            password=config['Database']['PASSWORD'],
            port=config['Database']['PORT']
        )
        return conn
    except Exception as e:
        print(f"Ошибка подключения к БД: {e}")
        return None

def get_yesterday_date():
    """Возвращает дату вчерашнего дня в формате YYYY-MM-DD"""
    return (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

def transfer_yesterday_data():
    config = load_config()
    yesterday = get_yesterday_date()
    
    # SQL-запрос для выборки данных за вчерашний день
    source_query = """
    SELECT yds.date
         , yds.campaign_id
         , yds.campaign_name
         , yds.impressions
         , yds.clicks
         , CASE 
             WHEN yds.clicks = 0 THEN 0
             ELSE ROUND(yds.cost * 1.0 / yds.clicks, 2)
           END AS day_click_cost
    FROM row.yandex_direct_stats yds
    WHERE yds.date = %s
    """
    
    # SQL-запрос для проверки существующих данных за вчера
    check_query = """
    SELECT COUNT(*) 
    FROM ppl.ya_direct_stats 
    WHERE date = %s
    """
    
    # SQL-запрос для вставки данных
    insert_query = """
    INSERT INTO ppl.ya_direct_stats 
    (date, campaign_id, campaign_name, impressions, clicks, day_click_cost)
    VALUES (%s, %s, %s, %s, %s, %s)
    """
    
    conn = None
    try:
        conn = get_db_connection(config)
        if conn is None:
            return
        
        with conn.cursor() as cursor:
            print(f"Начинаем обработку данных за {yesterday}")
            
            # Проверяем, есть ли уже данные за вчера в целевой таблице
            cursor.execute(check_query, (yesterday,))
            existing_count = cursor.fetchone()[0]
            
            if existing_count > 0:
                print(f"Данные за {yesterday} уже существуют в целевой таблице. Пропускаем вставку.")
                return
                
            # Получаем данные из исходной таблицы за вчерашний день
            cursor.execute(source_query, (yesterday,))
            rows = cursor.fetchall()
            
            if not rows:
                print(f"Нет данных в источнике за {yesterday}")
                return
                
            print(f"Получено {len(rows)} записей из row.yandex_direct_stats за {yesterday}")
            
            # Вставляем данные в целевую таблицу
            cursor.executemany(insert_query, rows)
            conn.commit()
            print(f"Успешно загружено {len(rows)} записей в ppl.ya_direct_stats за {yesterday}")
            
    except Exception as e:
        print(f"Ошибка при переносе данных: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()
            print("Подключение к БД закрыто")

if __name__ == "__main__":
    transfer_yesterday_data()
