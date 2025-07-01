import psycopg2
from psycopg2 import sql
import configparser
import os

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

def transfer_data():
    config = load_config()
    
    # SQL-запрос для выборки и преобразования данных
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
            print("Подключение к БД установлено")
            
            # Получаем данные из исходной таблицы
            cursor.execute(source_query)
            rows = cursor.fetchall()
            print(f"Получено {len(rows)} записей из row.yandex_direct_stats")
            
            # Вставляем данные в целевую таблицу
            cursor.executemany(insert_query, rows)
            conn.commit()
            print(f"Успешно загружено {len(rows)} записей в ppl.ya_direct_stats")
            
    except Exception as e:
        print(f"Ошибка при переносе данных: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()
            print("Подключение к БД закрыто")

if __name__ == "__main__":
    transfer_data()
