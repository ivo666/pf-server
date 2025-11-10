from configparser import ConfigParser
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_batch

def get_db_config():
    """Чтение конфигурации БД из файла"""
    config = ConfigParser()
    config.read('config.ini')
    return {
        'host': config['Database']['HOST'],
        'database': config['Database']['DATABASE'],
        'user': config['Database']['USER'],
        'password': config['Database']['PASSWORD'],
        'port': config['Database']['PORT']
    }

def get_data_query():
    """SQL-запрос для получения данных"""
    return """
    SELECT date,
           campaign_id,
           cn.utm_campaign,
           ad_id AS content_id,
           cn.content_profit AS content_benefit,
           impressions,
           clicks,
           ROUND((cost * 1.0 / 1000000) / NULLIF(clicks, 0), 2) AS click_cost,
           avg_click_position AS click_position,
           device,
           location_of_presence_id,
           match_type,
           slot, 
           case
           	when cn.utm_campaign = 'FVCART_OBSHCHAYA_POISK' then 'obshchaya'
           	when cn.utm_campaign in ('FVCART_AB_SSH_POISK', 'FVCART_S_G_POISK', 'FVCART_В_G_POISK', 'FVCART_AA_SS_POISK') then 'group'
           	else 'product'
           end as campaign_type 
    FROM rdl.yd_ad_performance_report yapr
    JOIN (SELECT campaign,
                 utm_campaign,
                 content_id,
                 content_profit
          FROM rdl.yd_campaigns_list ycl
          ORDER BY campaign, utm_campaign) cn 
      ON cn.campaign = yapr.campaign_name AND cn.content_id = yapr.ad_id::text
    """

def connection():
    """Основная функция для подключения и обработки данных"""
    conn = None
    try:
        # Подключаемся к БД
        conn = psycopg2.connect(**get_db_config())
        print('Успешное подключение к PostgreSQL')

        with conn.cursor() as cur:
            # Получаем данные
            cur.execute(get_data_query())
            data = cur.fetchall()
            
            if not data:
                print("Нет данных для вставки")
                return
            
            colnames = [desc[0] for desc in cur.description]
            print(f"Колонки: {colnames}")

            # 1. Очищаем целевую таблицу
            truncate_query = "TRUNCATE TABLE ppl.yd_stats"
            cur.execute(truncate_query)
            print("Целевая таблица очищена")
            
            # 2. Вставляем новые данные
            insert_query = sql.SQL("""
                INSERT INTO ppl.yd_stats ({})
                VALUES ({})
                """).format(
                    sql.SQL(', ').join(map(sql.Identifier, colnames)),
                    sql.SQL(', ').join([sql.Placeholder()] * len(colnames)))

            # Используем execute_batch для эффективной пакетной вставки
            execute_batch(cur, insert_query, data, page_size=1000)
            
            conn.commit()
            print(f"Успешно вставлено {len(data)} записей")

    except psycopg2.DatabaseError as e:
        print(f"Ошибка базы данных: {e}")
        if conn:
            conn.rollback()
    except Exception as e:
        print(f"Неожиданная ошибка: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()
            print("Соединение закрыто")

if __name__ == "__main__":
    connection()
