# import pandas as pd
from configparser import ConfigParser
import psycopg2
from psycopg2 import sql

# Читаем config
config = ConfigParser()
config.read('config.ini')

# Настраиваем подключение
db_connect = {
    'host': config['Database']['HOST'],
    'database': config['Database']['DATABASE'],
    'user': config['Database']['USER'],
    'password': config['Database']['PASSWORD'],
    'port': config['Database']['PORT']
}

# Содаем переменную-sql-запрос чтения данных
query = """
select date
 , campaign_id
 , cn.utm_campaign
 , ad_id as content_id
 , cn.content_profit as content_benefit
 , impressions
 , clicks
 , round((cost * 1.0 / 1000000) / clicks, 2) as click_cost
 , avg_click_position as click_position
 , device
 , location_of_presence_id
 , match_type
 , slot
from rdl.yd_ad_performance_report yapr
join (select campaign
     , utm_campaign
     , content_id
     , content_profit
    from rdl.yd_campaigns_list ycl
    order by campaign, utm_campaign
) cn on cn.campaign = yapr.campaign_name and cn.content_id = yapr.ad_id::text
"""

# Настраиваем подключение


def connection():
    try:
        conn = None
        conn = psycopg2.connect(**db_connect)
        print('Успешное подключение к постгрес')

        # Создание курсора с использованием конструкции with
        # with - это менеджер контекста в Python
        # Он гарантирует, что курсор cur будет автоматически закрыт после выхода из блока, даже если возникнет исключение
        # Это заменяет необходимость вручную вызывать cur.close()
        # Также обеспечивает более чистый и безопасный код

        with conn.cursor() as cur:
            cur.execute(query)
            data = cur.fetchall()

            # Вся строка представляет собой list comprehension, который создает список имён столбцов
            colnames = [desc[0] for desc in cur.description]
            print(colnames)

            # Подготавливаем запрос на вставку
            # sql.SQL() - безопасный способ создания SQL-запросов в psycopg2
            # {} - плейсхолдеры для динамических частей запроса
            # Первый .format() вставляет список столбцов:
            # map(sql.Identifier, colnames) преобразует каждое имя столбца в безопасный идентификатор
            # sql.SQL(', ').join() соединяет их через запятую
            # Второй .format() вставляет плейсхолдеры для значений:
            # [sql.Placeholder()] * len(colnames) создаёт список плейсхолдеров (по одному на каждый столбец)
            # sql.SQL(', ').join() соединяет их через запятую

            insert_query = sql.SQL("""
                INSERT INTO ppl.yd_stats ({})
                VALUES ({})
                ON CONFLICT ON CONSTRAINT yd_stats_unique 
                DO UPDATE SET 
                    content_benefit = EXCLUDED.content_benefit,
                    impressions = EXCLUDED.impressions,
                    clicks = EXCLUDED.clicks,
                    click_cost = EXCLUDED.click_cost,
                    click_position = EXCLUDED.click_position
                """).format(
                    sql.SQL(', ').join(map(sql.Identifier, colnames)),
                    sql.SQL(', ').join([sql.Placeholder()] * len(colnames)))

            # Вставляем данные
            cur.executemany(insert_query, data)

            # Подтверждает все изменения, сделанные в текущей транзакции
            # Без commit() все изменения будут отменены при закрытии соединения
            # В PostgreSQL изменения не видны другим соединениям до вызова commit()
            # Расположен после executemany, чтобы гарантировать, что все данные вставлены успешно
            conn.commit()

            print(f"Успешно вставлено {len(data)} записей")

        # cur = conn.cursor()
        # # Выполнение запроса, вычисляем одно значение
        # cur.execute('SELECT count(*) FROM yandex_metrika_visits')
        # res = cur.fetchone()
        # print(f'Результат запроса: {res[0]}')

    except (psycopg2.DatabaseError, Exception) as e:
        print(f"Ошибка: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()
            print("Соединение закрыто")


connection()
