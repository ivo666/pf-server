from configparser import ConfigParser
import psycopg2
from datetime import datetime
import sys

def load_config():
    """Загрузка конфигурации из файла"""
    config = ConfigParser()
    config.read('config.ini')
    return {
        'host': config['Database']['HOST'],
        'database': config['Database']['DATABASE'],
        'user': config['Database']['USER'],
        'password': config['Database']['PASSWORD'],
        'port': config['Database']['PORT']
    }

def get_queries():
    """Возвращает SQL-запросы для обновления таблиц"""
    return {
        'clients': """
            TRUNCATE TABLE cdm.table_clients;
            INSERT INTO cdm.table_clients
            WITH _source AS (
                SELECT yv.client_id,
                       MAX(yv.region_city) OVER(PARTITION BY yv.client_id) AS city,
                       MAX(yv.browser) OVER (PARTITION BY yv.client_id) AS browser,
                       MAX(physical_screen_width) OVER (PARTITION BY yv.client_id) AS screen_width,
                       MAX(physical_screen_height) OVER(PARTITION BY yv.client_id) AS screen_height
                FROM rdl.ym_visits yv 
                RIGHT JOIN (
                    SELECT DISTINCT client_id
                    FROM rdl.ym_visits 
                    WHERE region_country = 'Russia' 
                      AND device_category = '1'
                      AND (referer not in ('metrika.yandex.ru', 'klaue.cloudbpm.ru') OR referal_source != 'metrika.yandex.ru')
                      AND client_id NOT IN ('1742907795159016963', '1690275728585926726', '1745571831246112348', '1660561659524790881', '171759016385815372',
                      '1739452086606602606', '1744210585372274818', '1745570119620709361', '1745570221463237118', '1745571778695559054', '1745571831246112348',
                      '1647584953508775973', '1690275728585926726', '1744718065239449137', '1745414826627499142', '174850912228603235', '175031424839635669', 
                      '1750314250218877332', '1750314264447286457', '1750668704615626247', '1750668735991801723', '1750689878351145538', '1750689904452339531',
                      '1750690053505587543')
                ) yvc ON yv.client_id = yvc.client_id
            )
            SELECT client_id, city, browser, screen_width, screen_height 
            FROM _source
            GROUP BY 1, 2, 3, 4, 5;
        """,
        'hits': """
            TRUNCATE TABLE cdm.table_hits;
            INSERT INTO cdm.table_hits
            WITH a AS (
                SELECT thp.*, tw.watch_id AS w_watch_id, tw.visit_id, tw.client_id AS w_client_id
                FROM rdl.ym_hits_params thp INNER JOIN (
                    SELECT tv.client_id, visit_id, UNNEST(watch_ids) AS watch_id 
                    FROM rdl.ym_visits tv INNER JOIN (SELECT client_id FROM cdm.table_clients) tc
                    ON tv.client_id = tc.client_id 
                    WHERE tv.date >= '2025-06-01'
                ) tw
                ON thp.watch_id = tw.watch_id 
                WHERE (thp.url LIKE ('https://profi-filter.ru/%') AND is_page_view = '1') 
                OR thp.url LIKE ('goal://profi-filter.ru/pf_event')
                AND thp.button_location != 'mob_menu'
            ), b AS (
                SELECT visit_id, watch_id, w_client_id AS client_id, date_time, title, url, 
                       is_page_view, event_category, event_action, event_label, 
                       button_location, event_content, event_context, action_group, page_path
                FROM a 
            )
            SELECT * FROM b;
        """,
        'visits': """
            TRUNCATE TABLE cdm.table_visits;
            INSERT INTO cdm.table_visits
            WITH _source AS (
                SELECT t.client_id, t.visit_id, t."date", t.date_time, t.is_new_user
                       , split_part(t.start_url, '?', 1) as start_url
                       , split_part(t.end_url, '?', 1) as end_url
                       , t2.page_view, t.visit_duration
                       , case
                         	when search_engine_root = 'yandex' then 'yandex'
                         	when search_engine_root = 'google' then 'google'
                         	when search_engine_root = 'bing' then 'bing'
                         	when search_engine_root = 'yahoo' then 'yahoo'
                         	when search_engine_root = 'rambler' then 'rambler'
                         	when search_engine_root = 'ecosia' then 'ecosia'
                         	when search_engine_root = 'duckduckgo' then 'duckduckgo'
                            when referer like any (array ['https://yandex.ru/images/search%', 'https://yandex.ru/maps/%']) then 'yandex'
                         	when t.utm_source = 'yandex_rsya' then 'yandex_rsya'
                         	when t.traffic_source  = 'ad' and direct_platform_type in ('Search', 'Context') then 'yandex_poisk'
                         	when t.utm_source = 'ya_poisk' then 'yandex_poisk'
                         	when t.utm_medium = 'cpc' then t.utm_source
                         	when t.referal_source = 'e.mail.ru' then 'mail.ru'
                         	when t.utm_source = 'jSprav' then 'jSprav'
                         	when t.traffic_source = 'referral' then referal_source
                         	when t.utm_source = 'spravker' then t.referer
                         	when t.traffic_source  = 'social' then social_network
                         	when t.traffic_source  = 'messenger' then t.messenger 
                         	when t.traffic_source  = 'direct' then 'direct'
                         	WHEN t.traffic_source = 'internal' THEN split_part(split_part(t.referer, '://', 2),  '/', 1 )
                          else 'indef'
                         END as source
                        , case
                        	when t.utm_source in ('spravker', 'jSprav') then 'referral'
                           	when t.traffic_source = 'organic' then 'organic'
                            when referer like any (array ['https://yandex.ru/images/search%', 'https://yandex.ru/maps/%']) then 'organic'
                           	when t.utm_medium = 'cpc' then 'cpc'
                           	when t.referal_source = 'e.mail.ru' then 'mail'
                           	when t.traffic_source = 'referral' then 'referral'
                           	when t.traffic_source  = 'social' then 'social'
                           	when t.traffic_source  = 'messenger' then 'messenger'
                           	when t.traffic_source  = 'direct' then 'none'
                           	when t.traffic_source  = 'internal' then 'internal'
                           	when t.traffic_source  = 'ad' then 'cpc'
                           	else 'indef'
                           end as medium,
                       t.utm_source, t.utm_medium, t.utm_campaign, 
                       t.utm_content, t.utm_term, t.referer
                FROM rdl.ym_visits t INNER JOIN (
                    WITH a AS (
                        SELECT th.visit_id, split_part(url, '?', 1) AS url
                        FROM cdm.table_hits th 
                        WHERE url != 'goal://profi-filter.ru/pf_event' 
                        GROUP BY visit_id, url
                    )
                    SELECT visit_id, COUNT(url) OVER(PARTITION BY visit_id) AS page_view
                    FROM a
                ) AS t2 ON t.visit_id = t2.visit_id
                WHERE t.date >= '2025-06-01'
            ) 
            SELECT * FROM _source;
        """
    }

def update_table(conn, query, table_name):
    """Выполняет обновление одной таблицы"""
    try:
        with conn.cursor() as cur:
            print(f"⏳ Начинаем обновление {table_name}...")
            start = datetime.now()
            
            cur.execute(query)
            conn.commit()
            
            # Проверяем количество записей
            cur.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cur.fetchone()[0]
            duration = datetime.now() - start
            
            print(f"✅ {table_name} успешно обновлена. Записей: {count:,} | Время: {duration}")
            return True
            
    except Exception as e:
        conn.rollback()
        print(f"❌ Ошибка при обновлении {table_name}: {e}")
        return False

def main():
    print(f"\n🔄 Запуск обновления CDM слоя | {datetime.now()}")
    
    try:
        # Подключаемся к БД
        db_config = load_config()
        conn = psycopg2.connect(**db_config)
        conn.autocommit = False
        print("🔗 Успешное подключение к PostgreSQL")
        
        # Получаем SQL-запросы
        queries = get_queries()
        
        # Обновляем таблицы в строгом порядке
        success = (
            update_table(conn, queries['clients'], "cdm.table_clients") and
            update_table(conn, queries['hits'], "cdm.table_hits") and
            update_table(conn, queries['visits'], "cdm.table_visits")
        )
        
        if success:
            print(f"\n🎉 Обновление завершено успешно! | {datetime.now()}")
            sys.exit(0)
        else:
            print(f"\n⚠️ Обновление завершено с ошибками! | {datetime.now()}")
            sys.exit(1)
            
    except Exception as e:
        print(f"\n🔥 Критическая ошибка: {e}")
        sys.exit(1)
    finally:
        if 'conn' in locals() and conn:
            conn.close()
            print("🔌 Соединение с БД закрыто")

if __name__ == "__main__":
    main()
