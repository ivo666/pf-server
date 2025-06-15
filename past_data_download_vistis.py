from datetime import datetime, timedelta
from tapi_yandex_metrika import YandexMetrikaLogsapi
import pandas as pd
from time import sleep
import os
import configparser
import psycopg2
from psycopg2.extras import execute_batch

dirname = os.path.dirname(__file__)
# Чтение конфигурации
config = configparser.ConfigParser()
config.read(os.path.join(dirname, "config.ini"))

# Параметры подключения к БД
DB_CONFIG = {
    'host': config['Database']['HOST'],
    'database': config['Database']['DATABASE'],
    'user': config['Database']['USER'],
    'password': config['Database']['PASSWORD']
}

# Параметры Яндекс.Метрики
ACCESS_TOKEN = config['YandexMetrika']['ACCESS_TOKEN']
COUNTER_ID = config['YandexMetrika']['COUNTER_ID']

# Все необходимые поля
FIELDS = [
    'ym:s:clientID', 'ym:s:visitID', 'ym:s:watchIDs', 'ym:s:date', 'ym:s:dateTime',
    'ym:s:isNewUser', 'ym:s:startURL', 'ym:s:endURL', 'ym:s:pageViews', 'ym:s:visitDuration',
    'ym:s:regionCountry', 'ym:s:regionCity', 'ym:s:<attribution>TrafficSource',
    'ym:s:<attribution>AdvEngine', 'ym:s:<attribution>ReferalSource',
    'ym:s:<attribution>SearchEngineRoot', 'ym:s:<attribution>SearchEngine',
    'ym:s:<attribution>SocialNetwork', 'ym:s:referer', 'ym:s:<attribution>DirectClickOrder',
    'ym:s:<attribution>DirectBannerGroup', 'ym:s:<attribution>DirectClickBanner',
    'ym:s:<attribution>DirectClickOrderName', 'ym:s:<attribution>ClickBannerGroupName',
    'ym:s:<attribution>DirectClickBannerName', 'ym:s:<attribution>DirectPlatformType',
    'ym:s:<attribution>DirectPlatform', 'ym:s:<attribution>DirectConditionType',
    'ym:s:<attribution>UTMCampaign', 'ym:s:<attribution>UTMContent',
    'ym:s:<attribution>UTMMedium', 'ym:s:<attribution>UTMSource', 'ym:s:<attribution>UTMTerm',
    'ym:s:deviceCategory', 'ym:s:mobilePhone', 'ym:s:mobilePhoneModel', 'ym:s:browser',
    'ym:s:screenFormat', 'ym:s:screenOrientation', 'ym:s:physicalScreenWidth',
    'ym:s:physicalScreenHeight', 'ym:s:<attribution>Messenger',
    'ym:s:<attribution>RecommendationSystem'
]

def get_week_ranges(start_date, end_date):
    """Генерирует список диапазонов дат по неделям"""
    current_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")
    
    week_ranges = []
    
    while current_date <= end_date:
        week_start = current_date
        week_end = current_date + timedelta(days=6)
        if week_end > end_date:
            week_end = end_date
        
        week_ranges.append((
            week_start.strftime("%Y-%m-%d"),
            week_end.strftime("%Y-%m-%d")
        ))
        
        current_date = week_end + timedelta(days=1)
    
    return week_ranges

def wait_for_report_processing(client, request_id, check_interval=30):
    """Ожидает завершения обработки отчета"""
    while True:
        info = client.info(requestId=request_id).get()
        status = info["log_request"]["status"]
        
        if status == "processed":
            return info
        elif status == "created" or status == "pending":
            print(f"Отчет в обработке, статус: {status}. Ожидаю {check_interval} секунд...")
            sleep(check_interval)
        else:
            raise Exception(f"Ошибка обработки отчета. Статус: {status}")

def download_report_parts(client, request_id, parts_count):
    """Загружает все части отчета и объединяет их в один DataFrame"""
    all_data = []
    
    for part_number in range(parts_count):
        print(f"Загрузка части {part_number + 1} из {parts_count}...")
        part = client.download(requestId=request_id, partNumber=part_number).get()
        part_data = part().to_dicts()
        all_data.extend(part_data)
    
    df = pd.DataFrame(all_data, columns=FIELDS)
    
    # Отладочная информация о данных
    print("\nПример данных из отчета:")
    print(df[['ym:s:isNewUser', 'ym:s:clientID', 'ym:s:visitID']].head(10))
    print("\nТип данных ym:s:isNewUser:", type(df['ym:s:isNewUser'].iloc[0]) if len(df) > 0 else "Нет данных")
    print("Уникальные значения ym:s:isNewUser:", df['ym:s:isNewUser'].unique())
    
    return df

def create_connection():
    """Создает подключение к PostgreSQL"""
    return psycopg2.connect(**DB_CONFIG)

def create_table(conn):
    """Создает таблицу для хранения визитов"""
    with conn.cursor() as cursor:
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS yandex_metrika_visits (
            client_id TEXT,
            visit_id TEXT PRIMARY KEY,
            watch_ids TEXT[],
            date DATE,
            date_time TIMESTAMP,
            is_new_user TEXT,  -- Изменено на TEXT
            start_url TEXT,
            end_url TEXT,
            page_views INTEGER,
            visit_duration INTEGER,
            region_country TEXT,
            region_city TEXT,
            traffic_source TEXT,
            adv_engine TEXT,
            referal_source TEXT,
            search_engine_root TEXT,
            search_engine TEXT,
            social_network TEXT,
            referer TEXT,
            direct_click_order TEXT,
            direct_banner_group TEXT,
            direct_click_banner TEXT,
            direct_click_order_name TEXT,
            click_banner_group_name TEXT,
            direct_click_banner_name TEXT,
            direct_platform_type TEXT,
            direct_platform TEXT,
            direct_condition_type TEXT,
            utm_campaign TEXT,
            utm_content TEXT,
            utm_medium TEXT,
            utm_source TEXT,
            utm_term TEXT,
            device_category TEXT,
            mobile_phone TEXT,
            mobile_phone_model TEXT,
            browser TEXT,
            screen_format TEXT,
            screen_orientation TEXT,
            physical_screen_width INTEGER,
            physical_screen_height INTEGER,
            messenger TEXT,
            recommendation_system TEXT
        )
        """)
    conn.commit()

def insert_data(conn, df):
    """Вставляет данные в PostgreSQL"""
    with conn.cursor() as cursor:
        data = []
        for _, row in df.iterrows():
            watch_ids = row['ym:s:watchIDs']
            if isinstance(watch_ids, str):
                watch_ids = watch_ids.strip('[]').split(',')
                watch_ids = [x.strip(' "\'') for x in watch_ids if x.strip()]
            
            # Сохраняем isNewUser как строку без преобразований
            is_new_user = str(row.get('ym:s:isNewUser', ''))  # Преобразуем в строку
            
            data.append((
                row.get('ym:s:clientID'),
                row.get('ym:s:visitID'),
                watch_ids or None,
                row.get('ym:s:date'),
                pd.to_datetime(row.get('ym:s:dateTime')),
                is_new_user,  # Сохраняем как строку
                row.get('ym:s:startURL'),
                row.get('ym:s:endURL'),
                int(row.get('ym:s:pageViews', 0)),
                int(row.get('ym:s:visitDuration', 0)),
                row.get('ym:s:regionCountry'),
                row.get('ym:s:regionCity'),
                row.get('ym:s:<attribution>TrafficSource'),
                row.get('ym:s:<attribution>AdvEngine'),
                row.get('ym:s:<attribution>ReferalSource'),
                row.get('ym:s:<attribution>SearchEngineRoot'),
                row.get('ym:s:<attribution>SearchEngine'),
                row.get('ym:s:<attribution>SocialNetwork'),
                row.get('ym:s:referer'),
                row.get('ym:s:<attribution>DirectClickOrder'),
                row.get('ym:s:<attribution>DirectBannerGroup'),
                row.get('ym:s:<attribution>DirectClickBanner'),
                row.get('ym:s:<attribution>DirectClickOrderName'),
                row.get('ym:s:<attribution>ClickBannerGroupName'),
                row.get('ym:s:<attribution>DirectClickBannerName'),
                row.get('ym:s:<attribution>DirectPlatformType'),
                row.get('ym:s:<attribution>DirectPlatform'),
                row.get('ym:s:<attribution>DirectConditionType'),
                row.get('ym:s:<attribution>UTMCampaign'),
                row.get('ym:s:<attribution>UTMContent'),
                row.get('ym:s:<attribution>UTMMedium'),
                row.get('ym:s:<attribution>UTMSource'),
                row.get('ym:s:<attribution>UTMTerm'),
                row.get('ym:s:deviceCategory'),
                row.get('ym:s:mobilePhone'),
                row.get('ym:s:mobilePhoneModel'),
                row.get('ym:s:browser'),
                row.get('ym:s:screenFormat'),
                row.get('ym:s:screenOrientation'),
                int(row.get('ym:s:physicalScreenWidth', 0)),
                int(row.get('ym:s:physicalScreenHeight', 0)),
                row.get('ym:s:<attribution>Messenger'),
                row.get('ym:s:<attribution>RecommendationSystem')
            ))

        execute_batch(cursor, """
            INSERT INTO yandex_metrika_visits (
                client_id, visit_id, watch_ids, date, date_time,
                is_new_user, start_url, end_url, page_views, visit_duration,
                region_country, region_city, traffic_source, adv_engine,
                referal_source, search_engine_root, search_engine, social_network,
                referer, direct_click_order, direct_banner_group, direct_click_banner,
                direct_click_order_name, click_banner_group_name, direct_click_banner_name,
                direct_platform_type, direct_platform, direct_condition_type,
                utm_campaign, utm_content, utm_medium, utm_source, utm_term,
                device_category, mobile_phone, mobile_phone_model, browser,
                screen_format, screen_orientation, physical_screen_width,
                physical_screen_height, messenger, recommendation_system
            ) VALUES (
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s
            )
            ON CONFLICT (visit_id) DO NOTHING
        """, data)
    conn.commit()

def process_week(client, conn, date1, date2):
    """Обрабатывает данные за одну неделю"""
    try:
        # Запрос данных
        params = {
            "fields": ",".join(FIELDS),
            "source": "visits",
            "date1": date1,
            "date2": date2
        }

        # Создание и обработка запроса
        print(f"\nЗапрашиваю данные с {date1} по {date2}...")
        request = client.create().post(params=params)
        request_id = request["log_request"]["request_id"]
        print(f"Запрос создан, ID: {request_id}")
        
        # Ожидание обработки
        info = wait_for_report_processing(client, request_id)
        parts_count = len(info["log_request"]["parts"])
        print(f"Отчет обработан, количество частей: {parts_count}")
        
        # Загрузка данных
        df = download_report_parts(client, request_id, parts_count)
        
        # Сохранение в БД
        insert_data(conn, df)
        print(f"Успешно сохранено {len(df)} записей с {date1} по {date2}")
        
    except Exception as e:
        print(f"Ошибка при обработке недели с {date1} по {date2}: {e}")
        raise

def cleanup_temp_files():
    """Удаление всех временных файлов"""
    temp_files = [
        "yandex-metrika-visits.csv",
        "yandex-metrika-visits.json",
        "yandex-metrika-visits.xlsx"
    ]
    
    for file in temp_files:
        try:
            if os.path.exists(file):
                os.remove(file)
                print(f"Временный файл {file} удалён")
        except Exception as e:
            print(f"Ошибка при удалении файла {file}: {e}")

def main():
    try:
        # Инициализация клиента Яндекс.Метрики
        client = YandexMetrikaLogsapi(
            access_token=ACCESS_TOKEN,
            default_url_params={'counterId': COUNTER_ID}
        )

        # Получаем диапазоны дат по неделям
        week_ranges = get_week_ranges("2025-03-16", "2025-03-31")
        
        # Подключение к БД
        conn = create_connection()
        create_table(conn)
        
        # Обработка каждой недели
        for i, (date1, date2) in enumerate(week_ranges, 1):
            print(f"\nОбработка недели {i} из {len(week_ranges)}: {date1} - {date2}")
            try:
                process_week(client, conn, date1, date2)
            except Exception as e:
                print(f"Прерывание обработки недели {date1}-{date2} из-за ошибки: {e}")
                sleep(60)
                continue
                
    except Exception as e:
        print(f"Произошла критическая ошибка: {e}")
    finally:
        if 'conn' in locals() and conn is not None:
            conn.close()
        cleanup_temp_files()

if __name__ == "__main__":
    main()
