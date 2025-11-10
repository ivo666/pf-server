import os
import requests
import pandas as pd
import psycopg2
from datetime import datetime, timedelta
from dotenv import load_dotenv
from psycopg2.extras import execute_values

# Загружаем переменные окружения из .env файла
load_dotenv()

# Настройки авторизации
API_TOKEN = os.getenv('API_TOKEN')
BASE_URL = os.getenv('BASE_URL')

headers = {
    "Authorization": f"OAuth {API_TOKEN}",
    "Content-Type": "application/json"
}

# Настройки подключения к БД из .env файла
DB_CONFIG = {
    'host': os.getenv('HOST', 'localhost'),
    'port': os.getenv('PORT', '5432'),
    'database': os.getenv('NAME'),
    'user': os.getenv('USER'),
    'password': os.getenv('PASSWORD')
}

def get_existing_dates_from_db(conn):
    """Получает даты, которые уже есть в локальной БД"""
    print("🗄️ Проверяем существующие даты в базе данных...")
    
    query = "SELECT DISTINCT date FROM rdl.webm_api ORDER BY date"
    
    try:
        with conn.cursor() as cursor:
            cursor.execute(query)
            existing_dates = [row[0].strftime("%Y-%m-%d") for row in cursor.fetchall()]
        print(f"✅ В базе найдено {len(existing_dates)} дат")
        return existing_dates
    except Exception as e:
        print(f"❌ Ошибка при получении дат из БД: {e}")
        return []

def check_date_has_data_in_webmaster(user_id, host_id, target_date):
    """Проверяет, есть ли данные в Вебмастере для указанной даты"""
    monitoring_url = f"{BASE_URL}/user/{user_id}/hosts/{host_id}/query-analytics/list"
    
    payload = {
        "limit": 1,
        "text_indicator": "QUERY",
        "filters": {
            "statistic_filters": [{
                "statistic_field": "IMPRESSIONS",
                "operation": "GREATER_THAN",
                "value": "0",
                "from": target_date,
                "to": target_date
            }]
        }
    }
    
    try:
        response = requests.post(monitoring_url, headers=headers, json=payload)
        if response.status_code == 200:
            data = response.json()
            stats_list = data.get('text_indicator_to_statistics', [])
            return len(stats_list) > 0
        else:
            print(f"⚠️ Ошибка при проверке даты {target_date}: {response.status_code}")
            return False
    except Exception as e:
        print(f"⚠️ Ошибка при проверке даты {target_date}: {e}")
        return False

def get_missing_dates(conn, user_id, host_id, days_back=20):
    """Определяет даты за последние N дней, которых нет в БД"""
    print(f"📅 Проверяем данные за последние {days_back} дней...")
    
    # Получаем существующие даты из БД
    existing_dates = get_existing_dates_from_db(conn)
    
    # Генерируем список дат за последние N дней
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=days_back-1)  # -1 чтобы включить текущий день
    
    all_dates = []
    current_date = start_date
    while current_date <= end_date:
        all_dates.append(current_date.strftime("%Y-%m-%d"))
        current_date += timedelta(days=1)
    
    print(f"🔍 Проверяем наличие данных в Вебмастере для {len(all_dates)} дат...")
    print(f"   Период: {start_date} - {end_date}")
    
    # Проверяем, какие даты есть в Вебмастере
    available_dates = []
    for i, date_str in enumerate(all_dates, 1):
        if i % 5 == 0 or i == len(all_dates):
            print(f"   Проверено {i}/{len(all_dates)} дат...")
        
        if check_date_has_data_in_webmaster(user_id, host_id, date_str):
            available_dates.append(date_str)
    
    # Находим даты, которые есть в Вебмастере, но нет в БД
    missing_dates = [date for date in available_dates if date not in existing_dates]
    
    print(f"📊 Статистика:")
    print(f"   - Проверено период: {start_date} - {end_date}")
    print(f"   - Найдено данных в Вебмастере: {len(available_dates)} дат")
    print(f"   - Есть в БД: {len(existing_dates)} дат")
    print(f"   - Отсутствует в БД: {len(missing_dates)} дат")
    
    if missing_dates:
        print(f"   - Отсутствующие даты: {', '.join(sorted(missing_dates))}")
    
    return missing_dates

def get_all_urls_for_date(user_id, host_id, target_date):
    """Получает все уникальные URL для указанной даты"""
    monitoring_url = f"{BASE_URL}/user/{user_id}/hosts/{host_id}/query-analytics/list"
    urls = set()
    offset = 0
    limit = 500
    
    while True:
        payload = {
            "offset": offset,
            "limit": limit,
            "text_indicator": "URL",
            "filters": {
                "statistic_filters": [{
                    "statistic_field": "IMPRESSIONS",
                    "operation": "GREATER_THAN",
                    "value": "0",
                    "from": target_date,
                    "to": target_date
                }]
            }
        }
        
        response = requests.post(monitoring_url, headers=headers, json=payload)
        if response.status_code == 200:
            data = response.json()
            stats_list = data.get('text_indicator_to_statistics', [])
            
            if not stats_list:
                break
            
            for item in stats_list:
                url = item.get('text_indicator', {}).get('value', '')
                if url and url != 'N/A':
                    urls.add(url)
            
            # Проверяем, есть ли еще данные
            if len(stats_list) < limit:
                break
                
            offset += limit
        else:
            print(f"❌ Ошибка при получении URL за {target_date}: {response.status_code}")
            break
    
    return list(urls)

def get_data_for_date_and_url(user_id, host_id, target_date, url, device):
    """Получает данные для конкретной даты, URL и устройства"""
    monitoring_url = f"{BASE_URL}/user/{user_id}/hosts/{host_id}/query-analytics/list"
    
    payload = {
        "limit": 500,
        "text_indicator": "QUERY",
        "device_type_indicator": device,
        "filters": {
            "text_filters": [{
                "text_indicator": "URL",
                "operation": "TEXT_MATCH",
                "value": url
            }],
            "statistic_filters": [{
                "statistic_field": "DEMAND",
                "operation": "GREATER_THAN",
                "value": "0",
                "from": target_date,
                "to": target_date
            }]
        }
    }
    
    response = requests.post(monitoring_url, headers=headers, json=payload)
    data_rows = []
    
    if response.status_code == 200:
        data = response.json()
        stats_list = data.get('text_indicator_to_statistics', [])
        
        for item in stats_list:
            text_indicator = item.get('text_indicator', {})
            query_text = text_indicator.get('value', 'N/A')
            
            statistics = item.get('statistics', [])
            
            # Собираем метрики для целевой даты
            metrics = {}
            for stat in statistics:
                if stat.get('date') == target_date:
                    field = stat.get('field')
                    value = stat.get('value', 0)
                    metrics[field] = value
            
            # Фильтруем по DEMAND > 0
            if metrics and metrics.get('DEMAND', 0) > 0:
                data_row = {
                    'date': target_date,
                    'page_path': url,
                    'query': query_text,
                    'demand': metrics.get('DEMAND', 0),
                    'impressions': metrics.get('IMPRESSIONS', 0),
                    'clicks': metrics.get('CLICKS', 0),
                    'position': metrics.get('POSITION', 0),
                    'device': device
                }
                data_rows.append(data_row)
    
    return data_rows

def save_to_database(df, conn):
    """Сохраняет DataFrame в базу данных"""
    if df.empty:
        print("⚠️ Нет данных для сохранения")
        return
    
    # Подготавливаем данные для вставки
    data_tuples = [
        (
            row['date'],
            row['page_path'],
            row['query'],
            row['demand'],
            row['impressions'],
            row['clicks'],
            row['position'],
            row['device']
        )
        for _, row in df.iterrows()
    ]
    
    # SQL запрос для вставки данных
    insert_query = """
        INSERT INTO rdl.webm_api (date, page_path, query, demand, impressions, clicks, position, device)
        VALUES %s
    """
    
    try:
        with conn.cursor() as cursor:
            execute_values(cursor, insert_query, data_tuples)
            conn.commit()
        
        print(f"✅ Данные успешно сохранены в БД: {len(data_tuples)} записей")
    except Exception as e:
        print(f"❌ Ошибка при сохранении в БД: {e}")
        conn.rollback()

def load_data_for_missing_dates(conn, user_id, host_id, missing_dates):
    """Загружает данные за недостающие даты"""
    if not missing_dates:
        print("🎉 Все данные уже синхронизированы! Нет недостающих дат.")
        return
    
    device_types = ['DESKTOP', 'MOBILE', 'TABLET']
    total_data = []
    
    print(f"\n🔄 Начинаем загрузку данных за {len(missing_dates)} недостающих дат...")
    
    for i, target_date in enumerate(missing_dates, 1):
        print(f"\n📅 Обрабатываем дату {i}/{len(missing_dates)}: {target_date}")
        
        # Получаем все URL для этой даты
        urls = get_all_urls_for_date(user_id, host_id, target_date)
        print(f"   📍 Найдено URL: {len(urls)}")
        
        if not urls:
            print(f"   ⚠️ Нет URL с данными за {target_date}, пропускаем")
            continue
        
        date_data = []
        total_combinations = len(urls) * len(device_types)
        processed = 0
        
        for url in urls:
            for device in device_types:
                processed += 1
                if processed % 50 == 0:
                    print(f"   Прогресс: {processed}/{total_combinations}")
                
                url_data = get_data_for_date_and_url(user_id, host_id, target_date, url, device)
                date_data.extend(url_data)
        
        total_data.extend(date_data)
        print(f"   ✅ Собрано данных за {target_date}: {len(date_data)} записей")
    
    # Сохраняем все собранные данные
    if total_data:
        df = pd.DataFrame(total_data)
        df = df.drop_duplicates()
        
        # Приводим данные в порядок
        print("\n🔧 Приводим данные в порядок...")
        df['demand'] = df['demand'].astype('int')
        df['impressions'] = df['impressions'].astype('int')
        df['clicks'] = df['clicks'].astype('int')
        df['position'] = df['position'].astype('float')
        df['device'] = df['device'].str.lower()
        
        print(f"\n📊 ИТОГОВАЯ СТАТИСТИКА:")
        print(f"Всего записей: {len(df)}")
        print(f"Обработано дат: {len(missing_dates)}")
        print(f"Уникальных URL: {df['page_path'].nunique()}")
        print(f"Уникальных запросов: {df['query'].nunique()}")
        
        # Сохраняем в БД
        save_to_database(df, conn)
    else:
        print("❌ Не удалось собрать данные за указанные даты")

def main():
    print(f"\n{'='*60}")
    print("ИНКРЕМЕНТАЛЬНЫЙ СБОР ДАННЫХ ИЗ YANDEX WEBMASTER API")
    print(f"{'='*60}")
    
    # Получаем user_id
    user_info_url = f"{BASE_URL}/user"
    try:
        response = requests.get(user_info_url, headers=headers)
        if response.status_code != 200:
            print(f"❌ Ошибка авторизации: {response.status_code}")
            print(f"   Текст ошибки: {response.text}")
            return
        
        user_data = response.json()
        user_id = user_data['user_id']
        host_id = os.getenv('HOST_ID')
        
        print(f"👤 User ID: {user_id}")
        print(f"🌐 Host ID: {host_id}")
        
        # Подключаемся к БД
        print("\n🗄️ Подключаемся к базе данных...")
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            print("✅ Подключение к БД установлено")
            
            # Определяем недостающие даты (проверяем последние 20 дней)
            missing_dates = get_missing_dates(conn, user_id, host_id, days_back=20)
            
            # Загружаем данные за недостающие даты
            load_data_for_missing_dates(conn, user_id, host_id, missing_dates)
            
            conn.close()
            print("✅ Подключение к БД закрыто")
            
        except Exception as e:
            print(f"❌ Ошибка при работе с базой данных: {e}")
            
    except Exception as e:
        print(f"❌ Ошибка при подключении к API: {e}")

if __name__ == "__main__":
    main()
