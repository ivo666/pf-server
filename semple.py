import requests
import psycopg2
import time
import configparser
from datetime import datetime, timedelta

# Загрузка конфигурации
config = configparser.ConfigParser()
config.read('config.ini')

DB_CONFIG = {
    "host": config['Database']['HOST'],
    "database": config['Database']['DATABASE'],
    "user": config['Database']['USER'],
    "password": config['Database']['PASSWORD'],
    "port": config['Database']['PORT']
}

YANDEX_TOKEN = config['YandexDirect']['ACCESS_TOKEN']
DATE = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")  # Фиксируем вчерашнюю дату

def create_table(conn):
    """Создаём таблицу с автоинкрементным id"""
    with conn.cursor() as cursor:
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS yandex_direct_stats (
            id SERIAL PRIMARY KEY,
            date DATE NOT NULL,
            campaign_id BIGINT NOT NULL,
            campaign_name TEXT,
            ad_id BIGINT NOT NULL,
            impressions INTEGER,
            clicks INTEGER,
            cost DECIMAL(18, 2),
            avg_click_position DECIMAL(10, 2),
            device TEXT,
            location_of_presence_id INTEGER,
            match_type TEXT,
            slot TEXT,
            load_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (date, campaign_id, ad_id, device)
        )
        """)
        conn.commit()
    print("✅ Таблица создана/проверена")

def get_campaign_stats(token, date):
    """Получаем данные из API Яндекс.Директ с обработкой ошибок"""
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept-Language": "ru",
        "Content-Type": "application/json"
    }

    body = {
        "params": {
            "SelectionCriteria": {"DateFrom": date, "DateTo": date},
            "FieldNames": [
                "Date", "CampaignId", "CampaignName", "AdId",
                "Impressions", "Clicks", "Cost", "AvgClickPosition",
                "Device", "LocationOfPresenceId", "MatchType", "Slot"
            ],
            "ReportName": f"report_{date.replace('-', '')}",
            "ReportType": "AD_PERFORMANCE_REPORT",
            "DateRangeType": "CUSTOM_DATE",
            "Format": "TSV",
            "IncludeVAT": "YES",
            "IncludeDiscount": "NO"
        }
    }

    try:
        response = requests.post(
            "https://api.direct.yandex.com/json/v5/reports",
            headers=headers,
            json=body,
            timeout=120
        )
        
        if response.status_code == 200:
            return response.text
        elif response.status_code == 201:
            print("🔄 Отчет формируется, повторите запрос через 30 секунд")
            time.sleep(30)
            return get_campaign_stats(token, date)
        else:
            print(f"❌ Ошибка API: {response.status_code}\n{response.text}")
            return None
            
    except Exception as e:
        print(f"⚠️ Ошибка соединения: {e}")
        return None

def save_to_db(conn, raw_data):
    """Сохраняем данные в PostgreSQL"""
    if not raw_data:
        print("❌ Нет данных для сохранения")
        return

    lines = raw_data.strip().split('\n')
    
    # Проверяем, есть ли данные кроме заголовков
    if len(lines) <= 1:
        print("❌ Нет данных для сохранения (только заголовки)")
        return

    # Пропускаем строки, пока не найдем начало данных
    data_start = 0
    for i, line in enumerate(lines):
        if line.startswith('Total rows') or not line.strip():
            continue
        if line.split('\t')[0].replace('-', '').isdigit():  # Проверяем, что первое поле - дата
            data_start = i
            break

    with conn.cursor() as cursor:
        total = 0
        for line in lines[data_start:]:
            if not line.strip() or line.startswith("Total rows"):
                continue

            parts = line.split('\t')
            if len(parts) < 12:
                continue

            try:
                # Проверяем, что первое поле - валидная дата
                if not parts[0].replace('-', '').isdigit():
                    continue

                # Обработка данных
                date_value = parts[0]
                campaign_id = int(parts[1]) if parts[1] and parts[1].isdigit() else 0
                campaign_name = parts[2] if parts[2] else None
                ad_id = int(parts[3]) if parts[3] and parts[3].isdigit() else 0
                impressions = int(parts[4]) if parts[4] and parts[4].isdigit() else 0
                clicks = int(parts[5]) if parts[5] and parts[5].isdigit() else 0
                cost = float(parts[6].replace(',', '.'))/1000000 if parts[6] and parts[6] != '--' else 0.0
                avg_pos = float(parts[7].replace(',', '.')) if parts[7] and parts[7] != '--' else None
                device = parts[8] if parts[8] else None
                location_id = int(parts[9]) if parts[9] and parts[9].isdigit() else None
                match_type = parts[10] if parts[10] else None
                slot = parts[11] if parts[11] else None

                cursor.execute("""
                INSERT INTO yandex_direct_stats (...) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (...) DO UPDATE SET ...
                """, (...))
                total += 1
            except Exception as e:
                print(f"⚠️ Ошибка в строке: {line[:50]}...\nОшибка: {str(e)}")
        
        conn.commit()
        print(f"💾 Успешно сохранено строк: {total}")

def main():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print("🔌 Подключение к БД установлено")
        
        create_table(conn)
        
        print(f"📅 Запрашиваем данные за {DATE}")
        raw_data = get_campaign_stats(YANDEX_TOKEN, DATE)
        
        if raw_data:
            save_to_db(conn, raw_data)
        
    except Exception as e:
        print(f"💥 Критическая ошибка: {e}")
    finally:
        if 'conn' in locals():
            conn.close()
            print("🔌 Соединение с БД закрыто")
    print("🔴 Завершение работы")

if __name__ == "__main__":
    print("🟢 Запуск скрипта")
    main()
