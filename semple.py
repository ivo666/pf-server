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
DATE = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")

def create_table(conn):
    """Создаём таблицу с автоинкрементным id и всеми полями"""
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
    """Получаем данные из API Яндекс.Директ"""
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
            print("🔄 Отчет формируется, повторите запрос позже")
        else:
            print(f"❌ Ошибка API: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"⚠️ Ошибка соединения: {e}")
    
    return None

def save_to_db(conn, raw_data):
    """Парсим и сохраняем данные в PostgreSQL"""
    if not raw_data:
        print("❌ Нет данных для сохранения")
        return

    with conn.cursor() as cursor:
        total_rows = 0
        for line in raw_data.strip().split('\n')[1:]:  # Пропускаем заголовок
            if not line or "Total rows" in line:
                continue

            parts = line.split('\t')
            try:
                # Преобразование данных
                cost = float(parts[6].replace(',', '.')) / 1000000 if parts[6] else 0
                avg_pos = float(parts[7].replace(',', '.')) if parts[7] and parts[7] != '--' else None

                cursor.execute("""
                INSERT INTO yandex_direct_stats (
                    date, campaign_id, campaign_name, ad_id,
                    impressions, clicks, cost, avg_click_position,
                    device, location_of_presence_id, match_type, slot
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (date, campaign_id, ad_id, device) DO UPDATE SET
                    impressions = EXCLUDED.impressions,
                    clicks = EXCLUDED.clicks,
                    cost = EXCLUDED.cost,
                    avg_click_position = EXCLUDED.avg_click_position
                """, (
                    parts[0], int(parts[1]), parts[2], int(parts[3]),
                    int(parts[4]) if parts[4] else 0,
                    int(parts[5]) if parts[5] else 0,
                    cost,
                    avg_pos,
                    parts[8],
                    int(parts[9]) if parts[9] else None,
                    parts[10],
                    parts[11]
                ))
                total_rows += 1
            except Exception as e:
                print(f"⚠️ Ошибка обработки строки: {line}\nОшибка: {e}")

        conn.commit()
        print(f"💾 Сохранено строк: {total_rows}")

def main():
    try:
        # Подключение к БД
        conn = psycopg2.connect(**DB_CONFIG)
        print("🔌 Подключение к БД установлено")
        
        # Создание таблицы
        create_table(conn)
        
        # Получение данных
        print(f"📅 Запрашиваем данные за {DATE}")
        raw_data = get_campaign_stats(YANDEX_TOKEN, DATE)
        
        # Сохранение в БД
        if raw_data:
            save_to_db(conn, raw_data)
        
    except Exception as e:
        print(f"💥 Критическая ошибка: {e}")
    finally:
        if 'conn' in locals():
            conn.close()
            print("🔌 Соединение с БД закрыто")

if __name__ == "__main__":
    print("🟢 Запуск скрипта")
    main()
    print("🔴 Завершение работы")
