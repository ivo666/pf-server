import requests
import psycopg2
import time
import configparser
from datetime import datetime, timedelta

# Настройка логгирования
print("🟢 Запуск скрипта")

# Загрузка конфигурации
try:
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
    print("✅ Конфигурация загружена")
except Exception as e:
    print(f"💥 Ошибка загрузки конфигурации: {e}")
    exit(1)

# Параметры запроса
MAX_RETRIES = 3
RETRY_DELAY = 30
DATE = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")  # Вчерашняя дата

def get_campaign_stats(token, date, attempt=1):
    """Получение данных из API Яндекс.Директ"""
    print(f"📊 Попытка {attempt}: запрос данных за {date}")
    
    # Проверка что дата не будущая
    if datetime.strptime(date, "%Y-%m-%d") > datetime.now():
        print(f"❌ Ошибка: дата {date} является будущей")
        return None

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
            "ReportName": f"report_{int(time.time())}",
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
            print("✅ Данные успешно получены")
            return response.text
        elif response.status_code == 201:
            if attempt >= MAX_RETRIES:
                print(f"❌ Превышено максимальное количество попыток ({MAX_RETRIES})")
                return None
            print(f"🔄 Отчет формируется, ожидайте {RETRY_DELAY} сек... (попытка {attempt} из {MAX_RETRIES})")
            time.sleep(RETRY_DELAY)
            return get_campaign_stats(token, date, attempt+1)
        else:
            print(f"❌ Ошибка API: {response.status_code}\n{response.text}")
            return None
            
    except Exception as e:
        print(f"💥 Ошибка запроса: {e}")
        return None

def save_to_db(conn, raw_data):
    """Сохранение данных в БД"""
    if not raw_data:
        print("❌ Нет данных для сохранения")
        return

    lines = [line for line in raw_data.strip().split('\n') 
             if line.strip() and line.split('\t')[0].startswith('20')]
    
    if not lines:
        print("❌ Нет данных после фильтрации")
        return

    print(f"💾 Начало сохранения {len(lines)} строк...")
    
    with conn.cursor() as cursor:
        success = 0
        for i, line in enumerate(lines, 1):
            parts = line.split('\t')
            if len(parts) < 12:
                continue

            try:
                cursor.execute("""
                INSERT INTO rdl.yd_ad_performance_report (
                    date, campaign_id, campaign_name, ad_id, location_of_presence_id,
                    impressions, clicks, cost, avg_click_position,
                    device, match_type, slot
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (date, campaign_id, campaign_name, ad_id, location_of_presence_id) 
                DO UPDATE SET
                    impressions = EXCLUDED.impressions,
                    clicks = EXCLUDED.clicks,
                    cost = EXCLUDED.cost,
                    avg_click_position = EXCLUDED.avg_click_position
                """, (
                    parts[0],  # date
                    int(parts[1]) if parts[1] else 0,
                    parts[2] if parts[2] else '',
                    int(parts[3]) if parts[3] else 0,
                    int(parts[9]) if parts[9] else 0,
                    int(parts[4]) if parts[4] else 0,
                    int(parts[5]) if parts[5] else 0,
                    float(parts[6].replace(',', '.'))/1000000 if parts[6] and parts[6] != '--' else 0,
                    float(parts[7].replace(',', '.')) if parts[7] and parts[7] != '--' else None,
                    parts[8],
                    parts[10],
                    parts[11]
                ))
                success += 1
            except Exception as e:
                print(f"⚠️ Ошибка в строке {i}: {e}")

        conn.commit()
        print(f"✅ Успешно сохранено {success} из {len(lines)} строк")

def main():
    try:
        # Подключение к БД
        print("🔌 Подключение к БД...")
        conn = psycopg2.connect(**DB_CONFIG)
        print("✅ Подключение к БД установлено")
        
        # Получение данных
        raw_data = get_campaign_stats(YANDEX_TOKEN, DATE)
        
        # Сохранение данных
        if raw_data:
            save_to_db(conn, raw_data)
            
    except psycopg2.Error as e:
        print(f"💥 Ошибка PostgreSQL: {e}")
    except Exception as e:
        print(f"💥 Критическая ошибка: {e}")
    finally:
        if 'conn' in locals():
            conn.close()
            print("🔌 Соединение с БД закрыто")
    
    print("🔴 Завершение работы")

if __name__ == "__main__":
    main()
