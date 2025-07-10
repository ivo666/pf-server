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

DATE = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")

def get_campaign_stats(token, date):
    """Получение данных из API Яндекс.Директ"""
    print(f"📊 Запрос данных за {date}")
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept-Language": "ru",
        "Content-Type": "application/json"
    }

    # Генерируем уникальное имя отчета
    report_name = f"ad_report_{int(time.time())}"

    body = {
        "params": {
            "SelectionCriteria": {"DateFrom": date, "DateTo": date},
            "FieldNames": [
                "Date", "CampaignId", "CampaignName", "AdId",
                "Impressions", "Clicks", "Cost", "AvgClickPosition",
                "Device", "LocationOfPresenceId", "MatchType", "Slot"
            ],
            "ReportName": report_name,
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
            print("🔄 Отчет формируется, ожидайте...")
            time.sleep(30)
            return get_campaign_stats(token, date)  # Повторяем запрос
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

    lines = raw_data.strip().split('\n')
    data_lines = [line for line in lines if line.strip() and not line.startswith('Date\t') and not line.startswith('Total')]
    
    if not data_lines:
        print("❌ Нет данных после фильтрации")
        return

    print(f"💾 Начало сохранения {len(data_lines)} строк...")
    
    with conn.cursor() as cursor:
        success = 0
        for i, line in enumerate(data_lines, 1):
            if i % 50 == 0:
                print(f"⏳ Обработано {i} из {len(data_lines)} строк...")
                
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
                    int(parts[1]) if parts[1] else 0,  # campaign_id
                    parts[2] if parts[2] else '',  # campaign_name
                    int(parts[3]) if parts[3] else 0,  # ad_id
                    int(parts[9]) if parts[9] else 0,  # location_of_presence_id
                    int(parts[4]) if parts[4] else 0,  # impressions
                    int(parts[5]) if parts[5] else 0,  # clicks
                    float(parts[6].replace(',', '.'))/1000000 if parts[6] and parts[6] != '--' else 0,  # cost
                    float(parts[7].replace(',', '.')) if parts[7] and parts[7] != '--' else None,  # avg_click_position
                    parts[8],  # device
                    parts[10],  # match_type
                    parts[11]   # slot
                ))
                success += 1
            except Exception as e:
                print(f"⚠️ Ошибка в строке {i}: {e}")

        conn.commit()
        print(f"✅ Успешно сохранено {success} из {len(data_lines)} строк")

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
