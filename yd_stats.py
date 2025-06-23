import requests
import psycopg2
from datetime import datetime, timedelta
import configparser
from pathlib import Path
import sys

# --- Конфигурация ---
def load_config():
    """Загружает настройки из config.ini"""
    config = configparser.ConfigParser()
    config_path = Path(__file__).parent / 'config.ini'
    
    if not config_path.exists():
        print(f"❌ Ошибка: Файл config.ini не найден по пути: {config_path}")
        sys.exit(1)
    
    config.read(config_path)
    return config

# --- Получение данных из API ---
def get_yandex_direct_report(token, date_from, date_to):
    """Запрашивает статистику из Яндекс.Директ"""
    url = "https://api.direct.yandex.com/json/v5/reports"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept-Language": "ru",
        "Content-Type": "application/json"
    }

    report_body = {
        "params": {
            "SelectionCriteria": {
                "DateFrom": date_from,
                "DateTo": date_to
            },
            "FieldNames": [
                "Date",
                "CampaignId",
                "CampaignName",
                "Impressions",
                "Clicks",
                "Cost",
                "Ctr",
                "AvgClickPosition",
                "AvgImpressionPosition",
                "Conversions",
                "ConversionRate"
            ],
            "ReportName": "CampaignPerformance",
            "ReportType": "CAMPAIGN_PERFORMANCE_REPORT",
            "DateRangeType": "CUSTOM_DATE",
            "Format": "TSV",
            "IncludeVAT": "YES"
        }
    }

    try:
        print(f"🔄 Запрос данных за период {date_from} - {date_to}...")
        response = requests.post(url, headers=headers, json=report_body, timeout=30)
        response.raise_for_status()
        return response.text
    except requests.exceptions.RequestException as e:
        print(f"❌ Ошибка API: {str(e)}")
        return None

# --- Загрузка в PostgreSQL ---
def save_to_postgres(data, db_params):
    """Сохраняет данные в PostgreSQL"""
    conn = None
    try:
        # Подключение к БД
        conn = psycopg2.connect(
            host=db_params['HOST'],
            database=db_params['DATABASE'],
            user=db_params['USER'],
            password=db_params['PASSWORD'],
            port=db_params['PORT']
        )
        cur = conn.cursor()

        # Парсинг TSV
        lines = data.strip().split('\n')[1:]  # Пропускаем заголовок
        
        # Подготовка и выполнение запроса
        insert_query = """
            INSERT INTO row.yandex_direct_stats (
                date, campaign_id, campaign_name, impressions, clicks, cost,
                ctr, avg_click_position, avg_impression_position,
                conversions, conversion_rate
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        for line in lines:
            if not line.strip():
                continue
                
            values = line.split('\t')
            try:
                cur.execute(insert_query, (
                    values[0],  # Date
                    int(values[1]),  # CampaignId
                    values[2],  # CampaignName
                    int(values[3]),  # Impressions
                    int(values[4]),  # Clicks
                    float(values[5]),  # Cost
                    float(values[6]),  # Ctr
                    float(values[7]),  # AvgClickPosition
                    float(values[8]),  # AvgImpressionPosition
                    int(values[9]),  # Conversions
                    float(values[10])  # ConversionRate
                ))
            except (IndexError, ValueError) as e:
                print(f"⚠️ Ошибка обработки строки: {line}\nОшибка: {str(e)}")
                continue

        conn.commit()
        print(f"✅ Успешно загружено {len(lines)} записей")
        
    except Exception as e:
        print(f"❌ Ошибка БД: {str(e)}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

# --- Основной поток ---
if __name__ == "__main__":
    try:
        # Загрузка конфигурации
        config = load_config()
        
        # Настройки Яндекс.Директ
        yandex_token = config['YandexDirect']['ACCESS_TOKEN']
        
        # Настройки БД
        db_params = {
            'HOST': config['Database']['HOST'],
            'DATABASE': config['Database']['DATABASE'],
            'USER': config['Database']['USER'],
            'PASSWORD': config['Database']['PASSWORD'],
            'PORT': config['Database']['PORT']
        }

        # Определяем даты (последние 7 дней)
        date_to = datetime.now().strftime('%Y-%m-%d')
        date_from = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')

        # Получаем данные
        report_data = get_yandex_direct_report(yandex_token, date_from, date_to)
        
        if report_data:
            save_to_postgres(report_data, db_params)
        else:
            print("⚠️ Нет данных для загрузки")

    except Exception as e:
        print(f"🔥 Критическая ошибка: {str(e)}")
    finally:
        print("Завершение работы")
