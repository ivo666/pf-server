import requests
import psycopg2
from datetime import datetime, timedelta
import configparser
from pathlib import Path
import sys
import json  # Добавлено для логирования ошибок API

# --- Конфигурация ---
def load_config():
    config = configparser.ConfigParser()
    config_path = Path(__file__).parent / 'config.ini'
    if not config_path.exists():
        print(f"❌ Файл config.ini не найден: {config_path}")
        sys.exit(1)
    config.read(config_path)
    return config

# --- Получение данных из API ---
def get_yandex_direct_report(token, date_from, date_to):
    url = "https://api.direct.yandex.com/json/v5/reports"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept-Language": "ru",
        "Content-Type": "application/json"
    }

    report_body = {
        "params": {
            "SelectionCriteria": {"DateFrom": date_from, "DateTo": date_to},
            "FieldNames": [
                "Date",
                "CampaignId",
                "CampaignName",
                "AdId",
                "Impressions",
                "Clicks",
                "Cost",
                "Ctr",
                "AvgCpc",
                "Conversions",
                "ConversionRate"
            ],
            "ReportName": "AdPerformanceReport",
            "ReportType": "AD_PERFORMANCE_REPORT",
            "DateRangeType": "CUSTOM_DATE",
            "Format": "TSV",
            "IncludeVAT": "YES",
            "IncludeDiscount": "NO"
        }
    }

    try:
        print(f"🔄 Запрос данных за {date_from} - {date_to}...")
        response = requests.post(
            url,
            headers=headers,
            json=report_body,
            timeout=30
        )
        
        # Детальное логирование ошибок API
        if response.status_code != 200:
            error_details = response.json().get('error', {})
            print(f"❌ Ошибка API (код {response.status_code}):")
            print(f"Код ошибки: {error_details.get('error_code', 'неизвестен')}")
            print(f"Текст ошибки: {error_details.get('error_string', 'неизвестен')}")
            print(f"Детали: {error_details.get('error_detail', 'нет')}")
            print(f"Полный ответ: {json.dumps(response.json(), indent=2, ensure_ascii=False)}")
            return None
            
        return response.text
        
    except requests.exceptions.RequestException as e:
        print(f"❌ Ошибка соединения: {str(e)}")
        return None

# ... (остальные функции save_to_postgres и main остаются без изменений)

if __name__ == "__main__":
    try:
        config = load_config()
        yandex_token = config['YandexDirect']['ACCESS_TOKEN']
        db_params = {
            'HOST': config['Database']['HOST'],
            'DATABASE': config['Database']['DATABASE'],
            'USER': config['Database']['USER'],
            'PASSWORD': config['Database']['PASSWORD'],
            'PORT': config['Database']['PORT']
        }

        date_to = datetime.now().strftime('%Y-%m-%d')
        date_from = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')

        report_data = get_yandex_direct_report(yandex_token, date_from, date_to)
        if report_data:
            print("📊 Получены данные. Пример первых строк:")
            print("\n".join(report_data.split("\n")[:3]))  # Показываем первые 3 строки
            save_to_postgres(report_data, db_params)
        else:
            print("⚠️ Нет данных для загрузки. Проверьте логи ошибок выше.")

    except Exception as e:
        print(f"🔥 Критическая ошибка: {str(e)}")
    finally:
        print("Готово")
