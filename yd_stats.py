import requests
from datetime import datetime, timedelta
import configparser
from pathlib import Path

def load_config():
    config = configparser.ConfigParser()
    config.read(Path(__file__).parent / 'config.ini')
    return config

def get_direct_report(token, date_from, date_to):
    url = "https://api.direct.yandex.com/json/v5/reports"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept-Language": "ru",
        "Content-Type": "application/json"
    }

    report_body = {
        "method": "get",
        "params": {
            "SelectionCriteria": {
                "DateFrom": date_from,
                "DateTo": date_to
            },
            "FieldNames": [
                "Date",
                "CampaignId",
                "CampaignName",
                "AdId",
                "Clicks",
                "Cost",
                "Ctr"
            ],
            "ReportName": "AD_PERFORMANCE_REPORT",  # Обязательное поле!
            "ReportType": "AD_PERFORMANCE_REPORT",
            "DateRangeType": "CUSTOM_DATE",
            "Format": "TSV",
            "IncludeVAT": "YES"
        }
    }

    try:
        response = requests.post(url, headers=headers, json=report_body)
        response.raise_for_status()
        return response.text
    except requests.exceptions.RequestException as e:
        print(f"Ошибка запроса: {e}")
        print(f"Ответ сервера: {e.response.text if e.response else 'Нет ответа'}")
        return None

if __name__ == "__main__":
    try:
        config = load_config()
        token = config['YandexDirect']['ACCESS_TOKEN']
        
        date_to = datetime.now().strftime('%Y-%m-%d')
        date_from = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')

        print(f"🔄 Запрос данных за {date_from} - {date_to}...")
        report_data = get_direct_report(token, date_from, date_to)
        
        if report_data:
            print("✅ Данные получены. Первые строки:")
            print(report_data.split('\n')[0])  # Заголовки
            print(report_data.split('\n')[1])  # Первая строка данных
        else:
            print("❌ Не удалось получить данные")

    except Exception as e:
        print(f"🔥 Критическая ошибка: {e}")
