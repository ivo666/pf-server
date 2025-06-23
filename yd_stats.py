import requests
import psycopg2
from datetime import datetime, timedelta
import configparser
from pathlib import Path

def load_config():
    config = configparser.ConfigParser()
    config.read(Path(__file__).parent / 'config.ini')
    return config

def get_report(token, date_from, date_to):
    url = "https://api.direct.yandex.com/json/v5/reports"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept-Language": "ru",
        "Content-Type": "application/json"
    }

    body = {
        "method": "get",  # Ключевое исправление!
        "params": {
            "SelectionCriteria": {"DateFrom": date_from, "DateTo": date_to},
            "FieldNames": ["Date", "CampaignId", "AdId", "Clicks", "Cost"],
            "ReportType": "AD_PERFORMANCE_REPORT",
            "DateRangeType": "CUSTOM_DATE",
            "Format": "TSV",
            "IncludeVAT": "YES"
        }
    }

    try:
        response = requests.post(url, headers=headers, json=body)
        response.raise_for_status()
        return response.text
    except Exception as e:
        print(f"Ошибка: {e}\nПолный ответ: {response.text}")
        return None

if __name__ == "__main__":
    config = load_config()
    token = config['YandexDirect']['ACCESS_TOKEN']
    date_to = datetime.now().strftime('%Y-%m-%d')
    date_from = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')

    if report := get_report(token, date_from, date_to):
        print(report[:500])  # Вывод первых 500 символов для проверки
    else:
        print("Не удалось получить отчет")
