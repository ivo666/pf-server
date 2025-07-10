import requests
import configparser

# Загрузка конфигурации
config = configparser.ConfigParser()
config.read('config.ini')
token = config['YandexDirect']['ACCESS_TOKEN']

headers = {
    "Authorization": f"Bearer {token}",
    "Accept-Language": "ru",
    "Content-Type": "application/json"
}

body = {
    "params": {
        "SelectionCriteria": {"DateFrom": "2025-07-08", "DateTo": "2025-07-08"},
        "FieldNames": ["Date", "Clicks"],
        "ReportType": "AD_PERFORMANCE_REPORT",
        "DateRangeType": "CUSTOM_DATE",
        "Format": "TSV",
        "IncludeVAT": "YES"
    }
}

try:
    response = requests.post(
        "https://api.direct.yandex.com/json/v5/reports",
        headers=headers,
        json=body,
        timeout=30
    )
    print(f"Status: {response.status_code}")
    print(response.text)
except Exception as e:
    print(f"Error: {e}")
