import requests
import configparser
import time
from datetime import datetime

# Загрузка конфигурации
config = configparser.ConfigParser()
config.read('config.ini')
token = config['YandexDirect']['ACCESS_TOKEN']

# Уникальное имя отчета (используем timestamp)
report_name = f"report_{int(time.time())}"

headers = {
    "Authorization": f"Bearer {token}",
    "Accept-Language": "ru",
    "Content-Type": "application/json"
}

body = {
    "params": {
        "SelectionCriteria": {
            "DateFrom": "2025-07-08",
            "DateTo": "2025-07-08"
        },
        "FieldNames": ["Date", "CampaignId", "Clicks"],
        "ReportName": report_name,  # Обязательное поле!
        "ReportType": "AD_PERFORMANCE_REPORT",
        "DateRangeType": "CUSTOM_DATE",
        "Format": "TSV",
        "IncludeVAT": "YES",
        "IncludeDiscount": "NO"
    }
}

try:
    print("🟢 Отправка запроса к API Яндекс.Директ...")
    response = requests.post(
        "https://api.direct.yandex.com/json/v5/reports",
        headers=headers,
        json=body,
        timeout=30
    )
    
    print(f"🔵 Статус ответа: {response.status_code}")
    
    if response.status_code == 200:
        print("✅ Данные успешно получены:")
        print(response.text[:500] + "...")  # Вывод первых 500 символов
    elif response.status_code == 201:
        print("🔄 Отчет формируется, попробуйте запросить позже")
    else:
        print(f"❌ Ошибка API: {response.text}")
        
except Exception as e:
    print(f"💥 Критическая ошибка: {e}")
