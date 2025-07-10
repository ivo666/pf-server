import requests
from datetime import datetime

# 1. Проверка подключения библиотек
print("🟢 Проверка работы скрипта")

# 2. Тестовый запрос к API Яндекс.Директ
try:
    # Замените YOUR_ACCESS_TOKEN на реальный токен
    headers = {
        "Authorization": "Bearer YOUR_ACCESS_TOKEN",
        "Accept-Language": "ru"
    }

    # Тестовые параметры запроса (дата 2025-07-08)
    body = {
        "params": {
            "SelectionCriteria": {
                "DateFrom": "2025-07-08",
                "DateTo": "2025-07-08"
            },
            "FieldNames": ["Date", "CampaignId", "Clicks"],
            "ReportType": "AD_PERFORMANCE_REPORT",
            "DateRangeType": "CUSTOM_DATE",
            "Format": "TSV",
            "IncludeVAT": "YES"
        }
    }

    print("📊 Отправка тестового запроса...")
    response = requests.post(
        "https://api.direct.yandex.com/json/v5/reports",
        headers=headers,
        json=body,
        timeout=30
    )

    # Вывод результатов
    print(f"🟢 Статус ответа: {response.status_code}")
    print(f"🟢 Ответ сервера: {response.text[:200]}...")  # Выводим первые 200 символов

except Exception as e:
    print(f"🔴 Ошибка: {e}")
