import requests
import time
from datetime import datetime, timedelta

# Конфигурация
YANDEX_TOKEN = "y0__xCfm56NBhi4uzgg2IHdxxMB-11ibEFeXtYCgMHlML7g5RHDNA"  # Замените на реальный токен
DATE = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")  # Данные за вчера
MAX_RETRIES = 3
RETRY_DELAY = 5  # Секунды между попытками

def get_campaign_stats(token, date):
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept-Language": "ru",
        "Content-Type": "application/json"
    }

    body = {
        "params": {
            "SelectionCriteria": {
                "DateFrom": date,
                "DateTo": date
            },
            "FieldNames": [
                "Date",
                "CampaignId",
                "CampaignName",
                "Clicks",
                "Cost"
            ],
            "ReportName": "API_Report",
            "ReportType": "AD_PERFORMANCE_REPORT",
            "DateRangeType": "CUSTOM_DATE",  # Обязательное поле!
            "Format": "TSV",
            "IncludeVAT": "YES",
            "IncludeDiscount": "NO"
        }
    }

    for attempt in range(MAX_RETRIES):
        try:
            response = requests.post(
                "https://api.direct.yandex.com/json/v5/reports",
                headers=headers,
                json=body,
                timeout=30
            )

            if response.status_code == 200:
                return response.text
            elif response.status_code == 201:
                print(f"Отчет формируется... (попытка {attempt + 1})")
                time.sleep(RETRY_DELAY)
            else:
                print(f"Ошибка {response.status_code}: {response.text}")
                return None

        except Exception as e:
            print(f"Ошибка соединения: {str(e)}")
            return None

    print("Достигнуто максимальное число попыток.")
    return None

def main():
    print(f"Запрос данных за {DATE}")
    data = get_campaign_stats(YANDEX_TOKEN, DATE)

    if data:
        print("Успешно получены данные:")
        print(data)
    else:
        print("Данные не получены. Проверьте токен и параметры запроса.")

if __name__ == "__main__":
    main()
