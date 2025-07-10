import requests
import time
from datetime import datetime, timedelta

YANDEX_TOKEN = "y0__xCfm56NBhi4uzgg2IHdxxMB-11ibEFeXtYCgMHlML7g5RHDNA"  # Замените на реальный токен
DATE = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")  # Данные за вчера
MAX_RETRIES = 3
RETRY_DELAY = 30

def get_campaign_stats(token, date):
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept-Language": "ru",
        "Content-Type": "application/json"
    }

    body = {
        "params": {
            "SelectionCriteria": {"DateFrom": date, "DateTo": date},
            "FieldNames": ["Date", "CampaignId", "Clicks", "Cost"],
            "ReportName": "test_report",
            "ReportType": "AD_PERFORMANCE_REPORT",
            "Format": "TSV"
        }
    }

    for attempt in range(MAX_RETRIES):
        try:
            response = requests.post(
                "https://api.direct.yandex.com/json/v5/reports",
                headers=headers,
                json=body,
                timeout=120
            )
            if response.status_code == 200:
                return response.text
            elif response.status_code == 201:
                print(f"Ожидаем готовность отчёта... (попытка {attempt + 1})")
                time.sleep(RETRY_DELAY)
            else:
                print(f"Ошибка: {response.status_code}\n{response.text}")
                return None
        except Exception as e:
            print(f"Ошибка соединения: {e}")
            return None
    return None

def main():
    print(f"Запрос данных за {DATE}")
    data = get_campaign_stats(YANDEX_TOKEN, DATE)
    print("Результат:\n", data if data else "Данные не получены")

if __name__ == "__main__":
    main()
