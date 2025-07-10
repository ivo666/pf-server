import requests
from datetime import datetime

# Константы
YANDEX_TOKEN = "ваш_токен"  # Замените на реальный токен
DATE = "2025-07-01"

def get_campaign_stats(token, date):
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept-Language": "ru",
        "Content-Type": "application/json"
    }

    body = {
        "params": {
            "SelectionCriteria": {"DateFrom": date, "DateTo": date},
            "FieldNames": [
                "Date", "CampaignId", "CampaignName", "AdId",
                "Impressions", "Clicks", "Cost", "AvgClickPosition",
                "Device", "LocationOfPresenceId", "MatchType", "Slot"
            ],
            "ReportName": "ad_performance_report",
            "ReportType": "AD_PERFORMANCE_REPORT",
            "DateRangeType": "CUSTOM_DATE",
            "Format": "TSV",
            "IncludeVAT": "YES",
            "IncludeDiscount": "NO"
        }
    }

    response = requests.post(
        "https://api.direct.yandex.com/json/v5/reports",
        headers=headers,
        json=body,
        timeout=120
    )

    if response.status_code == 200:
        return response.text
    else:
        print(f"Ошибка API: {response.status_code} - {response.text}")
        return None

def main():
    print(f"Запрашиваем данные за {DATE}")
    
    raw_data = get_campaign_stats(YANDEX_TOKEN, DATE)
    if not raw_data:
        print("Нет данных")
        return
    
    print("\nПолученные данные:")
    print(raw_data)

if __name__ == "__main__":
    main()
