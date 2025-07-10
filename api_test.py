response = requests.post(
    "https://api.direct.yandex.com/json/v5/reports",
    headers=headers,
    json={
        "params": {
            "SelectionCriteria": {"DateFrom": "2025-07-05", "DateTo": "2025-07-05"},
            "FieldNames": ["Date", "Clicks"],
            "ReportType": "AD_PERFORMANCE_REPORT",
            "DateRangeType": "CUSTOM_DATE",
            "Format": "TSV"
        }
    }
)
print(response.status_code, response.text)
