import requests
import configparser
import time
from datetime import datetime

# Настройки
MAX_RETRIES = 3  # Максимальное количество попыток
RETRY_DELAY = 30  # Задержка между попытками (секунды)

def get_report(token, date, attempt=1):
    """Получение отчета с обработкой статуса 201"""
    report_name = f"report_{int(time.time())}"
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept-Language": "ru",
        "Content-Type": "application/json"
    }

    body = {
        "params": {
            "SelectionCriteria": {"DateFrom": date, "DateTo": date},
            "FieldNames": ["Date", "CampaignId", "Clicks", "Cost"],
            "ReportName": report_name,
            "ReportType": "AD_PERFORMANCE_REPORT",
            "DateRangeType": "CUSTOM_DATE",
            "Format": "TSV",
            "IncludeVAT": "YES"
        }
    }

    try:
        print(f"📊 Попытка {attempt}: запрос данных за {date}")
        response = requests.post(
            "https://api.direct.yandex.com/json/v5/reports",
            headers=headers,
            json=body,
            timeout=120
        )

        if response.status_code == 200:
            print("✅ Отчет успешно получен")
            return response.text
        elif response.status_code == 201:
            if attempt >= MAX_RETRIES:
                print(f"❌ Превышено максимальное количество попыток ({MAX_RETRIES})")
                return None
            print(f"🔄 Отчет формируется, повтор через {RETRY_DELAY} сек...")
            time.sleep(RETRY_DELAY)
            return get_report(token, date, attempt+1)
        else:
            print(f"❌ Ошибка API (код {response.status_code}): {response.text}")
            return None

    except Exception as e:
        print(f"💥 Ошибка соединения: {e}")
        return None

# Основной код
if __name__ == "__main__":  # Исправлено здесь
    # Загрузка конфигурации
    config = configparser.ConfigParser()
    config.read('config.ini')
    
    # Проверка наличия токена
    if 'YandexDirect' not in config or 'ACCESS_TOKEN' not in config['YandexDirect']:
        print("🔴 Ошибка: не найден ACCESS_TOKEN в config.ini")
        exit(1)

    token = config['YandexDirect']['ACCESS_TOKEN']
    
    # Дата запроса (можно изменить)
    report_date = "2025-07-08"
    
    print(f"🟢 Начало получения отчета за {report_date}")
    report_data = get_report(token, report_date)
    
    if report_data:
        print("🔵 Первые 100 символов отчета:")
        print(report_data[:100])
        # Здесь можно добавить сохранение в файл или обработку данных
    else:
        print("🔴 Не удалось получить отчет")
