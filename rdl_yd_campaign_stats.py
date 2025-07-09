import requests
import json
import logging
from datetime import datetime, timedelta

# Настройка логирования
logging.basicConfig(
    level=logging.DEBUG,  # Изменили на DEBUG для подробного лога
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

def get_direct_report(token, date_from, date_to):
    url = "https://api.direct.yandex.com/json/v5/reports"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept-Language": "ru",
        "Content-Type": "application/json"
    }

    # Максимально простой запрос
    report_body = {
        "params": {
            "SelectionCriteria": {
                "DateFrom": date_from,
                "DateTo": date_to
            },
            "FieldNames": ["AdId", "Clicks"],
            "ReportName": "Simple Ad Report",
            "ReportType": "AD_PERFORMANCE_REPORT",
            "DateRangeType": "CUSTOM_DATE",
            "Format": "TSV",
            "IncludeVAT": "YES",
            "IncludeDiscount": "NO"
        }
    }

    try:
        logger.debug(f"Отправляемый запрос: {json.dumps(report_body, indent=2, ensure_ascii=False)}")
        
        response = requests.post(
            url,
            headers=headers,
            json=report_body,
            timeout=60
        )
        
        logger.debug(f"Получен ответ: {response.status_code}")
        logger.debug(f"Заголовки ответа: {response.headers}")
        
        if response.status_code == 200:
            logger.debug("Успешно получен отчет")
            return response.text
        elif response.status_code == 201:
            logger.debug("Отчет формируется асинхронно")
            download_url = response.headers.get('Location')
            if download_url:
                logger.debug(f"URL для загрузки: {download_url}")
                return download_report(download_url, headers)
            else:
                logger.error("Location header отсутствует в ответе")
                return None
        else:
            logger.error(f"Ошибка API. Полный ответ: {response.text}")
            return None
            
    except requests.exceptions.RequestException as e:
        logger.error(f"Ошибка соединения: {str(e)}")
        return None

def download_report(url, headers):
    try:
        logger.debug(f"Загружаем отчет по URL: {url}")
        response = requests.get(url, headers=headers, timeout=60)
        response.raise_for_status()
        return response.text
    except requests.exceptions.RequestException as e:
        logger.error(f"Ошибка загрузки отчета: {str(e)}")
        return None

if __name__ == "__main__":
    try:
        # Укажите ваш токен здесь
        token = "ВАШ_ТОКЕН_ЯНДЕКС_ДИРЕКТ"
        
        # Тестовый период - вчерашний день
        start_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        end_date = start_date

        logger.info(f"Пробуем получить данные за {start_date}")
        
        data = get_direct_report(token, start_date, end_date)
        
        if data:
            logger.info("Успешно получены данные:")
            print("="*50)
            print(data)
            print("="*50)
        else:
            logger.error("Не удалось получить данные")

    except Exception as e:
        logger.critical(f"Критическая ошибка: {str(e)}", exc_info=True)
    finally:
        logger.info("Скрипт завершен")
