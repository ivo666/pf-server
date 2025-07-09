import requests
import json
import time
import logging

# Настройка логирования
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def get_direct_report(token, date_from, date_to):
    url = "https://api.direct.yandex.com/json/v5/reports"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept-Language": "ru",
        "Content-Type": "application/json"
    }

    report_body = {
        "params": {
            "SelectionCriteria": {
                "DateFrom": date_from,
                "DateTo": date_to
            },
            "FieldNames": ["CampaignId", "CampaignName"],
            "ReportName": f"Campaign report {date_from} to {date_to}",
            "ReportType": "CAMPAIGN_PERFORMANCE_REPORT",
            "DateRangeType": "CUSTOM_DATE",
            "Format": "TSV",
            "IncludeVAT": "YES",
            "IncludeDiscount": "NO"
        }
    }

    try:
        logger.info(f"\nЗапрос отчета за {date_from} — {date_to}")
        logger.debug(f"Тело запроса:\n{json.dumps(report_body, indent=2)}")
        
        response = requests.post(
            url,
            headers=headers,
            json=report_body,
            timeout=60
        )
        
        logger.debug(f"Статус ответа: {response.status_code}")
        
        if response.status_code == 200:
            logger.info("\nПолученные данные:")
            print("="*50)
            print(response.text)
            print("="*50)
            return response.text
        elif response.status_code == 201:
            download_url = response.headers.get('Location')
            if download_url:
                logger.info("Отчет формируется, ожидаем...")
                time.sleep(30)
                return download_report(download_url, headers)
            logger.error("Не получен URL для скачивания")
            return None
        else:
            logger.error(f"Ошибка API: {response.text}")
            response.raise_for_status()
            return None
            
    except requests.exceptions.RequestException as e:
        logger.error(f"Ошибка запроса: {str(e)}")
        if hasattr(e, 'response') and e.response:
            logger.error(f"Детали ошибки: {e.response.text}")
        return None

def download_report(url, headers):
    try:
        response = requests.get(url, headers=headers, timeout=60)
        response.raise_for_status()
        logger.info("\nПолученные данные (отсроченный отчет):")
        print("="*50)
        print(response.text)
        print("="*50)
        return response.text
    except requests.exceptions.RequestException as e:
        logger.error(f"Ошибка загрузки отчета: {str(e)}")
        return None

if __name__ == "__main__":
    try:
        # Укажите ваш токен напрямую (для теста)
        token = "y0__xCfm56NBhi4uzgg2IHdxxMB-11ibEFeXtYCgMHlML7g5RHDNA"
        
        # Тестовый период - 1 день
        start_date = "2025-06-10"
        end_date = "2025-06-10"

        logger.info(f"Старт выгрузки с {start_date} по {end_date}")
        data = get_direct_report(token, start_date, end_date)
        
        if not data:
            logger.error("Не удалось получить данные")

    except Exception as e:
        logger.critical(f"Фатальная ошибка: {str(e)}")
    finally:
        logger.info("Процесс завершен")
