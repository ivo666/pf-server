import requests
import json
import time
import logging
from datetime import datetime, timedelta

# Настройка логирования
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

def get_direct_report(token, date_from, date_to):
    url = "https://api.direct.yandex.com/json/v5/reports"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept-Language": "en",
        "Content-Type": "application/json"
    }

    report_body = {
        "params": {
            "SelectionCriteria": {
                "DateFrom": date_from,
                "DateTo": date_to
            },
            "FieldNames": ["AdId", "Clicks"],
            "ReportName": "Simple_Ad_Report",
            "ReportType": "AD_PERFORMANCE_REPORT",
            "DateRangeType": "CUSTOM_DATE",
            "Format": "TSV",
            "IncludeVAT": "YES",
            "IncludeDiscount": "NO"
        }
    }

    try:
        logger.debug("Подготовка запроса...")
        response = requests.post(
            url,
            headers=headers,
            json=report_body,
            timeout=60
        )
        
        logger.debug(f"Статус ответа: {response.status_code}")
        logger.debug(f"Заголовки ответа: {response.headers}")

        if response.status_code == 200:
            return response.text
        elif response.status_code == 201:
            # Проверяем несколько возможных заголовков для URL отчета
            download_url = (response.headers.get('Location') or 
                          response.headers.get('location') or
                          response.headers.get('download-url'))
            
            if download_url:
                logger.debug(f"Получен URL для загрузки: {download_url}")
                time.sleep(10)  # Даем время на формирование отчета
                return download_report(download_url, headers)
            else:
                # Пробуем получить URL из тела ответа
                try:
                    response_data = response.json()
                    download_url = response_data.get('result', {}).get('download_url')
                    if download_url:
                        logger.debug(f"Получен URL из тела ответа: {download_url}")
                        time.sleep(10)
                        return download_report(download_url, headers)
                except ValueError:
                    pass
                
                logger.error("Не удалось получить URL для загрузки отчета")
                logger.error("Попробуйте увеличить время ожидания или проверить лимиты API")
                return None
        else:
            logger.error(f"Ошибка API: {response.text}")
            return None
            
    except Exception as e:
        logger.error(f"Ошибка запроса: {str(e)}", exc_info=True)
        return None

def download_report(url, headers):
    try:
        logger.debug(f"Загрузка отчета по URL: {url}")
        response = requests.get(url, headers=headers, timeout=60)
        response.raise_for_status()
        return response.text
    except Exception as e:
        logger.error(f"Ошибка загрузки отчета: {str(e)}")
        return None

if __name__ == "__main__":
    try:
        # Укажите ваш токен здесь
        token = "ВАШ_ТОКЕН_ЯНДЕКС_ДИРЕКТ"
        
        # Тестовый период - вчерашний день
        start_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        end_date = start_date

        logger.info(f"Запрос данных за {start_date}")
        
        # Пробуем несколько раз с интервалом
        max_retries = 3
        for attempt in range(max_retries):
            logger.info(f"Попытка {attempt + 1} из {max_retries}")
            data = get_direct_report(token, start_date, end_date)
            
            if data:
                logger.info("Данные получены:")
                print("="*50)
                print(data)
                print("="*50)
                break
            else:
                if attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 10
                    logger.info(f"Ожидание {wait_time} секунд перед повторной попыткой...")
                    time.sleep(wait_time)
        else:
            logger.error("Данные не получены после всех попыток")

    except Exception as e:
        logger.critical(f"Фатальная ошибка: {str(e)}", exc_info=True)
    finally:
        logger.info("Работа завершена")
