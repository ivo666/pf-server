import requests
import json
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
    
    # Используем ASCII-совместимые заголовки
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept-Language": "en",  # Изменили на английский
        "Content-Type": "application/json"
    }

    report_body = {
        "params": {
            "SelectionCriteria": {
                "DateFrom": date_from,
                "DateTo": date_to
            },
            "FieldNames": ["AdId", "Clicks"],
            "ReportName": "Simple_Ad_Report",  # Только ASCII символы
            "ReportType": "AD_PERFORMANCE_REPORT",
            "DateRangeType": "CUSTOM_DATE",
            "Format": "TSV",
            "IncludeVAT": "YES",
            "IncludeDiscount": "NO"
        }
    }

    try:
        logger.debug("Подготовка запроса...")
        
        # Явное кодирование в UTF-8
        json_data = json.dumps(report_body, ensure_ascii=True).encode('utf-8')
        
        # Создаем сессию с правильными заголовками
        session = requests.Session()
        session.headers.update(headers)
        
        logger.debug("Отправка запроса...")
        response = session.post(
            url,
            data=json_data,  # Используем data вместо json
            timeout=60
        )
        
        logger.debug(f"Статус ответа: {response.status_code}")
        
        if response.status_code == 200:
            return response.text
        elif response.status_code == 201:
            download_url = response.headers.get('Location')
            if download_url:
                logger.debug("Получен URL для загрузки")
                return download_report(download_url, headers)
            logger.error("Location header отсутствует")
            return None
        else:
            logger.error(f"Ошибка API: {response.text}")
            return None
            
    except Exception as e:
        logger.error(f"Ошибка запроса: {str(e)}", exc_info=True)
        return None

def download_report(url, headers):
    try:
        session = requests.Session()
        session.headers.update(headers)
        response = session.get(url, timeout=60)
        response.raise_for_status()
        return response.text
    except Exception as e:
        logger.error(f"Ошибка загрузки: {str(e)}")
        return None

if __name__ == "__main__":
    try:
        # Укажите ваш токен здесь
        token = "y0__xCfm56NBhi4uzgg2IHdxxMB-11ibEFeXtYCgMHlML7g5RHDNA"
        
        # Тестовый период - вчерашний день
        start_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        end_date = start_date

        logger.info(f"Запрос данных за {start_date}")
        
        data = get_direct_report(token, start_date, end_date)
        
        if data:
            logger.info("Данные получены:")
            print("="*50)
            print(data)
            print("="*50)
        else:
            logger.error("Данные не получены")

    except Exception as e:
        logger.critical(f"Фатальная ошибка: {str(e)}", exc_info=True)
    finally:
        logger.info("Работа завершена")
