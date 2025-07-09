import requests
import json
import time
import logging
from datetime import datetime, timedelta

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
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

    report_body = {
        "params": {
            "SelectionCriteria": {
                "DateFrom": date_from,
                "DateTo": date_to
            },
            "FieldNames": [
                "Date",
                "CampaignId",
                "CampaignName",
                "Clicks",
                "Cost",
                "Impressions",
                "Ctr"
            ],
            "ReportName": f"Campaign report {datetime.now().strftime('%Y-%m-%d %H:%M')}",
            "ReportType": "CAMPAIGN_PERFORMANCE_REPORT",
            "DateRangeType": "CUSTOM_DATE",
            "Format": "TSV",
            "IncludeVAT": "YES",
            "IncludeDiscount": "NO"
        }
    }

    try:
        logger.info(f"Запрос отчета за период {date_from} — {date_to}")
        
        response = requests.post(
            url,
            headers=headers,
            json=report_body,
            timeout=60
        )
        
        if response.status_code == 200:
            return response.text
        elif response.status_code == 201:
            download_url = response.headers.get('Location')
            if download_url:
                logger.info("Отчет формируется, ожидаем 30 секунд...")
                time.sleep(30)
                return download_report(download_url, headers)
            logger.error("Не получен URL для скачивания")
            return None
        else:
            logger.error(f"Ошибка API: {response.text}")
            return None
            
    except requests.exceptions.RequestException as e:
        logger.error(f"Ошибка запроса: {str(e)}")
        return None

def download_report(url, headers):
    try:
        response = requests.get(url, headers=headers, timeout=60)
        response.raise_for_status()
        return response.text
    except requests.exceptions.RequestException as e:
        logger.error(f"Ошибка загрузки отчета: {str(e)}")
        return None

def print_beautiful_report(data):
    if not data:
        print("Нет данных для отображения")
        return

    lines = [line for line in data.split('\n') if line.strip() and not any(
        line.startswith(x) for x in ('"', 'Total', 'Date\t'))
    ]
    
    if not lines:
        print("Нет данных кампаний в выбранном периоде")
        return

    headers = lines[0].split('\t')
    rows = [line.split('\t') for line in lines[1:]]
    
    # Форматируем заголовки
    header_format = "{:<12} | {:<12} | {:<30} | {:<8} | {:<12} | {:<12} | {:<8}"
    row_format = "{:<12} | {:<12} | {:<30} | {:<8} | {:<12.2f} | {:<12} | {:<8.2f}%"
    
    print("\n" + "=" * 100)
    print(header_format.format(
        "Date", "Campaign ID", "Campaign Name", "Clicks", 
        "Cost (RUB)", "Impressions", "CTR"
    ))
    print("=" * 100)
    
    for row in rows:
        try:
            date = row[0]
            campaign_id = row[1]
            campaign_name = row[2][:30]  # Обрезаем длинные названия
            clicks = int(row[3])
            cost = float(row[4])
            impressions = int(row[5])
            ctr = float(row[6].strip('%')) if row[6] else 0
            
            print(row_format.format(
                date, campaign_id, campaign_name, clicks,
                cost, impressions, ctr
            ))
        except (IndexError, ValueError) as e:
            logger.warning(f"Ошибка обработки строки: {row} - {str(e)}")
    
    print("=" * 100)
    print(f"Всего кампаний: {len(rows)}")
    print("=" * 100 + "\n")

if __name__ == "__main__":
    try:
        # Укажите ваш токен Яндекс.Директ
        token = "y0__xCfm56NBhi4uzgg2IHdxxMB-11ibEFeXtYCgMHlML7g5RHDNA"
        
        # Установите нужный период
        start_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        end_date = start_date  # За один день

        logger.info(f"Начало выгрузки данных за {start_date}")
        
        data = get_direct_report(token, start_date, end_date)
        
        if data:
            print_beautiful_report(data)
        else:
            logger.error("Не удалось получить данные из API")

    except Exception as e:
        logger.critical(f"Критическая ошибка: {str(e)}")
    finally:
        logger.info("Завершение работы скрипта")
