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
                "AdId",
                "AdName",
                "CampaignId",
                "CampaignName",
                "Clicks",
                "Cost",
                "Impressions"
            ],
            "ReportName": f"Ad report {datetime.now().strftime('%Y-%m-%d %H:%M')}",
            "ReportType": "AD_PERFORMANCE_REPORT",
            "DateRangeType": "CUSTOM_DATE",
            "Format": "TSV",
            "IncludeVAT": "YES",
            "IncludeDiscount": "NO"
        }
    }

    try:
        logger.info(f"Запрос отчета по объявлениям за период {date_from} — {date_to}")
        
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
        print("Нет данных объявлений в выбранном периоде")
        return

    headers = lines[0].split('\t')
    rows = [line.split('\t') for line in lines[1:]]
    
    # Форматируем заголовки
    header_format = "{:<12} | {:<12} | {:<12} | {:<30} | {:<8} | {:<12} | {:<12}"
    row_format = "{:<12} | {:<12} | {:<12} | {:<30} | {:<8} | {:<12.2f} | {:<12}"
    
    print("\n" + "=" * 120)
    print(header_format.format(
        "Date", "Ad ID", "Campaign ID", "Campaign Name", "Clicks", 
        "Cost (RUB)", "Impressions"
    ))
    print("=" * 120)
    
    for row in rows:
        try:
            date = row[0]
            ad_id = row[1]
            ad_name = row[2][:20] if len(row) > 2 else "N/A"
            campaign_id = row[3] if len(row) > 3 else "N/A"
            campaign_name = row[4][:30] if len(row) > 4 else "N/A"
            clicks = int(row[5]) if len(row) > 5 else 0
            cost = float(row[6]) if len(row) > 6 else 0
            impressions = int(row[7]) if len(row) > 7 else 0
            
            print(row_format.format(
                date, ad_id, campaign_id, campaign_name,
                clicks, cost, impressions
            ))
        except (IndexError, ValueError) as e:
            logger.warning(f"Ошибка обработки строки: {row} - {str(e)}")
    
    print("=" * 120)
    print(f"Всего объявлений: {len(rows)}")
    print("=" * 120 + "\n")

if __name__ == "__main__":
    try:
        # Укажите ваш токен Яндекс.Директ
        token = "ВАШ_ТОКЕН_ЯНДЕКС_ДИРЕКТ"
        
        # Установите нужный период
        start_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        end_date = start_date  # За один день

        logger.info(f"Начало выгрузки данных по объявлениям за {start_date}")
        
        data = get_direct_report(token, start_date, end_date)
        
        if data:
            print_beautiful_report(data)
        else:
            logger.error("Не удалось получить данные из API")

    except Exception as e:
        logger.critical(f"Критическая ошибка: {str(e)}")
    finally:
        logger.info("Завершение работы скрипта")
