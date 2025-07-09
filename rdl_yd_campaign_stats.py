import requests
import logging
from datetime import datetime, timedelta

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

def get_ad_ids(token, date):
    url = "https://api.direct.yandex.com/json/v5/reports"
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept-Language": "en",  # Английский язык для избежания проблем с кодировкой
        "Content-Type": "application/json"
    }

    body = {
        "params": {
            "SelectionCriteria": {
                "DateFrom": date,
                "DateTo": date
            },
            "FieldNames": ["CampaignId"],
            "ReportName": "ad_ids_report",
            "ReportType": "AD_PERFORMANCE_REPORT",
            "DateRangeType": "CUSTOM_DATE",
            "Format": "TSV",
            "IncludeVAT": "YES",
            "IncludeDiscount": "NO"
        }
    }

    try:
        logger.info(f"Requesting Ad IDs for {date}")
        response = requests.post(
            url,
            headers=headers,
            json=body,
            timeout=30
        )
        
        if response.status_code == 200:
            return response.text
        elif response.status_code == 201:
            logger.info("Report is being generated, waiting...")
            time.sleep(15)  # Ожидаем формирования отчета
            download_url = response.headers.get('Location')
            if download_url:
                return requests.get(download_url, headers=headers).text
        logger.error(f"API error: {response.status_code} - {response.text}")
        return None
        
    except Exception as e:
        logger.error(f"Request failed: {str(e)}")
        return None

def print_ad_ids(data):
    if not data:
        print("No data received")
        return
    
    print("\nAd IDs Report:")
    print("=" * 80)
    print("{:<12} | {:<12} | {:<40}".format("Ad ID", "Campaign ID", "Ad Name"))
    print("=" * 80)
    
    for line in data.split('\n'):
        if line.strip() and not line.startswith(('"', 'AdId', 'Total')):
            parts = line.split('\t')
            if len(parts) >= 3:
                print("{:<12} | {:<12} | {:<40}".format(
                    parts[0], 
                    parts[1], 
                    parts[2][:40]  # Обрезаем длинные названия
                ))
    print("=" * 80)

if __name__ == "__main__":
    # Укажите ваш токен
    TOKEN = "y0__xCfm56NBhi4uzgg2IHdxxMB-11ibEFeXtYCgMHlML7g5RHDNA"
    
    # Дата для запроса (вчерашний день)
    report_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    
    logger.info(f"Starting Ad IDs report for {report_date}")
    
    data = get_ad_ids(TOKEN, report_date)
    
    if data:
        print_ad_ids(data)
    else:
        logger.error("Failed to get Ad IDs")
    
    logger.info("Script finished")
