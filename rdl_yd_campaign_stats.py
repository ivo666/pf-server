import requests
import logging
import time
from datetime import datetime, timedelta

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

def get_campaign_ids(token, date):
    url = "https://api.direct.yandex.com/json/v5/reports"
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept-Language": "en",
        "Content-Type": "application/json"
    }

    body = {
        "params": {
            "SelectionCriteria": {
                "DateFrom": date,
                "DateTo": date
            },
            "FieldNames": ["CampaignId"],  # Запрашиваем только CampaignId
            "ReportName": "campaign_ids_report",
            "ReportType": "AD_PERFORMANCE_REPORT",
            "DateRangeType": "CUSTOM_DATE",
            "Format": "TSV",
            "IncludeVAT": "YES",
            "IncludeDiscount": "NO"
        }
    }

    try:
        logger.info(f"Requesting Campaign IDs for {date}")
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
            time.sleep(15)
            download_url = response.headers.get('Location')
            if download_url:
                return requests.get(download_url, headers=headers).text
        logger.error(f"API error: {response.status_code} - {response.text}")
        return None
        
    except Exception as e:
        logger.error(f"Request failed: {str(e)}")
        return None

def print_campaign_ids(data):
    if not data:
        print("No data received")
        return
    
    print("\nCampaign IDs Report:")
    print("=" * 40)
    print("{:<15}".format("Campaign ID"))
    print("=" * 40)
    
    # Пропускаем заголовки и пустые строки
    for line in data.split('\n'):
        if line.strip() and not line.startswith(('"', 'CampaignId', 'Total')):
            campaign_id = line.strip()  # В TSV-отчете будет одна колонка
            print("{:<15}".format(campaign_id))
    print("=" * 40)

if __name__ == "__main__":
    # Укажите ваш токен
    TOKEN = "y0__xCfm56NBhi4uzgg2IHdxxMB-11ibEFeXtYCgMHlML7g5RHDNA"
    
    # Дата для запроса (вчерашний день)
    report_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    
    logger.info(f"Starting Campaign IDs report for {report_date}")
    
    data = get_campaign_ids(TOKEN, report_date)
    
    if data:
        print_campaign_ids(data)
    else:
        logger.error("Failed to get Campaign IDs")
    
    logger.info("Script finished")
