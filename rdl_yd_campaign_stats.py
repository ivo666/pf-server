import requests
import logging
from datetime import datetime, timedelta

# Настройка простого логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

def get_campaign_ids(token, date):
    url = "https://api.direct.yandex.com/json/v5/reports"
    
    # ASCII-only headers
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept-Language": "en",
        "Content-Type": "application/json"
    }

    # Максимально простой запрос
    body = {
        "params": {
            "SelectionCriteria": {"DateFrom": date, "DateTo": date},
            "FieldNames": ["CampaignId"],
            "ReportName": "ad_ids",
            "ReportType": "AD_PERFORMANCE_REPORT",
            "DateRangeType": "CUSTOM_DATE",
            "Format": "TSV",
            "IncludeVAT": "YES",
            "IncludeDiscount": "NO"
        }
    }

    try:
        logger.info(f"Requesting campaign IDs for {date}")
        response = requests.post(url, headers=headers, json=body, timeout=30)
        
        if response.status_code == 200:
            return response.text
        else:
            logger.error(f"API error: {response.status_code} - {response.text}")
            return None
            
    except Exception as e:
        logger.error(f"Request failed: {str(e)}")
        return None

def print_campaign_ids(data):
    if not data:
        print("No data received")
        return
    
    print("\nCampaign IDs:")
    print("=" * 30)
    for line in data.split('\n'):
        if line.strip() and not line.startswith(('"', 'CampaignId', 'Total')):
            print(line.split('\t')[0])
    print("=" * 30)

if __name__ == "__main__":
    # Укажите ваш токен (замените на реальный)
    TOKEN = "y0__xCfm56NBhi4uzgg2IHdxxMB-11ibEFeXtYCgMHlML7g5RHDNA"
    
    # Дата для запроса (вчерашний день)
    report_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    
    logger.info(f"Starting report for {report_date}")
    
    data = get_campaign_ids(TOKEN, report_date)
    
    if data:
        print_campaign_ids(data)
    else:
        logger.error("Failed to get campaign IDs")
    
    logger.info("Script finished")
