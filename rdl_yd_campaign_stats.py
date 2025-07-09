import requests
import logging
import time
from datetime import datetime, timedelta

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

def get_campaign_and_ad_ids(token, date, max_retries=3):
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
            "FieldNames": ["CampaignId", "AdId"],
            "ReportName": "campaign_and_ad_ids_report",
            "ReportType": "AD_PERFORMANCE_REPORT",
            "DateRangeType": "CUSTOM_DATE",
            "Format": "TSV",
            "IncludeVAT": "YES",
            "IncludeDiscount": "NO",
            "Types": ["TEXT_AD"]  # Указываем тип объявлений
        }
    }

    try:
        logger.info(f"Requesting Campaign and Ad IDs for {date}")
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
            retry_count = 0
            while retry_count < max_retries:
                time.sleep(15)
                download_url = response.headers.get('Location')
                if download_url:
                    logger.info(f"Trying to download report (attempt {retry_count + 1})")
                    download_response = requests.get(download_url, headers=headers)
                    if download_response.status_code == 200:
                        return download_response.text
                    else:
                        logger.warning(f"Download failed: {download_response.status_code}")
                retry_count += 1
            logger.error("Max retries reached. Report is not ready.")
            return None
        else:
            logger.error(f"API error: {response.status_code} - {response.text}")
            return None
        
    except Exception as e:
        logger.error(f"Request failed: {str(e)}")
        return None

def print_campaign_and_ad_ids(data):
    if not data:
        print("No data received")
        return
    
    print("\nCampaign and Ad IDs Report:")
    print("=" * 60)
    print("{:<15} | {:<15}".format("Campaign ID", "Ad ID"))
    print("=" * 60)
    
    for line in data.split('\n'):
        if line.strip() and not line.startswith(('"', 'CampaignId', 'Total')):
            parts = line.strip().split('\t')
            if len(parts) >= 2:
                print("{:<15} | {:<15}".format(parts[0], parts[1]))
    
    print("=" * 60)

if __name__ == "__main__":
    TOKEN = "y0__xCfm56NBhi4uzgg2IHdxxMB-11ibEFeXtYCgMHlML7g5RHDNA"
    report_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    
    logger.info(f"Starting report for {report_date}")
    data = get_campaign_and_ad_ids(TOKEN, report_date)
    
    if data:
        print_campaign_and_ad_ids(data)
    else:
        logger.error("Failed to get data")
    
    logger.info("Script finished")
