import requests
import logging
import time
import hashlib
from datetime import datetime, timedelta

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

def get_campaign_stats(token, date, max_retries=3):
    url = "https://api.direct.yandex.com/json/v5/reports"
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept-Language": "en",
        "Content-Type": "application/json"
    }

    # Генерируем уникальное имя отчета
    report_hash = hashlib.md5(date.encode()).hexdigest()[:8]
    report_name = f"campaign_stats_{report_hash}"

    body = {
        "params": {
            "SelectionCriteria": {
                "DateFrom": date,
                "DateTo": date
            },
            "FieldNames": [
                "Date",
                "CampaignId",
                "CampaignName",  # Добавлено
                "AdId",
                "Clicks",       # Добавлено
                "Impressions",  # Добавлено
                "Cost"          # Добавлено
            ],
            "ReportName": report_name,
            "ReportType": "AD_PERFORMANCE_REPORT",
            "DateRangeType": "CUSTOM_DATE",
            "Format": "TSV",
            "IncludeVAT": "YES",
            "IncludeDiscount": "NO"
        }
    }

    try:
        logger.info(f"Requesting Campaign stats for {date}")
        response = requests.post(
            url,
            headers=headers,
            json=body,
            timeout=60
        )
        
        if response.status_code == 200:
            return response.text
        elif response.status_code == 201:
            logger.info("Report is being generated, waiting...")
            retry_count = 0
            while retry_count < max_retries:
                wait_time = 30 * (retry_count + 1)
                logger.info(f"Waiting {wait_time} seconds before retry...")
                time.sleep(wait_time)
                
                download_url = response.headers.get('Location')
                if download_url:
                    logger.info(f"Trying to download report (attempt {retry_count + 1})")
                    download_response = requests.get(download_url, headers=headers, timeout=60)
                    if download_response.status_code == 200:
                        return download_response.text
                    else:
                        logger.warning(f"Download failed: {download_response.status_code}")
                else:
                    logger.warning("Download URL not found.")
                    break
                retry_count += 1
            logger.error(f"Max retries ({max_retries}) reached. Report is not ready.")
            return None
        else:
            logger.error(f"API error: {response.status_code} - {response.text}")
            return None
        
    except Exception as e:
        logger.error(f"Request failed: {str(e)}")
        return None

def print_campaign_stats(data):
    if not data:
        print("No data received")
        return
    
    print("\nCampaign Performance Report:")
    print("=" * 120)
    print("{:<12} | {:<12} | {:<30} | {:<12} | {:<8} | {:<12} | {:<12}".format(
        "Date", "Campaign ID", "Campaign Name", "Ad ID", "Clicks", "Cost", "Impressions"
    ))
    print("=" * 120)
    
    for line in data.split('\n'):
        if line.strip() and not line.startswith(('Date', 'Total')):
            parts = line.strip().split('\t')
            if len(parts) >= 7:
                date = parts[0]
                campaign_id = parts[1]
                campaign_name = parts[2][:30]  # Обрезаем длинные названия
                ad_id = parts[3]
                clicks = parts[4]
                cost = f"{float(parts[6]):.2f}"  # Форматируем стоимость
                impressions = parts[5]
                
                print("{:<12} | {:<12} | {:<30} | {:<12} | {:<8} | {:<12} | {:<12}".format(
                    date, campaign_id, campaign_name, ad_id, clicks, cost, impressions
                ))
    
    print("=" * 120)

if __name__ == "__main__":
    TOKEN = "y0__xCfm56NBhi4uzgg2IHdxxMB-11ibEFeXtYCgMHlML7g5RHDNA"
    report_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    
    logger.info(f"Starting report for {report_date}")
    data = get_campaign_stats(TOKEN, report_date)
    
    if data:
        print_campaign_stats(data)
    else:
        logger.error("Failed to get data")
    
    logger.info("Script finished")
