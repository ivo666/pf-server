import requests
import logging
import time
import hashlib
from datetime import datetime, timedelta
from io import StringIO

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Константы
TOKEN = "y0__xCfm56NBhi4uzgg2IHdxxMB-11ibEFeXtYCgMHlML7g5RHDNA"
REQUEST_DELAY = 15  # Пауза между запросами в секундах
MAX_RETRIES = 3
RETRY_DELAY = 30  # Начальная задержка между повторными попытками в секундах

def parse_tsv_data(tsv_data):
    """Парсит TSV данные из API и возвращает список словарей"""
    try:
        if not tsv_data or not tsv_data.strip():
            logger.error("Empty TSV data received")
            return None
            
        lines = [line for line in tsv_data.split('\n') if line.strip() and not line.startswith('Date\t')]
        
        data = []
        for line in lines:
            try:
                parts = line.strip().split('\t')
                if len(parts) >= 11:  # Увеличили количество полей
                    record = {
                        'Date': parts[0],
                        'CampaignId': parts[1],
                        'CampaignName': parts[2],
                        'AdId': parts[3],
                        'Clicks': parts[4],
                        'Impressions': parts[5],
                        'Cost': parts[6],
                        'Placement': parts[7],
                        'CriteriaType': parts[8],
                        'ClientLogin': parts[9],
                        'AvgClickPosition': parts[10]
                    }
                    data.append(record)
            except Exception as line_error:
                logger.error(f"Error parsing line: {line}. Error: {str(line_error)}")
                continue
        
        if not data:
            logger.error("No valid data found in TSV")
            return None
            
        return data
    except Exception as e:
        logger.error(f"Failed to parse TSV data: {str(e)}")
        return None

def get_campaign_stats(token, date_from, date_to, max_retries=3):
    url = "https://api.direct.yandex.com/json/v5/reports"
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept-Language": "en",
        "Content-Type": "application/json"
    }

    report_hash = hashlib.md5(f"{date_from}_{date_to}".encode()).hexdigest()[:8]
    report_name = f"campaign_stats_{report_hash}"

    body = {
        "params": {
            "SelectionCriteria": {
                "DateFrom": date_from,
                "DateTo": date_to
            },
            "FieldNames": [
                "Date",
                "CampaignId",
                "CampaignName",
                "AdId",
                "Clicks",
                "Impressions",
                "Cost",
                "Placement",
                "CriteriaType",
                "ClientLogin",
                "AvgClickPosition"
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
        logger.info(f"Requesting Campaign stats from {date_from} to {date_to}")
        response = requests.post(
            url,
            headers=headers,
            json=body,
            timeout=120
        )
        
        if response.status_code == 200:
            return response.text
        elif response.status_code == 201:
            logger.info("Report is being generated, waiting...")
            retry_count = 0
            while retry_count < max_retries:
                wait_time = RETRY_DELAY * (retry_count + 1)
                logger.info(f"Waiting {wait_time} seconds before retry...")
                time.sleep(wait_time)
                
                if 'Retry-After' in response.headers:
                    wait_time = int(response.headers['Retry-After'])
                    logger.info(f"Server requested to wait {wait_time} seconds")
                    time.sleep(wait_time)
                
                download_url = response.headers.get('Location')
                if download_url:
                    logger.info(f"Trying to download report (attempt {retry_count + 1})")
                    download_response = requests.get(download_url, headers=headers, timeout=120)
                    if download_response.status_code == 200:
                        return download_response.text
                    else:
                        logger.warning(f"Download failed: {download_response.status_code}")
                else:
                    logger.warning("Download URL not found, trying to request report again")
                    response = requests.post(
                        url,
                        headers=headers,
                        json=body,
                        timeout=120
                    )
                    if response.status_code == 200:
                        return response.text
                
                retry_count += 1
            logger.error(f"Max retries ({max_retries}) reached. Report is not ready.")
            return None
        else:
            logger.error(f"API error: {response.status_code} - {response.text}")
            return None
    except Exception as e:
        logger.error(f"Request failed: {str(e)}")
        return None

def print_data_as_table(data):
    """Выводит данные в виде таблицы в терминал"""
    if not data:
        print("No data to display")
        return
    
    # Определяем ширину колонок
    headers = data[0].keys()
    col_widths = {header: len(header) for header in headers}
    
    for row in data:
        for header, value in row.items():
            if len(str(value)) > col_widths[header]:
                col_widths[header] = len(str(value))
    
    # Печатаем заголовки
    header_line = " | ".join([f"{header:<{col_widths[header]}}" for header in headers])
    print(header_line)
    print("-" * len(header_line))
    
    # Печатаем данные
    for row in data:
        row_line = " | ".join([f"{str(row[header]):<{col_widths[header]}}" for header in headers])
        print(row_line)

if __name__ == "__main__":
    # Устанавливаем даты (с 01.07.2025 по 08.07.2025)
    start_date = "2025-07-01"
    end_date = "2025-07-08"
    
    logger.info(f"Requesting data from {start_date} to {end_date}")
    
    # Добавляем паузу перед первым запросом
    logger.info(f"Waiting {REQUEST_DELAY} seconds before request...")
    time.sleep(REQUEST_DELAY)
    
    data = get_campaign_stats(TOKEN, start_date, end_date)
    
    if data:
        parsed_data = parse_tsv_data(data)
        if parsed_data:
            print("\nReport data:")
            print_data_as_table(parsed_data)
        else:
            logger.error("Failed to parse data")
    else:
        logger.error("Failed to get data")
    
    logger.info("Script finished")
