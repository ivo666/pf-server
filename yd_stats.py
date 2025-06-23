import requests
import psycopg2
from datetime import datetime, timedelta
import configparser
from pathlib import Path
import sys

def load_config():
    config = configparser.ConfigParser()
    config_path = Path(__file__).parent / 'config.ini'
    config.read(config_path)
    return config

def get_direct_report(token, date_from, date_to):
    url = "https://api.direct.yandex.com/json/v5/reports"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept-Language": "ru",
        "Content-Type": "application/json"
    }

    report_body = {
        "method": "get",
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
                "Ctr",
                "Impressions"
            ],
            "ReportName": "CampaignPerformanceReport",
            "ReportType": "CAMPAIGN_PERFORMANCE_REPORT",
            "DateRangeType": "CUSTOM_DATE",
            "Format": "TSV",
            "IncludeVAT": "YES"
        }
    }

    try:
        print("Отправляемый запрос:", report_body)
        response = requests.post(url, headers=headers, json=report_body, timeout=30)
        print("Статус ответа:", response.status_code)
        response.raise_for_status()
        return response.text
    except requests.exceptions.RequestException as e:
        print(f"Ошибка API: {e}")
        if hasattr(e, 'response') and e.response:
            print(f"Тело ответа: {e.response.text}")
        return None

def save_to_postgres(data, db_config):
    conn = None
    try:
        conn = psycopg2.connect(
            host=db_config['HOST'],
            database=db_config['DATABASE'],
            user=db_config['USER'],
            password=db_config['PASSWORD'],
            port=db_config['PORT']
        )
        cur = conn.cursor()

        # Создаем таблицу если не существует
        cur.execute("""
            CREATE TABLE IF NOT EXISTS row.yandex_direct_stats (
                date DATE,
                campaign_id BIGINT,
                campaign_name TEXT,
                clicks INTEGER,
                cost DECIMAL(15, 2),
                ctr DECIMAL(5, 2),
                impressions INTEGER
            )
        """)

        lines = data.strip().split('\n')
        processed_rows = 0
        
        for line in lines:
            if not line.strip() or line.startswith('"') or line.startswith('Date\t') or line.startswith('Total rows:'):
                continue
                
            values = line.split('\t')
            if len(values) != 7:
                print(f"Пропущена строка (неверное кол-во полей): {line}")
                continue
                
            try:
                cur.execute("""
                    INSERT INTO row.yandex_direct_stats VALUES (
                        %s, %s, %s, %s, %s, %s, %s
                    )
                """, (
                    values[0].strip(),  # Date
                    int(values[1]),    # CampaignId
                    values[2].strip(),  # CampaignName
                    int(values[3]),     # Clicks
                    float(values[4]),   # Cost
                    float(values[5]),   # Ctr
                    int(values[6])      # Impressions
                ))
                processed_rows += 1
            except (ValueError, IndexError) as e:
                print(f"Пропущена строка: {line} | Ошибка: {str(e)}")
                continue

        conn.commit()
        print(f"✅ Успешно загружено {processed_rows} строк")
        
    except Exception as e:
        print(f"❌ Ошибка БД: {str(e)}")
        if conn:
            conn.rollback()
        raise  # Пробрасываем исключение дальше
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    try:
        config = load_config()
        token = config['YandexDirect']['ACCESS_TOKEN']
        db_config = config['Database']

        date_to = datetime.now().strftime('%Y-%m-%d')
        date_from = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')

        print(f"🔄 Загрузка данных за {date_from} - {date_to}...")
        report_data = get_direct_report(token, date_from, date_to)
        
        if report_data:
            print("📊 Пример данных:")
            print("\n".join(report_data.split('\n')[:3]))
            save_to_postgres(report_data, db_config)
        else:
            print("❌ Не удалось получить данные")
            sys.exit(1)

    except Exception as e:
        print(f"🔥 Критическая ошибка: {str(e)}")
        sys.exit(1)
    finally:
        print("Готово")
