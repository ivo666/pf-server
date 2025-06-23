import requests
import psycopg2
from datetime import datetime, timedelta
import configparser
from pathlib import Path
import sys

# --- Конфигурация ---
def load_config():
    config = configparser.ConfigParser()
    config_path = Path(__file__).parent / 'config.ini'
    config.read(config_path)
    return config

# --- Получение данных из API ---
def get_yandex_direct_report(token, date_from, date_to):
    url = "https://api.direct.yandex.com/json/v5/reports"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept-Language": "ru",
        "Content-Type": "application/json"
    }

    report_body = {
        "params": {
            "SelectionCriteria": {"DateFrom": date_from, "DateTo": date_to},
            "FieldNames": [
                "Date",
                "CampaignId",
                "CampaignName",
                "AdId",  # Добавляем ID объявления (для utm_content)
                "AdGroupName",
                "Impressions",
                "Clicks",
                "Cost",
                "Ctr",
                "AvgClickPosition",
                "AvgImpressionPosition",
                "Conversions",
                "ConversionRate"
            ],
            "ReportName": "CampaignPerformance",
            "ReportType": "AD_PERFORMANCE_REPORT",  # Изменен тип отчета!
            "DateRangeType": "CUSTOM_DATE",
            "Format": "TSV",
            "IncludeVAT": "YES"
        }
    }

    try:
        print(f"🔄 Запрос данных за {date_from} - {date_to}...")
        response = requests.post(url, headers=headers, json=report_body, timeout=30)
        response.raise_for_status()
        return response.text
    except requests.exceptions.RequestException as e:
        print(f"❌ Ошибка API: {str(e)}")
        return None

# --- Загрузка в PostgreSQL ---
def save_to_postgres(data, db_params):
    conn = None
    try:
        conn = psycopg2.connect(
            host=db_params['HOST'],
            database=db_params['DATABASE'],
            user=db_params['USER'],
            password=db_params['PASSWORD'],
            port=db_params['PORT']
        )
        cur = conn.cursor()

        # Создаем таблицу (если не существует)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS row.yandex_direct_stats (
                date DATE,
                campaign_id BIGINT,
                campaign_name TEXT,
                ad_id BIGINT,  # ID объявления (для utm_content)
                ad_group_name TEXT,
                impressions INTEGER,
                clicks INTEGER,
                cost DECIMAL(15, 2),
                ctr DECIMAL(5, 2),
                avg_click_position DECIMAL(5, 2),
                avg_impression_position DECIMAL(5, 2),
                conversions INTEGER,
                conversion_rate DECIMAL(5, 2)
            )
        """)

        lines = data.strip().split('\n')[1:]
        insert_query = """
            INSERT INTO row.yandex_direct_stats VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
        """

        for line in lines:
            if not line.strip():
                continue
            values = line.split('\t')
            try:
                cur.execute(insert_query, (
                    values[0], int(values[1]), values[2],  # Date, CampaignId, CampaignName
                    int(values[3]), values[4],  # AdId, AdGroupName
                    int(values[5]), int(values[6]), float(values[7]),  # Impressions, Clicks, Cost
                    float(values[8]), float(values[9]), float(values[10]),  # CTR, Positions
                    int(values[11]), float(values[12])  # Conversions, ConversionRate
                ))
            except Exception as e:
                print(f"⚠️ Ошибка в строке: {line}\n{str(e)}")
                continue

        conn.commit()
        print(f"✅ Успешно загружено {len(lines)} записей")
        
    except Exception as e:
        print(f"❌ Ошибка БД: {str(e)}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    try:
        config = load_config()
        yandex_token = config['YandexDirect']['ACCESS_TOKEN']
        db_params = {
            'HOST': config['Database']['HOST'],
            'DATABASE': config['Database']['DATABASE'],
            'USER': config['Database']['USER'],
            'PASSWORD': config['Database']['PASSWORD'],
            'PORT': config['Database']['PORT']
        }

        date_to = datetime.now().strftime('%Y-%m-%d')
        date_from = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')

        report_data = get_yandex_direct_report(yandex_token, date_from, date_to)
        if report_data:
            save_to_postgres(report_data, db_params)
        else:
            print("⚠️ Нет данных для загрузки")

    except Exception as e:
        print(f"🔥 Ошибка: {str(e)}")
    finally:
        print("Готово")
