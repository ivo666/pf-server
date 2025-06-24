import requests
import psycopg2
from datetime import datetime, timedelta
import configparser
from pathlib import Path
import sys
import time

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
        print(f"🔄 Загрузка данных за период {date_from} — {date_to}...")
        response = requests.post(url, headers=headers, json=report_body, timeout=60)
        response.raise_for_status()
        return response.text
    except requests.exceptions.RequestException as e:
        print(f"❌ Ошибка API: {e}")
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

        # Проверяем существование таблицы (на всякий случай)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS row.yandex_direct_stats (
                date DATE,
                campaign_id BIGINT,
                campaign_name TEXT,
                clicks INTEGER,
                cost DECIMAL(15, 2),
                ctr DECIMAL(5, 2),
                impressions INTEGER,
                PRIMARY KEY (date, campaign_id)
            )
        """)

        lines = data.strip().split('\n')
        processed_rows = 0
        skipped_rows = 0
        
        for line in lines:
            if not line.strip() or line.startswith('"') or line.startswith('Date\t') or line.startswith('Total rows:'):
                continue
                
            values = line.split('\t')
            if len(values) != 7:
                print(f"⚠ Пропущена строка (неверное кол-во полей): {line}")
                continue
                
            try:
                cur.execute("""
                    INSERT INTO row.yandex_direct_stats VALUES (
                        %s, %s, %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (date, campaign_id) DO NOTHING
                """, (
                    values[0].strip(),          # Date
                    int(values[1]),             # CampaignId
                    values[2].strip(),          # CampaignName
                    int(values[3]),             # Clicks
                    float(values[4]) / 1000000, # Cost (переводим микроединицы в рубли)
                    float(values[5]),           # Ctr
                    int(values[6])              # Impressions
                ))
                if cur.rowcount > 0:
                    processed_rows += 1
                else:
                    skipped_rows += 1
            except (ValueError, IndexError) as e:
                print(f"⚠ Пропущена строка: {line} | Ошибка: {str(e)}")
                continue

        conn.commit()
        print(f"✅ Успешно загружено {processed_rows} строк")
        if skipped_rows > 0:
            print(f"⏩ Пропущено {skipped_rows} дублирующихся строк")
        
    except Exception as e:
        print(f"❌ Ошибка БД: {str(e)}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()

def check_existing_data(db_config, date):
    """Проверяет, есть ли уже данные за указанную дату"""
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

        cur.execute("""
            SELECT EXISTS (
                SELECT 1 FROM row.yandex_direct_stats 
                WHERE date = %s
                LIMIT 1
            )
        """, (date,))
        
        exists = cur.fetchone()[0]
        return exists
        
    except Exception as e:
        print(f"⚠ Ошибка при проверке данных: {str(e)}")
        return False
    finally:
        if conn:
            conn.close()

def get_yesterday_date():
    """Возвращает дату вчерашнего дня в формате YYYY-MM-DD"""
    return (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

if __name__ == "__main__":
    try:
        config = load_config()
        token = config['YandexDirect']['ACCESS_TOKEN']
        db_config = config['Database']

        # Получаем вчерашнюю дату
        yesterday = get_yesterday_date()
        print(f"📅 Обрабатываемая дата: {yesterday}")

        # Проверяем, есть ли уже данные за этот день
        if check_existing_data(db_config, yesterday):
            print(f"⏩ Данные за {yesterday} уже загружены, завершение работы")
            sys.exit(0)
        
        # Загружаем данные за вчерашний день
        report_data = get_direct_report(token, yesterday, yesterday)
        
        if report_data:
            save_to_postgres(report_data, db_config)
        else:
            print(f"⚠ Не удалось получить данные за {yesterday}")
            sys.exit(1)

    except Exception as e:
        print(f"🔥 Критическая ошибка: {str(e)}")
        sys.exit(1)
    finally:
        print("\n✅ Выгрузка завершена")
