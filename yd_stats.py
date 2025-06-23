import requests
import psycopg2
from datetime import datetime, timedelta
import configparser
from pathlib import Path
import sys
import time  # Для паузы между запросами

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

        # Создаем таблицу, если не существует
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
            except (ValueError, IndexError) as e:
                print(f"⚠ Пропущена строка: {line} | Ошибка: {str(e)}")
                continue

        conn.commit()
        print(f"✅ Успешно загружено {processed_rows} строк")
        
    except Exception as e:
        print(f"❌ Ошибка БД: {str(e)}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()

def generate_weekly_ranges(start_date, end_date):
    """Разбивает период на недельные интервалы"""
    current_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")
    date_ranges = []
    
    while current_date < end_date:
        next_date = current_date + timedelta(days=6)  # Неделя = 7 дней (от current_date до next_date)
        if next_date > end_date:
            next_date = end_date
        date_ranges.append((
            current_date.strftime("%Y-%m-%d"),
            next_date.strftime("%Y-%m-%d")
        ))
        current_date = next_date + timedelta(days=1)  # Следующая неделя начинается со следующего дня
    
    return date_ranges

def check_existing_data(db_config, date_from, date_to):
    """Проверяет, есть ли уже данные за указанный период"""
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
                WHERE date BETWEEN %s AND %s
                LIMIT 1
            )
        """, (date_from, date_to))
        
        exists = cur.fetchone()[0]
        return exists
        
    except Exception as e:
        print(f"⚠ Ошибка при проверке данных: {str(e)}")
        return False
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    try:
        config = load_config()
        token = config['YandexDirect']['ACCESS_TOKEN']
        db_config = config['Database']

        # Указываем период за 2025 год (или любой другой)
        start_date = "2025-01-01"
        end_date = "2025-06-20"

        # Разбиваем на недельные интервалы
        date_ranges = generate_weekly_ranges(start_date, end_date)

        for date_from, date_to in date_ranges:
            print(f"\n📅 Проверка данных за {date_from} — {date_to}...")
            
            # Проверяем, есть ли уже данные за этот период
            if check_existing_data(db_config, date_from, date_to):
                print(f"⏩ Данные уже загружены, пропускаем...")
                continue
            
            # Загружаем данные, если их нет
            report_data = get_direct_report(token, date_from, date_to)
            
            if report_data:
                save_to_postgres(report_data, db_config)
            else:
                print(f"⚠ Не удалось получить данные за {date_from} — {date_to}")
            
            # Пауза 10 секунд между запросами (чтобы не превысить лимиты API)
            print("⏳ Ожидание 10 секунд...")
            time.sleep(10)

    except Exception as e:
        print(f"🔥 Критическая ошибка: {str(e)}")
        sys.exit(1)
    finally:
        print("\n✅ Выгрузка завершена")
