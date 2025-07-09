import configparser
import psycopg2
from datetime import datetime, timedelta
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_config(filename='config.ini'):
    """Загружает конфигурацию из файла"""
    config = configparser.ConfigParser()
    config.read(filename)
    return config['Database']

def connect_to_db(config):
    """Устанавливает соединение с базой данных"""
    try:
        conn = psycopg2.connect(
            host=config['HOST'],
            database=config['DATABASE'],
            user=config['USER'],
            password=config['PASSWORD'],
            port=config['PORT']
        )
        logger.info("Successfully connected to the database")
        return conn
    except Exception as e:
        logger.error(f"Error connecting to the database: {e}")
        raise

def data_exists(conn, date, campaign_id, ad_id):
    """Проверяет существование данных в базе"""
    with conn.cursor() as cursor:
        check_query = """
        SELECT EXISTS (
            SELECT 1 FROM rdl.yandex_direct_stats 
            WHERE date = %s AND campaign_id = %s AND ad_id = %s
        )
        """
        cursor.execute(check_query, (date, campaign_id, ad_id))
        return cursor.fetchone()[0]

def insert_campaign_data(conn, campaign_data):
    """Вставляет данные кампании в базу"""
    with conn.cursor() as cursor:
        insert_query = """
        INSERT INTO rdl.yandex_direct_stats (date, campaign_id, ad_id, clicks, cost, impressions)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        cursor.execute(insert_query, campaign_data)
        conn.commit()

def print_campaign_stats(conn, data):
    """Выводит статистику кампаний и сохраняет в базу"""
    if not data:
        print("No data received")
        return
    
    print("\nCampaign Performance Report:")
    print("=" * 120)
    print("{:<12} | {:<12} | {:<30} | {:<12} | {:<8} | {:<12} | {:<12}".format(
        "Date", "Campaign ID", "Ad ID", "Clicks", "Cost", "Impressions"
    ))
    print("=" * 120)
    
    for line in data.split('\n'):
        if line.strip() and not line.startswith(('Date', 'Total')):
            parts = line.strip().split('\t')
            if len(parts) >= 6:  # Убедитесь, что есть все необходимые поля
                date = parts[0]
                campaign_id = parts[1]
                ad_id = parts[3]
                clicks = int(parts[4])
                impressions = int(parts[5])
                cost = float(parts[6])
                
                # Проверяем наличие данных перед вставкой
                if not data_exists(conn, date, campaign_id, ad_id):
                    campaign_data = (date, campaign_id, ad_id, clicks, cost, impressions)
                    insert_campaign_data(conn, campaign_data)

                    print("{:<12} | {:<12} | {:<30} | {:<12} | {:<8} | {:<12} | {:<12}".format(
                        date, campaign_id, ad_id, clicks, f"{cost:.2f}", impressions
                    ))
                else:
                    logger.info(f"Data for date {date}, campaign {campaign_id}, ad {ad_id} already exists. Skipping.")

    print("=" * 120)

def get_data_from_yandex_direct(start_date, end_date):
    """Получает данные из Яндекс.Директ (заглушка)"""
    # Здесь должна быть реализация получения данных из Яндекс.Директ
    # Должна возвращать данные в формате строки (например, табуляция разделяет поля)
    logger.warning("Using stub function for Yandex Direct data")
    return None

def main():
    """Основная функция выполнения скрипта"""
    TOKEN = "y0__xCfm56NBhi4uzgg2IHdxxMB-11ibEFeXtYCgMHlML7g5RHDNA"
    start_date = datetime(2025, 1, 1)

    logger.info(f"Starting report from {start_date}")

    # Загрузка конфигурации
    config = load_config()
    
    # Подключение к базе данных
    conn = None
    try:
        conn = connect_to_db(config)

        # Получаем данные по неделям начиная с 01.01.2025
        current_date = start_date
        while current_date <= datetime.now():
            week_end_date = current_date + timedelta(days=6)
            data = get_data_from_yandex_direct(
                current_date.strftime("%Y-%m-%d"), 
                week_end_date.strftime("%Y-%m-%d")
            )
            
            if data:
                print_campaign_stats(conn, data)
            else:
                logger.error(f"Failed to get data for week starting {current_date}")

            current_date += timedelta(days=7)
    
        logger.info("Script finished")
    
    except Exception as e:
        logger.error(f"Error in main execution: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()
