import psycopg2
import logging
from datetime import datetime, timedelta

# Настройка логгирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

def check_source_data():
    try:
        conn = psycopg2.connect(
            host="localhost",
            database="pfserver",
            user="postgres",
            password="pfserverivo"
        )
        cur = conn.cursor()
        
        # Проверяем данные в исходных таблицах
        queries = [
            ("yandex_metrika_visits", "SELECT MAX(date), COUNT(*) FROM yandex_metrika_visits WHERE date = CURRENT_DATE - INTERVAL '1 day'"),
            ("yandex_metrika_hits", "SELECT MAX(date_time::date), COUNT(*) FROM yandex_metrika_hits WHERE date_time::date = CURRENT_DATE - INTERVAL '1 day'")
        ]
        
        logging.info("Checking source data freshness:")
        for table_name, query in queries:
            cur.execute(query)
            max_date, count = cur.fetchone()
            logging.info(f"{table_name:25} | Max date: {max_date} | Yesterday's records: {count}")
            
        # Проверяем данные в CDM-таблицах
        cdm_query = """
        SELECT 
            'table_visits' as table_name, 
            MAX(visit_date) as max_date, 
            COUNT(*) filter (WHERE visit_date = CURRENT_DATE - INTERVAL '1 day') as yesterday_count
        FROM cdm.table_visits
        UNION ALL
        SELECT 
            'table_page_views', 
            MAX(hit_ts::date), 
            COUNT(*) filter (WHERE hit_ts::date = CURRENT_DATE - INTERVAL '1 day')
        FROM cdm.table_page_views;
        """
        
        cur.execute(cdm_query)
        logging.info("\nCDM tables status:")
        logging.info("{:<20} {:<15} {:<10}".format("Table", "Max Date", "Yesterday"))
        logging.info("-" * 45)
        for row in cur.fetchall():
            logging.info("{:<20} {:<15} {:<10}".format(row[0], str(row[1]), row[2]))
            
    except Exception as e:
        logging.error(f"Database error: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    logging.info("Starting data freshness check")
    check_source_data()
    logging.info("Check completed")
