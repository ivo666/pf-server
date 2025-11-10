import pandas as pd
import os
import psycopg2
from dotenv import load_dotenv
import numpy as np

# Загружаем переменные окружения из .env файла
env_path = '/home/pf-server/yandex_webmaster/config/.env'
load_dotenv(env_path)

def log_message(message):
    print(f"[{pd.Timestamp.now().strftime('%H:%M:%S')}] {message}")

def get_connection():
    return psycopg2.connect(
        dbname=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        host=os.getenv('DB_HOST', 'localhost'),
        port=os.getenv('DB_PORT', '5432')
    )

def get_last_id():
    """Получаем последний ID из целевой таблицы"""
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT COALESCE(MAX(id), 0) FROM ppl.webmaster_aggregated")
        last_id = cursor.fetchone()[0]
        log_message(f"📊 Последний ID в целевой таблице: {last_id}")
        return int(last_id)  # Гарантируем int тип
    finally:
        conn.close()

def get_new_data(last_id):
    """Получаем только новые данные из rdl.webm_api"""
    conn = get_connection()
    try:
        # Получаем максимальную дату из целевой таблицы
        cursor = conn.cursor()
        cursor.execute("SELECT COALESCE(MAX(date), '2000-01-01') FROM ppl.webmaster_aggregated")
        max_date = cursor.fetchone()[0]
        
        log_message(f"📅 Последняя дата в целевой таблице: {max_date}")
        
        # Загружаем данные, которых нет в целевой таблице
        query = """
        SELECT * FROM rdl.webm_api 
        WHERE date > %s 
           OR (date = %s AND NOT EXISTS (
               SELECT 1 FROM ppl.webmaster_aggregated p 
               WHERE p.date = rdl.webm_api.date 
                 AND p.query = rdl.webm_api.query 
                 AND p.page_path = rdl.webm_api.page_path
                 AND p.device = rdl.webm_api.device
           ))
        ORDER BY date, query, page_path, device
        """
        
        df = pd.read_sql_query(query, conn, params=(max_date, max_date))
        log_message(f"🆕 Найдено {len(df):,} новых строк")
        return df
        
    finally:
        conn.close()

def prepare_data(df):
    """Подготавливаем данные: применяем бизнес-логику и обрабатываем типы"""
    if df.empty:
        return df
    
    # Создаем копию чтобы избежать предупреждений
    df_processed = df.copy()
    
    # Заполняем NaN значения перед преобразованием типов
    numeric_columns = ['demand', 'impressions', 'clicks', 'position']
    for col in numeric_columns:
        if col in df_processed.columns:
            df_processed[col] = df_processed[col].fillna(0)
    
    # Преобразуем типы данных
    df_processed['demand'] = df_processed['demand'].astype(int)
    df_processed['impressions'] = df_processed['impressions'].astype(int)
    df_processed['clicks'] = df_processed['clicks'].astype(int)
    df_processed['position'] = df_processed['position'].astype(float)
    
    # Применяем бизнес-логику
    df_processed['demand'] = np.where(
        df_processed['impressions'] > df_processed['demand'], 
        df_processed['impressions'], 
        df_processed['demand']
    )
    df_processed['clicks'] = np.where(
        df_processed['clicks'] > df_processed['impressions'], 
        df_processed['impressions'], 
        df_processed['clicks']
    )
    
    return df_processed

def save_incremental_data(df, start_id):
    """Сохраняем новые данные с продолжением нумерации ID"""
    if df.empty:
        log_message("ℹ️ Нет новых данных для сохранения")
        return 0
    
    log_message(f"💾 Сохранение {len(df):,} новых строк...")
    
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        # Создаем новые ID начиная с последнего + 1 (гарантируем int тип)
        df['id'] = range(int(start_id) + 1, int(start_id) + len(df) + 1)
        
        # Подготавливаем данные для вставки
        insert_sql = """
        INSERT INTO ppl.webmaster_aggregated 
        (id, date, query, page_path, device, demand, impressions, clicks, position)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        data_tuples = []
        for _, row in df.iterrows():
            data_tuples.append((
                int(row['id']),  # Гарантируем int тип
                row['date'],
                row['query'] if pd.notna(row['query']) else None,
                row['page_path'] if pd.notna(row['page_path']) else None,
                row['device'] if pd.notna(row['device']) else None,
                int(row['demand']) if pd.notna(row['demand']) else 0,
                int(row['impressions']) if pd.notna(row['impressions']) else 0,
                int(row['clicks']) if pd.notna(row['clicks']) else 0,
                float(row['position']) if pd.notna(row['position']) else 0.0
            ))
        
        # Вставляем новые данные
        cursor.executemany(insert_sql, data_tuples)
        conn.commit()
        
        log_message(f"✅ Сохранено {len(df):,} новых строк")
        log_message(f"📈 Новый диапазон ID: {start_id + 1} - {start_id + len(df)}")
        
        return len(df)
        
    except Exception as e:
        conn.rollback()
        log_message(f"💥 Ошибка при сохранении: {e}")
        raise e
    finally:
        cursor.close()
        conn.close()

def main():
    log_message("🚀 НАЧАЛО ИНКРЕМЕНТАЛЬНОГО ОБНОВЛЕНИЯ")
    
    try:
        # Шаг 1: Получаем последний ID из целевой таблицы
        last_id = get_last_id()
        
        # Шаг 2: Загружаем только новые данные
        new_data_df = get_new_data(last_id)
        
        if new_data_df.empty:
            log_message("🎉 Нет новых данных для обработки")
            return
        
        # Шаг 3: Подготавливаем данные (бизнес-логика + обработка типов)
        log_message("🔧 Применение бизнес-логики и подготовка типов данных...")
        processed_df = prepare_data(new_data_df)
        
        # Логируем информацию о типах данных
        log_message("📊 Типы данных после подготовки:")
        for col in ['demand', 'impressions', 'clicks']:
            if col in processed_df.columns:
                log_message(f"   - {col}: {processed_df[col].dtype}")
        
        # Шаг 4: Сохраняем новые данные с продолжением ID
        saved_count = save_incremental_data(processed_df, last_id)
        
        # Шаг 5: Финальная статистика
        log_message("📊 Финальная статистика:")
        
        conn = get_connection()
        total_count = pd.read_sql_query(
            "SELECT COUNT(*) as cnt FROM ppl.webmaster_aggregated", 
            conn
        ).iloc[0]['cnt']
        
        new_stats = pd.read_sql_query("""
            SELECT 
                COUNT(*) as new_rows,
                SUM(impressions) as new_impressions,
                SUM(clicks) as new_clicks,
                COUNT(DISTINCT device) as new_devices
            FROM ppl.webmaster_aggregated 
            WHERE id > %s
        """, conn, params=(last_id,))
        
        conn.close()
        
        log_message(f"   - Всего строк в таблице: {int(total_count):,}")
        log_message(f"   - Новых строк добавлено: {saved_count:,}")
        log_message(f"   - Новых показов: {int(new_stats.iloc[0]['new_impressions']):,}")
        log_message(f"   - Новых кликов: {int(new_stats.iloc[0]['new_clicks']):,}")
        log_message(f"   - Новых устройств: {int(new_stats.iloc[0]['new_devices']):,}")
        
        log_message(f"🎉 ИНКРЕМЕНТАЛЬНОЕ ОБНОВЛЕНИЕ ЗАВЕРШЕНО!")
        
    except Exception as e:
        log_message(f"💥 КРИТИЧЕСКАЯ ОШИБКА: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
