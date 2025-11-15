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

def get_pandas_dataframe(query, params=None):
    try:
        conn = get_connection()
        df = pd.read_sql_query(query, conn, params=params)
        return df
    except psycopg2.Error as e:
        log_message(f"Ошибка при выполнении запроса: {e}")
        return None
    finally:
        if 'conn' in locals():
            conn.close()

def execute_sql_query(query, params=None):
    """Выполняет SQL запрос без возврата результата"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(query, params)
        conn.commit()
        return True
    except psycopg2.Error as e:
        log_message(f"Ошибка при выполнении SQL запроса: {e}")
        if 'conn' in locals():
            conn.rollback()
        return False
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

def load_and_process_data():
    """Загружает и обрабатывает данные из таблиц"""
    log_message("📥 Загрузка данных из таблиц...")
    
    # Загружаем данные из таблиц
    df_pos = get_pandas_dataframe("SELECT * FROM ppl.webmaster_positions")
    df_cl = get_pandas_dataframe("SELECT * FROM ppl.webmaster_clicks") 
    df_aggr = get_pandas_dataframe("SELECT * FROM ppl.webmaster_aggregated")
    
    if df_pos is None or df_cl is None or df_aggr is None:
        log_message("💥 Ошибка при загрузке данных из таблиц")
        return None
    
    log_message(f"✅ Загружено данных: positions={len(df_pos):,}, clicks={len(df_cl):,}, aggregated={len(df_aggr):,}")
    
    # Обрабатываем данные как в локальном скрипте
    log_message("🔄 Обработка данных...")
    
    # Объединяем таблицы
    df = pd.merge(df_pos, df_cl, on=['id', 'impression_order'], how='left')
    df = pd.merge(df, df_aggr, on='id', how='left')
    
    # Оставляем нужные столбцы
    df = df[['id', 'date', 'query', 'page_path', 'device', 'demand', 'impression_position', 'click_position']].copy()
    
    # Добавляем параметр 'click'
    df['click'] = np.where(pd.isna(df['click_position']), 0, 1)
    
    # Добавляем параметр 'impression'
    df['impression'] = np.where(pd.isna(df['impression_position']), 0, 1)
    
    # Добавляем параметр 'serp_sector'
    df['serp_sector'] = ''
    df.loc[df['impression_position'] < 4, 'serp_sector'] = 'top'
    df.loc[(df['impression_position'] >= 4) & (df['impression_position'] < 11), 'serp_sector'] = 'garantia'
    df.loc[(df['impression_position'] >= 11) & (df['impression_position'] < 21), 'serp_sector'] = 'second_page'
    df.loc[(df['impression_position'] >= 21) & (df['impression_position'] < 31), 'serp_sector'] = 'third_page'
    df.loc[(df['impression_position'] >= 31) & (df['impression_position'] < 41), 'serp_sector'] = 'fourth_page'
    df.loc[df['impression_position'] >= 41, 'serp_sector'] = 'not_in_view'
    
    log_message(f"✅ Обработано {len(df):,} строк")
    
    return df

def create_target_table():
    """Создает целевую таблицу если она не существует"""
    create_table_query = """
    CREATE TABLE IF NOT EXISTS cdm.table_webmaster_serp (
        id INTEGER,
        date DATE,
        query TEXT,
        page_path TEXT,
        device TEXT,
        demand TEXT,
        impression_position INTEGER,
        click_position INTEGER,
        click INTEGER,
        impression INTEGER,
        serp_sector TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    CREATE INDEX IF NOT EXISTS idx_table_webmaster_serp_id ON cdm.table_webmaster_serp (id);
    CREATE INDEX IF NOT EXISTS idx_table_webmaster_serp_date ON cdm.table_webmaster_serp (date);
    CREATE INDEX IF NOT EXISTS idx_table_webmaster_serp_serp_sector ON cdm.table_webmaster_serp (serp_sector);
    """
    
    log_message("🔨 Проверка и создание целевой таблицы...")
    return execute_sql_query(create_table_query)

def clear_target_table():
    """Очищает целевую таблицу перед загрузкой новых данных"""
    log_message("🧹 Очистка целевой таблицы...")
    return execute_sql_query("TRUNCATE TABLE cdm.table_webmaster_serp")

def save_data_to_table(df):
    """Сохраняет DataFrame в целевую таблицу"""
    if df is None or df.empty:
        log_message("ℹ️ Нет данных для сохранения")
        return 0
    
    log_message("💾 Сохранение данных в целевую таблицу...")
    
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        # Подготавливаем данные для вставки
        data_tuples = []
        for _, row in df.iterrows():
            data_tuples.append((
                int(row['id']),
                row['date'],
                row['query'],
                row['page_path'],
                row['device'],
                row['demand'],
                int(row['impression_position']) if pd.notna(row['impression_position']) else None,
                int(row['click_position']) if pd.notna(row['click_position']) else None,
                int(row['click']),
                int(row['impression']),
                row['serp_sector']
            ))
        
        # Вставляем данные батчами
        insert_sql = """
        INSERT INTO cdm.table_webmaster_serp 
        (id, date, query, page_path, device, demand, impression_position, click_position, click, impression, serp_sector)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        batch_size = 1000
        total_rows = len(data_tuples)
        saved_rows = 0
        
        for i in range(0, total_rows, batch_size):
            batch = data_tuples[i:i + batch_size]
            cursor.executemany(insert_sql, batch)
            conn.commit()
            saved_rows += len(batch)
            log_message(f"   Сохранено {saved_rows:,} из {total_rows:,} строк")
        
        cursor.close()
        conn.close()
        
        log_message(f"✅ Успешно сохранено {saved_rows:,} строк в cdm.table_webmaster_serp")
        return saved_rows
        
    except Exception as e:
        log_message(f"💥 Ошибка при сохранении данных: {e}")
        if 'conn' in locals():
            conn.rollback()
        return 0

def update_webmaster_serp_table():
    """Основная функция обновления таблицы webmaster_serp"""
    log_message("🚀 НАЧАЛО ОБНОВЛЕНИЯ ТАБЛИЦЫ CDM.TABLE_WEBMASTER_SERP")
    
    try:
        # Шаг 1: Создаем целевую таблицу если нужно
        if not create_target_table():
            log_message("💥 Ошибка при создании таблицы")
            return False
        
        # Шаг 2: Очищаем целевую таблицу
        if not clear_target_table():
            log_message("💥 Ошибка при очистке таблицы")
            return False
        
        # Шаг 3: Загружаем и обрабатываем данные
        processed_df = load_and_process_data()
        
        if processed_df is None:
            log_message("💥 Ошибка при обработке данных")
            return False
        
        # Шаг 4: Сохраняем данные в целевую таблицу
        saved_count = save_data_to_table(processed_df)
        
        if saved_count == 0:
            log_message("💥 Данные не были сохранены")
            return False
        
        # Шаг 5: Проверяем результат
        check_query = "SELECT COUNT(*) as row_count FROM cdm.table_webmaster_serp"
        result_df = get_pandas_dataframe(check_query)
        
        if result_df is not None:
            final_count = result_df.iloc[0]['row_count']
            log_message(f"📊 Проверка: в таблице cdm.table_webmaster_serp теперь {final_count:,} строк")
        
        log_message("🎉 ОБНОВЛЕНИЕ ТАБЛИЦЫ CDM.TABLE_WEBMASTER_SERP ЗАВЕРШЕНО!")
        return True
        
    except Exception as e:
        log_message(f"💥 ОШИБКА: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    update_webmaster_serp_table()
