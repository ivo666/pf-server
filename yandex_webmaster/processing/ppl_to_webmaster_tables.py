import pandas as pd
import os
import psycopg2
from dotenv import load_dotenv
import numpy as np
import math

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
        log_message(f"Ошибка: {e}")
        return None
    finally:
        if 'conn' in locals():
            conn.close()

def get_new_aggregated_data():
    """Получаем ID строк из aggregated, которых нет в positions"""
    log_message("🔍 Поиск новых данных для обработки...")
    
    query = """
    SELECT 
        wa.id, wa.impressions, wa.clicks, wa.position
    FROM ppl.webmaster_aggregated wa
    WHERE wa.impressions > 0 
      AND NOT EXISTS (
          SELECT 1 FROM ppl.webmaster_positions wp 
          WHERE wp.id = wa.id
      )
    ORDER BY wa.id
    """
    
    new_data = get_pandas_dataframe(query)
    
    if new_data is not None and not new_data.empty:
        # Преобразуем типы данных
        new_data['id'] = new_data['id'].astype(int)
        new_data['impressions'] = new_data['impressions'].astype(int)
        new_data['clicks'] = new_data['clicks'].astype(int)
        new_data['position'] = new_data['position'].astype(float)
        
        log_message(f"📈 Найдено {len(new_data):,} новых строк для обработки")
        log_message(f"📊 Типы данных: id={new_data['id'].dtype}, impressions={new_data['impressions'].dtype}")
        return new_data
    else:
        log_message("ℹ️ Нет новых данных для обработки")
        return pd.DataFrame()

def generate_positions_array(row):
    """Генерация массива позиций для строки"""
    impressions = int(row['impressions'])
    avg_position = float(row['position'])
    
    if impressions == 0:
        return []
    
    round_position = int(round(avg_position - 0.01))
    sum_of_positions = int(math.ceil(avg_position * impressions))
    
    min_pos = max(1, math.floor(avg_position - 1.5))
    max_pos = math.ceil(avg_position + 1.5)
    
    p = max(0.05, min(0.95, (avg_position - min_pos) / (max_pos - min_pos)))
    
    positions = []
    for _ in range(impressions):
        binomial_result = 0
        for _ in range(max_pos - min_pos):
            if np.random.random() < p:
                binomial_result += 1
        position = min_pos + binomial_result
        positions.append(int(position))
    
    # Корректируем сумму
    current_sum = sum(positions)
    diff = sum_of_positions - current_sum
    
    if diff > 0:
        sorted_indices = np.argsort(positions)
        for i in range(min(diff, len(positions))):
            positions[sorted_indices[i]] += 1
    elif diff < 0:
        sorted_indices = np.argsort(positions)[::-1]
        for i in range(min(abs(diff), len(positions))):
            positions[sorted_indices[i]] = max(1, positions[sorted_indices[i]] - 1)
    
    return positions

def distribute_clicks_with_order(row, positions_with_order):
    """Распределение кликов по показам"""
    clicks = int(row['clicks'])
    if clicks == 0 or len(positions_with_order) == 0:
        return []
    
    position_weights = {
        1: 0.30, 2: 0.15, 3: 0.08, 4: 0.05, 5: 0.03,
        6: 0.02, 7: 0.015, 8: 0.012, 9: 0.01, 10: 0.008
    }
    
    weights = []
    for pos, order in positions_with_order:
        weight = position_weights.get(pos, 0.005)
        time_weight = 1.0 / (order * 0.1 + 1)
        weights.append(weight * time_weight)
    
    total_weight = sum(weights)
    if total_weight == 0:
        weights = [1.0 / len(positions_with_order)] * len(positions_with_order)
    else:
        weights = [w / total_weight for w in weights]
    
    if clicks <= len(positions_with_order):
        chosen_indices = np.random.choice(
            len(positions_with_order), 
            size=clicks, 
            replace=False, 
            p=weights
        )
    else:
        chosen_indices = np.random.choice(
            len(positions_with_order), 
            size=clicks, 
            replace=True, 
            p=weights
        )
    
    result = []
    for idx in chosen_indices:
        pos, order = positions_with_order[idx]
        result.append({
            'position': int(pos),   # Гарантируем int тип
            'order': int(order)     # Гарантируем int тип
        })
    
    return result

def save_positions_batch(positions_data):
    """Сохранение позиций батчем"""
    if not positions_data:
        return 0
    
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        insert_sql = """
        INSERT INTO ppl.webmaster_positions (id, impression_position, impression_order)
        VALUES (%s, %s, %s)
        """
        
        # Преобразуем данные с гарантией int типа
        data_tuples = []
        for item in positions_data:
            data_tuples.append((
                int(item['id']),           # Гарантируем int
                int(item['position']),     # Гарантируем int  
                int(item['order'])         # Гарантируем int
            ))
        
        cursor.executemany(insert_sql, data_tuples)
        conn.commit()
        
        log_message(f"✅ Сохранено {len(positions_data):,} позиций")
        return len(positions_data)
        
    except Exception as e:
        conn.rollback()
        log_message(f"💥 Ошибка при сохранении позиций: {e}")
        raise e
    finally:
        cursor.close()
        conn.close()

def save_clicks_batch(clicks_data):
    """Сохранение кликов батчем"""
    if not clicks_data:
        return 0
    
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        insert_sql = """
        INSERT INTO ppl.webmaster_clicks (id, click_position, impression_order)
        VALUES (%s, %s, %s)
        """
        
        # Преобразуем данные с гарантией int типа
        data_tuples = []
        for item in clicks_data:
            data_tuples.append((
                int(item['id']),           # Гарантируем int
                int(item['position']),     # Гарантируем int
                int(item['order'])         # Гарантируем int
            ))
        
        cursor.executemany(insert_sql, data_tuples)
        conn.commit()
        
        log_message(f"✅ Сохранено {len(clicks_data):,} кликов")
        return len(clicks_data)
        
    except Exception as e:
        conn.rollback()
        log_message(f"💥 Ошибка при сохранении кликов: {e}")
        raise e
    finally:
        cursor.close()
        conn.close()

def update_positions_and_clicks():
    """Основная функция обновления позиций и кликов"""
    log_message("🚀 НАЧАЛО ОБНОВЛЕНИЯ ПОЗИЦИЙ И КЛИКОВ")
    
    try:
        # Шаг 1: Получаем новые данные из aggregated
        new_aggregated_data = get_new_aggregated_data()
        
        if new_aggregated_data.empty:
            log_message("🎉 Все данные уже актуальны")
            return
        
        # Шаг 2: Генерируем позиции для новых данных
        log_message("🎲 Генерация позиций для новых данных...")
        
        all_positions_data = []
        positions_by_id = {}
        
        for _, row in new_aggregated_data.iterrows():
            row_id = int(row['id'])  # Гарантируем int тип
            positions = generate_positions_array(row)
            
            # Сохраняем позиции с порядковыми номерами
            positions_for_id = []
            for order, pos in enumerate(positions, 1):
                position_item = {
                    'id': row_id,
                    'position': int(pos),      # Гарантируем int
                    'order': int(order)        # Гарантируем int
                }
                all_positions_data.append(position_item)
                positions_for_id.append((int(pos), int(order)))  # Гарантируем int
            
            positions_by_id[row_id] = positions_for_id
        
        # Шаг 3: Сохраняем позиции
        positions_count = save_positions_batch(all_positions_data)
        
        # Шаг 4: Генерируем и сохраняем клики для строк с clicks > 0
        log_message("🎲 Распределение кликов...")
        
        all_clicks_data = []
        clicks_rows = new_aggregated_data[new_aggregated_data['clicks'] > 0]
        
        for _, row in clicks_rows.iterrows():
            row_id = int(row['id'])  # Гарантируем int тип
            positions_with_order = positions_by_id.get(row_id, [])
            
            if positions_with_order:
                click_assignments = distribute_clicks_with_order(row, positions_with_order)
                
                for click in click_assignments:
                    all_clicks_data.append({
                        'id': row_id,
                        'position': int(click['position']),  # Гарантируем int
                        'order': int(click['order'])         # Гарантируем int
                    })
        
        clicks_count = save_clicks_batch(all_clicks_data)
        
        # Шаг 5: Финальная статистика
        log_message("📊 ФИНАЛЬНАЯ СТАТИСТИКА:")
        log_message(f"   - Обработано строк: {len(new_aggregated_data):,}")
        log_message(f"   - Сгенерировано позиций: {positions_count:,}")
        log_message(f"   - Сгенерировано кликов: {clicks_count:,}")
        
        # Проверяем общее количество
        total_positions_df = get_pandas_dataframe("SELECT COUNT(*) as cnt FROM ppl.webmaster_positions")
        total_clicks_df = get_pandas_dataframe("SELECT COUNT(*) as cnt FROM ppl.webmaster_clicks")
        
        if total_positions_df is not None:
            total_positions = int(total_positions_df.iloc[0]['cnt'])
            log_message(f"   - Всего позиций в таблице: {total_positions:,}")
        
        if total_clicks_df is not None:
            total_clicks = int(total_clicks_df.iloc[0]['cnt'])
            log_message(f"   - Всего кликов в таблице: {total_clicks:,}")
        
        log_message("🎉 ОБНОВЛЕНИЕ ЗАВЕРШЕНО!")
        
    except Exception as e:
        log_message(f"💥 ОШИБКА: {e}")
        import traceback
        traceback.print_exc()

def check_data_consistency():
    """Проверка согласованности данных"""
    log_message("🔍 ПРОВЕРКА СОГЛАСОВАННОСТИ ДАННЫХ")
    
    checks = {
        "Строки без позиций": """
            SELECT COUNT(*) as missing_positions
            FROM ppl.webmaster_aggregated wa
            WHERE wa.impressions > 0 
              AND NOT EXISTS (
                  SELECT 1 FROM ppl.webmaster_positions wp 
                  WHERE wp.id = wa.id
              )
        """,
        "Клики без позиций": """
            SELECT COUNT(*) as orphaned_clicks
            FROM ppl.webmaster_clicks wc
            WHERE NOT EXISTS (
                SELECT 1 FROM ppl.webmaster_positions wp 
                WHERE wp.id = wc.id AND wp.impression_order = wc.impression_order
            )
        """,
        "Общая статистика": """
            SELECT 
                (SELECT COUNT(*) FROM ppl.webmaster_aggregated WHERE impressions > 0) as aggregated_with_impressions,
                (SELECT COUNT(DISTINCT id) FROM ppl.webmaster_positions) as positions_ids,
                (SELECT COUNT(DISTINCT id) FROM ppl.webmaster_clicks) as clicks_ids
        """,
        "Проверка типов данных": """
            SELECT 
                column_name, data_type
            FROM information_schema.columns 
            WHERE table_schema = 'ppl' 
              AND table_name IN ('webmaster_positions', 'webmaster_clicks')
            ORDER BY table_name, ordinal_position
        """
    }
    
    for check_name, query in checks.items():
        result = get_pandas_dataframe(query)
        if result is not None:
            if len(result.columns) == 1:
                # Для простых запросов с одним столбцом
                value = result.iloc[0, 0]
                log_message(f"   {check_name}: {int(value) if pd.notna(value) else 0}")
            else:
                # Для запросов с несколькими столбцами
                log_message(f"   {check_name}:")
                for _, row in result.iterrows():
                    log_message(f"     {row.to_dict()}")
        else:
            log_message(f"   {check_name}: ОШИБКА")

if __name__ == "__main__":
    update_positions_and_clicks()
    check_data_consistency()
