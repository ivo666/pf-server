import pandas as pd
import gspread
from sqlalchemy import create_engine, types
from pathlib import Path
import datetime

# 1. Проверка файла credentials
CREDS_PATH = "/etc/secrets/pf-server/profif2023-272a0a314fca.json"
if not Path(CREDS_PATH).exists():
    raise FileNotFoundError(f"Файл {CREDS_PATH} не найден!")

# 2. Настройка подключения к Google Sheets
try:
    gc = gspread.service_account(filename=CREDS_PATH)
    sh = gc.open("ProfiFilter_cpc_fvkart")
    worksheet = sh.worksheet("Campaigns")
    data = worksheet.get_all_records()
    df = pd.DataFrame(data)
    
    print(f"[{datetime.datetime.now()}] Данные получены. Записей: {len(df)}")
except Exception as e:
    raise Exception(f"Ошибка Google Sheets: {str(e)}")

# 3. Подготовка данных
df.columns = df.columns.str.replace('.', '_').str.lower()

if 'start_date' in df.columns:
    df['start_date'] = pd.to_datetime(df['start_date'], format='%d.%m.%Y', errors='coerce')
else:
    print("Предупреждение: столбец start_date отсутствует")

# 4. Подключение к PostgreSQL (серверные параметры)
DB_CONFIG = {
    'host': '212.67.12.162',  # или 'localhost' если на том же сервере
    'database': 'pvs',
    'user': 'postgres',
    'password': 'BdfyjdDbrnjh',
    'port': '5432'
}

try:
    engine = create_engine(
        f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
        f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )
    
    # Тест подключения
    with engine.connect() as conn:
        print(f"[{datetime.datetime.now()}] Подключение к PostgreSQL успешно")
except Exception as e:
    raise Exception(f"Ошибка PostgreSQL: {str(e)}")

# 5. Загрузка данных
try:
    df.to_sql(
        'yd_campaigns_list',
        engine,
        if_exists='append',
        index=False,
        dtype={
            'campaign': types.String(),
            'utm_campaign': types.String(),
            'content_id': types.String(),
            'content_profit': types.Numeric(),
            'start_date': types.Date(),
            'comments_date_17_06_2025': types.String(),
            'comments_date_24_06_2025': types.String(),
            'comments_date_30_06_25': types.String(),
            'comments_date_02_07_2025': types.String()
        }
    )
    print(f"[{datetime.datetime.now()}] Успешно загружено {len(df)} записей")
    
    # Проверка
    result = pd.read_sql("SELECT COUNT(*) as count FROM yd_campaigns_list", engine)
    print(f"Всего записей в таблице: {result['count'].iloc[0]}")
    
except Exception as e:
    raise Exception(f"Ошибка загрузки: {str(e)}")
finally:
    engine.dispose()
