import pandas as pd
import gspread
from sqlalchemy import create_engine, types
import configparser
from pathlib import Path

# 1. Загрузка конфигурации
config = configparser.ConfigParser()
config.read('config.ini')

# Путь к credentials.json (добавьте этот параметр в config.ini)
CREDENTIALS_PATH = config.get('GoogleSheets', 'CREDENTIALS_PATH', fallback=None)
if not CREDENTIALS_PATH:
    raise ValueError("Не указан путь к credentials.json в config.ini")

# 2. Получение данных из Google Sheets
try:
    gc = gspread.service_account(filename=CREDENTIALS_PATH)
    sh = gc.open("ProfiFilter_cpc_fvkart")
    worksheet = sh.worksheet("Campaigns")
    data = worksheet.get_all_records()
    df = pd.DataFrame(data)
    
    print(f"Успешно получено {len(df)} записей из Google Sheets")
except Exception as e:
    raise Exception(f"Ошибка при получении данных из Google Sheets: {str(e)}")

# 3. Подготовка данных
df.columns = df.columns.str.replace('.', '_').str.lower()

# Преобразование дат с явным указанием формата
if 'start_date' in df.columns:
    df['start_date'] = pd.to_datetime(df['start_date'], format='%d.%m.%Y', errors='coerce')
else:
    print("Предупреждение: столбец 'start_date' не найден в данных")

# 4. Подключение к удалённой PostgreSQL
try:
    db_config = {
        'host': config.get('Database', 'HOST'),
        'database': config.get('Database', 'DATABASE'),
        'user': config.get('Database', 'USER'),
        'password': config.get('Database', 'PASSWORD'),
        'port': config.get('Database', 'PORT', fallback='5432')
    }
    
    connection_string = f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
    engine = create_engine(connection_string)
    
    # Проверка подключения
    with engine.connect() as conn:
        print(f"Успешное подключение к PostgreSQL серверу: {db_config['host']}")
except Exception as e:
    raise Exception(f"Ошибка подключения к PostgreSQL: {str(e)}")

# 5. Загрузка данных в PostgreSQL
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
    print(f"Успешно загружено {len(df)} записей в таблицу yd_campaigns_list")
    
    # Проверка загрузки
    result = pd.read_sql("SELECT COUNT(*) as count FROM yd_campaigns_list", engine)
    print(f"Всего записей в таблице: {result['count'].iloc[0]}")
    
except Exception as e:
    raise Exception(f"Ошибка при загрузке данных в PostgreSQL: {str(e)}")
finally:
    engine.dispose()
