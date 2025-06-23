import requests
import configparser
from pathlib import Path
import sys

def load_config():
    try:
        config = configparser.ConfigParser()
        config_path = Path(__file__).parent / 'config.ini'
        
        if not config_path.exists():
            print(f"❌ Ошибка: Файл config.ini не найден по пути: {config_path}")
            sys.exit(1)
        
        config.read(config_path)
        
        if 'YandexDirect' not in config:
            print("❌ Ошибка: В config.ini нет секции [YandexDirect]")
            sys.exit(1)
            
        return config
    
    except Exception as e:
        print(f"❌ Ошибка при чтении config.ini: {e}")
        sys.exit(1)

def check_yandex_direct_token():
    try:
        print("🔍 Загрузка конфигурации...")
        config = load_config()
        
        TOKEN = config['YandexDirect']['ACCESS_TOKEN']
        print("✅ Конфиг загружен. Проверяем токен...")
        
        BASE_URL = "https://api.direct.yandex.com/json/v5/"
        
        headers = {
            "Authorization": f"Bearer {TOKEN}",
            "Accept-Language": "ru",
        }
        
        data = {
            "method": "get",
            "params": {
                "SelectionCriteria": {},
                "FieldNames": ["Id", "Name"],
            }
        }

        print("🔄 Отправка запроса к API Яндекс.Директ...")
        response = requests.post(f"{BASE_URL}campaigns", headers=headers, json=data, timeout=10)
        response.raise_for_status()
        
        result = response.json()
        
        if 'error' in result:
            print(f"❌ Ошибка API: {result['error']['error_string']}")
            return False
        
        campaigns = result.get('result', {}).get('Campaigns', [])
        print(f"✅ Успех! Найдено кампаний: {len(campaigns)}")
        if campaigns:
            print(f"Пример кампании: ID={campaigns[0]['Id']}, Name='{campaigns[0]['Name']}'")
        return True

    except requests.exceptions.RequestException as e:
        print(f"❌ Ошибка запроса: {e}")
        return False
    except Exception as e:
        print(f"❌ Неожиданная ошибка: {e}")
        return False

if __name__ == "__main__":
    print("=== Проверка токена Яндекс.Директ ===")
    if check_yandex_direct_token():
        print("🎉 Проверка пройдена успешно!")
    else:
        print("🔴 Проверка не удалась. См. ошибки выше.")
    print("Готово.")
