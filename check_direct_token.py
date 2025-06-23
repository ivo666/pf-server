import requests
import configparser
from pathlib import Path

def load_config():
    config = configparser.ConfigParser()
    config_path = Path(__file__).parent / 'config.ini'
    
    if not config_path.exists():
        raise FileNotFoundError(f"Конфигурационный файл {config_path} не найден")
    
    config.read(config_path)
    return config

def check_yandex_direct_token():
    try:
        config = load_config()
        
        # Получаем настройки из конфига
        TOKEN = config['YandexDirect']['ACCESS_TOKEN']
        CLIENT_ID = config['YandexDirect'].get('CLIENT_ID', '')  # Необязательный параметр
        
        BASE_URL = "https://api.direct.yandex.com/json/v5/"
        
        headers = {
            "Authorization": f"Bearer {TOKEN}",
            "Accept-Language": "ru",
            "Client-Login": CLIENT_ID if CLIENT_ID else "",
        }

        data = {
            "method": "get",
            "params": {
                "SelectionCriteria": {},
                "FieldNames": ["Id", "Name", "State"],
            }
        }

        print("Проверка токена Яндекс.Директ...")
        response = requests.post(
            f"{BASE_URL}campaigns",
            headers=headers,
            json=data
        )
        response.raise_for_status()
        
        result = response.json()
        
        if 'error' in result:
            print("Ошибка при запросе:")
            print(f"Код ошибки: {result['error']['error_code']}")
            print(f"Описание: {result['error']['error_string']}")
            print(f"Детали: {result['error'].get('error_detail', 'нет')}")
            return False
        else:
            campaigns = result.get('result', {}).get('Campaigns', [])
            print("Токен рабочий! Успешное подключение к API Яндекс.Директ.")
            print(f"Найдено кампаний: {len(campaigns)}")
            if campaigns:
                print("Пример названия кампании:", campaigns[0]['Name'])
            return True
            
    except requests.exceptions.RequestException as e:
        print(f"Ошибка при выполнении запроса: {e}")
        return False
    except Exception as e:
        print(f"Ошибка при проверке токена: {e}")
        return False

if __name__ == "__main__":
    if check_yandex_direct_token():
        print("Проверка прошла успешно!")
    else:
        print("Проверка не удалась. Пожалуйста, проверьте настройки в config.ini")
