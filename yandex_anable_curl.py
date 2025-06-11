import requests

# Конфигурация
OAUTH_TOKEN = "y0__xDFpPhxGMefLCDCyJnYEoKHyvz-OcOa8ZZkupeJKlFmzb6S"
API_URL = "https://api-metrika.yandex.net/management/v1/counters"

def get_metrika_counters():
    """Запрашивает список счётчиков Яндекс.Метрики"""
    headers = {
        "Authorization": f"OAuth {OAUTH_TOKEN}"
    }
    
    try:
        response = requests.get(API_URL, headers=headers)
        response.raise_for_status()  # Проверка на ошибки HTTP
        
        # Возвращаем данные в формате JSON
        return response.json()
        
    except requests.exceptions.RequestException as e:
        print(f"Ошибка при запросе к API: {e}")
        return None

if __name__ == "__main__":
    print("Запрашиваю список счётчиков Яндекс.Метрики...")
    
    counters_data = get_metrika_counters()
    
    if counters_data:
        print("\nУспешный ответ от API:")
        print(f"Найдено счётчиков: {counters_data.get('rows', 0)}")
        
        # Выводим основные данные по счётчикам
        for counter in counters_data.get('counters', []):
            print(f"\nID: {counter['id']}")
            print(f"Название: {counter['name']}")
            print(f"Сайт: {counter['site']}")
            print(f"Статус: {counter['status']}")
    else:
        print("Не удалось получить данные")
