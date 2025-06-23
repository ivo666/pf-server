def check_yandex_direct_token():
    try:
        config = load_config()
        TOKEN = config['YandexDirect']['ACCESS_TOKEN']
        CLIENT_ID = config['YandexDirect'].get('CLIENT_ID', '').strip()  # Удаляем пробелы

        BASE_URL = "https://api.direct.yandex.com/json/v5/"
        
        headers = {
            "Authorization": f"Bearer {TOKEN}",
            "Accept-Language": "ru",
        }
        if CLIENT_ID:  # Добавляем Client-Login только если он есть
            headers["Client-Login"] = CLIENT_ID

        data = {
            "method": "get",
            "params": {
                "SelectionCriteria": {},
                "FieldNames": ["Id", "Name"],
            }
        }

        print("Запрос к API Яндекс.Директ...")
        response = requests.post(f"{BASE_URL}campaigns", headers=headers, json=data)
        response.raise_for_status()
        
        result = response.json()
        
        if 'error' in result:
            print("Ошибка:", result['error']['error_string'])
            return False
        
        print("Успех! Доступные кампании:", len(result['result']['Campaigns']))
        return True

    except Exception as e:
        print("Ошибка:", str(e))
        return False
