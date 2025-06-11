import requests

# Простой тестовый запрос к API
url = "https://api-metrika.yandex.net/management/v1/counters"
headers = {"Authorization": "OAuth YOUR_TOKEN"}
response = requests.get(url, headers=headers)
print(response.status_code, response.json())
