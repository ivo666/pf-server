def get_direct_report(token, date_from, date_to):
    url = "https://api.direct.yandex.com/json/v5/reports"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept-Language": "ru",
        "Content-Type": "application/json"
    }

    report_body = {
        "params": {
            "SelectionCriteria": {
                "DateFrom": date_from,
                "DateTo": date_to
            },
            "FieldNames": ["CampaignId"],
            "ReportName": "CampaignId report",
            "ReportType": "CAMPAIGN_PERFORMANCE_REPORT",
            "DateRangeType": "CUSTOM_DATE",
            "Format": "TSV",
            "IncludeVAT": "YES",
            "IncludeDiscount": "NO"
        }
    }

    try:
        logger.info(f"Формирование отчета за {date_from} — {date_to}")
        logger.debug(f"Запрос: {json.dumps(report_body, indent=2)}")
        
        response = requests.post(
            url,
            headers=headers,
            json=report_body,
            timeout=60
        )
        
        logger.debug(f"Статус: {response.status_code}")
        logger.debug(f"Ответ: {response.text[:500]}...")
        
        if response.status_code == 200:
            return response.text
        elif response.status_code == 201:
            download_url = response.headers.get('Location')
            if download_url:
                logger.info("Отчет формируется, ожидаем...")
                time.sleep(30)
                return download_report(download_url, headers)
            logger.error("Не получен URL для скачивания")
            return None
        else:
            response.raise_for_status()
            return None
            
    except requests.exceptions.RequestException as e:
        logger.error(f"Ошибка запроса: {str(e)}")
        if hasattr(e, 'response') and e.response:
            logger.error(f"Детали ошибки: {e.response.text}")
        return None
