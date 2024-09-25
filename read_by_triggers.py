import requests
import sqlite3
import json
from tabulate import tabulate

# Функция для выполнения запроса API
def fetch_logs(page_number, start_date, end_date):
    url = 'https://de.backend.salesrender.com/companies/61/CRM'
    headers = {
        'Accept-Encoding': 'gzip, deflate, br',
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'Connection': 'keep-alive',
        'Origin': 'chrome-extension://flnheeellpciglgpaodhkhmapeljopja',
        'Authorization': 'Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2RlLmJhY2tlbmQuc2FsZXNyZW5kZXIuY29tLyIsImF1ZCI6IkNSTSIsImp0aSI6Ijc5YWZhNTc0MDI3NWFjNjJjYWFiZDI3N2RkYjRlZjhhIiwiaWF0IjoxNzI2MjE0ODg2LCJ0eXBlIjoiYXBpIiwiY2lkIjoiNjEiLCJyZWYiOnsiYWxpYXMiOiJBUEkiLCJpZCI6IjYifX0.naAtK0BCWZ5oP209IqgvU-wPYFxOOElHt8uJjbxF-08'
    }
    data = {
        "query": f"""
        {{
          logsFetcher(
            filters: {{include:{{action:UPDATE, createdAt:{{gte:"{start_date} 00:00:00",lte:"{end_date} 00:00:00"}}, performer:{{entity:OrderTriggerInterface}}, entity:{{entity:Order}}}}}}
            pagination: {{ pageNumber: {page_number}, pageSize: 100 }}
            sort: {{ field: createdAt, direction: ASC }}
          ) {{
            log {{
              id 
              createdAt
              performer{{id ...on User{{id}}}}
              action
              old{{id ...on Order{{cart{{total,items{{sku{{item{{category{{id}}}}}}}}}},data{{userFields{{value{{id}}}}}},status{{id,name}}}}}}
              new{{id ...on Order{{cart{{total,items{{sku{{item{{category{{id}}}}}}}}}},data{{userFields{{value{{id}}}}}},status{{id,name}}}}}}
              duration
            }}
            pageInfo {{
              itemsCount
              pageSize
              pageNumber
              pagesCount
            }}
          }}
        }}
        """,
        "variables": {}
    }

    response = requests.post(url, headers=headers, json=data)
    return response.json()


# Создание базы данных и таблиц
conn = sqlite3.connect('crm_logs.db') 
cursor = conn.cursor()

# Указание дат
start_date = "2024-09-12"
end_date = "2024-09-13"

# Получение первой страницы для определения общего количества страниц
initial_response = fetch_logs(1, start_date, end_date)
total_pages = initial_response['data']['logsFetcher']['pageInfo']['pagesCount']

# Обработка всех страниц
for page in range(1, total_pages + 1):
    response = fetch_logs(page, start_date, end_date)
    logs = response['data']['logsFetcher'].get('log', [])

    if logs is None:
        continue

    # Фильтрация данных
    filtered_logs = [
        {
            "id": log["id"],
            "createdAt": log["createdAt"],
            "user_id": log["performer"]["id"],
            "new_resp_user_id": next((field["value"]["id"] for field in log["new"]["data"]["userFields"] if "value" in field), None),
            "old_resp_user_id": next((field["value"]["id"] for field in log["old"]["data"]["userFields"] if "value" in field), None),
            "order_id": log["old"]["id"],
            "old_status_id": log["old"]["status"]["id"],
            "old_status_name": log["old"]["status"]["name"],
            "new_status_id": log["new"]["status"]["id"],
            "new_status_name": log["new"]["status"]["name"],
            "cart_total": log["new"]["cart"]["total"],
            "duration": log["duration"],
            "new_category_id": ','.join([item["sku"]["item"]["category"]["id"] for item in log["new"]["cart"]["items"]])+',',
            "old_category_id": ','.join([item["sku"]["item"]["category"]["id"] for item in log["old"]["cart"]["items"]])+','
        }
        for log in logs
        if log["old"]["status"]["id"] != log["new"]["status"]["id"]
    ]

    # Вставка данных в таблицу
    for log in filtered_logs:
        cursor.execute('''
            INSERT OR REPLACE INTO logs (id, createdAt, user_id, new_resp_user_id, old_resp_user_id, order_id, old_status_id, 
                       old_status_name, new_status_id, new_status_name, cart_total, duration, new_category_id, old_category_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            log["id"], log["createdAt"],log["user_id"], log["new_resp_user_id"], log["old_resp_user_id"],
            log["order_id"], log["old_status_id"], log["old_status_name"], log["new_status_id"], log["new_status_name"], 
            log["cart_total"], log["duration"], ','+log["new_category_id"], ','+log["old_category_id"]
        ))

    conn.commit()

# Обновление ответственного пользователя
cursor.execute('''
    UPDATE LOGS 
    SET user_id=new_resp_user_id 
    WHERE new_resp_user_id IS NOT NULL 
    AND createdAt BETWEEN ? AND ?
''', (start_date, end_date))
conn.commit()
conn.close()