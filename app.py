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
            filters: {{include:{{action:UPDATE, createdAt:{{gte:"{start_date} 00:00:00",lte:"{end_date} 00:00:00"}}, performer:{{entity:User}}, entity:{{entity:Order}}}}}}
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

# Функция для получения данных пользователей
def fetch_users():
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
        "query": """
        {
          usersFetcher(
            filters: {include:{banned:false}}
            pagination: { pageNumber: 1, pageSize: 100 }
          ) {
            users {
              id
              name{firstName, lastName}
              role {name}
            }
          }
        }
        """,
        "variables": {}
    }

    response = requests.post(url, headers=headers, json=data)
    return response.json()

# Создание базы данных и таблиц
conn = sqlite3.connect('crm_logs.db')
cursor = conn.cursor()
cursor.execute('''DROP TABLE IF EXISTS LOGS''')
cursor.execute('''
    CREATE TABLE IF NOT EXISTS logs (
        id TEXT PRIMARY KEY,
        createdAt TEXT,
        user_id INT,
        new_resp_user_id TEXT NULL,
        old_resp_user_id TEXT NULL,
        order_id INT,
        old_status_id INT,
        old_status_name TEXT,
        new_status_id INT,
        new_status_name TEXT,
        cart_total REAL,
        duration INT,
        new_category_id INT,
        old_category_id INT
    )
''')

# Создание таблицы users
cursor.execute('''
    CREATE TABLE IF NOT EXISTS users (
        id TEXT PRIMARY KEY,
        firstName TEXT,
        lastName TEXT,
        role_name TEXT
    )
''')

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
cursor.execute('''UPDATE LOGS SET user_id=new_resp_user_id where new_resp_user_id is not null''')
conn.commit()

# Получение данных пользователей и вставка в таблицу users
users_response = fetch_users()
users = users_response['data']['usersFetcher']['users']

for user in users:
    cursor.execute('''
        INSERT OR REPLACE INTO users (id, firstName, lastName, role_name)
        VALUES (?, ?, ?, ?)
    ''', (
        user["id"], user["name"]["firstName"], user["name"]["lastName"], user["role"]["name"]
    ))

conn.commit()

# Вывод итоговых данных
cursor.execute('''
    SELECT role_name, user_id, firstName, lastName,
           SUM(CASE WHEN old_status_id in (10,27,30,18) and new_status_id not in (10,27,30,18) THEN 1 ELSE 0 END) as total_orders,
           SUM(CASE WHEN new_status_id = 3 and old_status_id=4 THEN 1 ELSE 0 END) as approved,
           CASE WHEN SUM(CASE WHEN old_status_id in (10,27,30,18) and new_status_id not in (10,27,30,18) THEN 1 ELSE 0 END)=0 then 0 ELSE ROUND(SUM(CASE WHEN new_status_id = 3 and old_status_id=4 THEN 1 ELSE 0 END)*100.0/SUM(CASE WHEN old_status_id in (10,27,30,18) and new_status_id not in (10,27,30,18) THEN 1 ELSE 0 END),2) END as cross_sell,
           SUM(CASE WHEN new_status_id = '13' THEN 1 ELSE 0 END) as rejected,
           SUM(CASE WHEN new_status_id = '32' THEN 1 ELSE 0 END) as trash,
           SUM(CASE WHEN new_category_id like '%,8,%' or old_category_id like '%,8,%' THEN 1 ELSE 0 END) as cross,
           CASE WHEN SUM(CASE WHEN new_status_id = 3 and old_status_id=4 THEN 1 ELSE 0 END)=0 then 0 else ROUND(cart_total/SUM(CASE WHEN new_status_id = 3 and old_status_id=4 THEN 1 ELSE 0 END)/100, 2) END as avg_sum,    
           SUM(CASE WHEN new_status_id = 3 and old_status_id=4 THEN cart_total ELSE 0 END)/100 as sum,
           CASE WHEN SUM(CASE WHEN new_status_id = '11' THEN 1 ELSE 0 END)=0 then 0 ELSE ROUND((SUM(CASE WHEN new_status_id = '6' THEN 1 ELSE 0 END) * 1.0 / NULLIF(SUM(CASE WHEN new_status_id = '11' THEN 1 ELSE 0 END), 0)) * 100.0, 2) END as buyout_percentage,
           SUM(duration)/60 as total_duration
    FROM logs
    LEFT JOIN users
        on users.id=logs.user_id
    GROUP BY user_id
    ORDER BY total_orders DESC
''')

rows = cursor.fetchall()

# Форматирование данных в виде таблицы
table = tabulate(rows, headers=["Отдел", "ID Оператора", "Имя", "Фамилия", "Всего принято", "Апрув", "Апрув %", "Отказ", "Треш", "Кроссы", "Ср.чек", "Сумма за период", "Выкуп %", "Время в разговоре"], tablefmt="pretty")
print(table)

conn.close()