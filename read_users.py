import requests
import sqlite3
import json
from tabulate import tabulate

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

    try:
        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()  # Проверка на успешный статус ответа
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Ошибка при выполнении запроса: {e}")
        return None

# Создание базы данных и таблиц
conn = sqlite3.connect('crm_logs.db')
cursor = conn.cursor()

# Убедиться, что таблица users существует
cursor.execute('''
    CREATE TABLE IF NOT EXISTS users (
        id TEXT PRIMARY KEY,
        firstName TEXT,
        lastName TEXT,
        role_name TEXT
    )
''')

# Получение данных пользователей и вставка в таблицу users
users_response = fetch_users()
if users_response:
    users = users_response['data']['usersFetcher']['users']

    for user in users:
        cursor.execute('''
            INSERT OR REPLACE INTO users (id, firstName, lastName, role_name)
            VALUES (?, ?, ?, ?)
        ''', (
            user["id"], user["name"]["firstName"], user["name"]["lastName"], user["role"]["name"]
        ))

    conn.commit()

# Закрытие соединения с базой данных
conn.close()