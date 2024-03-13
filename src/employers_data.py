import json

import psycopg2


#
# def create_db_hh():
#     conn_params = psycopg2.connect(host="127.0.0.1", database="employers_vacancies_hh", user="postgres", password="bd_pyt", client_encoding="utf-8")
#     cursor = conn_params.cursor()
#     # автокоммит
#     conn_params.autocommit = True
#     # команда для создания базы данных employers_vacancies_hh
#     cursor.execute("CREATE DATABASE employers_vacancies_hh")
#     print("База данных успешно создана")
#     cursor.close()
#     conn_params.close()

def create_table_employershh():
    conn_params = psycopg2.connect(host="127.0.0.1", database="employers_vacancies_hh", user="postgres",
                                   password="bd_pyt", client_encoding="utf-8")
    cursor = conn_params.cursor()
    # автокоммит
    conn_params.autocommit = True
    # команда для создания таблицы employers_hh
    cursor.execute(
        "CREATE TABLE employers_hh (id int PRIMARY KEY, name text, open_vacancies int, employer_url text, vacancies_url text)")
    conn_params.close()


#
def insert_table_employershh(id: int, name: str, open_vacancies: int, employer_url:str, vacancies_url: str):
    conn_params = psycopg2.connect(host="127.0.0.1", database="employers_vacancies_hh", user="postgres",
                                   password="bd_pyt", client_encoding="utf-8")
    try:
        with conn_params:
            with conn_params.cursor() as cur:
                conn_params.autocommit = True
                cur.execute("INSERT INTO employers_hh VALUES (%s, %s, %s, %s, %s)", (id, name, open_vacancies, employer_url, vacancies_url))
    finally:
        conn_params.close()


dbf_tab = create_table_employershh()
with open("employers_hh.json", "r+", encoding='utf-8') as file:
    db = json.load(file)


for i in db: # your python json data
    id = int(i["id"])
    name = str(i["name"])
    open_vacancies = int(i["open_vacancies"])
    employer_url = str(i["url"])
    vacancies_url = str(i["vacancies_url"])
    insert_table_employershh(id, name, open_vacancies,employer_url, vacancies_url)