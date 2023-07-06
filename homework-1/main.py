import csv
import psycopg2

# """Скрипт для заполнения данными таблиц в БД Postgres."""

conn = psycopg2.connect(host='localhost', database='north', user='postgres', password='1058746tch')
try:
    with conn:
        with conn.cursor() as cur:
            with open('north_data/employees_data.csv', 'r', encoding='utf-8') as file:
                data_emp = csv.DictReader(file)
                for i in data_emp:
                    cur.execute("INSERT INTO employees VALUES (%s, %s, %s, %s, %s, %s)",
                                (i['employee_id'], i['first_name'], i['last_name'], i['title'], i['birth_date'],
                                 i['notes']))

            with open('north_data/customers_data.csv', 'r', encoding='utf-8') as file:
                data = csv.DictReader(file)
                for i in data:
                    cur.execute("INSERT INTO customers VALUES (%s, %s, %s)",
                                (i['customer_id'], i['company_name'], i['contact_name']))

            with open('north_data/orders_data.csv', 'r', encoding='utf-8') as file:
                data = csv.DictReader(file)
                for i in data:
                    cur.execute("INSERT INTO orders VALUES (%s, %s, %s, %s, %s)",
                                (i['order_id'], i['customer_id'], i['employee_id'], i['order_date'], i['ship_city']))

finally:
    conn.close()
