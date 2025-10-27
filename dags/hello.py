# ./dags/hello.py
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime

def say_hello():
    print("Hello Airflow!")

def say_goodbye():
    print("Goodbye Airflow!")

with DAG(
    "hello_dag",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["demo"],
) as dag:
    t1 = PythonOperator(task_id="hello",   python_callable=say_hello)
    t2 = PythonOperator(task_id="goodbye", python_callable=say_goodbye)

    # 依存関係：hello が終わってから goodbye
    t1 >> t2