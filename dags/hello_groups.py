from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

def pull_api():
    return "API OK"

def pull_s3():
    return "S3 OK"

def pull_db():
    return "DB OK"

def clean_task(results):
    print(f"[Transform] results: {results}")

with DAG(
    dag_id="hello_groups_dag",
    start_date=datetime(2025,1,1),
    schedule="@daily",
    catchup=False,
) as dag:

    start = PythonOperator(task_id="start", python_callable=lambda: print("start!"))

    with TaskGroup("extract") as extract:
        t_api = PythonOperator(task_id="api", python_callable=pull_api)
        t_s3  = PythonOperator(task_id="s3",  python_callable=pull_s3)
        t_db  = PythonOperator(task_id="db",  python_callable=pull_db)

    # XCom合体タスク（upstream全部が完了してから一つのXComでリスト受取）
    def collect(ti):
        api_res = ti.xcom_pull(task_ids="extract.api")
        s3_res  = ti.xcom_pull(task_ids="extract.s3")
        db_res  = ti.xcom_pull(task_ids="extract.db")
        return [api_res, s3_res, db_res]

    collect_results = PythonOperator(
        task_id="collect_results",
        python_callable=collect,
    )

    transform = PythonOperator(
        task_id="transform",
        python_callable=lambda ti: clean_task(ti.xcom_pull(task_ids="collect_results")),
    )

    end = PythonOperator(task_id="end", python_callable=lambda: print("end!"))

    start >> extract >> collect_results >> transform >> end
