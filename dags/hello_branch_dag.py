from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

# --- 抽出（例：件数を返す想定） ---
def pull_api():
    return 0

def pull_s3():
    return 0

def pull_db():
    return 0

# 集計（extract 3つの結果 = 件数の合計）
def collect(ti):
    total = (
        (ti.xcom_pull(task_ids="extract.api") or 0)
        + (ti.xcom_pull(task_ids="extract.s3") or 0)
        + (ti.xcom_pull(task_ids="extract.db") or 0)
    )
    return total

# 変換
def transform(results_total: int):
    print(f"[Transform] total rows = {results_total}")

with DAG(
    dag_id="hello_branch_dag",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["demo", "branch"],
):
    start = EmptyOperator(task_id="start")

    # ---- TaskGroup: extract（並列）----
    with TaskGroup("extract") as extract:
        t_api = PythonOperator(task_id="api", python_callable=pull_api)
        t_s3  = PythonOperator(task_id="s3",  python_callable=pull_s3)
        t_db  = PythonOperator(task_id="db",  python_callable=pull_db)
        # 依存を書かない＝3つ並列

    collect_results = PythonOperator(
        task_id="collect_results",
        python_callable=collect,
    )

    # ---- 分岐：合計>0 なら transform、0 なら nothing_to_do へ ----
    branch = BranchPythonOperator(
        task_id="branch",
        python_callable=lambda ti: (
            "transform" if (ti.xcom_pull(task_ids="collect_results") or 0) > 0
            else "nothing_to_do"
        ),
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=lambda ti: transform(ti.xcom_pull(task_ids="collect_results")),
    )

    nothing = EmptyOperator(task_id="nothing_to_do")

    # 分岐の後で合流（片方は SKIPPED になるので trigger_rule が超重要）
    end = EmptyOperator(
        task_id="end",
        trigger_rule="none_failed_min_one_success"  # ← これを付けないと end が動かない
    )

    # 依存関係
    start >> extract >> collect_results >> branch
    branch >> transform_task >> end
    branch >> nothing >> end
