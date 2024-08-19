from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import (
        PythonOperator,
        PythonVirtualenvOperator,
        BranchPythonOperator
    )

with DAG(
    'movies-dynamic-json',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
    },
    description='make parquet DAG',
    schedule="10 4 * * *",
    start_date=datetime(2024, 7, 10),
    catchup=True,
    tags=['movis', 'dynamic', 'json'],
) as dag:

    def get_data():
        pass

    def parsing_parquet():
        pass

    def select_parquet():
        pass

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    task_start = EmptyOperator(task_id='start')
    task_end = EmptyOperator(task_id='end', trigger_rule="all_done")

    task_get_data = PythonVirtualenvOperator(
        task_id='get.data',
        python_callable=get_data,
        requirements=["git+https://github.com/j25ng/movie.git@0.3/api"],
        system_site_packages=False,
    )

    task_parsing_parquet = PythonVirtualenvOperator(
        task_id='parsing.parquet',
        python_callable=parsing_parquet,
        requirements=["git+https://github.com/j25ng/movie.git@0.3/api"],
        system_site_packages=False,
    )

    task_select_parquet = PythonVirtualenvOperator(
        task_id='select.parquet',
        python_callable=select_parquet,
        requirements=["git+https://github.com/j25ng/movie.git@0.3/api"],
        system_site_packages=False,
    )

    task_start >> task_get_data >> task_parsing_parquet >> task_select_parquet >> task_end
