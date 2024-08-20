from datetime import datetime, timedelta
from textwrap import dedent
from pprint import pprint

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
    'movies_dynamic_json',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
    },
    description='movies dynamic json DAG',
    schedule="@once",
    start_date=datetime(2015, 1, 1),
    end_date=datetime(2015, 1, 1),
    catchup=True,
    tags=['movis', 'dynamic', 'json'],
) as dag:

    def get_data(ds_nodash):
        from movdata.movieList import save_movie_json
        year = str(ds_nodash)[:4]
        total_pages = 10
        file_path = "/home/j25ng/data/json/movie.json"

        save_movie_json(year, total_pages, file_path)

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    task_start = EmptyOperator(task_id='start')
    task_end = EmptyOperator(task_id='end', trigger_rule="all_done")

    task_get_data = PythonVirtualenvOperator(
        task_id='get.data',
        python_callable=get_data,
        requirements=["git+https://github.com/j25ng/movdata.git@0.2/movieList"],
        system_site_packages=False,
    )

    task_pars_parq = BashOperator(
        task_id='parsing.parquet',
        bash_command="""
            $SPARK_HOME/bin/spark-submit /home/j25ng/airflow/py/pars_parq.py parsing_parquet 
        """
    )

    task_sel_parq = BashOperator(
        task_id='select.parquet',
        bash_command="""
            $SPARK_HOME/bin/spark-submit /home/j25ng/airflow/py/sel_parq.py select_parquet 
        """
    )

    task_start >> task_get_data >> task_pars_parq >> task_sel_parq >> task_end
