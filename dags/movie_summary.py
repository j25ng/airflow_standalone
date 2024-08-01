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

# import func
#from movie.api.call import gen_url, req, get_key, req2list, list2df, save2df

with DAG(
    'movie_summary',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
    },
    max_active_runs=1,
    max_active_tasks=3,
    description='movie',
    schedule="10 2 * * *",
    start_date=datetime(2024, 7, 24),
    catchup=True,
    tags=['movie', 'summary'],
) as dag:

    def apply_type():
        return 0

    def merge_df():
        return 0

    def de_dup():
        return 0

    def summary_df():
        return 0

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end', trigger_rule="all_done")

    apply_type = PythonVirtualenvOperator(
        task_id='apply.type',
        python_callable=apply_type,
    )

    merge_df = PythonVirtualenvOperator(
        task_id='merge.df',
        python_callable=merge_df,
    )

    de_dup = PythonVirtualenvOperator(
        task_id='de.dup',
        python_callable=de_dup,
    )

    summary_df = PythonVirtualenvOperator(
        task_id='summary.df',
        python_callable=summary_df,
    )

    start >> apply_type >> merge_df >> de_dup >> summary_df >> end
