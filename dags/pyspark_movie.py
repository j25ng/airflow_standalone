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
    'pyspark_movie',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
    },
    max_active_runs=1,
    max_active_tasks=3,
    description='movie',
    schedule="10 2 * * *",
    start_date=datetime(2015, 1, 1),
    end_date=datetime(2015, 3, 1),
    catchup=True,
    tags=['pyspark', 'movie'],
) as dag:

    def re_partition(ds_nodash):
        import pandas as pd
        from spark_flow.re import re_partition
        df_row_cnt, read_path, write_path= re_partition(ds_nodash)
        print(f'df_row_cnt:{df_row_cnt}')
        print(f'read_path:{read_path}')
        print(f'write_path:{write_path}')

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end', trigger_rule="all_done")

    re_partition = PythonVirtualenvOperator(
        task_id='re.partition',
        python_callable=re_partition,
        system_site_packages=False,
        requirements=["git+https://github.com/j25ng/spark_flow.git"],

    )

    join_df = BashOperator(
        task_id='join.df',
        bash_command="""
            echo "join_df"
        """
    )

    agg_df = BashOperator(
        task_id='agg.df',
        bash_command="""
            echo "add_df"
        """
    )

    start >> re_partition >> join_df >> agg_df >> end
