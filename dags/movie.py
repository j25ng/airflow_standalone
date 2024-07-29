from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
from pprint import pprint

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import (
        PythonOperator,
        PythonVirtualenvOperator
    )

# import func
from movie.api.call import gen_url, req, get_key, req2list, list2df, save2df

with DAG(
    'movie',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
    },
    description='import db DAG',
    schedule="10 4 * * *",
    start_date=datetime(2024, 7, 24),
    catchup=True,
    tags=['movie'],
) as dag:

    def print_context(ds=None, **kwargs):
        """Print the Airflow context and ds variable from the context."""
        print("::group::All kwargs")
        print(kwargs)
        print("::endgroup::")
        print("::group::Context variable ds")
        print(ds)
        print("::endgroup::")
        return "Whatever you return gets printed in the logs"

    def get_data(ds, **kwargs):
        print(ds)
        print(kwargs)
        print(f"ds_nodash => {kwargs['ds_nodash']}")
        key = get_key()
        print(f"MOVIE_API_KEY => {key}")
        date = kwargs['ds_nodash']
        df = save2df(date)
        print(df.head(5))

    def branch_fun(**kwargs):
        ld = kwargs['ds_nodash']
        import os
        if os.path.exists(f'~/tmp/test_parquet/load_dt={ld}'):
            return rm_dir 
        else:
            return get_data
    branch_op = BranchPythonOperator(
        task_id="branch.op",
        python_callable=branch_fun,
    )

    run_this = PythonOperator(
        task_id="print_the_context",
        python_callable=print_context,
    )
    
    # t1, t2 and t3 are examples of tasks created by instantiating operators
    task_start = EmptyOperator(task_id='start')
    task_end = EmptyOperator(task_id='end', trigger_rule="all_done")

    task_get_data = PythonVirtualenvOperator(
        task_id='get.data',
        python_callable=get_data,
        requirements=["git+https://github.com/j25ng/movie.git@0.2/api"],
        system_site_packages=False,
    )
    
    task_save_data = BashOperator(
        task_id='save.data',
        bash_command='date'
    )

    rm_dir = BashOperator(
        task_id='rm.dir',
        bash_command='rm -rf ~/tmp/test_parquet/load_dt={{ ds_nodash }}'
    )

    task_start >> branch_op
    branch_op >> rm_dir >> task_get_data
    branch_op >> task_get_data >> task_save_data >> task_end
