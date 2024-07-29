from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
from pprint import pprint

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

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

    def get_data(ds, **kwargs):
        print(ds)
        pprint(kwargs)
        print(f"ds_nodash => {kwargs['ds_nodash']}")
        key = get_key()
        print(f"MOVIE_API_KEY => {key}")
        date = kwargs['ds_nodash']
        df = save2df(date)
        print(df.head(5))
    
    # t1, t2 and t3 are examples of tasks created by instantiating operators
    task_start = EmptyOperator(task_id='start')
    task_end = EmptyOperator(task_id='end', trigger_rule="all_done")

    task_get_data = PythonOperator(
        task_id='get.data',
        python_callable=get_data
    )
    
    task_save_data = BashOperator(
        task_id='save.data',
        bash_command="""
        """
    )

    task_start >> task_get_data >> task_save_data >> task_end
