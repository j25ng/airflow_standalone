from datetime import datetime, timedelta
from textwrap import dedent
from pprint import pprint as pp

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
    schedule="30 2 * * *",
    start_date=datetime(2024, 7, 24),
    catchup=True,
    tags=['movie', 'summary'],
) as dag:
    REQUIREMENTS=["git+https://github.com/j25ng/movie.git@0.3/api"]

    #def gen_empty(id):
    def gen_empty(*ids):
        tasks = []
        for id in ids:
            task = EmptyOperator(task_id=id)
            tasks.append(task)
        return tuple(tasks)
    
    #def gen_vpython(id):
    def gen_vpython(**kw):
        id = kw['id']
        fn = kw['fn']
        url = kw['url']

        task = PythonVirtualenvOperator(
            task_id=id,
            python_callable=fn,
            system_site_packages=False,
            requirements=REQUIREMENTS,
            op_kwargs={
                "url_param" : url,
                }
            )
        return task

    def pro_data(**kwargs):
        print("pro_data")
        print(kwargs['url_param'])

    def pro_data2(**Kwargs):
        print(kwargs)

    def apply_type():
        return 0

    def merge_df():
        print('merge')

    def de_dup():
        print('de_dup')

    def summary_df():
        print('summary')

    #start = gen_empty('start')
    #end = gen_emtpy('end')
    start, end = gen_empty('start', 'end')

    apply_type = gen_vpython(
            id = 'apply.type',
            fn = pro_data,
            task_name = 'apply_type!!!',
            url = { 'multiMovieYn': 'Y' }
    )

    merge_df = gen_vpython(
            id = 'merge.df',
            fn = pro_data,
            task_name = 'merge_df!!!',
            url = { 'multiMovieYn': 'Y' }
    )

    de_dup = gen_vpython(
            id = 'de.dup', 
            fn = pro_data, 
            task_name = 'de_dup!!!',
            url = { 'multiMovieYn': 'Y' }
    )

    summary_df = gen_vpython(
            id = 'summary.df',
            fn = pro_data,
            task_name = 'summary_df!!!',
            url = { 'multiMovieYn': 'Y' }
    )

start >> apply_type >> merge_df >> de_dup >> summary_df >> end
