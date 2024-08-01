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
    'movie',
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
    tags=['movie'],
) as dag:

    def get_data(ds_nodash):
        from movie.api.call import save2df
        df = save2df(ds_nodash, url_param={"": ""})
        print(df.head(5))

    def save_data(ds_nodash):
        from movie.api.call import apply_type2df
        df = apply_type2df(load_dt=ds_nodash)

        g = df.groupby('openDt')
        sum_df = g.agg({'audiCnt': 'sum'}).reset_index()
        print(sum_df)

    def branch_fun(ds_nodash):
        import os
        home_dir = os.path.expanduser("~")
        path = os.path.join(home_dir, f"tmp/test_parquet/loadDt={ds_nodash}")

       #if os.path.exists(f'~/tmp/test_parquet/load_dt={ld}')
        if os.path.exists(path):
            return rm_dir.task_id
        else:
            return "get.start", "echo.task"

    def fun_param(ds_nodash, url_param):
        from movie.api.call import save2df
        df = save2df(ds_nodash, url_param)

        print(df.head(5))

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end', trigger_rule="all_done")

    get_start = EmptyOperator(
        task_id='get.start',
        trigger_rule="all_done"
    )
    
    get_end = EmptyOperator(task_id='get.end')

    multi_y = PythonVirtualenvOperator(
        task_id='multi.y',
        python_callable=fun_param,
        system_site_packages=False,
        requirements=["git+https://github.com/j25ng/movie.git@0.3/api"],
        op_kwargs={"url_param": {"multiMovieYn": "Y"}}
    )

    multi_n = PythonVirtualenvOperator(
        task_id='multi.n',
        python_callable=fun_param,
        system_site_packages=False,
        requirements=["git+https://github.com/j25ng/movie.git@0.3/api"],
        op_kwargs={"url_param": {"multiMovieYn": "N"}}
    )
    
    nation_k = PythonVirtualenvOperator(
        task_id='nation.k',
        python_callable=fun_param,
        system_site_packages=False,
        requirements=["git+https://github.com/j25ng/movie.git@0.3/api"],
        op_kwargs={"url_param": {"repNationCd": "K"}}
    )
    
    nation_f = PythonVirtualenvOperator(
        task_id='nation.f',
        python_callable=fun_param,
        system_site_packages=False,
        requirements=["git+https://github.com/j25ng/movie.git@0.3/api"],
        op_kwargs={"url_param": {"repNationCd": "F"}}
    )

    throw_err = BashOperator(
        task_id='throw.err',
        bash_command="exit 1",
        trigger_rule="all_done"
    )

    get_data = PythonVirtualenvOperator(
        task_id='get.data',
        python_callable=get_data,
        requirements=["git+https://github.com/j25ng/movie.git@0.3/api"],
        system_site_packages=False,
        #venv_cache_path="/home/j25ng/tmp2/airflow_venv/get_data"
    )
    
    save_data = PythonVirtualenvOperator(
        task_id='save.data',
        trigger_rule="one_success",
        python_callable=save_data,
        requirements=["git+https://github.com/j25ng/movie.git@0.3/api"],
        system_site_packages=False,
        #venv_cache_path="/home/j25ng/tmp2/airflow_venv/get_data"
    )

    branch_op = BranchPythonOperator(
        task_id="branch.op",
        python_callable=branch_fun,
    )

    rm_dir = BashOperator(
        task_id='rm.dir',
        bash_command='rm -rf ~/tmp/test_parquet/loadDt={{ ds_nodash }}'
    )

    echo_task = BashOperator(
        task_id="echo.task",
        bash_command="echo 'task'"
    )

    start >> branch_op >> get_start
    start >> throw_err >> save_data 

    branch_op >> [rm_dir, echo_task] >> get_start
    

    get_start >> [get_data, multi_y, multi_n, nation_k, nation_f] >> get_end

    get_end >> save_data >> end

