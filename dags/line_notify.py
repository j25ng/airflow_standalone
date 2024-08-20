from datetime import datetime, timedelta
from textwrap import dedent
from pprint import pprint
from airflow.models.dagrun import DagRun

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
    'line_notify',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
    },
    description='movies dynamic json DAG',
    schedule="10 2 * * *",
    start_date=datetime(2015, 1, 1),
    end_date=datetime(2015, 1, 5),
    catchup=True,
    tags=['line', 'notify'],
) as dag:

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    task_start = EmptyOperator(task_id='start')
    task_end = EmptyOperator(task_id='end', trigger_rule="all_done")
    
    task_bash_job = BashOperator(
        task_id='bash.job',
        bash_command="""
            # 0 또는 1을 랜덤하게 생성
            RANDOM_NUM=$RANDOM
            REMAIN=$(( RANDOM_NUM % 3 ))
            echo "RANDOM_NUM:$RANDOM_NUM, REMAIN:$REMAIN"

            if [ $REMAIN -eq 0 ]; then
                echo "작업이 성공했습니다."
                exit 0

            else
                echo "작업이 실패했습니다."
                exit 1
            fi
        """
    )

    task_notify_success = BashOperator(
        task_id='notify.success',
        bash_command="""
            curl -X POST -H 'Authorization: Bearer {{ var.value.LINE_KEY }}' -F 'message=success' https://notify-api.line.me/api/notify
        """
    )

    task_notify_fail = BashOperator(
        task_id='notify.fail',
        trigger_rule='one_failed',
        bash_command="""
            curl -X POST -H 'Authorization: Bearer {{ var.value.LINE_KEY }}' -F 'message=fail' https://notify-api.line.me/api/notify
        """
    )

    task_start >> task_bash_job >> [ task_notify_success, task_notify_fail ] >> task_end
