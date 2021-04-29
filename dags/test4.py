from builtins import range
from datetime import timedelta, datetime

from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
import boto3
from airflow.operators.python_operator import PythonOperator
import json

DEFAULT_ARGS = {
    'owner': 'dianaUpdated'
}

with DAG(   
    dag_id='test4',
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=2),
    start_date=days_ago(0),
    catchup=False,
    schedule_interval='*/2 * * * *',
) as dag:
    commands = """
    curl -L https://idpjobschedulerservice-qal.api.intuit.com/api/v1/pipelines/Intuit.data.datalake.p2ptestppl2/status?environment=E2E;
    sleep 10;
    ls -lrt ;
    """
    # bpp_call=PythonOperator(
    #         task_id='bpp_call',
    #         python_callable=bpp_call_operator,
    #         provide_context=True
    # )
    run1 = BashOperator(
        task_id='run_after_loop_1',
        bash_command=commands,
        dag=dag,
    )
    run2 = BashOperator(
        task_id='test_step2',
        bash_command=commands,
        dag=dag,
    )
    run3 = BashOperator(
        task_id='test_step3',
        bash_command=commands,
        dag=dag,
    )
    run4 = BashOperator(
        task_id='test_last_step',
        bash_command=commands,
        dag=dag,
    )
    run1 >> run2 >> run3 >> run4
