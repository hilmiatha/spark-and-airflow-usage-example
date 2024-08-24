from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable, Connection
from airflow.providers.sftp.operators.sftp import SFTPOperator
from airflow.operators.python import PythonVirtualenvOperator

from datetime import datetime, timedelta
from modules.requests_module import get_request


def print_context(ds=None, **kwargs):
    """Print the Airflow context and ds variable from the context."""
    print("::group::All kwargs")
    print(kwargs)
    print("::endgroup::")
    print("::group::Context variable ds")
    print(ds)
    print("::endgroup::")
    print(get_request())
    print('test')
    print('variable', Variable.get('url'))
    print(Connection.get_connection_from_secrets("postgres-sample").host)
    return "Whatever you return gets printed in the logs"

def print_dataframe(**kwargs):
    import pandas as pd
    data = [
        {"name": "singgih"},
        {"name": "amir"},
        {"name": "fikri"}
    ]
    print(pd.DataFrame(data))
    return True


with DAG(
    dag_id='first_sample_dag_test_singgih',
    start_date=datetime(2024, 7, 31),
    schedule_interval='0 23 * * *',  # https://crontab.guru/#0_23_*_*_*
    catchup=True,
    default_args={
        'owner' : 'singgih',
        'retries': 5,
        'retry_delay': timedelta(minutes=2),
    }
) as dag:

    start_task = EmptyOperator(
        task_id='start'
    )

    print_hello_world = BashOperator(
        task_id='print_hello_world',
        bash_command='echo "HelloWorld!"'
    )

    print_hello_world_py = BashOperator(
        task_id='print_hello_world_py',
        bash_command='python /opt/airflow/dags/modules/print_hello.py'
    )

    run_this = PythonOperator(
        task_id="print_the_context",
        python_callable=print_context
    )

    run_sftp = SFTPOperator(
        task_id="test_sftp",
        ssh_conn_id="ssh_default",
        local_filepath="/opt/airflow/dags/modules/print_hello.py",
        remote_filepath="/tmp/tmp1/tmp2/print_hello.py",
        operation="put",
        create_intermediate_dirs=True
    )

    virtualenv_task = PythonVirtualenvOperator(
        task_id = 'virtualenv_task',
        python_callable = print_dataframe,
        requirements = ["pandas"],
        system_site_packages = False
    )

    end_task = EmptyOperator(
        task_id='end'
    )

    start_task >> print_hello_world >> print_hello_world_py >> end_task  # bash command
    start_task >> run_this  >> end_task  # python operator
    start_task >> run_sftp >> end_task # sftp operator
    start_task >> virtualenv_task >> end_task # python virtualenv operator
