import datetime as dt
import time
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator


def greet():
    print('Write in file')
    print('Sleep 90 seconds')
    time.sleep(90)
    return 'Greeted'


def respond():
    return 'Greet Responded Again'


default_args = {
    'owner': 'Raja',
    'start_date': dt.datetime(2018, 9, 24, 10, 00, 00),
    'concurrency': 1,
    'retries': 0
}

with DAG('my_simple_dag1_1',
         catchup=False,
         default_args=default_args,
         schedule_interval='*/10 * * * *',
         # schedule_interval=None,
         ) as dag:
    opr_hello = BashOperator(task_id='say_Hi1_1',
                             bash_command='echo "Hi!!"')

    opr_greet = PythonOperator(task_id='greet1_1',
                               python_callable=greet)
    opr_sleep = BashOperator(task_id='sleep_me1_1',
                             bash_command='sleep 5')

    opr_respond = PythonOperator(task_id='respond1_1',
                                 python_callable=respond)

opr_hello >> opr_greet >> opr_sleep >> opr_respond
