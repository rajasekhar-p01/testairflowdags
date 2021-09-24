from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
default_args = { "owner": "Tomi Tyrrell",
                 "retries": 3,
                 "start_date": "2021-09-24",
                 "retries_delay": 1
                }
                
def hello(name):
    print(f"Hi {name} I'm writing a Python function!")
                
with DAG(dag_id="my_dag", default_args=default_args, schedule_interval=None) as dag:
    task1 = BashOperator(task_id="head", bash_command='echo "Hi"')
    task2 = PythonOperator(task_id ="python_task", python_callable=hello, op_kwargs={"name":"Raja"})
    task3 = BashOperator(task_id="tail", bash_command='echo "Bye"')
    
task1 >> task2 >> task3
