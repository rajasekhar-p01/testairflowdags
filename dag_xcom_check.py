from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.dates import days_ago


def check_year(ti,**context):
    number = context['dag_run'].conf['year']
    if number%400 == 0 or (number%4 == 0 and number%100 != 0):
        val = True
    else:
        val = False
    ti.xcom_push(key = 'check', value = val)

def checker(ti):
    c = ti.xcom_pull(key = 'check')

    if c:
        return 'Leap'
    return 'Not_Leap'

def leap(**context):
    print(str(context['dag_run'].conf['year'])+" is a leap year!!!")

def not_leap(**context):
    print(str(context['dag_run'].conf['year'])+" is not a leap year!!!")

with DAG(
dag_id = 'dag_xcom_check',
schedule_interval = None,
start_date = days_ago(2)
) as dag:

    Leap_or_Not = PythonOperator(
            task_id = 'Leap_or_Not',
            python_callable = check_year
            )
    Checker = BranchPythonOperator(
            task_id = 'Checker',
            python_callable = checker,
            do_xcom_push = False
            )
    Leap = PythonOperator(
            task_id = 'Leap',
            python_callable = leap
            )
    Not_Leap = PythonOperator(
            task_id = 'Not_Leap',
            python_callable = not_leap
            )
    Leap_or_Not>>Checker>>[Leap,Not_Leap]
