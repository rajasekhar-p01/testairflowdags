from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup

def method1(**context):
    n = context['dag_run'].conf['number']
    if n%2 == 0:
        return True
    return False

def method2(ti):
    if ti.xcom_pull(task_ids = 'task_1') == True:
        return 'group1.task_x'
    return 'group2.task_x'


def printMethod(n):
    print('This is even ::'+str(n))


def methodPrint(n):
    print('This is odd ::'+str(n))


def the_end():
    print('The End')


with DAG(
        dag_id = 'TaskGroup_BranchPythonOperator',
        schedule_interval = None,
        start_date = days_ago(2)
        ) as dag:
    task_1 = PythonOperator(
            task_id = 'task_1',
            python_callable = method1
            )
    task_2 = BranchPythonOperator(
            task_id = 'task_2',
            python_callable = method2
            )
    with TaskGroup('group1') as group1:

        task_x = PythonOperator(
                task_id = 'task_x',
                python_callable = printMethod,
                op_kwargs = {'n' : 1}
                )
        task_n = [
                PythonOperator(
                    task_id = f'task_{i}',
                    python_callable = printMethod,
                    op_kwargs = {'n' : i}
                    ) for i in range(2,6)
                ]
        task_x>>task_n

    with TaskGroup('group2') as group2:

        task_x = PythonOperator(
                task_id = 'task_x',
                python_callable = methodPrint,
                op_kwargs = {'n' : 1}
                )
        task_n = [
                PythonOperator(
                    task_id = f'task_{i}',
                    python_callable = methodPrint,
                    op_kwargs = {'n' : i}
                    ) for i in range(2,6)
                ]
        task_x>>task_n
    task_3 = PythonOperator(
            task_id = 'task_3',
            python_callable = the_end,
            trigger_rule = 'one_success'
            )

    task_1>>task_2>>[group1,group2]>>task_3
