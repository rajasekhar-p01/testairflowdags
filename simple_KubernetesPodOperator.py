from datetime import timedelta

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': days_ago(0),
    'catchup': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'simple_KubernetesPodOperator',
    default_args=default_args,
    schedule_interval=None,
    max_active_runs=1,
    concurrency=2
)

# Generate 4 tasks
example_dag_start_node = DummyOperator(task_id="example_dag_start", dag=dag)
tasks = ["task{}".format(i) for i in range(1, 5)]
example_dag_complete_node = DummyOperator(task_id="example_dag_complete", dag=dag)

org_dags = []
for task in tasks:

    #bash_command = 'echo HELLO'

    org_node = KubernetesPodOperator(
        namespace='kube-node-lease',
        image="demoairflowcontainer.azurecr.io/argspython:latest",
        image_pull_secrets='acrsecret',
        cmds=["python","argspython.py"],
        arguments=["Raja","Sekhar","Pudota"],
        image_pull_policy="Always",
        name=task,
        #resources={'request_memory':'5Gi','request_cpu':'1000m','limit_memory':'6Gi','limit_cpu':'1200m'},
        task_id=task,
        is_delete_operator_pod=True,
        get_logs=True,
        dag=dag
    )
    org_node.set_upstream(example_dag_start_node)
    org_node.set_downstream(example_dag_complete_node)
