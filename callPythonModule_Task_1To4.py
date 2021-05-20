from datetime import timedelta

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from azure.keyvault.secrets import SecretClient
from azure.identity import ClientSecretCredential

default_args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': days_ago(0),
    'catchup': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=8),
}
resource1={"request_memory":"1Mi","request_cpu":"1m","limit_memory":"1Mi","limit_cpu":"1m"}

dag = DAG(
    'callPythonModule_Task_1To4',
    default_args=default_args,
    schedule_interval=None
)

# Generate 4 tasks
tasks = ["task{}".format(i) for i in range(1, 5)]
example_dag_complete_node = DummyOperator(task_id="example_dag_complete", dag=dag)

org_dags = []
for python_task in tasks:

    bash_command = 'echo HELLO'

    org_node = KubernetesPodOperator(
        namespace='kube-node-lease',
        image="airflowacrdemocontainer.azurecr.io/argspython",
        image_pull_secrets='acrsecret',
        cmds=["python","name.py"],
        arguments=["Pudota","Raja","Sekhar"],
        labels={"foo": "bar"},
        image_pull_policy="Always",
        resources=resource1,
        name=python_task,
        task_id=python_task,
        is_delete_operator_pod=False,
        get_logs=True,
        dag=dag
    )

    org_node.set_downstream(example_dag_complete_node)
