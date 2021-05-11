from datetime import timedelta

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator
from azure.keyvault.secrets import SecretClient
from azure.identity import ClientSecretCredential

secret_value_op = ''
default_args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': days_ago(0),
    'catchup': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'callPythonModule_Task_5To8',
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(0)
)

def pull_secret_value():
    KVUri = f"https://airflow-keyvault-3.vault.azure.net"
    credential = ClientSecretCredential('d3c91205-02f7-4bba-bd33-0fde50b3a8b4', '0a34166b-75ea-45c5-89a1-b2f62b9ca602', 'e~scpuZ5R-65ueaItpAReX0T-6~kV-j~HU')
    client = SecretClient(vault_url=KVUri, credential=credential)
    secretName="secretname3"
    retrieved_secret = client.get_secret(secretName)
    print(f"Your secret is '{retrieved_secret.value}'.")
    secret_value_op = retrieved_secret.value
    #return retrieved_secret.value
    return secret_value_op

# Generate 4 tasks
tasks = ["task{}".format(i) for i in range(50, 55)]
example_dag_complete_node = DummyOperator(task_id="example_dag_complete", dag=dag)
python_pull_secret = PythonOperator(task_id="python_pull_secret", python_callable=pull_secret_value)


org_dags = []
for python_task in tasks:

    bash_command = 'echo HELLO'
    #task_instance = context['task_instance']
    #secret_value_op =task_instance.xcom_pull(task_ids='python_pull_secret')
    org_node = KubernetesPodOperator(
        namespace='kube-public',
        image="testcontainerkubernetraja.azurecr.io/argspython",
        image_pull_secrets='testcontainerkubernetraja',
        cmds=[secret_value_op,"name.py"],
        arguments=["Pudota","Raja","Sekhar"],
        labels={"foo": "bar"},
        image_pull_policy="Always",
        name=python_task,
        task_id=python_task,
        is_delete_operator_pod=True,
        get_logs=True,
        dag=dag
    )
    
    org_node.set_upstream(python_pull_secret)
    org_node.set_downstream(example_dag_complete_node)
