from datetime import timedelta
import time
import sys

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator
from azure.keyvault.secrets import SecretClient
from azure.identity import ClientSecretCredential

secret_value_op = ''
default_args = {
    'owner': 'Myself',
    'depends_on_past': False,
    'start_date': days_ago(0),
    'catchup': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
}
#dagid = f'{{ dag_run.conf.uuid_val }}'
dag = DAG(
    'my_simple_dag2_2',
    #dag_id=dagid,
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(0)
)
resource1={"request_memory":"5Mi","request_cpu":"2m","limit_memory":"50Mi","limit_cpu":"10m"}

def pull_secret_value():#(ti,**context):
    """KVUri = f"https://airflow-key-vault.vault.azure.net"
    credential = ClientSecretCredential('cbf850c9-ee20-4a40-8e9d-4766fbb2a17a', '8d4d448d-4b4d-42e1-9bb9-41f90e8f1636', '.V7xf9UbEt.mf6~Er2mzbuRH6_BDtcMyv~')
    client = SecretClient(vault_url=KVUri, credential=credential)
    secretName="postgresDBpassword"
    retrieved_secret = client.get_secret(secretName)
    print(f"Your secret is '{retrieved_secret.value}'.")
    secret_value_op = retrieved_secret.value
    #l=[]
    return retrieved_secret.value"""
    return "abc"
    """uuid_val = context['dag_run'].conf['uuid']
    ti.xcom_push(key = 'uuid_key', value = uuid_val)"""

# Generate 4 tasks
#tasks = ["python_taks{}".format(i) for i in range(60, 120)]
example_dag_complete_node1 = DummyOperator(task_id="example_dag_complete", dag=dag)
python_pull_secret = PythonOperator(task_id="python_pull_secret", python_callable=pull_secret_value, dag=dag)

org_node = KubernetesPodOperator(
        namespace='kube-node-lease',
        image="airflowacrcontainer.azurecr.io/argspython", #memory",
        image_pull_secrets='acrsecret',
        cmds=["python","name.py"],
        #pool='pool2',
        arguments=["Pudota","Raja","Sekhar"],
        labels={"foo": "bar"},
        image_pull_policy="Always",
        resources=resource1,
        name="python_task",
        task_id= 'python_task', #+str(int(time.time())), #str(python_pull_secret.output),#{{do_xcom_pull(key = 'uuid_key')}}',#"python",
        is_delete_operator_pod=True,
        get_logs=True,
        dag=dag
    )

org_node.set_upstream(python_pull_secret)
org_node.set_downstream(example_dag_complete_node1)

"""org_dags = []
for python_task in tasks:
    #source_objects=["{{ task_instance.xcom_pull(task_ids='python_pull_secret') }}"]
    #print("source", source_objects)
    bash_command = 'echo HELLO'
    #task_instance = context['task_instance']
    #secret_value_op = ti.xcom_pull(key="secretname3")
    #secret_value_op =task_instance.xcom_pull(task_ids='python_pull_secret')
    org_node = KubernetesPodOperator(
        namespace='kube-node-lease',
        image="airflowacrcontainer.azurecr.io/argspython", #memory",
        image_pull_secrets='acrsecret',
        cmds=["python","name.py"],
        #pool='pool2',
        arguments=["Pudota","Raja","Sekhar"],
        labels={"foo": "bar"},
        image_pull_policy="Always",
        resources=resource1,
        name=python_task,
        task_id=python_task,
        is_delete_operator_pod=True,
        get_logs=True,
        dag=dag
    )
    
    #org_node.set_upstream(python_pull_secret)
    org_node.set_downstream(example_dag_complete_node1)"""

