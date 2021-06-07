from datetime import timedelta
from kubernetes.client import models as k8s

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from kubernetes.client.models.v1_env_var import V1EnvVar


default_args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': days_ago(0),
    'catchup': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
}
resource1={"request_memory":"5Mi","request_cpu":"2m","limit_memory":"50Mi","limit_cpu":"10m"}
#uuid2 = dag_run.conf["uuid"]
#temp_uuid= f'{ dag_run.conf["uuid"] }'
#uuid = "raja +" + str(temp_uuid)
dag = DAG(
    'callPythonModule_Task_1To4',
    default_args=default_args,
    schedule_interval=None
)

start_task = DummyOperator(task_id="start", dag=dag)
def create_kpo_task(current_uuid):
    return KubernetesPodOperator(
        namespace='kube-node-lease',
        image="airflowacrcontainer.azurecr.io/argspython",
        image_pull_secrets='acrsecret',
        cmds=["python","name.py"],
        arguments=["Pudota","Raja","Sekhar"],
        tolerations=[k8s.V1Toleration(
            key='key1',
            operator='Equal',
            value='value1',
            effect='NoSchedule')],
        labels={"foo": "bar"},
        image_pull_policy="Always",
        resources=resource1,
        name="python_task_name",
        task_id=eval({current_uuid}), #"kb_task", #"checktask",#'{{ dag_run.conf.uuid }}',
        is_delete_operator_pod=False,
        get_logs=True,
        dag=dag
    )
stop_task = DummyOperator(task_id="stop", dag=dag)
uuid_inp = f'{{ dag_run.conf.uuid }}'
created_task = create_kpo_task(str(uuid_inp))
start_task >> created_task >> stop_task
#org_node.set_upstream(start_task)
#org_node.set_downstream(stop_task)

# Generate 4 tasks
"""tasks = ["py_task{}".format(i) for i in range(1, 5)]
org_dags = []
for python_task in tasks:

    bash_command = 'echo HELLO'

    org_node = KubernetesPodOperator(
        namespace='kube-node-lease',
        image="airflowacrcontainer.azurecr.io/argspython",
        image_pull_secrets='acrsecret',
        cmds=["python","name.py"],
        arguments=["Pudota","Raja","Sekhar"],
        tolerations=[k8s.V1Toleration(
            key='key1',
            operator='Equal',
            value='value1',
            effect='NoSchedule')],
        labels={"foo": "bar"},
        image_pull_policy="Always",
        resources=resource1,
        name=python_task,
        task_id=python_task,
        is_delete_operator_pod=False,
        get_logs=True,
        dag=dag
    )

    org_node.set_downstream(example_dag_complete_node)"""

