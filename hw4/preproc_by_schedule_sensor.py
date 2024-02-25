import os
import datetime
from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.models.dagrun import DagRun
from airflow.operators.bash import BashOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.providers.sftp.operators.sftp import SFTPOperator
from airflow.providers.ssh.operators.ssh import SSHOperator

def get_most_recent_dag_run(dt):
    dag_runs = DagRun.find(dag_id="run_spark")
    dag_runs.sort(key=lambda x: x.execution_date, reverse=True)
    if dag_runs:
        return dag_runs[0].execution_date


default_args = {
    "owner": "airflow",
}
dag = DAG("run_preproc_on_spark", default_args=default_args, schedule_interval=None)


wait_for_cluster = ExternalTaskSensor(
    task_id="wait_for_cluster",
    external_dag_id="run_spark",
    external_task_id="start_spark_cluster",
    execution_date_fn=get_most_recent_dag_run,
    mode="reschedule"
)

ssh_hook = SSHHook(
    remote_host=os.getenv("MASTERNODE_IP", "0.0.0.0"),
    username="ubuntu",
    key_file="/home/airflow/.ssh/id_rsa",
    port=22,
)

sftp_transfer = SFTPOperator(
    ssh_hook=ssh_hook,
    remote_host=os.getenv("MASTERNODE_IP", "0.0.0.0"),
    task_id="sftp_transfer",
    local_filepath="~/scripts/preproc.py",
    remote_filepath="/home/ubuntu/preproc.py",
    operation="put",
    dag=dag,
    create_intermediate_dirs=True,
)

run_preprocess = SSHOperator(
    ssh_hook=ssh_hook,
    remote_host=os.getenv("MASTERNODE_IP", "0.0.0.0"),
    task_id="ssh_remote_preprocess",
    command="""spark-submit preproc.py "s3a://otus-mlops-course/raw/" processed \"processed_data.parquet\"""",
    dag=dag,
    cmd_timeout=None,
)

remove_spark = BashOperator(
    task_id="remove_spark_cluster",
    bash_command="~/scripts/remove_cluster.sh ",
    dag=dag,
)

(
    wait_for_cluster
    >> sftp_transfer
    >> run_preprocess
    >> remove_spark
)
