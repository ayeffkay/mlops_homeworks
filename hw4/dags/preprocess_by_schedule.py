import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.sftp.operators.sftp import SFTPOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.providers.ssh.operators.ssh import SSHOperator

default_args = {
    "owner": "airflow",
}
dag = DAG(
    "run_preproc_on_spark",
    default_args=default_args,
    start_date=datetime.datetime(2024, 2, 25, 7),
    schedule_interval="@daily",  # for debug: every three hours "0 */3 * * *"
    max_active_runs=3,
)

start_spark = BashOperator(
    task_id="start_spark_cluster",
    bash_command="/home/airflow/scripts/run_spark.sh ",
    dag=dag,
    append_env=True,
    cwd="/opt/airflow",
    do_xcom_push=True,
)


def sftp_transfer_callable(**kwargs):
    ti = kwargs["ti"]
    masternode_ip = str(
        ti.xcom_pull(task_ids="start_spark_cluster", key="return_value")
    )
    ssh_hook = SSHHook(
        remote_host=masternode_ip,
        username="ubuntu",
        key_file="/home/airflow/.ssh/id_rsa",
        port=22,
    )
    t = SFTPOperator(
        ssh_hook=ssh_hook,
        task_id="sftp_transfer",
        local_filepath="/home/airflow/scripts/preproc.py",
        remote_filepath="/home/ubuntu/preproc.py",
        operation="put",
        dag=dag,
        create_intermediate_dirs=True,
    )
    t.execute(dict())


def copy_s3cfg_callable(**kwargs):
    ti = kwargs["ti"]
    masternode_ip = str(
        ti.xcom_pull(task_ids="start_spark_cluster", key="return_value")
    )
    ssh_hook = SSHHook(
        remote_host=masternode_ip,
        username="ubuntu",
        key_file="/home/airflow/.ssh/id_rsa",
        port=22,
    )
    t = SFTPOperator(
        ssh_hook=ssh_hook,
        task_id="copy_s3cfg",
        local_filepath="/home/airflow/scripts/s3cfg",
        remote_filepath="/home/ubuntu/.s3cfg",
        operation="put",
        dag=dag,
        create_intermediate_dirs=True,
    )
    t.execute(dict())


def preprocess_callable(**kwargs):
    ti = kwargs["ti"]
    masternode_ip = str(
        ti.xcom_pull(task_ids="start_spark_cluster", key="return_value")
    )
    ssh_hook = SSHHook(
        remote_host=masternode_ip,
        username="ubuntu",
        key_file="/home/airflow/.ssh/id_rsa",
        port=22,
    )
    t = SSHOperator(
        ssh_hook=ssh_hook,
        task_id="ssh_remote_preprocess",
        command=f"""spark-submit preproc.py "s3a://otus-mlops-course/raw/" processed_data 
                    \"{datetime.datetime.now(datetime.timezone.utc).isoformat()}.parquet\"""",
        dag=dag,
        cmd_timeout=None,
    )
    t.execute(dict())


sftp_transfer = PythonOperator(
    task_id="sftp_transfer_wrapper", python_callable=sftp_transfer_callable
)

copy_s3cfg = PythonOperator(
    task_id="copy_s3cfg_wrapper", python_callable=copy_s3cfg_callable
)

preprocess = PythonOperator(
    task_id="ssh_remote_preprocess_wrapper", python_callable=preprocess_callable
)

remove_spark = BashOperator(
    task_id="remove_spark_cluster",
    bash_command="/home/airflow/scripts/remove_cluster.sh ",
    dag=dag,
)

start_spark >> sftp_transfer >> copy_s3cfg >> preprocess >> remove_spark
