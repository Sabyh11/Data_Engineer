from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable

import os
from datetime import datetime, timedelta

# These variables live with every hearbeat to ensure all tasks within DAGs share same value
dag_id = "nifi_git_rps"

# Setup the storing file path
nifi_dir = "/home/team/data/nifi/nifi-1.9.2/{config,jdbc,lib}"
nifi_backup_dir = "/home/team/data/nifi/nifi_backup"


# Path definition for GIT
git_dir = "/home/team/data/nifi/nifi_backup_gitrps"

# Prepare bash command
remove_files_cmd = f"rm -rf {nifi_backup_dir}/nifi"
sync_files_cmd = f"rsync -arRv {nifi_dir} {nifi_backup_dir}"
push_files_cmd = f"git -C {nifi_backup_dir} --git-dir={git_dir}/.git add . && git --git-dir={git_dir}/.git commit -m 'airflow commit' && git --git-dir={git_dir}/.git push"

default_args = {
    "owner": "jane_pham",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": True,
    "email_on_retry": False,
    "depends_on_past": False,
    "sla": timedelta(hours=1)
}

dag = DAG(
    dag_id=dag_id,
    default_args=default_args,
    schedule_interval="0 23 * * *",
    start_date=datetime(2020, 4, 14),
    catchup=False,
    tags=['full', 'small']
)


with dag:
    
    init_task = BashOperator(
        task_id="init",
        bash_command=remove_files_cmd
    )

    sync_intermediate = BashOperator(
        task_id="sync",
        bash_command=sync_files_cmd
    )

    pushtogit_task = BashOperator(
        task_id="gitpush",
        bash_command=push_files_cmd
    )

    init_task >> sync_intermediate >> pushtogit_task
    