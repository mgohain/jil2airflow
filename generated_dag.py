from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.timetables.base import Timetable
from airflow.utils.email import send_email
from airflow.utils.task_group import TaskGroup
from airflow.utils.template import literal
from airflow.utils.trigger_rule import TriggerRule
from custom_calendars import *
from custom_operators.profile_aware_sftp_sensor import ProfileAwareSftpSensor
from datetime import datetime, timedelta
import pendulum

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2024, 1, 1, tz="US/Eastern"),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'monthly_cal_test',
    default_args=default_args,
    description='Converted from Autosys JIL',
    schedule=CDW_EC_TENTH_DAY_OF_MONTH(2, 40, 'US/Eastern'),
    catchup=False,
    tags=['autosys-conversion'],
)

def notify_failure(context):
    ti = context['task_instance']
    dag_id = ti.dag_id
    task_id = ti.task_id
    state = ti.state
    log_url = ti.log_url
    exception = context.get("exception")
    recipients = context['params'].get("emails")
    subject = f"{state} | Task {task_id} in DAG {dag_id} is :: {state}"
    body = f""" 
    <h3>DAG: {dag_id}</h3>
    <p>Task: {task_id}</p>
    <p>Execution Date: {context['logical_date']}</p>
    <p>exception :: {exception}</p>
    <p>Log: <a href="{log_url}">View Logs</a></p>
    """
    send_email(to=recipients, subject=subject, html_content=body)


with TaskGroup(
    group_id='CDW_EC_MONTHLY_UPDATE',
    tooltip='"Box to hold file watcher and command job"',
    dag=dag,
) as CDW_EC_MONTHLY_UPDATE:

    file_watch_job_remote = ProfileAwareSftpSensor(
        task_id='file_watch_job_remote',
        path='${HOMEDIR_MRIDUL}/jil2airflow/control.c',
        sftp_conn_id='ubuntu-22',
        poke_interval=5,
        timeout=300,
        profile_file='/home/mridul/.mridul_profile',
        params={'emails': ['mridul.gohain@triedatum.com']},
        on_failure_callback=notify_failure,
        dag=dag,
    )


    process_file = SSHOperator(
        task_id='process_file',
        ssh_conn_id='ubuntu-22',
        command=literal("bash -c '. /home/mridul/.mridul_profile && ${HOMEDIR_MRIDUL}/jil2airflow/process_file.sh ${HOMEDIR_MRIDUL}/jil2airflow/myfile.c > ${HOMEDIR_MRIDUL}/logs/process_file.log 2> ${HOMEDIR_MRIDUL}/logs/process_file.err'"),
        environment={},
        retries=2,
        retry_delay=timedelta(minutes=1),
        dag=dag,
    )


    file_watch_job_remote >> process_file

