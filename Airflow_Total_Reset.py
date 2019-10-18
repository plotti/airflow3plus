import time
import shutil
import logging
import os
import pin_func_exp

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from datetime import datetime, timedelta


DAG_ID = 'dag_reset'
SLACK_CONN_ID = 'slack'


# Alerts if the DAG fails to execute
def fail_slack_alert(context):

    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    slack_msg = """
            :red_circle: Task Failed. 
            *Task*: {task}  
            *Dag*: {dag} 
            *Execution Time*: {exec_date}  
            *Log Url*: {log_url} 
            """.format(
            task=context.get('task_instance').task_id,
            dag=context.get('task_instance').dag_id,
            ti=context.get('task_instance'),
            exec_date=context.get('execution_date'),
            log_url=context.get('task_instance').log_url,
        )

    failed_alert = SlackWebhookOperator(
        task_id='Slack_failure_alert',
        http_conn_id='slack',
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username='AirflowAlert')

    return failed_alert.execute(context=context)


# Implement a DAG in case a total reset is required
default_args = {
    'owner': '3plus',
    'depends_on_past': False,
    'email': ['floosli@3plus.tv'],
    'email_on_failure': False,
    'email_on_retry': False,
    'on_failure_callback': fail_slack_alert
    }


# DAG Definition and various setting influencing the workflow of the DAG
dag_reset = DAG(dag_id=DAG_ID,
                description='DAG used in case all the data has to be recomputed',
                schedule_interval=None,
                start_date=datetime(year=2000, month=1, day=1),
                end_date=None,
                default_args=default_args,
                concurrency=2,
                max_active_runs=4,
                dagrun_timeout=timedelta(hours=4),
                catchup=False)


# Add some time to reconsider your decision
def wait_some_time():

    logging.info('are you sure to recompute everything, decide in less than 1 minute')
    time.sleep(60)


# Delete all the PIN_Data
def delete_pin_data():

    logging.info('Delete everything')
    dir_path = '/home/floosli/Documents/PIN_Data'
    shutil.rmtree(dir_path)
    os.mkdir(dir_path)

# TODO paths are wrong
# Create a new live facts table
def create_live_facts_table():

    logging.info('Creating live facts table')

    cond = False
    path = '/home/floosli/Documents/PIN_Data/'

    year = datetime.now().year
    month = datetime.now().month
    day = datetime.now().day

    end = str(year) + str(month) + str(day)
    start = str(year) + '0101'

    date = datetime.strptime(start, '%Y%m%d')

    # Does not respect leap-years
    for i in range(364):
        pot_date = (date + timedelta(days=i)).strftime('%Y%m%d')
        if os.path.isfile(path + '%s_%s_Live_DE_15_49_mG.csv' % (start, pot_date)):
            date = datetime.strptime(pot_date, '%Y%m%d')
            break

    while True:

        dates = set()

        for i in range(7):

            add = date.strftime('%Y%m%d')
            if add == end:
                cond = True
                break
            dates.update([int(add)])
            date = date + timedelta(days=1)
        pin_func_exp.update_live_facts_table(dates)

        if cond:
            break


# Create a new tsv facts table
def create_tsv_facts_table():

    logging.info('Creating tsv facts table')

    cond = False
    path = '/home/floosli/Documents/PIN_Data/'

    year = datetime.now().year
    month = datetime.now().month
    day = datetime.now().day

    end = str(year) + str(month) + str(day)
    start = str(year) + '0101'

    date = datetime.strptime(start, '%Y%m%d')

    for i in range(364):
        pot_date = (date + timedelta(days=i)).strftime('%Y%m%d')
        if os.path.isfile(path + '%s_%s_delayedviewing_DE_15_49_mG.csv' % (start, pot_date)):
            date = datetime.strptime(pot_date, '%Y%m%d')
            break

    while True:

        dates = set()

        for i in range(7):

            add = date.strftime('%Y%m%d')
            if add == end:
                cond = True
                break
            dates.update([int(add)])
            date = date + timedelta(days=1)

        pin_func_exp.update_tsv_facts_table(dates)

        if cond:
            break


# Bash command downloads all the files like doing it manually
# wget -nc 'ftp://ftp.mpg-ftp.ch/PIN-Daten/BrdCst_*.pin' --ftp-user=3plus@mpg-ftp.ch --ftp-password=cJGQNd0d
bash_download = "cd /home/floosli/Documents/PIN_Data &&" \
                "wget -nc 'ftp://ftp.mpg-ftp.ch/PIN-Daten/*.pin' --ftp-user=3plus@mpg-ftp.ch --ftp-password=cJGQNd0d"


Task_sleep = PythonOperator(
    task_id='sleep',
    python_callable=wait_some_time,
    retries=0,
    dag=dag_reset
)

Task_delete_all = PythonOperator(
    task_id='delete_all',
    python_callable=delete_pin_data,
    retries=5,
    retry_delay=timedelta(seconds=5),
    execution_timeout=timedelta(minutes=2),
    dag=dag_reset
)


# Execute bash command to download the files from the external ftp-server
Task_Bash_Download = BashOperator(
    task_id='download_everything',
    bash_command=bash_download,
    retries=20,
    retry_delay=timedelta(seconds=5),
    execution_timeout=timedelta(minutes=10),
    dag=dag_reset
)


Task_create_live_facts_table = PythonOperator(
    task_id='create_live_facts_table',
    python_callable=create_live_facts_table,
    retries=10,
    retry_delay=timedelta(seconds=5),
    execution_timeout=timedelta(days=1),
    dag=dag_reset
)


Task_create_tsv_facts_table = PythonOperator(
    task_id='create_tsv_facts_table',
    python_callable=create_tsv_facts_table,
    retries=10,
    retry_delay=timedelta(seconds=5),
    execution_timeout=timedelta(days=1),
    dag=dag_reset
)

# Schedule of the hard reset DAG
Task_sleep >> Task_delete_all >> Task_Bash_Download >> [Task_create_live_facts_table, Task_create_tsv_facts_table]
