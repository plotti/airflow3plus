import time
import shutil
import logging
import os
import pin_func_exp
import calendar
import Airflow_variables

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from datetime import datetime, timedelta

"""
DAG to run in case of total failure and requirement of the recomputing the whole facts table
Deletes in a first step all the data of the previous iteration then downloads all the files and 
consequently computes the tables in the end. The reset only computes the facts table of the current year for 
a computation of additional year you have to change the year variable in the create_live_facts_table and the respective 
tsv function. Ensure correct file paths.
The reset DAG is structured in 3 blocks, annotated by a graphical lines grouping the tasks. First step is deleting the
old data and facts tables. Second step is download of the current PIN data and the last step is computing the 
facts tables.
"""
Airflow_var = Airflow_variables.AirflowVariables()
DAG_ID = 'dag_reset'
SLACK_CONN_ID = Airflow_var.slack_conn_id
LOCAL_PATH = Airflow_var.local_path
SUFFIX = Airflow_var.suffix


def fail_slack_alert(context):
    """
    Sends a message to the slack channel airflowalerts if a task in the pipeline fails
    :param context: context
    :return: fail_alert execution
    """
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
                description='DAG used in case that all the facts table has to be recomputed',
                schedule_interval=None,
                start_date=datetime(year=2000, month=1, day=1),
                end_date=None,
                default_args=default_args,
                concurrency=2,
                max_active_runs=4,
                dagrun_timeout=timedelta(hours=4),
                catchup=False)


def wait_some_time():
    """
    Wait 1 min before continuing with the execution, to give the possibility to cancel the process
    :return: None
    """
    logging.info('are you sure to recompute everything, cancel if required in less than 1 minute')
    for i in range(7):
        time.sleep(10)
        logging.info('You have %i seconds left to decide' % (60-i*10))


def delete_pin_data():
    """
    Delete all the locally saved PIN Data and the facts table,
    Rename the facts tables if you want to keep them
    :return: None
    """
    logging.info('Delete everything')

    regular_file_list = ['BrdCst', 'SocDem', 'UsageLive', 'UsageTimeShifted', 'Weight']
    irregular_file_list = ['Station', 'CritCode', 'Crit']

    for name in regular_file_list:
        dir_path = LOCAL_PATH + name
        shutil.rmtree(dir_path)
        os.mkdir(dir_path)

    for name in irregular_file_list:
        file_path = LOCAL_PATH + name + SUFFIX
        if os.path.isfile(file_path):
            os.remove(file_path)

    year = datetime.now().year
    start = str(year) + '0101'
    date = datetime.strptime(start, '%Y%m%d')
    r_date = date
    ir_date = date

    leap = 0
    if calendar.isleap(year):
        leap = 1

    for i in range(364 + leap):
        r_date = (r_date + timedelta(days=i))
        str_date = r_date.strftime('%Y%m%d')
        if os.path.isfile(LOCAL_PATH + '%s_%s_Live_DE_15_49_mG.csv' % (start, str_date)):
            os.remove(LOCAL_PATH + '%s_%s_Live_DE_15_49_mG.csv' % (start, str_date))
            break

    for i in range(364 + leap):
        ir_date = (ir_date + timedelta(days=i))
        istr_date = ir_date.strftime('%Y%m%d')
        if os.path.isfile(LOCAL_PATH + '%s_%s_delayedviewing_DE_15_49_mG.csv' % (start, istr_date)):
            os.remove(LOCAL_PATH + '%s_%s_delayedviewing_DE_15_49_mG.csv' % (start, istr_date))
            break


def create_live_facts_table():
    """
    Create a new live facts table up to the present day
    To change the year of computation adjust the year variable to your desired one
    by default it will be the current year
    :return: None
    """
    logging.info('Creating live facts table')

    cond = False
    year = datetime.now().year
    month = datetime.now().month
    day = datetime.now().day

    end = str(year) + str(month) + str(day)
    start = str(year) + '0101'

    date = datetime.strptime(start, '%Y%m%d')

    leap = 0
    if calendar.isleap(year):
        leap = 1

    # Detection of checkpoint
    for i in range(364+leap):
        pot_date = (date + timedelta(days=i)).strftime('%Y%m%d')
        if os.path.isfile(LOCAL_PATH + '%s_%s_Live_DE_15_49_mG.csv' % (start, pot_date)):
            date = datetime.strptime(pot_date, '%Y%m%d')
            break

    logging.info('Will update from date %s' % date)
    while True:
        logging.info('Updating still ongoing')
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
            logging.info("Reached the end date successfully, finished live table")
            break


def create_tsv_facts_table():
    """
    Create a new live facts table up to the present day
    To change the year of computation adjust the year variable to your desired one
    by default it will be the current year
    :return: None
    """
    logging.info('Creating tsv facts table')

    cond = False

    year = datetime.now().year
    month = datetime.now().month
    day = datetime.now().day

    end = str(year) + str(month) + str(day)
    start = str(year) + '0101'

    date = datetime.strptime(start, '%Y%m%d')

    leap = 0
    if calendar.isleap(year):
        leap = 1

    for i in range(364+leap):
        pot_date = (date + timedelta(days=i)).strftime('%Y%m%d')
        if os.path.isfile(LOCAL_PATH + '%s_%s_delayedviewing_DE_15_49_mG.csv' % (start, pot_date)):
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


# Bash command downloads all the files like doing it manually with command line inputs
bash_weight_download = "cd /home/floosli/Documents/PIN_Data/Weight &&" \
    "wget -nc 'ftp://ftp.mpg-ftp.ch/PIN-Daten/Weight_*.pin' --ftp-user=3plus@mpg-ftp.ch --ftp-password=cJGQNd0d"

bash_socdem_download = "cd /home/floosli/Documents/PIN_Data/SocDem &&" \
    "wget -nc 'ftp://ftp.mpg-ftp.ch/PIN-Daten/SocDem_*.pin' --ftp-user=3plus@mpg-ftp.ch --ftp-password=cJGQNd0d"

bash_brdcst_download = "cd /home/floosli/Documents/PIN_Data/BrdCst &&" \
    "wget -nc 'ftp://ftp.mpg-ftp.ch/PIN-Daten/BrdCst_*.pin' --ftp-user=3plus@mpg-ftp.ch --ftp-password=cJGQNd0d"

bash_usagelive_download = "cd /home/floosli/Documents/PIN_Data/UsageLive &&" \
    "wget -nc 'ftp://ftp.mpg-ftp.ch/PIN-Daten/UsageLive_*.pin' --ftp-user=3plus@mpg-ftp.ch --ftp-password=cJGQNd0d"

bash_usagetimeshifted_download = "cd /home/floosli/Documents/PIN_Data/UsageTimeShifted &&" \
    "wget -nc 'ftp://ftp.mpg-ftp.ch/PIN-Daten/UsageTimeShifted_*.pin' --ftp-user=3plus@mpg-ftp.ch --ftp-password=cJGQNd0d"

bash_irregular_download = "cd /home/floosli/Documents/PIN_Data &&" \
    "wget -nc 'ftp://ftp.mpg-ftp.ch/PIN-Daten/CritCode.pin' --ftp-user=3plus@mpg-ftp.ch --ftp-password=cJGQNd0d &&" \
    "wget -nc 'ftp://ftp.mpg-ftp.ch/PIN-Daten/Crit.pin' --ftp-user=3plus@mpg-ftp.ch --ftp-password=cJGQNd0d &&" \
    "wget -nc 'ftp://ftp.mpg-ftp.ch/PIN-Daten/Station.pin' --ftp-user=3plus@mpg-ftp.ch --ftp-password=cJGQNd0d"


# -------------------------------------------------------------------------------------------------------------------
# 1. Step: Give some time to reconsider your decision and delete eventually all the PIN Data
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


# --------------------------------------------------------------------------------------------------------------------
# 2. Step: Download each type of file one after another according to the bash commands from above
Task_Weight_Download = BashOperator(
    task_id='download_weight',
    bash_command=bash_weight_download,
    retries=20,
    retry_delay=timedelta(seconds=5),
    execution_timeout=timedelta(minutes=30),
    priority_weight=2,
    dag=dag_reset
)
Task_BrdCst_Download = BashOperator(
    task_id='download_brdcst',
    bash_command=bash_brdcst_download,
    retries=20,
    retry_delay=timedelta(seconds=5),
    execution_timeout=timedelta(minutes=30),
    priority_weight=2,
    dag=dag_reset
)
Task_UsageLive_Download = BashOperator(
    task_id='download_usagelive',
    bash_command=bash_usagelive_download,
    retries=20,
    retry_delay=timedelta(seconds=5),
    execution_timeout=timedelta(minutes=30),
    priority_weight=2,
    dag=dag_reset
)
Task_UsageTimeShifted_Download = BashOperator(
    task_id='download_usagetimeshifted',
    bash_command=bash_usagetimeshifted_download,
    retries=20,
    retry_delay=timedelta(seconds=5),
    execution_timeout=timedelta(minutes=30),
    priority_weight=2,
    dag=dag_reset
)
Task_SocDem_Download = BashOperator(
    task_id='download_socdem',
    bash_command=bash_socdem_download,
    retries=20,
    retry_delay=timedelta(seconds=5),
    execution_timeout=timedelta(minutes=30),
    priority_weight=2,
    dag=dag_reset
)
Task_Irregular_Download = BashOperator(
    task_id='download_irregular',
    bash_command=bash_irregular_download,
    retries=20,
    retry_delay=timedelta(seconds=5),
    execution_timeout=timedelta(minutes=10),
    priority_weight=1,
    dag=dag_reset
)


# --------------------------------------------------------------------------------------------------------------------
# 3. Step
# Compute the facts tables
Task_create_live_facts_table = PythonOperator(
    task_id='create_live_facts_table',
    python_callable=create_live_facts_table,
    retries=20,
    retry_delay=timedelta(seconds=5),
    execution_timeout=timedelta(days=1),
    priority_weight=1,
    trigger_rule='all_success',
    dag=dag_reset
)
Task_create_tsv_facts_table = PythonOperator(
    task_id='create_tsv_facts_table',
    python_callable=create_tsv_facts_table,
    retries=20,
    retry_delay=timedelta(seconds=5),
    execution_timeout=timedelta(days=1),
    priority_weight=1,
    trigger_rule='all_success',
    dag=dag_reset
)


# Schedule of the hard reset DAG
Task_sleep >> Task_delete_all >> [Task_Weight_Download, Task_BrdCst_Download, Task_SocDem_Download,
                                  Task_UsageLive_Download, Task_UsageTimeShifted_Download, Task_Irregular_Download] >> \
Task_create_live_facts_table >> Task_create_tsv_facts_table
