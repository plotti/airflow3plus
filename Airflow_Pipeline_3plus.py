import logging
import json
import os
import hashlib
import shutil

import Sensors_3plus
import Pin_Functions
import Airflow_Variables

from datetime import datetime, timedelta, timezone
from shutil import copyfile

from airflow import DAG
from airflow.models import xcom
from airflow.operators.sqlite_operator import SqliteOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.ftp_hook import FTPHook
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
"""
Used airflow version for development 1.10.5
A scheduling system based on the Airflow library. We'd like to update locally saved files daily and
generate enriched reports and tables based on the new entries.
The 3plus_dag is the defined DAG in which all the task are unraveled and scheduled.
Most Python_functions used in the PythonOperators are defined in the accompanied file Pin_Functions.py
for clearer maintainability add additional functions in the previously mentioned file or create new ones.

Install airflow:
1. pip3 install apache-airflow[*], at * you can add additional packages if required, if no packages leave the brackets 
out of the command. 22.10.19 additional packages: crypto

2. airflow initdb to initiate the airflow database, execute this command at the directory where you want to setup 
your pipeline. Default Db is a SQLite for different one have a look at the documentation and the 
airflow database backend

*Default Executor of our pipeline is the Serialexecutor for other options have a look at the airflow documentation.

How to start the airflow process:
1. Start the airflow scheduler with the command: airflow scheduler
optional*. Start the airflow webserver to observe the progress of the DAG with: airflow webserver

All the configurations can be made through the command line and the GUI is not required but is very useful for
the application. All commands can be found in the airflow documentation

2. Start the DAG which should be processed with the command: airflow un-pause dag_id
To trigger a run outside of schedule: airflow trigger_dag dag_id

The handling of the webserver should be self explanatory, some exploratory actions might be useful
to get used to the GUI. Connections can be defined and modified and other useful operations

Access credentials to the remote ftp-server from mediapuls
wget -nc ftp://ftp.mpg-ftp.ch/PIN-Daten/*.pin' --ftp-user=3plus@mpg-ftp.ch --ftp-password=cJGQNd0d
host = ftp.mpg-ftp.ch,
user = 3plus@mpg-ftp.ch,
password = cJGQNd0d

Many variables for path, ranges etc can be found in the Airflow_variables class.

The slack alert app is managed over the official slack API, settings and additional apps can be added there
pls have a look at this blogpost to recreate the implementation 
https://medium.com/datareply/integrating-slack-alerts-in-airflow-c9dcd155105 version 2
Current Password slack: /T1JBVG25S/BNWRSJB99/z4Jua0GiOFfJPo303pSQQB76

The FTP-connection is secured with a fernet-key, according to the implementation from airflow[crypt] package
Current Fernet-Key for the ftp connection: yP-Y5bM5fRJoyuBPIP31cRZng4Ktk4hV3vNtBuzkSl4=

Currently the xcom variables are stored on a hosted sql_alchemy db. Have a look at 
the airflow.cfg file for configuration of the database.

The Schedule at the bottom of the file shows how the task are executed after each other, it is not allowed to contain 
any circles. If you have task defined but not in the schedule they will still be executed as a single instance so
you have to comment them out or delete them if this behaviour is not desired
"""
DAG_ID = 'dag_3plus'
Airflow_var = Airflow_Variables.AirflowVariables()
# Global variables used in the DAG
REMOTE_PATH = Airflow_var.remote_path
LOCAL_PATH = Airflow_var.local_path
SUFFIX = Airflow_var.suffix
# Sequence of the file is important for the execution of the algorithm
REGULAR_FILE_LIST = Airflow_var.regular_file_list
IRREGULAR_FILE_LIST = Airflow_var.irregular_file_list
SENSOR_IN_PAST = Airflow_var.sensor_in_past
DAYS_IN_YEAR = Airflow_var.days_in_year
# Connections to the used servers, configurations can be found in the airflow webserver GUI or over the commandline
FTP_CONN_ID = Airflow_var.ftp_conn_id
SQL_ALCHEMY_CONN = Airflow_var.sql_alchemy_conn
INFOSYS_FTP_CONN_ID = Airflow_var.infosys_ftp_conn_id
# Slack connection ID
SLACK_CONN_ID = Airflow_var.slack_conn_id
# Time properties
START = Airflow_var.start
END_DAY = Airflow_var.end
"""
Continue development of this system.
Structure of this DAG:
Each step is marked with a short explanation of the function in the section and is graphically separated
1. Step: Sensor new files on the FTP server
2. Step: Download new files from the FTP server
3. Step: Compare hashes of new and old files and update facts table according to the new files
4. Step: Clean up, empty temp folder and delete all xcom variables

To add Task:
Read some Airflow examples.
1. Define a task of what you need(What kind of Operator or Sensor you need -> respective Documentation)
2. Add it to the schedule at the position you lie
3. Load it into the airflow directory
"""


# ----------------------------------------------------------------------------------------------------------------------
def fail_slack_alert(context):
    """
    Sends a message to the slack channel airflowalerts if a task in the pipeline fails
    :param context: context
    :return: fail_alert execution
    """
    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    slack_msg = """
            :clown_face: Task Failed. 
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
        username='AirflowAlert'
    )

    return failed_alert.execute(context=context)


# Default arguments for the DAG dag_3plus
default_args = {
    'owner': '3plus',
    'depends_on_past': False,
    'email': ['floosli@3plus.tv'],
    'email_on_failure': False,
    'email_on_retry': False,
}
# DAG Definition and various setting influencing the workflow of the DAG
dag_3plus = DAG(dag_id=DAG_ID,
                description='DAG used to automate the PIN data gathering of 3plus and to update modified files',
                schedule_interval='0 8,20 * * 1-5',
                start_date=datetime(year=2019, month=10, day=15, hour=12),
                end_date=None,
                default_args=default_args,
                concurrency=2,
                max_active_runs=3,
                dagrun_timeout=timedelta(hours=6),
                catchup=False
                )


# ----------------------------------------------------------------------------------------------------------------------
def extract_regular_dates():
    """
    Pull the xcom variables for the regular files from the database
    Saves the pulled dates in a global variable
    :return: None
    """
    puller = xcom.XCom
    regular_dates = []

    for file in REGULAR_FILE_LIST:

        updates = puller.get_many(key=file + '_date',
                                  execution_date=datetime.now(timezone.utc),
                                  dag_ids=DAG_ID,
                                  include_prior_dates=True)
        dates = set()
        for date in updates:
            try:
                val = json.loads(str(date.value))
                dates.update({val})
            except TypeError as e:
                logging.info('Unfortunately got %s, will continue as planed' % str(e))
                continue

        regular_dates.append(dates)

    return regular_dates


def extract_irregular_dates():
    """
    Pull the xcom variables for the irregular files from the database
    Saves the pulled dates in a global variable
    :return: None
    """
    puller = xcom.XCom
    irregular_dates = []

    for file in IRREGULAR_FILE_LIST:

        update = puller.get_one(key=file + '_bool',
                                execution_date=datetime.now(timezone.utc),
                                dag_id=DAG_ID,
                                include_prior_dates=True)

        irregular_dates.append(update)

    return irregular_dates


def download_regular_files_from_ftp_server(remote_path, local_path, file_list, suffix='.pin',
                                           ftp_conn='ftp_default', **kwargs):
    """
    Download all regular files at one at a time sequential from ftp server and save them in a temporary directory
    The dates are pulled from Xcom variables and are pushed from the Sensor_Regular_Files
    :param remote_path: Path to the location on the server
    :param local_path: Path to the local directory where to save the files
    :param file_list: Names of the files to download from the FTP server
    :param suffix: Usually .pin should not change
    :param ftp_conn: Connection to the server, for editing have look at the GUI of airflow under connections
    :param kwargs: Has to be added otherwise it won't work
    :return: None
    """
    conn = FTPHook(ftp_conn_id=ftp_conn)
    regular_dates = extract_regular_dates()

    for file, r_dates in zip(file_list, regular_dates):
        for date_value in r_dates:

            remote_path_full = remote_path + file + '_' + str(date_value) + suffix
            local_path_full = local_path + 'temp/' + file + '_' + str(date_value) + suffix

            conn.retrieve_file(remote_full_path=remote_path_full, local_full_path_or_buffer=local_path_full)
            logging.info('Saved file at {}'.format(local_path_full))


def download_irregular_files_from_ftp_server(remote_path, local_path, file_list, suffix='.pin',
                                             ftp_conn='ftp_default', **kwargs):
    """
    Download all irregular files at one at a time sequential from ftp server and save them in a temporary directory
    The dates are pulled from Xcom variables and are pushed from the Sensor_Irregular_Files
    :param remote_path: Path to the location on the server
    :param local_path: Path to the local directory where to save the files
    :param file_list: Names of the files to download from the FTP server
    :param suffix: Usually .pin should not change
    :param ftp_conn: Connection to the server, for editing have look at the GUI of airflow under connections
    :param kwargs: Has to be added otherwise it won't work
    :return: None
    """
    conn = FTPHook(ftp_conn_id=ftp_conn)
    irregular_dates = extract_irregular_dates()

    for file, update in zip(file_list, irregular_dates):
        if not update:
            continue

        remote_path_full = remote_path + file + suffix
        local_path_full = local_path + 'temp/' + file + suffix

        conn.retrieve_file(remote_full_path=remote_path_full, local_full_path_or_buffer=local_path_full)
        logging.info('Saved file at {}'.format(local_path_full))


def check_hash_of_new_files(regular_dates, irregular_dates):
    """
    Check the hash of the newly downloaded files to ensure updates with an effect.
    At first check the regular files and then the irregular files. If not change has been detected remove the
    respective date from the list od dates to update. Before the execution of this function the files are stored at
    a temporary directory called temp and are moved to their assigned folder if they are determined
    as new  or updated.
    :return: None
    """
    dates_to_update = set()
    for r_file, r_dates in zip(REGULAR_FILE_LIST, regular_dates):
        for present_date in r_dates:

            r_temp_full_path = LOCAL_PATH + 'temp/' + r_file + '_' + str(present_date) + SUFFIX
            r_local_full_path = LOCAL_PATH + r_file + '/' + r_file + '_' + str(present_date) + SUFFIX

            sha256_hash_1 = hashlib.sha256()
            with open(r_temp_full_path, 'rb') as f:
                for byte_block in iter(lambda: f.read(4096), b""):
                    sha256_hash_1.update(byte_block)

            sha256_hash_2 = hashlib.sha256()
            try:
                with open(r_local_full_path, 'rb') as f:
                    for byte_block in iter(lambda: f.read(4096), b""):
                        sha256_hash_2.update(byte_block)

            except (FileNotFoundError, OSError) as e:
                logging.info('No local file found %s, moving to update' % str(e))
                dates_to_update.update({present_date})
                copyfile(r_temp_full_path, r_local_full_path)
                continue

            if sha256_hash_1.hexdigest() == sha256_hash_2.hexdigest():
                logging.info("Hashes haven't changed in %s" % r_local_full_path)
            else:
                logging.info('The new file is different from %s, continue with update' % r_local_full_path)
                dates_to_update.update({present_date})
                copyfile(r_temp_full_path, r_local_full_path)

    for ir_file, ir_update in zip(IRREGULAR_FILE_LIST, irregular_dates):
        if not ir_update:
            continue

        ir_temp_full_path = LOCAL_PATH + 'temp/' + ir_file + SUFFIX
        ir_local_full_path = LOCAL_PATH + ir_file + SUFFIX

        sha256_hash_3 = hashlib.sha256()
        with open(ir_temp_full_path, 'rb') as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash_3.update(byte_block)

        sha256_hash_4 = hashlib.sha256()
        try:
            with open(ir_local_full_path, 'rb') as f:
                for byte_block in iter(lambda: f.read(4096), b""):
                    sha256_hash_4.update(byte_block)

        except (FileNotFoundError, OSError) as e:
            logging.info('No local file found %s, moving to update' % str(e))
            copyfile(ir_temp_full_path, ir_local_full_path)
            continue

        if sha256_hash_3.hexdigest() == sha256_hash_4.hexdigest():
            logging.info("Hashes haven't changed in %s" % ir_local_full_path)
        else:
            logging.info('The new file is different from %s, continue with update' % ir_local_full_path)
            copyfile(ir_temp_full_path, ir_local_full_path)

    logging.info('Following dates changed %s' % dates_to_update)
    return dates_to_update


def detect_correct_dates():
    """
    Remove dates did not get updated
    """
    pusher = xcom.XCom
    regular_dates = extract_regular_dates()
    irregular_dates = extract_irregular_dates()

    dates = check_hash_of_new_files(regular_dates, irregular_dates)

    i = 0
    for date in dates:
        pusher.set(key=f'{i}_real', value=str(date), execution_date=datetime.now(timezone.utc),
                   task_id='dates_real', dag_id=DAG_ID)
        i += 1


def update_facts_tables():
    """
    Data transformation and aggregation
    Updates both facts tables based on the remaining dates computed after the hashes has been compared
    :return: None
    """
    pusher = xcom.XCom
    END = datetime.strptime(END_DAY, '%Y%m%d')

    updates = pusher.get_many(task_ids=['dates_real'],
                              execution_date=datetime.now(timezone.utc),
                              dag_ids=DAG_ID,
                              include_prior_dates=True)
    dates = set()
    for date in updates:
        try:
            val = json.loads(str(date.value))
            dates.update({val})
        except TypeError as e:
            logging.info('Unfortunately got %s, will continue as planed' % str(e))
            continue

    for i in range(DAYS_IN_YEAR):
        date_old = (END - timedelta(days=i)).strftime('%Y%m%d')
        if date_old == START:
            break
        if os.path.isfile(LOCAL_PATH + '%s_%s_Live_DE_15_49_mG.csv' % (START, date_old)) and not pusher.get_one(
                key='newest_day', execution_date=datetime.now(timezone.utc),
                dag_id='dag_3plus', include_prior_dates=True):
            pusher.set(key='newest_day', value=str(date_old), execution_date=datetime.now(timezone.utc),
                       task_id='date_update', dag_id='dag_3plus')
            break

    logging.info('Starting with updating live facts-table')
    Pin_Functions.update_live_facts_table(dates)

    logging.info('Continuing with updating time-shifted facts-table')
    Pin_Functions.update_tsv_facts_table(dates)

    for s in range(DAYS_IN_YEAR):
        date_lv_old = (END - timedelta(days=s)).strftime('%Y%m%d')
        if date_lv_old == START:
            break
        if os.path.isfile(LOCAL_PATH + '%s_%s_Live_DE_15_49_mG.csv' % (START, date_lv_old)):
            shutil.copy(LOCAL_PATH + '%s_%s_Live_DE_15_49_mG.csv' % (START, date_lv_old),
                        '/home/floosli/Dropbox (3 Plus TV Network AG)/3plus_ds_team/Projects/data/Processed_pin_data/'
                        'updated_live_facts_table.csv')

    for t in range(DAYS_IN_YEAR):
        date_tsv_old = (END - timedelta(days=t)).strftime('%Y%m%d')
        if date_tsv_old == START:
            break
        if os.path.isfile(LOCAL_PATH + '%s_%s_delayedviewing_DE_15_49_mG.csv' % (START, date_tsv_old)):
            shutil.copy(LOCAL_PATH + '%s_%s_delayedviewing_DE_15_49_mG.csv' % (START, date_tsv_old),
                        '/home/floosli/Dropbox (3 Plus TV Network AG)/3plus_ds_team/Projects/data/Processed_pin_data/'
                        'updated_tsv_facts_table.csv')

    for date in dates:
        pusher.set(key='update', value=str(date), execution_date=datetime.now(timezone.utc),
                   task_id='date_update', dag_id='dag_3plus')


def delete_content_temp_dir(**kwargs):
    """
    Delete content of the temp folder such that it returns to an empty state
    :return: None
    """
    path = LOCAL_PATH + 'temp/'
    for file in os.listdir(path):
        file_path = os.path.join(path, file)
        try:
            if os.path.isfile(file_path):
                os.unlink(file_path)
        except Exception as e:
            logging.info(e)


# ----------------------------------------------------------------------------------------------------------------------
# 1. and 2. Step:
# Sensors and Tasks to update and download files from the ftp server
# The logic under which the tasks operate is defined in the python operations above or in the accompanied
# Sensor_3plus.py file Sensor to observe the creation and the modification time of regular files
Sensor_Regular_Files = Sensors_3plus.SensorRegularFiles(
    task_id='Sensor_regular_files',
    server_path=REMOTE_PATH,
    local_path=LOCAL_PATH,
    suffix=SUFFIX,
    days_past=SENSOR_IN_PAST,
    file_list=REGULAR_FILE_LIST,
    ftp_conn_id=FTP_CONN_ID,
    fail_on_transient_errors=True,
    poke_interval=60,
    timeout=60,
    soft_fail=False,
    mode='reschedule',
    do_xcom_push=True,
    dag=dag_3plus
)
# Download regular files to a temporary directory before continuing with the update
Task_Download_Regular_Files = PythonOperator(
    task_id='Download_regular_files',
    provide_context=True,
    python_callable=download_regular_files_from_ftp_server,
    op_kwargs={
        'remote_path': REMOTE_PATH,
        'local_path': LOCAL_PATH,
        'file_list': REGULAR_FILE_LIST,
        'suffix': SUFFIX,
        'ftp_conn': FTP_CONN_ID
        },
    retries=5,
    retry_delay=timedelta(seconds=5),
    execution_timeout=timedelta(minutes=2),
    priority_weight=2,
    on_failure_callback=fail_slack_alert,
    dag=dag_3plus
)

# Sensor the creation and modification time of the irregular files Station, CritCode, Crit
Sensor_Irregular_Files = Sensors_3plus.SensorIrregularFiles(
    task_id='Sensor_irregular_files',
    server_path=REMOTE_PATH,
    local_path=LOCAL_PATH,
    file_list=IRREGULAR_FILE_LIST,
    suffix=SUFFIX,
    ftp_conn_id=FTP_CONN_ID,
    fail_on_transient_errors=True,
    poke_interval=60,
    timeout=60,
    soft_fail=False,
    mode='reschedule',
    do_xcom_push=True,
    dag=dag_3plus
)
# Download irregular files to a temporary directory before moving to further update
Task_Download_Irregular_Files = PythonOperator(
    task_id='Download_irregular_files',
    provide_context=True,
    python_callable=download_irregular_files_from_ftp_server,
    op_kwargs={
                'remote_path': REMOTE_PATH,
                'local_path': LOCAL_PATH,
                'file_list': IRREGULAR_FILE_LIST,
                'suffix': SUFFIX,
                'ftp_conn': FTP_CONN_ID
                },
    retries=5,
    retry_delay=timedelta(seconds=5),
    execution_timeout=timedelta(minutes=1),
    priority_weight=2,
    on_failure_callback=fail_slack_alert,
    dag=dag_3plus
)


# ----------------------------------------------------------------------------------------------------------------------
# 3. Step
# Compares also hashes to check if the file really has to be updated with the check_hashes function
# eventually updates the facts table with the remaining dates which have been determined to be new
Task_Detect_Dates = PythonOperator(
    task_id='Detect_dates',
    provide_context=False,
    python_callable=detect_correct_dates,
    retries=3,
    retry_delay=timedelta(seconds=10),
    execution_timeout=timedelta(hours=1),
    priority_weight=1,
    dag=dag_3plus
)

Task_Update_Facts_Table = PythonOperator(
    task_id='Update_facts_table',
    provide_context=False,
    python_callable=update_facts_tables,
    retries=3,
    retry_delay=timedelta(seconds=5),
    execution_timeout=timedelta(hours=1, minutes=20),
    priority_weight=1,
    dag=dag_3plus
)


# ----------------------------------------------------------------------------------------------------------------------
# 4. Step
# Delete all the xcom variables from the sqlite database
# The deletion of the Xcom has to be done on the default DB for configuration or changing the db
# Also, the existing DB and connections should be viewable in the Airflow-GUI.
SensorInfosysExtract = Sensors_3plus.SensorInfosysExtract(
    task_id='Sensor_Infosys_Extract',
    ftp_conn_id=INFOSYS_FTP_CONN_ID,
    fail_on_transient_errors=True,
    poke_interval=60,
    timeout=200,
    soft_fail=False,
    mode='reschedule',
    trigger_rule='all_done',
    dag=dag_3plus
)

SensorValidity = Sensors_3plus.SensorVerifyFactsTables(
    task_id='Sensor_validity_facts_tables',
    fail_on_transient_errors=True,
    poke_interval=60,
    timeout=600,
    soft_fail=False,
    mode='reschedule',
    on_failure_callback=fail_slack_alert,
    trigger_rule='all_done',
    do_xcom_push=True,
    dag=dag_3plus
)

Task_Delete_Xcom_Variables = SqliteOperator(
    task_id='Delete_xcom_date_push',
    sql="delete from xcom where task_id='date_push'",
    sqlite_conn_id=SQL_ALCHEMY_CONN,
    trigger_rule='all_done',
    on_failure_callback=fail_slack_alert,
    dag=dag_3plus
)
# Delete the content of the temp dir for a rounded execution and no remaining files in future iteration
Task_Delete_Content_temp_dir = PythonOperator(
    task_id='Delete_content_temp_dir',
    provide_context=True,
    python_callable=delete_content_temp_dir,
    retries=5,
    retry_delay=timedelta(seconds=5),
    execution_timeout=timedelta(minutes=1),
    on_failure_callback=fail_slack_alert,
    dag=dag_3plus
)


# ----------------------------------------------------------------------------------------------------------------------
# Task Scheduling for Retrieving data and Transforming them to the facts table.
# Scheduling is not allowed to contain any circles or repetitions of the same task
# A graphical view of the DAG is given in the GUI
# Schedule of Tasks:
Sensor_Regular_Files >> Task_Download_Regular_Files >> Task_Detect_Dates >> Task_Update_Facts_Table
Sensor_Irregular_Files >> Task_Download_Irregular_Files >> Task_Detect_Dates >> Task_Update_Facts_Table
Task_Update_Facts_Table >> SensorInfosysExtract >> SensorValidity
SensorValidity >> Task_Delete_Xcom_Variables >> Task_Delete_Content_temp_dir


# ----------------------------------------------------------------------------------------------------------------------
