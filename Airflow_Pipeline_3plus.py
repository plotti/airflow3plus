import logging
import pandas as pd
import pin_func_exp as pin_functions
import json
import os
import hashlib

import Sensors_3plus

from datetime import datetime, timedelta, timezone
from shutil import copyfile

from airflow import DAG
from airflow.models import xcom
from airflow.operators.sqlite_operator import SqliteOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.ftp_hook import FTPHook


# A scheduling system based on the Airflow library. We'd like to update locally saved files daily and
# generate enriched reports and tables based on the new entries.
# The 3plus_dag is the defined DAG in which all the task are unraveled and scheduled.
# Most Python_functions used in the PythonOperators are defined in the accompanied file pin_functions.py
# for clearer maintainability add additional functions in the previously mentioned file.

# How to start the airflow process:
# 1. Start the airflow scheduler with the command: airflow scheduler
# optional*. Start the airflow webserver to observe the progress of the DAG with: airflow webserver
# All the configurations can be made through the command line and the GUI is not required but is very useful for
# the application. All commands can be found in the airflow documentation
# Start the DAG which should be processed with the command: airflow unpause dag_id
# To trigger a run outside of schedule: airflow trigger_dag dag_id

# Access credentials to the remote ftp-server from mediapuls
# wget -nc ftp://ftp.mpg-ftp.ch/PIN-Daten/*.pin' --ftp-user=3plus@mpg-ftp.ch --ftp-password=cJGQNd0d
# host = ftp.mpg-ftp.ch,
# user = 3plus@mpg-ftp.ch,
# password = cJGQNd0d

# General file path on the local directory and on the remote server
# Local Path:       /home/floosli/Documents/PIN_Data_Test/*/*
# Remote Path:      ftp://ftp.mpg-ftp.ch/PIN-Daten/*.pin with

# Global variables used in the DAG
DAG_ID = 'dag_3plus'  # Set right Id for xcom pusher and puller otherwise the variables can't be found, Important at
# the Sensor and respective Function
REMOTE_PATH = '/PIN-Daten/'
LOCAL_PATH = '/home/floosli/Documents/PIN_Data_Test/'
SUFFIX = '.pin'
# Sequence of the file is important for the execution of the algorithm
REGULAR_FILE_LIST = ['BrdCst', 'SocDem', 'UsageLive', 'UsageTimeShifted', 'Weight']
IRREGULAR_FILE_LIST = ['Station', 'CritCode', 'Crit']
DAYS_IN_PAST = 10
# Connections to the used servers, configurations can be found in the airflow-webserver GUI or over the commandline
FTP_CONN_ID = 'ftp_server_pin_data'
# Fernet-Key for the ftp connection: yP-Y5bM5fRJoyuBPIP31cRZng4Ktk4hV3vNtBuzkSl4=
SQL_ALCHEMY_CONN = 'sql_alchemy_conn'
#
regular_dates = []
irregular_dates = []


# Default arguments for the DAG dag_3plus
# TODO adjust arguments for production
default_args = {
    'owner': '3plus',
    'depends_on_past': False,
    'email': ['floosli@3plus.tv'],
    'email_on_failure': False,
    'email_on_retry': False
    }

# DAG Definition and its preset settings
dag_3plus = DAG(dag_id=DAG_ID,
                description='DAG used to automate the PIN data gathering of 3plus and to update modified files',
                schedule_interval=timedelta(days=1),
                start_date=datetime(2019, 9, 23, 10),
                end_date=None,
                default_args=default_args,
                concurrency=2,
                max_active_runs=4,
                dagrun_timeout=timedelta(hours=4),
                catchup=False)


# Functions for transformation, enriching and downloading data in the tasks further below
# These functions are executed by PythonOperators and have to be callable
# Important: If the function requires arguments, you need to add **kwargs and set provide_context = True in the
# Operator settings.

# Pull the xcom variables for the regular files from the database and store them locally in a global variable
# for the duration of the execution of a task
def extract_regular_dates():

    puller = xcom.XCom
    global regular_dates

    for file in REGULAR_FILE_LIST:

        updates = puller.get_many(key=file + '_date',
                                  execution_date=datetime.now(timezone.utc),
                                  dag_ids=DAG_ID,
                                  include_prior_dates=True)
        dates = []
        for date in updates:
            try:
                val = json.loads(date.value)
                dates.append(val)
            except TypeError as e:  # TypeError if boolean is loaded
                logging.info('Got %s, will continue as planed' % str(e))
                continue

        dates = set(dates)
        regular_dates.append(dates)


# Pull the xcom variables for the irregular files from the database
def extract_irregular_dates():

    puller = xcom.XCom
    global irregular_dates

    for file in IRREGULAR_FILE_LIST:

        update = puller.get_one(key=file + '_bool',
                                execution_date=datetime.now(timezone.utc),
                                dag_id=DAG_ID,
                                include_prior_dates=True)

        irregular_dates.append(update)


# Download all regular files at one at a time sequential from ftp server and save them in a temporary directory
# The dates are pulled from Xcom variables and are pushed from the Sensor_Regular_Files
def download_regular_files_from_ftp_server(remote_path, local_path, file_list, suffix='.pin',
                                           ftp_conn='ftp_default', **kwargs):

    conn = FTPHook(ftp_conn_id=ftp_conn)
    extract_regular_dates()

    for file, r_dates in zip(file_list, regular_dates):

        for date_value in r_dates:

            remote_path_full = remote_path + file + '_' + str(date_value) + suffix
            local_path_full = local_path + 'temp/' + file + '_' + str(date_value) + suffix

            conn.retrieve_file(remote_full_path=remote_path_full, local_full_path_or_buffer=local_path_full)
            logging.info('Saved file at {}'.format(local_path_full))


# Download irregular files from the ftp server if xcom variables are set to True
# The xcom variables are only a boolean and only pushed if a new file is uploaded
def download_irregular_files_from_ftp_server(remote_path, local_path, file_list, suffix='.pin',
                                             ftp_conn='ftp_default', **kwargs):

    conn = FTPHook(ftp_conn_id=ftp_conn)
    extract_irregular_dates()

    for file, update in zip(file_list, irregular_dates):

        if not update:
            continue

        remote_path_full = remote_path + file + suffix
        local_path_full = local_path + 'temp/' + file + suffix

        conn.retrieve_file(remote_full_path=remote_path_full, local_full_path_or_buffer=local_path_full)
        logging.info('Saved file at {}'.format(local_path_full))


# Check the hash of the newly downloaded files to ensure updates with an effect
def check_hash_of_new_files():

    # Regular Files
    # Pull dates from the database which are to be updated with the respective file type
    # Compare the hash of the new and the old file to check for changes
    global regular_dates
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
                print('No local file found %s, moving to update' % str(e))
                dates_to_update.update({present_date})
                copyfile(r_temp_full_path, r_local_full_path)
                continue

            if sha256_hash_1.hexdigest() == sha256_hash_2.hexdigest():
                logging.info("Hashes haven't changed in %s" % r_local_full_path)
            else:
                logging.info('The new file is different from %s, continue with update' % r_local_full_path)
                dates_to_update.update({present_date})
                copyfile(r_temp_full_path, r_local_full_path)

    # Irregular files
    # Compare the hash of the new and the old file to check for changes
    global irregular_dates
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
            print('No local file found %s, moving to update' % str(e))
            copyfile(ir_temp_full_path, ir_local_full_path)
            continue

        if sha256_hash_3.hexdigest() == sha256_hash_4.hexdigest():
            logging.info("Hashes haven't changed in %s" % ir_local_full_path)
        else:
            logging.info('The new file is different from %s, continue with update' % ir_local_full_path)
            copyfile(ir_temp_full_path, ir_local_full_path)

    logging.info('Following dates are valid %s' % dates_to_update)
    return dates_to_update


# Data transformation and aggregation
# Updated both facts-tables based on the remaining dates computes after the hashes has been compared
def transformation_facts_table():

    dates = set()

    # Get the dates which has to be updated according to new and updated files
    global regular_dates
    global irregular_dates
    extract_regular_dates()
    extract_irregular_dates()

    for file_dates in regular_dates:
        dates = dates | file_dates

    update = check_hash_of_new_files()
    dates = dates.intersection(update)

    if not dates:
        logging.info('No dates to update found, exiting execution')
        exit()

    logging.info('Starting with updating live facts-table')
    pin_functions.update_live_facts_table(dates)

    logging.info('Continuing with updating time-shifted facts-table')
    pin_functions.update_tsv_facts_table(dates)


# Delete content of temp folder such that it returns to an empty state
def delete_content_temp_dir():

    path = LOCAL_PATH + 'temp/'
    for file in os.listdir(path):
        file_path = os.path.join(path, file)
        try:
            if os.path.isfile(file_path):
                os.unlink(file_path)
        except Exception as e:
            logging.info(e)


# Sensors and Tasks to update and download files
# Observation of regular files
Sensor_Regular_Files = Sensors_3plus.SensorRegularFiles(
    task_id='Sensor_regular_files',
    server_path=REMOTE_PATH,
    local_path=LOCAL_PATH,
    suffix=SUFFIX,
    days_past=10,
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

# Download regular files, calls a Python function further up
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
    dag=dag_3plus
)

# Observe and download the irregular files Station, CritCode, Crit
# The Sensor is also defined in a separate file Sensors_3plus.py
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

# Download irregular files, also calls a function further up for the execution
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
    dag=dag_3plus
)


# Compares also hashes to check if the file really has to be updated with the check_hashes function
# eventually updates the facts table with the remaining dates which have to be updated
Task_Update_Facts_Table = PythonOperator(
    task_id='Update_facts_table',
    provide_context=False,
    python_callable=transformation_facts_table,
    retries=3,
    retry_delay=timedelta(seconds=5),
    execution_timeout=timedelta(minutes=40),
    dag=dag_3plus
)


# Delete all the xcom variables from the sqlite database
# The deletion of the Xcom has to be done on the default DB for configuration or changing the db
# consult the airflow.cfg file for configuration of the database.
# Also, the existing DB and connections should be viewable in the Airflow-GUI.
Task_Delete_Xcom_Variables = SqliteOperator(
    task_id='Delete_xcom',
    sql="delete from xcom where dag_id='dag_3plus'",
    sqlite_conn_id=SQL_ALCHEMY_CONN,
    trigger_rule='all_done',
    dag=dag_3plus
)

# Delete the content of the temp dir for clean execution and no remaining files
Task_Delete_Content_temp_dir = PythonOperator(
    task_id='Delete_content_temp_dir',
    provide_context=False,
    python_callable=delete_content_temp_dir,
    retries=5,
    retry_delay=timedelta(seconds=5),
    execution_timeout=timedelta(minutes=1),
    dag=dag_3plus
)

# Task Scheduling for Retrieving data and Transforming them to the facts table.
# Scheduling is not allowed to contain any circles or repetitions of the same task
# Schedule of Tasks:
Sensor_Regular_Files >> Task_Download_Regular_Files >> Task_Update_Facts_Table
Sensor_Irregular_Files >> Task_Download_Irregular_Files >> Task_Update_Facts_Table
Task_Update_Facts_Table >> Task_Delete_Xcom_Variables >> Task_Delete_Content_temp_dir
