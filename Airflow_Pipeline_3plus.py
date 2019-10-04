import logging
import pandas as pd
import pin_func_exp as pin_functions
import json
import os
import hashlib

import Sensors_3plus

from datetime import datetime, timedelta, timezone

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
#
# Remote Path:      ftp://ftp.mpg-ftp.ch/PIN-Daten/*.pin with

# Global variables used in the DAG
DAG_ID = 'dag_3plus'  # Set right Id for xcom pusher and puller otherwise the variables can't be found, Important at
# the Sensor and respective Function
REMOTE_PATH = '/PIN-Daten/'
LOCAL_PATH = '/home/floosli/Documents/PIN_Data_Test/'
SUFFIX = '.pin'
REGULAR_FILE_LIST = ['BrdCst', 'SocDem', 'UsageLive', 'UsageTimeShifted', 'Weight']
IRREGULAR_FILE_LIST = ['Station', 'CritCode', 'Crit']
DAYS_IN_PAST = 10
# Connections to the used servers, configurations can be found in the airflow-webserver GUI or over the commandline
FTP_CONN_ID = 'ftp_server_pin_data'
# Fernet-Key for the ftp connection: yP-Y5bM5fRJoyuBPIP31cRZng4Ktk4hV3vNtBuzkSl4=
SQL_ALCHEMY_CONN = 'sql_alchemy_conn'


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

# Download all regular files at one at a time sequential from ftp server and save them in a temporary directory
# The dates are pulled from Xcom variables and are pushed from the Sensor_Regular_Files
def download_regular_files_from_ftp_server(remote_path, local_path, file_list, suffix='.pin',
                                           ftp_conn='ftp_default', **kwargs):

    conn = FTPHook(ftp_conn_id=ftp_conn)
    puller = xcom.XCom

    for file in file_list:

        list_dates = puller.get_many(key=file + '_date',
                                     execution_date=datetime.now(timezone.utc),
                                     dag_ids=DAG_ID,
                                     include_prior_dates=True)

        for date_value in list_dates:

            present_date = json.loads(date_value.value)

            remote_path_full = remote_path + file + '_' + str(present_date) + suffix
            local_path_full = local_path + 'temp/' + file + '_' + str(present_date) + suffix

            conn.retrieve_file(remote_full_path=remote_path_full, local_full_path_or_buffer=local_path_full)
            logging.info('Saved file at {}'.format(local_path_full))


# Download irregular files from the ftp server if xcom variables are set to True
# The xcom variables are only a boolean and only pushed if a new file is uploaded
def download_irregular_files_from_ftp_server(remote_path, local_path, file_list, suffix='.pin',
                                             ftp_conn='ftp_default', **kwargs):

    conn = FTPHook(ftp_conn_id=ftp_conn)
    puller = xcom.XCom

    for file in file_list:

        update = puller.get_one(key=file + '_bool',
                                execution_date=datetime.now(timezone.utc),
                                dag_id=DAG_ID,
                                include_prior_dates=True)

        if not update:
            continue

        remote_path_full = remote_path + file + suffix
        local_path_full = local_path + 'temp/' + file + suffix

        conn.retrieve_file(remote_full_path=remote_path_full, local_full_path_or_buffer=local_path_full)
        logging.info('Saved file at {}'.format(local_path_full))


# Check the hash of the newly downloaded files to ensure updates with an effect
def check_hash_of_new_files(regular_file_list, irregular_file_list, suffix='.pin', **kwargs):

    puller = xcom.XCom

    # Regular Files
    # Pull dates from the database which are to be updated with the respective file type
    for r_file in regular_file_list:

        r_update = puller.get_many(key=r_file + '_date',
                                   execution_date=datetime.now(timezone.utc),
                                   dag_ids=DAG_ID,
                                   include_prior_dates=True)

        # Compare the hash of the new and the old file to check for changes
        for r_date in r_update:

            present_date = json.loads(r_date.value)

            r_temp_full_path = LOCAL_PATH + 'temp/' + r_file + '_' + str(present_date) + suffix
            r_local_full_path = LOCAL_PATH + r_file + '/' + r_file + '_' + str(present_date) + suffix

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
                os.rename(r_temp_full_path, r_local_full_path)
                continue

            if sha256_hash_1.hexdigest() == sha256_hash_2.hexdigest():
                logging.info('Hashes havent changed in %s' % r_local_full_path)
                os.remove(r_temp_full_path)
            else:
                logging.info('The new file is different from %s, continue with update' % r_local_full_path)
                os.rename(r_temp_full_path, r_local_full_path)

    # Irregular files
    # Compare the hash of the new and the old file to check for changes
    for ir_file in irregular_file_list:

        ir_update = puller.get_one(key=ir_file + '_bool',
                                   execution_date=datetime.now(timezone.utc),
                                   dag_id=DAG_ID,
                                   include_prior_dates=True)

        if not ir_update:
            continue

        ir_temp_full_path = LOCAL_PATH + 'temp/' + ir_file + suffix
        ir_local_full_path = LOCAL_PATH + ir_file + suffix

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
            os.rename(ir_temp_full_path, ir_local_full_path)
            continue

        if sha256_hash_3.hexdigest() == sha256_hash_4.hexdigest():
            logging.info('Hashes havent changed in %s' % ir_local_full_path)
            os.remove(ir_temp_full_path)
        else:
            logging.info('The new file is different from %s, continue with update' % ir_local_full_path)
            os.rename(ir_temp_full_path, ir_local_full_path)


# Data transformation and aggregation
# Updated facts_table function for the airflow system
def transformation_facts_table():

    # xcom pull the dates from the upated files and delete afterwards
    puller = xcom.XCom
    list_dates = puller.get_many(execution_date=datetime.now(timezone.utc),
                                 dag_ids=DAG_ID,
                                 include_prior_dates=True)
    dates = []
    for values in list_dates:
        try:
            val = json.loads(values.value)
            dates.append(val)
        except TypeError as e:  # TypeError if boolean is loaded
            logging.info('Got %s, will continue as planed' % str(e))
            continue

    if not dates:
        logging.info('No dates to update found, exiting code')
        exit()

    date_cols = ['show_endtime', 'show_starttime', 'StartTime', 'EndTime']

    # Read the old file in and update dates with day from latest update, Singel-Day-Problem
    df_old = pd.DataFrame()
    for i in range(DAYS_IN_PAST):
        date_old = (datetime.now() - timedelta(days=i - 1)).strftime('%Y%m%d')
        if os.path.isfile(LOCAL_PATH + '20190101_%s_Live_DE_15_49_mG.csv' % date_old):
            dates.append(int(date_old))
            df_old = pd.read_csv(LOCAL_PATH + '20190101_%s_Live_DE_15_49_mG.csv' % date_old, parse_dates=date_cols,
                                 dtype={'Description': str, 'Title': str, 'date': int})

    # Remove duplicates
    dates_to_update = set(dates)
    logging.info('Following dates will be updated %s' % dates_to_update)

    # Remove updated entries from the old file
    for date_remove in dates_to_update:
        df_old = df_old[df_old['date'] != date_remove]

    stations = pin_functions.get_station_dict()
    update = pd.DataFrame()
    out_shows = []
    out_viewers = []

    # Create a new
    for date in dates_to_update:

        date = str(date)
        date = datetime(year=int(date[0:4]), month=int(date[4:6]), day=int(date[6:8]))

        try:
            logging.info('Updating %s' % date.strftime("%Y%m%d"))

            # Import live-viewers
            lv = pin_functions.get_live_viewers(agemax=49, agemin=15, date=date.strftime("%Y%m%d"))
            lv = pd.concat([lv, pin_functions.get_live_viewers(agemax=49, agemin=15,
                                                               date=(date + timedelta(days=1)).strftime("%Y%m%d"))],
                           axis=0, ignore_index=False)
            lv["station"] = lv["StationId"].map(stations)
            lv = lv[['StartTime', 'EndTime', 'H_P', 'Weights', 'station', 'Kanton']]
            lv['Weights'] = lv['H_P'].map(pin_functions.get_weight_dict((date.strftime("%Y%m%d"))))

            # Import Broadcasting Schedule
            sched = pin_functions.get_brc(date.strftime("%Y%m%d"), (date + timedelta(days=1)).strftime("%Y%m%d"))
            sched["station"] = sched["ChannelCode"].map(stations).astype(str)
            sched = sched[['Date', 'Title', 'StartTime', 'EndTime', 'BrdCstId', 'Description', 'Duration', 'station']]

            # Map both together and append to a list
            viewers, shows = pin_functions.map_viewers(sched, lv)
            out_shows.append(shows)
            out_viewers.append(viewers)

            # Concatenate to a dataframe which will be concatenated to the old file
            df_new = pin_functions.df_to_disk(out_viewers, out_shows, date.strftime("%Y%m%d"))
            update = pd.concat([update, df_new], axis=0, ignore_index=False, sort=True)

        # If following day is not available, handle it here with computing just the single day for now
        except FileNotFoundError as e:
            print("Problems with %s, no consecutive file found %s" % (date, str(e)))

            lv = pin_functions.get_live_viewers(agemax=49, agemin=15, date=date.strftime("%Y%m%d"))
            lv["station"] = lv["StationId"].map(stations)
            lv = lv[['StartTime', 'EndTime', 'H_P', 'Weights', 'station', 'Kanton']]
            lv['Weights'] = lv['H_P'].map(pin_functions.get_weight_dict((date.strftime("%Y%m%d"))))

            sched = pin_functions.single_brc(date.strftime("%Y%m%d"))
            sched["station"] = sched["ChannelCode"].map(stations).astype(str)
            sched = sched[['Date', 'Title', 'StartTime', 'EndTime', 'BrdCstId', 'Description', 'Duration', 'station']]

            viewers, shows = pin_functions.map_viewers(sched, lv)
            out_shows.append(shows)
            out_viewers.append(viewers)

            df_new = pin_functions.df_to_disk(out_viewers, out_shows, date.strftime("%Y%m%d"))
            update = pd.concat([update, df_new], axis=0, ignore_index=False, sort=True)

    logging.info('Created updated entries')

    # Concatenate update with old file
    df_updated = pd.concat([df_old, update], axis=0, ignore_index=False)
    df_updated['date'] = pd.to_numeric(df_updated['date'], downcast='integer')
    df_updated.to_csv(f'{LOCAL_PATH}20190101_{df_updated["date"].max()}_Live_DE_15_49_mG.csv', index=False)

    # Delete redundant files from directory
    newest = False
    for i in range(DAYS_IN_PAST):
        date = (datetime.now() - timedelta(days=i - 1)).strftime('%Y%m%d')
        if os.path.isfile(LOCAL_PATH + '20190101_%s_Live_DE_15_49_mG.csv' % date):
            if newest:
                os.remove((LOCAL_PATH + '20190101_%s_Live_DE_15_49_mG.csv' % date))
            else:
                newest = True


# Supported Version
# Sensors and Tasks to update and download files
# TODO parameters for production and logic
# Observation of regular files
# The Sensor definition is in an accompanied file called Sensors_3plus.py
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
    execution_timeout=timedelta(minutes=1),
    dag=dag_3plus
)

# Observe and download Station, CritCode, Crit files
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
    dag=dag_3plus
)


# Compare the hashes of the new downloaded files with the already existing files to determine
# if the file has significantly changed
Task_Compare_Hashes = PythonOperator(
    task_id='Compare_hashes',
    provide_context=True,
    python_callable=check_hash_of_new_files,
    op_kwargs={
        'regular_file_list': REGULAR_FILE_LIST,
        'irregular_file_list': IRREGULAR_FILE_LIST,
        'suffix': SUFFIX
    },
    retries=5,
    retry_delay=timedelta(seconds=5),
    execution_timeout=timedelta(minutes=3),
    trigger_rule='one_success',
    dag=dag_3plus
)

# Operator to update the FactsTable
Task_Update_Facts_Table = PythonOperator(
    task_id='Update_facts_table',
    provide_context=False,
    python_callable=transformation_facts_table,
    retries=5,
    retry_delay=timedelta(seconds=5),
    execution_timeout=timedelta(minutes=15),
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

# Task Scheduling for Retrieving data and Transforming them to the facts table.
# Scheduling is not allowed to contain any circles
# Schedule of Tasks:

Sensor_Regular_Files >> Task_Download_Regular_Files >> Task_Compare_Hashes
Sensor_Irregular_Files >> Task_Download_Irregular_Files >> Task_Compare_Hashes
Task_Compare_Hashes >> Task_Update_Facts_Table >> Task_Delete_Xcom_Variables

# transformation_facts_table()
