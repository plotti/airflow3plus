import Sensors_3plus
import Pin_Functions
import Airflow_Variables
import Transformations_Daily_Report
import json
import logging
import os

from airflow.models import DAG, xcom
from airflow.operators.python_operator import PythonOperator
from airflow.operators.sqlite_operator import SqliteOperator
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from datetime import datetime, timedelta, timezone

DAG_ID = 'dag_daily_reports'
Airflow_var = Airflow_Variables.AirflowVariables()
# Global variables used in this file
SLACK_CONN_ID = Airflow_var.slack_conn_id
LOCAL_PATH = Airflow_var.local_path
DAYS_IN_YEAR = Airflow_var.days_in_year
CHANNELS = Airflow_var.relevant_channels
SQL_ALCHEMY_CONN = Airflow_var.sql_alchemy_conn
START = Airflow_var.start
END_DAY = Airflow_var.end
"""
This DAG runs functions which are transformations from the facts table.
Consequently most of the functions used in this DAG expect that both the live facts table and the 
tsv facts table exist and are located at the given path.
The two main functions implemented in this file are the computation of the vorwoche email which is send after each 
new day in the facts table and the compute viewers of show which is actually not a used .pkl file for viewers of shows.
Maybe one day it will be used, but so long it will be computed 
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
            :angry: Task Failed. 
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


default_args = {
    'owner': '3plus',
    'depends_on_past': False,
    'email': ['floosli@3plus.tv'],
    'email_on_failure': False,
    'email_on_retry': False,
}

dag_daily_reports = DAG(dag_id=DAG_ID,
                        description='DAG to update the viewers table for shows',
                        schedule_interval='0 10,22 * * 1-5',
                        start_date=datetime(year=2019, month=10, day=15, hour=14),
                        end_date=None,
                        default_args=default_args,
                        concurrency=2,
                        max_active_runs=3,
                        dagrun_timeout=timedelta(hours=6),
                        catchup=False
                        )


# ----------------------------------------------------------------------------------------------------------------------
def compute_viewers_of_show():
    """
    Compute the tables of the show viewers of all the channels relevant for our analysis.
    Needs existing Facts table, live and ovn+7.
    The dates which has to be updated should be on the saved as xcom variable on the sql_alchemy server
    :return: None
    """
    puller = xcom.XCom
    updates = puller.get_many(key='update', execution_date=datetime.now(timezone.utc), dag_ids='dag_3plus',
                              include_prior_dates=True)

    dates = set()
    for date in updates:
        try:
            val = json.loads(date.value)
            dates.update({val})
        except TypeError as e:
            logging.info('Unfortunately got %s, will continue as planed' % str(e))
            continue

    if not dates:
        logging.info('No dates have to be updated, exiting')
        exit()

    logging.info('Following dates will be updated %s' % dates)
    for channel in CHANNELS:

        Transformations_Daily_Report.compute_viewers_of_channel(channel, dates)
        logging.info('Finished updating %s' % channel)


def create_vorwoche_zuschauer():
    """
    Create the vorwoche zuschauer report based on the days that are new.
    One xcom variable should be pushed onto the db to get all days which are new
    :return: None
    """
    df = Pin_Functions.get_live_facts_table()[0]

    recent_day = START
    END = datetime.strptime(END_DAY, '%Y%m%d')
    for i in range(DAYS_IN_YEAR):
        recent_day = (END - timedelta(days=i)).strftime('%Y%m%d')
        if recent_day == START:
            break
        if os.path.isfile(LOCAL_PATH + '%s_%s_Live_DE_15_49_mG.csv' % (START, recent_day)):
            break

    puller = xcom.XCom
    update = puller.get_one(key='newest_day', execution_date=datetime.now(timezone.utc), dag_id='dag_3plus',
                            include_prior_dates=True)

    update_date = datetime.strptime(update, '%Y%m%d')
    recent_day = datetime.strptime(recent_day, '%Y%m%d')
    dates = set()
    while True:
        update_date += timedelta(days=1)
        if recent_day < update_date:
            break
        else:
            dates.update({update_date.strftime('%Y%m%d')})

    if not dates:
        logging.info('No dates have to be updated, exiting')
        exit()

    logging.info('Following dates will be updated %s' % dates)
    for date in dates:
        Transformations_Daily_Report.compute_zuschauer_vorwoche(df, date)


# ----------------------------------------------------------------------------------------------------------------------
# Sensor first if the facts table has been updated do start the update of these reports
# Later in the process it also checks if in the last 4 days the facts table has been updated.
# If this is not the case, have a look at the FTP-server of mediapulse or right a strongly worded email to them.
# Bot consider during vacation/holidays it can happen quiet easily, do don't bother writing an email.
Sensor_facts_table_change = Sensors_3plus.SensorFactsTable(
    task_id='Sensor_facts_table',
    local_path=LOCAL_PATH,
    fail_on_transient_errors=True,
    retries=2,
    poke_interval=60,
    timeout=120,
    soft_fail=False,
    mode='reschedule',
    dag=dag_daily_reports
)

Task_Compute_Tables = PythonOperator(
    task_id='Compute_viewers_of_show',
    provide_context=False,
    python_callable=compute_viewers_of_show,
    retries=3,
    retry_delay=timedelta(minutes=3),
    execution_timeout=timedelta(hours=1),
    priority_weight=1,
    dag=dag_daily_reports
)

Task_Generate_Report_Vorwoche = PythonOperator(
    task_id='Report_Vorwoche',
    provide_context=False,
    python_callable=create_vorwoche_zuschauer,
    retries=3,
    retry_delay=timedelta(minutes=3),
    execution_timeout=timedelta(hours=1),
    priority_weight=2,
    dag=dag_daily_reports
)

Sensor_most_recent_update = Sensors_3plus.SensorMostRecentUpdate(
    task_id='Sensor_4days_since_last_update',
    local_path=LOCAL_PATH,
    fail_on_transient_errors=True,
    retries=2,
    poke_interval=60,
    timeout=120,
    soft_fail=False,
    mode='reschedule',
    trigger_rule='all_done',
    on_failure_callback=fail_slack_alert,
    dag=dag_daily_reports
)

Task_Delete_Xcom_Variables = SqliteOperator(
    task_id='Delete_xcom_update',
    sql="delete from xcom where dag_id='dag_3plus'",
    sqlite_conn_id=SQL_ALCHEMY_CONN,
    trigger_rule='all_done',
    on_failure_callback=fail_slack_alert,
    dag=dag_daily_reports
)


# ----------------------------------------------------------------------------------------------------------------------
# Schedule of tasks, same rule applies for creating the schedule as before
Sensor_facts_table_change >> [Task_Generate_Report_Vorwoche, Task_Compute_Tables] >> Sensor_most_recent_update
Sensor_most_recent_update >> Task_Delete_Xcom_Variables


# ----------------------------------------------------------------------------------------------------------------------
