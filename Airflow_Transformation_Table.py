import Airflow_variables
import Sensors_3plus
import pin_func_exp
import json
import logging
import pandas as pd
import os

from airflow.models import DAG, xcom
from airflow.operators.python_operator import PythonOperator
from airflow.operators.sqlite_operator import SqliteOperator
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from datetime import datetime, timedelta, timezone

DAG_ID = 'dag_transformation'
Airflow_var = Airflow_variables.AirflowVariables()
# Global variables used in this file
SLACK_CONN_ID = Airflow_var.slack_conn_id
LOCAL_PATH = Airflow_var.local_path
TABLES_PATH = Airflow_var.table_viewers_path
ADJUST_YEAR = Airflow_var.adjust_year
DAYS_IN_YEAR = Airflow_var.days_in_year
CHANNELS = Airflow_var.relevant_channels
SQL_ALCHEMY_CONN = Airflow_var.sql_alchemy_conn
CHANNELS_OF_INTEREST = Airflow_var.channels_of_interest
"""
This DAG runs functions which are transformations from the facts table.
Consequently most of the functions used in this DAG expect that both the live facts table and the 
tsv facts table exist and are located at the given path.
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


# Default arguments for the DAG dag_3plus
default_args = {
    'owner': '3plus',
    'depends_on_past': False,
    'email': ['floosli@3plus.tv'],
    'email_on_failure': False,
    'email_on_retry': False,
    'on_failure_callback': fail_slack_alert
}
# DAG Definition and various setting influencing the workflow of the DAG
dag_transformation = DAG(dag_id=DAG_ID,
                         description='DAG to update the viewers table for shows',
                         schedule_interval=timedelta(days=1),
                         start_date=datetime(year=2019, month=10, day=15, hour=12),
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

    dates = []
    for date in updates:

        try:
            val = json.loads(date.value)
            dates.append(val)
        except TypeError as e:
            logging.info('Unfortunately got %s, will continue as planed' % str(e))
            continue

    dates = set(dates)

    if not dates:
        logging.info('No dates have to be updated, exiting')
        exit()

    logging.info('Following dates will be updated %s' % dates)
    for channel in CHANNELS:

        pin_func_exp.compute_viewers_for_channel(channel, dates)
        logging.info('Finished updating %s' % channel)


def create_vorwoche_zuschauer():

    df = pin_func_exp.get_live_facts_table()

    df = df[df['station'].isin(CHANNELS_OF_INTEREST)]
    df = pin_func_exp.add_individual_ratings(df)
    df['Date'] = df['show_starttime'].dt.date

    puller = xcom.XCom
    updates = puller.get_many(key='new_days', execution_date=datetime.now(timezone.utc), dag_ids='dag_3plus',
                              include_prior_dates=True)
    # TODO logic of new days, can work with date old
    dates = []
    for date in updates:

        try:
            val = json.loads(date.value)
            dates.append(val)
        except TypeError as e:
            logging.info('Unfortunately got %s, will continue as planed' % str(e))
            continue

    dates = set(dates)

    if not dates:
        logging.info('No dates have to be updated, exiting')
        exit()

    logging.info('Following dates will be updated %s' % dates)
    for date in dates:

        pin_func_exp.compute_zuschauer_vorwoche(date, df)


# ----------------------------------------------------------------------------------------------------------------------
Sensor_facts_table_change = Sensors_3plus.SensorFactsTable(
    task_id='Sensor_facts_table',
    local_path=LOCAL_PATH,
    fail_on_transient_errors=True,
    retries=2,
    poke_interval=60,
    timeout=120,
    soft_fail=False,
    mode='reschedule',
    dag=dag_transformation
)

Task_Compute_Tables = PythonOperator(
    task_id='Compute_transformation_table',
    provide_context=False,
    python_callable=compute_viewers_of_show,
    retries=3,
    retry_delay=timedelta(seconds=5),
    execution_timeout=timedelta(hours=1),
    priority_weight=1,
    dag=dag_transformation
)

Task_Delete_Xcom_Variables = SqliteOperator(
    task_id='Delete_xcom_update',
    sql="delete from xcom where dag_id='dag_3plus'",
    sqlite_conn_id=SQL_ALCHEMY_CONN,
    trigger_rule='all_done',
    dag=dag_transformation
)

# ----------------------------------------------------------------------------------------------------------------------
# Schedule of tasks
Sensor_facts_table_change >> Task_Compute_Tables >> Task_Delete_Xcom_Variables
# ----------------------------------------------------------------------------------------------------------------------
