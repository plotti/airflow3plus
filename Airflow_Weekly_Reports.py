import Airflow_Variables
import Transformations_Weekly_Reports
import Plotly_Graph_Heavy_Viewers
import Plotly_Metrics_Eps
import logging
import os
import shutil
import getpass

from airflow.models import DAG, xcom
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
from airflow.operators.sqlite_operator import SqliteOperator
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from datetime import datetime, timedelta, timezone


DAG_ID = 'dag_weekly_reports'
Airflow_var = Airflow_Variables.AirflowVariables()
# Global variables used in this file
SLACK_CONN_ID = Airflow_var.slack_conn_id
SQL_ALCHEMY_CONN = Airflow_var.sql_alchemy_conn
LOCAL_PATH = Airflow_var.local_path
HV_STEAL_PATH = Airflow_var.steal_pot_path
HEATMAP_PATH = Airflow_var.heatmap_path
DAYS_IN_YEAR = Airflow_var.days_in_year
START = Airflow_var.start
END_DAY = Airflow_var.end
EPS = Airflow_var.eps
user = getpass.getuser()
"""
This Dag is used to generate reports that are required weekly. Such as the Heatmap of zapping and 
the HeavyViewersTool to optimize cross promotion. The computations are mainly made in the Transformation_Weekly_Reports
file.
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

dag_weekly_reports = DAG(dag_id=DAG_ID,
                         description='DAG to update the viewers table for shows',
                         schedule_interval='0 21 * * 2',
                         start_date=datetime(year=2019, month=10, day=15, hour=14),
                         end_date=None,
                         default_args=default_args,
                         concurrency=2,
                         max_active_runs=3,
                         dagrun_timeout=timedelta(hours=6),
                         catchup=False
                         )


# ----------------------------------------------------------------------------------------------------------------------
def update_heatmap_week():
    """
    Update the heatmap for last week. Determine which are the new dates to be updated.
    Safe versions of the updated version in locally and on dropbox
    :return: None
    """
    recent_day = START
    END = datetime.strptime(END_DAY, '%Y%m%d')
    for i in range(DAYS_IN_YEAR):
        recent_day = (END - timedelta(days=i)).strftime('%Y%m%d')
        if recent_day == START:
            break
        if os.path.isfile(LOCAL_PATH + '%s_%s_Live_DE_15_49_mG.csv' % (START, recent_day)):
            break

    puller = xcom.XCom

    oldest = puller.get_one(key='oldest_day', execution_date=datetime.now(timezone.utc), dag_id='dag_weekly_reports',
                            include_prior_dates=True)

    start = datetime.strptime(oldest, '%Y%m%d')
    date = datetime.strptime(recent_day, '%Y%m%d')
    dates = set()
    while True:
        if date <= start:
            break
        dates.update({date.strftime('%Y%m%d')})
        date -= timedelta(days=1)

    if not dates:
        logging.info('No dates have to be updated, exiting')
        exit()

    logging.info('Updating following dates: %s' % dates)
    for days in dates:
        try:
            Transformations_Weekly_Reports.update_heatmap(str(days), threshold_duration=False)
        except FileNotFoundError as e:
            logging.info(str(e))
            continue

    for days in dates:
        try:
            Transformations_Weekly_Reports.update_heatmap(str(days), threshold_duration=True)
        except FileNotFoundError as e:
            logging.info(str(e))
            continue

    logging.info('Picklefile has been updated, can be uploaded from Dash')
    shutil.copy(HEATMAP_PATH + 'data_heatmap_chmedia.pkl',
                f'/home/{user}/Dropbox (3 Plus TV Network AG)/3plus_ds_team/Projects/'
                f'P38 Zapping sequences clustering (Heatmaps & more)/data_heatmap_chmedia.pkl')

    shutil.copy(HEATMAP_PATH + 'data_heatmap_chmedia_threshold.pkl',
                f'/home/{user}/Dropbox (3 Plus TV Network AG)/3plus_ds_team/Projects/'
                f'P38 Zapping sequences clustering (Heatmaps & more)/data_heatmap_chmedia_threshold.pkl')


def xcom_push_oldest():
    """
    Push the current date of the newest version of the facts table.
    Used to keep track of which days need to be updated every week
    :return: None
    """
    pusher = xcom.XCom
    recent_day = START
    END = datetime.strptime(END_DAY, '%Y%m%d')
    for i in range(DAYS_IN_YEAR):
        recent_day = (END - timedelta(days=i)).strftime('%Y%m%d')
        if recent_day == START:
            break
        if os.path.isfile(LOCAL_PATH + '%s_%s_Live_DE_15_49_mG.csv' % (START, recent_day)):
            break

    pusher.set(key='oldest_day', value=str(recent_day), execution_date=datetime.now(timezone.utc),
               task_id='Oldest_Push', dag_id='dag_weekly_reports')
    logging.info('Pushed the date of the facts table as xcom variable: value %s' % recent_day)


def create_heavy_viewer_report():
    """
    Create the excel Matrix of the HeavyViewersStealPotential and generate the
    Plotly Table based on the excel file.
    :return: None
    """
    Transformations_Weekly_Reports.analyse_heavy_viewers()
    logging.info('Created the excel file of all the values')

    Plotly_Graph_Heavy_Viewers.generate_plotly_table()
    logging.info('Created the plotly table and saved it locally as .txt file')


def create_metrics_eps():
    """
    Create the div for the plotly div of the EPs metrics
    """

    Plotly_Metrics_Eps.gather_stats(EPS)
    for show in EPS:
        Plotly_Metrics_Eps.generate_graphs_eps(show)
    logging.info(f'Graphs and statstable generated')

    Plotly_Metrics_Eps.generate_statstables_eps()
    logging.info(f'Plotly tables generated')

    Plotly_Metrics_Eps.create_audienceflow_div()
    logging.info(f'Audienceflow of eps generated')


# ----------------------------------------------------------------------------------------------------------------------
# The flow is pretty straight forward. First generate all the reports based on the updated dates, then save them
# in various places and finally clean up a bit
Task_Generate_Plotly_Tool = PythonOperator(
    task_id='HeavyViewersTool',
    provide_context=False,
    python_callable=create_heavy_viewer_report,
    retries=3,
    retry_delay=timedelta(minutes=1),
    execution_timeout=timedelta(hours=1),
    priority_weight=1,
    dag=dag_weekly_reports
)

Task_Generate_Plotly_Metrics = PythonOperator(
    task_id='Metrics_Eps',
    provide_context=False,
    python_callable=create_metrics_eps,
    retries=3,
    retry_delay=timedelta(minutes=1),
    execution_timeout=timedelta(hours=2),
    trigger_rule='all_done',
    priority_weight=1,
    dag=dag_weekly_reports
)

Task_Update_Heatmap = PythonOperator(
    task_id='Update_Heatmap',
    provide_context=False,
    python_callable=update_heatmap_week,
    retries=3,
    retry_delay=timedelta(minutes=1),
    execution_timeout=timedelta(hours=2),
    priority_weight=1,
    trigger_rule='all_success',
    dag=dag_weekly_reports
)

Task_Delete_Xcom_Oldest = SqliteOperator(
    task_id='Delete_xcom_oldest_push',
    sql="delete from xcom where task_id='Oldest_Push'",
    sqlite_conn_id=SQL_ALCHEMY_CONN,
    trigger_rule='all_done',
    on_failure_callback=fail_slack_alert,
    dag=dag_weekly_reports
)

Task_Push_Oldest_Xcom = PythonOperator(
    task_id='Oldest_Push',
    provide_context=False,
    python_callable=xcom_push_oldest,
    retries=2,
    retry_delay=timedelta(minutes=3),
    execution_timeout=timedelta(hours=1),
    trigger_rule='all_success',
    on_failure_callback=fail_slack_alert,
    dag=dag_weekly_reports
)

# ----------------------------------------------------------------------------------------------------------------------
# Schedule of Tasks
Task_Generate_Plotly_Tool >> Task_Generate_Plotly_Metrics
Task_Update_Heatmap >> Task_Delete_Xcom_Oldest >> Task_Push_Oldest_Xcom


# ----------------------------------------------------------------------------------------------------------------------
