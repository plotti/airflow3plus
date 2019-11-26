from Airflow_Utils import Airflow_variables
from Weekly_Reports import Heavy_viewers_analysis, Plotly_graph_heavy_viewers
import smtplib
import logging

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from datetime import datetime, timedelta

from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.text import MIMEText

DAG_ID = 'dag_weekly_reports'
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
YEAR = Airflow_var.year
MONTH = Airflow_var.month
DAY = Airflow_var.day
START = Airflow_var.start
END_DAY = Airflow_var.end
"""
This Dag is used to automize the generating reports that are required weekly.
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


# Default arguments for the DAG dag_3plus
default_args = {
    'owner': '3plus',
    'depends_on_past': False,
    'email': ['floosli@3plus.tv'],
    'email_on_failure': False,
    'email_on_retry': False,
}
# DAG Definition and various setting influencing the workflow of the DAG
dag_weekly_reports = DAG(dag_id=DAG_ID,
                         description='DAG to update the viewers table for shows',
                         schedule_interval='0 12 * * 1',
                         start_date=datetime(year=2019, month=10, day=15, hour=14),
                         end_date=None,
                         default_args=default_args,
                         concurrency=2,
                         max_active_runs=3,
                         dagrun_timeout=timedelta(hours=6),
                         catchup=False
                         )


# ----------------------------------------------------------------------------------------------------------------------
def create_heavy_viewer_report():
    """
    Create the excel Matrix of the HeavyViewersStealPotential and generate the
    Plotly Table based on the excel file.
    :return: None
    """
    Heavy_viewers_analysis.analyse_heavy_viewers()
    logging.info('Created the excel file of all the values')

    Plotly_graph_heavy_viewers.generate_plotly_table()
    logging.info('Created the plotly table and saved it locally as html file')


def send_mail_plotly_graph():
    """
    Send an html file of the plotly table to all recipients
    :return: None
    """
    COMMASPACE = ', '
    msg = MIMEMultipart()
    msg['Subject'] = f'HeavyViewersTool Updated Version'
    recipients = ['floosli@3plus.tv', 'hb@3plus.tv']
    msg['From'] = 'Harold Bessis <hb@3plus.tv>'
    msg['To'] = COMMASPACE.join(recipients)
    body = f"Hallo Zusammen, \n\nIm Anhang findet Ihr das HeavyViewersTool mit den aktualisierten Werten,"
    body += f"\n\nBeste Grüsse,\nHarold (automatic email)"
    body = MIMEText(body)
    msg.attach(body)

    with open('/home/floosli/Documents/Heavy_Viewers_StealPot/HeavyViewersTool.html', 'rb') as f:
        att = MIMEApplication(f.read(), Name='HeavyViewersTool')
        msg.attach(att)

    s = smtplib.SMTP('10.3.3.103')
    s.send_message(msg)
    s.quit()
    logging.info('The email has been sent, the receivers will be notified shortly')


# ----------------------------------------------------------------------------------------------------------------------
Task_Generate_Plotly_Tool = PythonOperator(
    task_id='HeavyViewersTool',
    provide_context=False,
    python_callable=create_heavy_viewer_report,
    retries=3,
    retry_delay=timedelta(minutes=3),
    execution_timeout=timedelta(hours=1),
    priority_weight=1,
    dag=dag_weekly_reports
)


Task_Send_Mail = PythonOperator(
    task_id='Send_Mail',
    provide_context=False,
    python_callable=send_mail_plotly_graph,
    retries=1,
    retry_delay=timedelta(minutes=3),
    execution_timeout=timedelta(hours=1),
    priority_weight=1,
    trigger_rule='all_success',
    dag=dag_weekly_reports
)


# ----------------------------------------------------------------------------------------------------------------------
# Schedule of Tasks
Task_Generate_Plotly_Tool >> Task_Send_Mail


# ----------------------------------------------------------------------------------------------------------------------
create_heavy_viewer_report()
send_mail_plotly_graph()
