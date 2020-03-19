import Airflow_Variables
import Transformations_Weekly_Reports
import Plotly_Graph_Heavy_Viewers
import Plotly_Metrics_Eps
import logging
import os
import shutil
import getpass
import pandas as pd
import json

from airflow.models import DAG, xcom
from airflow.operators.bash_operator import BashOperator
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

channel_dict = {'3+ (Schweiz)': '3+', '4+ (Schweiz)': '4+', '5+ (Schweiz)': '5+', '6+ (Schweiz)': '6+', '3sat': '3sat',
                'ATV (Österreich)': 'ATV', 'HD Suisse (Schweiz)': 'HD Suisse', 'n-tv': 'ntv', 'ntv': 'ntv',
                'ORF 1 (Österreich)': 'ORF 1', 'ProSieben': 'ProSieben CH', 'ProSieben Fun': 'ProSieben Fun',
                'Puls 4 (Österreich)': 'Puls 4', 'RTL': 'RTL CH', 'RTL II': 'RTLzwei', 'RTL Living': 'RTL Living',
                'RTL Zwei': 'RTLzwei', 'RTLplus': 'RTLplus', 'Sat.1': 'Sat1.CH', 'SF 1 (Schweiz)': 'SRF 1',
                'sixx': 'Sixx CH', 'SRF 1 (Schweiz)': 'SRF 1', 'SRF zwei (Schweiz)': 'SRF zwei',
                'Super RTL': 'Super RTL', 'TV24 (Schweiz)': 'TV24', 'VOX': 'VOX CH', 'VOXup': 'VOXup'}

title_dict = {'der-bachelor-ch': 'Der Bachelor', 'die-bachelorette-ch': 'Die Bachelorette',
              'bumann-der-restauranttester': 'Bumann der Restauranttester',
              'bauer-ledig-sucht': 'Bauer, ledig, sucht ...',
              'ninja-warrior-switzerland': 'Ninja Warrior Switzerland',
              'die-hoehle-der-loewen-schweiz': 'Die Höhle der Löwen Schweiz',
              'adieu-heimat-schweizer-wandern-aus': 'Adieu Heimat Schweizer wandern aus', 'notruf-2013': 'Notruf',
              'germanys-next-topmodel': 'Germany next topmodel',
              'der-bachelor': 'Der Bachelor', 'die-bachelorette': 'Die Bachelorette',
              'the-voice-of-germany': 'The Voice of Germany', 'die-hoehle-der-loewen': 'Die Höhle der Löwen',
              'sing-meinen-song-das-tauschkonzert': 'Sing meinen Song - Das Tauschkonzert',
              'die-geissens-eine-schrecklich-glamouroese-familie': 'Die Geissens - Eine schrecklich glamouröse Familie!',
              'goodbye-deutschland': 'Goodbye Deutschland! Die Auswanderer',
              'deutschland-sucht-den-superstar': 'Deutschland sucht den Superstar',
              'das-supertalent': 'Das Supertalent',
              'ich-bin-ein-star-holt-mich-hier-raus': 'Ich bin ein Star - holt mich hier raus!',
              'lets-dance': "Let's Dance", 'the-masked-singer-2019': 'The Masked Singer',
              'auf-und-davon-2009': 'Auf und davon',
              'srf-bi-de-luet': 'SRF bi de Lüt',
              'sing-meinen-song-das-schweizer-tauschkonzert': 'Sing meinen Song - Das Schweizer Tauschkonzert',
              'the-voice-of-switzerland': 'The Voice of Switzerland'}


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
                         description='DAG to update weekly reports and tools',
                         schedule_interval='0 1 * * 3',
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

    Transformations_Weekly_Reports.generate_epdates_pickle()

    logging.info('Picklefile has been updated, can be uploaded from Dash')
    shutil.copy(HEATMAP_PATH + 'data_heatmap_chmedia.pkl',
                f'/home/{user}/Dropbox (3 Plus TV Network AG)/3plus_ds_team/Projects/'
                f'P38 Zapping sequences clustering (Heatmaps & more)/data_heatmap_chmedia.pkl')

    shutil.copy(HEATMAP_PATH + 'data_heatmap_chmedia_threshold.pkl',
                f'/home/{user}/Dropbox (3 Plus TV Network AG)/3plus_ds_team/Projects/'
                f'P38 Zapping sequences clustering (Heatmaps & more)/data_heatmap_chmedia_threshold.pkl')

    shutil.copy(HEATMAP_PATH + 'data_heatmap_chmedia.pkl',
                f'/home/floosli/PycharmProjects/Flask_App/metric_app/static/heatmap/data_heatmap_chmedia.pkl')

    shutil.copy(HEATMAP_PATH + 'data_heatmap_chmedia_threshold.pkl',
                f'/home/floosli/PycharmProjects/Flask_App/metric_app/static/heatmap/data_heatmap_chmedia_threshold.pkl')


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


def json_to_csv(df_dates, show):
    """
    Json to csv transformation
    :param df_dates: dataframe
    :param show: show of interest
    :return: None
    """

    openfile = open(f"/home/floosli/Documents/EP_dates/{show}_dates.json")
    jsondata = json.load(openfile)
    df = pd.DataFrame(jsondata)

    df['date'] = pd.to_datetime(df['date'], format="%Y-%m-%d")

    df = df.sort_values(by='date', ascending=False)

    df = declare_firstruns(df, show)
    df = df[~(df['Title'] == 'NEU')]

    df['channel'] = df['channel'].map(channel_dict)
    df['Title'] = df['Title'].map(title_dict)

    df_dates = pd.concat([df_dates, df], axis=0, ignore_index=True, sort=True)
    df_dates.to_csv(f'/home/floosli/Documents/EP_dates/ep_dates.csv')

    return df_dates


def declare_firstruns(df, show):
    """
    Detect first runs of shows
    :param df: df of interest
    :param show: show of interest
    :return: Dataframe with added first run declaration
    """
    if show in ['brt', 'gd']:
        df['First_Run'] = ''
        df = df.reset_index(drop=True)
        for firsts in df['Description'].unique():
            position = max(df[df['Description'] == firsts].index)
            df.at[position, 'First_Run'] = 'NEU_Algo'
        return df

    elif show in ['bachelor', 'bachelorette']:
        df.loc[(df['time'].isin(['20:15:00', '20:14:00'])) & (df['weekday'].isin(['Mo', 'Di', 'Do'])) &
               (df['channel'] == '3+ (Schweiz)'), 'First_Run'] = 'NEU_Algo'
        return df

    elif show in ['nws', 'hdl', 'voice_ch', 'sms_ch']:
        df.loc[(df['time'].isin(['20:15:00', '20:14:00'])) & (df['weekday'].isin(['Di', 'Do', 'Mo', 'Fr'])) &
               (df['channel'] == 'TV24 (Schweiz)'), 'First_Run'] = 'NEU_Algo'
        return df

    elif show in ['ah', 'bls', 'notruf', 'aud', 'bdl']:
        df['First_Run'] = ''
        df['temp'] = df['season'].astype(str) + df['episode'].astype(str)
        for firsts in df['temp'].unique():
            try:
                position = max((df[(df['temp'] == firsts) &
                                   (df['channel'].isin(['3+ (Schweiz)', 'SRF 1 (Schweiz)']))]).index)
            except ValueError:
                continue
            df.at[position, 'First_Run'] = 'NEU_Algo'
        df = df.reset_index(drop=True)
        df = df.drop(columns=['temp'])
        return df

    else:
        return df


def move_json():
    """
    Move the json file to a local directory where it will be transformed
    :return: None
    """
    shows = ['bachelor', 'bachelorette', 'bls', 'brt', 'hdl', 'nws', 'notruf', 'ah',
             'gmnt', 'blor_de', 'bte_de', 'voice_de', 'gd', 'hdl_de', 'sms_de', 'dg',
             'dsds', 'st', 'jc', 'ld', 'ms_de', 'aud', 'bdl', 'sms_ch', 'voice_ch']
    df_ = pd.DataFrame()

    for show in shows:
        shutil.move(f'/home/floosli/FernsehseriesScrapper/{show}_dates.json',
                    f"/home/floosli/Documents/EP_dates/{show}_dates.json")
        df_ = json_to_csv(df_, show)
    df_ = df_[df_['First_Run'] != 'Meat&Greet (BS)']

    shutil.copy(f'/home/floosli/Documents/EP_dates/ep_dates.csv',
                f'/home/floosli/Dropbox (3 Plus TV Network AG)'
                f'/3plus_ds_team/Projects/data/Home production dates/ep_dates.csv')

    shutil.copy(f'/home/floosli/Documents/EP_dates/ep_dates.csv',
                f'/home/floosli/PycharmProjects/Flask_App/metric_app/static/heatmap/ep_dates.csv')

    # Create a pivot table format for the prediction model
    channels = ['3+', '4+', '5+', 'ARD', 'ProSieben CH', 'ProSieben Maxx CH', 'RTL CH', 'S1', 'SAT.1 CH', 'SRF 1',
                'SRF info', 'SRF zwei', 'Sixx CH', 'TV24', 'TV25', 'VOX CH', 'ZDF', 'kabel eins CH']

    df_ = df_.sort_values(by='date')
    df_ = df_[df_['channel'].isin(channels)]
    df_ = df_[~df_['First_Run'].isna()]
    df_.sort_values(by='date', inplace=True)
    df_['prez'] = 1
    df_ = df_.pivot_table(index='date', columns='channel', values='prez').fillna(0)
    df_.columns = [f'fr_{c}' for c in df_.columns]
    df_.to_csv(f'/home/floosli/Dropbox (3 Plus TV Network AG)'
               f'/3plus_ds_team/Projects/data/Home production dates/ep_dates_pivot_table.csv')


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

Task_Scrape_Stuff = BashOperator(
    task_id='Scrape_Stuff',
    bash_command='cd /home/floosli/FernsehseriesScrapper &&'
                 'scrapy crawl show -o bls_dates.json -a show=bauer-ledig-sucht &&'
                 'scrapy crawl show -o brt_dates.json -a show=bumann-der-restauranttester &&'
                 'scrapy crawl show -o bachelor_dates.json -a show=der-bachelor-ch &&'
                 'scrapy crawl show -o bachelorette_dates.json -a show=die-bachelorette-ch &&'
                 'scrapy crawl show -o nws_dates.json -a show=ninja-warrior-switzerland &&'
                 'scrapy crawl show -o hdl_dates.json -a show=die-hoehle-der-loewen-schweiz &&'
                 'scrapy crawl show -o ah_dates.json -a show=adieu-heimat-schweizer-wandern-aus &&'
                 'scrapy crawl show -o notruf_dates.json -a show=notruf-2013 &&'
                 'scrapy crawl show -o gmnt_dates.json -a show=germanys-next-topmodel &&'
                 'scrapy crawl show -o blor_de_dates.json -a show=der-bachelor &&'
                 'scrapy crawl show -o bte_de_dates.json -a show=die-bachelorette &&'
                 'scrapy crawl show -o voice_de_dates.json -a show=the-voice-of-germany &&'
                 'scrapy crawl show -o hdl_de_dates.json -a show=die-hoehle-der-loewen &&'
                 'scrapy crawl show -o sms_de_dates.json -a show=sing-meinen-song-das-tauschkonzert &&'
                 'scrapy crawl show -o dg_dates.json -a show=die-geissens-eine-schrecklich-glamouroese-familie &&'
                 'scrapy crawl show -o gd_dates.json -a show=goodbye-deutschland &&'
                 'scrapy crawl show -o dsds_dates.json -a show=deutschland-sucht-den-superstar &&'
                 'scrapy crawl show -o st_dates.json -a show=das-supertalent &&'
                 'scrapy crawl show -o jc_dates.json -a show=ich-bin-ein-star-holt-mich-hier-raus &&'
                 'scrapy crawl show -o ld_dates.json -a show=lets-dance &&'
                 'scrapy crawl show -o ms_de_dates.json -a show=the-masked-singer-2019 &&'
                 'scrapy crawl show -o aud_dates.json -a show=auf-und-davon-2009 &&'
                 'scrapy crawl show -o bdl_dates.json -a show=srf-bi-de-luet &&'
                 'scrapy crawl show -o sms_ch_dates.json -a show=sing-meinen-song-das-schweizer-tauschkonzert &&'
                 'scrapy crawl show -o voice_ch_dates.json -a show=the-voice-of-switzerland',

    retries=1,
    execution_timeout=timedelta(minutes=30),
    dag=dag_weekly_reports
)

Task_Json_To_CSV = PythonOperator(
    task_id='Transform_Json_to_CSV',
    provide_context=False,
    python_callable=move_json,
    retries=2,
    retry_delay=timedelta(minutes=3),
    execution_timeout=timedelta(hours=1),
    trigger_rule='all_success',
    on_failure_callback=None,  # TODO
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
Task_Scrape_Stuff >> Task_Json_To_CSV


# ----------------------------------------------------------------------------------------------------------------------
