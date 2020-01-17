import datetime
import pandas as pd
import Pin_Functions
import Airflow_Variables
import logging
import smtplib
import os

from email.mime.multipart import MIMEMultipart
from os.path import basename
from email.mime.application import MIMEApplication
from email.mime.text import MIMEText
from datetime import timedelta, datetime

Airflow_var = Airflow_Variables.AirflowVariables()
# Some global variables
VORWOCHE_PATH = Airflow_var.vorwoche_path
DAYS_IN_YEAR = Airflow_var.days_in_year
ADJUST_YEAR = Airflow_var.adjust_year
TABLES_PATH = Airflow_var.table_viewers_path
CHANNELS = Airflow_var.relevant_channels
CHANNELS_OF_INTEREST = Airflow_var.channels_of_interest
YEAR = Airflow_var.year
MONTH = Airflow_var.month
DAY = Airflow_var.day
START = Airflow_var.start
END = Airflow_var.end
"""
The functions defined in this filed are used to transform the facts tables and generate useful reports 
for various departments in the CH Media group.
The automation of the generation of the reports is done via a Airflow pipeline defined 
in the Airflow_Daily_Reports.py. Most reports are updated daily, some have a different schedule.
For additional reports implement them in the according DAG for maintainability
"""


# ----------------------------------------------------------------------------------------------------------------------
# TRANSFORMATION FUNCTIONS
# Functions to transform the facts table to other representations
# ----------------------------------------------------------------------------------------------------------------------
def compute_viewers_of_channel(channel, dates):
    """
    Compute the viewers table for a given channel and for a given date
    :param channel: Station to compute the table for, should be a string
    :param dates: Day of which we want the table of is in for of a set of strings
    :return: df of the new DataFrame
    """
    dates = list(dates)

    facts_lv = Pin_Functions.get_live_facts_table()[0]
    facts_ovn = Pin_Functions.get_tsv_facts_table()[0]

    if facts_ovn.empty or facts_lv.empty:
        logging.info('A facts table is empty please check for correct paths, unable to update, will exit')
        exit()

    if not dates:
        logging.info('No dates to update')
        exit()

    df_lv = facts_lv.copy()
    df_ovn = facts_ovn.copy()

    # Filter for updated live facts
    df_lv = df_lv.drop(columns=['EndTime', 'Kanton', 'StartTime', 'show_endtime',
                                'show_starttime', 'Title', 'Description'])
    df_lv = df_lv[df_lv['date'].isin(dates)]
    df_lv = df_lv[df_lv['station'] == channel]
    df_lv = df_lv[df_lv['program_duration'] >= 600]
    df_lv['weighted_duration'] = df_lv['duration'] * df_lv['Weights']

    # Filter for updated tsv facts
    df_ovn = df_ovn.drop(columns=['Title', 'Description', 'HouseholdId', 'IndividualId', 'Kanton', 'Platform',
                                  'RecordingDate', 'RecordingEndTime', 'RecordingStartTime', 'StationId',
                                  'TvSet', 'UsageDate', 'ViewingActivity', 'ViewingStartTime', 'ViewingTime',
                                  'age', 'show_endtime', 'show_starttime'])
    df_ovn = df_ovn[df_ovn['date'].isin(dates)]
    df_ovn = df_ovn[df_ovn['station'] == channel]
    df_ovn = df_ovn[df_ovn['program_duration'] >= 600]
    df_ovn['weighted_duration'] = df_ovn['duration'] * df_ovn['Weights']

    table_hv = pd.concat([df_lv, df_ovn], join='inner', ignore_index=False)
    table_hv = table_hv.groupby(by=['date', 'H_P', 'broadcast_id'])['weighted_duration'].sum()

    org_table = pd.DataFrame()
    if os.path.isfile(TABLES_PATH + 'table_viewers_pik_{}'.format(channel)):
        org_table = pd.read_pickle(TABLES_PATH + 'table_viewers_pik_{}'.format(channel))
        org_table = org_table.drop(labels=dates, level=0)

    if org_table.size == 0:
        table_hv.to_pickle(TABLES_PATH + 'table_viewers_pik_{}'.format(channel))
    else:
        new_table = pd.concat([org_table, table_hv], axis=0, join='outer', sort=False)
        new_table.to_pickle(TABLES_PATH + 'table_viewers_pik_{}'.format(channel))


def compute_complete_viewers_table():
    """
    Compute the viewers who watch a show and save the weighted duration of every instance in a pickle file
    :return: None
    """
    end = str(YEAR) + str(MONTH) + str(DAY)
    if ADJUST_YEAR < 0:
        end = str(YEAR) + '1231'

    date = datetime.strptime(START, '%Y%m%d')
    dates = set()

    for i in range(DAYS_IN_YEAR):
        add = date.strftime('%Y%m%d')
        if add == end:
            dates.update({int(add)})
            break
        dates.update([int(add)])
        date = date + timedelta(days=1)

    for channel in CHANNELS:
        compute_viewers_of_channel(channel, dates)
        logging.info('end of %s' % channel)


# ----------------------------------------------------------------------------------------------------------------------
# VORWOCHE ZUSCHAUER COMPUTATION
# Functions for creating the vorwoche zuschauer report
# ----------------------------------------------------------------------------------------------------------------------
def get_brcst_viewer_dict(df, date, group_chans):
    """
    Compute the viewers interesting for our analysis
    :param df: Dataframe used
    :param date: Day of interest
    :param group_chans: Channels to filter for
    :return: Dictionary of the viewers
    """
    cond = ((df['Date'] == date) & (df['station'].isin(group_chans)))
    cols = ['broadcast_id', 'H_P']

    return df[cond][cols].groupby(cols[0])[cols[1]].apply(list).to_dict()


def flat(l):
    """
    Flat the input with a comprehensive list
    :param l: Dataframe to flat
    :return: Flatten Dataframe as a list
    """
    return [o for subo in l for o in subo]


def get_rt_from_vws(data, viewers):
    """
    Get the rating from the viewers
    :param data: Dataframe to get the rating from
    :param viewers: Viewers who produced rating for these shows
    :return: Dataframe with the summed up ratings
    """
    return data[data['H_P'].isin(viewers)]['individual_Rt-T_live'].sum()


def format_dataframe(broadcast_id, ratings_dict, show_format):
    """
    Create a format for the dataframe
    :param broadcast_id: Id of the show of interest
    :param ratings_dict: Dictionary of the ratings of this show
    :param show_format: Format of the show
    :return: Formatted dataframe
    """
    return pd.DataFrame().from_dict(ratings_dict, orient='index', columns=[show_format[broadcast_id]])


def to_xl_multi_sheet(df, f, group_chans):
    """
    Write the data to an excel multi sheet
    :param df: dataframe to write
    :param f: Path where to save the sheets
    :param group_chans: Channels of interest for our analysis
    :return: None
    """
    df = df.T.copy()
    writer = pd.ExcelWriter(f, engine='xlsxwriter')
    hours_col = [o.split(" ")[1].split(":")[0] for o in df.columns]

    vorabend = df[[col for col, h in zip(df.columns, hours_col) if int(h) in [18, 19]]]
    vorabend = [vorabend[[col for col in vorabend.columns if chan in col]] for chan in group_chans]

    for frame, chan in zip(vorabend, group_chans):
        frame.to_excel(writer, sheet_name=chan + "_vorabend")
        worksheet = writer.sheets[chan + "_vorabend"]
        worksheet.set_column(0, 0, 67)
        for idx, col in enumerate(frame):
            series = frame[col]
            max_len = max((series.astype(str).map(len).max(), len(str(series.name)))) + 1
            worksheet.set_column(idx+1, idx+1, max_len)

    primetime = df[[col for col, h in zip(df.columns, hours_col) if int(h) in [20, 21, 22, 23, 24]]]
    primetime = [primetime[[col for col in primetime.columns if chan in col]] for chan in group_chans]

    for frame, chan in zip(primetime, group_chans):
        frame.to_excel(writer, sheet_name=chan + "_primetime")
        worksheet = writer.sheets[chan + "_primetime"]
        worksheet.set_column(0, 0, 67)
        for idx, col in enumerate(frame):
            series = frame[col]
            max_len = max((series.astype(str).map(len).max(), len(str(series.name)))) + 1
            worksheet.set_column(idx+1, idx+1, max_len)

    writer.save()
    logging.info('Created the excel multi sheet and saved it at the given path')


def send_mail(date, date_day_b4, date_week_b4, fname_day_b4, fname_week_b4):
    """
    Send an email to all persons of relevance
    :param date: Date of interest
    :param date_day_b4: Day before the day of interest
    :param date_week_b4: Week before a day of interest
    :param fname_day_b4: Filename of the day before report
    :param fname_week_b4: Filename of the week before report
    :return: None
    """
    COMMASPACE = ', '
    msg = MIMEMultipart()
    msg['Subject'] = f'[Vorwoche Zuschauer] Rt-T für {date},' \
                     f' berechnet mit Zuschauern von {date_day_b4} und {date_week_b4}'
    recipients = ['hb@3plus.tv', 'floosli@3plus.tv', 'lhe@3plus.tv', 'sk@3plus.tv', 'tp@3plus.tv',
                  'plotti@gmx.net', 'kh@3plus.tv', 'ps@3plus.tv']
    msg['From'] = 'Harold Bessis <hb@3plus.tv>'
    msg['To'] = COMMASPACE.join(recipients)
    body = f"Hallo Zusammen, \n\nIm Anhang findet Ihr die Rt-T für die Programme vom {date}," \
           f"berechnet mit den Zuschauern von Programmen"
    body += f" vom {date_day_b4} und {date_week_b4}. \n\nBeste Grüsse,\nHarold (automatic email)"
    body = MIMEText(body)
    msg.attach(body)

    for file in [fname_day_b4, fname_week_b4]:
        with open(file, 'rb') as f:
            att = MIMEApplication(f.read(), Name=basename(file))
            msg.attach(att)

    s = smtplib.SMTP('3plus-tv.mail.protection.outlook.com:25')
    s.send_message(msg)
    s.quit()
    logging.info('The email has been sent, the receivers will be notified shortly')


def compute_zuschauer_vorwoche(df, date):
    """
    Compute the report of the vorwoche zuschauer and sends an email to all intereted persons
    :param df: Dataframe of the current facts table or a facts table on which the report should be generated on
    :param date: Day of interest to compute the report for
    :return: None
    """
    df = df[(df['show_starttime'].dt.hour.isin([18, 19, 20, 21, 22, 23])) & (df['program_duration'] > 600)
            & (df['station'].isin(CHANNELS_OF_INTEREST))]
    df['broadcast_id'] = df['broadcast_id'].astype(int)
    df = Pin_Functions.add_individual_ratings(df)
    df['Date'] = df['show_starttime'].dt.date

    logging.info('Creating the zuschauer vorwoche report for %s' % date)

    id_title_dict = (df[['broadcast_id', 'Title']].drop_duplicates().set_index('broadcast_id').to_dict()['Title'])

    id_starttime_dict = (df[['broadcast_id', 'show_starttime']].drop_duplicates().groupby('broadcast_id').min()
                         .to_dict()['show_starttime'])
    id_starttime_dict = {k: v.strftime('%H:%M') for k, v in id_starttime_dict.items()}

    id_chan_dict = (df[['broadcast_id', 'station']].drop_duplicates().set_index('broadcast_id')
                    .to_dict()['station'])

    show_format = {k: id_chan_dict[k] + " " + id_starttime_dict[k] + " " + id_title_dict[k] for k in id_chan_dict}

    date = datetime.strptime(date, '%Y%m%d').date()
    day_before = (date + timedelta(days=-1))
    week_before = (date + timedelta(days=-7))

    # dict of {broadcast_id : viewer_list} for each broadcast_id of 7 days ago for our group channels
    brcst_to_viewers_week_b4 = get_brcst_viewer_dict(df, week_before, CHANNELS)

    # dict of {broadcast_id : viewer_list} for each broadcast_id of 1 day ago for our group channels
    brcst_to_viewers_day_b4 = get_brcst_viewer_dict(df, day_before, CHANNELS)

    # For Date == 'date', getting ratings for all programs of channels of interest
    # using subsets of viewers from our group channels' programs, from day_before and last_week
    viewers_of_interest = (set(flat(list(brcst_to_viewers_week_b4.values())))
                           | set(flat(list(brcst_to_viewers_day_b4.values()))))

    df_date = df[(df['Date'] == date) & (df['H_P'].isin(viewers_of_interest))].copy()

    date_ratings_week_b4 = {}
    date_ratings_day_b4 = {}

    # Looping over yesterday's programs
    for bcst_id, group in df_date.groupby('broadcast_id'):

        week_b4 = {}
        day_b4 = {}

        # Computing yesterday's Rt-T using last week's viewers
        for bcst_id_week_b4, viewers_week_b4 in brcst_to_viewers_week_b4.items():
            week_b4[bcst_id_week_b4] = get_rt_from_vws(group, viewers_week_b4)

        # Computing yesterday's Rt-T using the day before's viewers
        for bcst_id_day_b4, viewers_day_b4 in brcst_to_viewers_day_b4.items():
            day_b4[bcst_id_day_b4] = get_rt_from_vws(group, viewers_day_b4)

        date_ratings_day_b4[bcst_id] = day_b4
        date_ratings_week_b4[bcst_id] = week_b4

    # Formatting the computations to dataframes
    week_b4_data = []
    for broadcast_id, ratings in date_ratings_week_b4.items():
        week_b4_data.append(format_dataframe(broadcast_id, ratings, show_format))
    week_b4 = pd.concat(week_b4_data, axis=1)
    week_b4.index = week_b4.index.map(show_format)

    day_b4_data = []
    for broadcast_id, ratings in date_ratings_day_b4.items():
        day_b4_data.append(format_dataframe(broadcast_id, ratings, show_format))
    day_b4 = pd.concat(day_b4_data, axis=1)
    day_b4.index = day_b4.index.map(show_format)

    date_format = '%A %d.%m.%Y'
    filename = "Rt from " + date.strftime(date_format) + " with viewers from "
    filename_day_b4 = VORWOCHE_PATH + filename + day_before.strftime(date_format) + '.xlsx'
    filename_week_b4 = VORWOCHE_PATH + filename + week_before.strftime(date_format) + '.xlsx'

    to_xl_multi_sheet(day_b4, filename_day_b4, CHANNELS)
    to_xl_multi_sheet(week_b4, filename_week_b4, CHANNELS)

    send_mail(*(map(lambda x: x.strftime(date_format), [date, day_before, week_before])),
              filename_day_b4, filename_week_b4)

    os.remove(filename_day_b4)
    os.remove(filename_week_b4)


# ----------------------------------------------------------------------------------------------------------------------
