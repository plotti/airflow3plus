import datetime
import pandas as pd
import logging
import os
import Airflow_variables
import pin_func_exp

from datetime import timedelta, datetime
"""
Function defined in this file are used to automize the process of detecting conflict of interest conserning
heavy viewers of different shows. We try to diminish the competition of of our shows between each other, 
consequently we do not want to compete for the same heavy viewers during the same time slot

1 detect heavy viewers of a show, with pickle table??? Heavy viewers 95% of total usage and other properties
2 define some metric for comparison (one or more)
3 alert if the shows are in conflict
4* maybe check embeddings
"""
var = Airflow_variables.AirflowVariables()

ADJUST_YEAR = var.adjust_year
DAYS_IN_YEAR = var.days_in_year
LOCAL_PATH = var.local_path


def preprocessing__live_data():
    """
    Read the current live facts table and add rating to each row
    :return: df facts table
    """
    date_cols = ['show_endtime', 'show_starttime', 'StartTime', 'EndTime']

    year = datetime.now().year + ADJUST_YEAR
    start = str(year) + '0101'
    end = str(year) + '1231'
    end = datetime.strptime(end, '%Y%m%d')

    df_lv = pd.DataFrame()
    for i in range(DAYS_IN_YEAR):
        date_old = (end - timedelta(days=i)).strftime('%Y%m%d')
        if date_old == start:
            break
        if os.path.isfile(LOCAL_PATH + '%s_%s_Live_DE_15_49_mG.csv' % (start, date_old)):
            df_lv = pd.read_csv(LOCAL_PATH + '%s_%s_Live_DE_15_49_mG.csv' % (start, date_old), parse_dates=date_cols,
                                dtype={'Description': str, 'H_P': str, 'Kanton': int, 'Title': str, 'Weights': float,
                                       'broadcast_id': int, 'date': str, 'duration': float, 'program_duration': int,
                                       'station': str})
            break

    df_lv = pin_func_exp.add_individual_ratings(df_lv)

    return df_lv


def preprocessing_tsv_data():
    """
    Read the current time shifted facts table
    :return: Dataframe of the tsv facts table
    """
    delayed_date_cols = ['ViewingStartTime', 'ViewingTime',
                         'RecordingStartTime', 'show_endtime',
                         'show_starttime', 'RecordingEndTime']
    year = datetime.now().year + ADJUST_YEAR
    start = str(year) + '0101'
    end = str(year) + '1231'
    end = datetime.strptime(end, '%Y%m%d')

    # Read the old file in and update dates with day from latest update
    df_tsv = pd.DataFrame()
    for i in range(DAYS_IN_YEAR):
        date_old = (end - timedelta(days=i)).strftime('%Y%m%d')
        if date_old == start:
            break
        if os.path.isfile(LOCAL_PATH + '%s_%s_delayedviewing_DE_15_49_mG.csv' % (start, date_old)):
            df_tsv = pd.read_csv(LOCAL_PATH + '%s_%s_delayedviewing_DE_15_49_mG.csv' % (start, date_old),
                                 parse_dates=delayed_date_cols, dtype={'Description': str, 'H_P': str,
                                                                       'HouseholdId': int, 'IndividualId': int,
                                                                       'Kanton': int, 'Platform': int, 'StationId': int,
                                                                       'Title': str, 'TvSet': int, 'Weights': float,
                                                                       'UsageDate': int, 'ViewingActivity': int,
                                                                       'age': int, 'broadcast_id': int, 'date': str,
                                                                       'duration': float, 'program_duration': int,
                                                                       'station': str})
            break

    df_tsv = pin_func_exp.add_individual_ratings_ovn(df_tsv)

    return df_tsv


def filter_for_show(df, channel, title, start_hour=20, end_hour=23, date=None, min_duration=60):
    """
    Filter for various options to specify a show and timeframe
    :param df: Dataframe to filter from
    :param channel: String of channel to filter
    :param title: String of the title of the show
    :param start_hour: Integer of the earliest time to filter for [0,24]
    :param end_hour: Integer of the latest time to filter for, range [0,24]
    :param min_duration: Integer of seconds minimum watched of a show by the viewer, to reduce noice
    :param date: String of the date in format YYYYmmdd
    :return: Dataframe filtered on every given aspect
    """
    df_ah = (df[(df['station'] == channel) & (df['Title'].str.contains(title))
                & (df['show_starttime'].dt.hour >= start_hour)] & (df['show_endtime'].dt.hour <= end_hour)
             & (df['duration'] > min_duration)
             .groupby(['Description', 'Viewing', 'H_P'])['duration_weighted'].sum()).to_frame().reset_index()
    if date is not None:
        df_ah = df[df['Date'].isin(date)]

    return df_ah
    # TODO maybe filter on the duration watched at least 1 min?


def intersection_of_viewers(viewers_1, viewers_2):
    """
    Compute the intersection between two set of viewers
    :param viewers_1: Set of viewers
    :param viewers_2: Set of viewsers
    :return: A set of the resulting intersection of both sets of viewers
    """
    return viewers_1 & viewers_2


def compute_heavy_viewers(df):
    """
    Compute the heavy viewers based on the dataframe given and characteristic of a heavy viewer which are arbitrary
    chosen and not compelte yet.
    :param df: Dataframe of which we want to compute the heavy viewers, should be prefiltered
    :return: A list of the indexes for the viewers contributing the most to the rating of the show
    """
    ranked_viewers_ah = (df.groupby('H_P')['duration_weighted'].sum().sort_values(ascending=False).index.tolist())

    vals = []
    tot = df['duration_weighted'].sum()
    for n_viewers in range(len(ranked_viewers_ah)):
        vals.append(df[(df['H_P'].isin(ranked_viewers_ah[:n_viewers]))]['duration_weighted'].sum() / tot)

    heavy_viewers = (df.groupby('H_P')['duration_weighted'].sum()
                     .sort_values(ascending=False)[:int(len(vals)*0.4)]  # TODO formalize the 90%
                     .index.tolist())

    return heavy_viewers


def compare_embeddings(show_1, show_2):
    """
    Compare the embeddings of two shows with the help of the dot product, similar shows will have a high
    result and not similar shows will converge to 0.
    The range of the result should be [0,1]
    :param show_1: String Name of a show
    :param show_2: String Name of a show
    :return: scalar in range [0,1]
    """
    show_embs = pd.read_csv('/home/floosli/Documents/Embeddings/show_embeddings_41_primetime_2019_ovn_.csv',
                            index_col=0)

    show_emb_1 = show_embs.loc[show_1]
    show_emb_2 = show_embs.loc[show_2]

    return show_emb_1 * show_emb_2


def analyse_heavy_viewers():
    pass