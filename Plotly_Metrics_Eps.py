import plotly.graph_objects as go
import plotly.offline
import datetime
import plotly
import plotly.figure_factory as ff
import pandas as pd
import numpy as np
import calendar
import getpass
import logging
import shutil

from Airflow_Variables import AirflowVariables
from datetime import datetime, timedelta, time
from Pin_Utils import get_gender_dict, get_age_dict


user = getpass.getuser()
PATH = f'/home/{user}/Dropbox (3 Plus TV Network AG)/3plus_ds_team/Projects/data/Flask_Application/'

var = AirflowVariables()
EPS = var.eps
"""
Various computations for the Metrics cards in the Flask app each individual computation has its own section
and is shortly described.
The whole computation can take up to 30min so be a bit patient
"""


# ----------------------------------------------------------------------------------------------------------------------
def add_individual_ratings_ovn(df):
    """
    Computes the rating of the show with additional time-shifted viewing. The offset second
    has to be added because otherwise the results won't be equal to the infosys values.
    A good explanation of infosys why they add this second is yet to be received
    :param df: pd.Dataframe of the time-shifted facts table
    :return: Time-shifted facts table with added rating
    """
    offset_index = (df['show_endtime'] > df['RecordingEndTime']).astype(int)
    df['duration'] += offset_index
    df['individual_Rt-T_tsv'] = df['duration'] * df['Weights'] / df['program_duration']

    return df


def add_individual_ratings(df):
    """
    Add rating column to the dataframe
    :param df: dataframe to add to
    :return: Dataframe with added column
    """
    df = df.copy()
    df['Live_Rating'] = df['duration'] * df['Weights'] / df['program_duration']

    return df


def load_dates_eps(show):
    """
    Load the first run airing dates of the given show
    :param show: String of show title
    :return: list of datetimes
    """
    dates = pd.read_csv(f'/home/{user}/Dropbox (3 Plus TV Network AG)/3plus_ds_team/Projects/data/'
                        f'Home production dates/ep_dates.csv')

    dates = dates[(dates['Title'] == show) & (dates['channel'].isin(['3+', 'TV24']))]

    if show not in ['Bauer, ledig, sucht ...', 'Bumann, der Restauranttester',
                    'Adieu Heimat Schweizer wandern aus', 'Notruf']:
        dates = dates[dates['First_Run'].isin(['NEU', 'NEU_Algo'])]
    if show in ['Bumann, der Restauranttester', 'Bauer, ledig, sucht ...']:
        dates = dates[(dates['channel'] == '3+') & (dates['time'].isin(['20:15:00', '20:14:00']))]
    dates['date'] = dates['date'].str.replace('-', '')

    return dates['date'].tolist()


def load_data():
    """
    load the data of 3 years
    :return: Concatenated dataframe of 3 years
    """
    date_cols = ['show_endtime', 'show_starttime', 'StartTime', 'EndTime']
    df_20 = pd.read_csv(f'/home/{user}/Dropbox (3 Plus TV Network AG)/3plus_ds_team/'
                        f'Projects/data/Processed_pin_data/updated_live_facts_table.csv',
                        parse_dates=date_cols,
                        dtype={'Description': str, 'H_P': str, 'Title': str, 'Weights': float,
                               'date': str, 'duration': float, 'program_duration': int,
                               'station': str},
                        usecols=['station', 'Title', 'date', 'duration', 'Weights', 'program_duration',
                                 'show_endtime', 'show_starttime', 'StartTime', 'EndTime', 'H_P', 'Description'])
    df_20 = add_individual_ratings(df_20)

    date_cols = ['show_endtime', 'show_starttime', 'StartTime', 'EndTime']
    df_19 = pd.read_csv(f'/home/{user}/Dropbox (3 Plus TV Network AG)/3plus_ds_team/'
                        f'Projects/data/Processed_pin_data/20190101_20191231_Live_DE_15_49_mG.csv',
                        parse_dates=date_cols,
                        dtype={'Description': str, 'H_P': str, 'Title': str, 'Weights': float,
                               'date': str, 'duration': float, 'program_duration': int,
                               'station': str},
                        usecols=['station', 'Title', 'date', 'duration', 'Weights', 'program_duration',
                                 'show_endtime', 'show_starttime', 'StartTime', 'EndTime', 'H_P', 'Description']
                        )
    df_19 = add_individual_ratings(df_19)

    df_18 = pd.read_csv(f'/home/{user}/Dropbox (3 Plus TV Network AG)/3plus_ds_team/'
                        f'Projects/data/Processed_pin_data/20180101_20181231_Live_DE_15_49_mG.csv',
                        parse_dates=date_cols,
                        dtype={'Description': str, 'H_P': str, 'Title': str, 'Weights': float,
                               'date': str, 'duration': float, 'program_duration': int,
                               'station': str},
                        usecols=['station', 'Title', 'date', 'duration', 'Weights', 'program_duration',
                                 'show_endtime', 'show_starttime', 'StartTime', 'EndTime', 'H_P', 'Description']
                        )
    df_18 = add_individual_ratings(df_18)

    df_17 = pd.read_csv(f'/home/{user}/Dropbox (3 Plus TV Network AG)/3plus_ds_team/'
                        f'Projects/data/Processed_pin_data/20170101_20171231_Live_DE_15_49_mG.csv',
                        parse_dates=date_cols,
                        dtype={'Description': str, 'H_P': str, 'Title': str, 'Weights': float,
                               'date': str, 'duration': float, 'program_duration': int,
                               'station': str},
                        usecols=['station', 'Title', 'date', 'duration', 'Weights', 'program_duration',
                                 'show_endtime', 'show_starttime', 'StartTime', 'EndTime', 'H_P', 'Description']
                        )
    df_17 = add_individual_ratings(df_17)

    df = pd.concat([df_20, df_19, df_18, df_17])
    del df_20, df_19, df_18, df_17

    return df


def process_pin_data(date_ep, episode_start_time, show, df_2019_live):
    """
    Preprocessing of the given data
    :param date_ep: List of the dates, string format
    :param episode_start_time: List of the start_times, datetime format
    :param show: String of the show title
    :param df_2019_live: dataframe of interest
    :return: Processed dataframe
    """
    if show not in ['Ninja Warrior Switzerland', 'Die Höhle der Löwen Schweiz']:
        df_2019_live = df_2019_live[(df_2019_live['date'].isin([date_ep]))].sort_values(by='show_starttime')
    else:
        df_2019_live = df_2019_live[(df_2019_live['date'].isin([date_ep]))].sort_values(by='show_starttime')

    heavy_viewers = df_2019_live[(df_2019_live['Title'] == show)].groupby('H_P')[
        'Live_Rating'].sum().sort_values(ascending=False).index.tolist()

    # Compute Ratings per Minute
    time_start = datetime.strptime("20:14:09", '%H:%M:%S').time()
    viewer_minutes_df = pd.DataFrame(index=heavy_viewers, columns=[f"{i}" for i in range(130)],
                                     data=np.zeros((len(heavy_viewers), 130)))

    datetime_start = datetime.combine(datetime.strptime(date_ep, '%Y%m%d'), time_start)
    datetime_range = [datetime_start + timedelta(minutes=m) for m in range(130)]

    daily_df = df_2019_live[df_2019_live['date'].isin([date_ep])].copy()

    for viewer in heavy_viewers:
        start_end_list = get_start_end_list(
            daily_df[daily_df['H_P'] == viewer][['StartTime', 'EndTime']].drop_duplicates())
        if len(start_end_list) > 0:
            usage = get_usage_vector(datetime_range, start_end_list)
            viewer_minutes_df.loc[viewer] = viewer_minutes_df.loc[viewer] + usage * df_2019_live[
                df_2019_live['H_P'] == viewer]['Weights'].iloc[0]

    # Resample to seconds
    tmp = pd.DataFrame(viewer_minutes_df.sum())
    tmp["minutes"] = tmp.index
    tmp = tmp.astype(int)
    # shift usage if the material was not recorded from 0 second on
    tmp['time'] = datetime(1970, 1, 1) + pd.TimedeltaIndex(tmp["minutes"], unit='m') + episode_start_time
    tmp.index = tmp["time"]
    tmp = tmp.resample('S').pad()
    tmp = tmp[[0]]
    tmp.columns = ["viewer_minutes"]

    return tmp


def get_start_end_list(start_end_df):
    return start_end_df.apply(lambda x: tuple(x), axis=1).tolist()


def in_date_range(moment, start_end):
    return start_end[0] <= moment <= start_end[1]


def get_usage_segment(datetime_range, start_end_tuple):
    seg = [in_date_range(el, start_end_tuple) for el in datetime_range]
    return seg


def get_usage_vector(datetime_range, start_end_list):
    usage_segments = [False] * len(datetime_range)
    for start_end_tuple in start_end_list:
        new_seg = get_usage_segment(datetime_range, start_end_tuple)
        usage_segments = [a or b for a, b in zip(new_seg, usage_segments)]
    usage_segments = [int(b) for b in usage_segments]
    return np.array(usage_segments)


def visibility(date1, date2):
    return date1 == date2


def generate_graphs_eps(show):
    """
    Generate the timeband graph of the last 3 airing dates of the show.
    Save the div as .txt file onto dropbox
    :param show: String of show title
    :return: None
    """
    df = load_data()
    df['Date'] = df['show_starttime'].dt.date
    df['Date'] = pd.to_datetime(df['Date'], format='%Y-%m-%d')
    df['Weekday'] = df['Date'].dt.weekday.apply(lambda x: calendar.day_abbr[x])
    df['Week_number'] = df['Date'].dt.week
    df['year'] = df['Date'].dt.year
    df['week_identifier'] = df['year'].astype('str') + "_" + df['Week_number'].astype('str')

    # Define Show
    if show not in ['Ninja Warrior Switzerland', 'Die Höhle der Löwen Schweiz']:
        df = df[(df['station'] == '3+')].sort_values(by='show_starttime')
    else:
        df = df[(df['station'] == 'TV24')].sort_values(by='show_starttime')

    dates = load_dates_eps(show)[:3]
    colors = ['rgb(49,130,189)', 'rgb(115,115, 189)', 'rgb(189,189,189)']

    fig = go.Figure()

    i = 0
    buttons = list()
    buttons.append(dict(label=show,
                        method='update',
                        args=[{'visible': [True, True, True]}]
                        ))

    for date in dates:

        df_res = process_pin_data(date, timedelta(seconds=0), show, df)
        df_res = df_res.reset_index(drop=False)

        fig.add_trace(go.Scatter(x=df_res['time'], y=df_res['viewer_minutes'],
                                 mode='lines',
                                 name=f'{date[:4]}-{date[4:6]}-{date[6:8]}',
                                 line=dict(color=f'{colors[i]}')
                                 )
                      )

        buttons.append(dict(label=f'{date}',
                            method='update',
                            args=[{'visible': [visibility(now, date) for now in dates]}]
                            ))
        i += 1

    fig.update_layout(title=f'Rating over the course of a evening of <b>{show}</b>',
                      xaxis=dict(
                          title_text='Time',
                          title_font={"size": 15},
                          title_standoff=15,
                          ticks='outside',
                          tickformat='%H:%M',
                          tickangle=90,
                          nticks=10,
                      ),
                      yaxis=dict(
                          title_text='Rating',
                          title_font={'size': 15},
                          title_standoff=15,
                          ticks='outside',
                          range=[0, 100],
                          nticks=10
                      ),
                      legend=dict(
                          font_size=14,
                          x=0.0,
                          y=1.1,
                      ),
                      showlegend=True,
                      plot_bgcolor='white',
                      autosize=False,
                      width=800,
                      height=400
                      )

    div = plotly.offline.plot(fig, show_link=False, output_type="div", include_plotlyjs=True)

    with open(f'{PATH}Metrics/Zeitschiene/plotly_timeband_{show}.txt', 'w') as f:
        f.write(div)

    shutil.copy(f'{PATH}Metrics/Zeitschiene/plotly_timeband_{show}.txt',
                f'/home/floosli/PycharmProjects/Flask_App/metric_app/static/zeitschiene/plotly_timeband_{show}.txt')


# Compute Stats
# ----------------------------------------------------------------------------------------------------------------------
def compute_hv_produced_ratings(df, show, episode_1, episode_2):
    """
    Compute rating produced by the heavy viewers of a different episodes
    :param df: Dataframe
    :param show: String of show title
    :param episode_1: String of Folge X
    :param episode_2: String of Folge X
    :return: Percentage of the produced rating
    """
    episode_past = df[(df['Title'].str.contains(show)) & (df['Description'].str.contains(episode_1))].copy()

    show_df_sorted = episode_past.groupby('H_P')['Live_Rating'].sum().sort_values(ascending=False)
    show_df_sorted = show_df_sorted / show_df_sorted.sum()
    heavy_viewers = show_df_sorted[show_df_sorted.cumsum() <= 0.9].index.to_list()

    episode_present = df[(df['Title'].str.contains(show)) & (df['Description'].str.contains(episode_2))].copy()

    hv_episode_present = episode_present[episode_present['H_P'].isin(heavy_viewers)]

    return hv_episode_present['Live_Rating'].sum() / episode_present['Live_Rating'].sum()


def gather_stats(eps):
    """
    Compute various metrics for eps and save it as and excel for further transformations
    :param eps: List of EPS
    :return: None
    """
    writer = pd.ExcelWriter(PATH + 'Metrics/plotly_stats.xlsx', engine='xlsxwriter')
    df_lv = load_data()

    channels = ['SRF 1', 'SRF zwei', 'RTL CH', 'ProSieben CH', '3+', 'SAT.1 CH', 'VOX CH',
                'kabel eins CH', '4+', 'SRF info', 'TV24', '5+', 'TV25', 'S1', '6+', 'ORF Eins']

    df_hv = df_lv[(df_lv['station'].isin(channels))]

    for show in eps:

        dates = load_dates_eps(show)

        if show in ['Ninja Warrior Switzerland', 'Die Höhle der Löwen Schweiz',
                    'Sing meinen Song - Das Schweizer Tauschkonzert']:
            df_filt_ep = df_lv[(df_lv['program_duration'] > 600) & (df_lv['station'] == 'TV24') &
                               (df_lv['date'].isin(dates)) & (df_lv['Title'] == show)]
        else:
            df_filt_ep = df_lv[(df_lv['program_duration'] > 600) & (df_lv['station'] == '3+') &
                               (df_lv['date'].isin(dates)) & (df_lv['Title'] == show)]

        f = {'Live_Rating': 'sum', 'date': 'first'}

        try:
            df_filt_ep['Gender'] = df_filt_ep['H_P'].map(get_gender_dict(dates[0]))
            df_filt_ep['Age'] = df_filt_ep['H_P'].map(get_age_dict(dates[0]))
        except FileNotFoundError:
            df_filt_ep['Gender'] = df_filt_ep['H_P'].map(get_gender_dict(dates[1]))
            df_filt_ep['Age'] = df_filt_ep['H_P'].map(get_age_dict(dates[1]))

        df_ep = df_filt_ep.groupby(['Description'])['Live_Rating', 'date'].agg(f)

        df_ep = df_ep.reset_index(drop=False)
        df_ep = df_ep.round(2)

        if show not in ['Bumann, der Restauranttester', 'Ninja Warrior Switzerland', 'Die Höhle der Löwen Schweiz']:
            df_ep['Season'] = df_ep['Description'].str.extract(r'(Staffel\s[0-9]*)')
            df_ep['Episode'] = df_ep['Description'].str.extract(r'(Folge\s[0-9][0-9])')
        else:
            df_ep['Season'] = df_ep['date'].str[0:4]
            df_ep['Episode'] = df_ep['Description']

        viewers = list()
        gender = list()
        age = list()
        sec_watched = list()
        stickyness = list()
        for episode in df_filt_ep['Description'].unique():

            df_temp = df_filt_ep[df_filt_ep['Description'] == episode]

            viewers.append(len(df_temp['H_P'].unique()))

            gender.append(df_temp[df_temp['Gender'] == 'Frauen']['Weights'].sum() / (df_temp['Weights'].sum()))

            temp = df_temp.groupby(['H_P'])['Age'].first()
            age.append(temp.mean())
            del temp

            sec_watched.append(df_temp['duration'].mean())

            # Stickyness with derivatives
            unique_starts = df_temp['show_starttime'].unique()
            times_dict = {x: y[0] for x, y in zip(unique_starts, enumerate(unique_starts))}
            df_temp['Sequ'] = df_temp['show_starttime'].map(times_dict)
            perc = list()
            for i, value in enumerate(unique_starts):
                temp = df_temp
                temp['Present'] = ((temp['EndTime'].clip(upper=value + pd.Timedelta(seconds=60)) - temp[
                    'StartTime'].clip(lower=value)).dt.total_seconds() * temp['Weights']) / 60
                rt_after_ad = temp[temp['Present'] > 0]['Present'].sum()
                temp['Present_after'] = ((temp['EndTime'].clip(upper=value + pd.Timedelta(seconds=360)) - temp[
                    'StartTime'].clip(lower=value + pd.Timedelta(seconds=300))).dt.total_seconds() * temp[
                                             'Weights']) / 60
                rt_after_fivemin = temp[temp['Present_after'] > 0]['Present_after'].sum()
                perc.append(round((rt_after_fivemin - rt_after_ad) / 60, 2))
                if i == 4:
                    break
            stickyness.append(str([x for x in perc if x != 0]))

        viewers = pd.DataFrame(viewers, columns=['#viewers']).reset_index(drop=True)
        gender = pd.DataFrame(gender, columns=['women percentage']).reset_index(drop=True)
        age = pd.DataFrame(age, columns=['avg. age']).reset_index(drop=True)
        sec_watched = pd.DataFrame(sec_watched, columns=['avg. seconds']).reset_index(drop=True)
        stickyness = pd.DataFrame(stickyness, columns=['Stickyness']).reset_index(drop=True)

        df_ep = pd.concat([df_ep, viewers, gender, age, sec_watched, stickyness], axis=1, sort=False)

        episodes = df_filt_ep['Description'].unique()
        del df_temp, df_filt_ep

        # Loyality, did the heavy viewers stick for the next episode, ongoing work TODO
        loyalty = list()
        if show not in ['Bumann, der Restauranttester', 'Ninja Warrior Switzerland', 'Die Höhle der Löwen Schweiz']:

            for i, episode_present in enumerate(episodes, 1):

                if 'Folge 1' in episode_present:
                    loyalty.append(0)
                    continue

                try:
                    episode_past = episodes[i-2]
                    loyalty.append(compute_hv_produced_ratings(df_hv, show, episode_past, episode_present))
                except IndexError as e:
                    logging.info(str(e))
                    loyalty.append(0)

            loyalty = pd.DataFrame(loyalty, columns=['Loyality'])
            df_ep = pd.concat([df_ep, loyalty], axis=1)
        else:
            df_ep['Loyality'] = 0
            df_ep = df_ep.sort_values(by=['Season'], ascending=False)

        df_ep[['#viewers', 'avg. seconds', 'avg. age']] = df_ep[['#viewers', 'avg. seconds', 'avg. age']].round(0)
        df_ep[['women percentage', 'Live_Rating', 'Stickyness', 'Loyality']] =\
            df_ep[['women percentage', 'Live_Rating', 'Stickyness', 'Loyality']].round(2)

        df_ep['date'] = df_ep['date'].str[0:4] + '.' + df_ep['date'].str[4:6] + '.' + df_ep['date'].str[6:8]

        df_ep = df_ep[['date', 'Season', 'Episode', 'Live_Rating', '#viewers',
                       'women percentage', 'avg. age', 'avg. seconds', 'Stickyness', 'Loyality']]

        if show == 'Bauer, ledig, sucht ...':
            seasons = ['Staffel 1', 'Staffel 2', 'Staffel 3', 'Staffel 4',
                       'Staffel 5', 'Staffel 6', 'Staffel 7', 'Staffel 8', 'Staffel 9']
            df_ep = df_ep[~df_ep['Season'].isin(seasons)]
        df_ep = df_ep[df_ep['Season'] != ""]

        df_ep.to_excel(writer, sheet_name=show)
        worksheet = writer.sheets[show]
        for idx, col in enumerate(df_ep):
            series = df_ep[col]
            max_len = max((series.astype(str).map(len).max(), len(str(series.name)))) + 1
            worksheet.set_column(idx+1, idx+1, max_len)

    writer.save()


def generate_statstables_eps():
    """
    Generate stats tables for each EP and save the div for the flask app
    :return:
    """
    shows_list = list()
    x = 0
    y = 1

    annotation_loyality = [dict(text="<b>Description of Loyality metric</b><br>"
                                     "<br>"
                                     "This metric shows the percentage"
                                     "of Rating, the heavy viewers from<br>"
                                     "last weeks episode produced<br>"
                                     "this week.<br>"
                                     "Feel free to contact the"
                                     "Data Science Team<br>"
                                     "if you have any questions<br>"
                                     "<br><br>"
                                     "<b>Description of Stickyness metric</b><br>"
                                     "<br>"
                                     "This metric shows velocity<br>"
                                     "of viewers coming back after an<br>"
                                     "ad-break. Every number is the <br>"
                                     "value after one ad-break",
                                     showarrow=False, x=x, xref='paper',
                                     y=y - 0.9, yref="paper",
                                     align="left", xanchor='right')]

    for show in EPS:

        filter_list = list()

        df = pd.read_excel(PATH + 'Metrics/plotly_stats.xlsx', sheet_name=show, header=0)
        df = df.drop(columns=['Unnamed: 0']).reset_index(drop=True)
        df = df.dropna()

        filter_list.append(dict(label=f'{show}',
                                method='update',
                                args=[{'header': {'values': None},
                                       "cells": {'values': None,
                                                 'height': 30}},
                                      {'title': f'Stats of {show}',
                                       'width': 1700}
                                      ]
                                )
                           )

        for season in df['Season'].unique():
            filter_list.append(dict(label=f'{season}',
                                    method='update',
                                    args=[{'header': {'values': df.columns},
                                           "cells": {'values': df[df['Season'] == season].T.values,
                                                     'height': 30}},
                                          {'title': f'Stats of {show} {season}',
                                           'width': 1700}
                                          ]
                                    )
                               )

        # for each season a selection
        shows_list.append(go.layout.Updatemenu(
            type='dropdown',
            buttons=list(
                filter_list
            ),
            direction='down',
            pad={"r": 10, "t": 15},
            showactive=True,
            visible=True,
            active=0,
            x=x,
            xanchor='right',
            y=y,
            yanchor='top',
            )
        )
        y -= 0.06

    fig = go.Figure()

    fig.add_trace(go.Table(
        name='Heavy_viewers',
        columnwidth=30,
        header=dict(
            values=None,
            line_color='darkslategray',
            align='center',
        ),
        cells=dict(
            values=None,
            line_color='darkslategray',
            align='center',
            font_size=10,
            height=60
        ),
        visible=True
        )
    )

    fig.update_layout(updatemenus=shows_list, dragmode='pan',
                      height=800, width=1700, title_text="<b>Choose a show and season:</b>",
                      annotations=annotation_loyality)

    div = plotly.offline.plot(fig, show_link=False, output_type="div", include_plotlyjs=True)

    with open(PATH + 'Metrics/plotly_table_div.txt', 'w') as f:
        f.write(div)

    shutil.copy(PATH + 'Metrics/plotly_table_div.txt',
                '/home/floosli/PycharmProjects/Flask_App/metric_app/static/plotly_table_div.txt')


# Generate audienceflow
# ----------------------------------------------------------------------------------------------------------------------
def compute_dataframe(channels):
    """
    Dataframe of the channels of interest
    :param channels: list of channels
    :return: filtered dataframe
    """
    dc = ['StartTime', 'EndTime', 'date']
    db_path = '/home/floosli/Dropbox (3 Plus TV Network AG)/3plus_ds_team/Projects/data/Processed_pin_data/'

    df_2018 = pd.read_csv(db_path + '20180101_20181231_Live_DE_15_49_mG.csv', parse_dates=dc,
                          usecols=['Weights', 'duration', 'program_duration', 'date', 'station',
                                   'Title', 'H_P', 'StartTime', 'EndTime'])
    df_2019 = pd.read_csv(db_path + '20190101_20191231_Live_DE_15_49_mG.csv', parse_dates=dc,
                          usecols=['Weights', 'duration', 'program_duration', 'date', 'station',
                                   'Title', 'H_P', 'StartTime', 'EndTime'])

    df = pd.concat([df_2018, df_2019], axis=0)

    df['Weighted_duration'] = df['Weights'] * df['duration']
    df['Rt-T'] = df['Weighted_duration'] / df['program_duration']
    df['wd'] = df['date'].dt.weekday.apply(lambda x: calendar.day_abbr[x])

    df = df[(df['station'].isin(channels))]

    return df


def generate_audienceflow_map(df, channels, show):
    """
    Generate audienceflow heatmap for the flask app. Save div as .txt file and safe them in dropbox
    :param df: dataframe prefiltered
    :param channels: list of channels
    :param show: show of interest in a string
    :return: None
    """
    dates = df['date'].unique()
    st = [18, 19, 20, 21, 22, 23, 24]
    cols = [f"{s}:00-{e}:00" for s, e in zip(st[:-1], st[1:])]
    vals = {}

    gd = df[df['Title'].str.contains(show)].copy()
    gd_stations = gd.groupby('station')['Rt-T'].sum().index.tolist()
    gd = gd[gd['station'].isin(gd_stations)]

    show_df_sorted = gd.groupby('H_P')['duration'].sum().sort_values(ascending=False)
    show_df_sorted = show_df_sorted / show_df_sorted.sum()
    heavy_viewers = show_df_sorted[show_df_sorted.cumsum() <= 0.9].index.to_list()

    df = df[(df['H_P'].isin(heavy_viewers))]

    for date in dates:
        sts = []
        for start_hour in st[:-1]:

            temp = df[(df['date'] == date)].copy()
            t = time(start_hour, 0, 0)
            start_time = pd.Timestamp.combine(pd.to_datetime(date).date(), t)
            end_time = start_time + pd.Timedelta(hours=1)

            temp['StartTime'].clip(lower=start_time, inplace=True)
            temp['EndTime'].clip(upper=end_time, inplace=True)
            temp['dur'] = (temp['EndTime'] - temp['StartTime']).dt.total_seconds()

            temp = temp.drop_duplicates(subset=['H_P', 'station', 'StartTime', 'EndTime'])

            temp['dur'] = temp['dur'] * temp['Weights'] / (end_time - start_time).total_seconds()
            temp = temp[temp['dur'] > 0]
            sts.append(temp.groupby('station')['dur'].sum().reindex(channels))

        daily = pd.concat(sts, axis=1)
        daily.columns = cols
        vals[pd.to_datetime(date).date()] = daily

    daily_data = {}
    for weekday_int in range(7):
        daily_arrays = [data.values for date, data in vals.items() if date.weekday() == weekday_int]
        daily_mean = np.nanmean(np.stack(daily_arrays, axis=2), axis=2)
        daily_data[calendar.day_abbr[weekday_int]] = pd.DataFrame(daily_mean,
                                                                  columns=cols,
                                                                  index=channels).T.round(2)

    annots = {}
    for wd in daily_data.keys():
        fig = ff.create_annotated_heatmap(daily_data[wd].values, x=daily_data[wd].columns.tolist(),
                                          y=daily_data[wd].index.tolist())
        fig.update_yaxes(autorange='reversed')
        annots[wd] = fig['layout']['annotations']

    fig = go.Figure()

    for ix, wd in enumerate(daily_data.keys()):
        fig.add_trace(go.Heatmap(z=daily_data[wd].values,
                                 x=daily_data[wd].columns.tolist(),
                                 y=daily_data[wd].index.tolist(),
                                 text=annots[wd], visible=ix == 0, showscale=False))
        fig.update_yaxes(autorange='reversed')

    viz = np.eye(7).astype(bool)
    updatemenus = [go.layout.Updatemenu(active=0,
                                        buttons=list([dict(label=wd, method='update',
                                                           args=[{'visible': viz[ix]},
                                                                 {
                                                                     'title': f"Live Rt-T from {show} viewers on {wd}",
                                                                     'annotations': annots[wd]}])
                                                      for ix, wd in enumerate(daily_data.keys())]),
                                        direction="down",
                                        pad={"r": 10, "t": 10},
                                        showactive=True,
                                        x=1,
                                        xanchor="left",
                                        y=1.01,
                                        yanchor="top")]

    fig.update_layout(updatemenus=updatemenus, width=1700,
                      title=f"Live Rt-T from {show} viewers<br>Please select weekday (button on the right)")

    div = plotly.offline.plot(fig, show_link=False, output_type="div", include_plotlyjs=True)

    with open(f'{PATH}Metrics/Audienceflow/plotly_flow_{show}.txt', 'w') as f:
        f.write(div)

    shutil.copy(f'{PATH}Metrics/Audienceflow/plotly_flow_{show}.txt',
                f'/home/floosli/PycharmProjects/Flask_App/metric_app/static/plotly_flow_{show}.txt')


def create_audienceflow_div():
    """
    Loop functions for each EPS to create the audienceflow table
    :return: None
    """
    channels = ['SRF 1', 'SRF zwei', 'RTL CH', 'ProSieben CH', '3+', 'SAT.1 CH', 'VOX CH',
                'kabel eins CH', '4+', 'SRF info', 'TV24', '5+', 'TV25', 'S1', '6+', 'ORF Eins']

    df = compute_dataframe(channels)

    for show in EPS:
        generate_audienceflow_map(df, channels, show)


# ----------------------------------------------------------------------------------------------------------------------
