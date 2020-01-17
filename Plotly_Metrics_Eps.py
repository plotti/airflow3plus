import plotly.graph_objects as go
import plotly.offline
import pandas as pd
import numpy as np
import calendar
from datetime import datetime, timedelta

# TODO adjust
PATH = '/home/floosli/Dropbox (3 Plus TV Network AG)/3plus_ds_team/Projects/data/Flask_Application/'


def add_individual_ratings(df):

    df = df.copy()
    df['Live_Rating'] = df['duration'] * df['Weights'] / df['program_duration']

    return df


def load_dates_eps(show):

    ep_dates = pd.read_pickle(
        '/home/floosli/Dropbox (3 Plus TV Network AG)/3plus_ds_team/Projects/data/Home production dates/epdates_.pkl')

    # ep_dates['Adieu Heimat - Schweizer wandern aus'] = ep_dates.pop('Adieu Heimat')
    ep_dates['Bauer, ledig, sucht ...'] = ep_dates.pop('Bauer, ledig, sucht...')

    ep_dates = list(ep_dates[show])

    dates = list()
    for dt in ep_dates:
        ts = (dt - np.datetime64('1970-01-01T00:00:00Z')) / np.timedelta64(1, 's')
        ts = datetime.utcfromtimestamp(ts)
        ts = ts.strftime('%Y%m%d')
        dates.append(ts)

    return dates


def load_data():

    date_cols = ['show_endtime', 'show_starttime', 'StartTime', 'EndTime']
    df = pd.read_csv('/home/floosli/Dropbox (3 Plus TV Network AG)/3plus_ds_team/'
                     'Projects/data/Processed_pin_data/20190101_20191231_Live_DE_15_49_mG.csv',
                     parse_dates=date_cols,
                     dtype={'Description': str, 'H_P': str, 'Kanton': int, 'Title': str, 'Weights': float,
                            'broadcast_id': int, 'date': str, 'duration': float, 'program_duration': int,
                            'station': str})
    df = add_individual_ratings(df)

    return df


def process_pin_data(date_ep, episode_start_time, show, df):

    df_2019_live = df

    df_2019_live['Date'] = df_2019_live['show_starttime'].dt.date
    df_2019_live['Date'] = pd.to_datetime(df_2019_live['Date'], format='%Y-%m-%d')
    df_2019_live['Weekday'] = df_2019_live['Date'].dt.weekday.apply(lambda x: calendar.day_abbr[x])
    df_2019_live['Week_number'] = df_2019_live['Date'].dt.week
    df_2019_live['year'] = df_2019_live['Date'].dt.year
    df_2019_live['week_identifier'] = df_2019_live['year'].astype('str') + "_" + df_2019_live['Week_number'].astype(
        'str')

    # Define Show
    df_2019_live = df_2019_live[(df_2019_live['station'] == '3+') & (df_2019_live['date'].isin([date_ep]))].sort_values(
        by='show_starttime')
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
            viewer_minutes_df.loc[viewer] = viewer_minutes_df.loc[viewer] +\
                                            usage * df_2019_live[df_2019_live['H_P'] == viewer]['Weights'].iloc[0]

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

    # Load Data
    df = load_data()

    # TODO how to get the recent 3 dates?
    dates = load_dates_eps(show)[-3:]
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
                          showgrid=True,
                          showline=True,
                          zeroline=True,
                          ticks='outside',
                          tickformat='%H:%M',
                          tickangle=90,
                          nticks=10
                      ),
                      yaxis=dict(
                          title_text='Rating',
                          title_font={'size': 15},
                          title_standoff=15,
                          showgrid=True,
                          showline=True,
                          zeroline=True,
                          ticks='outside',
                          range=[0, 100]
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

    with open(f'{PATH}Metrics/plotly_metrics_{show}.txt', 'w') as f:
        f.write(div)


# ----------------------------------------------------------------------------------------------------------------------
# TODO MA-% average over last X episodes, atm Rating
# sort dates for the relevant ones
def gather_metrics(eps):

    writer = pd.ExcelWriter(PATH + 'Metrics/plotly_metrics.xlsx', engine='xlsxwriter')
    df_lv = load_data()

    for show in eps:

        dates = load_dates_eps(show)

        # individual Rating of each episode
        df_filt_ep = df_lv[(df_lv['program_duration'] > 600) & (df_lv['station'] == '3+') &
                           (df_lv['date'].isin(dates)) & (df_lv['Title'] == show)]
        f = {'Live_Rating': 'sum', 'date': 'first'}

        df_ep = df_filt_ep.groupby(['Description'])['Live_Rating', 'date'].agg(f)

        df_ep = df_ep.reset_index(drop=False)
        df_ep = df_ep.round(2)

        # avg_rating_season TODO

        # compute viewers of episode
        viewers = list()
        for episode in df_filt_ep['Description'].unique():
            df_temp = df_filt_ep[df_filt_ep['Description'] == episode]
            viewers.append(len(df_temp['H_P'].unique()))
        viewers = pd.DataFrame(viewers, columns=['#viewers'])
        df_ep = pd.concat([df_ep, viewers], axis=1)

        # avg seconds watched per viewer
        sec_watched = list()
        for episode in df_filt_ep['Description'].unique():
            df_temp = df_filt_ep[df_filt_ep['Description'] == episode]
            sec_watched.append(df_temp['duration'].mean())
        sec_watched = pd.DataFrame(sec_watched, columns=['avg. seconds'])
        df_ep = pd.concat([df_ep, sec_watched], axis=1)
        df_ep[['#viewers', 'avg. seconds']] = df_ep[['#viewers', 'avg. seconds']].round(0)

        df_ep = df_ep[['date', 'Description', 'Live_Rating', '#viewers', 'avg. seconds']]

        df_ep.to_excel(writer, sheet_name=show)
        worksheet = writer.sheets[show]
        for idx, col in enumerate(df_ep):
            series = df_ep[col]
            max_len = max((series.astype(str).map(len).max(), len(str(series.name)))) + 1
            worksheet.set_column(idx+1, idx+1, max_len)

    writer.save()


def generate_frame_eps():

    eps = ['Der Bachelor', 'Die Bachelorette', 'Bauer, ledig, sucht ...', 'Bumann, der Restauranttester']
    gather_metrics(eps)

    shows_list = list()
    filter_list = list()
    x = 0
    y = 1.00

    for show in eps:

        df = pd.read_excel(PATH + 'Metrics/plotly_metrics.xlsx', sheet_name=show, header=0)
        df = df.drop(columns=['Unnamed: 0']).reset_index(drop=True)

        filter_list.append(dict(label=show,
                                method='update',
                                args=[{'header': {'values': df.columns},
                                       "cells": {'values': df.T.values}},
                                      {'title': f'Stats of {show}',
                                       'width': 1900}
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
    y -= 0.05

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

    fig.update_layout(updatemenus=shows_list, dragmode='pan', height=1000, width=1900)

    div = plotly.offline.plot(fig, show_link=False, output_type="div", include_plotlyjs=True)

    with open(PATH + 'Metrics/plotly_table_div.txt', 'w') as f:
        f.write(div)


eps = ['Der Bachelor', 'Die Bachelorette', 'Bauer, ledig, sucht ...', 'Bumann, der Restauranttester']

gather_metrics(eps)
generate_frame_eps()
"""
for show in eps:
    generate_graphs_eps(show)
"""