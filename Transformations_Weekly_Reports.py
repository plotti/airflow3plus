import pandas as pd
import Pin_Functions
import Airflow_Variables
import itertools
import numpy as np
import datetime
from itertools import groupby
import pickle
import logging
from datetime import timedelta
import getpass
"""
Function defined in this file are used to automize the process of detecting conflict of interest concerning
heavy viewers of different shows. We try to diminish the competition of of our shows between each other, 
consequently we do not want to compete for the same heavy viewers during the same time slot
"""
Airflow_var = Airflow_Variables.AirflowVariables()
# Some global variables
YEAR = Airflow_var.year
MONTH = Airflow_var.month
DAY = Airflow_var.day
ADJUST_YEAR = Airflow_var.adjust_year
STEAL_POT_PATH = Airflow_var.steal_pot_path
HEATMAP_PATH = Airflow_var.heatmap_path
DROPBOX_PATH = Airflow_var.flask_path
CHANNELS = Airflow_var.relevant_channels + ['3+: First Runs']
CHANNELS_OF_INTEREST = Airflow_var.channels_of_interest + ['n-tv CH', 'Andere']
threeplus = Airflow_var.shows_3plus
fourplus = Airflow_var.shows_4plus
fiveplus = Airflow_var.shows_5plus
sixplus = Airflow_var.shows_6plus
tvtwentyfour = Airflow_var.shows_TV24
tvtwentyfive = Airflow_var.shows_TV25
sone = Airflow_var.shows_S1

list_EPs = [['Der Bachelor', '3+: First Runs'], ['Die Bachelorette', '3+: First Runs'],
            ['Adieu Heimat - Schweizer wandern aus', '3+: First Runs'],
            ['Bumann, der Restauranttester', '3+: First Runs'], ['Bauer, ledig, sucht ...', '3+: First Runs']]

name_EPs = ['Der Bachelor', 'Die Bachelorette', 'Adieu Heimat - Schweizer wandern aus',
            'Bauer, ledig, sucht ...', 'Bumann, der Restauranttester']

user = getpass.getuser()
dates_EPs = pd.read_pickle(f'/home/{user}/Dropbox (3 Plus TV Network AG)/3plus_ds_team/'
                           f'Projects/P38 Zapping sequences clustering (Heatmaps & more)/epdates_.pkl')


# ----------------------------------------------------------------------------------------------------------------------
# Create the HeavyViewers stealing potential excel sheet and generate the plotly table
# ----------------------------------------------------------------------------------------------------------------------
def filter_for_channel(df, channel, start_hour=20, end_hour=23, date=None, min_duration=60):
    """
    Filter for various options to specify a show and timeframe
    :param df: Dataframe to filter from
    :param channel: Channel of interest to filter the dataframe for
    :param start_hour: Integer of the earliest time to filter for [0,24]
    :param end_hour: Integer of the latest time to filter for, range [0,24]
    :param min_duration: Integer of seconds minimum watched of a show by the viewer, to reduce noice
    :param date: datetime.date in format YYYYmmdd
    :return: Dataframe filtered on every given aspect
    """
    df_filtered = (df[(df['station'] == channel)
                      & (df['show_starttime'].dt.hour >= start_hour)
                      & (df['show_endtime'].dt.hour <= end_hour)
                   & (df['duration'] > min_duration)]
                   .groupby(['Description', 'H_P', 'Title', 'date'])['duration_weighted'].sum()).\
        to_frame().reset_index()
    if date is not None:
        df_filtered = df_filtered[df_filtered['date'].isin(date)]
    return df_filtered


def filter_for_show(df, title, first_run=False):

    if title in name_EPs and first_run:
        dates = dates_EPs.get(title)
        trans_date = list()
        for date in dates:
            ts = pd.to_datetime(str(date))
            date = ts.strftime('%Y%d%m')
            trans_date.append(date)
        df = df[df['date'].isin(trans_date)]
        df_show = df[df['Title'] == title]
    else:
        df_show = df[df['Title'] == title]

    return df_show


def intersection_of_viewers(viewers_1, viewers_2):
    """
    Compute the intersection between two set of viewers
    :param viewers_1: Set of viewers
    :param viewers_2: Set of viewsers
    :return: A set of the resulting intersection of both sets of viewers
    """
    viewers_1 = set(viewers_1)
    viewers_2 = set(viewers_2)
    return viewers_1 & viewers_2


def compute_heavy_viewers(df):
    """
    Compute the heavy viewers based on the dataframe given and characteristic of a heavy viewer which are arbitrary
    chosen and not compelte yet.
    :param df: Dataframe of which we want to compute the heavy viewers, should be prefiltered
    :return: A list of the indexes for the viewers contributing the most to the rating of the show
    """

    show_df_sorted = df.groupby('H_P')['duration_weighted'].sum().sort_values(ascending=False)
    show_df_sorted = show_df_sorted / show_df_sorted.sum()
    heavy_viewers = show_df_sorted[show_df_sorted.cumsum() <= 0.9].index.to_list()

    return heavy_viewers


def compute_sum_duration_weighted(df, viewers=None):
    """
    Compute the sum of the weighted duration
    :param df: Dataframe to be filtered
    :param viewers: Optional only compute based on a subset of viewers
    :return: filtered dataframe
    """
    if viewers is not None:
        df = df[(df['H_P'].isin(viewers))]
    return df['duration_weighted'].sum()


def analyse_heavy_viewers():
    """
    Analyse the heavy viewers of various shows of our group of interest.
    Compute the potential of stealing heavy viewers from each other
    :return: None
    """
    df_20_lv = Pin_Functions.get_live_facts_table()[0]
    df_20_lv = Pin_Functions.add_individual_ratings(df_20_lv)

    df_19_lv = Pin_Functions.get_older_facts_table(year=2019)[0]
    df_19_lv = Pin_Functions.add_individual_ratings(df_19_lv)

    df_18_lv = Pin_Functions.get_older_facts_table(year=2018)[0]
    df_18_lv = Pin_Functions.add_individual_ratings(df_18_lv)

    df_lv = pd.concat([df_20_lv, df_19_lv, df_18_lv], axis=0)
    df_lv = df_lv.rename(columns={'individual_Rt-T_live': 'individual_Rt-T'})
    del df_18_lv
    del df_19_lv
    del df_20_lv

    df_20_tsv = Pin_Functions.get_tsv_facts_table()[0]
    df_20_tsv = Pin_Functions.add_individual_ratings_ovn(df_20_tsv)

    df_19_tsv = Pin_Functions.get_older_facts_table(year=2019)[1]
    df_19_tsv = Pin_Functions.add_individual_ratings_ovn(df_19_tsv)

    df_18_tsv = Pin_Functions.get_older_facts_table(year=2018)[1]
    df_18_tsv = Pin_Functions.add_individual_ratings_ovn(df_18_tsv)

    df_tsv = pd.concat([df_20_tsv, df_19_tsv, df_18_tsv], axis=0)
    df_tsv = df_tsv.rename(columns={'RecordingEndTime': 'EndTime',
                                    'RecordingStartTime': 'StartTime',
                                    'individual_Rt-T_tsv': 'individual_Rt-T'})
    df_tsv = df_tsv[df_lv.columns]
    del df_18_tsv
    del df_19_tsv
    del df_20_tsv

    df = pd.concat([df_lv, df_tsv], axis=0)
    del df_lv
    del df_tsv

    df['Date'] = df['show_starttime'].dt.date
    df['Date'] = pd.to_datetime(df['Date'], format='%Y-%m-%d')
    df['duration_weighted'] = df['duration'] * df['Weights']

    list_list_shows = []

    list_list_shows.extend(threeplus)
    list_list_shows.extend(list_EPs)
    list_list_shows.extend(fourplus)
    list_list_shows.extend(fiveplus)
    list_list_shows.extend(sixplus)
    list_list_shows.extend(tvtwentyfour)
    list_list_shows.extend(tvtwentyfive)
    list_list_shows.extend(sone)

    channel_df_dict = {}
    for channel in CHANNELS:
        if channel == '3+: First Runs':
            trans_date = list()
            for title in name_EPs:
                dates = dates_EPs.get(title)
                for date in dates:
                    ts = pd.to_datetime(str(date))
                    date = ts.strftime('%Y%d%m')
                    trans_date.append(date)
            df_temp = filter_for_channel(df, '3+', date=trans_date)
        else:
            df_temp = filter_for_channel(df, channel)
        channel_df_dict.update({channel: df_temp})

    combinations = itertools.combinations_with_replacement(list_list_shows, 2)
    df_shows = pd.DataFrame(columns=['show1', 'chan1', 'name1', 'show2', 'chan2', 'name2', 'steal_pot1', 'steal_pot2'])

    for shows in combinations:

        show_1 = shows[0]
        show_2 = shows[1]

        df_chan_1 = channel_df_dict.get(show_1[1])
        df_chan_2 = channel_df_dict.get(show_2[1])

        if show_1[1] == '3+: First Runs':
            df_show_1 = filter_for_show(df_chan_1, show_1[0], first_run=True)
        else:
            df_show_1 = filter_for_show(df_chan_1, show_1[0], first_run=False)

        if show_2[1] == '3+: First Runs':
            df_show_2 = filter_for_show(df_chan_2, show_2[0], first_run=True)
        else:
            df_show_2 = filter_for_show(df_chan_2, show_2[0], first_run=False)

        if df_show_2.empty or df_show_1.empty:
            continue

        hv_show_1 = compute_heavy_viewers(df_show_1)
        hv_show_2 = compute_heavy_viewers(df_show_2)
        shared_viewers = intersection_of_viewers(hv_show_1, hv_show_2)

        scalar_show_1 = compute_sum_duration_weighted(df_show_1, shared_viewers)
        scalar_show_2 = compute_sum_duration_weighted(df_show_2, shared_viewers)

        res_show_1 = compute_sum_duration_weighted(df_show_1)
        res_show_2 = compute_sum_duration_weighted(df_show_2)

        name1 = "{} {}".format(show_1[1], show_1[0])
        name2 = "{} {}".format(show_2[1], show_2[0])

        entry = pd.DataFrame(data={'show1': show_1[0], 'chan1': show_1[1], 'name1': name1,
                                   'show2': show_2[0], 'chan2': show_2[1], 'name2': name2,
                                   'steal_pot1': scalar_show_1/res_show_1, 'steal_pot2': scalar_show_2/res_show_2},
                             index=[0])

        df_shows = pd.concat([df_shows, entry], axis=0, ignore_index=True, sort=False)

    df_shows = df_shows.reindex(df_shows.columns, axis=1)

    df_reg = pd.pivot_table(data=df_shows, index=['chan1', 'show1'], columns='name2', values='steal_pot1')
    df_reg = df_reg.fillna(0)
    np.fill_diagonal(df_reg.values, 1)

    df_trans = pd.pivot_table(data=df_shows, index=['chan2', 'show2'], columns='name1', values='steal_pot2')
    df_trans = df_trans.fillna(0)
    np.fill_diagonal(df_trans.values, 0)

    results = 100*(df_reg + df_trans)
    results.index.names = ['channel', 'show']
    results = results.reset_index(drop=False)
    results = results.round(decimals=2)

    writer = pd.ExcelWriter(DROPBOX_PATH + 'Heavy_Viewers/' + 'table_heavy_viewers_stealing.xlsx', engine='xlsxwriter')
    results.to_excel(writer, sheet_name='report_hv')

    worksheet = writer.sheets['report_hv']

    for idx, col in enumerate(results, start=1):
        series = results[col]
        max_len = max((series.astype(str).map(len).max(), len(str(series.name)))) + 1
        worksheet.set_column(idx, idx, max_len)

    writer.save()


# ----------------------------------------------------------------------------------------------------------------------
# Update the Heatmap for the Dash application
# ----------------------------------------------------------------------------------------------------------------------
def get_live_zapping(df_usagelive_df, start_time, end_time, date):
    """
    Get the zapping from the viewers.
    :param df_usagelive_df: DataFrame of a given day and timeslot
    :param start_time: Starttime of observation
    :param end_time: Endtime of observation
    :param date: Day of the observation
    :return: Dataframe of the zapping
    """
    station_dict = Pin_Functions.get_station_dict()

    df_usagelive = df_usagelive_df.copy()
    ud = df_usagelive['UsageDate'].iloc[0]
    start_time = pd.Timestamp.combine(ud, datetime.datetime.strptime(start_time, "%H%M%S").time())
    end_time = pd.Timestamp.combine(ud, datetime.datetime.strptime(end_time, "%H%M%S").time())
    cond = (df_usagelive['EndTime'] >= start_time) & (df_usagelive['StartTime'] <= end_time)

    df_usagelive = df_usagelive[cond]

    df_usagelive['EndTime'].clip(upper=end_time, inplace=True)
    df_usagelive['StartTime'].clip(lower=start_time, inplace=True)

    df_usagelive = df_usagelive.sort_values(by="StartTime")
    df_usagelive['StationId'] = df_usagelive['StationId'].map(station_dict)

    # Grouping the potentially fragmented usage of viewers
    ret = df_usagelive[['H_P', 'StationId', 'StartTime', 'EndTime']].groupby('H_P')

    # Flagging people already watching at StartTime
    watched_at_st = ret['StartTime'].min().apply(lambda x: x.strftime("%H%M%S")) == start_time.strftime("%H%M%S")
    # Flagging people still watching at EndTime
    watched_at_et = ret['EndTime'].max().apply(lambda x: x.strftime("%H%M%S")) == end_time.strftime("%H%M%S")

    ret = ret['StationId'].apply(list)
    ret.loc[watched_at_st] = ret.loc[watched_at_st].apply(lambda x: ['start_pt'] + x)
    ret.loc[~watched_at_st] = ret.loc[~watched_at_st].apply(lambda x: ['off'] + x)
    ret.loc[watched_at_et] = ret.loc[watched_at_et].apply(lambda x: x + ['end_pt'])
    ret.loc[~watched_at_et] = ret.loc[~watched_at_et].apply(lambda x: x + ['off'])
    ret = ret.apply(lambda x: [a[0] for a in groupby(x)])
    ret = ret[ret.apply(lambda x: len(x)) > 5]
    ret.name = date

    return ret


def instances_of_zapping(df, seq, l):
    """
    Counts the instances of zapping in the given DataFrame
    :param df: Dataframe of zapping
    :param seq:
    :param l: History of previously visited channels
    :return: Dataframe of the counted zappings
    """
    idx = df.index.tolist()
    for ix, el in enumerate(seq[l:]):
        if (el in idx) & (seq[ix] in idx):
            df.loc[el, seq[ix]] += 1
        elif (el not in idx) & (seq[ix] in idx):
            df.loc['Andere', seq[ix]] += 1
        elif (el in idx) & (seq[ix] not in idx):
            df.loc[el, 'Andere'] += 1
        else:
            df.loc['Andere', 'Andere'] += 1

    return df


def get_zapping_list_by_date(d, df):
    """
    Zapping vector of 1 year of watching
    :param d:
    :param df: DataFrame of the instance
    :return: Vector of the zapping sequence of the viewer
    """
    pers_hops = df.loc[:, d].dropna().tolist()
    pers_hops = [o for subo in pers_hops for o in subo]
    return pers_hops


def update_heatmap(date, threshold_duration=False):
    """
    Updated the heatmap and saves it to a pickle file
    :param date: Day to update
    :param threshold_duration: If the threshold of watching duration should be respected
    :return: None
    """
    daily_hops_list = list()
    starts = ['180000', '190000', '200000', '210000', '220000', '230000']
    ends = ['190000', '200000', '210000', '220000', '230000', '235900']
    timeslots = ['18', '19', '20', '21', '22', '23']

    liv = Pin_Functions.get_live_viewers(date=date,  agemin=15, agemax=49)
    if threshold_duration:
        liv = liv[liv['EndTime'] - liv['StartTime'] > timedelta(seconds=30)]

    for start, end in zip(starts, ends):

        daily_hop = list()
        daily_hop.append(get_live_zapping(liv.copy(), start, end, date))
        daily_hops = pd.concat(daily_hop, axis=1)
        daily_hops_list.append(daily_hops)

    daily_zap_dict = {}
    for daily_hops, slot in zip(daily_hops_list, timeslots):
        daily_hops[daily_hops.applymap(lambda x: x if x != x else len(x)) == 1.0] = np.NaN
        for d in daily_hops.columns:
            format_ = slot
            dz = get_zapping_list_by_date(d, daily_hops)
            d_f = pd.DataFrame(0, index=CHANNELS_OF_INTEREST, columns=CHANNELS_OF_INTEREST)
            d_f = instances_of_zapping(d_f, dz, 1)
            daily_zap_dict[d + format_] = d_f

    if threshold_duration:
        with open(HEATMAP_PATH + 'data_heatmap_new_PIN.pkl', 'wb') as f:
            pickle.dump(daily_zap_dict, f)

        with open(HEATMAP_PATH + 'data_heatmap_chmedia_threshold.pkl', 'rb') as f:
            data_new_heatmap = pickle.load(f)

        data_new_heatmap = {**data_new_heatmap, **daily_zap_dict}

        with open(HEATMAP_PATH + 'data_heatmap_chmedia_threshold.pkl', 'wb') as f:
            pickle.dump(data_new_heatmap, f)

        with open(DROPBOX_PATH + 'Heatmap/' + 'data_heatmap_chmedia_threshold.pkl', 'wb') as f:
            pickle.dump(data_new_heatmap, f)

    else:
        with open(HEATMAP_PATH + 'data_heatmap_new_PIN.pkl', 'wb') as f:
            pickle.dump(daily_zap_dict, f)

        with open(HEATMAP_PATH + 'data_heatmap_chmedia.pkl', 'rb') as f:
            data_new_heatmap = pickle.load(f)

        data_new_heatmap = {**data_new_heatmap, **daily_zap_dict}

        with open(HEATMAP_PATH + 'data_heatmap_chmedia.pkl', 'wb') as f:
            pickle.dump(data_new_heatmap, f)

        with open(DROPBOX_PATH + 'Heatmap/' + 'data_heatmap_chmedia.pkl', 'wb') as f:
            pickle.dump(data_new_heatmap, f)


def compute_heatmap_whole_year():
    """
    Recompute the heatmap for the whole year and saves it as a pickle file
    :return: None
    """
    with open(HEATMAP_PATH + 'data_heatmap_chmedia_threshold.pkl', 'wb') as f:
        pickle.dump({}, f)

    cond = False

    end = str(YEAR) + str(MONTH) + str(DAY)
    start = str(YEAR) + '0101'
    if ADJUST_YEAR < 0:
        end = str(YEAR) + '1231'

    date = datetime.datetime.strptime(start, '%Y%m%d')

    logging.info('Will update from date %s' % date)
    while True:
        logging.info('Updating still ongoing from date %s' % date)

        for i in range(7):
            add = date.strftime('%Y%m%d')
            if add == end:
                cond = True
                try:
                    update_heatmap(add, threshold_duration=False)
                except FileNotFoundError as e:
                    logging.info(str(e))
                    date = date + timedelta(days=1)
                    break
            try:
                update_heatmap(add, threshold_duration=False)
            except FileNotFoundError as e:
                logging.info(str(e))
                cond = True
                date = date + timedelta(days=1)
                break
            date = date + timedelta(days=1)

        if cond:
            logging.info("Reached the end date successfully, finished live table")
            break


# ----------------------------------------------------------------------------------------------------------------------
