import pandas as pd
from Airflow_Utils import Airflow_variables, pin_functions
import itertools
import numpy as np
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
STEAL_POT_PATH = var.steal_pot_path
CHANNELS = var.relevant_channels
threeplus = var.shows_3plus
fourplus = var.shows_4plus
fiveplus = var.shows_5plus
sixplus = var.shows_6plus
tvtwentyfour = var.shows_TV24
tvtwentyfive = var.shows_TV25
sone = var.shows_S1


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
                   .groupby(['Description', 'H_P', 'Title'])['duration_weighted'].sum()).to_frame().reset_index()
    if date is not None:
        df_filtered = df_filtered[df_filtered['Date'].isin(date)]
    return df_filtered


def filter_for_show(df, title):
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
    ranked_viewers_ah = (df.groupby('H_P')['duration_weighted'].sum().sort_values(ascending=False).index.tolist())

    vals = []
    tot = df['duration_weighted'].sum()
    for n_viewers in range(len(ranked_viewers_ah)):
        vals.append(df[(df['H_P'].isin(ranked_viewers_ah[:n_viewers]))]['duration_weighted'].sum() / tot)

    heavy_viewers = (df.groupby('H_P')['duration_weighted'].sum()
                     .sort_values(ascending=False)[:int(len(vals)*0.4)]
                     .index.tolist())

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
    """
    Analyse the heavy viewers of various shows of our group of interest.
    Compute the potential of stealing heavy viewers from each other
    :return: None
    """
    df_19_lv = pin_functions.get_live_facts_table()[0]
    df_19_lv = pin_functions.add_individual_ratings(df_19_lv)

    df_18_lv = pin_functions.get_older_facts_table()[0]
    df_18_lv = pin_functions.add_individual_ratings(df_18_lv)

    df_lv = pd.concat([df_19_lv, df_18_lv], axis=0)
    df_lv = df_lv.rename(columns={'individual_Rt-T_live': 'individual_Rt-T'})
    del df_18_lv
    del df_19_lv

    df_19_tsv = pin_functions.get_tsv_facts_table()[0]
    df_19_tsv = pin_functions.add_individual_ratings_ovn(df_19_tsv)

    df_18_tsv = pin_functions.get_older_facts_table()[1]
    df_18_tsv = pin_functions.add_individual_ratings_ovn(df_18_tsv)

    df_tsv = pd.concat([df_19_tsv, df_18_tsv], axis=0)
    df_tsv = df_tsv.rename(columns={'RecordingEndTime': 'EndTime',
                                    'RecordingStartTime': 'StartTime',
                                    'individual_Rt-T_tsv': 'individual_Rt-T'})
    df_tsv = df_tsv[df_lv.columns]
    del df_18_tsv
    del df_19_tsv

    df = pd.concat([df_lv, df_tsv], axis=0)
    del df_lv
    del df_tsv

    df['Date'] = df['show_starttime'].dt.date
    df['Date'] = pd.to_datetime(df['Date'], format='%Y-%m-%d')
    df['duration_weighted'] = df['duration'] * df['Weights']

    list_list_shows = []

    list_list_shows.extend(threeplus)
    list_list_shows.extend(fourplus)
    list_list_shows.extend(fiveplus)
    list_list_shows.extend(sixplus)
    list_list_shows.extend(tvtwentyfour)
    list_list_shows.extend(tvtwentyfive)
    list_list_shows.extend(sone)

    channel_df_dict = {}
    for channel in CHANNELS:
        df_temp = filter_for_channel(df, channel)
        channel_df_dict.update({channel: df_temp})

    combinations = itertools.combinations_with_replacement(list_list_shows, 2)
    df_shows = pd.DataFrame(columns=['show1', 'chan1', 'name1', 'show2', 'chan2', 'name2', 'steal_pot1', 'steal_pot2'])

    for shows in combinations:

        show_1 = shows[0]
        show_2 = shows[1]

        df_chan_1 = channel_df_dict.get(show_1[1])
        df_chan_2 = channel_df_dict.get(show_2[1])

        df_show_1 = filter_for_show(df_chan_1, show_1[0])
        df_show_2 = filter_for_show(df_chan_2, show_2[0])
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

        df_shows = pd.concat([df_shows, entry], axis=0, ignore_index=True, sort=True)

    df_shows = df_shows.reindex(sorted(df_shows.columns), axis=1)

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
    writer = pd.ExcelWriter(STEAL_POT_PATH + 'table_heavy_viewers_stealing.xlsx',
                            engine='xlsxwriter')
    results.to_excel(writer, sheet_name='report_hv')

    worksheet = writer.sheets['report_hv']

    for idx, col in enumerate(results, start=1):
        series = results[col]
        max_len = max((series.astype(str).map(len).max(), len(str(series.name)))) + 1
        worksheet.set_column(idx, idx, max_len)

    writer.save()
