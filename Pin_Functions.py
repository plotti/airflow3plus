import datetime
import pandas as pd
import logging
import os
import Airflow_Variables
import calendar

from datetime import timedelta, datetime
from collections import Counter
"""
These pin functions are used to compute both facts table, meaning the live facts table and the time shifted table
A logical flow of the computation can be found above each block which are responsible for the respective 
computation. In the first part of the file many getter functions are defined to improve the overall structure 
of the computation.
The correctness of the computation has been verified with comparision with values from infosys. Some unexplained 
operations are direct consequence of infosys data architecture, as an example the added second to compute the rating.
"""
Airflow_var = Airflow_Variables.AirflowVariables()
# Some global variables
LOCAL_PATH = Airflow_var.local_path
SOL_PATH = Airflow_var.verify_path
VORWOCHE_PATH = Airflow_var.vorwoche_path
DROPBOX_PATH = Airflow_var.dropbox_path
DAYS_IN_YEAR = Airflow_var.days_in_year
TSV_DAYS = Airflow_var.days_tsv
ADJUST_YEAR = Airflow_var.adjust_year
TABLES_PATH = Airflow_var.table_viewers_path
CHANNELS = Airflow_var.relevant_channels
YEAR = Airflow_var.year
MONTH = Airflow_var.month
DAY = Airflow_var.day
START = Airflow_var.start
END_DAY = Airflow_var.end


# ----------------------------------------------------------------------------------------------------------------------
# GENERAL PURPOSE FILE OPENERS AND DICTIONARIES
# Getter/ Helper functions for facts tables aggregation and computation
# ----------------------------------------------------------------------------------------------------------------------
def get_live_facts_table():
    """
    Returns the current version of the facts table for live viewing
    """
    date_cols = ['show_endtime', 'show_starttime', 'StartTime', 'EndTime']
    END = datetime.strptime(END_DAY, '%Y%m%d')

    df = pd.DataFrame()
    i = 0
    for i in range(DAYS_IN_YEAR):
        date_old = (END - timedelta(days=i)).strftime('%Y%m%d')
        if date_old == START:
            break
        if os.path.isfile(LOCAL_PATH + '%s_%s_Live_DE_15_49_mG.csv' % (START, date_old)):
            df = pd.read_csv(LOCAL_PATH + '%s_%s_Live_DE_15_49_mG.csv' % (START, date_old), parse_dates=date_cols,
                             dtype={'Description': str, 'H_P': str, 'Kanton': int, 'Title': str, 'Weights': float,
                                    'broadcast_id': int, 'date': str, 'duration': float, 'program_duration': int,
                                    'station': str})
            break
    return df, i


def get_tsv_facts_table():
    """
    Returns the current version of the facts table for time shifted viewing
    """
    delayed_date_cols = ['ViewingStartTime', 'ViewingTime', 'RecordingStartTime', 'show_endtime',
                         'show_starttime', 'RecordingEndTime']
    END = datetime.strptime(END_DAY, '%Y%m%d')

    df = pd.DataFrame()
    i = 0
    for i in range(DAYS_IN_YEAR):
        date_old = (END - timedelta(days=i)).strftime('%Y%m%d')
        if date_old == START:
            break
        if os.path.isfile(LOCAL_PATH + '%s_%s_delayedviewing_DE_15_49_mG.csv' % (START, date_old)):
            df = pd.read_csv(LOCAL_PATH + '%s_%s_delayedviewing_DE_15_49_mG.csv' % (START, date_old),
                             parse_dates=delayed_date_cols, dtype={'Description': str, 'H_P': str,
                                                                   'HouseholdId': float, 'IndividualId': float,
                                                                   'Kanton': float, 'Platform': float, 'StationId': float,
                                                                   'Title': str, 'TvSet': float, 'Weights': float,
                                                                   'UsageDate': float, 'ViewingActivity': float,
                                                                   'age': float, 'broadcast_id': float, 'date': str,
                                                                   'duration': float, 'program_duration': float,
                                                                   'station': str})
            break
    return df, i


def get_older_facts_table(year):
    """
    Returns the facts-table for older years, 2018 and 2017 are available
    """
    try:
        date_cols = ['show_endtime', 'show_starttime', 'StartTime', 'EndTime']
        df_lv = pd.read_csv(LOCAL_PATH + '%s0101_%s1231_Live_DE_15_49_mG.csv' % (year, year), parse_dates=date_cols,
                            dtype={'Description': str, 'H_P': str, 'Kanton': int, 'Title': str, 'Weights': float,
                                   'broadcast_id': int, 'date': str, 'duration': float, 'program_duration': int,
                                   'station': str})
    except FileNotFoundError as e:
        print(f'Live facts table for the year {year} does not exist or is saved at a different path as given')
        df_lv = pd.DataFrame()

    try:
        delayed_date_cols = ['ViewingStartTime', 'ViewingTime', 'RecordingStartTime', 'show_endtime',
                             'show_starttime', 'RecordingEndTime']
        df_tsv = pd.read_csv(LOCAL_PATH + '%s0101_%s1231_delayedviewing_DE_15_49_mG.csv' % (year, year),
                             parse_dates=delayed_date_cols,
                             dtype={'Description': str, 'H_P': str, 'HouseholdId': int, 'IndividualId': int,
                                    'Kanton': int, 'Platform': int, 'StationId': int, 'Title': str, 'TvSet': int,
                                    'Weights': float, 'UsageDate': int, 'ViewingActivity': int, 'age': int,
                                    'broadcast_id': int, 'date': str, 'duration': float, 'program_duration': int,
                                    'station': str})

    except FileNotFoundError as e:
        print(f'Tsv facts table for the year {year} does not exist or is saved at a different path as given')
        df_tsv = pd.DataFrame()

    return df_lv, df_tsv


def get_dropbox_facts_table(year=None):
    """
    Get the facts table from dropbox
    :param year: year of the wishing
    :return: both tables
    """
    try:
        date_cols = ['show_endtime', 'show_starttime', 'StartTime', 'EndTime']
        if year is None:
            df_lv = pd.read_csv(DROPBOX_PATH + 'updated_live_facts_table.csv',
                                parse_dates=date_cols,
                                dtype={'Description': str, 'H_P': str, 'Kanton': int, 'Title': str, 'Weights': float,
                                       'broadcast_id': int, 'date': str, 'duration': float, 'program_duration': int,
                                       'station': str})
        else:
            df_lv = pd.read_csv(DROPBOX_PATH + '%s0101_%s1231_Live_DE_15_49_mG.csv' % (year, year),
                                parse_dates=date_cols,
                                dtype={'Description': str, 'H_P': str, 'Kanton': float, 'Title': str, 'Weights': float,
                                       'broadcast_id': float, 'date': str, 'duration': float, 'program_duration': float,
                                       'station': str})
    except FileNotFoundError as e:
        print(f'Live facts table does not exist on dropbox or is saved at a different path as given')
        df_lv = pd.DataFrame()

    try:
        delayed_date_cols = ['ViewingStartTime', 'ViewingTime', 'RecordingStartTime', 'show_endtime',
                             'show_starttime', 'RecordingEndTime']
        if year is None:
            df_tsv = pd.read_csv(DROPBOX_PATH + 'updated_tsv_facts_table.csv',
                                 parse_dates=delayed_date_cols,
                                 dtype={'Description': str, 'H_P': str, 'HouseholdId': float, 'IndividualId': float,
                                        'Kanton': float, 'Platform': float, 'StationId': int, 'Title': str, 'TvSet': float,
                                        'Weights': float, 'UsageDate': float, 'ViewingActivity': float, 'age': float,
                                        'broadcast_id': float, 'date': str, 'duration': float, 'program_duration': float,
                                        'station': str, 'RecordingDate': str})
        else:
            df_tsv = pd.read_csv(DROPBOX_PATH + '%s0101_%s1231_delayedviewing_DE_15_49_mG.csv' % (year, year),
                                 parse_dates=delayed_date_cols,
                                 dtype={'Description': str, 'H_P': str, 'HouseholdId': float, 'IndividualId': int,
                                        'Kanton': int, 'Platform': int, 'StationId': int, 'Title': str, 'TvSet': int,
                                        'Weights': float, 'UsageDate': int, 'ViewingActivity': int, 'age': float,
                                        'broadcast_id': int, 'date': str, 'duration': float, 'program_duration': int,
                                        'station': str, 'RecordingDate': str})

    except FileNotFoundError as e:
        print(f'Tsv facts table does not exist on dropbox or is saved at a different path as given')
        df_tsv = pd.DataFrame()

    return df_lv, df_tsv


def get_gender_dict(date):
    """
    Return viewers gender on a given date
    :param date: Day of interest, string
    :return: Dict of the viewers gender
    """
    with open(f'{LOCAL_PATH}SocDem/SocDem_{date}.pin', 'r', encoding='latin-1') as f:
        df_socdem= pd.read_csv(f, dtype='int32', usecols=['SampleId', 'Person', 'SocDemVal2'])

    df_socdem['H_P'] = df_socdem['SampleId'].astype(str) + "_" + df_socdem['Person'].astype(str)
    gender_dict = {a: b for a, b in zip(df_socdem['H_P'].values.tolist(),
                                        df_socdem['SocDemVal2'].values.tolist())}
    return gender_dict


def get_kanton_dict(date):
    """
    Returns viewer's Kanton on a given date
    :param date: Day of interest as string
    :return: Dict of the viewers kanton
    """
    with open(f'{LOCAL_PATH}SocDem/SocDem_{date}.pin', 'r', encoding='latin-1') as f:
        df_socdem = pd.read_csv(f, dtype='int32', usecols=['SampleId', 'Person', 'SocDemVal5'])

    df_socdem['H_P'] = df_socdem['SampleId'].astype(str) + "_" + df_socdem['Person'].astype(str)
    kanton_dict = {a: b for a, b in zip(df_socdem['H_P'].values.tolist(),
                                        df_socdem['SocDemVal5'].values.tolist())}
    return kanton_dict


def get_station_dict():
    """
    Get all the stations currently available with their respective Id
    :return: Dict of the stations and their Ids
    """""
    with open(f'{LOCAL_PATH}Station.pin', 'r', encoding='latin-1') as f:
        df_sta = pd.read_csv(f)

    return {k: v for k, v in zip(df_sta['StationID'].tolist(), df_sta['StationAbbr'].tolist())}


def get_lang_dict(date):
    """
    Viewer's language on a given date
    :param date: Day of interest
    :return: Dictionary of the spoken language of each viewer
    """
    with open(f'{LOCAL_PATH}SocDem/SocDem_{date}.pin', 'r', encoding='latin-1') as f:
        df_socdem = pd.read_csv(f, dtype='int32', usecols=['SampleId', 'Person', 'SocDemVal4'])

    df_socdem['H_P'] = df_socdem['SampleId'].astype(str) + "_" + df_socdem['Person'].astype(str)
    lang_dict = {a: b for a, b in zip(df_socdem['H_P'].values.tolist(),
                                      df_socdem['SocDemVal4'].values.tolist())}
    return lang_dict


def get_weight_dict(date):
    """
    Returns viewer's weight on a given date in form of a dict (H_P: Weight)
    :param date: Day of interest
    :return: Dictionary of the viewers Id and the respective weight
    """
    with open(f'{LOCAL_PATH}Weight/Weight_{date}.pin', 'r', encoding='latin-1') as f:
        df_wei = pd.read_csv(f)

    df_wei['H_P'] = df_wei['SampledIdRep'].astype(str) + "_" + df_wei['PersonNr'].astype(str)
    weight_dict = {a: b for a, b in zip(df_wei['H_P'].values.tolist(),
                                        df_wei['PersFactor'].values.tolist())}
    return weight_dict


def get_weight_dict_ovn(date):
    """
    Makes a dict of the weights of the previous seven days with keys of the form (H_P, Date)
    Used for a correct computation of the tsv Rt-T
    :param date: Day of interest
    :return: A complete dictionary of the weights of the last seven days for every viewer
    """
    weight_dict = {}
    date = datetime.strptime(date, '%Y%m%d')

    for i in range(TSV_DAYS):

        with open(f'{LOCAL_PATH}Weight/Weight_{(date-timedelta(days=i)).strftime("%Y%m%d")}.pin',
                  'r', encoding='latin-1') as f:
            df_wei = pd.read_csv(f)

        df_wei['H_P'] = df_wei['SampledIdRep'].astype(str) + "_" + df_wei['PersonNr'].astype(str)
        temp_dict = {(a, c): b for a, c, b in zip(df_wei['H_P'].values.tolist(), df_wei['WghtDate'].values.tolist(),
                                                  df_wei['PersFactor'].values.tolist())}
        weight_dict.update(temp_dict)

    return weight_dict


def get_age_dict(date):
    """
    Viewer's age on a given date"
    :param date: Day of interest
    :return: A dictionary of the age of each viewer
    """
    with open(f'{LOCAL_PATH}SocDem/SocDem_{date}.pin', 'r', encoding='latin-1') as f:
        df_socdem = pd.read_csv(f, dtype='int32', usecols=['SampleId', 'Person', 'SocDemVal1'])

    df_socdem['H_P'] = df_socdem['SampleId'].astype(str) + "_" + df_socdem['Person'].astype(str)
    age_dict = {a: b for a, b in zip(df_socdem['H_P'].values.tolist(),
                                     df_socdem['SocDemVal1'].values.tolist())}
    return age_dict


def get_age_dict_ovn(date):
    """
    Get the age dictionary for ovn viewing. Only implemeted because someone could have birthday between
    airing date and watching date.
    :param date: Day of interest
    :return: dict of the age with key (H_P, date)
    """
    age_dict = {}
    date = datetime.strptime(date, '%Y%m%d')

    for i in range(TSV_DAYS):
        with open(f'{LOCAL_PATH}SocDem/SocDem_{(date - timedelta(days=i)).strftime("%Y%m%d")}.pin',
                  'r', encoding='latin-1') as f:
            df_socdem = pd.read_csv(f, dtype='int32', usecols=['FileDate', 'SampleId', 'Person', 'SocDemVal1'])

        df_socdem['H_P'] = df_socdem['SampleId'].astype(str) + "_" + df_socdem['Person'].astype(str)
        temp_dict = {(a, str(c)): b for a, c, b in zip(df_socdem['H_P'].values.tolist(),
                                                       df_socdem['FileDate'].values.tolist(),
                                                       df_socdem['SocDemVal1'].values.tolist())}
        age_dict.update(temp_dict)
    return age_dict


def get_brc(date):
    """
    Aggregates the broadcast file of 'date' with right dtypes
    assigned. Adds 1 day to the StarTime id the threshold of 2am isn't reached.
    Also adds the segmented duration of the show together to compute the full duration of the time
    watched.
    :param date: Day of interst
    :return: pd.Dataframe of the broadcast schedule of the day
    """
    with open(f'{LOCAL_PATH}BrdCst/BrdCst_{date}.pin', 'r', encoding='latin-1') as f:
        brc = pd.read_csv(f, dtype={'Date': 'object', 'StartTime': 'object'}, error_bad_lines=False)

    # Padding time to 6 digits and to datetime
    brc['StartTime'] = brc['Date'] + brc['StartTime'].str.zfill(6)
    brc['StartTime'] = pd.to_datetime(brc['StartTime'], format='%Y%m%d%H%M%S')

    # Flagging data belonging to the next day, adding 1 day to dates
    new_day_tresh = pd.to_datetime("020000", format='%H%M%S').time()
    brc['add_day'] = (brc['StartTime'].dt.time < new_day_tresh).astype(int)
    brc['StartTime'] += pd.to_timedelta(brc['add_day'], 'd')

    # Getting end time from start and duration
    brc['EndTime'] = brc['StartTime'] + pd.to_timedelta(brc['Duration'], 's')

    # Adds the segmented broadcast together
    segmented_prog_duration_dict = (brc[(brc['SumPieces'] > 0) & (brc['BrdCstSeq'] > 0)]
                                    .groupby(['BrdCstId'])['Duration'].sum().to_dict())
    single_prog_duration_dict = (brc[(brc['SumPieces'] == 0) & (brc['BrdCstSeq'] == 0)]
                                 .groupby(['BrdCstId'])['Duration'].nth(0).to_dict())

    prog_duration_dict = dict((Counter(segmented_prog_duration_dict) + Counter(single_prog_duration_dict)))

    brc = brc[~((brc['SumPieces'] > 0) & (brc['BrdCstSeq'] == 0))]
    brc['Duration'] = brc['BrdCstId'].map(prog_duration_dict)

    return brc


def get_live_viewers_from_show(show, df_live):
    """
    Filters 'live_df' to keep only the viewers of 'show'
    Maps a viewer to the show he/she watches.
    :param show: pd.Series of all shows run in the given broadcast
    :param df_live: pd.Dataframe of the liveviewers of the given Usagefile
    :return: pd.Dataframe of the shows watched of each viewer
    """
    df_live = df_live[df_live['station'] == show[0][7]].copy()
    df_live['duration'] = (df_live['EndTime'].clip(upper=show[0][3])
                           - df_live['StartTime'].clip(lower=show[0][2])).dt.total_seconds()
    df_live = df_live[df_live['duration'] > 0]

    return df_live


def get_ovn_viewers_from_show(show, df):
    """
    Filters 'df' to keep only the viewers of 'show'
    Maps a viewer to the show she/he watchers.
    :param show: pd.Series of the broadcast aired in a certain time
    :param df: pd.Dataframe of the viewers watching TV on a given date
    :return: pd.Dataframe of the shows watched of the viewers
    """
    df = df[df['station'] == show[0][7]].copy()
    df['duration'] = (df['RecordingEndTime'].clip(upper=show[0][3])
                      - df['RecordingStartTime'].clip(lower=show[0][2])).dt.total_seconds()
    df = df[df['duration'] >= 0]

    return df


# ----------------------------------------------------------------------------------------------------------------------
# PREPROCESSING OF TIME SHIFTED DATA
# Flow:
# 1. get_tsv_viewers. -> all shifted usage from a date
# 2. get_brc for 7 days before tsv date -> all programs that ran in the last 7 days
# 3. map_viewers_ovn. -> maps usage to shows, from the 2 above files
# 4. df_to_disk_ovn. -> saves to disk. Create Dataframe for update
# ----------------------------------------------------------------------------------------------------------------------
def map_viewers_ovn(sched, ovn_df):
    """
    Maps viewer's time shifted usage to shows. Filter for unique values to
    remove channels with 0 viewers. Iterate over every show aired.
    :param sched: pd.Dataframe of the schedule of the given day
    :param ovn_df: pd.Dataframe of the TV-watchers on the given day
    :return: A pd.Dataframe of the shows watched of each viewer
    """
    out_viewers = []
    out_shows = []

    delete = ovn_df.station.unique()
    sched = sched[sched['station'].isin(delete)]

    for show in zip(sched.values):
        viewers = get_ovn_viewers_from_show(show, ovn_df)
        if len(viewers) > 0:
            out_viewers.append(viewers)
            out_shows.append(show)

    return out_viewers, out_shows


def get_tsv_viewers(date, agemin, agemax):
    """
    Computes all the viewers who watched time-shifted and maps the right
    weights to the viewers. Some filtering is done to gain the information
    of the target group.
    :param date: Day on interest
    :param agemin: Minimum age of the viewer, filtering for group of interest
    :param agemax: Maximum age of the viewer, filtering for group of interest
    :return: pd.Dataframe of all the viewers who watched TV shows time-shifted
    """
    date_cols = ['UsageDate', 'RecordingDate']
    time_cols = ['ViewingStartTime', 'ViewingTime', 'RecordingStartTime']

    with open(f'{LOCAL_PATH}UsageTimeShifted/UsageTimeShifted_{date}.pin', 'r', encoding='latin-1') as f:
        df = pd.read_csv(f, dtype={**{c: 'object' for c in date_cols},
                                   **{c: 'object' for c in time_cols}})

    # Filtering out TSV activity
    cond0 = df['ViewingActivity'].isin([4, 10])
    df = df[cond0]

    # Combining Id's
    df['H_P'] = df['HouseholdId'].astype(str) + "_" + df['IndividualId'].astype(str)

    # Filtering out based on ages
    df['age'] = df[['H_P', 'RecordingDate']].T.apply(tuple).map(get_age_dict_ovn(date))
    df = df[(df['age'] >= agemin) & (df['age'] <= agemax)]

    # Filtering on language
    df = df[df['H_P'].map(get_lang_dict(date)) == 1]

    # Assigning the right dtypes to date cols
    for tc in time_cols[:2]:
        df[tc] = pd.to_datetime(df['UsageDate'] + df[tc].str.zfill(6), format="%Y%m%d%H%M%S")

    # Adding additional day based on the threshold, 2am problem on ViewingStart-End and RecordStartTime
    new_day_thresh = pd.to_datetime('020000', format='%H%M%S').time()
    df.loc[df['ViewingStartTime'].dt.time < new_day_thresh, 'ViewingStartTime'] += pd.to_timedelta('1 Days')
    df.loc[df['ViewingTime'].dt.time <= new_day_thresh, 'ViewingTime'] += pd.to_timedelta('1 Days')

    df['duration'] = (df['ViewingTime'] - df['ViewingStartTime']).dt.total_seconds()
    df['RecordingStartTime'] = pd.to_datetime(df['RecordingDate'] + df['RecordingStartTime'].str.zfill(6),
                                              format="%Y%m%d%H%M%S")

    df.loc[df['RecordingStartTime'].dt.time < new_day_thresh, 'RecordingStartTime'] += pd.to_timedelta('1 Days')
    df['RecordingEndTime'] = df['RecordingStartTime'] + pd.to_timedelta(df['duration'], unit='s')
    df['RecordingDate'] = pd.to_datetime(df['RecordingDate'], format='%Y%m%d')

    # Mapping weights and kanton from the day of the recording
    df['RecordingDate'] = df['RecordingDate'].dt.strftime('%Y%m%d').astype(int)
    df['Weights'] = df[['H_P', 'RecordingDate']].T.apply(tuple).map(get_weight_dict_ovn(date))
    df['Kanton'] = df['H_P'].map(get_kanton_dict(date))

    return df


def df_to_disk_ovn(vw, sh, date):
    """
    Adjusts the format of the Dataframe before we can concatenate the table with the
    existing facts table
    :param vw: pd.Dataframe of the time-shifted viewers
    :param sh: pd.Dataframe of the schedule of the show of the given date
    :param date: Day added to the dataframe for easier filtering if entries of the
    facts table has to be deleted
    :return: pd.Dataframe ready to be concatenated to the facts table
    """
    lens = [[len(l) for l in ls] for ls in vw]
    lens = [o for subo in lens for o in subo]

    viewers = [[l for l in ls] for ls in vw]
    viewers = [o for subo in viewers for o in subo]
    viewers = pd.concat(viewers, axis=0, ignore_index=False)

    tits = [[l[0][1] for l in ls] for ls in sh]
    tits = [o for subo in tits for o in subo]
    titls = [[o] * l for o, l in zip(tits, lens)]
    titls = [o for subo in titls for o in subo]

    viewers['Title'] = titls

    utits = [[l[0][5] for l in ls] for ls in sh]
    utits = [o for subo in utits for o in subo]
    utitls = [[o] * l for o, l in zip(utits, lens)]
    utitls = [o for subo in utitls for o in subo]

    viewers['Description'] = utitls

    prog_dur = [[l[0][6] for l in ls] for ls in sh]
    prog_dur = [o for subo in prog_dur for o in subo]
    prog_durs = [[o] * l for o, l in zip(prog_dur, lens)]
    prog_durs = [o for subo in prog_durs for o in subo]

    viewers['program_duration'] = prog_durs

    end = [[l[0][3] for l in ls] for ls in sh]
    end = [o for subo in end for o in subo]
    ends = [[o] * l for o, l in zip(end, lens)]
    ends = [o for subo in ends for o in subo]

    viewers['show_endtime'] = ends

    start = [[l[0][2] for l in ls] for ls in sh]
    start = [o for subo in start for o in subo]
    starts = [[o] * l for o, l in zip(start, lens)]
    starts = [o for subo in starts for o in subo]

    viewers['show_starttime'] = starts

    bid = [[l[0][4] for l in ls] for ls in sh]
    bid = [o for subo in bid for o in subo]
    bids = [[o] * l for o, l in zip(bid, lens)]
    bids = [o for subo in bids for o in subo]

    viewers['broadcast_id'] = bids
    viewers['date'] = date

    return viewers


def update_tsv_facts_table(dates):
    """
    Main function to update the time-shifted facts table
    :param dates: Series of int dates which have to be updated on the table
    :return: None
    """
    df_old, i = get_tsv_facts_table()
    END = datetime.strptime(END_DAY, '%Y%m%d')

    # Remove updated entries from the old file
    if not df_old.empty and ADJUST_YEAR >= 0:
        # Check if older entries exist of files which are present, otherwise update them
        for k in range(DAYS_IN_YEAR):
            date = (END - timedelta(days=i+k)).strftime('%Y%m%d')
            if date == START:
                break
            if date not in df_old['date'].values:
                dates.update([str(date)])

        for date_remove in dates:
            df_old = df_old[df_old['date'] != str(date_remove)]

    if not dates:
        logging.info('No dates to update found, exiting execution')
        exit()

    logging.info('Following dates will be updated %s' % dates)

    stations = get_station_dict()
    df_update = pd.DataFrame()

    for date in dates:

        date = str(date)
        date = datetime(year=int(date[0:4]), month=int(date[4:6]), day=int(date[6:8]))
        out_shows = []
        out_viewers = []

        try:
            logging.info('Updating time-shifted entry at date %s' % date.strftime("%Y%m%d"))

            # Import Schedule
            list_sched = pd.DataFrame()
            for step in range(TSV_DAYS):

                sched = get_brc((date - timedelta(days=step)).strftime("%Y%m%d"))
                sched["station"] = sched["ChannelCode"].map(stations).astype(str)
                sched = sched[['Date', 'Title', 'StartTime', 'EndTime', 'BrdCstId',
                               'Description', 'Duration', 'station']]

                list_sched = pd.concat([list_sched, sched], axis=0, ignore_index=False)

            # Import Live Viewers
            tsvs = get_tsv_viewers(agemax=49, agemin=15, date=date.strftime("%Y%m%d"))
            tsvs["station"] = tsvs["StationId"].map(stations)
            # Map schedule and live-viewers together and append to a list
            viewers, shows = map_viewers_ovn(list_sched, tsvs)
            out_viewers.append(viewers)
            out_shows.append(shows)

            # Create updated dataframe of dates
            df_new = df_to_disk_ovn(out_viewers, out_shows, date.strftime("%Y%m%d"))
            df_update = pd.concat([df_update, df_new], axis=0, ignore_index=False, sort=True)

        except FileNotFoundError as e:

            logging.info('%s did not found file for date %s, continue with next file' % (str(e), date))
            continue

    logging.info('Created updated time-shifted entries, continuing with concatenation')

    # Concatenate update with old file

    df_updated = pd.concat([df_old, df_update], axis=0, ignore_index=False, sort=True)

    df_updated['date'] = pd.to_numeric(df_updated['date'], downcast='integer')
    df_updated.to_csv(f'{LOCAL_PATH}{START}_{int(df_updated["date"].max())}_delayedviewing_DE_15_49_mG.csv',
                      index=False)

    # Delete redundant files from directory
    newest = False
    END = datetime.strptime(END_DAY, '%Y%m%d')
    for g in range(DAYS_IN_YEAR):
        date_new = (END - timedelta(days=g)).strftime('%Y%m%d')
        if date_new == START:
            break
        if os.path.isfile(LOCAL_PATH + '%s_%s_delayedviewing_DE_15_49_mG.csv' % (START, date_new)):
            if newest:
                os.remove((LOCAL_PATH + '%s_%s_delayedviewing_DE_15_49_mG.csv' % (START, date_new)))
            else:
                newest = True

    logging.info('Successfully updated time-shifted facts-table')


# ----------------------------------------------------------------------------------------------------------------------
# PREPROCESSING OF LIVE DATA
# Function for the airflow pipeline, main function for computation update_facts_table
# Flow:
# 1. get_live_viewers. -> all live usage from a date
# 2. get_brc. -> all programs from a date
# 3. map_viewers. -> maps viewers to shows from the 2 above files
# 4. df_to_disk. -> saves results to disk. Create Dataframe for update
# ----------------------------------------------------------------------------------------------------------------------
def map_viewers(sched, lv):
    """
    Maps viewer's live usage to shows. Filter for unique values to
    remove channels with 0 viewers. Iterate over every show aired.
    :param sched: pd.Dataframe of the broadcast
    :param lv: pd.Dataframe of the liveviewers for the same day as the given schedule
    :return: Mapping of the viewers to the aired shows
    """
    out_viewers = []
    out_shows = []

    delete = lv.station.unique()
    sched = sched[sched['station'].isin(delete)]

    for show in zip(sched.values):
        viewers = get_live_viewers_from_show(show, lv)
        if len(viewers) > 0:
            out_viewers.append(viewers)
            out_shows.append(show)

    return out_viewers, out_shows


def df_to_disk(vw, sh, date):
    """
    Creates a pd.Dataframe in the right format for further computations.
    :param vw: pd.Dataframe of the viewers
    :param sh: pd.Series of the schedule
    :param date: Adding the date to the Dataframe for easier filtering
    :return: pd.Dataframe of the right format for concatenation
    """
    lens = [[len(l) for l in ls] for ls in vw]
    lens = [o for subo in lens for o in subo]

    viewers = [[l for l in ls] for ls in vw]
    viewers = [o for subo in viewers for o in subo]
    viewers = pd.concat(viewers, axis=0, ignore_index=False, sort=True)

    tits = [[l[0][1] for l in ls] for ls in sh]
    tits = [o for subo in tits for o in subo]
    titls = [[o] * l for o, l in zip(tits, lens)]
    titls = [o for subo in titls for o in subo]

    viewers['Title'] = titls

    utits = [[l[0][5] for l in ls] for ls in sh]
    utits = [o for subo in utits for o in subo]
    utitls = [[o] * l for o, l in zip(utits, lens)]
    utitls = [o for subo in utitls for o in subo]

    viewers['Description'] = utitls

    prog_dur = [[l[0][6] for l in ls] for ls in sh]
    prog_dur = [o for subo in prog_dur for o in subo]
    prog_durs = [[o] * l for o, l in zip(prog_dur, lens)]
    prog_durs = [o for subo in prog_durs for o in subo]

    viewers['program_duration'] = prog_durs

    end = [[l[0][3] for l in ls] for ls in sh]
    end = [o for subo in end for o in subo]
    ends = [[o] * l for o, l in zip(end, lens)]
    ends = [o for subo in ends for o in subo]

    viewers['show_endtime'] = ends

    start = [[l[0][2] for l in ls] for ls in sh]
    start = [o for subo in start for o in subo]
    starts = [[o] * l for o, l in zip(start, lens)]
    starts = [o for subo in starts for o in subo]

    viewers['show_starttime'] = starts

    bid = [[l[0][4] for l in ls] for ls in sh]
    bid = [o for subo in bid for o in subo]
    bids = [[o] * l for o, l in zip(bid, lens)]
    bids = [o for subo in bids for o in subo]

    viewers['broadcast_id'] = bids
    viewers['date'] = date

    return viewers


def get_live_viewers(date, agemin, agemax):
    """
    Helper function to open the Live Usage file for a given date,
    format it to right dtypes and filter on the parameters passed to the function.
    Adds also 1 day to the End -and Starttime according to the threshold to ensure a correct computation.
    :param date: Day of interest
    :param agemin: Minimum age of the viewer, for filtering of the group of interest
    :param agemax: Maximum age of the viewer, for filtering of the group of interest
    :return: All live viewers interesting for our facts table
    """
    with open(f'{LOCAL_PATH}UsageLive/UsageLive_{date}.pin', 'r', encoding='latin-1') as f:
        df_usagelive = pd.read_csv(f, dtype={'HouseholdId': 'object', 'IndividualId': 'object',
                                             'EndTime': 'object', 'StartTime': 'object'})
    # Filters only on live viewers
    df_usagelive = df_usagelive[df_usagelive['AudienceType'] == 1]
    df_usagelive['H_P'] = df_usagelive['HouseholdId'] + "_" + df_usagelive['IndividualId']

    # Filtering on language
    df_usagelive['Sprache'] = df_usagelive['H_P'].map(get_lang_dict(date))
    df_usagelive = df_usagelive[df_usagelive['Sprache'] == 1]

    # Filter on age
    df_usagelive['Age'] = df_usagelive['H_P'].map(get_age_dict(date))
    df_usagelive = df_usagelive[(df_usagelive['Age'] >= agemin) & (df_usagelive['Age'] <= agemax)]

    # Formatting date, start and end time
    ud_str = str(df_usagelive['UsageDate'].iloc[0])
    ud = pd.to_datetime(ud_str, format="%Y%m%d")
    df_usagelive['UsageDate'] = ud

    st_vec = df_usagelive['StartTime'].tolist()
    st_vec = [ud_str + o.zfill(6) for o in st_vec]
    st_vec = pd.to_datetime(st_vec, format="%Y%m%d%H%M%S")
    df_usagelive['StartTime'] = st_vec

    et_vec = df_usagelive['EndTime'].tolist()
    et_vec = [ud_str + o.zfill(6) for o in et_vec]
    et_vec = pd.to_datetime(et_vec, format="%Y%m%d%H%M%S")
    df_usagelive['EndTime'] = et_vec

    # Threshold adjustment, 2am problem for StartTime and EndTime
    new_day_thresh = pd.to_datetime('020000', format='%H%M%S').time()
    df_usagelive.loc[df_usagelive['StartTime'].dt.time < new_day_thresh, 'StartTime'] += pd.to_timedelta('1 Days')
    df_usagelive.loc[df_usagelive['EndTime'].dt.time <= new_day_thresh, 'EndTime'] += pd.to_timedelta('1 Days')

    # Mapping of weights and kanton to the viewers
    df_usagelive['Weights'] = df_usagelive['H_P'].map(get_weight_dict(date))
    df_usagelive['Kanton'] = df_usagelive['H_P'].map(get_kanton_dict(date))

    return df_usagelive


def imp_live_viewers(date, stations):
    """
    Get all the live viewers of the date given
    :param date: Date of interest
    :param stations: All the official stations
    :return: pd.Dataframe of the live viewers
    """
    lv = get_live_viewers(agemax=49, agemin=15, date=date.strftime("%Y%m%d"))

    lv["station"] = lv["StationId"].map(stations)
    lv = lv[['StartTime', 'EndTime', 'H_P', 'Weights', 'station', 'Kanton']]
    lv['Weights'] = lv['H_P'].map(get_weight_dict((date.strftime("%Y%m%d"))))

    return lv


def imp_live_brdcst_sched(date, stations):
    """
    Get the broadcast schedule of the given date
    :param date: Day of interest
    :param stations: All the official channels, pd.Dataframe expected
    :return: pd.Dataframe of the schedule of the day
    """
    sched = get_brc(date.strftime("%Y%m%d"))
    sched["station"] = sched["ChannelCode"].map(stations).astype(str)
    sched = sched[['Date', 'Title', 'StartTime', 'EndTime', 'BrdCstId', 'Description', 'Duration', 'station']]

    return sched


def update_live_facts_table(dates):
    """
    Updates the facts table of the dates given, check that the paths are correct and the files are complete.
    Does not raise an Filenotfounderror and only wrightes to the log
    :param dates: Date to update the entries in the facts table
    :return: None
    """
    df_old, i = get_live_facts_table()
    END = datetime.strptime(END_DAY, '%Y%m%d')

    # Remove updated entries from the old file
    if not df_old.empty and ADJUST_YEAR == 0:
        # Check if older entries exist, otherwise update them
        for k in range(DAYS_IN_YEAR):
            date = (END - timedelta(days=i + k)).strftime('%Y%m%d')
            if date == START:
                break
            if date not in df_old['date'].values:
                dates.update([str(date)])
        for date_remove in dates:
            df_old = df_old[df_old['date'] != str(date_remove)]

    if not dates:
        logging.info('No dates to update found, exiting execution')
        exit()

    logging.info('Following dates will be updated %s' % dates)

    stations = get_station_dict()
    df_update = pd.DataFrame()

    # Updates every date to be found in the dates list
    for date in dates:

        date = str(date)
        date = datetime(year=int(date[0:4]), month=int(date[4:6]), day=int(date[6:8]))
        out_shows = []
        out_viewers = []

        try:
            logging.info('Updating table entry at date %s' % date.strftime("%Y%m%d"))
            # Import live-viewers
            lv = imp_live_viewers(date, stations)

            # Import Broadcasting Schedule
            sched = imp_live_brdcst_sched(date, stations)

            # Map schedule and live-viewers together and append to a list
            viewers, shows = map_viewers(sched, lv)
            out_shows.append(shows)
            out_viewers.append(viewers)

            # Concatenate to a dataframe which will then be concatenated to the old file
            df_new = df_to_disk(out_viewers, out_shows, date.strftime("%Y%m%d"))
            df_update = pd.concat([df_update, df_new], axis=0, ignore_index=False, sort=True)

        except FileNotFoundError as e:
            logging.info("%s, no file found for date %s, continue with next file" % (str(e), date))
            continue

    logging.info('Created updated live entries, continuing with concatenation')

    # Concatenate update with old file
    df_updated = pd.concat([df_old, df_update], axis=0, ignore_index=False, sort=True)
    df_updated['date'] = pd.to_numeric(df_updated['date'], downcast='integer')
    df_updated.to_csv(f'{LOCAL_PATH}{START}_{int(df_updated["date"].max())}_Live_DE_15_49_mG.csv', index=False)

    # Delete redundant files from directory
    newest = False
    END = datetime.strptime(END_DAY, '%Y%m%d')
    for g in range(DAYS_IN_YEAR):
        date_new = (END - timedelta(days=g)).strftime('%Y%m%d')
        if date_new == START:
            break
        if os.path.isfile(LOCAL_PATH + '%s_%s_Live_DE_15_49_mG.csv' % (START, date_new)):
            if newest:
                os.remove((LOCAL_PATH + '%s_%s_Live_DE_15_49_mG.csv' % (START, date_new)))
            else:
                newest = True

    logging.info('Successfully updated live facts-table')


# ----------------------------------------------------------------------------------------------------------------------
# VERIFICATION FUNCTIONS
# Functions to compute the ratings based on the facts tables
# ----------------------------------------------------------------------------------------------------------------------
def add_individual_ratings(df):
    """
    Computed the rating of each show
    :param df: pd.Dataframe of the facts table
    :return: Facts table with added Rating
    """
    df = df.copy()
    df['individual_Rt-T_live'] = df['duration'] * df['Weights'] / df['program_duration']

    return df


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


def compute_rating_per_channel(path):
    """
    Check if all the channels live ratings are over 0
    """
    date_cols = ['show_endtime', 'show_starttime', 'StartTime', 'EndTime']
    df_2019_live = pd.read_csv(path, parse_dates=date_cols)

    df_2019_live = add_individual_ratings(df_2019_live)

    df_2019_live = df_2019_live.groupby(by=['date', 'station'])['individual_Rt-T_live'].sum()

    channel_with_zeros = df_2019_live[df_2019_live.values == 0]

    return all(df_2019_live) > 0, channel_with_zeros


# ----------------------------------------------------------------------------------------------------------------------
# Which channels to have a look at many interesting ones are missing
# values are close but not correct, make sure what the extract is about, ask thomas
# 3 rows named the same in the extract?
def infosys_comparison():
    """
    Compare the extracted infosys values with the facts tabel values
    """
    infosys_val = pd.read_csv('/home/floosli/Documents/PIN_Data/solutions/infosys_extract.txt', skiprows=3, sep='|',
                              encoding='latin-1', usecols=[0, 1, 2, 18],
                              names=['Datum', 'Zeitschienen', 'Station', 'Rating'])

    if infosys_val.empty:
        print('No infosys extract available cant compute the verification')
        exit()

    infosys_val = infosys_val[infosys_val['Datum'] != 'Last N Weeks']
    infosys_val = infosys_val[infosys_val['Zeitschienen'] != 'Whole day']

    infosys_val = infosys_val[infosys_val['Station'].isin(CHANNELS)]
    infosys_val['date'] = infosys_val['Datum'].str[6:10] + '-' + infosys_val["Datum"].str[3:5] + \
                          '-' + infosys_val["Datum"].str[:2]

    infosys_val['StartTime'] = infosys_val['date'].astype(str) + ' ' + infosys_val['Zeitschienen'].str[:8]
    infosys_val['EndTime'] = infosys_val['date'].astype(str) + ' ' + infosys_val['Zeitschienen'].str[11:20]

    # Keep the whitespace in the string replacement, important
    infosys_val['StartTime'] = infosys_val['StartTime'].str.replace(' 24', ' 00')
    infosys_val['StartTime'] = infosys_val['StartTime'].str.replace(' 25', ' 01')
    infosys_val['StartTime'] = infosys_val['StartTime'].str.replace(' 26', ' 02')

    infosys_val['EndTime'] = infosys_val['EndTime'].str.replace(' 24', ' 00')
    infosys_val['EndTime'] = infosys_val['EndTime'].str.replace(' 25', ' 01')
    infosys_val['EndTime'] = infosys_val['EndTime'].str.replace(' 26', ' 01')

    infosys_val['StartTime'] = pd.to_datetime(infosys_val['StartTime'], errors='coerce')
    infosys_val['EndTime'] = pd.to_datetime(infosys_val['EndTime'], errors='coerce')

    new_day_thresh = pd.to_datetime('020000', format='%H%M%S').time()
    infosys_val.loc[infosys_val['StartTime'].dt.time < new_day_thresh, 'StartTime'] += pd.to_timedelta('1 Days')
    infosys_val.loc[infosys_val['EndTime'].dt.time <= new_day_thresh, 'EndTime'] += pd.to_timedelta('1 Days')

    df_tsv = get_dropbox_facts_table()[1]

    df_tsv['RecordingStartTime'] = df_tsv['RecordingStartTime'].astype(str).str.replace(' 24', ' 00')
    df_tsv['RecordingStartTime'] = df_tsv['RecordingStartTime'].astype(str).str.replace(' 25', ' 01')
    df_tsv['RecordingStartTime'] = df_tsv['RecordingStartTime'].astype(str).str.replace(' 26', ' 02')

    df_tsv['RecordingEndTime'] = df_tsv['RecordingEndTime'].astype(str).str.replace(' 24', ' 00')
    df_tsv['RecordingEndTime'] = df_tsv['RecordingEndTime'].astype(str).str.replace(' 25', ' 01')
    df_tsv['RecordingEndTime'] = df_tsv['RecordingEndTime'].astype(str).str.replace(' 26', ' 02')

    df_tsv['RecordingStartTime'] = pd.to_datetime(df_tsv['RecordingStartTime'], errors='coerce')
    df_tsv['RecordingEndTime'] = pd.to_datetime(df_tsv['RecordingEndTime'], errors='coerce')

    df_lv = get_dropbox_facts_table()[0]

    df_lv['StartTime'] = df_lv['StartTime'].astype(str).str.replace(' 24', ' 00')
    df_lv['StartTime'] = df_lv['StartTime'].astype(str).str.replace(' 25', ' 01')
    df_lv['StartTime'] = df_lv['StartTime'].astype(str).str.replace(' 26', ' 02')

    df_lv['EndTime'] = df_lv['EndTime'].astype(str).str.replace(' 24', ' 00')
    df_lv['EndTime'] = df_lv['EndTime'].astype(str).str.replace(' 25', ' 01')
    df_lv['EndTime'] = df_lv['EndTime'].astype(str).str.replace(' 26', ' 02')

    df_lv['StartTime'] = pd.to_datetime(df_lv['StartTime'], errors='coerce')
    df_lv['EndTime'] = pd.to_datetime(df_lv['EndTime'], errors='coerce')

    results = pd.DataFrame(data=None, index=None, columns=['Datum', 'Station', 'Live', 'Tsv', 'Rating'])
    for date in infosys_val['Datum'].unique():

        zeitschiene = infosys_val[(infosys_val['Datum'] == date) & (infosys_val['Station'] == '3+')].copy()

        for channel in ['3+']:

            live_score = 0
            tsv_score = 0

            tsv_chan = df_tsv[df_tsv['station'] == channel]
            lv_chan = df_lv[df_lv['station'] == channel]

            for index, row in zeitschiene.iterrows():
                # if row['StartTime'] != pd.Timestamp(2019, 12, 17, 21, 35, 00):
                #    continue

                tsv = tsv_chan[~(tsv_chan['show_starttime'] > row['EndTime']) & ~(tsv_chan['show_endtime'] < row['StartTime']) &
                               ~(tsv_chan['RecordingEndTime'] < row['StartTime']) & ~(tsv_chan['RecordingStartTime'] > (row['EndTime']))]

                temp_tsv = pd.DataFrame()
                temp_tsv['Viewer'] = tsv['H_P']
                temp_tsv['broadcast_id'] = tsv['broadcast_id']
                temp_tsv['Weights'] = tsv['Weights']
                temp_tsv['Duration'] = ((tsv['RecordingEndTime'].clip(upper=row['EndTime']) -
                                         tsv['RecordingStartTime'].clip(lower=row['StartTime'])).dt.total_seconds())

                temp_tsv = temp_tsv[temp_tsv['Duration'] >= 0]
                if not temp_tsv.empty:
                    temp_tsv['offset'] = temp_tsv['Duration'].apply(lambda x: 1 if x == 0 else 0)
                    temp_tsv['Duration'] = temp_tsv['Duration'] + temp_tsv['offset']
                    temp_tsv['Rating'] = temp_tsv['Duration'] * temp_tsv['Weights'] / 300
                    temp_tsv = temp_tsv.groupby(['broadcast_id', 'Viewer', 'Weights', 'offset'])['Duration', 'Rating'].sum().reset_index(drop=False)
                    temp_tsv = temp_tsv.groupby(['Viewer', 'Weights', 'offset'])['Duration', 'Rating'].max().reset_index(drop=True)
                    tsv_score += temp_tsv['Rating'].sum()

                lv = lv_chan[~(lv_chan['show_starttime'] > row['EndTime']) & ~(lv_chan['show_endtime'] < row['StartTime'])]

                temp_lv = pd.DataFrame()
                temp_lv['Viewer'] = lv['H_P']
                temp_lv['Weights'] = lv['Weights']
                temp_lv['Duration'] = ((lv['EndTime'].clip(upper=row['EndTime']) -
                                        lv['StartTime'].clip(lower=row['StartTime'])).dt.total_seconds())

                temp_lv = temp_lv[temp_lv['Duration'] >= 0]
                if not temp_lv.empty:
                    temp_lv['offset'] = temp_lv['Duration'].apply(lambda x: 1 if x == 0 else 0)
                    temp_lv['Duration'] = temp_lv['Duration'] + temp_lv['offset']
                    temp_lv['Rating'] = temp_lv['Duration'] * temp_lv['Weights'] / 300
                    temp_lv = temp_lv.drop_duplicates()
                    live_score += temp_lv['Rating'].sum()

            results = results.append(pd.DataFrame(data=[[date, channel, live_score, tsv_score, live_score+tsv_score]],
                                                  columns=['Datum', 'Station', 'Live', 'Tsv', 'Rating']),
                                     ignore_index=True)

    infosys_val = infosys_val.groupby(['Datum', 'Station'])['Rating'].sum().reset_index(drop=False)

    difference = infosys_val.merge(results, how='inner', on=['Datum', 'Station'], suffixes=('_infosys', '_facts_table'))

    difference['Difference'] = difference['Rating_infosys'] - difference['Rating_facts_table']
    difference.to_csv(f'{SOL_PATH}comparison_infosys.csv')
    return difference['Difference'].sum()


# ----------------------------------------------------------------------------------------------------------------------
