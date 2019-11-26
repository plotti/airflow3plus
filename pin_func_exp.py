import datetime
import pandas as pd
import logging
import os
import Airflow_variables
import smtplib
import glob

from email.mime.multipart import MIMEMultipart
from os.path import basename
from email.mime.application import MIMEApplication
from email.mime.text import MIMEText

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
Airflow_var = Airflow_variables.AirflowVariables()
# Some global variables
LOCAL_PATH = Airflow_var.local_path
DAYS_IN_YEAR = Airflow_var.days_in_year
TSV_DAYS = Airflow_var.days_tsv
ADJUST_YEAR = Airflow_var.adjust_year
TABLES_PATH = Airflow_var.table_viewers_path
CHANNELS = Airflow_var.relevant_channels
CHANNELS_OF_INTEREST = Airflow_var.channels_of_interest


# ----------------------------------------------------------------------------------------------------------------------
# GENERAL PURPOSE FILE OPENERS AND DICTIONARIES
# Getter/ Helper functions for facts tables aggregation and computation
# ----------------------------------------------------------------------------------------------------------------------
def get_live_facts_table():

    date_cols = ['show_endtime', 'show_starttime', 'StartTime', 'EndTime']

    year = datetime.now().year + ADJUST_YEAR
    start = str(year) + '0101'
    end = str(year) + '1231'
    end = datetime.strptime(end, '%Y%m%d')

    df = pd.DataFrame()
    i = 0
    for i in range(DAYS_IN_YEAR):
        date_old = (end - timedelta(days=i)).strftime('%Y%m%d')
        if date_old == start:
            break
        if os.path.isfile(LOCAL_PATH + '%s_%s_Live_DE_15_49_mG.csv' % (start, date_old)):
            df = pd.read_csv(LOCAL_PATH + '%s_%s_Live_DE_15_49_mG.csv' % (start, date_old), parse_dates=date_cols,
                             dtype={'Description': str, 'H_P': str, 'Kanton': int, 'Title': str, 'Weights': float,
                                    'broadcast_id': int, 'date': str, 'duration': float, 'program_duration': int,
                                    'station': str})
            break
    return df, i


def get_tsv_facts_table():

    delayed_date_cols = ['ViewingStartTime', 'ViewingTime',
                         'RecordingStartTime', 'show_endtime',
                         'show_starttime', 'RecordingEndTime']
    year = datetime.now().year + ADJUST_YEAR
    start = str(year) + '0101'
    end = str(year) + '1231'
    end = datetime.strptime(end, '%Y%m%d')

    # Read the old file in and update dates with day from latest update
    df = pd.DataFrame()
    i = 0
    for i in range(DAYS_IN_YEAR):
        date_old = (end - timedelta(days=i)).strftime('%Y%m%d')
        if date_old == start:
            break
        if os.path.isfile(LOCAL_PATH + '%s_%s_delayedviewing_DE_15_49_mG.csv' % (start, date_old)):
            df = pd.read_csv(LOCAL_PATH + '%s_%s_delayedviewing_DE_15_49_mG.csv' % (start, date_old),
                             parse_dates=delayed_date_cols, dtype={'Description': str, 'H_P': str,
                                                                   'HouseholdId': int, 'IndividualId': int,
                                                                   'Kanton': int, 'Platform': int, 'StationId': int,
                                                                   'Title': str, 'TvSet': int, 'Weights': float,
                                                                   'UsageDate': int, 'ViewingActivity': int,
                                                                   'age': int, 'broadcast_id': int, 'date': str,
                                                                   'duration': float, 'program_duration': int,
                                                                   'station': str})
            break
    return df, i


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
        brc = pd.read_csv(f, dtype={'Date': 'object', 'StartTime': 'object',
                                    'ChannelCode': 'int'})

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
    df['age'] = df['H_P'].map(get_age_dict(date))
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
    year = datetime.now().year + ADJUST_YEAR
    start = str(year) + '0101'
    end = str(year) + '1231'
    end = datetime.strptime(end, '%Y%m%d')

    df_old, i = get_tsv_facts_table()

    # Remove updated entries from the old file
    if not df_old.empty and ADJUST_YEAR >= 0:
        # Check if older entries exist of files which are present, otherwise update them
        for k in range(DAYS_IN_YEAR):
            date = (datetime.now() - timedelta(days=i + k)).strftime('%Y%m%d')
            if date == start:
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
                sched = sched[['Date', 'Title', 'StartTime', 'EndTime',
                               'BrdCstId', 'Description', 'Duration', 'station']]
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
    df_updated.to_csv(f'{LOCAL_PATH}{start}_{int(df_updated["date"].max())}_delayedviewing_DE_15_49_mG.csv',
                      index=False)

    # Delete redundant files from directory
    newest = False
    for g in range(DAYS_IN_YEAR):
        date_new = (end - timedelta(days=g)).strftime('%Y%m%d')
        if date_new == start:
            break
        if os.path.isfile(LOCAL_PATH + '%s_%s_delayedviewing_DE_15_49_mG.csv' % (start, date_new)):
            if newest:
                os.remove((LOCAL_PATH + '%s_%s_delayedviewing_DE_15_49_mG.csv' % (start, date_new)))
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


def imp_brdcst_sched(date, stations):
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
    year = datetime.now().year + ADJUST_YEAR
    start = str(year) + '0101'
    end = str(year) + '1231'
    end = datetime.strptime(end, '%Y%m%d')

    df_old, i = get_live_facts_table()

    # Remove updated entries from the old file
    if not df_old.empty and ADJUST_YEAR == 0:
        # Check if older entries exist, otherwise update them
        for k in range(DAYS_IN_YEAR):
            date = (datetime.now() - timedelta(days=i + k)).strftime('%Y%m%d')
            if date == start:
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
            sched = imp_brdcst_sched(date, stations)

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
    df_updated.to_csv(f'{LOCAL_PATH}{start}_{int(df_updated["date"].max())}_Live_DE_15_49_mG.csv', index=False)

    # Delete redundant files from directory
    newest = False

    for g in range(DAYS_IN_YEAR):
        date_new = (end - timedelta(days=g)).strftime('%Y%m%d')
        if date_new == start:
            break
        if os.path.isfile(LOCAL_PATH + '%s_%s_Live_DE_15_49_mG.csv' % (start, date_new)):
            if newest:
                os.remove((LOCAL_PATH + '%s_%s_Live_DE_15_49_mG.csv' % (start, date_new)))
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


def compute_live_rt(path, station, path_comp_sol):
    """
    Computes and saves ratings for the live facts table
    :param path: Path to the live facts table
    :param station: Station on which the ratings should be computed for
    :param path_comp_sol: Path where to save the resulting file
    :return: None
    """

    date_cols = ['show_endtime', 'show_starttime', 'StartTime', 'EndTime']

    df_2019_live = pd.read_csv(path, parse_dates=date_cols)

    df_2019_live = add_individual_ratings(df_2019_live)
    df_2019_live = df_2019_live[df_2019_live['station'] == station]
    df_2019_live = df_2019_live.groupby(['broadcast_id', 'date'])[['individual_Rt-T_live', 'duration']].sum()

    df_2019_live.to_csv(path_comp_sol)


def compute_tsv_rt(path, station, path_comp_sol):
    """
    Computes the rating for the time-shifted facts table
    :param path: Path to the time-shifted facts table
    :param station: Channel of which the ratings should be computed for
    :param path_comp_sol: Path where to save the resulting file
    :return: None
    """

    delayed_date_cols = ['ViewingStartTime', 'ViewingTime', 'RecordingStartTime',
                         'show_endtime', 'show_starttime', 'RecordingEndTime']

    df_2019_tsv = pd.read_csv(path, parse_dates=delayed_date_cols)

    df_2019_tsv = add_individual_ratings_ovn(df_2019_tsv)
    df_2019_tsv = df_2019_tsv[df_2019_tsv['station'] == station]
    df_2019_tsv = df_2019_tsv.groupby(['broadcast_id'])['individual_Rt-T_tsv'].sum().to_frame()
    df_2019_tsv = df_2019_tsv.reset_index(level=0)

    df_2019_tsv.to_csv(path_comp_sol)


def verify_live_table(path_comp_sol, path_infosys_sol, comparison_path):
    """
    Verify the result from our live facts table by comparing it with the data from infosys
    :param path_comp_sol: Path to our computed ratings
    :param path_infosys_sol: Path to our infosys table
    :param comparison_path: Path to where we save the comparison between both values
    :return: None
    """
    comp_sol = pd.read_csv(path_comp_sol)
    infosys_sol = pd.read_excel(path_infosys_sol, names=[1, 2, 3, 4, 'date', 6, 7, 8, 9, 'broadcast_id',
                                                         'individual_Rt-T_live', 'individual_Rt-T_tsv',
                                                         13, 14, 15, 16, 17, 18])
    infosys_sol = infosys_sol.iloc[3:]

    infosys_sol['date'] = infosys_sol['date'].str.replace('.', '', regex=False)
    infosys_sol['date'] = pd.to_datetime(infosys_sol['date'], format='%d%m%Y')
    infosys_sol['date'] = infosys_sol['date'].dt.strftime('%Y%m%d').astype(int)

    sol = pd.merge(infosys_sol, comp_sol, on=['broadcast_id'], how='inner', suffixes=["_x", "_y"])

    sol['difference_live'] = sol['individual_Rt-T_live_x'] - sol['individual_Rt-T_live_y']
    sol = sol.drop(columns=[1, 2, 3, 4, 6, 7, 8, 9, 13, 14, 15, 16, 17, 18])

    sol.to_csv(comparison_path)


def verify_tsv_table(path_comp_sol, path_infosys_sol, comparison_path):
    """
    Verify the result from our tsv facts table by comparing it with the data from infosys
    :param path_comp_sol: Path to our computed ratings
    :param path_infosys_sol: Path to our infosys table
    :param comparison_path: Path to where we save the comparison between both values
    :return: None
    """
    comp_sol = pd.read_csv(path_comp_sol)
    infosys_sol = pd.read_excel(path_infosys_sol, names=[1, 2, 3, 4, 'date', 6, 7, 8, 9, 'broadcast_id',
                                                         'individual_Rt-T_live', 'individual_Rt-T_tsv',
                                                         13, 14, 15, 16, 17, 18])
    infosys_sol = infosys_sol.iloc[3:]

    infosys_sol['date'] = infosys_sol['date'].str.replace('.', '', regex=False)
    infosys_sol['date'] = pd.to_datetime(infosys_sol['date'], format='%d%m%Y')
    infosys_sol['date'] = infosys_sol['date'].dt.strftime('%Y%m%d').astype(int)

    f = {'individual_Rt-T_live': 'sum', 'individual_Rt-T_tsv': 'sum'}
    infosys_sol = infosys_sol.groupby(['broadcast_id'])[['individual_Rt-T_live', 'individual_Rt-T_tsv']].agg(f)
    infosys_sol = infosys_sol.reset_index(level=0)

    sol = pd.merge(infosys_sol, comp_sol, on=['broadcast_id'], how='inner', suffixes=["_x", "_y"])

    sol['difference_tsv'] = sol['individual_Rt-T_tsv_x'] - sol['individual_Rt-T_tsv_y'] - sol['individual_Rt-T_live']

    sol.to_csv(comparison_path)


def see_if_correct():
    compute_live_rt(path='/home/floosli/Documents/PIN_Data/20190101_20191030_Live_DE_15_49_mG.csv',
                    station='3+', path_comp_sol='/home/floosli/Documents/PIN_Data/solutions/live_rt_T_table')
    verify_live_table(path_comp_sol='/home/floosli/Documents/PIN_Data/solutions/live_rt_T_table',
                      path_infosys_sol='/home/floosli/Documents/PIN_Data/solutions/test_tab.xlsx',
                      comparison_path='/home/floosli/Documents/PIN_Data/solutions/diff_sol_lv')

    compute_tsv_rt(path='/home/floosli/Documents/PIN_Data/20190101_20191030_delayedviewing_DE_15_49_mG.csv',
                   station='3+', path_comp_sol='/home/floosli/Documents/PIN_Data/solutions/tsv_rt_T_table')
    verify_tsv_table(path_comp_sol='/home/floosli/Documents/PIN_Data/solutions/tsv_rt_T_table',
                     path_infosys_sol='/home/floosli/Documents/PIN_Data/solutions/test_tab.xlsx',
                     comparison_path='/home/floosli/Documents/PIN_Data/solutions/diff_sol_tsv')


# ----------------------------------------------------------------------------------------------------------------------
# TRANSFORMATION FUNCTIONS
# Functions to transform the facts table to other representations
# ----------------------------------------------------------------------------------------------------------------------
def compute_viewers_for_channel(channel, dates):
    """
    Compute the viewers table for a given channel and for a given date
    :param channel: Station to compute the table for, should be a string
    :param dates: Day of which we want the table of is in for of a set of strings
    :return: df of the new DataFrame
    """
    date_cols = ['show_endtime', 'show_starttime', 'StartTime', 'EndTime']
    delayed_date_cols = ['ViewingStartTime', 'ViewingTime', 'RecordingStartTime', 'show_endtime',
                         'show_starttime', 'RecordingEndTime']
    dates = list(dates)
    year = datetime.now().year + ADJUST_YEAR
    start = str(year) + '0101'
    end = str(year) + '1231'
    end = datetime.strptime(end, '%Y%m%d')

    facts_lv = pd.DataFrame()
    for i in range(DAYS_IN_YEAR):
        date_old = (end - timedelta(days=i)).strftime('%Y%m%d')
        if date_old == start:
            break
        if os.path.isfile(LOCAL_PATH + '%s_%s_Live_DE_15_49_mG.csv' % (start, date_old)):
            facts_lv = pd.read_csv(LOCAL_PATH + '%s_%s_Live_DE_15_49_mG.csv' % (start, date_old), parse_dates=date_cols,
                                   dtype={'Description': str, 'H_P': str, 'Kanton': int, 'Title': str, 'Weights': float,
                                          'broadcast_id': int, 'date': str, 'duration': float, 'program_duration': int,
                                          'station': str})
            break

    facts_ovn = pd.DataFrame()
    for i in range(DAYS_IN_YEAR):
        date_old = (end - timedelta(days=i)).strftime('%Y%m%d')
        if date_old == start:
            break
        if os.path.isfile(LOCAL_PATH + '%s_%s_delayedviewing_DE_15_49_mG.csv' % (start, date_old)):
            facts_ovn = pd.read_csv(LOCAL_PATH + '%s_%s_delayedviewing_DE_15_49_mG.csv' % (start, date_old),
                                    parse_dates=delayed_date_cols, dtype={'Description': str, 'H_P': str,
                                                                          'HouseholdId': int, 'IndividualId': int,
                                                                          'Kanton': int, 'Platform': int,
                                                                          'StationId': int, 'Title': str, 'TvSet': int,
                                                                          'Weights': float, 'UsageDate': int,
                                                                          'ViewingActivity': int, 'age': int,
                                                                          'broadcast_id': int, 'duration': float,
                                                                          'program_duration': int,
                                                                          'station': str})

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

    year = datetime.now().year + ADJUST_YEAR
    month = datetime.now().month
    day = datetime.now().day

    end = str(year) + str(month) + str(day)
    start = str(year) + '0101'
    if ADJUST_YEAR < 0:
        end = str(year) + '1231'

    date = datetime.strptime(start, '%Y%m%d')

    dates = set()

    for channel in CHANNELS:

        for i in range(DAYS_IN_YEAR):
            add = date.strftime('%Y%m%d')
            if add == end:
                dates.update([int(add)])
                break
            dates.update([int(add)])
            date = date + timedelta(days=1)

        compute_viewers_for_channel(channel, dates)
        print('end of %s' % channel)
# ----------------------------------------------------------------------------------------------------------------------


def brcst_viewer_dict(df, date, group_chans):

    cond = ((df['Date'] == date) & (df['station'].isin(group_chans)))
    cols = ['broadcast_id', 'H_P']

    return df[cond][cols].groupby(cols[0])[cols[1]].apply(list).to_dict()


def flat(l):
    return [o for subo in l for o in subo]


def get_rt_from_vws(data, viewers):
    return data[data['H_P'].isin(viewers)]['individual_Rt-T'].sum()


def format_dataframe(broadcast_id, ratings_dict, show_format):
    return pd.DataFrame().from_dict(ratings_dict, orient='index', columns=[show_format[broadcast_id]])


def to_xl_multi_sheet(df, f, group_chans):

    writer = pd.ExcelWriter(f, engine='xlsxwriter')
    dfs = [df[[col for col in df.columns if chan in col]] for chan in group_chans]

    for frame, chan in zip(dfs, group_chans):

        frame.to_excel(writer, sheet_name=chan)  # send df to writer
        worksheet = writer.sheets[chan]  # pull worksheet object

        for idx, col in enumerate(frame):  # loop through all columns
            series = df[col]
            max_len = max((series.astype(str).map(len).max(),  # len of largest item
                          len(str(series.name))  # len of column name/header
                           )) + 1  # adding a little extra space
            worksheet.set_column(idx, idx, max_len)  # set column width

    writer.save()


def send_mail(date, date_day_b4, date_week_b4, fname_day_b4, fname_week_b4):

    COMMASPACE = ', '
    msg = MIMEMultipart()
    msg['Subject'] = f'[Vorwoche Zuschauer] Rt-T für {date},' \
                     f' berechnet mit Zuschauern von {date_day_b4} und {date_week_b4}'
    recipients = ['hb@3plus.tv', 'floosli@3plus.tv']
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

    s = smtplib.SMTP('10.3.3.103')
    s.send_message(msg)
    s.quit()


def compute_zuschauer_vorwoche(date, df):

    df = df[(df['show_starttime'].dt.hour.isin([18, 19, 20, 21, 22, 23]))
            & (df['program_duration'] > 600)
            & (df['station'].isin(CHANNELS_OF_INTEREST))]
    df['broadcast_id'] = df['broadcast_id'].astype(int)

    id_title_dict = (df[['broadcast_id', 'Title']].drop_duplicates().set_index('broadcast_id').to_dict()['Title'])

    id_starttime_dict = (df[['broadcast_id', 'show_starttime']].drop_duplicates().groupby('broadcast_id').min()
                         .to_dict()['show_starttime'])
    id_starttime_dict = {k: v.strftime('%H:%M') for k, v in id_starttime_dict.items()}

    id_chan_dict = (df[['broadcast_id', 'station']].drop_duplicates().set_index('broadcast_id')
                    .to_dict()['station'])

    show_format = {k: id_chan_dict[k] + " " + id_starttime_dict[k] + " " + id_title_dict[k] for k in id_chan_dict}

    day_before = (date + datetime.timedelta(days=-1))
    week_before = (date + datetime.timedelta(days=-7))

    # dict of {broadcast_id : viewer_list} for each broadcast_id
    # of 7 days ago for our group channels
    brcst_to_viewers_week_b4 = brcst_viewer_dict(df, week_before, CHANNELS)

    # dict of {broadcast_id : viewer_list} for each broadcast_id
    # of 1 day ago for our group channels
    brcst_to_viewers_day_b4 = brcst_viewer_dict(df, day_before, CHANNELS)

    # For date == 'date', getting ratings for all programs of channels of interest
    # using subsets of viewers from our group chans' programs, from day_before and last_week
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
    week_b4 = week_b4.T

    day_b4_data = []
    for broadcast_id, ratings in date_ratings_day_b4.items():
        day_b4_data.append(format_dataframe(broadcast_id, ratings, show_format))
    day_b4 = pd.concat(day_b4_data, axis=1)
    day_b4.index = day_b4.index.map(show_format)
    day_b4 = day_b4.T

    date_format = '%A %d.%m.%Y'
    filename = "Rt from " + date.strftime(date_format) + " with viewers from "
    filename_day_b4 = filename + day_before.strftime(date_format) + '.xlsx'
    filename_week_b4 = filename + week_before.strftime(date_format) + '.xlsx'

    to_xl_multi_sheet(day_b4, filename_day_b4, CHANNELS)
    to_xl_multi_sheet(week_b4, filename_week_b4, CHANNELS)
