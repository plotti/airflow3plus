import datetime
import pandas as pd
import logging
import os
import Airflow_Variables
import Pin_Utils

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
START = Airflow_var.start
END_DAY = Airflow_var.end


def get_live_facts_table(end_day, start_day, days_in_year):
    """
    Returns the current version of the facts table for live viewing
    """
    date_cols = ['show_endtime', 'show_starttime', 'StartTime', 'EndTime']
    end = datetime.strptime(end_day, '%Y%m%d')

    df = pd.DataFrame()
    i = 0
    for i in range(days_in_year):
        date_old = (end - timedelta(days=i)).strftime('%Y%m%d')
        if date_old == START:
            break
        if os.path.isfile(LOCAL_PATH + '%s_%s_Live_DE_15_49_mG.csv' % (start_day, date_old)):
            df = pd.read_csv(LOCAL_PATH + '%s_%s_Live_DE_15_49_mG.csv' % (start_day, date_old),
                             parse_dates=date_cols,
                             dtype={'Description': str, 'H_P': str, 'Kanton': int, 'Title': str, 'Weights': float,
                                    'broadcast_id': int, 'date': str, 'duration': float, 'program_duration': int,
                                    'station': str})
            break

    return df, i


def get_brc(date):
    """
    Aggregates the broadcast file of 'date' with right dtypes
    assigned. Adds 1 day to the StarTime id the threshold of 2am isn't reached.
    Also adds the segmented duration of the show together to compute the full duration of the time
    watched.
    :param date: Day of interst
    :return: pd.Dataframe of the broadcast schedule of the day
    """
    try:
        with open(f'{LOCAL_PATH}BrdCst/BrdCst_{date}.pin', 'r', encoding='latin-1') as f:
            brc = pd.read_csv(f, dtype={'Date': 'object', 'StartTime': 'object',
                                        'ChannelCode': 'int'})
    except FileNotFoundError:
        with open(f'/media/floosli/UeliTheDisk/Pin_Data/BrdCst_old/BrdCst_{date}.pin', 'r', encoding='latin-1') as f:
            brc = pd.read_csv(f, dtype={'Date': 'object', 'StartTime': 'object', 'ChannelCode': 'int'},
                              error_bad_lines=False)
    brc = brc.rename(columns={'StartTime': 'show_starttime'})

    # Padding time to 6 digits and to datetime
    brc['show_starttime'] = brc['Date'] + brc['show_starttime'].str.zfill(6)
    brc['show_starttime'] = pd.to_datetime(brc['show_starttime'], format='%Y%m%d%H%M%S')

    # Flagging data belonging to the next day, adding 1 day to dates
    new_day_tresh = pd.to_datetime("020000", format='%H%M%S').time()
    brc['add_day'] = (brc['show_starttime'].dt.time < new_day_tresh).astype(int)
    brc['show_starttime'] += pd.to_timedelta(brc['add_day'], 'd')

    # Getting end time from start and duration
    brc['show_endtime'] = brc['show_starttime'] + pd.to_timedelta(brc['Duration'], 's')

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
    df_live = df_live[(df_live['station'] == show[0][7])].copy()

    df_live['Channel_StartTime'] = df_live['StartTime'].clip(lower=show[0][2])
    df_live['Channel_EndTime'] = df_live['EndTime'].clip(upper=show[0][3])
    df_live['duration'] = (df_live['Channel_EndTime'] - df_live['Channel_StartTime']).dt.total_seconds()
    df_live = df_live[df_live['duration'] > 0]

    return df_live


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

    delete = lv['station'].unique()
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
    try:
        with open(f'{LOCAL_PATH}UsageLive/UsageLive_{date}.pin', 'r', encoding='latin-1') as f:
            df_usagelive = pd.read_csv(f, dtype={'HouseholdId': 'object', 'IndividualId': 'object',
                                                 'EndTime': 'object', 'StartTime': 'object'})
    except FileNotFoundError:
        with open(f'/media/floosli/UeliTheDisk/Pin_Data/UsageLive_old/UsageLive_{date}.pin', 'r', encoding='latin-1') as f:
            df_usagelive = pd.read_csv(f, dtype={'HouseholdId': 'object', 'IndividualId': 'object',
                                                 'EndTime': 'object', 'StartTime': 'object'})
    # Filters only on live viewers
    df_usagelive = df_usagelive[df_usagelive['AudienceType'] == 1]
    df_usagelive['H_P'] = df_usagelive['HouseholdId'] + "_" + df_usagelive['IndividualId']

    # Filtering on language
    df_usagelive['Sprache'] = df_usagelive['H_P'].map(Pin_Utils.get_lang_dict(date))
    df_usagelive = df_usagelive[df_usagelive['Sprache'] == 1]

    # Filter on age
    df_usagelive['Age'] = df_usagelive['H_P'].map(Pin_Utils.get_age_dict(date))
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
    df_usagelive['Weights'] = df_usagelive['H_P'].map(Pin_Utils.get_weight_dict(date))
    df_usagelive['Kanton'] = df_usagelive['H_P'].map(Pin_Utils.get_kanton_dict(date))
    df_usagelive['Gender'] = df_usagelive['H_P'].map(Pin_Utils.get_gender_dict(date))

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
    lv = lv[['station', 'StartTime', 'EndTime', 'H_P', 'Age', 'Gender', 'Weights', 'Kanton']]

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
    sched = sched[['Date', 'Title', 'show_starttime', 'show_endtime', 'BrdCstId',
                   'Description', 'Duration', 'station', 'ChannelCode']]

    return sched


# TODO ongoing work
def compute_individual_marketshare_live(df, date):
    """
    Compute the Marketshare based on the facts table
    The values will differ from the infosys extract as we do not have access to all the channels
    """
    org = df.copy(deep=True)
    org['Marketshare_live'] = 0

    df = org[(org['date'] == date)]

    brc = get_brc(date)
    df_usg = get_live_viewers(date, agemax=49, agemin=15)

    for index, row in df.iterrows():

        whole_rt = 0
        temp_brc = brc[brc['BrdCstId'] == row['broadcast_id']]

        for idx, seq in temp_brc.iterrows():

            df_usg['Duration'] = ((df_usg['EndTime'].clip(upper=seq['show_endtime']) -
                                   df_usg['StartTime'].clip(lower=seq['show_starttime'])).dt.total_seconds())
            df_usg['Rating'] = (df_usg['Duration'] * df_usg['Weights']) / (seq['Duration'])

            whole_rt += df_usg[df_usg['Rating'] > 0]['Rating'].sum()

        org.iloc[row.name, org.columns.get_loc('Marketshare_live')] = row['individual_Rt-T_live'] / whole_rt

    return org


def add_individual_ratings(df):
    """
    Computed the rating of each show
    :param df: pd.Dataframe of the facts table
    :return: Facts table with added Rating
    """
    df = df.copy()
    df['individual_Rt-T_live'] = df['duration'] * df['Weights'] / df['program_duration']

    return df


def update_live_facts_table(dates, end_day, start_day, adjust_year, days_in_year):
    """
    Updates the facts table of the dates given, check that the paths are correct and the files are complete.
    Does not raise an Filenotfounderror and only writes to the log
    :param dates:
    :param end_day:
    :param adjust_year:
    :param days_in_year:
    :param start_day:
    :return:
    """
    df_old, i = get_live_facts_table(end_day, start_day, days_in_year)
    end = datetime.strptime(end_day, '%Y%m%d')

    # Remove updated entries from the old file
    if not df_old.empty and adjust_year == 0:
        # Check if older entries exist, otherwise update them
        for k in range(days_in_year):
            date = (end - timedelta(days=i+k)).strftime('%Y%m%d')
            if date == start_day:
                break
            if date not in df_old['date'].values:
                dates.update([str(date)])
        for date_remove in dates:
            df_old = df_old[df_old['date'] != str(date_remove)]

    if not dates:
        logging.info('No dates to update found, exiting execution')
        exit()

    logging.info('Following dates will be updated %s' % dates)

    stations = Pin_Utils.get_station_dict()
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
            df = df_to_disk(out_viewers, out_shows, date.strftime("%Y%m%d"))

            # TODO Add Rating/Marketshare to each entry
            # df = add_individual_ratings(df)
            # df = compute_individual_marketshare_live(df, date) , 'Marketshare_live'

            df = df[['date', 'station', 'broadcast_id', 'Title', 'Description',
                     'show_starttime', 'show_endtime', 'program_duration',
                     'StartTime', 'EndTime',
                     'H_P', 'Kanton', 'Age', 'Gender', 'Channel_StartTime', 'Channel_EndTime', 'duration',
                     'Weights']].reset_index(drop=True)

            df_update = pd.concat([df_update, df], axis=0, ignore_index=False, sort=True)

        except FileNotFoundError as e:
            logging.info("%s, no file found for date %s, continue with next file" % (str(e), date))
            continue

    logging.info('Created updated live entries, continuing with concatenation')

    # Concatenate update with old file
    df_updated = pd.concat([df_old, df_update], axis=0, ignore_index=False, sort=True)
    df_updated['date'] = pd.to_numeric(df_updated['date'], downcast='integer')
    df_updated.to_csv(f'{LOCAL_PATH}{start_day}_{int(df_updated["date"].max())}_Live_DE_15_49_mG.csv', index=False)

    # Delete redundant files from directory
    newest = False
    for g in range(days_in_year):
        date_new = (end - timedelta(days=g)).strftime('%Y%m%d')
        if date_new == start_day:
            break
        if os.path.isfile(LOCAL_PATH + '%s_%s_Live_DE_15_49_mG.csv' % (start_day, date_new)):
            if newest:
                os.remove((LOCAL_PATH + '%s_%s_Live_DE_15_49_mG.csv' % (start_day, date_new)))
            else:
                newest = True

    logging.info('Successfully updated live facts-table')
