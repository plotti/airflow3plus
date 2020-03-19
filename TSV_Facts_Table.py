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
DAYS_IN_YEAR = Airflow_var.days_in_year
TSV_DAYS = Airflow_var.days_tsv
START = Airflow_var.start
END_DAY = Airflow_var.end


def get_tsv_facts_table():
    """
    Returns the current version of the facts table for time shifted viewing
    """
    delayed_date_cols = ['ViewingStartTime', 'ViewingTime', 'RecordingStartTime', 'show_endtime',
                         'show_starttime', 'RecordingEndTime', 'Channel_StartTime', 'Channel_EndTime']
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
                                                                   'Kanton': float, 'Platform': float,
                                                                   'StationId': float,
                                                                   'Title': str, 'TvSet': float, 'Weights': float,
                                                                   'UsageDate': float, 'ViewingActivity': float,
                                                                   'age': float, 'broadcast_id': float, 'date': str,
                                                                   'duration': float, 'program_duration': float,
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


def imp_tsv_brdcst_sched(date, stations):
    """
    Get the broadcast schedule of the given date
    :param date: Day of interest
    :param stations: All the official channels, pd.Dataframe expected
    :return: pd.Dataframe of the schedule of the day
    """
    sched = get_brc(date)
    sched["station"] = sched["ChannelCode"].map(stations).astype(str)
    sched = sched[['Date', 'Title', 'show_starttime', 'show_endtime', 'BrdCstId',
                   'Description', 'Duration', 'station', 'ChannelCode']]

    return sched


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
    try:
        with open(f'{LOCAL_PATH}UsageTimeShifted/UsageTimeShifted_{date}.pin', 'r', encoding='latin-1') as f:
            df = pd.read_csv(f, dtype={**{c: 'object' for c in date_cols},
                                       **{c: 'object' for c in time_cols}})
    except FileNotFoundError:
        with open(f'/media/floosli/UeliTheDisk/Pin_Data/UsageTimeShifted_old/UsageTimeShifted_{date}.pin',
                  'r', encoding='latin-1') as f:
            df = pd.read_csv(f, dtype={**{c: 'object' for c in date_cols},
                                       **{c: 'object' for c in time_cols}}, error_bad_lines=False)

    # Filtering out TSV activity
    cond0 = df['ViewingActivity'].isin([4, 10])
    df = df[cond0]

    # Combining Id's
    df['H_P'] = df['HouseholdId'].astype(str) + "_" + df['IndividualId'].astype(str)

    # Filtering out based on ages
    df['age'] = df[['H_P', 'RecordingDate']].T.apply(tuple).map(Pin_Utils.get_age_dict_ovn(date))
    df = df[(df['age'] >= agemin) & (df['age'] <= agemax)]

    # Filtering on language
    df = df[df['H_P'].map(Pin_Utils.get_lang_dict(date)) == 1]

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

    df['Weights'] = df[['H_P', 'RecordingDate']].T.apply(tuple).map(Pin_Utils.get_weight_dict_ovn(date))
    df['Kanton'] = df['H_P'].map(Pin_Utils.get_kanton_dict(date))
    df['Gender'] = df['H_P'].map(Pin_Utils.get_gender_dict(date))

    return df


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


def imp_tsv_viewers(date, stations):

    tsv = get_tsv_viewers(agemax=49, agemin=15, date=date)
    tsv["station"] = tsv["StationId"].map(stations)
    tsv = tsv[['station', 'ViewingStartTime', 'ViewingTime', 'H_P', 'age', 'Weights', 'Gender', 'Kanton',
               'RecordingDate', 'RecordingStartTime', 'RecordingEndTime']]

    return tsv


def get_ovn_viewers_from_show(show, df):
    """
    Filters 'df' to keep only the viewers of 'show'
    Maps a viewer to the show she/he watchers.
    :param show: pd.Series of the broadcast aired in a certain time
    :param df: pd.Dataframe of the viewers watching TV on a given date
    :return: pd.Dataframe of the shows watched of the viewers
    """
    df = df[(df['station'] == show[0][7])].copy()
    df['Channel_StartTime'] = df['RecordingStartTime'].clip(lower=show[0][2])
    df['Channel_EndTime'] = df['RecordingEndTime'].clip(upper=show[0][3])
    df['duration'] = (df['Channel_EndTime'] - df['Channel_StartTime']).dt.total_seconds()
    df = df[df['duration'] >= 0]

    return df


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


# TODO ongoing work
def compute_individual_marketshare_tsv(df, date):

    org = df.copy(deep=True)
    org['Marketshare_live'] = 0

    df = org[(org['date'] == date)]

    list_brc = pd.DataFrame()
    for step in range(TSV_DAYS):
        brc = get_brc((date - timedelta(days=step)).strftime("%Y%m%d"))
        list_brc = pd.concat([list_brc, brc], axis=0, ignore_index=False)

    df_usg = get_tsv_viewers(date, agemin=15, agemax=49)
    df_lv_usg = get_live_viewers(date, agemax=49, agemin=15)

    for index, row in df.iterrows():

        whole_rt = 0
        temp_brc = list_brc[list_brc['BrdCstId'] == row['broadcast_id']]

        for idx, seq in temp_brc.iterrows():

            df_lv_usg['Duration'] = ((df_lv_usg['EndTime'].clip(upper=seq['show_endtime']) -
                                      df_lv_usg['StartTime'].clip(lower=seq['show_starttime'])).dt.total_seconds())
            df_lv_usg['Rating'] = (df_lv_usg['Duration'] * df_lv_usg['Weights']) / (seq['Duration'])
            whole_rt += df_lv_usg[df_lv_usg['Rating'] > 0]['Rating'].sum()

            df_usg['Duration'] = ((df_usg['RecordingEndTime'].clip(upper=seq['show_endtime']) -
                                   df_usg['RecordingStartTime'].clip(lower=seq['show_starttime'])).dt.total_seconds())
            df_usg['Rating'] = (df_usg['Duration'] * df_usg['Weights']) / (seq['Duration'])
            whole_rt += df_usg[df_usg['Rating'] > 0]['Rating'].sum()

        org.iloc[row.name, org.columns.get_loc('Marketshare_live')] = row['individual_Rt-T_live'] / whole_rt

    return org


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


def update_tsv_facts_table(dates, end_day, start_day, days_in_year, adjust_year, tsv_days):
    """
    Main function to update the time-shifted facts table
    :param dates: Series of int dates which have to be updated on the table
    :param end_day: Date of the last day of the timeframe
    :param start_day: Start date of the timeframe
    :param days_in_year: Days in the updated year
    :param adjust_year: Integer of days away from the current year
    :param tsv_days: Timeshifted days respected for the computation
    :return: None
    """
    df_old, i = get_tsv_facts_table()
    end = datetime.strptime(end_day, '%Y%m%d')

    # Remove updated entries from the old file
    if not df_old.empty and adjust_year >= 0:
        # Check if older entries exist of files which are present, otherwise update them
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

    for date in dates:

        date = str(date)
        date = datetime(year=int(date[0:4]), month=int(date[4:6]), day=int(date[6:8]))
        out_shows = []
        out_viewers = []

        try:
            logging.info('Updating time-shifted entry at date %s' % date.strftime("%Y%m%d"))

            # Import Schedule
            list_sched = pd.DataFrame()
            for step in range(tsv_days):
                sched = imp_tsv_brdcst_sched((date - timedelta(days=step)).strftime("%Y%m%d"), stations)
                list_sched = pd.concat([list_sched, sched], axis=0, ignore_index=False)

            # Import Live Viewers
            tsvs = imp_tsv_viewers(date.strftime("%Y%m%d"), stations)

            # Map schedule and live-viewers together and append to a list
            viewers, shows = map_viewers_ovn(list_sched, tsvs)
            out_viewers.append(viewers)
            out_shows.append(shows)
            del viewers, shows

            # Create updated dataframe of dates
            df = df_to_disk_ovn(out_viewers, out_shows, date.strftime("%Y%m%d"))
            del tsvs, out_shows, out_viewers

            # TODO remove until required
            # df = add_individual_ratings_ovn(df)
            # df = compute_individual_marketshare_tsv(df, date) , 'Marketshare_tsv'

            df_update = pd.concat([df_update, df], axis=0, ignore_index=False, sort=True)
            del df

        except FileNotFoundError as e:
            logging.info('%s did not found file for date %s, continue with next file' % (str(e), date))
            print(e)
            continue

    logging.info('Created updated time-shifted entries, continuing with concatenation')
    # Concatenate update with old file
    df_update = pd.concat([df_old, df_update], axis=0, ignore_index=False, sort=True)
    del df_old

    df_update['date'] = pd.to_numeric(df_update['date'], downcast='integer')
    df_update = df_update[['date', 'station', 'broadcast_id', 'Title', 'Description',
                           'show_starttime', 'show_endtime', 'program_duration',
                           'RecordingDate', 'RecordingStartTime', 'RecordingEndTime',
                           'H_P', 'Kanton', 'Age', 'Gender', 'ViewingStartTime', 'ViewingTime',
                           'Channel_StartTime', 'Channel_EndTime', 'duration',
                           'Weights']].reset_index(drop=True)
    df_update.to_csv(f'{LOCAL_PATH}{start_day}_{int(df_update["date"].max())}_delayedviewing_DE_15_49_mG.csv',
                     index=False)

    # Delete redundant files from directory
    newest = False
    for g in range(days_in_year):
        date_new = (end - timedelta(days=g)).strftime('%Y%m%d')
        if date_new == start_day:
            break
        if os.path.isfile(LOCAL_PATH + '%s_%s_delayedviewing_DE_15_49_mG.csv' % (start_day, date_new)):
            if newest:
                os.remove((LOCAL_PATH + '%s_%s_delayedviewing_DE_15_49_mG.csv' % (start_day, date_new)))
            else:
                newest = True

    logging.info('Successfully updated time-shifted facts-table')

