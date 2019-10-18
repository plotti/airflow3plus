import datetime
import pandas as pd
import logging
import os

from datetime import timedelta, datetime
from collections import Counter


# Some variable to influence the computation of the facts tables
local_path = '/home/floosli/Documents/PIN_Data/'
days_in_past = 10


#########################################################
##### GENERAL PURPOSE FILE OPENERS AND DICTIONARIES #####
# Getter/ Helper functions for facts tables aggregation and computation
def get_kanton_dict(date):
    """
    Returns viewer's Kanton on a given date
    :param date: Day of interest
    :return: Dict of the viewers kanton
    """
    with open(f'{local_path}SocDem_{date}.pin', 'r', encoding='latin-1') as f:
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
    with open(f'{local_path}Station.pin', 'r', encoding='latin-1') as f:
        df_sta = pd.read_csv(f)

    return {k: v for k, v in zip(df_sta['StationID'].tolist(), df_sta['StationAbbr'].tolist())}


def get_weight_dict(date):
    """
    Returns viewer's weight on a given date in form of a dict (H_P: Weight)
    :param date: Day of interest
    :return: Dictionary of the viewers Id and the respective weight
    """
    with open(f'{local_path}Weight_{date}.pin', 'r', encoding='latin-1') as f:
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

    for i in range(8):

        with open(f'{local_path}Weight_{(date-timedelta(days=i)).strftime("%Y%m%d")}.pin',
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
    with open(f'{local_path}SocDem_{date}.pin', 'r', encoding='latin-1') as f:
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
    with open(f'{local_path}SocDem_{date}.pin', 'r', encoding='latin-1') as f:
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
    with open(f'{local_path}BrdCst_{date}.pin', 'r', encoding='latin-1') as f:
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
    with open(f'{local_path}UsageLive_{date}.pin', 'r', encoding='latin-1') as f:
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

    # Threshold adjustment
    new_day_thresh = pd.to_datetime('020000', format='%H%M%S').time()
    df_usagelive.loc[df_usagelive['StartTime'].dt.time < new_day_thresh, 'StartTime'] += pd.to_timedelta('1 Days')
    df_usagelive.loc[df_usagelive['EndTime'].dt.time <= new_day_thresh, 'EndTime'] += pd.to_timedelta('1 Days')

    # Mapping of weights and kanton to the viewers
    df_usagelive['Weights'] = df_usagelive['H_P'].map(get_weight_dict(date))
    df_usagelive['Kanton'] = df_usagelive['H_P'].map(get_kanton_dict(date))

    return df_usagelive


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


##############################################
##### PREPROCESSING OF TIME SHIFTED DATA #####
# ------------------------------------------------------------------------------------------
# Flow:
# 1. get_tsv_viewers. -> all shifted usage from a date
# 2. get_brc for 7 days before tsv date -> all programs that ran in the last 7 days
# 3. map_viewers_ovn. -> maps usage to shows, from the 2 above files
# 4. df_to_disk_ovn. -> saves to disk. Create Dataframe for update
# ------------------------------------------------------------------------------------------
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

    with open(f'{local_path}UsageTimeShifted_{date}.pin', 'r', encoding='latin-1') as f:
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
    delayed_date_cols = ['ViewingStartTime', 'ViewingTime',
                         'RecordingStartTime', 'show_endtime',
                         'show_starttime', 'RecordingEndTime']
    year = datetime.now().year
    year = str(year) + '0101'

    # Read the old file in and update dates with day from latest update
    df_old = pd.DataFrame()
    for i in range(days_in_past*2):
        date_old = (datetime.now() - timedelta(days=i)).strftime('%Y%m%d')
        if os.path.isfile(local_path + '%s_%s_delayedviewing_DE_15_49_mG.csv' % (year, date_old)):
            df_old = pd.read_csv(local_path + '%s_%s_delayedviewing_DE_15_49_mG.csv' % (year, date_old),
                                 parse_dates=delayed_date_cols, dtype={'Description': str, 'Title': str, 'date': int})

    # Check if older entries exist of files which are present, otherwise update them
    for k in range(days_in_past):
        date = (datetime.now() - timedelta(days=i + k)).strftime('%Y%m%d')
        if not df_old['date'].astype(str).str.contains(date).any():
            if date == year:
                dates.update([int(date)])
                break
            dates.update([int(date)])

    # Remove updated entries from the old file
    if not df_old.empty:
        for date_remove in dates:
            df_old = df_old[df_old['date'] != date_remove]

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
            for step in range(8):
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
    df_updated.to_csv(f'{local_path}{year}_{int(df_updated["date"].max())}_delayedviewing_DE_15_49_mG.csv',
                      index=False)

    # Delete redundant files from directory
    newest = False
    for i in range(days_in_past*2):
        date = (datetime.now() - timedelta(days=i - 1)).strftime('%Y%m%d')
        if os.path.isfile(local_path + '%s_%s_delayedviewing_DE_15_49_mG.csv' % (year, date)):
            if newest:
                os.remove((local_path + '%s_%s_delayedviewing_DE_15_49_mG.csv' % (year, date)))
            else:
                newest = True

    logging.info('Successfully updates time-shifted facts-table')


######################################
##### PREPROCESSING OF LIVE DATA #####
# ------------------------------------------------------------------------------------------
# Function for the airflow pipeline, main function for computation update_facts_table
# Flow:
# 1. get_live_viewers. -> all live usage from a date
# 2. get_brc. -> all programs from a date
# 3. map_viewers. -> maps viewers to shows from the 2 above files
# 4. df_to_disk. -> saves results to disk. Create Dataframe for update
# ------------------------------------------------------------------------------------------
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
    date_cols = ['show_endtime', 'show_starttime', 'StartTime', 'EndTime']
    year = datetime.now().year
    year = str(year) + '0101'

    # Read the old file in and update dates with day from latest update 20190101_%s_Live_DE_15_49_mG
    df_old = pd.DataFrame()
    for i in range(days_in_past*2):
        date_old = (datetime.now() - timedelta(days=i - 1)).strftime('%Y%m%d')
        if os.path.isfile(local_path + '%s_%s_Live_DE_15_49_mG.csv' % (year, date_old)):
            df_old = pd.read_csv(local_path + '%s_%s_Live_DE_15_49_mG.csv' % (year, date_old), parse_dates=date_cols,
                                 dtype={'Description': str, 'Title': str, 'date': int})

    # Check if older entries exist, otherwise update them
    for k in range(days_in_past):
        date = (datetime.now() - timedelta(days=i + k)).strftime('%Y%m%d')
        if not df_old['date'].astype(str).str.contains(date).any():
            if date == year:
                dates.update([int(date)])
                break
            dates.update([int(date)])

    # Remove updated entries from the old file
    if not df_old.empty:
        for date_remove in dates:
            df_old = df_old[df_old['date'] != date_remove]

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
    df_updated.to_csv(f'{local_path}{year}_{int(df_updated["date"].max())}_Live_DE_15_49_mG.csv', index=False)

    # Delete redundant files from directory
    newest = False
    for i in range(days_in_past*2):
        date = (datetime.now() - timedelta(days=i - 1)).strftime('%Y%m%d')
        if os.path.isfile(local_path + '%s_%s_Live_DE_15_49_mG.csv' % (year, date)):
            if newest:
                os.remove((local_path + '%s_%s_Live_DE_15_49_mG.csv' % (year, date)))
            else:
                newest = True

    logging.info('Successfully updated live facts-table')


# -------------------------------------------------------------------------------------
# Functions to compute the ratings based on the facts tables
# -------------------------------------------------------------------------------------
def add_individual_ratings(df):
    """
    Computed the rating of each show
    :param df: pd.Dataframe of the facts table
    :return: Facts table with added Rating
    """
    df = df.copy()
    df['individual_Rt-T'] = df['duration'] * df['Weights'] / df['program_duration']

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
    df['individual_Rt-T'] = df['duration'] * df['Weights'] / df['program_duration']

    return df


def compute_live_rt(path, station, path_sol):
    """
    Computes and saves ratings for the live facts table
    :param path: Path to the live facts table
    :param station: Station on which the ratings should be computed for
    :param path_sol: Path where to save the resulting file
    :return: None
    """

    date_cols = ['show_endtime', 'show_starttime', 'StartTime', 'EndTime']

    df_2019_live = pd.read_csv(path, parse_dates=date_cols)

    df_2019_live = add_individual_ratings(df_2019_live)
    df_2019_live = df_2019_live[df_2019_live['station'] == station]
    df_2019_live = df_2019_live.groupby(['broadcast_id', 'date'])[['individual_Rt-T', 'duration']].sum()

    df_2019_live.to_csv(path_sol + 'live_rt_T_table')


def compute_tsv_rt(path, station, path_sol):
    """
    Computes the rating for the time-shifted facts table
    :param path: Path to the time-shifted facts table
    :param station: Channel of which the ratings should be computed for
    :param path_sol: Path where to save the resulting file
    :return: None
    """

    delayed_date_cols = ['ViewingStartTime', 'ViewingTime', 'RecordingStartTime',
                         'show_endtime', 'show_starttime', 'RecordingEndTime']

    df_2019_tsv = pd.read_csv(path, parse_dates=delayed_date_cols)

    df_2019_tsv = add_individual_ratings_ovn(df_2019_tsv)
    df_2019_tsv = df_2019_tsv[df_2019_tsv['station'] == station]
    df_2019_tsv = df_2019_tsv.groupby(['broadcast_id', 'RecordingDate'])[['individual_Rt-T']].sum()

    df_2019_tsv.to_csv(path_sol + 'tsv_rt_T_table')
