import datetime
import pandas as pd
import csv
import logging
import os

from datetime import timedelta, datetime
from collections import Counter
from typing import List, Tuple, Dict

nested_list_strs = List[List[str]]
PATH = '/home/floosli/Documents/PIN_Data_Test/'
LOCAL_PATH = '/home/floosli/Documents/PIN_Data_Test/'
DAYS_IN_PAST = 10


#########################################################
##### GENERAL PURPOSE FILE OPENERS AND DICTIONARIES #####
# Getter/ Helper functions for facts table aggregation

def get_kanton_dict(date: str) -> Dict[str, int]:
    """Returns viewer's Kanton on a given date"""
    with open(f'{PATH}SocDem/SocDem_{date}.pin', 'r', encoding='latin-1') as f:
        df_socdem = pd.read_csv(f, dtype='int32', usecols=['SampleId', 'Person', 'SocDemVal5'])
    df_socdem['H_P'] = df_socdem['SampleId'].astype(str) + "_" + df_socdem['Person'].astype(str)
    kanton_dict = {a: b for a, b in zip(df_socdem['H_P'].values.tolist(),
                                        df_socdem['SocDemVal5'].values.tolist())}
    return kanton_dict


def get_station_id(chan_name: str) -> int:
    """Returns the ID of a chan"""
    with open(f'{PATH}Station.pin', 'r', encoding='latin-1') as f:
        df_sta = pd.read_csv(f)
        if chan_name in df_sta['StationName'].values:
            return df_sta.loc[df_sta['StationName'] == chan_name, 'StationID'].values[0]
        elif chan_name in df_sta['StationAbbr'].values:
            return df_sta.loc[df_sta['StationAbbr'] == chan_name, 'StationID'].values[0]
        else:
            print(f"Please provide valid station chan_name, {chan_name} not found in Channel list")


def get_station_dict() -> Dict[int, str]:
    """Returns a dictionary mapping station IDs to their names"""
    with open(f'{PATH}Station.pin', 'r', encoding='latin-1') as f:
        df_sta = pd.read_csv(f)
    return {k: v for k, v in zip(df_sta['StationID'].tolist(), df_sta['StationAbbr'].tolist())}


def get_chan_title_start(df: pd.DataFrame, ch_d: Dict[int, str]):
    """Helper function to return lists of titles, start times, channels, from a df"""
    t = df['Title'].tolist()
    s = df['StartTime'].apply(lambda x: x.strftime("%H%M")).tolist()
    c = df['ChannelCode'].map(ch_d).tolist()
    return c, t, s


def get_weight_dict(date: str) -> Dict[str, float]:
    """Returns viewer's weight on a given date"""
    with open(f'{PATH}Weight/Weight_{date}.pin', 'r', encoding='latin-1') as f:
        df_wei = pd.read_csv(f)

    df_wei['H_P'] = df_wei['SampledIdRep'].astype(str) + "_" + df_wei['PersonNr'].astype(str)
    weight_dict = {a: b for a, b in zip(df_wei['H_P'].values.tolist(),
                                        df_wei['PersFactor'].values.tolist())}
    return weight_dict


def get_lang_dict(date: str) -> Dict[str, int]:
    """Returns viewer's language on a given date"""
    with open(f'{PATH}SocDem/SocDem_{date}.pin', 'r', encoding='latin-1') as f:
        df_socdem = pd.read_csv(f, dtype='int32', usecols=['SampleId', 'Person', 'SocDemVal4'])
    df_socdem['H_P'] = df_socdem['SampleId'].astype(str) + "_" + df_socdem['Person'].astype(str)
    lang_dict = {a: b for a, b in zip(df_socdem['H_P'].values.tolist(),
                                      df_socdem['SocDemVal4'].values.tolist())}
    return lang_dict


def get_age_dict(date: str) -> Dict[str, int]:
    """Returns viewer's age on a given date"""
    with open(f'{PATH}SocDem/SocDem_{date}.pin', 'r', encoding='latin-1') as f:
        df_socdem = pd.read_csv(f, dtype='int32', usecols=['SampleId', 'Person', 'SocDemVal1'])
    df_socdem['H_P'] = df_socdem['SampleId'].astype(str) + "_" + df_socdem['Person'].astype(str)
    age_dict = {a: b for a, b in zip(df_socdem['H_P'].values.tolist(),
                                     df_socdem['SocDemVal1'].values.tolist())}
    return age_dict


def get_brc_single(date):
    """
    Returns the broadcast file of 'date' with right dtypes
    assigned. Also keeps only sequence number 0
    of segmented programs when seqzero is True.
    """
    with open(f'{PATH}BrdCst/BrdCst_{date}.pin', 'r', encoding='latin-1') as f:
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

    # By default, returns only BrdCstSeq > 0 for segmented programs
    segmented_prog_duration_dict = (brc[(brc['SumPieces'] > 0)
                                    & (brc['BrdCstSeq'] > 0)].groupby(['BrdCstId'])['Duration'].sum().to_dict())
    single_prog_duration_dict = (brc[(brc['SumPieces'] == 0)
                                 & (brc['BrdCstSeq'] == 0)].groupby(['BrdCstId'])['Duration'].nth(0).to_dict())

    prog_duration_dict = {**segmented_prog_duration_dict, **single_prog_duration_dict}
    brc = brc[~((brc['SumPieces'] > 0) & (brc['BrdCstSeq'] == 0))]
    brc['Duration'] = brc['BrdCstId'].map(prog_duration_dict)

    return brc


def get_brc(date0, date1):

    br0 = get_brc_file(date0)
    br1 = get_brc_file(date1)

    br1 = br1[br1['BrdCstId'].isin(br0['BrdCstId'])]
    brc = pd.concat([br0, br1], axis=0, ignore_index=False)

    segmented_prog_duration_dict = (brc[(brc['SumPieces'] > 0) & (brc['BrdCstSeq'] > 0)]
                                    .groupby(['BrdCstId'])['Duration'].sum().to_dict())

    single_prog_duration_dict = (brc[(brc['SumPieces'] == 0) & (brc['BrdCstSeq'] == 0)]
                                 .groupby(['BrdCstId'])['Duration'].nth(0).to_dict())

    prog_duration_dict = dict((Counter(segmented_prog_duration_dict) + Counter(single_prog_duration_dict)))

    brc = brc[~((brc['SumPieces'] > 0) & (brc['BrdCstSeq'] == 0))]
    brc['Duration'] = brc['BrdCstId'].map(prog_duration_dict)

    return brc


def get_brc_file(date):
    """
    Returns the broadcast file of 'date' with right dtypes
    assigned. Also keeps only sequence number 0
    of segmented programs when seqzero is True.
    """
    with open(f'{PATH}BrdCst/BrdCst_{date}.pin', 'r', encoding='latin-1') as f:
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

    return brc


def get_live_viewers(date="20180101", agemin=0, agemax=100):
    """Helper function to open the Live Usage file for a given date,
    format it to right dtypes and filter on the parameters passed to the function."""

    with open(f'{PATH}UsageLive/UsageLive_{date}.pin', 'r', encoding='latin-1') as f:
        df_usagelive = pd.read_csv(f, dtype={'HouseholdId': 'object',
                                             'IndividualId': 'object',
                                             'EndTime': 'object',
                                             'StartTime': 'object'})

    df_usagelive = df_usagelive[df_usagelive['AudienceType'] == 1]  # Live viewers
    df_usagelive['H_P'] = df_usagelive['HouseholdId'] + "_" + df_usagelive['IndividualId']  # Combined ID
    # Filtering on language
    df_usagelive['Sprache'] = df_usagelive['H_P'].map(get_lang_dict(date))
    df_usagelive = df_usagelive[df_usagelive['Sprache'] == 1]
    # Filter on age
    df_usagelive['Age'] = df_usagelive['H_P'].map(get_age_dict(date))
    df_usagelive = df_usagelive[(df_usagelive['Age'] >= agemin) & (df_usagelive['Age'] <= agemax)]

    # Formating date, start and end time
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

    df_usagelive['add_day'] = (df_usagelive['StartTime'] > df_usagelive['EndTime']).astype(int)
    df_usagelive['EndTime'] += pd.to_timedelta(df_usagelive['add_day'], 'd')
    df_usagelive['Weights'] = df_usagelive['H_P'].map(get_weight_dict(date))
    df_usagelive['Kanton'] = df_usagelive['H_P'].map(get_kanton_dict(date))

    # return df
    return df_usagelive


def get_live_viewers_from_show(show: pd.Series, df_live: pd.DataFrame) -> pd.DataFrame:
    """
    Filters 'live_df' to keep only the viewers of 'show'
    'live_df' is a live_usage dataframe, 'show' is a row from a broadcast dataframe.
    """
    df_live = df_live[df_live['station'] == show[0][7]].copy()
    df_live['duration'] = (df_live['EndTime'].clip(upper=show[0][3])
                           - df_live['StartTime'].clip(lower=show[0][2])).dt.total_seconds()
    df_live = df_live[df_live['duration'] > 0]
    return df_live


def get_start_and_dur(prog_name, start_hour, date_, chan_name):
    """Returns exact starting time and duration of program given by prog_name, start_hour, date, chan_name"""
    # Was used with "old" pin functions, most probably not needed with the preprocessed PIN files
    with open(f'{PATH}BrdCst/BrdCst_{date_}.pin', 'r', encoding='latin-1') as f:
        reader = csv.reader(f,quotechar='"', delimiter=',',quoting=csv.QUOTE_ALL)
        df_brc = pd.DataFrame([l for l in reader])
        df_brc.columns = df_brc.iloc[0,:]
        df_brc.drop(index=0,inplace=True)
    cond = ((df_brc['Title'] == prog_name) & (df_brc['ChannelCode'] == str(get_station_id(chan_name)))
            & (df_brc['StartTime'].apply(lambda x : x.startswith(start_hour))) & (df_brc["BrdCstSeq"] == "0"))
    if df_brc.loc[cond].shape[0] == 0 :
        print(f"Please provide a valid program title, {prog_name} not found on {chan_name} on the {date_} starting at {start_hour}")
    else :
        progID = df_brc.loc[cond,'BrdCstId'].values[0]
        exact_start = df_brc[df_brc['BrdCstId'] == progID]["StartTime"].values
        duration = df_brc.loc[df_brc['BrdCstId'] == progID,"Duration"].values
        if int(df_brc.loc[df_brc['BrdCstId'] == progID,'SumPieces'].iloc[0]) == 0 : # Not segmented program
            return [[pd.Timestamp.combine(date = pd.to_datetime(str(date_),format="%Y%m%d"),
                                          time = pd.to_datetime(exact_start[0],format="%H%M%S").time()),
                     datetime.timedelta(seconds=int(duration[0]))]]
        else:  # Segmented program, returnging start and durations of all segments
            toret = []
            for i in range(1, int(df_brc.loc[df_brc['BrdCstId'] == progID, 'SumPieces'].iloc[0]) + 1):
                toret.append([pd.Timestamp.combine(date=pd.to_datetime(str(date_), format="%Y%m%d"),
                                                   time=pd.to_datetime(exact_start[i], format="%H%M%S").time()),
                              datetime.timedelta(seconds=int(duration[i]))])
            return toret


def get_OVN_viewers_from_show(show: pd.Series, df: pd.DataFrame) -> pd.DataFrame:
    """
    Filters 'df' to keep only the viewers of 'show'
    'df' is a time shifted usage dataframe, show is a row from a broadcast dataframe.
    """
    df = df[df['station'] == show['station']].copy()
    df['duration'] = (df['RecordingEndTime'].clip(upper=show["EndTime"])
                      - df['RecordingStartTime'].clip(lower=show['StartTime'])).dt.total_seconds()
    df = df[df['duration'] > 0]
    return df


##############################################
##### PREPROCESSING OF TIME SHIFTED DATA #####
# ------------------------------------------------------------------------------------------
# Flow:
# 1. get_tsv_viewers. -> all shifted usage from a date
# 2. get_brc for 7 days before tsv date -> all programs that ran in the last 7 days
# 3. map_viewers_ovn. -> maps usage to shows, from the 2 above files
# 4. df_to_disk_ovn. -> saves to disk.
# ------------------------------------------------------------------------------------------
def map_viewers_ovn(sched: pd.DataFrame, ovn_df: pd.DataFrame) -> Tuple[list, list]:
    """
    'sched': schedule dataframe. 'ovn_df': time shifted usage dataframe.
    Maps viewer's time shifted usage to shows.
    """
    out_viewers = []
    out_shows = []
    for index, show in sched.iterrows():
        viewers = get_OVN_viewers_from_show(show, ovn_df)
        if len(viewers) > 0:
            out_viewers.append(viewers)
            if show['BrdCstId'] == 67656527:
                print(show)
            out_shows.append(show[['Title', 'StartTime', 'EndTime', 'BrdCstId', 'Description', 'Duration']])
    return out_viewers, out_shows


def get_tsv_viewers(weight_date: str = "20180101", date: str = "20180101",
                    agemin: int = 0, agemax: int = 100) -> pd.DataFrame:

    date_cols = ['UsageDate', 'RecordingDate']
    time_cols = ['ViewingStartTime', 'ViewingTime', 'RecordingStartTime']

    with open(f'{PATH}UsageTimeShifted/UsageTimeShifted_{date}.pin', 'r', encoding='latin-1') as f:
        df = pd.read_csv(f, dtype={**{c: 'object' for c in date_cols},
                                   **{c: 'object' for c in time_cols}})

    # Filtering out channels and guests
    cond0 = df['ViewingActivity'].isin([4, 10])  # TSV activity
    df = df[cond0]

    df['H_P'] = df['HouseholdId'].astype(str) + "_" + df['IndividualId'].astype(str)

    # Filtering out based on ages
    df['age'] = df['H_P'].map(get_age_dict(date))
    df = df[(df['age'] >= agemin) & (df['age'] <= agemax)]

    # Filtering on language
    df = df[df['H_P'].map(get_lang_dict(date)) == 1]

    # Assigining the right dtypes to date cols
    for tc in time_cols[:2]:
        df[tc] = pd.to_datetime(df['UsageDate'] + df[tc].str.zfill(6),
                                format="%Y%m%d%H%M%S")

    df['add_day'] = (df['ViewingStartTime'] > df['ViewingTime']).astype(int)
    df['ViewingTime'] += pd.to_timedelta(df['add_day'], 'd')

    df['duration'] = df['ViewingTime'] - df['ViewingStartTime']
    df['RecordingStartTime'] = pd.to_datetime(df['RecordingDate']
                                              + df['RecordingStartTime'].str.zfill(6),
                                              format="%Y%m%d%H%M%S")
    df['RecordingEndTime'] = df['RecordingStartTime'] + df['duration']

    # Mapping weights
    df['Weights'] = df['H_P'].map(get_weight_dict(weight_date))
    df['Kanton'] = df['H_P'].map(get_kanton_dict(date))

    return df


def df_to_disk_ovn(vw: nested_list_strs, sh: nested_list_strs, date) -> pd.DataFrame:

    lens = [[len(l) for l in ls] for ls in vw]
    lens = [o for subo in lens for o in subo]

    viewers = [[l for l in ls] for ls in vw]
    viewers = [o for subo in viewers for o in subo]
    viewers = pd.concat(viewers, axis=0, ignore_index=False)

    tits = [[l['Title'] for l in ls] for ls in sh]
    tits = [o for subo in tits for o in subo]
    titls = [[o] * l for o, l in zip(tits, lens)]
    titls = [o for subo in titls for o in subo]

    viewers['Title'] = titls

    utits = [[l['Description'] for l in ls] for ls in sh]
    utits = [o for subo in utits for o in subo]
    utitls = [[o] * l for o, l in zip(utits, lens)]
    utitls = [o for subo in utitls for o in subo]

    viewers['Description'] = utitls

    prog_dur = [[l['Duration'] for l in ls] for ls in sh]
    prog_dur = [o for subo in prog_dur for o in subo]
    prog_durs = [[o] * l for o, l in zip(prog_dur, lens)]
    prog_durs = [o for subo in prog_durs for o in subo]

    viewers['program_duration'] = prog_durs

    end = [[l['EndTime'] for l in ls] for ls in sh]
    end = [o for subo in end for o in subo]
    ends = [[o] * l for o, l in zip(end, lens)]
    ends = [o for subo in ends for o in subo]

    viewers['show_endtime'] = ends

    start = [[l['StartTime'] for l in ls] for ls in sh]
    start = [o for subo in start for o in subo]
    starts = [[o] * l for o, l in zip(start, lens)]
    starts = [o for subo in starts for o in subo]

    viewers['show_starttime'] = starts

    bid = [[l['BrdCstId'] for l in ls] for ls in sh]
    bid = [o for subo in bid for o in subo]
    bids = [[o] * l for o, l in zip(bid, lens)]
    bids = [o for subo in bids for o in subo]

    viewers['broadcast_id'] = bids
    viewers['date'] = date

    return viewers


# Update the timeshifted facts table with the help of helper functions according to the flow defined further up
def update_tsv_facts_table(dates):

    delayed_date_cols = ['ViewingStartTime', 'ViewingTime',
                         'RecordingStartTime', 'show_endtime',
                         'show_starttime', 'RecordingEndTime']

    # Read the old file in and update dates with day from latest update
    df_old = pd.DataFrame()
    for i in range(DAYS_IN_PAST):
        date_old = (datetime.now() - timedelta(days=i - 1)).strftime('%Y%m%d')
        if os.path.isfile(LOCAL_PATH + '20190101_%s_delayedviewing_DE_15_49_mG.csv' % date_old):
            dates.update({int(date_old)})
            df_old = pd.read_csv(LOCAL_PATH + '20190101_%s_delayedviewing_DE_15_49_mG.csv' % date_old,
                                 parse_dates=delayed_date_cols, dtype={'Description': str, 'Title': str, 'date': int})

    logging.info('Following dates will be updated %s' % dates)

    # Remove updated entries from the old file
    for date_remove in dates:
        df_old = df_old[df_old['date'] != date_remove]

    stations = get_station_dict()
    df_update = pd.DataFrame()
    out_shows = []
    out_viewers = []

    for date in dates:

        date = str(date)
        date = datetime(year=int(date[0:4]), month=int(date[4:6]), day=int(date[6:8]))
        try:

            logging.info('Updating time-shifted entry at date %s' % date.strftime("%Y%m%d"))

            # Import Schedule
            list_sched = pd.DataFrame()

            for step in range(7):
                sched = get_brc((date-timedelta(days=step)).strftime("%Y%m%d"),
                                (date-timedelta(days=step+1)).strftime("%Y%m%d"))
                sched["station"] = sched["ChannelCode"].map(stations)

                list_sched = pd.concat([list_sched, sched], axis=0, ignore_index=False)

            # Import Live Viewers
            tsvs = get_tsv_viewers(agemax=49, agemin=15, weight_date=date.strftime("%Y%m%d"),
                                   date=date.strftime("%Y%m%d"))
            tsvs["station"] = tsvs["StationId"].map(stations)

            # Map schedule and live-viewers together and append to a list
            viewers, shows = map_viewers_ovn(sched, tsvs)
            out_viewers.append(viewers)
            out_shows.append(shows)

            # Create updated dataframe of dates
            df_new = df_to_disk_ovn(out_viewers, out_shows, date.strftime("%Y%m%d"))
            df_update = pd.concat([df_update, df_new], axis=0, ignore_index=False, sort=True)

        except FileNotFoundError as e:
            logging.info('Did not found file for date %s, will continue with incomplete data' % str(e))

            # Import Schedule
            list_sched = pd.DataFrame()
            single_sched = get_brc_single(date)
            list_sched = pd.concat([list_sched, single_sched], axis=0, ignore_index=False)

            for step in range(6):
                sched = get_brc((date - timedelta(days=int(step+1))).strftime("%Y%m%d"),
                                (date - timedelta(days=int(step))).strftime("%Y%m%d"))
                sched["station"] = sched["ChannelCode"].map(stations)
                list_sched = pd.concat([list_sched, sched], axis=0, ignore_index=False)

            # Import Live Viewers
            tsvs = get_tsv_viewers(agemax=49, agemin=15, weight_date=date.strftime("%Y%m%d"),
                                   date=date.strftime("%Y%m%d"))
            tsvs["station"] = tsvs["StationId"].map(stations)

            # Map schedule and live-viewers together and append to a list
            viewers, shows = map_viewers_ovn(sched, tsvs)
            out_viewers.append(viewers)
            out_shows.append(shows)

            # Create updated dataframe of dates
            df_new = df_to_disk_ovn(out_viewers, out_shows, date.strftime("%Y%m%d"))
            df_update = pd.concat([df_update, df_new], axis=0, ignore_index=False, sort=True)

    logging.info('Created updated time-shifted entries, continuing with concatenation')

    # Concatenate update with old file
    df_updated = pd.concat([df_old, df_update], axis=0, ignore_index=False, sort=True)
    df_updated['date'] = pd.to_numeric(df_updated['date'], downcast='integer')
    df_updated.to_csv(f'{LOCAL_PATH}20190101_{df_updated["date"].max()}_delayedviewing_DE_15_49_mG.csv', index=False)

    # Delete redundant files from directory
    newest = False
    for i in range(DAYS_IN_PAST):
        date = (datetime.now() - timedelta(days=i - 1)).strftime('%Y%m%d')
        if os.path.isfile(LOCAL_PATH + '20190101_%s_delayedviewing_DE_15_49_mG.csv' % date):
            if newest:
                os.remove((LOCAL_PATH + '20190101_%s_delayedviewing_DE_15_49_mG.csv' % date))
            else:
                newest = True

    logging.info('Successfully updates time-shifted facts-table')


######################################
##### PREPROCESSING OF LIVE DATA #####
# ------------------------------------------------------------------------------------------
# Function for the airflow pipeline, main function update_facts_table
# Flow:
# 1. get_live_viewers. -> all live usage from a date
# 2. get_brc. -> all programs from a date
# 3. map_viewers. -> maps viewers to shows from the 2 above files
# 4. df_to_disk. -> saves results to disk.
# ------------------------------------------------------------------------------------------
def map_viewers(sched: pd.DataFrame, lv: pd.DataFrame) -> Tuple[list, list]:
    """
    'sched': schedule dataframe. 'lv': live usage dataframe.
    Maps viewer's live usage to shows.
    """
    out_viewers = []
    out_shows = []

    delete = lv.station.unique()
    sched = sched[sched['station'].isin(delete)]

    for show in zip(sched.values):
        viewers = get_live_viewers_from_show(show, lv)
        if len(viewers) > 0:  # with precomputation not necessary
            out_viewers.append(viewers)
            out_shows.append(show)

    return out_viewers, out_shows


def df_to_disk(vw: nested_list_strs, sh: nested_list_strs, date) -> pd.DataFrame:

    lens = [[len(l) for l in ls] for ls in vw]
    lens = [o for subo in lens for o in subo]

    viewers = [[l for l in ls] for ls in vw]
    viewers = [o for subo in viewers for o in subo]
    viewers = pd.concat(viewers, axis=0, ignore_index=False)

    tits = [[l[0][0] for l in ls] for ls in sh]
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


def filter_live_usage(df_usagelive, prog, start, date, chan):
    """Helper function to filter an usage DF on the segmented programs of a channel"""
    # Was used with "old" pin functions, most probably not needed with the preprocessed PIN files
    cond = False
    sts_dus = get_start_and_dur(prog,start,date,chan)
    for el in sts_dus:
        new_cond = (df_usagelive['EndTime'] >= el[0]) & (df_usagelive['StartTime'] <= el[0] + el[1])
        cond = cond | new_cond
    return df_usagelive[cond & (df_usagelive['StationId'] == get_station_id(chan))]


def imp_live_viewers(date, stations, single=False):

    lv = get_live_viewers(agemax=49, agemin=15, date=date.strftime("%Y%m%d"))
    if not single:
        lv = pd.concat([lv, get_live_viewers(agemax=49, agemin=15,
                                             date=(date + timedelta(days=1)).strftime("%Y%m%d"))],
                       axis=0, ignore_index=False)
    lv["station"] = lv["StationId"].map(stations)
    lv = lv[['StartTime', 'EndTime', 'H_P', 'Weights', 'station', 'Kanton']]
    lv['Weights'] = lv['H_P'].map(get_weight_dict((date.strftime("%Y%m%d"))))

    return lv


def imp_brdcst_sched(date, stations, single=False):

    if not single:
        sched = get_brc(date.strftime("%Y%m%d"), (date + timedelta(days=1)).strftime("%Y%m%d"))
    else:
        sched = get_brc_single(date.strftime("%Y%m%d"))

    sched["station"] = sched["ChannelCode"].map(stations).astype(str)
    sched = sched[['Date', 'Title', 'StartTime', 'EndTime', 'BrdCstId', 'Description', 'Duration', 'station']]

    return sched


# Update the live facts table with the help of helper functions according to the flow further up in the file
def update_live_facts_table(dates):

    date_cols = ['show_endtime', 'show_starttime', 'StartTime', 'EndTime']

    # Read the old file in and update dates with day from latest update
    df_old = pd.DataFrame()
    for i in range(DAYS_IN_PAST):
        date_old = (datetime.now() - timedelta(days=i - 1)).strftime('%Y%m%d')
        if os.path.isfile(LOCAL_PATH + '20190101_%s_Live_DE_15_49_mG.csv' % date_old):
            dates.update({int(date_old)})
            df_old = pd.read_csv(LOCAL_PATH + '20190101_%s_Live_DE_15_49_mG.csv' % date_old, parse_dates=date_cols,
                                 dtype={'Description': str, 'Title': str, 'date': int})

    logging.info('Following dates will be updated %s' % dates)

    # Remove updated entries from the old file
    for date_remove in dates:
        df_old = df_old[df_old['date'] != date_remove]

    stations = get_station_dict()
    df_update = pd.DataFrame()
    out_shows = []
    out_viewers = []

    for date in dates:

        date = str(date)
        date = datetime(year=int(date[0:4]), month=int(date[4:6]), day=int(date[6:8]))

        try:
            logging.info('Updating table entry at date %s' % date.strftime("%Y%m%d"))

            # Import live-viewers
            lv = imp_live_viewers(date, stations, False)

            # Import Broadcasting Schedule
            sched = imp_brdcst_sched(date, stations, False)

            # Map schedule and live-viewers together and append to a list
            viewers, shows = map_viewers(sched, lv)
            out_shows.append(shows)
            out_viewers.append(viewers)

            # Concatenate to a dataframe which will be concatenated to the old file
            df_new = df_to_disk(out_viewers, out_shows, date.strftime("%Y%m%d"))
            df_update = pd.concat([df_update, df_new], axis=0, ignore_index=False, sort=True)

        # If following day is not available, handle it here with computing just the single day for now
        except FileNotFoundError as e:
            logging.info("%s, no consecutive file found, single evaluation" % str(e))

            # Import live-viewers
            lv = imp_live_viewers(date, stations, True)

            # Import Broadcasting Schedule
            sched = imp_brdcst_sched(date, stations, True)

            # Map schedule and live-viewers together and append to a list
            viewers, shows = map_viewers(sched, lv)
            out_shows.append(shows)
            out_viewers.append(viewers)

            df_new = df_to_disk(out_viewers, out_shows, date.strftime("%Y%m%d"))
            df_update = pd.concat([df_update, df_new], axis=0, ignore_index=False, sort=True)

    logging.info('Created updated live entries, continuing with concatenation')

    # Concatenate update with old file
    df_updated = pd.concat([df_old, df_update], axis=0, ignore_index=False)
    df_updated['date'] = pd.to_numeric(df_updated['date'], downcast='integer')
    df_updated.to_csv(f'{LOCAL_PATH}20190101_{df_updated["date"].max()}_Live_DE_15_49_mG.csv', index=False)

    # Delete redundant files from directory
    newest = False
    for i in range(DAYS_IN_PAST):
        date = (datetime.now() - timedelta(days=i - 1)).strftime('%Y%m%d')
        if os.path.isfile(LOCAL_PATH + '20190101_%s_Live_DE_15_49_mG.csv' % date):
            if newest:
                os.remove((LOCAL_PATH + '20190101_%s_Live_DE_15_49_mG.csv' % date))
            else:
                newest = True

    logging.info('Successfully updated live facts-table')
