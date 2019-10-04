PATH = '/home/floosli/Documents/PIN_Data_Test/'

import datetime
import numpy as np
import pandas as pd
import csv
import time

from collections import Counter
from typing import List, Tuple, Dict
nested_list_strs = List[List[str]]


#########################################################
##### GENERAL PURPOSE FILE OPENERS AND DICTIONARIES #####


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


def get_brc_(date, seqzero=True):
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
    if seqzero:
        segmented_prog_duration_dict = (brc[(brc['SumPieces'] > 0)
                                            & (brc['BrdCstSeq'] > 0)]
                                        .groupby(['BrdCstId'])['Duration'].sum().to_dict())

        single_prog_duration_dict = (brc[(brc['SumPieces'] == 0)
                                         & (brc['BrdCstSeq'] == 0)]
                                     .groupby(['BrdCstId'])['Duration'].nth(0).to_dict())
        prog_duration_dict = {**segmented_prog_duration_dict,
                              **single_prog_duration_dict}
        brc = brc[~((brc['SumPieces'] > 0) & (brc['BrdCstSeq'] == 0))]
        brc['Duration'] = brc['BrdCstId'].map(prog_duration_dict)
    return brc


def get_brc(date0, date1, seqzero=True):
    br0 = brc_file(date0, seqzero)
    br1 = brc_file(date1, seqzero)
    br1 = br1[br1['BrdCstId'].isin(br0['BrdCstId'])]
    brc = pd.concat([br0, br1], axis=0)
    if seqzero:
        segmented_prog_duration_dict = (brc[(brc['SumPieces'] > 0)
                                            & (brc['BrdCstSeq'] > 0)]
                                        .groupby(['BrdCstId'])['Duration'].sum().to_dict())

        single_prog_duration_dict = (brc[(brc['SumPieces'] == 0)
                                         & (brc['BrdCstSeq'] == 0)]
                                     .groupby(['BrdCstId'])['Duration'].nth(0).to_dict())
        prog_duration_dict = dict((Counter(segmented_prog_duration_dict)
                                   + Counter(single_prog_duration_dict)))
        brc = brc[~((brc['SumPieces'] > 0) & (brc['BrdCstSeq'] == 0))]
        brc['Duration'] = brc['BrdCstId'].map(prog_duration_dict)
    return brc


def single_brc(date):
    brc = brc_file(date, True)

    segmented_prog_duration_dict = (brc[(brc['SumPieces'] > 0)
                                        & (brc['BrdCstSeq'] > 0)]
                                    .groupby(['BrdCstId'])['Duration'].sum().to_dict())

    single_prog_duration_dict = (brc[(brc['SumPieces'] == 0)
                                     & (brc['BrdCstSeq'] == 0)]
                                 .groupby(['BrdCstId'])['Duration'].nth(0).to_dict())
    prog_duration_dict = dict((Counter(segmented_prog_duration_dict)
                               + Counter(single_prog_duration_dict)))
    brc = brc[~((brc['SumPieces'] > 0) & (brc['BrdCstSeq'] == 0))]
    brc['Duration'] = brc['BrdCstId'].map(prog_duration_dict)

    return brc


def brc_file(date, seqzero=True):
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


######################################
##### PREPROCESSING OF LIVE DATA #####
# Flow:
# 1. get_live_viewers. -> all live usage from a date
# 2. get_brc. -> all programs from a date
# 3. map_viewers. -> maps viewers to shows from the 2 above files
# 4. df_to_disk. -> saves results to disk.

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

    df_usagelive['add_day'] = (df_usagelive['StartTime'] > df_usagelive['EndTime']).astype(int)  # boolean
    df_usagelive['EndTime'] += pd.to_timedelta(df_usagelive['add_day'], 'd')  # TODO
    df_usagelive['Weights'] = df_usagelive['H_P'].map(get_weight_dict(date))
    df_usagelive['Kanton'] = df_usagelive['H_P'].map(get_kanton_dict(date))

    # return df
    return df_usagelive


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
        if len(viewers) > 0:
            out_viewers.append(viewers)
            out_shows.append(show)
    return out_viewers, out_shows


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


def df_to_disk(vw: nested_list_strs, sh: nested_list_strs, date) -> pd.DataFrame:
    lens = [[len(l) for l in ls] for ls in vw]
    lens = [o for subo in lens for o in subo]

    viewers = [[l for l in ls] for ls in vw]
    viewers = [o for subo in viewers for o in subo]
    viewers = pd.concat(viewers, axis=0)

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


def filter_live_usage(df_usagelive,prog,start,date,chan) :
    """Helper function to filter an usage DF on the segmented programs of a channel"""
    # Was used with "old" pin functions, most probably not needed with the preprocessed PIN files
    cond = False
    sts_dus = get_start_and_dur(prog,start,date,chan)
    for el in sts_dus :
        new_cond = (df_usagelive['EndTime'] >= el[0]) & (df_usagelive['StartTime'] <= el[0] + el[1])
        cond = cond | new_cond
    return df_usagelive[cond & (df_usagelive['StationId'] == get_station_id(chan))]


def get_start_and_dur(prog_name,start_hour,date_,chan_name) :
    """Returns exact starting time and duration of program given by prog_name, start_hour, date, chan_name"""
    # Was used with "old" pin functions, most probably not needed with the preprocessed PIN files
    with open(f'{PATH}BrdCst/BrdCst_{date_}.pin','r',encoding='latin-1') as f :
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
        else : # Segmented program, returnging start and durations of all segments
            toret = []
            for i in range(1,int(df_brc.loc[df_brc['BrdCstId'] == progID,'SumPieces'].iloc[0]) + 1) :
                toret.append([pd.Timestamp.combine(date = pd.to_datetime(str(date_),format="%Y%m%d"),
                                                   time = pd.to_datetime(exact_start[i],format="%H%M%S").time()),
                              datetime.timedelta(seconds=int(duration[i]))])
            return toret