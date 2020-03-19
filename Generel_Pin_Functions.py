import datetime
import pandas as pd
import getpass
from collections import Counter
from datetime import timedelta, datetime

user = getpass.getuser()
LOCAL_PATH = f'/home/{user}/Documents/PIN_Data/'


def get_dropbox_facts_table(year=None, table_type=None):

    user = getpass.getuser()
    df_lv = pd.DataFrame()
    df_tsv = pd.DataFrame()

    if (table_type == 'live') or (table_type is None):
        try:
            date_cols = ['show_endtime', 'show_starttime', 'StartTime', 'EndTime',
                         'Channel_StartTime', 'Channel_EndTime']
            if (year is None) or (year == str(datetime.now().year)):
                df_lv = pd.read_csv(f'/home/{user}/Dropbox (3 Plus TV Network AG)/3plus_ds_team/'
                                    f'Projects/data/Processed_pin_data/updated_live_facts_table.csv',
                                    header=0,
                                    parse_dates=date_cols,
                                    dtype={'date': str, 'station': str, 'broadcast_id': float, 'Title': str,
                                           'Description': str, 'program_duration': float, 'H_P': str, 'Age': float,
                                           'Gender': str, 'Kanton': float, 'duration': float, 'Weights': float})
            else:
                df_lv = pd.read_csv(f'/home/{user}/Dropbox (3 Plus TV Network AG)/3plus_ds_team/'
                                    f'Projects/data/Processed_pin_data/{year}0101_{year}1231_Live_DE_15_49_mG.csv',
                                    header=0,
                                    parse_dates=date_cols,
                                    dtype={'date': str, 'station': str, 'broadcast_id': float, 'Title': str,
                                           'Description': str, 'program_duration': float, 'H_P': str, 'Age': float,
                                           'Gender': str, 'Kanton': float, 'duration': float, 'Weights': float})
        except FileNotFoundError as e:
            print(f'Live facts table does not exist on dropbox or is saved at a different path as given, {e}')

    if (table_type == 'tsv') or (table_type is None):
        try:
            delayed_date_cols = ['ViewingStartTime', 'ViewingTime', 'RecordingStartTime', 'RecordingEndTime',
                                 'show_endtime', 'show_starttime', 'Channel_StartTime', 'Channel_EndTime']
            if (year is None) or (year == str(datetime.now().year)):
                df_tsv = pd.read_csv(f'/home/{user}/Dropbox (3 Plus TV Network AG)/3plus_ds_team/'
                                     f'Projects/data/Processed_pin_data/updated_tsv_facts_table.csv',
                                     header=0,
                                     parse_dates=delayed_date_cols,
                                     dtype={'date': str, 'station': str, 'broadcast_id': float, 'Title': str,
                                            'Description': str, 'program_duration': float, 'RecordingDate': float,
                                            'H_P': str, 'Age': float, 'Gender': str, 'Kanton': float, 'duration': float,
                                            'Weights': float})
            else:
                df_tsv = pd.read_csv(f'/home/{user}/Dropbox (3 Plus TV Network AG)/3plus_ds_team/'
                                     f'Projects/data/Processed_pin_data/'
                                     f'{year}0101_{year}1231_delayedviewing_DE_15_49_mG.csv',
                                     header=0,
                                     parse_dates=delayed_date_cols,
                                     dtype={'date': str, 'station': str, 'broadcast_id': float, 'Title': str,
                                            'Description': str, 'program_duration': float, 'RecordingDate': float,
                                            'H_P': str, 'Age': float, 'Gender': str, 'Kanton': float, 'duration': float,
                                            'Weights': float})

        except (FileNotFoundError, ValueError) as e:
            print(f'Tsv facts table does not exist on dropbox or is saved at a different path as given. Error: {e}')

    if df_lv.empty and df_tsv.empty:
        print(f'Your given type {table_type} does not exist, chose "live", "tsv" or "None" for both tables'
              f' It also could be that the path are incorrect')

    return df_lv, df_tsv


def get_gender_dict(date):
    """
    Return viewers gender on a given date
    :param date: Day of interest, string
    :return: Dict of the viewers gender
    """
    try:
        with open(f'{LOCAL_PATH}SocDem/SocDem_{date}.pin', 'r', encoding='latin-1') as f:
            df_socdem = pd.read_csv(f, dtype='int32', usecols=['SampleId', 'Person', 'SocDemVal2'])
    except FileNotFoundError:
        print('It is required to have the .pin files locally on your machine')
        exit()

    critcode = pd.read_csv(f'{LOCAL_PATH}CritCode.pin', encoding='latin-1')
    critcode = critcode[critcode['CritId'] == 2]
    genders = {x: y for x, y in zip(critcode['SocDemVal'], critcode['Description'])}

    df_socdem['H_P'] = df_socdem['SampleId'].astype(str) + "_" + df_socdem['Person'].astype(str)
    gender_dict = {a: b for a, b in zip(df_socdem['H_P'].values.tolist(),
                                        df_socdem['SocDemVal2'].map(genders).values.tolist())}
    return gender_dict


def get_kanton_dict(date):
    """
    Returns viewer's Kanton on a given date
    :param date: Day of interest as string
    :return: Dict of the viewers kanton
    """
    try:
        with open(f'{LOCAL_PATH}SocDem/SocDem_{date}.pin', 'r', encoding='latin-1') as f:
            df_socdem = pd.read_csv(f, dtype='int32', usecols=['SampleId', 'Person', 'SocDemVal5'])
    except FileNotFoundError:
        print('It is required to have the .pin files locally on your machine')
        exit()

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
    try:
        with open(f'{LOCAL_PATH}SocDem/SocDem_{date}.pin', 'r', encoding='latin-1') as f:
            df_socdem = pd.read_csv(f, dtype='int32', usecols=['SampleId', 'Person', 'SocDemVal4'])
    except FileNotFoundError:
        print('It is required to have the .pin files locally on your machine')
        exit()

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
    try:
        with open(f'{LOCAL_PATH}SocDem/SocDem_{date}.pin', 'r', encoding='latin-1') as f:
            df_socdem = pd.read_csv(f, dtype='int32', usecols=['SampleId', 'Person', 'SocDemVal1'])
    except FileNotFoundError:
        print('It is required to have the .pin files locally on your machine')
        exit()

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

    for i in range(8):
        try:
            with open(f'{LOCAL_PATH}SocDem/SocDem_{(date - timedelta(days=i)).strftime("%Y%m%d")}.pin',
                      'r', encoding='latin-1') as f:
                df_socdem = pd.read_csv(f, dtype='int32', usecols=['FileDate', 'SampleId', 'Person', 'SocDemVal1'])
        except FileNotFoundError:
            with open(f'/media/floosli/UeliTheDisk/Pin_Data/SocDem_old/'
                      f'SocDem_{(date - timedelta(days=i)).strftime("%Y%m%d")}.pin', 'r', encoding='latin-1') as f:
                df_socdem = pd.read_csv(f, dtype={'Date': 'object', 'StartTime': 'object',
                                                  'ChannelCode': 'int'}, error_bad_lines=False)

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
    try:
        with open(f'{LOCAL_PATH}BrdCst/BrdCst_{date}.pin', 'r', encoding='latin-1') as f:
            brc = pd.read_csv(f, dtype={'Date': 'object', 'StartTime': 'object',
                                        'ChannelCode': 'int'})
    except FileNotFoundError:
        print('It is required to have the .pin files locally on your machine')
        exit()

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
        print('It is required to have the .pin files locally on your machine')
        exit()

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
    df_usagelive['Gender'] = df_usagelive['H_P'].map(get_gender_dict(date))

    return df_usagelive


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
    df['Age'] = df[['H_P', 'RecordingDate']].T.apply(tuple).map(get_age_dict_ovn(date))
    df = df[(df['Age'] >= agemin) & (df['Age'] <= agemax)]

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
    df['Gender'] = df['H_P'].map(get_gender_dict(date))

    return df


def get_weight_dict(date):
    """
    Returns viewer's weight on a given date in form of a dict (H_P: Weight)
    :param date: Day of interest
    :return: Dictionary of the viewers Id and the respective weight
    """
    try:
        with open(f'{LOCAL_PATH}Weight/Weight_{date}.pin',
                  'r', encoding='latin-1') as f:
            df_wei = pd.read_csv(f)
    except FileNotFoundError:
        print('It is required to have the .pin files locally on your machine')
        exit()

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
        try:
            with open(f'{LOCAL_PATH}Weight/Weight_{(date-timedelta(days=i)).strftime("%Y%m%d")}.pin',
                      'r', encoding='latin-1') as f:
                df_wei = pd.read_csv(f)
        except FileNotFoundError:
            with open(f'/media/floosli/UeliTheDisk/Pin_Data/Weight_old/'
                      f'Weight_{(date-timedelta(days=i)).strftime("%Y%m%d")}.pin', 'r',
                      encoding='latin-1') as f:
                df_wei = pd.read_csv(f, dtype={'Date': 'object', 'StartTime': 'object',
                                               'ChannelCode': 'int'}, error_bad_lines=False)

        df_wei['H_P'] = df_wei['SampledIdRep'].astype(str) + "_" + df_wei['PersonNr'].astype(str)
        temp_dict = {(a, c): b for a, c, b in zip(df_wei['H_P'].values.tolist(), df_wei['WghtDate'].values.tolist(),
                                                  df_wei['PersFactor'].values.tolist())}
        weight_dict.update(temp_dict)

    return weight_dict


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


def compute_individual_marketshare_live(df, dates):
    """
    Compute the Marketshare based on the facts table
    The values will differ from the infosys extract as we do not have access to all the channels
    """
    org = df.copy(deep=True)
    org['Marketshare_live'] = 0

    for date in dates:
        df = org[org['date'] == int(date)]

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

            org.iloc[row.name, org.columns.get_loc('Marketshare_live')] = row['individual_Rt-T_live'] / whole_rt * 100

    return org


def compute_individual_marketshare_tsv(tsv, ratings, dates):

    org_tsv = tsv.copy(deep=True)
    org_tsv['Marketshare_tsv'] = 0
    live_ma = pd.DataFrame(columns=['broadcast_id', 'Marketshare_tsv', 'Rt_live'])

    for date in dates:

        tsv = org_tsv[(org_tsv['RecordingDate'] == int(date))]

        list_brc = pd.DataFrame()
        df_usg = pd.DataFrame()
        for step in range(8):

            brc = get_brc((datetime.strptime(date, "%Y%m%d") - timedelta(days=step)).strftime("%Y%m%d"))
            list_brc = pd.concat([list_brc, brc], axis=0, ignore_index=False)

            tsv_usg = get_tsv_viewers((datetime.strptime(date, "%Y%m%d") + timedelta(days=step)).strftime("%Y%m%d"),
                                      agemin=15, agemax=49)
            df_usg = pd.concat([df_usg, tsv_usg], axis=0, ignore_index=False)

        df_lv_usg = get_live_viewers(date, agemax=49, agemin=15)

        for index, row in tsv.iterrows():

            whole_rt = 0
            temp_brc = list_brc[list_brc['BrdCstId'] == row['broadcast_id']]

            try:
                rt_live = ratings[ratings['broadcast_id'] == row['broadcast_id']]['individual_Rt-T_live'].values[0]
            except IndexError:
                rt_live = 0

            for idx, seq in temp_brc.iterrows():

                df_lv_usg['Duration'] = ((df_lv_usg['EndTime'].clip(upper=seq['show_endtime']) -
                                          df_lv_usg['StartTime'].clip(lower=seq['show_starttime'])).dt.total_seconds())
                df_lv_usg['Rating'] = (df_lv_usg['Duration'] * df_lv_usg['Weights']) / (seq['Duration'])
                whole_rt += df_lv_usg[df_lv_usg['Rating'] > 0]['Rating'].sum()

                df_usg['Duration'] = ((df_usg['RecordingEndTime'].clip(upper=seq['show_endtime']) -
                                       df_usg['RecordingStartTime'].clip(lower=seq['show_starttime'])).dt.total_seconds())
                df_usg['Rating'] = (df_usg['Duration'] * df_usg['Weights']) / (seq['Duration'])
                whole_rt += df_usg[df_usg['Rating'] > 0]['Rating'].sum()

            if whole_rt == 0:
                continue
            org_tsv.loc[row.name, 'Marketshare_tsv'] = (row['individual_Rt-T_tsv'] * 100)\
                                                                                 / whole_rt

            temp = pd.DataFrame(data=[[row['broadcast_id'], ((rt_live*100) / whole_rt), rt_live]],
                                columns=['broadcast_id', 'Marketshare_tsv', 'Rt_live'])
            live_ma = pd.concat([live_ma, temp], axis=0, sort=True)

    return org_tsv, live_ma


def compute_zeitschiene_rt(starttime, endtime, interval, source=None,
                           channels=None, gender=None, min_age=None, max_age=None):
    """
    :param starttime: pandas.Timestamp(year, month, day, hour, minute, second) of the start time
    :param endtime: pandas.Timestamp(year, month, day, hour, minute, second) of the end time
    :param interval: Integer of seconds of the duration of the interval
    :param source: Either live or tsv, will compute the zeitschiene for the chosen source, if None -> live
    :param channels: List of channels to respect, if None take all
    :param gender: 'Männer' or 'Frauen'
    :param min_age: Minimum age of the group you look at, Integer
    :param max_age: Maximum age of the group you look at, Integer
    :return: Zeitschiene as pd.Dataframe for the desired specifications with 3 columns:
    ['start_time', 'end_time', 'station', 'rating']
    """
    year = starttime.year
    df = pd.DataFrame()

    if source in [None, 'live']:
        df = get_dropbox_facts_table(year=str(year), table_type='live')[0]
    elif source == 'tsv':
        df = get_dropbox_facts_table(year=str(year), table_type='tsv')[1]
    else:
        print('Please choose a valid source or leave it empty')
        exit()

    assert endtime > starttime
    assert not df.empty
    assert interval > 0
    assert (min_age is None) or ((min_age >= 15) and (min_age <= 49))
    assert (max_age is None) or ((max_age >= 15) and (max_age <= 49))

    if gender and (gender in ['Männer', 'Frauen']):
        df = df[df['Gender'] == gender]
    if channels:
        df = df[df['station'].isin(channels)]
        if df.empty:
            print(f'{channels} are not in the dataframe represented.')
            exit()
    if min_age:
        df = df[df['Age'] >= min_age]
    if max_age:
        df = df[df['Age'] <= max_age]

    zeitschiene = pd.DataFrame(columns=['start_time', 'end_time', 'station', 'rating'])
    start_int = starttime

    while True:

        temp = pd.DataFrame()
        if start_int >= endtime:
            break

        end_int = start_int + timedelta(seconds=interval)

        temp['station'] = df['station']
        temp['rating'] = ((df['Channel_EndTime'].clip(upper=end_int) -
                          df['Channel_StartTime'].clip(lower=start_int)).dt.total_seconds()
                          * df['Weights']) / interval
        temp.loc[temp['rating'] < 0, 'rating'] = 0
        temp = temp.groupby(['station'])['rating'].sum().reset_index(drop=False)
        temp['start_time'] = start_int
        temp['end_time'] = end_int
        zeitschiene = pd.concat([zeitschiene, temp], axis=0, sort=True)

        start_int = end_int

    zeitschiene = zeitschiene[['start_time', 'end_time', 'station', 'rating']]

    return zeitschiene


def compute_zeitschiene_ma(starttime, endtime, interval, source=None,
                           channels=None, gender=None, min_age=None, max_age=None):
    year = starttime.year
    df = pd.DataFrame()

    if source in [None, 'live']:
        df = get_dropbox_facts_table(year=str(year), table_type='live')[0]
    elif source == 'tsv':
        df = get_dropbox_facts_table(year=str(year), table_type='tsv')[1]
    else:
        print('Please choose a valid source or leave it empty')
        exit()

    assert endtime > starttime
    assert not df.empty
    assert interval > 0
    assert (min_age is None) or ((min_age >= 15) and (min_age <= 49))
    assert (max_age is None) or ((max_age >= 15) and (max_age <= 49))

    if gender and (gender in ['Männer', 'Frauen']):
        df = df[df['Gender'] == gender]
    if channels:
        df = df[df['station'].isin(channels)]
        if df.empty:
            print(f'{channels} are not in the dataframe represented.')
            exit()
    if min_age:
        df = df[df['Age'] >= min_age]
    if max_age:
        df = df[df['Age'] <= max_age]

    zeitschiene = pd.DataFrame(columns=['start_time', 'end_time', 'station', 'rating'])
    start_int = starttime

    while True:

        temp = pd.DataFrame()
        if start_int >= endtime:
            break

        end_int = start_int + timedelta(seconds=interval)

        temp['station'] = df['station']
        temp['rating'] = ((df['Channel_EndTime'].clip(upper=end_int) -
                           df['Channel_StartTime'].clip(lower=start_int)).dt.total_seconds()
                          * df['Weights']) / interval
        temp.loc[temp['rating'] < 0, 'rating'] = 0
        temp = temp.groupby(['station'])['rating'].sum().reset_index(drop=False)
        temp['start_time'] = start_int
        temp['end_time'] = end_int
        zeitschiene = pd.concat([zeitschiene, temp], axis=0, sort=True)

        start_int = end_int

    zeitschiene = zeitschiene[['start_time', 'end_time', 'station', 'rating']]

    return zeitschiene
