import datetime
import os
import pickle
import logging
import calendar
import pandas as pd
import Airflow_Variables
import Transformations_Daily_Report as tdr
import Transformations_Weekly_Reports as twr
import Live_Facts_Table
import TSV_Facts_Table
import shutil
import Generel_Pin_Functions as gpf

from collections import Counter
from datetime import timedelta, datetime
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
TSV_DAYS = Airflow_var.days_tsv
YEAR = Airflow_var.year
MONTH = Airflow_var.month
DAY = Airflow_var.day
START = Airflow_var.start
END = Airflow_var.end
DAYS_IN_YEAR = Airflow_var.days_in_year
ADJUST_YEAR = Airflow_var.adjust_year
CHANNELS = Airflow_var.relevant_channels
HEATMAP_PATH = Airflow_var.heatmap_path


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
        with open(f'/media/floosli/UeliTheDisk/Pin_Data/SocDem_old/SocDem_{date}.pin', 'r', encoding='latin-1') as f:
            df_socdem = pd.read_csv(f, dtype='int32', usecols=['SampleId', 'Person', 'SocDemVal2'])

    critcode = pd.read_csv('/home/floosli/Documents/PIN_Data/CritCode.pin', encoding='latin-1')
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
        with open(f'/media/floosli/UeliTheDisk/Pin_Data/SocDem_old/SocDem_{date}.pin', 'r', encoding='latin-1') as f:
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
    try:
        with open(f'{LOCAL_PATH}SocDem/SocDem_{date}.pin', 'r', encoding='latin-1') as f:
            df_socdem = pd.read_csv(f, dtype='int32', usecols=['SampleId', 'Person', 'SocDemVal4'])
    except FileNotFoundError:
        with open(f'/media/floosli/UeliTheDisk/Pin_Data/SocDem_old/SocDem_{date}.pin', 'r', encoding='latin-1') as f:
            df_socdem = pd.read_csv(f, dtype='int32', usecols=['SampleId', 'Person', 'SocDemVal4'],
                                    error_bad_lines=False)

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
    try:
        with open(f'{LOCAL_PATH}Weight/Weight_{date}.pin',
                  'r', encoding='latin-1') as f:
            df_wei = pd.read_csv(f)
    except FileNotFoundError:
        with open(
                f'/media/floosli/UeliTheDisk/Pin_Data/Weight_old/Weight_{date}.pin', 'r',
                encoding='latin-1') as f:
            df_wei = pd.read_csv(f, dtype={'Date': 'object', 'StartTime': 'object',
                                           'ChannelCode': 'int'}, error_bad_lines=False)

    df_wei['H_P'] = df_wei['SampledIdRep'].astype(str) + "_" + df_wei['PersonNr'].astype(str)
    weight_dict = {a: b for a, b in zip(df_wei['H_P'].values.tolist(),
                                        df_wei['PersFactor'].values.tolist())}
    return weight_dict


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
        with open(f'/media/floosli/UeliTheDisk/Pin_Data/SocDem_old/SocDem_{date}.pin', 'r', encoding='latin-1') as f:
            df_socdem = pd.read_csv(f, dtype='int32', usecols=['SampleId', 'Person', 'SocDemVal1'])

    df_socdem['H_P'] = df_socdem['SampleId'].astype(str) + "_" + df_socdem['Person'].astype(str)
    age_dict = {a: b for a, b in zip(df_socdem['H_P'].values.tolist(),
                                     df_socdem['SocDemVal1'].values.tolist())}
    return age_dict


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


# ----------------------------------------------------------------------------------------------------------------------
def compute_complete_viewers_table():
    """
    Compute the viewers who watch a show and save the weighted duration of every instance in a pickle file
    :return: None
    """
    end = str(YEAR) + str(MONTH) + str(DAY)
    if ADJUST_YEAR < 0:
        end = str(YEAR) + '1231'

    date = datetime.strptime(START, '%Y%m%d')
    dates = set()

    for i in range(DAYS_IN_YEAR):
        add = date.strftime('%Y%m%d')
        if add == end:
            dates.update({int(add)})
            break
        dates.update([int(add)])
        date = date + timedelta(days=1)

    for channel in CHANNELS:
        tdr.compute_viewers_of_channel(channel, dates)


def create_live_facts_table():
    """
    Create a new live facts table up to the present day
    To change the year of computation adjust the year variable to your desired one
    by default it will be the current year
    :return: None
    """
    logging.info('Creating live facts table')

    cond = False

    end = str(YEAR) + str(MONTH) + str(DAY)
    start = str(YEAR) + '0101'
    if ADJUST_YEAR < 0:
        end = str(YEAR) + '1231'

    end_date = datetime.strptime(end, '%Y%m%d')
    date = datetime.strptime(start, '%Y%m%d')

    leap = 0
    if calendar.isleap(YEAR):
        leap = 1

    # Detection of checkpoint
    for i in range(DAYS_IN_YEAR+leap):
        pot_date = (end_date - timedelta(days=i)).strftime('%Y%m%d')
        if os.path.isfile(LOCAL_PATH + '%s_%s_Live_DE_15_49_mG.csv' % (start, pot_date)):
            date = datetime.strptime(pot_date, '%Y%m%d') + timedelta(days=1)
            break
    logging.info('Will update from date %s' % date)
    while True:
        logging.info('Updating still ongoing from date %s' % date)

        dates = set()
        for i in range(8):
            add = date.strftime('%Y%m%d')
            if add == end:
                cond = True
                dates.update({str(add)})
                break
            dates.update({str(add)})
            date = date + timedelta(days=1)
        print(dates)
        Live_Facts_Table.update_live_facts_table(dates, end_day=END, adjust_year=ADJUST_YEAR,
                                                 days_in_year=DAYS_IN_YEAR, start_day=START)

        if cond:
            logging.info("Reached the end date successfully, finished live table")
            break


def create_tsv_facts_table():
    """
    Create a new live facts table up to the present day
    To change the year of computation adjust the year variable to your desired one
    by default it will be the current year
    :return: None
    """
    logging.info('Creating tsv facts table')

    cond = False

    end = str(YEAR) + str(MONTH) + str(DAY)
    if ADJUST_YEAR < 0:
        end = str(YEAR) + '1231'

    end_date = datetime.strptime(end, '%Y%m%d')
    date = datetime.strptime('20170108', '%Y%m%d')

    leap = 0
    if calendar.isleap(YEAR):
        leap = 1

    # Detection of checkpoint
    for i in range(DAYS_IN_YEAR+leap):
        pot_date = (end_date - timedelta(days=i)).strftime('%Y%m%d')
        if os.path.isfile(LOCAL_PATH + '%s_%s_delayedviewing_DE_15_49_mG.csv' % (START, pot_date)):
            date = datetime.strptime(pot_date, '%Y%m%d') + timedelta(days=1)
            break

    logging.info('Will update from date %s' % date)
    while True:
        logging.info('Updating still ongoing from date %s' % date)
        dates = set()

        for i in range(7):
            add = date.strftime('%Y%m%d')

            if add == end:
                cond = True
                dates.update([int(add)])
                break
            dates.update([int(add)])
            date = date + timedelta(days=1)

        TSV_Facts_Table.update_tsv_facts_table(dates, end_day=END, start_day=START, adjust_year=ADJUST_YEAR,
                                               days_in_year=DAYS_IN_YEAR, tsv_days=TSV_DAYS)

        if cond:
            logging.info("Reached the end date successfully, finished tsv table")
            break


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
                    twr.update_heatmap(add, threshold_duration=False)
                except FileNotFoundError as e:
                    logging.info(str(e))
                    date = date + timedelta(days=1)
                    break
            try:
                twr.update_heatmap(add, threshold_duration=False)
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


def compute_individual_marketshare_live(df, dates):
    """
    Compute the Marketshare based on the facts table
    The values will differ from the infosys extract as we do not have access to all the channels
    """
    org = df.copy(deep=True)
    org['Marketshare_live'] = 0

    for date in dates:
        df = org[org['date'] == str(date)]

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

            if whole_rt == 0:
                continue
            org.loc[row.name, 'Marketshare_live'] = (row['individual_Rt-T_live'] * 100) / whole_rt

    return org


def compute_individual_marketshare_tsv(tsv, ratings, dates):

    org_tsv = tsv.copy(deep=True)
    org_tsv['Marketshare_tsv'] = 0
    live_ma = pd.DataFrame(columns=['broadcast_id', 'Marketshare_tsv', 'Rt_live'])

    for date in dates:

        tsv = org_tsv[(org_tsv['RecordingDate'] == int(date))]

        list_brc = pd.DataFrame()
        df_usg = pd.DataFrame()
        for step in range(TSV_DAYS):

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


def compare_values_live(i):

    df = pd.read_csv(f'/home/floosli/Documents/PIN_Data/20{i}0101_20{i}1231_Live_DE_15_49_mG.csv',
                     usecols=['broadcast_id', 'duration', 'Weights', 'program_duration',
                              'Title', 'Description', 'date'])
    df = add_individual_ratings(df)

    result = df.groupby(['Title', 'broadcast_id'])['individual_Rt-T_live'].sum().to_frame().reset_index()

    solution = pd.read_excel(f'/home/floosli/Downloads/test_rt_20{i}.xlsx', sheet_name='Sendungen(Live)', header=1,
                             usecols=['Title', 'Description', 'Channel', 'Rt-T', 'MA-%', 'BrdCstId'])
    solution = solution.drop([0])
    solution = solution.rename(columns={'Channel': 'station', 'BrdCstId': 'broadcast_id'})
    solution = solution.groupby(['Title', 'broadcast_id'])['Rt-T'].sum().to_frame().reset_index()

    diff = solution.merge(result, on='broadcast_id')
    diff['Difference'] = diff['Rt-T'] - diff['individual_Rt-T_live']
    diff.to_csv(f'/home/floosli/Downloads/difference_{i}.xlsx')


def compare_values_tsv(i):

    dc = ['show_endtime', 'RecordingEndTime']
    df_tsv = pd.read_csv(f'/home/floosli/Documents/PIN_Data/20{i}0101_20{i}0311_delayedviewing_DE_15_49_mG.csv',
                         parse_dates=dc,
                         usecols=['broadcast_id', 'duration', 'Weights', 'program_duration',
                                  'Title', 'Description', 'date', 'show_endtime', 'RecordingEndTime'])
    df_tsv = add_individual_ratings_ovn(df_tsv)

    result = df_tsv.groupby(['Title', 'broadcast_id'])['individual_Rt-T_tsv'].sum().to_frame().reset_index()

    tsv_solution = pd.read_excel(f'/home/floosli/Downloads/test_ma_20{i}.xlsx', sheet_name='Sendungen(Consolidated)',
                                 header=1,
                                 usecols=['Title', 'Description', 'Channel', 'Rt-T', 'MA-%', 'BrdCstId'])
    tsv_solution = tsv_solution.drop([0])
    tsv_solution = tsv_solution.rename(columns={'Channel': 'station', 'BrdCstId': 'broadcast_id'})
    tsv_solution = tsv_solution.groupby(['Title', 'broadcast_id'])['Rt-T'].sum().to_frame().reset_index()

    live_solution = pd.read_excel(f'/home/floosli/Downloads/test_ma_20{i}.xlsx', sheet_name='Sendungen(Live)',
                                  header=1,
                                  usecols=['Title', 'Rt-T', 'MA-%', 'BrdCstId'])
    live_solution = live_solution.drop([0])
    live_solution = live_solution.rename(columns={'Channel': 'station', 'BrdCstId': 'broadcast_id'})
    live_solution = live_solution.groupby(['Title', 'broadcast_id'])['Rt-T'].sum().to_frame().reset_index()

    solution = tsv_solution.merge(live_solution, on='broadcast_id')
    solution['tsv_solution'] = solution['Rt-T_x'] - solution['Rt-T_y']

    diff = solution.merge(result, on='broadcast_id')
    diff['Difference'] = diff['tsv_solution'] - diff['individual_Rt-T_tsv']
    diff.to_csv(f'/home/floosli/Downloads/difference_tsv_{i}.xlsx')


def compare_ma_live(df):

    result = df.groupby(['Title', 'broadcast_id'])['Marketshare_live'].sum().to_frame().reset_index()

    solution = pd.read_excel(f'/home/floosli/Downloads/test_ma_2020.xlsx', sheet_name='Sendungen(Live)', header=1,
                             usecols=['Title', 'Description', 'Channel', 'Rt-T', 'MA-%', 'BrdCstId', 'Date'])
    solution = solution[solution['Date'].isin(['03.03.2020', '01.03.2020', '02.03.2020', '04.03.2020'])]

    solution = solution.rename(columns={'Channel': 'station', 'BrdCstId': 'broadcast_id'})
    solution = solution.groupby(['Title', 'broadcast_id'])['MA-%'].sum().to_frame().reset_index()

    diff = solution.merge(result, on='broadcast_id')
    diff['Difference'] = diff['MA-%'] - diff['Marketshare_live']
    diff.to_csv(f'/home/floosli/Downloads/difference_ma_20.xlsx')


"""def compare_ma_tsv(df, lv):

    lv = lv.groupby(['broadcast_id'])['Marketshare_tsv', 'Rt_live'].first().reset_index()
    result = df.groupby(['Title', 'broadcast_id'])['Marketshare_tsv', 'individual_Rt-T_tsv'].sum().reset_index()
    result = result.merge(lv, on='broadcast_id')
    result['Marketshare_tsv'] = result['Marketshare_tsv_x'] + result['Marketshare_tsv_y']

    solution = pd.read_excel(f'/home/floosli/Downloads/test_ma_2020.xlsx', sheet_name='Sendungen(Consolidated)',
                             header=1,
                             usecols=['Title', 'Description', 'Channel', 'Rt-T', 'MA-%', 'BrdCstId', 'Date'])
    solution = solution[solution['Date'].isin(['03.03.2020', '01.03.2020', '02.03.2020', '04.03.2020'])]

    solution = solution.rename(columns={'Channel': 'station', 'BrdCstId': 'broadcast_id'})
    solution = solution.groupby(['Title', 'broadcast_id'])['MA-%'].sum().to_frame().reset_index()

    diff = solution.merge(result, on='broadcast_id')
    diff['Difference'] = diff['MA-%'] - diff['Marketshare_tsv']
    diff.to_csv(f'/home/floosli/Downloads/difference_ma_tsv_20.xlsx')
"""


