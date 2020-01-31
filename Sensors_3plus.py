import os
import re
import json
import ftplib
import logging
import Pin_Functions
import pandas as pd

from datetime import datetime, timedelta, timezone, date

from airflow.models import xcom
from airflow.operators.sensors import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.ftp_hook import FTPHook
from Airflow_Variables import AirflowVariables
"""
Both Sensor check for newly created files and push a xcom variable of a date or a boolean on the database
depending on the type of the file. Regular files push a date and irregular files push a boolean.
The logic behind the sensors is a comparison between most recent modification time of same files.
"""


class SensorRegularFiles(BaseSensorOperator):
    """
    Sensor for missing files and recent modifications
    Checks all regular file for existence on the local directory and recency of the upload
    """
    # Errors that are transient in nature, and where action can be retried
    transient_errors = [421, 425, 426, 434, 450, 451, 452]

    error_code_pattern = re.compile(r"([\d]+)")

    @apply_defaults
    def __init__(
            self,
            server_path,
            local_path,
            suffix,
            days_past,
            file_list,
            ftp_conn_id='ftp_default',
            fail_on_transient_errors=True,
            *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.server_path = server_path
        self.local_path = local_path
        self.suffix = suffix
        self.days_past = days_past
        self.file_list = file_list
        self.ftp_conn_id = ftp_conn_id
        self.fail_on_transient_errors = fail_on_transient_errors

    # Return connection Hook pointing to the given ftp server
    def _create_hook(self):
        return FTPHook(ftp_conn_id=self.ftp_conn_id)

    # Extract error code from the ftp exception
    def _get_error_code(self, e):
        try:
            matches = self.error_code_pattern.match(str(e))
            code = int(matches.group(0))
            return code
        except ValueError:
            return e

    def poke(self, context):

        pusher = xcom.XCom
        update = False

        with self._create_hook() as hook:

            for name in self.file_list:

                self.log.info('Poking for %s', name)

                for i in range(self.days_past):

                    date = (datetime.today() - timedelta(days=(self.days_past - i))).strftime('%Y%m%d')
                    server_full_path = self.server_path + name + '_' + date + self.suffix
                    local_full_path = self.local_path + name + '/' + name + '_' + date + self.suffix

                    try:
                        server_mod_time = hook.get_mod_time(server_full_path)

                        if not os.path.isfile(local_full_path):
                            pusher.set(key=name + '_date', value=str(date), execution_date=datetime.now(timezone.utc),
                                       task_id='date_push', dag_id='dag_3plus')
                            self.log.info('New file found: %s', name)
                            update = True
                            continue

                        present_mod = datetime.fromtimestamp(os.path.getmtime(local_full_path))
                        if present_mod < server_mod_time:
                            pusher.set(key=name + '_date', value=str(date), execution_date=datetime.now(timezone.utc),
                                       task_id='date_push', dag_id='dag_3plus')
                            self.log.info('Update found for %s', name)
                            update = True
                            continue

                    except ftplib.error_perm as e:
                        self.log.info('Ftp error encountered: %s, No date found for %s' % (str(e), name))
                        error_code = self._get_error_code(e)
                        if ((error_code != 550) and
                                (self.fail_on_transient_errors or
                                 (error_code not in self.transient_errors))):
                            raise e
                        continue

        self.log.info('Regular files found to update: %s', update)
        return update


class SensorIrregularFiles(BaseSensorOperator):
    """
    Sensor to observe the files which are not updated very often and do not change nomenclature
    in our case Station.pin, Crit.pin and CritCode.pin
    """
    # Errors that are transient in nature, and where action can be retried
    transient_errors = [421, 425, 426, 434, 450, 451, 452]

    error_code_pattern = re.compile(r"([\d]+)")

    @apply_defaults
    def __init__(
            self,
            server_path,
            local_path,
            suffix,
            file_list,
            ftp_conn_id='ftp_default',
            fail_on_transient_errors=True,
            *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.server_path = server_path
        self.local_path = local_path
        self.suffix = suffix
        self.file_list = file_list
        self.ftp_conn_id = ftp_conn_id
        self.fail_on_transient_errors = fail_on_transient_errors

    # Return connection Hook pointing to the given ftp server
    def _create_hook(self):
        return FTPHook(ftp_conn_id=self.ftp_conn_id)

    # Extract error code from the ftp exception
    def _get_error_code(self, e):
        try:
            matches = self.error_code_pattern.match(str(e))
            code = int(matches.group(0))
            return code
        except ValueError:
            return e

    def poke(self, context):

        pusher = xcom.XCom

        new_upload = False

        with self._create_hook() as hook:

            for name in self.file_list:

                self.log.info('Poking for %s', name)

                server_full_path = self.server_path + name + self.suffix
                local_full_path = self.local_path + name + self.suffix

                try:
                    server_mod_time = hook.get_mod_time(server_full_path)

                    if not os.path.isfile(local_full_path):
                        pusher.set(key=name + '_bool', value=True, execution_date=datetime.now(timezone.utc),
                                   task_id='date_push', dag_id='dag_3plus')
                        self.log.info('New File found for %s', name)
                        new_upload = True
                        continue

                    present_mod = datetime.fromtimestamp(os.path.getmtime(local_full_path))
                    if present_mod < server_mod_time:
                        pusher.set(key=name + '_bool', value=True, execution_date=datetime.now(timezone.utc),
                                   task_id='date_push', dag_id='dag_3plus')
                        self.log.info('Modification found for %s', name)
                        new_upload = True
                        continue

                except ftplib.error_perm as e:
                    self.log.info('Ftp error encountered: %s, No update found for %s' % (str(e), name))
                    error_code = self._get_error_code(e)
                    if ((error_code != 550) and
                            (self.fail_on_transient_errors or
                             (error_code not in self.transient_errors))):
                        raise e
                    continue

        self.log.info('Irregular files found to update: %s', new_upload)
        return new_upload


class SensorFactsTable(BaseSensorOperator):
    """
    Sensor to check if the facts table has changed in the last 24 hours.
    Consequently the poking interval should be set to max 24 hours
    :returns True if it has changed to signalise further operations and False if nothing has
    happened in the last 24 hours.
    """
    @apply_defaults
    def __init__(
            self,
            local_path,
            fail_on_transient_errors=True,
            *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.local_path = local_path
        self.fail_on_transient_errors = fail_on_transient_errors

    def poke(self, context):

        variables = AirflowVariables()
        days_in_year = variables.days_in_year

        end = variables.end
        start = variables.start

        end = datetime.strptime(end, '%Y%m%d')

        for i in range(days_in_year):
            date_old = (end - timedelta(days=i)).strftime('%Y%m%d')
            if date_old == start:
                break
            if os.path.isfile(self.local_path + '%s_%s_Live_DE_15_49_mG.csv' % (start, date_old)):
                mod_time = os.path.getmtime(self.local_path + '%s_%s_Live_DE_15_49_mG.csv' % (start, date_old))
                last_mod = datetime.fromtimestamp(mod_time)
                if last_mod > datetime.today() - timedelta(days=1):
                    logging.info('The facts table was updated continue with update')
                    return True

        logging.info('No change detected in the facts table, no update required')
        return False


class SensorMostRecentUpdate(BaseSensorOperator):
    """
    Alerts the user if the facts table hasn't been updated in the last 4 days
    Consequently the user should check if there is a problem with mediapulse or the ftp server
    """
    @apply_defaults
    def __init__(
            self,
            local_path,
            fail_on_transient_errors=True,
            *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.local_path = local_path
        self.fail_on_transient_errors = fail_on_transient_errors

    def poke(self, context):

        variables = AirflowVariables()
        days_in_year = variables.days_in_year

        end = variables.end
        start = variables.start

        end = datetime.strptime(end, '%Y%m%d')

        for i in range(days_in_year):
            date_old = (end - timedelta(days=i)).strftime('%Y%m%d')
            if date_old == start:
                break
            if os.path.isfile(self.local_path + '%s_%s_Live_DE_15_49_mG.csv' % (start, date_old)):
                mod_time = os.path.getmtime(self.local_path + '%s_%s_Live_DE_15_49_mG.csv' % (start, date_old))
                last_mod = datetime.fromtimestamp(mod_time)
                if last_mod > datetime.today() - timedelta(days=4):
                    logging.info('The facts table was recently updated')
                    return True

        logging.info('The facts table has not been updated in the last for years, should have a look at the server')
        return False


class SensorVerifyFactsTables(BaseSensorOperator):
    """
    Sensor to check if our facts table is able to reproduce the values from infosys and other
    heuristics to check the validity of our downloaded files.
    """
    @apply_defaults
    def __init__(
            self,
            fail_on_transient_errors=True,
            *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fail_on_transient_errors = fail_on_transient_errors

    def poke(self, context):

        var = AirflowVariables()
        local_path = var.local_path
        dropbox_path = var.dropbox_path

        puller = xcom.XCom
        updates = puller.get_many(key='update', execution_date=datetime.now(timezone.utc), dag_ids='dag_3plus',
                                  include_prior_dates=True)

        dates = set()
        for update in updates:
            try:
                val = json.loads(update.value)
                dates.update({val})
            except TypeError as e:
                logging.info('Unfortunately got %s, will continue as planed' % str(e))
                continue

        if not dates:
            logging.info('No dates have to be updated, exiting')
            return True

        # Parameter to check if a condition fails
        condition = True

        # Check if all channels have a rating above 0 which should be the case
        # and otherwise an indicator that something is wrong
        zero_threshold, channels = Pin_Functions.compute_rating_per_channel(dropbox_path +
                                                                            'updated_live_facts_table.csv')
        if not zero_threshold:
            logging.info('Some Channels have a rating of 0, more precisely: %s' % channels)
        else:
            logging.info('All channels have a rating above 0, nothing suspicious')

        # Check the facts table rating values TODO
        try:
            results = Pin_Functions.infosys_comparison()
        except ValueError as e:
            logging.info(str(e))
            results = 13
        if abs(results) > 0.1:
            logging.info('The facts table has a difference of %.2f' % results)
            logging.info('if the result is exactly 13 the computation could not be loaded')
        else:
            logging.info('The computed rating of our facts table are in the range of correctness')
            logging.info('Difference' % results)

        try:
            with open(f'{local_path}Crit.pin', 'r', encoding='latin-1') as f:
                df_crit = pd.read_csv(f)
        except FileNotFoundError as e:
            logging.info('Crit file is missing got %s' % str(e))

        for date_new in dates:

            try:
                with open(f'{local_path}Weight/Weight_{date_new}.pin', 'r', encoding='latin-1') as f:
                    df_wei = pd.read_csv(f)

                with open(f'{local_path}SocDem/SocDem_{date_new}.pin', 'r', encoding='latin-1') as f:
                    df_socdem = pd.read_csv(f, dtype='int32')

                with open(f'{local_path}BrdCst/BrdCst_{date_new}.pin', 'r', encoding='latin-1') as f:
                    df_brc = pd.read_csv(f, dtype={'Date': 'object', 'StartTime': 'object', 'ChannelCode': 'int'})

            except FileNotFoundError as e:
                logging.info('Got %s even though it should exist' % str(e))
                condition = False

            try:
                # Check Socdem and Weights, # Ids has to be the same:
                if len(df_wei['SampledIdRep'].unique()) != len(df_socdem['SampleId'].unique()):
                    logging.info('The amount of IDs to no match in Weights and SocDem')
                    condition = False

                # Check SocDem values, if for each aspect a description exist
                if (len(df_socdem.columns) - 3) != len(df_crit.index):
                    logging.info('There is not a value for every SocDem value in the Crit file')
                    condition = False

                # Check if amount of unique channels in the BrdCst file is nothing unusual
                if len(df_brc['ChannelCode'].unique()) < 50 or len(df_brc['ChannelCode'].unique()) > 75:
                    logging.info('Unusual amount of channels detected in the BrdCst file')
                    condition = False

            except NameError as e:
                logging.info(f'A Dataframe was not defined, {e}')
                condition = False

        # Check if there are the same amount of files of each kind
        if (len(next(os.walk(local_path + 'SocDem/'))[2]) != len(next(os.walk(local_path + 'Weight/'))[2])) or\
                (len(next(os.walk(local_path + 'BrdCst/'))[2]) != len(next(os.walk(local_path + 'UsageLive/'))[2])):
            logging.info('There is not the same amount of each type of file present')
            condition = False
        else:
            logging.info('The number of files present is correct')

        return condition


class SensorInfosysExtract(BaseSensorOperator):
    """
    Sensor to extract the last infosys extract.
    The extract is expected to be uploaded last friday, and has to comfort the expected path on the ftp server.
    Atm based on the logic the file be extracted on tuesday on the other days the sensor will return false.
    """
    @apply_defaults
    def __init__(
            self,
            ftp_conn_id='ftp_default',
            fail_on_transient_errors=True,
            *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.ftp_conn_id = ftp_conn_id
        self.fail_on_transient_errors = fail_on_transient_errors

    # Return connection Hook pointing to the given ftp server
    def _create_hook(self):
        return FTPHook(ftp_conn_id=self.ftp_conn_id)

    def poke(self, context):

        # check if file from last friday was uploaded
        with self._create_hook() as hook:

            self.log.info('Checking if last fridays a Infosysextract was uploaded')

            local_path = '/home/floosli/Documents/PIN_Data/solutions/infosys_extract.txt'

            today = date.today()
            last_friday_upload = today - timedelta(days=3+5)
            date_string = str(last_friday_upload.day) + '.' +\
                          str(last_friday_upload.month) + '.' + str(last_friday_upload.year)
            server_path = f'marketshare ({date_string}).txt'

            try:
                self.log.info(f'A Infosys extract was uploaded, the values will be checked')
                if hook.get_mod_time(server_path):
                    hook.retrieve_file(remote_full_path=server_path, local_full_path_or_buffer=local_path)
                return True

            except ftplib.error_perm as e:
                self.log.info(f'There was no file uploaded last friday {e}')
                return True
