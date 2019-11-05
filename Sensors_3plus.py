import os
import re
import ftplib
import logging

from datetime import datetime, timedelta, timezone

from airflow.models import xcom
from airflow.operators.sensors import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.ftp_hook import FTPHook
from Airflow_variables import AirflowVariables
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
                            pusher.set(key=name + '_date', value=date, execution_date=datetime.now(timezone.utc),
                                       task_id='date_push', dag_id='dag_3plus')
                            self.log.info('New file found: %s', name)
                            update = True
                            continue

                        present_mod = datetime.fromtimestamp(os.path.getmtime(local_full_path))
                        if present_mod < server_mod_time:
                            pusher.set(key=name + '_date', value=date, execution_date=datetime.now(timezone.utc),
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

        year = datetime.now().year
        end = str(year) + '1231'
        start = str(year) + '0101'

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
