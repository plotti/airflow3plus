import os
import re
import ftplib

from datetime import datetime, timedelta, timezone

from airflow.models import xcom
from airflow.operators.sensors import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.ftp_hook import FTPHook


# Sensor for missing files and recent modifications
# Checks all regular file for existence on the local directory and recency of the upload
class SensorRegularFiles(BaseSensorOperator):

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
                                       task_id=name + '_push', dag_id='dag_3plus')
                            self.log.info('New file found: %s', name)
                            update = True
                            continue

                        present_mod = datetime.fromtimestamp(os.path.getmtime(local_full_path))
                        if present_mod < server_mod_time:
                            pusher.set(key=name + '_date', value=date, execution_date=datetime.now(timezone.utc),
                                       task_id=name + '_push', dag_id='dag_3plus')
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


# Sensor to observe the files which are not updated very often and do not change nomenclature
class SensorIrregularFiles(BaseSensorOperator):

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
                                   task_id=name + '_push', dag_id='dag_3plus')
                        self.log.info('New File found for %s', name)
                        new_upload = True
                        continue

                    present_mod = datetime.fromtimestamp(os.path.getmtime(local_full_path))
                    if present_mod < server_mod_time:
                        pusher.set(key=name + '_bool', value=True, execution_date=datetime.now(timezone.utc),
                                   task_id=name + '_push', dag_id='dag_3plus')
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
