"""
This class is only used to synchronize variables which are used in multiple files and scripts
Change the variables here to ensure global change of the respective parameter
local_path: Path to the local directory
remote_path: Path to the remote directory on the FTP-server
suffix: Current suffix of almost every file on the FTP-server
regular_file_list: Current list of all regular file names
irregular_file_list: Current list of all irregular file names
sensor_in_past: Amount of days the sensor senses into the past
days_in_year: Amount of days in a year
days_tsv: Amount of days relevant for time-shifted viewing
ftp_conn_id: Current Id of the ftp connection on the airflow webserver
sql_alchemy_conn: Current Id of the sql_alchemy Db on the airflow webserver, database where the xcom variables
are stored
slack_conn_id: Current Id of the slack connection on the airflow webserver
"""


class AirflowVariables:

    def __init__(self):
        self.local_path = '/home/floosli/Documents/PIN_Data/'
        self.remote_path = '/PIN-Daten/'
        self.suffix = '.pin'
        self.regular_file_list = ['BrdCst', 'SocDem', 'UsageLive', 'UsageTimeShifted', 'Weight']
        self.irregular_file_list = ['Station', 'CritCode', 'Crit']
        self.sensor_in_past = 10
        self.days_in_year = 365
        self.days_tsv = 8
        self.ftp_conn_id = 'ftp_server_pin_data'
        self.sql_alchemy_conn = 'sql_alchemy_conn'
        self.slack_conn_id = 'slack'
