import datetime

from datetime import datetime
"""
This class is only used to synchronize variables which are used in multiple files and scripts
Change the variables here to ensure global change of the respective parameter
local_path: Path to the local directory
remote_path: Path to the remote directory on the FTP-server
suffix: Current suffix of almost every file on the FTP-server
regular_file_list: Current list of all regular file names
irregular_file_list: Current list of all irregular file names
relevant_channels: Channels which are core of our analysis and belong to our group
channels_of_interest: Channels which stand in direct competition to our products and are interesting for our analysis
sensor_in_past: Amount of days the sensor senses into the past
days_in_year: Amount of days in a year
days_tsv: Amount of days relevant for time-shifted viewing
adjust_year: To change away from the current year set a negative integer on how far back you want to go. range[-2,0]
ftp_conn_id: Current Id of the ftp connection on the airflow webserver
sql_alchemy_conn: Current Id of the sql_alchemy Db on the airflow webserver, database where the xcom variables
are stored
slack_conn_id: Current Id of the slack connection on the airflow webserver
year: Year currently interesting for us
month: Current month which are we interested in 
day: Current day of interest
start: Start date to begin various things, usually start of a year
end: End date for our analysis, usually last day of the year
shows_lists: Lists of the most popular channels on each channel of our main group, movies and 
one time events are excluded
"""
# TODO put them on airflow variables


class AirflowVariables:

    def __init__(self):
        self.local_path = '/home/floosli/Documents/PIN_Data/'
        self.remote_path = '/PIN-Daten/'
        self.table_viewers_path = '/home/floosli/Documents/Tables_Channel_Viewers/'
        self.vorwoche_path = '/home/floosli/Documents/Excel_VorwocheZuschauer/'
        self.steal_pot_path = '/home/floosli/Documents/Heavy_Viewers_StealPot/'
        self.suffix = '.pin'
        self.regular_file_list = ['BrdCst', 'SocDem', 'UsageLive', 'UsageTimeShifted', 'Weight']
        self.irregular_file_list = ['Station', 'CritCode', 'Crit']
        self.relevant_channels = ['3+', '4+', '5+', '6+', 'TV24', 'TV25', 'S1']
        self.channels_of_interest = self.relevant_channels + ["RTL CH", "RTL II CH", "VOX CH", "SUPER RTL CH",
                                                              "NITRO CH", "ProSieben CH", "SRF 1", "SRF zwei",
                                                              "SRF info", "ZDF", "ARD", "Puls 8", "kabel eins CH",
                                                              "SAT.1 CH", "DMAX CH"]
        self.sensor_in_past = 10
        self.days_in_year = 365
        self.days_tsv = 8
        self.adjust_year = 0
        self.ftp_conn_id = 'ftp_server_pin_data'
        self.sql_alchemy_conn = 'sql_alchemy_conn'
        self.slack_conn_id = 'slack'
        self.year = datetime.now().year + self.adjust_year
        self.month = datetime.now().month
        self.day = datetime.now().day
        self.start = str(self.year) + '0101'
        self.end = str(self.year) + '1231'

        self.shows_3plus = [['Der Bachelor', '3+'], ['Die Bachelorette', '3+'],
                            ['Adieu Heimat - Schweizer wandern aus', '3+'], ['Hawaii Five-0', '3+'],
                            ['The Big Bang Theory', '3+'], ['Bumann, der Restauranttester', '3+'],
                            ['Bauer, ledig, sucht ...', '3+'], ['Navy CIS', '3+'], ['FBI', '3+'],
                            ['Young Sheldon', '3+'], ['C.S.I. - Tatort Las Vegas', '3+']]

        self.shows_4plus = [['The Big Bang Theory', '4+'], ['Steel Buddies - Stahlharte Geschäfte', '4+'],
                            ['C.S.I. - Tatort Las Vegas', '4+'], ['Mein peinlicher Sex-Unfall', '4+'],
                            ['King of Queens ', '4+'], ['9-1-1', '4+'], ['The Blacklist', '4+'],
                            ['Wild Card', '4+'], ['Ein Zwilling kommt selten allein', '4+'], ['Young Sheldon', '4+']]

        self.shows_5plus = [['Steel Buddies - Stahlharte Geschäfte', '5+'], ['Navy CIS', '5+'],
                            ['Bones - Die Knochenjägerin', '5+'], ['Hawaii Five-0', '5+'],
                            ['Naked Survival - Ausgezogen in die Wildnis', '5+'], ['C.S.I. - Tatort Las Vegas', '5+'],
                            ['Unforgettable', '5+'], ['King of Queens', '5+'], ['Bumann, der Restauranttester', '5+'],
                            ['Notruf - Retter im Einsatz', '5+']]

        self.shows_6plus = [['Criminal Minds', '6+'], ['Navy CIS: L.A.', '6+'], ['Die Modellbauer - Das Duell', '6+'],
                            ['Camping Paradiso Grandioso', '6+'],
                            ['Jung, wild & sexy - Baggern, saufen, Party machen', '6+'], ['Superstar', '6+'],
                            ['CSI:Cyber', '6+'], ['Die geheimen Akten der NASA', '6+'], ['Flugzeug-Katastrophen', '6+']]

        self.shows_TV24 = [['Die Aquarium-Profis', 'TV24'], ['Border Control - Spaniens Grenzschützer', 'TV24'],
                           ['Timber Kings - Blockhaus-Paläste XXL', 'TV24'], ['Die Höhle der Löwen Schweiz', 'TV24'],
                           ['Airport Security: Colombia', 'TV24'], ['Navy CIS: L.A.', 'TV24'],
                           ['Diesel Brothers', 'TV24'], ['Ninja Warrior Switzerland', 'TV24']]

        self.shows_TV25 = [['King of Queens', 'TV25'], ['Pawn Stars - Die Drei vom Pfandhaus', 'TV25'],
                           ['CSI: Miami', 'TV25'], ['Die Nanny', 'TV25'], ['Die Simpsons', 'TV25'],
                           ['Der Letzte Bulle', 'TV25'], ['Auction Hunters - Zwei Asse machen Kasse', 'TV25'],
                           ['Criminal Minds', 'TV25'], ['The Good Doctor', 'TV25'],
                           ['Mountain Life - Traumhaus gesucht', 'TV25']]

        self.shows_S1 = [['The First 48 - Am Tatort mit den US-Ermittlern', 'S1'],
                         ['Law & Order: Special Victims Unit', 'S1'],
                         ['Fang des Lebens - Der gefährlichste Job Alaskas ', 'S1'],
                         ['Stalked - Leben in Angst', 'S1'], ['Eine schrecklich nette Familie', 'S1'],
                         ['Rick der Restaurator', 'S1'], ['Hard Time', 'S1'], ['Diggers - Die Schatzsucher', 'S1'],
                         ['Überleben!', 'S1'], ['Super-Fabriken', 'S1']]
