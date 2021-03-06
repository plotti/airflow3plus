import datetime
import getpass

from datetime import datetime
"""
This class is only used to synchronize variables which are used in multiple files and scripts
Change the variables here to ensure global change of the respective parameter.
Also in this file are various list for reports and more.
If you add a parameter, feel free to write a short description to it.

local_path: Path to the local directory
remote_path: Path to the remote directory on the FTP-server
table_viewers_path: Table of the viewers of a channel
vorwoche_path: Save the Excel for the vorwoche zuschauer
steal_pot_path: Path to the heavy viewers excel sheet
heatmap_path: Path to the Heatmap
dropbox_path: Path to my dropbox
flask_path: Path to the files for the flask application
verify_path: Path to the solutions folder, where we can compare infosys values with facts values
suffix: Current suffix of almost every file on the FTP-server

ftp_conn_id: Current Id of the ftp connection on the airflow webserver
sql_alchemy_conn: Current Id of the sql_alchemy Db on the airflow webserver, database where the xcom variables
are stored
slack_conn_id: Current Id of the slack connection on the airflow webserver
infosys_conn: FTconnection id to the automated Infosys extracts

sensor_in_past: Amount of days the sensor senses into the past
days_in_year: Amount of days in a year
days_tsv: Amount of days relevant for time-shifted viewing
adjust_year: To change away from the current year set a negative integer on how far back you want to go. range[-2,0]
year: Year currently interesting for us
month: Current month which are we interested in 
day: Current day of interest
start: Start date to begin various things, usually start of a year
end: End date for our analysis, usually last day of the year

regular_file_list: Current list of all regular file names
irregular_file_list: Current list of all irregular file names

relevant_channels: Channels which are core of our analysis and belong to our group
channels_of_interest: Channels which stand in direct competition to our products and are interesting for our analysis

shows_lists: Lists of the most popular channels on each channel of our main group, movies and 
one time events are excluded

eps: List of Eps that are mainly looked at can be expanded if needed

bad_channle_list: List of channels who do not provide a serious broadcast file, holes and incomplete
"""
user = getpass.getuser()


class AirflowVariables:

    def __init__(self):
        self.local_path = f'/home/{user}/Documents/PIN_Data/'
        self.remote_path = '/PIN-Daten/'
        self.table_viewers_path = f'/home/{user}/Documents/Tables_Channel_Viewers/'
        self.vorwoche_path = f'/home/{user}/Documents/Excel_VorwocheZuschauer/'
        self.steal_pot_path = f'/home/{user}/Documents/Heavy_Viewers_StealPot/'
        self.heatmap_path = f'/home/{user}/Documents/Heatmap/'
        self.verify_path = f'/home/{user}/Documents/PIN_Data/solutions/'
        self.dropbox_path = f'/home/{user}/Dropbox (3 Plus TV Network AG)/3plus_ds_team/' \
                            'Projects/data/Processed_pin_data/'
        self.flask_path = f'/home/{user}/Dropbox (3 Plus TV Network AG)/3plus_ds_team/Projects/data/Flask_Application/'
        self.suffix = '.pin'

        self.sensor_in_past = 10
        self.days_in_year = 365
        self.days_tsv = 8
        self.adjust_year = 0  # Input negative number, difference until the desired year
        self.year = datetime.now().year + self.adjust_year
        self.month = datetime.now().month
        self.day = datetime.now().day
        self.start = str(self.year) + '0101'
        self.end = str(self.year) + '1231'

        self.ftp_conn_id = 'ftp_server_pin_data'
        self.sql_alchemy_conn = 'sql_alchemy_conn'
        self.slack_conn_id = 'slack'
        self.infosys_ftp_conn_id = 'infosys_conn'
        self.ch_media_ftp = 'chmedia_conn'

        self.regular_file_list = ['BrdCst', 'SocDem', 'UsageLive', 'UsageTimeShifted', 'Weight']
        self.irregular_file_list = ['Station', 'CritCode', 'Crit']

        self.relevant_channels = ['3+', 'TV24', '4+', '5+', 'TV25', 'S1', '6+']
        self.channels_of_interest = self.relevant_channels + ["RTL CH", "RTLZWEI CH", "VOX CH", "SUPER RTL CH",
                                                              "NITRO CH", "ProSieben CH", "SRF 1", "SRF zwei",
                                                              "SRF info", "ZDF", "ARD", "Puls 8", "kabel eins CH",
                                                              "SAT.1 CH", "DMAX CH", 'ORF Eins', 'ORF 2']
        # Used particulary in the Flask metric cards
        self.eps = ['Der Bachelor', 'Die Bachelorette', 'Bauer, ledig, sucht ...', 'Bumann, der Restauranttester',
                    'Ninja Warrior Switzerland', 'Die Höhle der Löwen Schweiz']

        self.shows_3plus = [['Der Bachelor', '3+'], ['Die Bachelorette', '3+'],
                            ['Adieu Heimat - Schweizer wandern aus', '3+'], ['Hawaii Five-0', '3+'],
                            ['The Big Bang Theory', '3+'], ['Bumann, der Restauranttester', '3+'],
                            ['Bauer, ledig, sucht ...', '3+'], ['Navy CIS', '3+'], ['FBI', '3+'],
                            ['Young Sheldon', '3+'], ['C.S.I. - Tatort Las Vegas', '3+'],
                            ['Knallerfrauen', '3+'], ['The Voice of Switzerland', '3+']]

        self.shows_4plus = [['The Big Bang Theory', '4+'], ['Steel Buddies - Stahlharte Geschäfte', '4+'],
                            ['C.S.I. - Tatort Las Vegas', '4+'], ['Mein peinlicher Sex-Unfall', '4+'],
                            ['King of Queens', '4+'], ['9-1-1', '4+'], ['The Blacklist', '4+'],
                            ['CSI: NY', '4+'], ['Notruf - Retter im Einsatz', '4+'],
                            ['Young Sheldon', '4+']]

        self.shows_5plus = [['Steel Buddies - Stahlharte Geschäfte', '5+'], ['Navy CIS', '5+'],
                            ['Bones - Die Knochenjägerin', '5+'], ['Hawaii Five-0', '5+'],
                            ['Naked Survival - Ausgezogen in die Wildnis', '5+'], ['C.S.I. - Tatort Las Vegas', '5+'],
                            ['Unforgettable', '5+'], ['King of Queens', '5+'], ['Bumann, der Restauranttester', '5+'],
                            ['Notruf - Retter im Einsatz', '5+']]

        self.shows_6plus = [['Criminal Minds', '6+'], ['Navy CIS: L.A.', '6+'], ['Die Modellbauer - Das Duell', '6+'],
                            ['Camping Paradiso Grandioso', '6+'], ['Border Patrol New Zealand', '6+'],
                            ['Jung, wild & sexy - Baggern, saufen, Party machen', '6+'], ['Superstar', '6+'],
                            ['CSI: Cyber', '6+'], ['Die geheimen Akten der NASA', '6+'],
                            ['Flugzeug-Katastrophen', '6+']]

        self.shows_TV24 = [['Die Aquarium-Profis', 'TV24'], ['Border Control - Spaniens Grenzschützer', 'TV24'],
                           ['Timber Kings - Blockhaus-Paläste XXL', 'TV24'], ['Die Höhle der Löwen Schweiz', 'TV24'],
                           ['Airport Security: Colombia', 'TV24'], ['Navy CIS: L.A.', 'TV24'],
                           ['Diesel Brothers', 'TV24'], ['Ninja Warrior Switzerland', 'TV24'],
                           ['S.W.A.T.', 'TV24'], ['Sing meinen Song - Das Schweizer Tauschkonzert', 'TV24']]

        self.shows_TV25 = [['King of Queens', 'TV25'], ['Pawn Stars - Die Drei vom Pfandhaus', 'TV25'],
                           ['CSI: Miami', 'TV25'], ['Die Nanny', 'TV25'], ['Die Simpsons', 'TV25'],
                           ['Auction Hunters - Zwei Asse machen Kasse', 'TV25'],
                           ['Criminal Minds', 'TV25'], ['The Good Doctor', 'TV25'],
                           ['Mountain Life - Traumhaus gesucht', 'TV25'], ['Suits', 'TV25']]

        self.shows_S1 = [['The First 48 - Am Tatort mit den US-Ermittlern', 'S1'],
                         ['Law & Order: Special Victims Unit', 'S1'],
                         ['Fang des Lebens - Der gefährlichste Job Alaskas', 'S1'],
                         ['Stalked - Leben in Angst', 'S1'], ['Eine schrecklich nette Familie', 'S1'],
                         ['Rick der Restaurator', 'S1'], ['Hard Time', 'S1'], ['Diggers - Die Schatzsucher', 'S1'],
                         ['Überleben!', 'S1'], ['Super-Fabriken', 'S1']]

        self.bad_channels_list = ['WELT CH', 'Sport 1 CH']


# ----------------------------------------------------------------------------------------------------------------------
