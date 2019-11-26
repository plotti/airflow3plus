import unittest
import os

from Airflow_Utils import pin_functions
from Daily_Reports import Airflow_Pipeline_3plus

from airflow.models import DagBag


# Test if each implemented Dag is loadable and contains no cycle
class TestDags(unittest.TestCase):

    LOAD_SECOND_THRESHOLD = 2

    def setUp(self):
        self.dagbag = DagBag()

    def test_dagbag_import(self):
        self.assertFalse(len(self.dagbag.import_errors),
                         'There Should be no DAG failures. Got: {}'.format(self.dagbag.import_errors))

    def test_dagbag_import_time(self):
        stats = self.dagbag.dagbag_stats
        slow_files = list(filter(lambda d: d.duration > self.LOAD_SECOND_THRESHOLD, stats))
        res = ', '.join(map(lambda d: d.file[1:], slow_files))

        self.assertEqual(0, len(slow_files),
                         'The following files take more than {threshold}s to load: {res}'
                         .format(threshold=self.LOAD_SECOND_THRESHOLD, res=res))


# Test the 3plus_dag on expected task sequence, nomenclature, cardinality and up,-downstream
class Test3plusDAG(unittest.TestCase):

    def setUp(self):
        self.dagbag = DagBag()

    def test_task_count(self):
        dag_id = 'dag_3plus'
        dag = self.dagbag.get_dag(dag_id)
        self.assertEqual(len(dag.tasks), 7)

    def test_contain_tasks(self):
        dag_id = 'dag_3plus'
        dag = self.dagbag.get_dag(dag_id)
        tasks = dag.tasks
        task_ids = list(map(lambda task: task.task_id, tasks))
        self.assertListEqual(task_ids, ['Sensor_regular_files', 'Download_regular_files', 'Sensor_irregular_files',
                                        'Download_irregular_files', 'Update_facts_table', 'Delete_xcom',
                                        'Delete_content_temp_dir'])

    def test_dependencies_of_sensor_task(self):
        dag_id = 'dag_3plus'
        dag = self.dagbag.get_dag(dag_id)
        task_sensor_regular_files = dag.get_task('Sensor_regular_files')

        upstream_task_ids = list(map(lambda task: task.task_id, task_sensor_regular_files.upstream_list))
        self.assertListEqual(upstream_task_ids, [])
        downstream_task_ids = list(map(lambda task: task.task_id, task_sensor_regular_files.downstream_list))
        self.assertListEqual(downstream_task_ids, ['Download_regular_files'])

    def test_dependencies_of_download_task(self):
        dag_id = 'dag_3plus'
        dag = self.dagbag.get_dag(dag_id)
        task_download_regular_files = dag.get_task('Download_regular_files')

        upstream_task_ids = list(map(lambda task: task.task_id, task_download_regular_files.upstream_list))
        self.assertListEqual(upstream_task_ids, ['Sensor_regular_files'])
        downstream_task_ids = list(map(lambda task: task.task_id, task_download_regular_files.downstream_list))
        self.assertListEqual(downstream_task_ids, ['Update_facts_table'])


# Test the implementation of the reset DAG
class TestDAGreset(unittest.TestCase):

    def setUp(self):
        self.dagbag = DagBag()

    def test_task_count(self):
        dag_id = 'dag_reset'
        dag = self.dagbag.get_dag(dag_id)
        self.assertEqual(len(dag.tasks), 10)

    def test_contain_tasks(self):
        dag_id = 'dag_reset'
        dag = self.dagbag.get_dag(dag_id)
        tasks = dag.tasks
        task_ids = list(map(lambda task: task.task_id, tasks))
        self.assertListEqual(task_ids, ['sleep', 'delete_all', 'download_weight', 'download_brdcst',
                                        'download_usagelive', 'download_usagetimeshifted', 'download_socdem',
                                        'download_irregular', 'create_live_facts_table', 'create_tsv_facts_table'])

    def test_dependencies_of_sensor_task(self):
        dag_id = 'dag_reset'
        dag = self.dagbag.get_dag(dag_id)
        task_create_live_facts_table = dag.get_task('create_live_facts_table')

        upstream_task_ids = list(map(lambda task: task.task_id, task_create_live_facts_table.upstream_list))
        self.assertListEqual(sorted(upstream_task_ids), ['download_brdcst', 'download_irregular', 'download_socdem',
                                                         'download_usagelive', 'download_usagetimeshifted',
                                                         'download_weight'])
        downstream_task_ids = list(map(lambda task: task.task_id, task_create_live_facts_table.downstream_list))
        self.assertListEqual(downstream_task_ids, ['create_tsv_facts_table'])


# Test the python methods used in the 3plus DAG
class TestDAG3plusPythonMethods(unittest.TestCase):

    def test_empty_temp_folder(self):
        Airflow_Pipeline_3plus.delete_content_temp_dir()
        self.assertTrue(len(os.listdir('/home/floosli/Documents/PIN_Data/temp')) == 0)


# Test the pin functions
class TestPINfunctions(unittest.TestCase):

    def test_get_kanton_dict(self):
        kanton_dict = pin_functions.get_kanton_dict('20190101')
        self.assertTrue(kanton_dict['9_1'] == 23)

    def test_get_station_dict(self):
        station_dict = pin_functions.get_station_dict()
        self.assertTrue(station_dict[8216] == '3+' and station_dict[8636] == 'TV24')


if __name__ == '__main__':
    unittest.main()
