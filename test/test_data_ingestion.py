import unittest
from airflow.models import DagBag


class TestDataIngestionDag(unittest.TestCase):
    def setUp(self):
        self.dagbag = DagBag(dag_folder='path_to_your_dag_folder')
        self.dag_id = 'data_ingestion_dag'

    def test_dag_loaded(self):
        """Verify DAG is correctly loaded."""
        self.assertIsNotNone(self.dagbag.get_dag(self.dag_id))

    def test_task_count(self):
        """Verify the number of tasks in the DAG."""
        dag = self.dagbag.get_dag(self.dag_id)
        self.assertEqual(len(dag.tasks), 4)

    def test_task_dependencies(self):
        """Verify task dependencies."""
        dag = self.dagbag.get_dag(self.dag_id)
        ingest_ticker_name_task = dag.get_task('ingest_ticker_name')
        check_gcs_file_task = dag.get_task('check_gcs_file')
        ingest_daily_ticker_info_task = dag.get_task('ingest_daily_ticker_info')
        ingest_gainers_losers_info_task = dag.get_task('ingest_gainers_losers_info')
        submit_spark_job_task = dag.get_task('submit_spark_job')

        # Check DAG's task dependencies
        self.assertTrue(ingest_ticker_name_task.has_downstream(check_gcs_file_task))
        self.assertTrue(check_gcs_file_task.has_downstream(ingest_daily_ticker_info_task))
        self.assertTrue(check_gcs_file_task.has_downstream(ingest_gainers_losers_info_task))
        self.assertTrue(ingest_daily_ticker_info_task.has_downstream(submit_spark_job_task))
        self.assertTrue(ingest_gainers_losers_info_task.has_downstream(submit_spark_job_task))

        # Ensure no unexpected dependencies
        self.assertEqual(len(dag.tasks) - 1,  # Subtract 1 for the starting task
                         len(ingest_ticker_name_task.get_direct_relatives(upstream=False)))

if __name__ == '__main__':
    unittest.main()