import unittest
from fastapi.testclient import TestClient
from src.fastapi.app import app


class TestFastAPIApp(unittest.TestCase):

    def setUp(self):
        self.client = TestClient(app)

    def test_trigger_ingestion_success(self):
        """Test the successful triggering of ingestion."""
        request_data = {
            "dag_id": "submit_spark_job",
            "execution_date": "2023-08-02",
            "args": {"param1": "value1", "param2": "value2"}
        }
        response = self.client.post("/trigger-ingestion/", json=request_data)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"message": "Ingestion triggered successfully!"})

    def test_trigger_ingestion_failure(self):
        """Test triggering ingestion with invalid data (should return 500)."""
        invalid_request_data = {
            "dag_id": "submit_spark_job",
            "execution_date": "2023-08-01",  # Invalid date format
            "args": {"param1": "value1", "param2": "value2"}
        }
        response = self.client.post("/trigger-ingestion/", json=invalid_request_data)
        self.assertEqual(response.status_code, 500)

if __name__ == '__main__':
    unittest.main()