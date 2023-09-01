from datetime import date

GCS_BUCKET_NAME = "your_google_cloud_bucket_name"
ALPHA_VANTAGE_API_KEY = "your_alpha_vantage_api_key"
TICKERS_GCS_PATH = f"{GCS_BUCKET_NAME}/tickers/{date.today()}_tickers.txt"

DPROC_CLUSTER_NAME = "your_cluster_name"
DPROC_REGION = "your_region"
CONSUMER_GROUP_ID = "your_consumer_group_id"


KAFKA_TOPIC = "data_topic"
KAFKA_BOOTSTRAP_SERVER = "localhost:9092"
KAFKA_GCS_OUTPUT_LOCATION = f"{GCS_BUCKET_NAME}/your_kafka_gcs_output_location"  # GCS path
KAFKA_GCS_CHECKPOINT_LOCATION = f"{GCS_BUCKET_NAME}/your_kafka_gcs_checkpoint_location"  # GCS path
KAFKA_GCS_OUTPUT_FORMAT = "csv"
FORMAT_CSV = KAFKA_GCS_OUTPUT_FORMAT
KAFKA_GCS_OUTPUT_MODE = "update"
OUTPUT_MODE_APPEND = "append"
OUTPUT_MODE_UPDATE = "update"
OUTPUT_MODE_COMPLETE = "complete"
KAFKA_GCS_TERMINATION_TIMEOUT = 16 * 3600  # 16 hours
KAFKA_STARTING_OFFSET = 'latest'