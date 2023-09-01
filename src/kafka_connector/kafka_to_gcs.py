import csv
import json
import logging
from datetime import datetime, date
from typing import Dict, Sequence, Optional, Any
from logging import Logger
import argparse
import pprint

import requests
from google.cloud import storage
from pyspark.sql import SparkSession, DataFrame


from src.kafka_connector import BaseTemplate
from src.kafka_connector.dataframe_writer_wrappers import persist_streaming_dataframe_to_cloud_storage, persist_dataframe_to_cloud_storage
import src.data_pipeline.config as constants


__all__ = ["KafkaToGCS"]


class KafkaToGCS(BaseTemplate):
    """
    Implementation of loading from Kafka to GCS.
    """

    @staticmethod
    def parse_args(args: Optional[Sequence[str]] = None) -> Dict[str, Any]:
        """Parses arguments passed as constants from the 'config.py' file."""

        parser: argparse.ArgumentParser = argparse.ArgumentParser()

        parser.add_argument(
            f"--{constants.KAFKA_GCS_CHECKPOINT_LOCATION}",
            dest=constants.KAFKA_GCS_CHECKPOINT_LOCATION,
            required=True,
            help="Checkpoint location for Kafka to GCS Template"
        )
        parser.add_argument(
            f"--{constants.KAFKA_GCS_OUTPUT_LOCATION}",
            dest=constants.KAFKA_GCS_OUTPUT_LOCATION,
            required=True,
            help="GCS location of the destination folder"
        )
        parser.add_argument(
            f"--{constants.KAFKA_GCS_BOOTSTRAP_SERVER}",
            dest=constants.KAFKA_GCS_BOOTSTRAP_SERVER,
            required=True,
            help="Kafka topic address from where data is coming"
        )
        parser.add_argument(
            f"--{constants.KAFKA_TOPIC}",
            dest=constants.KAFKA_TOPIC,
            required=True,
            help="Kafka Topic Name"
        )
        parser.add_argument(
            f"--{constants.KAFKA_STARTING_OFFSET}",
            dest=constants.KAFKA_STARTING_OFFSET,
            required=True,
            help="Starting offset value (earliest, latest, json_string)"
        )
        parser.add_argument(
            f"--{constants.KAFKA_GCS_OUTPUT_FORMAT}",
            dest=constants.KAFKA_GCS_OUTPUT_FORMAT,
            required=True,
            help="Output format of the data (json , csv, avro, parquet)"
        )
        parser.add_argument(
            f"--{constants.KAFKA_GCS_OUTPUT_MODE}",
            dest=constants.KAFKA_GCS_OUTPUT_MODE,
            required=True,
            help="Output write mode (append, update, complete)",
            choices=[
                constants.OUTPUT_MODE_APPEND,
                constants.OUTPUT_MODE_UPDATE,
                constants.OUTPUT_MODE_COMPLETE
            ]
        )
        parser.add_argument(
            f"--{constants.KAFKA_GCS_TERMINATION_TIMEOUT}",
            dest=constants.KAFKA_GCS_TERMINATION_TIMEOUT,
            required=True,
            help="Timeout for termination of kafka subscription"
        )

        known_args: argparse.Namespace
        known_args, _ = parser.parse_known_args(args)

        return vars(known_args)

    @staticmethod
    def load_tickers_from_gcs(gcs_path: str) -> list:
        """Loads a list of ticker names from GCS."""

        client = storage.Client()
        bucket_name, blob_name = gcs_path.split("/", 1)
        bucket = client.get_bucket(bucket_name)
        blob = bucket.blob(blob_name)

        tickers = []
        try:
            blob_content = blob.download_as_text()
            tickers = blob_content.split("\n")
            tickers = [ticker.strip() for ticker in tickers if ticker.strip()]
        except Exception as e:
            logging.error(f"Error loading tickers from GCS: {e}")

        return tickers

    @staticmethod
    def fetch_news_data(ticker: str) -> list | json:
        """Makes a request to AlphaVantage API to get JSON with news about this ticker."""

        news_url = f"https://www.alphavantage.co/query?function=NEWS_SENTIMENT&tickers={ticker}&apikey={constants.ALPHA_VANTAGE_API_KEY}"
        response = requests.get(news_url)

        if response.status_code == 200:
            return response.json()
        else:
            logging.error(f"Failed to get news data for {ticker}. Status code: {response.status_code}")
            return []

    @staticmethod
    def process_news_data(news_data: list | json) -> list:
        """Combines received news about all tickers into one list with three columns."""

        sentiment_data = []
        try:
            for news in news_data["feed"]:
                news_date = datetime.strptime(news["time_published"], "%Y%m%dT%H%M%S")
                n_date = news_date.date().strftime("%Y-%m-%d")
                for tckr in news["ticker_sentiment"]:
                    ticker_sentiment = tckr["ticker_sentiment_label"]
                    sentiment_data.append((tckr["ticker"], n_date, ticker_sentiment))
        except Exception as e:
            logging.error(f"Error processing news data: {e}")

        return sentiment_data

    def update_news_data_file(self, batch_df: DataFrame, existing_df: DataFrame, tickers: list) -> DataFrame:
        """Filters and processes relevant data from Kafka messages based on a
        list of tickers and appends it to an existing DataFrame,
        ensuring a continuous update of sentiment information."""

        processed_data = []
        for row in batch_df.collect():
            ticker = row.ticker
            if ticker in tickers:
                news_data = self.fetch_news_data(ticker)
                ticker_sentiment_data = self.process_news_data(news_data)
                processed_data.extend(ticker_sentiment_data)

        if processed_data:
            spark = SparkSession.builder.getOrCreate()
            processed_df = spark.createDataFrame(processed_data, ["ticker", "timestamp", "sentiment"])

            # Append processed data to the existing DataFrame
            updated_df = existing_df.union(processed_df)
            return updated_df

    @staticmethod
    def fetch_intraday_data(tickers: list) -> list | None:
        """Makes a request to AlphaVantage API to get CSV with intraday data about
        all tickers and combine them."""

        intraday_data = []
        for ticker in tickers:
            url = f"https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={ticker}&interval=1min&outputsize=full&datatype=csv&apikey=my_key"
            response = requests.get(url)

            if response.status_code == 200:
                csv_reader = csv.reader(response.text.splitlines())
                next(csv_reader)  # Skip the header row
                for row in csv_reader:
                    intraday_data.append([ticker] + row)
            else:
                logging.error(f"Failed to get data for {ticker}. Status code: {response.status_code}")
                return None
        return intraday_data

    def update_intraday_ticker_file(self, existing_df: DataFrame, tickers: list, spark: SparkSession,
                                    args: Dict[str, Any], output_format) -> None:
        """Creates and continuously updates the intraday ticker information file in the GCS."""
        intraday_data = self.fetch_intraday_data(tickers)

        if intraday_data:
            intraday_df = spark.createDataFrame(intraday_data,
                                                ["ticker", "timestamp", "open", "high", "low", "close", "volume"])
            updated_df = existing_df.union(intraday_df)

            # Write updated intraday data to GCS
            gcs_output_uri = f"{constants.KAFKA_GCS_OUTPUT_LOCATION}/intraday/{date.today()}_intraday.csv"
            persist_dataframe_to_cloud_storage(
                updated_df, args, gcs_output_uri,
                output_format, "append", "kafka.gcs.output"
            )

    def run(self, spark: SparkSession, args: Dict[str, Any]) -> None:
        """Extracts ticker names from the GCS, and then fetches and uploads both
        news and intraday data for these tickers into the GCS. Data updates continue for the 
        entire period of time that API data is available."""

        logger: Logger = self.get_logger(spark=spark)

        # Arguments
        bootstrap_server_list: str = args[constants.KAFKA_GCS_BOOTSTRAP_SERVER]
        kafka_topics: str = args[constants.KAFKA_TOPIC]
        offset: str = args[constants.KAFKA_STARTING_OFFSET]
        output_location: str = args[constants.KAFKA_GCS_OUTPUT_LOCATION]
        output_format: str = args[constants.KAFKA_GCS_OUTPUT_FORMAT]
        output_mode: str = args[constants.KAFKA_GCS_OUTPUT_MODE]
        timeout: int = int(args[constants.KAFKA_GCS_TERMINATION_TIMEOUT])
        checkpoint_loc: str = args[constants.KAFKA_GCS_CHECKPOINT_LOCATION]
        tickers_path: str = args[constants.TICKERS_GCS_PATH]
        ignore_keys = {offset}
        filtered_args = {key: val for key, val in args.items() if key not in ignore_keys}
        logger.info(
            "Starting Kafka to GCS Pyspark job with parameters:\n"
            f"{pprint.pformat(filtered_args)}"
        )

        # Load the list of tickers from the GCS
        tickers = self.load_tickers_from_gcs(tickers_path)

        # Define the initial DataFrame
        df = spark.createDataFrame([], ["ticker", "timestamp", "sentiment"])

        # Write the DataFrame to GCS directly
        writer = df.writeStream
        writer = persist_streaming_dataframe_to_cloud_storage(
            writer, args, checkpoint_loc, output_location,
            output_format, output_mode, "kafka.gcs.output")

        # Start the write operation
        query = writer.start(output_location)

        # Continuously process and append data from Kafka
        kafka_stream_df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", bootstrap_server_list) \
            .option("subscribe", kafka_topics) \
            .load()
        kafka_processed_df = kafka_stream_df.selectExpr("CAST(value AS STRING)")

        # Process and append data from Kafka to the DataFrame
        kafka_batch_query = kafka_processed_df.writeStream \
            .outputMode("append") \
            .foreachBatch(lambda batch_df, start_df: self.update_news_data_file(batch_df, df, tickers)) \
            .start()

        # Start the intraday data update operation
        intraday_query = df.writeStream \
            .trigger(processingTime="1 minute") \
            .foreachBatch(lambda batch_df, batch_id: self.update_intraday_ticker_file(batch_df, tickers, spark,
                                                                                      args, output_format)) \
            .start()

        # Wait for the termination of the main query
        query.awaitTermination(timeout)
        query.stop()
        kafka_batch_query.stop()
        intraday_query.stop()