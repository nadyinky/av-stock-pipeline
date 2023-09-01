from io import StringIO
import json
import csv
from datetime import date
from time import sleep

import requests
import logging
from bs4 import BeautifulSoup
from google.cloud import storage
from google.cloud.exceptions import GoogleCloudError
from config import GCS_BUCKET_NAME, ALPHA_VANTAGE_API_KEY


logging.basicConfig(level=logging.INFO)


def retryable(max_retries: int):
    """A decorator that adds retry functionality with error alerts to a function."""
    def decorator(func):
        def wrapper(*args, **kwargs):
            for retry in range(max_retries):
                try:
                    result = func(*args, **kwargs)
                    return result  # Successful, no need to retry

                except (requests.RequestException, GoogleCloudError) as e:
                    logging.info(f"Error during operation: {e}")
                    if retry < max_retries - 1:
                        logging.info(f"Retrying in 5 seconds... (retry {retry + 1}/{max_retries})")
                        sleep(5)
                    else:
                        logging.error(f"Max retries reached. Could not complete operation.")
                        return None  # Operation unsuccessful after retries
        return wrapper
    return decorator


def fetch_data(url: str) -> requests.Response:
    """Fetches data from a URL using requests and returns the response.
    Raises an exception if the request is not successful."""

    response = requests.get(url)
    if response.ok:
        return response
    else:
        raise Exception(f"Failed to fetch data from the URL. Status code: {response.status_code}")


@retryable(max_retries=2)
def ingest_ticker_name() -> None:
    """Extract ticker names and upload to Google Cloud Storage.
    Retrieves ticker names of the top companies by market capitalization from a website,
    and then uploads the extracted ticker names to a GCS bucket"""

    url = "https://companiesmarketcap.com/usa/largest-companies-in-the-usa-by-market-cap/"
    response = fetch_data(url)
    soup = BeautifulSoup(response.content, "html.parser")

    tickers = [ticker.get_text(" ", strip=True).split()[1]
               for ticker in soup.select("div.company-code")][:50]

    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob(f"tickers/{date.today()}_tickers.txt")
    blob.upload_from_string("\n".join(tickers))
    logging.info("List of ticker names successfully saved to GCS")


@retryable(max_retries=2)
def ingest_daily_ticker_info() -> None:
    """Fetches a list of stock names from GCS, retrieves their daily data using
    the Alpha Vantage API, combines it in-memory into a CSV file, and uploads the CSV
    to the 'tickers/' folder in GCS."""

    output_filename = f"tickers/{date.today()}_daily_ticker_info.csv"
    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET_NAME)

    # Get ticker names from GCS
    blob = bucket.blob(f"tickers/{date.today()}_tickers.txt")
    tickers = blob.download_as_text().splitlines() if blob.exists() else []

    # Request data for each ticker and combine
    combined_data = []
    for ticker in tickers:
        url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={ticker}&datatype=csv&apikey={ALPHA_VANTAGE_API_KEY}"
        response = fetch_data(url)
        data_rows = response.text.splitlines()[1:]
        combined_data.extend([[ticker] + row.split(",") for row in data_rows])

    # In-memory CSV creation and upload to GCS
    output_io = StringIO()
    csv_writer = csv.writer(output_io)
    csv_writer.writerow(["ticker", "timestamp", "open", "high", "low", "close", "volume"])
    csv_writer.writerows(combined_data)

    blob = bucket.blob(output_filename)
    blob.upload_from_filename(output_io.getvalue())
    logging.info("Daily ticker data successfully saved to GCS")


@retryable(max_retries=2)
def ingest_gainers_losers_info() -> None:
    """Fetches stock market data for top gainers, top losers, and most actively traded stocks 
    from an API and uploads it as JSON and CSV files to GCS within a 'best_worst/' folder.
    JSON file hold the overall data, separate CSV files data for each category."""

    url = f"https://www.alphavantage.co/query?function=TOP_GAINERS_LOSERS&apikey={ALPHA_VANTAGE_API_KEY}"
    data = fetch_data(url).json()

    # Upload the JSON data to GCS
    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob(f"best_worst/{date.today()}_best_worst.json")
    blob.upload_from_string(json.dumps(data), content_type="application/json")
    logging.info("JSON file containing top gainers/losers ticker data successfully saved to GCS.")

    # Create and upload CSV files to GCS
    categories = ["top_gainers", "top_losers", "most_actively_traded"]
    for category in categories:
        csv_data = []
        for item in data[category]:
            csv_data.append([
                item["ticker"],
                item["price"],
                item["change_amount"],
                item["change_percentage"][:-1],
                item["volume"]
            ])

        csv_file = StringIO()
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(["ticker", "price", "change_amount", "change_percentage", "volume"])
        csv_writer.writerows(csv_data)

        csv_blob = bucket.blob(f"{date.today()}_{category}.csv")
        csv_blob.upload_from_string(csv_file.getvalue(), content_type="text/csv")

    logging.info("CSV files containing top gainers/losers ticker data successfully saved to GCS.")

