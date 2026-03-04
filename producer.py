"""
Real-Time Stock Quote Ingestion Producer

This script continuously polls the Finnhub API for real-time stock quotes
and streams the enriched payload to a Kafka topic. It serves as the entry
point for the bronze layer of the streaming data pipeline.
"""

import os
import time
import json
import requests
from kafka import KafkaProducer
from dotenv import load_dotenv

# Load environment variables securely
load_dotenv(verbose=True)

# Use os.environ.get() to prevent KeyError crashes if the variable is missing
API_KEY = os.environ.get('API_KEY')
BASE_URL = 'https://finnhub.io/api/v1/quote'
SYMBOLS = ['AAPL', 'MSFT', 'TSLA', 'GOOGL', 'AMZN']

# Initialize Kafka Producer
# Connect from the external host (Mac) to the internal Docker network via localhost:29092
# The value_serializer ensures Python dictionaries are serialized into JSON bytes before transmission
producer = KafkaProducer(
    bootstrap_servers=['localhost:29092'],
    value_serializer=lambda m: json.dumps(m).encode('utf-8')
)


def fetch_quote(symbol: str) -> dict | None:
    """
    Fetches real-time stock quote data from the Finnhub API for a given symbol.

    This function constructs the API request, handles HTTP communication,
    and enriches the response payload with tracking metadata (symbol and
    ingestion timestamp) to facilitate downstream processing in the data lakehouse.

    Args:
        symbol (str): The stock ticker symbol (e.g., 'AAPL', 'TSLA').

    Returns:
        dict | None: A dictionary containing the enriched quote data if the
                     API call is successful. Returns None if an exception
                     occurs during the request or JSON parsing.
    """
    url = f'{BASE_URL}?symbol={symbol}&token={API_KEY}'

    try:
        # 💡 Engineering Optimization: Add a timeout parameter.
        # In streaming applications, missing a timeout can cause the pipeline
        # to hang indefinitely if the network drops or the API is unresponsive.
        response = requests.get(url, timeout=5)
        response.raise_for_status()

        data = response.json()

        # Data Enrichment: Add metadata to facilitate downstream tracking
        data['symbol'] = symbol
        data['fetched_at'] = int(time.time())

        return data

    except requests.exceptions.RequestException as e:
        # Catch specific network-related exceptions to separate them from logic errors
        print(f"Network error fetching quote for {symbol}: {e}")
        return None
    except Exception as e:
        # Catch unexpected errors (e.g., JSON parsing failures)
        print(f"Unexpected error fetching quote for {symbol}: {e}")
        return None


# ==========================================
# Main Execution Loop
# ==========================================
# 💡 Engineering Optimization: Use the __main__ block to prevent the script
# from executing automatically if imported as a module elsewhere.
if __name__ == "__main__":
    print("Starting stock quote ingestion stream... Press Ctrl+C to stop.")

    try:
        while True:
            for symbol in SYMBOLS:
                data = fetch_quote(symbol)
                if data:
                    print(f"Successfully fetched and queued quote for {symbol}")
                    # .send() is asynchronous; it places data in a local buffer
                    # rather than transmitting it to the broker immediately.
                    producer.send(topic='stock-quotes', value=data)

            # 💡 Engineering Optimization: Move flush() outside the for loop!
            # This creates a "micro-batch" effect. It collects data for all 5 stocks
            # before packing and sending them to Kafka all at once. This significantly
            # improves network I/O efficiency compared to flushing after every single message.
            producer.flush()

            # Rate limiting: Respect Finnhub API rate limits and control streaming throughput
            time.sleep(6)

    except KeyboardInterrupt:
        # 💡 Engineering Optimization: Graceful Shutdown
        # When stopping the script manually (Ctrl+C), ensure any remaining messages
        # in the buffer are sent and the connection is closed securely.
        print("\nIngestion stream stopped by user. Flushing remaining messages...")
        producer.flush()
        producer.close()
        print("Kafka producer closed securely. Goodbye!")