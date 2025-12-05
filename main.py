import datetime
import os

import requests
from google.cloud import bigquery

# Change if needed
PROJECT_ID = os.environ.get("GCP_PROJECT", "bitcoin-480204")
DATASET_ID = "crypto"
TABLE_ID = "btc_ohlc_1m"
SYMBOL = "BTCUSDT"
INTERVAL = "1m"       # 1-minute candles
LIMIT = 120           # how many recent candles to fetch


def fetch_binance_klines(symbol: str = SYMBOL,
                         interval: str = INTERVAL,
                         limit: int = LIMIT):
    """
    Fetch 1m OHLC candles from Binance.
    Returns a list of dicts ready for BigQuery insert.
    """
    url = "https://api.binance.com/api/v3/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}

    resp = requests.get(url, params=params, timeout=10)
    resp.raise_for_status()
    raw = resp.json()

    rows = []
    for k in raw:
        open_time_ms = k[0]         # open time in milliseconds
        open_time = datetime.datetime.utcfromtimestamp(
            open_time_ms / 1000.0
        ).replace(tzinfo=datetime.timezone.utc)

        rows.append(
            {
                "ts": open_time.isoformat(),  # BigQuery accepts RFC3339 string
                "symbol": symbol,
                "open": float(k[1]),
                "high": float(k[2]),
                "low": float(k[3]),
                "close": float(k[4]),
                "volume": float(k[5]),
            }
        )

    return rows


def insert_rows_bigquery(rows):
    """
    Insert rows into BigQuery table bitcoin-480204.crypto.btc_ohlc_1m.
    """
    client = bigquery.Client(project=PROJECT_ID)
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    if not rows:
        print("No rows to insert.")
        return "No rows", 200

    errors = client.insert_rows_json(table_ref, rows)
    if errors:
        # Log and return error
        print("BigQuery insert errors:", errors)
        return f"BigQuery insert errors: {errors}", 500

    print(f"Inserted {len(rows)} rows into {table_ref}")
    return f"Inserted {len(rows)} rows into {table_ref}", 200


# 🌐 Cloud Function HTTP entry point
def ingest_binance_to_bq(request):
    """
    HTTP Cloud Function entrypoint.
    Fetches recent Binance candles and inserts into BigQuery.
    """
    try:
        rows = fetch_binance_klines()
        msg, status = insert_rows_bigquery(rows)
        return msg, status
    except Exception as e:
        print("Error in ingest_binance_to_bq:", e)
        return str(e), 500
