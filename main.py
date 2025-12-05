import datetime
import requests
from google.cloud import bigquery

PROJECT_ID = "bitcoin-480204"
DATASET = "crypto"
TABLE = "btc_ohlc_1m"

SYMBOL = "BTC-USD"  # Coinbase product id
GRANULARITY = 60    # 60 seconds = 1 minute
LIMIT = 120         # how many recent candles


def fetch_coinbase():
    """
    Fetch 1-minute OHLC candles from Coinbase Exchange for BTC-USD.
    """
    url = f"https://api.exchange.coinbase.com/products/{SYMBOL}/candles"
    params = {
        "granularity": GRANULARITY
        # Coinbase returns up to 300 candles; no explicit 'limit' param,
        # but granularity gives 1m candles going back in time.
    }

    headers = {
        "Accept": "application/json",
        "User-Agent": "coinbase-ohlc-loader/1.0"
    }

    r = requests.get(url, params=params, headers=headers, timeout=10)
    r.raise_for_status()
    raw = r.json()

    # raw is: [ [time, low, high, open, close, volume], ... ]
    # time is Unix timestamp (seconds)
    rows = []
    for c in raw[:LIMIT]:
        ts = datetime.datetime.utcfromtimestamp(c[0]).replace(
            tzinfo=datetime.timezone.utc
        )
        low, high, open_, close, volume = map(float, c[1:6])

        rows.append({
            "ts": ts.isoformat(),
            "symbol": SYMBOL,
            "open": open_,
            "high": high,
            "low":  low,
            "close": close,
            "volume": volume,
        })

    # Coinbase returns newest first; BigQuery doesn’t care, but you might
    # want chronological order:
    rows.sort(key=lambda r: r["ts"])
    return rows


def replace_bigquery_table(rows):
    client = bigquery.Client(project=PROJECT_ID)
    table_id = f"{PROJECT_ID}.{DATASET}.{TABLE}"

    # Delete all existing rows
    delete_query = f"DELETE FROM `{table_id}` WHERE TRUE"
    client.query(delete_query).result()

    # Insert new rows
    errors = client.insert_rows_json(table_id, rows)
    if errors:
        print("Insert errors:", errors)
    else:
        print(f"Inserted {len(rows)} rows into {table_id}")


def main(request=None):
    print("Fetching Coinbase candles…")
    rows = fetch_coinbase()

    print(f"Fetched {len(rows)} rows")
    print("Replacing BigQuery table…")
    replace_bigquery_table(rows)

    return "Done"


if __name__ == "__main__":
    main()
