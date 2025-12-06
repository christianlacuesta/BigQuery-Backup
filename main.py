import datetime
import time
import requests
from google.cloud import bigquery

PROJECT_ID = "bitcoin-480204"
DATASET = "crypto"
TABLE = "btc_ohlc_1m"

SYMBOL = "BTC-USD"  # Coinbase product id
GRANULARITY = 60    # 60 seconds = 1 minute
LIMIT = 120         # how many recent candles


def fetch_coinbase():
    url = f"https://api.exchange.coinbase.com/products/{SYMBOL}/candles"
    params = {"granularity": GRANULARITY}
    headers = {
        "Accept": "application/json",
        "User-Agent": "coinbase-ohlc-loader/1.0"
    }

    r = requests.get(url, params=params, headers=headers, timeout=10)
    print("Coinbase status:", r.status_code)
    print("Coinbase url:", r.url)
    r.raise_for_status()
    raw = r.json()

    # raw: [ [ time, low, high, open, close, volume ], ... ]
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

    rows.sort(key=lambda r: r["ts"])
    return rows


def replace_bigquery_table(rows):
    client = bigquery.Client(project=PROJECT_ID)
    table_id = f"{PROJECT_ID}.{DATASET}.{TABLE}"

    # Use LOAD job with WRITE_TRUNCATE instead of DELETE + streaming insert
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )

    print(f"Loading {len(rows)} rows into {table_id} with WRITE_TRUNCATE...")
    load_job = client.load_table_from_json(
        rows,
        table_id,
        job_config=job_config,
    )
    load_job.result()  # wait for job to finish

    print("Load job state:", load_job.state)
    print("Output rows:", load_job.output_rows)


def run_once():
    """Run a single fetch + BigQuery replace cycle."""
    print("Fetching Coinbase candles…")
    rows = fetch_coinbase()
    print("Fetched", len(rows), "rows")

    print("Replacing BigQuery table via LOAD job…")
    replace_bigquery_table(rows)

    print("Cycle completed successfully.")


def main():
    """Continuous loop: run every ~60 seconds."""
    while True:
        start = datetime.datetime.utcnow()
        print(f"\n=== New cycle at {start.isoformat()}Z ===")

        try:
            run_once()
        except Exception as e:
            import traceback
            print("Error during cycle:", e)
            traceback.print_exc()

        # Sleep ~60 seconds before next run
        print("Sleeping 60 seconds before next cycle…")
        time.sleep(60)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Shutting down loop gracefully…")
