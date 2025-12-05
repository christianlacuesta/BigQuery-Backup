CREATE SCHEMA IF NOT EXISTS `bitcoin-480204.crypto`;

CREATE TABLE IF NOT EXISTS `bitcoin-480204.crypto.btc_ohlc_1m` (
  ts        TIMESTAMP NOT NULL,  -- candle open time (UTC)
  symbol    STRING NOT NULL,     -- e.g. 'BTCUSDT'
  open      FLOAT64,
  high      FLOAT64,
  low       FLOAT64,
  close     FLOAT64,
  volume    FLOAT64
)
PARTITION BY DATE(ts)
CLUSTER BY symbol;
