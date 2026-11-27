-- ==============================================================================
-- Esquema analítico para Crypto Market ETL (Base de datos: crypto_dw)
-- Modelo estrella ligero con soporte para snapshots diarios idempotentes.
-- ==============================================================================

CREATE TABLE IF NOT EXISTS dim_cryptocurrencies (
    coin_id     VARCHAR(100) PRIMARY KEY,
    symbol      VARCHAR(20)  NOT NULL,
    name        VARCHAR(150) NOT NULL,
    updated_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS fact_crypto_market_snapshots (
    coin_id               VARCHAR(100) NOT NULL REFERENCES dim_cryptocurrencies (coin_id),
    snapshot_date         DATE         NOT NULL,
    current_price         NUMERIC(20, 8) NOT NULL CHECK (current_price > 0),
    market_cap            NUMERIC(24, 2) CHECK (market_cap >= 0),
    total_volume          NUMERIC(24, 2) CHECK (total_volume >= 0),
    price_change_pct_24h  NUMERIC(10, 4),
    market_cap_rank       INTEGER      CHECK (market_cap_rank >= 1),
    high_24h              NUMERIC(20, 8) CHECK (high_24h >= 0),
    low_24h               NUMERIC(20, 8) CHECK (low_24h >= 0),
    circulating_supply    NUMERIC(28, 6) CHECK (circulating_supply >= 0),
    extracted_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (coin_id, snapshot_date),
    CONSTRAINT chk_high_gte_low CHECK (
        high_24h IS NULL OR low_24h IS NULL OR high_24h >= low_24h
    )
);

CREATE INDEX IF NOT EXISTS idx_fact_snapshots_date
    ON fact_crypto_market_snapshots (snapshot_date DESC);

CREATE INDEX IF NOT EXISTS idx_fact_snapshots_rank
    ON fact_crypto_market_snapshots (snapshot_date DESC, market_cap_rank ASC);

CREATE OR REPLACE VIEW vw_latest_crypto_market AS
SELECT
    f.snapshot_date,
    f.market_cap_rank,
    d.coin_id,
    d.symbol,
    d.name,
    f.current_price,
    f.market_cap,
    f.total_volume,
    f.price_change_pct_24h,
    f.high_24h,
    f.low_24h,
    f.circulating_supply,
    f.extracted_at
FROM fact_crypto_market_snapshots f
INNER JOIN dim_cryptocurrencies d
    ON d.coin_id = f.coin_id
WHERE f.snapshot_date = (
    SELECT MAX(snapshot_date) FROM fact_crypto_market_snapshots
)
ORDER BY f.market_cap_rank ASC NULLS LAST;
