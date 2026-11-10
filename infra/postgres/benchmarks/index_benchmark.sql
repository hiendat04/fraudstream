-- Time the dashboard and API lookups with and without their indexes.
--
-- Each case runs once with its index, then again inside a transaction that drops
-- the index and rolls back, so the index is never really gone. The drop locks the
-- table until the rollback, so run this when nothing else is reading it.
--
--   docker compose exec -T postgres psql -U "$POSTGRES_USER" -d fraudstream -f - < infra/postgres/benchmarks/index_benchmark.sql > data/benchmarks/index_benchmark.txt

\set ON_ERROR_STOP on

-- One real value per lookup, picked from the newest day so every case finds rows.
SELECT max(event_date) AS newest_day, max(event_time) AS newest_time FROM gold.fact_transactions \gset
SELECT customer_key, merchant_key
FROM gold.fact_transactions
WHERE event_date = :'newest_day'
LIMIT 1 \gset
SELECT customer_id FROM gold.dim_customer WHERE is_current LIMIT 1 \gset
SELECT transaction_id AS quality_transaction_id FROM silver.stg_transaction_quality_issues LIMIT 1 \gset

\echo '=== Index sizes'
SELECT indexrelname AS index, pg_size_pretty(pg_relation_size(indexrelid)) AS size
FROM pg_stat_user_indexes
WHERE indexrelname IN (
    'ix_fact_transactions_customer_event_time',
    'ix_fact_transactions_merchant_event_time',
    'ix_fact_transactions_event_date',
    'ux_dim_customer_current',
    'ix_dim_customer_natural_history',
    'ix_stg_quality_issues_transaction_id'
)
ORDER BY indexrelname;

\echo '=== Customer history: with ix_fact_transactions_customer_event_time'
EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM gold.fact_transactions
WHERE customer_key = :customer_key AND event_time >= :'newest_time'::timestamptz - INTERVAL '7 days'
ORDER BY event_time DESC LIMIT 50;

\echo '=== Customer history: without it'
BEGIN;
DROP INDEX gold.ix_fact_transactions_customer_event_time;
EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM gold.fact_transactions
WHERE customer_key = :customer_key AND event_time >= :'newest_time'::timestamptz - INTERVAL '7 days'
ORDER BY event_time DESC LIMIT 50;
ROLLBACK;

\echo '=== Merchant history: with ix_fact_transactions_merchant_event_time'
EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM gold.fact_transactions
WHERE merchant_key = :merchant_key AND event_time >= :'newest_time'::timestamptz - INTERVAL '7 days'
ORDER BY event_time DESC LIMIT 50;

\echo '=== Merchant history: without it'
BEGIN;
DROP INDEX gold.ix_fact_transactions_merchant_event_time;
EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM gold.fact_transactions
WHERE merchant_key = :merchant_key AND event_time >= :'newest_time'::timestamptz - INTERVAL '7 days'
ORDER BY event_time DESC LIMIT 50;
ROLLBACK;

\echo '=== One day: with ix_fact_transactions_event_date'
EXPLAIN (ANALYZE, BUFFERS)
SELECT count(*), sum(amount) FROM gold.fact_transactions WHERE event_date = :'newest_day';

\echo '=== One day: without it'
BEGIN;
DROP INDEX gold.ix_fact_transactions_event_date;
EXPLAIN (ANALYZE, BUFFERS)
SELECT count(*), sum(amount) FROM gold.fact_transactions WHERE event_date = :'newest_day';
ROLLBACK;

\echo '=== Current customer: with ux_dim_customer_current'
EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM gold.dim_customer WHERE customer_id = :'customer_id' AND is_current;

-- The history index also starts with customer_id, so it goes too; otherwise Postgres just switches to it.
\echo '=== Current customer: without ux_dim_customer_current and ix_dim_customer_natural_history'
BEGIN;
DROP INDEX gold.ux_dim_customer_current;
DROP INDEX gold.ix_dim_customer_natural_history;
EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM gold.dim_customer WHERE customer_id = :'customer_id' AND is_current;
ROLLBACK;

\echo '=== A transaction''s quality issues: with ix_stg_quality_issues_transaction_id'
EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM silver.stg_transaction_quality_issues WHERE transaction_id = :'quality_transaction_id';

\echo '=== A transaction''s quality issues: without it'
BEGIN;
DROP INDEX silver.ix_stg_quality_issues_transaction_id;
EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM silver.stg_transaction_quality_issues WHERE transaction_id = :'quality_transaction_id';
ROLLBACK;
