# Fraud Feature Engineering

The `fraud_v1` feature contract: what is built from where, how time is handled, and
the rules that keep the model from seeing the future. Table schemas are in
[05_gold_tables.md](05_gold_tables.md).

No single signal proves fraud. These five families each add a different view:

| Family | What it measures |
|---|---|
| Customer velocity | Count, amount, merchant variety and declines over time windows |
| Merchant risk | Volume bursts, past fraud rate, and behaviour against the merchant's category |
| Amount anomaly | The current amount against prior customer, merchant and category history |
| Device and IP reuse | Shared devices and networks across customers and accounts |
| Late arrival | The delay between event time and source creation time |

## Inputs

Features come from the persisted core Gold facts (`data/gold/fact_transactions`,
`fact_customer_daily`, `fact_account_daily`, `fact_merchant_daily`). The fact holds one
clean row per selected Silver transaction, and the daily facts hold reusable history.
Windows use `event_time`, never Bronze partition dates, ingestion time, or
`_silver_processed_at` / `_gold_processed_at`. `is_fraud` is the training label and
feeds historical-label aggregates only.

## Point-in-time rule

For a transaction on business date `D`, history stops at the end of `D - 1`:

```text
feature cutoff   = end of D - 1 day
eligible history = events with event_date <= D - 1 day
```

A transaction at `2026-01-10 09:00` may use its own fields (`amount`, `channel`,
`device_id`), but aggregates must end on January 9. They exclude the current
transaction and any later one. Daily boundaries are UTC: source timestamps are
timezone-naive and are treated as UTC, so the Spark session timezone is set to `UTC`.

A daily snapshot for `D` covers activity through the end of `D` and becomes usable on
`D + 1`. It carries `feature_date` (last date included), `event_timestamp` (what it
represents) and `created` (when it was built; operational only, never a window column).

## Features

Windows are calendar days, so quiet days count: "7 days" is the last 7 calendar days,
not the customer's last 7 active dates.

### Customer velocity: `gold.feat_customer_rolling`, one row per `customer_key` and `feature_date`

| Feature | Definition |
|---|---|
| `customer_txn_count_1d` / `_7d` / `_30d` | Transactions in the last day, 7 days, 30 days |
| `customer_amount_sum_1d` / `_7d` / `_30d` | Amount summed over the same windows |
| `customer_amount_avg_7d` / `_30d` | The sum divided by the count |
| `customer_distinct_merchant_count_7d` / `_30d` | Distinct merchants used |
| `customer_declined_txn_count_7d` | Declined transactions |
| `customer_velocity_ratio_1d_to_prior_30d` | Last day's count over `max(daily average of the preceding non-overlapping 30 days, 1)` |

### Merchant burst and risk: `gold.feat_merchant_risk_rolling`, one row per `merchant_key` and `feature_date`

| Feature | Definition |
|---|---|
| `merchant_txn_count_1d` / `_7d` / `_30d` | Merchant transactions per window |
| `merchant_amount_sum_1d` | Amount on the last day |
| `merchant_distinct_customer_count_1d` | Distinct customers on the last day |
| `merchant_declined_txn_count_1d` | Declined transactions on the last day |
| `merchant_burst_ratio_1d_to_prior_30d` | Same construction as the customer velocity ratio |
| `merchant_prior_fraud_rate_30d` | Eligible fraud labels over eligible transactions |
| `merchant_category_txn_count_1d`, `merchant_category_prior_fraud_rate_30d` | The category's activity and fraud rate |
| `merchant_vs_category_amount_ratio_30d` | Merchant average amount over category average |

These windows run over `fact_merchant_daily`, so a hot merchant adds one row a day. The
`UNKNOWN` merchant is excluded, since unrelated missing IDs must not act like one giant
merchant. Category activity is pre-aggregated and broadcast into the joins, with
Spark's adaptive and skew-join handling on.

Fraud-rate features may only use labels known by the cutoff. The data has no
`label_available_at` column yet, so labels are assumed available on their `feature_date`.
Production needs `label_available_at <= feature_cutoff`.

### Amount anomaly: in `gold.feat_transaction_training`, one row per `transaction_id`

The current `amount` is fine to use, since it is known at scoring time. The baseline
must be historical, and compared within one currency (`USD` today).

| Feature | Definition |
|---|---|
| `customer_amount_mean_30d`, `customer_amount_stddev_30d` | Mean and sample standard deviation over the prior 30-day snapshot |
| `amount_to_customer_avg_30d` | Current amount over the customer mean |
| `amount_zscore_customer_30d` | `(amount - mean) / stddev` |
| `merchant_amount_mean_30d`, `amount_to_merchant_avg_30d` | The merchant baseline and ratio |
| `category_amount_mean_30d`, `amount_to_category_avg_30d` | The category baseline and ratio |
| `amount_anomaly_cold_start` | No reliable customer, merchant or category baseline exists |

Ratios are `NULL` when the baseline mean is zero or missing, and the z-score is `NULL`
with fewer than two observations or a zero deviation. Give the model the cold-start
flag rather than letting missing history look like a score of zero. Never compute
baselines on the full dataset before the time split.

### Device and IP reuse: `gold.feat_device_risk`, one row per `network_identifier`, `identifier_type`, `feature_date`

A device shared by many customers is stronger evidence than an IP, which households,
offices, carriers and VPNs legitimately share. These fields exist from schema `v2`, so
build them only where `risk_signal_version = "v2"` and the identifier is not null.

| Feature | Definition |
|---|---|
| `device_txn_count_1d` / `_7d` | Transactions from the device |
| `device_distinct_customer_count_1d` / `_7d` | Distinct customers on the device |
| `device_distinct_account_count_7d` | Distinct accounts on the device |
| `device_shared_flag_7d` | More than one distinct customer in 7 days |
| `ip_txn_count_1d`, `ip_distinct_customer_count_1d` | The same, for the IP |
| `ip_distinct_account_count_7d` | Distinct accounts on the IP |
| `ip_shared_flag_7d` | The configured customer threshold is exceeded |

The training row keeps `device_feature_available` and `ip_feature_available`. Missing `v1`
values are never turned into reuse counts, and nulls are removed before aggregating, so
missing devices don't collapse into one fake high-volume entity. These identifiers are
high-cardinality and skewed: pre-aggregate by `feature_date`, filter nulls before
shuffles, and inspect the largest groups before choosing partition counts.

### Late arrival

`arrival_delay_minutes = source_created_at - event_time`. Silver already warns above 60.

| Feature | Definition |
|---|---|
| `arrival_delay_minutes` | This transaction's delay |
| `is_late_arrival` | Delay over 60 minutes |
| `is_negative_arrival_delay` | Delay below 0: a clock or source problem, mostly for data-quality monitoring |
| `arrival_delay_missing` | The delay can't be calculated |
| `customer_late_arrival_count_30d`, `customer_late_arrival_rate_30d` | The customer's prior late arrivals, as a count and a rate |
| `merchant_late_arrival_rate_30d` | The merchant's prior late-arrival rate |

## The training table

`gold.feat_transaction_training` has exactly one row per selected Silver `transaction_id`.
Each row combines the current transaction's known fields, the customer, merchant,
category and device/IP features from the latest eligible snapshot, the late-arrival
features, and `is_fraud` as the label, never as an input.

Joins are left joins, so a new customer or device doesn't remove the transaction, and
cardinality is checked after every join. Customer and account features use
`event_date - 1`. Merchant features use an as-of lookup for the newest snapshot with
`feature_date < event_date`, which keeps sparse merchants useful without touching a
same-day or future row. Category features use the previous day's broadcast snapshot.

## Missing history

Zero means "measured, nothing there". Null means "couldn't be observed". Don't fill
every null with zero.

| Case | Value | Companion signal |
|---|---|---|
| Count or sum, known entity, empty window | `0` | `*_history_available = true` |
| Rate, average, deviation or ratio without enough history | `NULL` | `*_cold_start = true` |
| Device or IP missing on a `v1` row | `NULL` | `device_feature_available` / `ip_feature_available = false` |
| Timing inputs missing | `NULL` | `arrival_delay_missing = true` |

## Leakage rules

1. Order by `event_time`.
2. Exclude the current transaction from every historical aggregate.
3. Exclude every later transaction.
4. Never use the current `is_fraud` as an input.
5. Split train, validation and test by time, not randomly.
6. Fit imputers, scalers, encoders and amount baselines on training data only.
7. Historical fraud rates depend on when labels became available.
8. Keep operational timestamps such as `_gold_processed_at` out of the model.

## Checks

| Check | Expected |
|---|---|
| Grain | `count(*) = count(distinct transaction_id)`, and equals the eligible Silver count |
| Point in time | Every joined `feature_date` is strictly before the transaction's `event_date` |
| Window edges | Rows just inside and outside the 1, 7 and 30-day boundaries give the right values |
| No current row | Changing the current transaction doesn't change its own aggregates |
| No future row | Adding a later transaction doesn't change an earlier one's features |
| Cold start | A first-seen entity stays in training, with documented nulls and flags |
| `v1` device | `v1` rows don't create a shared null device or IP group |
| Label isolation | Removing the current label leaves the features unchanged |
| Skew | The largest merchant, device, IP and `UNKNOWN` groups are measured and don't create an unbounded partition |
| Reproducible | The same Silver input and timestamp give identical values |

The unit fixtures cover a customer straddling a window edge, a clear one-day merchant
burst, a normal amount and an outlier, one device reused by several customers, a `v1`
row without device or IP, on-time, late, missing-delay and negative-delay transactions,
and a future transaction that must not affect an earlier row.
