# Fraud Feature Engineering

This document defines the `fraud_v1` feature contract: source columns, output
grains, event-time windows, point-in-time joins, null behavior, leakage rules,
and validation requirements.

## Detection Signals

| Feature family | Measured behavior |
|---|---|
| Customer velocity | Transaction count, amount, merchant diversity, and declines across event-time windows. |
| Merchant risk | Volume bursts, historical fraud rate, and deviation from merchant-category behavior. |
| Amount anomaly | Current amount relative to prior customer, merchant, and category distributions. |
| Device and IP reuse | Shared infrastructure across customers and accounts. |
| Late arrival | Delay between business event time and source creation time. |

No single signal establishes fraud. These features provide complementary
behavioral, entity-risk, infrastructure-reuse, and data-timeliness measurements.
Gold table schemas and physical storage details are documented in
[`docs/05_gold_tables.md`](05_gold_tables.md).

## Source Contract

Offline features are built from the selected Silver transaction table:

```text
data/silver/transactions/
```

Silver supplies one clean, typed, deterministic row per `transaction_id`.
Important inputs are:

| Input | Feature use |
|---|---|
| `transaction_id` | Preserves one training row per selected transaction. |
| `customer_id`, `account_id` | Customer and account history. |
| `merchant_id`, `merchant_category` | Merchant and category history. |
| `amount`, `currency` | Amount behavior and anomaly comparisons. |
| `event_time`, `event_date` | Business time for every feature window. |
| `source_created_at`, `arrival_delay_minutes` | Late-arrival behavior. |
| `device_id`, `ip_address`, `risk_signal_version` | Device and network reuse on schema-v2 rows. |
| `transaction_status` | Decline, approval, and reversal behavior. |
| `quality_issue_codes` | Auditable late-arrival and missing-field evidence. |
| `is_fraud` | Training label and eligible historical-label aggregates only. |

Use `event_time` for business time. Do not build behavior windows from Bronze
partition dates, ingestion time, `_silver_processed_at`, or `_gold_processed_at`.

## Point-In-Time Contract

Let the transaction being scored have business time `t` and business date `D`.
For the current daily offline design:

```text
feature cutoff = end of D - 1 day
eligible history = events with event_date <= D - 1 day
```

Daily boundaries use UTC. Generated source timestamps are currently
timezone-naive, so feature jobs treat them as UTC and should set the Spark
session timezone to `UTC` before calculating dates or windows.

For example:

```text
transaction event time:  2026-01-10 09:00:00
latest feature date:     2026-01-09
```

The January 10 transaction may use its own immediately available fields, such
as `amount`, `channel`, and `device_id`. Historical aggregates must stop on
January 9. They must not include the current transaction or a later January 10
transaction.

### Snapshot Availability

A daily feature snapshot for date `D` represents activity through the end of
that date. It becomes eligible for offline joins on date `D + 1`.

| Column | Meaning |
|---|---|
| `feature_date` | Last business date included in the aggregate. |
| `event_timestamp` | Logical timestamp represented by the feature snapshot. |
| `created` | Processing time when the snapshot was materialized. |

`created` is operational metadata. It must not replace `event_time` in behavior
windows.

## Feature Set

Feature set `fraud_v1` uses reproducible daily batch snapshots.

### Customer Velocity

Customer velocity measures how quickly and how broadly a customer is
transacting. Fraudulent account access often produces a sudden increase in
transaction count, amount, merchant diversity, or declines.

Output grain:

```text
one row per customer_key and feature_date
```

Target table:

```text
gold.feat_customer_rolling
```

| Feature | Definition through the snapshot cutoff | Purpose |
|---|---|---|
| `customer_txn_count_1d` | Number of customer transactions on the last eligible day. | Recent activity volume. |
| `customer_txn_count_7d` | Customer transaction count over the last 7 calendar days. | Short-term velocity. |
| `customer_txn_count_30d` | Customer transaction count over the last 30 calendar days. | Longer behavior baseline. |
| `customer_amount_sum_1d` | Sum of customer amounts on the last eligible day. | Recent money movement. |
| `customer_amount_sum_7d` | Sum of customer amounts over 7 days. | Short-term amount velocity. |
| `customer_amount_sum_30d` | Sum of customer amounts over 30 days. | Longer amount baseline. |
| `customer_amount_avg_7d` | `customer_amount_sum_7d / customer_txn_count_7d`. | Typical recent amount. |
| `customer_amount_avg_30d` | `customer_amount_sum_30d / customer_txn_count_30d`. | Stable comparison baseline. |
| `customer_distinct_merchant_count_7d` | Distinct merchants used by the customer over 7 days. | Rapid merchant hopping. |
| `customer_distinct_merchant_count_30d` | Distinct merchants used over 30 days. | Normal merchant breadth. |
| `customer_declined_txn_count_7d` | Declined customer transactions over 7 days. | Repeated failed attempts. |
| `customer_velocity_ratio_1d_to_prior_30d` | Last eligible day's count divided by `max(the daily average over the preceding non-overlapping 30 days, 1)`. | Recent velocity compared with an earlier customer baseline. |

Calendar windows include quiet days. A 7-day feature means the last 7 calendar
days, not merely the customer's last 7 active dates.

### Merchant Burst And Risk

Merchant burst features detect sudden concentration at one merchant. They help
surface compromised merchants, laundering activity, bot traffic, or a fraud ring
reusing the same destination.

Output grain:

```text
one row per merchant_key and feature_date
```

Target table:

```text
gold.feat_merchant_risk_rolling
```

| Feature | Definition through the snapshot cutoff | Purpose |
|---|---|---|
| `merchant_txn_count_1d` | Merchant transaction count on the last eligible day. | Immediate merchant volume. |
| `merchant_txn_count_7d` | Merchant transaction count over 7 days. | Short-term volume. |
| `merchant_txn_count_30d` | Merchant transaction count over 30 days. | Merchant baseline. |
| `merchant_amount_sum_1d` | Sum of merchant transaction amounts on the last eligible day. | Immediate value concentration. |
| `merchant_distinct_customer_count_1d` | Distinct customers at the merchant on the last eligible day. | Breadth of the burst. |
| `merchant_declined_txn_count_1d` | Declined merchant transactions on the last eligible day. | Failed or blocked activity. |
| `merchant_burst_ratio_1d_to_prior_30d` | Last eligible day's count divided by `max(the daily average over the preceding non-overlapping 30 days, 1)`. | Recent volume compared with an earlier merchant baseline. |
| `merchant_prior_fraud_rate_30d` | Eligible historical fraud labels divided by eligible transactions over 30 days. | Prior observed merchant risk. |
| `merchant_category_txn_count_1d` | Transactions for the merchant's category on the last eligible day. | Category traffic context. |
| `merchant_category_prior_fraud_rate_30d` | Eligible historical fraud rate for the category over 30 days. | Category risk context. |
| `merchant_vs_category_amount_ratio_30d` | Merchant average amount divided by category average amount over 30 days. | Merchant behavior relative to peers. |

The implementation computes rolling windows from `fact_merchant_daily` instead
of raw transactions, so a hot merchant contributes at most one row per day to a
window. It excludes `merchant_dim_id = "UNKNOWN"` from merchant-risk windows
because unrelated missing IDs must not behave like one giant merchant. Category
activity is pre-aggregated once and broadcast into merchant and training joins;
Spark adaptive execution and skew-join handling are also enabled.

Fraud-rate features may only consume labels that were available by the snapshot
cutoff. The source contract currently has no `label_available_at` column, so the
calculation assumes historical labels are available on their `feature_date`.
Production data must track label availability and enforce
`label_available_at <= feature_cutoff`.

### Amount Anomaly

Amount anomaly features compare the current transaction amount with history that
ends before the current transaction. The current `amount` is allowed because it
is known at scoring time; the comparison baseline must be historical.

Output grain:

```text
one row per transaction_id in gold.feat_transaction_training
```

| Feature | Definition | Purpose |
|---|---|---|
| `customer_amount_mean_30d` | Mean customer amount over the prior 30-day snapshot. | Customer's normal amount. |
| `customer_amount_stddev_30d` | Sample standard deviation over the prior 30-day snapshot. | Normal amount variability. |
| `amount_to_customer_avg_30d` | `current_amount / customer_amount_mean_30d`. | Relative size for this customer. |
| `amount_zscore_customer_30d` | `(current_amount - mean_30d) / stddev_30d`. | Standardized customer deviation. |
| `merchant_amount_mean_30d` | Mean merchant amount over the prior 30-day snapshot. | Merchant's normal ticket size. |
| `amount_to_merchant_avg_30d` | `current_amount / merchant_amount_mean_30d`. | Relative size for this merchant. |
| `category_amount_mean_30d` | Mean amount for the merchant category over the prior 30-day snapshot. | Peer-group baseline. |
| `amount_to_category_avg_30d` | `current_amount / category_amount_mean_30d`. | Relative size for this category. |
| `amount_anomaly_cold_start` | True when no reliable customer, merchant, or category baseline exists. | Separates missing history from normal behavior. |

If the baseline mean is zero or missing, ratio features are `NULL`. If fewer
than two historical observations exist or standard deviation is zero, the
z-score is `NULL`. Models should receive the accompanying cold-start flag rather
than interpreting missing history as a normal score of zero.

Do not compute amount baselines using the full dataset before splitting by time.
That would allow future transactions to influence earlier training rows.
Calculate amount comparisons within the same currency. The current generated
contract expects `USD`; future multi-currency data must be converted with a
point-in-time exchange rate or kept in separate currency groups.

### Device And IP Reuse

Device and IP features detect shared infrastructure. A device appearing across
many customers is stronger evidence than an IP address because households,
companies, mobile carriers, and VPNs can legitimately share an IP.

These fields were introduced by schema version `v2`. Build device/IP features
only when `risk_signal_version = "v2"` and the relevant identifier is non-null.

Output grain:

```text
one row per network_identifier, identifier_type, and feature_date
```

Target table:

```text
gold.feat_device_risk
```

| Feature | Definition through the snapshot cutoff | Purpose |
|---|---|---|
| `device_txn_count_1d` | Transactions from the device on the last eligible day. | Immediate device activity. |
| `device_txn_count_7d` | Transactions from the device over 7 days. | Device velocity. |
| `device_distinct_customer_count_1d` | Distinct customers using the device on the last eligible day. | Same-day identity reuse. |
| `device_distinct_customer_count_7d` | Distinct customers using the device over 7 days. | Sustained identity reuse. |
| `device_distinct_account_count_7d` | Distinct accounts using the device over 7 days. | Account sharing. |
| `device_shared_flag_7d` | True when the 7-day distinct-customer count is greater than 1. | Explainable shared-device signal. |
| `ip_txn_count_1d` | Transactions from the IP on the last eligible day. | Immediate network activity. |
| `ip_distinct_customer_count_1d` | Distinct customers on the IP on the last eligible day. | Same-day network sharing. |
| `ip_distinct_account_count_7d` | Distinct accounts on the IP over 7 days. | Broader network reuse. |
| `ip_shared_flag_7d` | True when the configured IP distinct-customer threshold is exceeded. | High network sharing, interpreted cautiously. |

Keep device and IP missing flags in the training row:

```text
device_feature_available
ip_feature_available
```

Do not turn missing `v1` fields into suspicious reuse counts. Null identifiers
must be excluded before aggregation so all missing devices do not collapse into
one artificial high-volume entity.

Device and IP identifiers are high-cardinality and can be skewed. Pre-aggregate
them by `feature_date`, filter nulls before shuffles, and inspect the largest
identifier groups before choosing Spark partition counts.

### Late-Arrival Features

Late-arrival features describe the difference between business event time and
source creation or arrival time:

```text
arrival_delay_minutes = source_created_at - event_time
```

The existing Silver warning threshold is 60 minutes.

| Feature | Definition | Purpose |
|---|---|---|
| `arrival_delay_minutes` | Current transaction delay in minutes. | Direct timeliness signal available at ingestion. |
| `is_late_arrival` | True when `arrival_delay_minutes > 60`. | Explainable late-event flag. |
| `is_negative_arrival_delay` | True when `arrival_delay_minutes < 0`. | Clock/source-quality anomaly, not direct proof of fraud. |
| `arrival_delay_missing` | True when the delay cannot be calculated. | Separates missing timing from an on-time value. |
| `customer_late_arrival_count_30d` | Customer late arrivals over the prior 30-day snapshot. | Repeated customer timing behavior. |
| `customer_late_arrival_rate_30d` | Prior customer late arrivals divided by prior customer transactions. | Normalized customer timing behavior. |
| `merchant_late_arrival_rate_30d` | Prior merchant late arrivals divided by prior merchant transactions. | Source or merchant timing behavior. |

`arrival_delay_minutes` is safe only if both timestamps are available when the
transaction is scored. Negative delays should primarily drive data-quality
monitoring because clock skew or incorrect source semantics can cause them.

## Training Table Contract

The model-ready output remains:

```text
gold.feat_transaction_training
```

Grain:

```text
exactly one row per selected Silver transaction_id
```

Each row combines:

1. Current transaction fields known at decision time.
2. Customer features from the latest eligible snapshot.
3. Merchant and category features from the latest eligible snapshot.
4. Device/IP features from the latest eligible snapshot.
5. Current and historical late-arrival features.
6. `is_fraud` as the label, never as a current-transaction input feature.

Use left joins so a new customer, merchant, device, or IP does not delete the
transaction from training. Preserve one-row-per-transaction cardinality after
every join.

Customer and account features currently use
`previous_feature_date = event_date - 1`. Merchant features use an as-of lookup
that selects the newest snapshot whose `feature_date < event_date`; this keeps a
sparse merchant history useful without selecting a same-day or future row.
Category features use the previous day's small category snapshot through a
broadcast join.

## Missing History And Default Values

Missing history is meaningful and must be distinguishable from normal behavior.

| Feature type | Offline representation | Companion signal |
|---|---|---|
| Count or sum with a known entity and an empty eligible window | `0` | `*_history_available = true` |
| Rate, average, standard deviation, or ratio without enough history | `NULL` | `*_cold_start = true` |
| Missing device/IP because the row is schema `v1` | `NULL` | `device_feature_available = false` or `ip_feature_available = false` |
| Missing timing inputs | `NULL` | `arrival_delay_missing = true` |

Do not silently fill every null with zero. Zero means measured normal or empty
activity; null can mean the feature could not be observed.

## Leakage Rules

The following rules are mandatory:

1. Use `event_time` as the business-time ordering column.
2. Exclude the current transaction from every historical aggregate.
3. Exclude all transactions later than the current transaction.
4. Never use the current transaction's `is_fraud` value as an input.
5. Build train, validation, and test splits by time, not random row order.
6. Fit imputers, scalers, encoders, and amount baselines on training data only.
7. Treat historical fraud-rate features as label-availability dependent.
8. Keep operational timestamps such as `_gold_processed_at` out of the model.

## Validation Expectations

Feature validation must include these checks:

| Check | Expected result |
|---|---|
| Training grain | `count(*) = count(distinct transaction_id)`. |
| Source reconciliation | Training row count equals eligible selected Silver transaction count. |
| Point-in-time safety | Every joined `feature_date` is strictly earlier than the transaction `event_date`. |
| Window boundaries | Rows immediately inside and outside 1-day, 7-day, and 30-day boundaries produce expected values. |
| Current-row exclusion | Changing the current transaction does not change its historical aggregates. |
| Future-row exclusion | Adding a later transaction does not change an earlier transaction's features. |
| Cold start | A first-seen entity remains in training with documented nulls and flags. |
| Device schema version | `v1` rows do not create a shared null device or IP group. |
| Label isolation | Removing the current label from the input projection does not change feature values. |
| Skew | Largest merchant, device, IP, and `UNKNOWN` groups are measured and do not cause an unbounded partition. |
| Reproducibility | The same Silver input and processing timestamp produce identical feature values. |

Unit fixtures should include at least:

- a customer with activity on both sides of a window boundary;
- a merchant with a clear one-day burst relative to prior history;
- a normal amount and an obvious amount outlier;
- one device reused by several customers;
- a schema-v1 row with no device/IP fields;
- an on-time, late, missing-delay, and negative-delay transaction;
- a future transaction that must not affect an earlier training row.
