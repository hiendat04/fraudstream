# Flink Streaming Job Optimization

Operator chaining and parallelism reduced the runtime graph from **30 vertices
to 6** and increased execution parallelism from **1 to 4**. The remaining
bottleneck is now clear: both five-minute feature chains run at **92–95% busy**,
while the Kafka source remains about **98% backpressured**.

## Configuration

| Setting | Baseline | Optimized |
|---|---:|---:|
| Operator chaining | Disabled | Enabled |
| Parallelism | 1 | 4 |
| Runtime vertices | 30 | 6 |
| Runtime subtask instances | 30 | 24 |

Chaining keeps compatible forward operators in one task, removing unnecessary
serialization and task hand-offs. The required hash shuffles for event,
customer, and merchant keys remain.

## Core Results

| Metric | Baseline | Optimized | Impact |
|---|---:|---:|---|
| Runtime vertices | 30 | 6 | **80% fewer** |
| Parallelism | 1 | 4 | **4x parallel execution** |
| Runtime subtask instances | 30 | 24 | **20% fewer tasks despite 4x parallelism** |
| Source backpressure | 997–998 ms/s | 975–979 ms/s | About **2 percentage points lower**, but still severe |
| Kafka pending records | ~580,000 → ~990,000 | ~113,400 → ~108,500 | Backlog grows in baseline but drains in optimized run |
| Feature-chain busy time | 15–81% across captures | 92–95% | Parallel capacity is being used; windows are now the limit |
| Steady data skew | 0% | Usually 0–2% | Skew is not the primary bottleneck |
| Kafka sink backpressure | 0% | 0% | Sinks are not the bottleneck |

The baseline source spent only **2–3 ms/s doing work**, about **997–998 ms/s
blocked**, and **0 ms/s idle**. During the captured interval, Kafka pending
records rose from roughly **0.58 million to almost 1 million**. This is direct
evidence that a single-threaded job with disabled chaining could not consume the
input rate.

The optimized graph distributes each chain over four subtasks and keeps the
subtasks balanced. Its Kafka backlog falls from approximately **113,400 to
108,500 records** during the 38-second capture: about **4,900 fewer pending
records**, or a net drain of roughly **130 records/s**. This is the strongest
end-to-end evidence that the optimized configuration is processing faster than
records arrive during that interval.

However, customer and merchant feature chains still reach **890–975 busy ms/s**
with **0 backpressured ms/s**. They are not waiting for Kafka sinks; they are
consuming nearly all available processing time. Their upstream operators
therefore remain backpressured.

## Flink UI Evidence

### Baseline: parallelism 1, chaining disabled

**1. Job graph: 30 runtime vertices**

![Baseline Flink graph with 30 runtime vertices](../../../images/flink/baseline/01-job-overview-30-vertices.png)

**2. Source pending records around 580,000**

![Baseline Kafka source pending records around 580,000](../../../images/flink/baseline/02-source-pending-records-580k.png)

**3. Source backlog grows toward 680,000**

![Baseline Kafka source backlog growing toward 680,000](../../../images/flink/baseline/03-source-backlog-growth-680k.png)

**4. Source backlog approaches one million**

![Baseline Kafka source backlog approaching one million records](../../../images/flink/baseline/04-source-backlog-near-1m.png)

**5. Source spends 997–998 ms/s backpressured**

![Baseline source busy time versus backpressured time](../../../images/flink/baseline/05-source-busy-vs-backpressured-time.png)

**6. Source has zero idle time**

![Baseline source idle time and output watermark](../../../images/flink/baseline/06-source-idle-time-and-watermark.png)

**7. Validation input and output rates**

![Baseline validation operator record rates](../../../images/flink/baseline/07-validation-record-rate.png)

**8. Validation receives 936–941 ms/s backpressure**

![Baseline validation busy time and backpressure](../../../images/flink/baseline/08-validation-busy-and-backpressure.png)

**9. Deduplication input and output counts**

![Baseline deduplication record counts](../../../images/flink/baseline/09-deduplication-record-counts.png)

**10. Deduplication record rates**

![Baseline deduplication record rates](../../../images/flink/baseline/10-deduplication-record-rate.png)

**11. Deduplication busy time and propagated backpressure**

![Baseline deduplication busy time and backpressure](../../../images/flink/baseline/11-deduplication-busy-and-backpressure.png)

**12. Customer-window record counts**

![Baseline customer window record counts](../../../images/flink/baseline/12-customer-window-record-counts.png)

**13. Customer-window record rates**

![Baseline customer window record rates](../../../images/flink/baseline/13-customer-window-record-rate.png)

**14. Customer-window busy time and backpressure**

![Baseline customer window busy time and backpressure](../../../images/flink/baseline/14-customer-window-busy-and-backpressure.png)

**15. Customer-window idle time and advancing watermark**

![Baseline customer window idle time and watermark](../../../images/flink/baseline/15-customer-window-idle-and-watermark.png)

**16. Merchant-window record counts**

![Baseline merchant window record counts](../../../images/flink/baseline/16-merchant-window-record-counts.png)

**17. Merchant-window record rates**

![Baseline merchant window record rates](../../../images/flink/baseline/17-merchant-window-record-rate.png)

**18. Merchant-window busy time and backpressure**

![Baseline merchant window busy time and backpressure](../../../images/flink/baseline/18-merchant-window-busy-and-backpressure.png)

**19. Merchant-window idle time and watermark**

![Baseline merchant window idle time and watermark](../../../images/flink/baseline/19-merchant-window-idle-and-watermark.png)

**20. Clean-topic committer rate**

![Baseline clean Kafka sink committer record rate](../../../images/flink/baseline/20-clean-sink-committer-record-rate.png)

**21. Clean-topic committer utilization**

![Baseline clean Kafka sink committer utilization](../../../images/flink/baseline/21-clean-sink-committer-utilization.png)

**22. Customer-feature sink metrics**

![Baseline customer feature Kafka sink writer metrics](../../../images/flink/baseline/22-customer-feature-sink-writer-metrics.png)

**23. Merchant-feature sink metrics**

![Baseline merchant feature Kafka sink writer metrics](../../../images/flink/baseline/23-merchant-feature-sink-writer-metrics.png)

**24. Fraud-alert sink metrics**

![Baseline fraud alert Kafka sink writer metrics](../../../images/flink/baseline/24-alert-sink-writer-metrics.png)

### Optimized: parallelism 4, chaining enabled

**1. Job graph: 6 chained runtime vertices**

![Optimized Flink graph with six chained runtime vertices](../../../images/flink/optimized/01-job-overview-6-chained-vertices.png)

**2. Per-subtask source output and busy time**

![Optimized source output rate and busy time](../../../images/flink/optimized/02-source-output-rate-and-busy-time.png)

**3. Source still spends 975–979 ms/s backpressured**

![Optimized source backpressure and idle time](../../../images/flink/optimized/03-source-backpressure-and-idle-time.png)

**4. Kafka pending records decrease from about 113,400 to 108,500**

![Optimized Kafka source pending records decreasing](../../../images/flink/optimized/04-source-pending-records-draining.png)

**5. Deduplication-chain record counts**

![Optimized deduplication chain record counts](../../../images/flink/optimized/05-deduplication-record-counts.png)

**6. Deduplication-chain record rates**

![Optimized deduplication chain record rates](../../../images/flink/optimized/06-deduplication-record-rate.png)

**7. Deduplication-chain busy time and backpressure**

![Optimized deduplication chain busy time and backpressure](../../../images/flink/optimized/07-deduplication-busy-and-backpressure.png)

**8. Customer-feature-chain record counts**

![Optimized customer window record counts](../../../images/flink/optimized/08-customer-window-record-counts.png)

**9. Customer-feature-chain record rates**

![Optimized customer window record rates](../../../images/flink/optimized/09-customer-window-record-rate.png)

**10. Customer chain reaches 890–975 busy ms/s**

![Optimized customer window busy time and backpressure](../../../images/flink/optimized/10-customer-window-busy-and-backpressure.png)

**11. Customer-chain idle time and advancing watermark**

![Optimized customer window idle time and watermark](../../../images/flink/optimized/11-customer-window-idle-and-watermark.png)

**12. Merchant-feature-chain record counts**

![Optimized merchant window record counts](../../../images/flink/optimized/12-merchant-window-record-counts.png)

**13. Merchant-feature-chain record rates**

![Optimized merchant window record rates](../../../images/flink/optimized/13-merchant-window-record-rate.png)

**14. Merchant chain reaches 910–941 busy ms/s**

![Optimized merchant window busy time and backpressure](../../../images/flink/optimized/14-merchant-window-busy-and-backpressure.png)

**15. Late-topic exactly-once committer activity**

![Optimized late Kafka sink committer record rate](../../../images/flink/optimized/15-late-sink-committer-record-rate.png)

**16. Late-topic sink is idle and not backpressured**

![Optimized late Kafka sink idle time and backpressure](../../../images/flink/optimized/16-late-sink-idle-and-backpressure.png)

**17. Four balanced source subtasks**

![Optimized source subtask balance](../../../images/flink/optimized/17-source-subtask-balance.png)

**18. Four balanced deduplication subtasks**

![Optimized deduplication subtask balance](../../../images/flink/optimized/18-deduplication-subtask-balance.png)

**19. Four balanced customer-feature subtasks**

![Optimized customer window subtask balance](../../../images/flink/optimized/19-customer-window-subtask-balance.png)

**20. Four balanced merchant-feature subtasks**

![Optimized merchant window subtask balance](../../../images/flink/optimized/20-merchant-window-subtask-balance.png)

## Conclusion

The current optimization successfully removes unnecessary task boundaries and
uses four parallel subtasks with low steady skew. Its strongest measured impact
is the **80% smaller runtime graph**.

The decreasing Kafka backlog shows an end-to-end improvement during the
captured interval. The job is still not fully optimized: source backpressure
remains near **97–98%**, and the customer and merchant chains are **92–95%
busy**.