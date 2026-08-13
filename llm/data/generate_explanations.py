import random

from data.models import AlertRecord

DEVICE_REUSE_THRESHOLD = 3
BURST_RATIO_THRESHOLD = 2.0
VELOCITY_THRESHOLD = 5

OPENING_TEMPLATES = [
    "Transaction {transaction_id} (${amount:,.2f} via {channel}) is flagged as high-risk.",
    "This ${amount:,.2f} {channel} transaction ({transaction_id}) triggered a fraud alert.",
    "Alert on transaction {transaction_id}: a ${amount:,.2f} charge through {channel}.",
]

BURST_TEMPLATES = [
    "The merchant's transaction volume today is {ratio:.1f}x its typical rate, indicating a possible burst attack.",
    "Merchant activity spiked to {ratio:.1f}x the normal baseline, consistent with coordinated fraud.",
]

DEVICE_REUSE_TEMPLATES = [
    "The device used has been linked to {count} different customers in the last day, a strong reuse signal.",
    "{count} distinct customers shared this device within 24 hours, suggesting device farming.",
]

VELOCITY_TEMPLATES = [
    "The customer has made {count} transactions in the past 7 days, above their usual pattern.",
    "Customer velocity is elevated: {count} transactions in the last 7 days.",
]

CLOSING_TEMPLATES = [
    "Recommend manual review before approval.",
    "This warrants analyst review before further processing.",
    "Flagging for investigation prior to settlement.",
]


def generate_explanation(record: AlertRecord, rng: random.Random) -> str:
    sentences = [
        rng.choice(OPENING_TEMPLATES).format(
            transaction_id=record.transaction_id,
            amount=record.amount,
            channel=record.channel,
        )
    ]

    if (
        record.merchant_burst_ratio_1d_to_prior_30d is not None
        and record.merchant_burst_ratio_1d_to_prior_30d >= BURST_RATIO_THRESHOLD
    ):
        sentences.append(
            rng.choice(BURST_TEMPLATES).format(
                ratio=record.merchant_burst_ratio_1d_to_prior_30d
            )
        )

    if (
        record.device_distinct_customer_count_1d is not None
        and record.device_distinct_customer_count_1d >= DEVICE_REUSE_THRESHOLD
    ):
        sentences.append(
            rng.choice(DEVICE_REUSE_TEMPLATES).format(
                count=record.device_distinct_customer_count_1d
            )
        )

    if (
        record.customer_txn_count_7d is not None
        and record.customer_txn_count_7d >= VELOCITY_THRESHOLD
    ):
        sentences.append(
            rng.choice(VELOCITY_TEMPLATES).format(count=record.customer_txn_count_7d)
        )

    sentences.append(rng.choice(CLOSING_TEMPLATES))

    return " ".join(sentences)
