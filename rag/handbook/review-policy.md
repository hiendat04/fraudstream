# Fraud review policy

The fraud detection model scores every payment. A payment whose score is at or above the
production model's threshold is held for review. Analysts never change a score:
they decide what happens to a held payment.

## Who decides

The model's score against its threshold decides whether a payment is held. The
analyst decides whether a held payment is released, kept on hold or blocked. The
copilot explains and cites the handbook; it never decides.

## Review queue

Held payments are reviewed oldest first, except that payments of 1,000 USD or more
go to the front of the queue. Every held payment gets a first look within 30
minutes during staffed hours, 07:00 to 23:00 UTC. Outside those hours held
payments wait for the morning shift, and the cardholder is not called at night
unless the amount is 5,000 USD or more.

## Releasing a payment

Release a held payment when the cardholder confirms it, or when the evidence
explains every signal that raised the score: a known device, a trip the customer
told us about, a merchant the customer pays every month. The note says which
signals were explained and how.

## Blocking a payment

Block the payment and the card when the cardholder denies the payment, when the
card appears in a confirmed fraud ring, or when the cardholder cannot be reached
within 24 hours and the payment is 2,000 USD or more. A block above 5,000 USD
needs a second analyst to agree: the four-eyes rule.

## Escalation

Escalate to the fraud operations lead when a merchant appears in three or more
confirmed cases in one week, when one device or IP address is used by five or more
customers, or when an analyst suspects an employee is involved.

## Case notes

Every decision needs a note: the signals seen, the evidence checked, the decision
and the time. Notes never contain a full card number, a password or a one-time
code.
