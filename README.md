# Transactional Outbox and Inbox

Companion code for the post [Transaction'lar ve Dış Dünya: Transactional Outbox ve Inbox](https://blog.onurcanogul.com/posts/transactionlar-ve-dis-dunya-transactional-outbox-ve-inbox).

An order service has to save an order and tell the rest of the system about it. `@Transactional` protects the database, but the Kafka send sits outside that transaction. This repository shows where that dual write loses or invents events, then closes the gap with an outbox on the producer side and an inbox on the consumer side, and finally kills both at the worst moments to check the invariants hold.

Everything runs on Spring Boot 3, PostgreSQL 17 and Kafka 3.9. Tests use Testcontainers, so Docker has to be running.

## Modules

| Module | What it shows |
|---|---|
| `naive-dual-write` | Tests that reproduce the ghost event (commit fails after the send) and the lost event (broker outage after the commit) |
| `outbox-polling` | Order service: events written to an `outbox` table in the order transaction, a `FOR UPDATE SKIP LOCKED` relay, broker acks before marking rows published, one active relay through an advisory lock, poison event parking, lag metrics, cleanup |
| `inbox-consumer` | Billing service: `INSERT ... ON CONFLICT DO NOTHING` into an `inbox` table in the same transaction as the invoice, aggregate version guard, inbox retention |
| `chaos-tests` | Relay killed between send and update, every record replayed, consumer crashed between inbox insert and commit |
| `event-contracts` | Event payloads and header names shared by both services |
| `test-support` | Shared Testcontainers setup plus small Kafka producer and consumer helpers |

## The flow

```
order-service                                      billing-service
--------------                                     ---------------
BEGIN
  INSERT INTO orders ...
  INSERT INTO outbox ...                           @KafkaListener
COMMIT                                             BEGIN
   |                                                 INSERT INTO inbox (event_id)
   v                                                   ON CONFLICT DO NOTHING  -> 0 rows: skip
relay: SELECT ... FOR UPDATE SKIP LOCKED             version guard on the aggregate
       send to Kafka, key = aggregate_id             INSERT INTO invoices ...
       wait for broker acks                        COMMIT
       UPDATE outbox SET published_at = now()      offset commit
```

Delivery is at-least-once end to end. The outbox removes lost events, the inbox removes duplicate effects, the version guard removes out-of-order effects.

## Running it locally

```bash
docker compose up -d
```

```bash
mvn -q install -DskipTests
```

```bash
mvn -pl outbox-polling spring-boot:run
```

```bash
mvn -pl inbox-consumer spring-boot:run
```

Place an order and cancel it:

```bash
curl -s -X POST localhost:8080/orders -H 'Content-Type: application/json' \
  -d '{"customerId":"6f1b4a4e-8f9a-4c1e-9d5b-7a2c3e4f5a6b","clientReference":"ref-1","total":120.00}'
```

```bash
curl -s -X POST localhost:8080/orders/<orderId>/cancel -H 'Content-Type: application/json' \
  -d '{"reason":"changed my mind"}'
```

The relay picks the rows up within 200 ms; the billing service issues and then cancels the invoice. Outbox metrics are on `localhost:8080/actuator/metrics/outbox.lag.seconds`, `outbox.events.pending` and `outbox.events.failed`.

## Running the tests

```bash
mvn verify
```

All tests run against real PostgreSQL and Kafka containers. The chaos tests boot both services in the same JVM against two databases in one PostgreSQL container.

## Design notes

**Outbox row is written in the caller's transaction.** `OutboxService.add` is `Propagation.MANDATORY`: calling it without a transaction fails fast instead of silently committing the event on its own. The event is a fact with an `eventId`, a schema version and the aggregate version, never a serialized entity.

**Relay rules.** Small batches, `FOR UPDATE SKIP LOCKED` so several instances never pick the same rows, broker acks awaited before `published_at` is set, `acks=all` and `enable.idempotence=true` on the producer. The relay filters on `published_at IS NULL` rather than keeping a cursor: `BIGSERIAL` ids are handed out in allocation order, not commit order, and a cursor would skip a row that commits late.

**Attempts and poison events.** Rows the broker did not acknowledge get `attempts + 1` and are retried on the next run. Once `attempts` reaches `outbox.relay.max-attempts` the row is parked with `failed_at` set, leaves the partial index and stops blocking the batch. `outbox.events.failed` is the metric to alert on.

**Ordering.** Records are keyed by `aggregate_id`, so one order stays in one partition. A single active relay is elected with `pg_try_advisory_lock` on a dedicated connection; a dead leader releases the lock with its session. Even so, a retry after a partial batch failure can hand events of one order over out of sequence, so the consumer keeps the last applied aggregate version per order and drops anything older.

**Inbox.** The inbox row and the invoice commit together. A crash before the commit rolls both back and the record is redelivered; a crash after the commit but before the offset commit redelivers a record the inbox already knows, which is skipped. Inbox retention (14 days) is longer than the topic retention (7 days) so a replayed record is still recognised.

**Housekeeping.** Published outbox rows and old inbox rows are deleted in batches on a schedule. The partial index `outbox_pending` only contains unpublished rows, so it stays small while the table grows.

## Checklist from the post

- Writing to the database and the broker in one flow is a dual write; an `AFTER_COMMIT` listener does not fix it.
- Write the event to the outbox in the same transaction; never `REQUIRES_NEW`.
- Design the event as a fact with an `event_id` and a schema version.
- Relay with `FOR UPDATE SKIP LOCKED`, small batches and short timeouts; wait for the broker ack, then mark.
- `acks=all` and `enable.idempotence=true` on the producer.
- A `published_at` flag, not a cursor.
- Delivery is at-least-once: write the inbox row and do the work in one transaction on the consumer.
- Key by `aggregate_id`; one active relay or partitioning for order, plus a version check on the consumer.
- Have outbox lag, poison event alerts and a cleanup job ready.
- Start with polling; move to CDC when latency or load calls for it.
