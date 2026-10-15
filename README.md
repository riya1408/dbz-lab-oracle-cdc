\# Oracle -> Debezium -> Kafka -> MongoDB CDC lab



A small change-data-capture (CDC) lab on my own laptop, using synthetic data only.

Nothing here comes from any employer or client.



\## What it does

Oracle 26ai Free (table BANK.CUSTOMER) -> Debezium Oracle connector (LogMiner) -> Kafka topic bankdb3.BANK.CUSTOMER -> Python consumer -> MongoDB Atlas, with idempotent, version-guarded writes.



\## Stack

\- Oracle Database Free (container-registry.oracle.com/database/free)

\- Kafka 4.3 in KRaft mode, with dual internal/external listeners (quay.io/debezium/kafka:3.6)

\- Kafka Connect with Debezium 3.6.3.Final (quay.io/debezium/connect:3.6)

\- Oracle JDBC driver ojdbc11 (not in this repo, download it from Maven Central)

\- Python consumer using confluent-kafka and pymongo

\- MongoDB Atlas (M0 free tier)



\## Setup notes

1\. Oracle: ARCHIVELOG mode on (already on in this image), database-level supplemental logging, and table-level ALL COLUMN logging on BANK.CUSTOMER.

2\. A dedicated common user (c##dbzuser) with LogMiner and read privileges. Choose your own password.

3\. A private Docker network (dbz-net) shared by the oracle, kafka and connect containers.

4\. Kafka runs with two listeners: an internal one (kafka:9092) for Debezium/Connect, and an external one (localhost:29092) so a script running directly on the host (not in Docker) can read it too.

5\. The ojdbc11 jar mounted into the Connect container at /kafka/libs/.

6\. The connector registered through the Connect REST API (see connector-v2.example.json for the shape; secrets are placeholders).

7\. Python: `.env` file holds the MongoDB connection string (never committed - see .gitignore). Load it with python-dotenv.



\## What I observed

\- The initial snapshot emits one event per existing row with op "r" and before = null.

\- A live UPDATE emits op "u" with the full old row in "before" and the new row in "after".

\- Two updates in one Oracle transaction produced events with the same scn and commit\_scn, so an "only accept a greater SCN" guard silently drops the second update. Reproduced this for real: a two-statement UPDATE (city, then segment) landed in MongoDB with only the city change applied.

\- The fix: use scn as the primary version, and fall back to txSeq (position within the transaction) as a tiebreaker when scn is equal. Verified this correctly accepts both updates in a same-SCN transaction.

\- Oracle NUMBER columns with no precision arrive base64-encoded by default. Setting decimal.handling.mode to string gives readable values.

\- A connector stores its offset under its name, so a new name (and topic prefix) is needed to force a fresh snapshot.

\- kafka-python (the library) repeatedly failed to complete consumer group rebalancing against this Kafka 4.3 broker on Windows, even though raw TCP connectivity worked. Switched to confluent-kafka (a wrapper around librdkafka), which worked immediately - a good reminder that "can't connect" and "connects but can't negotiate the protocol" are different failure modes.

\- On Windows, kafka-python (and possibly other tools) can hang trying to resolve "localhost" via IPv6 (::1) first; using 127.0.0.1 explicitly avoided this for one debugging step.

\- Kafka Connect requires its own bookkeeping topics (offsets, configs, statuses) to have cleanup.policy=compact, not delete. One of mine was auto-created with the wrong policy after a crash-and-retry cycle, which made Connect refuse to start with a clear error naming the exact topic and setting. Fixed by deleting the topic and letting Connect recreate it correctly.

\- docker restart isn't always safe for a service with an embedded web server mid-crash; a clean remove-and-recreate was more reliable than restarting the same container.

\- Debezium/Oracle gives at-least-once delivery, not exactly-once: after a Connect restart, I saw a transaction's events re-emitted with a new, higher SCN even though nothing had actually changed in Oracle. My idempotent writer correctly accepted the (harmless, identical) replay without creating duplicate or incorrect data.



\## MongoDB write logic (the idempotency guard)

Each customer document stores `\_last\_scn` and `\_last\_txseq` alongside its data. An incoming event is applied only if:

\- the document doesn't exist yet, OR

\- the event's scn is greater than the stored one, OR

\- the event's scn is equal AND its txseq is greater than the stored one.



This is done as a single atomic MongoDB update filter (not read-then-write), so there's no race condition between checking and writing. A rejected update raises MongoDB's own duplicate-key error on the upsert, which is treated as an expected "stale event, skip" signal rather than a real failure.



\## Not done yet

\- Handling deletes (tombstones) in the consumer.

\- A second table (or a second, related collection) to demonstrate a Customer-360-style join.

\- Reconciliation checks (row counts) between Oracle and MongoDB.


## Load test (Oct 2026)

Script: `consumer_load.py`. Each round inserts 10,000 rows into Oracle (commit every 1,000) and measures how long each change takes to reach MongoDB.

Latency is split into stages using three timestamps on every event:
Oracle commit time, Kafka message time, and the time the MongoDB write finished.

| Round | Batch size | Throughput | End-to-end p50 / p95 | Oracle->Kafka p50 / p95 | Kafka->MongoDB p50 / p95 | Avg bulk_write |
|---|---|---|---|---|---|---|
| 1 | 500 | 210 ev/s | 28.0s / 51.8s | not split | not split | - |
| 2 | 500 | 237 ev/s | 27.2s / 45.2s | 3.8s / 4.4s | 23.6s / 40.8s | 2.09s (20 batches) |
| 3 | 2000 | 308 ev/s | 22.0s / 35.9s | 3.9s / 4.6s | 18.1s / 31.4s | 6.41s (5 batches) |

All 10,000 events written in every round, 0 rejected by the version guard.

### What I learned
- **Latency grew steadily during each run**, which means a backlog: events arrived faster than the consumer could write them. Kafka held them safely until the consumer caught up.
- **The consumer is the bottleneck, not Debezium.** Debezium added about 4s; the rest was waiting to be written to MongoDB Atlas (free M0 tier, over the internet).
- **Bigger batches helped, but less each time.** Modelling batch time as a fixed round-trip cost plus a per-document cost gives roughly 0.65s per round trip + 2.9 ms per document. At batch size 2000 the per-document cost dominates, so a single writer tops out around 345 ev/s.
- **Next step:** parallel writers, with the batch split by customer ID so the same customer is always written by the same writer (keeps per-customer order).

### Caveats
- Single laptop (Docker on WSL2) and a shared free Atlas cluster, so absolute numbers are specific to this setup.
- Checked clock skew between the Oracle container and the host: under 200 ms. Oracle commit time has 1-second precision, so Oracle->Kafka is approximate (+/- 1s).
- Load-test consumer does not commit offsets; it is a measurement tool, not the production consumer (`consumer_v1.py`).

### Round 4: parallel writers
`consumer_parallel.py`: same as round 3 (batch 2000), but each batch is split into 4 groups by customer ID and written by 4 threads at the same time. The same customer always goes to the same group, so per-customer order is kept.

| Round | Batch size | Writers | Throughput | End-to-end p50 / p95 | Kafka->MongoDB p50 / p95 | Avg batch write |
|---|---|---|---|---|---|---|
| 3 | 2000 | 1 | 308 ev/s | 22.0s / 35.9s | 18.1s / 31.4s | 6.41s |
| 4 | 2000 | 4 | 430 ev/s | 16.8s / 24.7s | 14.8s / 22.3s | 4.54s |

All 10,000 written, 0 rejected. 4 writers gave about 1.4x, not 4x: the writers share the same free-tier Atlas cluster and the same internet connection, so the bottleneck moved from the consumer code to the database. Next lever would be a larger cluster or running the consumer in the same cloud region as the database, not more threads.
Note: Oracle->Kafka varied between runs (about 2s to 4s) with no Debezium changes, so it is not compared across rounds.

## Consumer v2: deletes, tombstones and safe commits
`consumer_v2.py` is the main consumer, now complete:
- **Deletes** are soft deletes: the document is kept with `_deleted: true` and its SCN, so a replayed older insert or update is rejected instead of bringing the customer back.
- **Tombstones** (the empty message Debezium sends after a delete) are skipped instead of crashing the consumer.
- **Offsets are committed only after the MongoDB write succeeds.** A crash between write and commit means replay, not data loss.

### Tests
1. Insert, update, then delete customer 50001: output was ACCEPTED (c), ACCEPTED (u), DELETED, TOMBSTONE skipped.
2. Restart with the same consumer group: nothing reprocessed (continued from the committed offset).
3. Simulated a crash by rewinding the group 4 offsets (`kafka-consumer-groups.sh --reset-offsets --shift-by -4`): the replayed insert, update and delete were all REJECTED as stale, and MongoDB was unchanged.
