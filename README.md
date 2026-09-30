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

