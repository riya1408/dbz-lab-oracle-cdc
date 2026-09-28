# Oracle -> Debezium -> Kafka CDC lab

A small change-data-capture (CDC) lab on my own laptop, using synthetic data only.
Nothing here comes from any employer or client.

## What it does
Oracle 26ai Free (table BANK.CUSTOMER) -> Debezium Oracle connector (LogMiner) -> Kafka topic bankdb2.BANK.CUSTOMER

## Stack
- Oracle Database Free (container-registry.oracle.com/database/free)
- Kafka 4.3 in KRaft mode (quay.io/debezium/kafka:3.6)
- Kafka Connect with Debezium 3.6.3.Final (quay.io/debezium/connect:3.6)
- Oracle JDBC driver ojdbc11 (not in this repo, download it from Maven Central)

## Setup notes
1. Oracle: ARCHIVELOG mode on (already on in this image), database-level supplemental logging, and table-level ALL COLUMN logging on BANK.CUSTOMER.
2. A dedicated common user (c##dbzuser) with LogMiner and read privileges. Choose your own password.
3. A private Docker network (dbz-net) shared by the oracle, kafka and connect containers.
4. The ojdbc11 jar mounted into the Connect container at /kafka/libs/.
5. The connector registered through the Connect REST API (see connector-v2.example.json).

## What I observed
- The initial snapshot emits one event per existing row with op "r" and before = null.
- A live UPDATE emits op "u" with the full old row in "before" and the new row in "after".
- Two updates in one transaction produced events with the same scn and commit_scn, so an "only accept a greater SCN" guard would silently drop the second update. The events differed in rs_id and txSeq.
- Oracle NUMBER columns with no precision arrive base64-encoded by default. Setting decimal.handling.mode to string gives readable values.
- A connector stores its offset under its name, so a new name (and topic prefix) is needed to force a fresh snapshot.

## Not done yet
- A consumer that writes these events into MongoDB with idempotent writes.
