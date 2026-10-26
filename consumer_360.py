"""Lumen Bank Customer 360 consumer (step 1b: customers + accounts).

Reads the CUSTOMER and ACCOUNTS CDC topics and builds ONE MongoDB document per customer:

    { _id: 42, name: ..., city: ..., kyc_status: ..., _last_scn: ..., _last_txseq: ...,
      accounts: { "100123": { type, balance, status, opened_on, _scn, _txseq }, ... } }

Design rules
- One version guard per SOURCE ROW: the customer row guards the top-level fields,
  each account row guards its own entry under accounts.<account_id>.
- Events from different topics arrive in any order. Whichever arrives first creates the
  document (a "stub"); the other fills in its own fields. $set only touches its own fields.
- Batched writes (bulk_write, unordered) and Kafka offsets committed only after the batch
  is safely written (at-least-once + idempotent writes).
- account_index collection maps account_id -> customer_id (needed for transactions in step 1c).
"""
import datetime as dt
import json
import os
import time

from confluent_kafka import Consumer
from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError

TOPIC_CUSTOMER = "bankdb3.BANK.CUSTOMER"
TOPIC_ACCOUNTS = "bankdb3.BANK.ACCOUNTS"
BATCH_SIZE = 1000

CUSTOMER_FIELDS = {"NAME": "name", "EMAIL": "email", "CITY": "city", "SEGMENT": "segment",
                   "ANNUAL_INCOME": "annual_income", "KYC_STATUS": "kyc_status",
                   "RISK_TIER": "risk_tier", "JOINED_ON": "joined_on"}
ACCOUNT_FIELDS = {"ACCOUNT_TYPE": "type", "BALANCE": "balance", "STATUS": "status",
                  "OPENED_ON": "opened_on"}


# ---------- small helpers ----------

def version_of(event):
    """(scn, txSeq) - the position of this change in Oracle's history."""
    src = event["source"]
    return int(src["scn"]), int(src.get("txSeq") or 0)


def clean(value, field):
    """Convert Debezium's encodings into proper types."""
    if value is None:
        return None
    if field in ("annual_income", "balance"):
        return float(value)                       # decimals arrive as strings
    if field in ("joined_on", "opened_on"):
        return dt.datetime.fromtimestamp(value / 1000, dt.timezone.utc)  # DATE = epoch millis
    return value


def newer_than(scn_field, txseq_field, scn, txseq):
    """Filter: the stored version is missing, or older than (scn, txseq)."""
    return {"$or": [
        {scn_field: {"$exists": False}},
        {scn_field: {"$lt": scn}},
        {scn_field: scn, txseq_field: {"$lt": txseq}},
    ]}


# ---------- turning one event into MongoDB operations ----------

def customer_ops(event):
    op = event["op"]
    row = event["before"] if op == "d" else event["after"]
    cid = int(row["CUSTOMER_ID"])
    scn, txseq = version_of(event)
    if op == "d":
        fields = {"_deleted": True}
    else:
        fields = {new: clean(row.get(old), new) for old, new in CUSTOMER_FIELDS.items()}
        fields["_deleted"] = False
    fields.update({"_last_scn": scn, "_last_txseq": txseq})
    return [UpdateOne({"_id": cid, **newer_than("_last_scn", "_last_txseq", scn, txseq)}, {"$set": fields}, upsert=True)]


def account_ops(event):
    op = event["op"]
    row = event["before"] if op == "d" else event["after"]
    aid, cid = str(int(row["ACCOUNT_ID"])), int(row["CUSTOMER_ID"])
    scn, txseq = version_of(event)
    path = f"accounts.{aid}."
    if op == "d":
        fields = {path + "_deleted": True}
    else:
        fields = {path + new: clean(row.get(old), new) for old, new in ACCOUNT_FIELDS.items()}
        fields[path + "_deleted"] = False
    fields.update({path + "_scn": scn, path + "_txseq": txseq})
    doc_op = UpdateOne({"_id": cid, **newer_than(path + "_scn", path + "_txseq", scn, txseq)}, {"$set": fields}, upsert=True)
    index_op = UpdateOne({"_id": int(aid)}, {"$set": {"customer_id": cid}}, upsert=True)
    return [doc_op], [index_op]


def dedupe(events):
    """Inside one batch keep only the newest event per source row."""
    newest = {}
    for topic, key, event in events:
        k = (topic, key)
        if k not in newest or version_of(event) > version_of(newest[k]):
            newest[k] = event
    return [(k[0], e) for k, e in newest.items()]


def write(collection, ops):
    """Unordered bulk write. Duplicate-key errors mean 'a newer version is already stored'."""
    if not ops:
        return 0, 0
    try:
        r = collection.bulk_write(ops, ordered=False)
        return r.matched_count + r.upserted_count, 0
    except BulkWriteError as e:
        d = e.details
        if any(w["code"] != 11000 for w in d["writeErrors"]):
            raise
        return d["nMatched"] + d["nUpserted"], len(d["writeErrors"])


def process_batch(batch, c360, account_index, stats):
    events = dedupe(batch)
    stats["collapsed"] += len(batch) - len(events)
    doc_ops, idx_ops = [], []
    for topic, event in events:
        if topic == TOPIC_CUSTOMER:
            doc_ops += customer_ops(event)
        else:
            d, i = account_ops(event)
            doc_ops += d
            idx_ops += i
    applied, stale = write(c360, doc_ops)
    write(account_index, idx_ops)
    stats["applied"] += applied
    stats["stale"] += stale


# ---------- main loop ----------

def main():
    load_dotenv()
    db = MongoClient(os.environ["MONGO_URI"])[os.getenv("MONGO_DB", "lumen_bank")]
    c360, account_index = db["customer_360"], db["account_index"]

    consumer = Consumer({
        "bootstrap.servers": "127.0.0.1:29092",
        "group.id": "lumen-360-v1",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([TOPIC_CUSTOMER, TOPIC_ACCOUNTS])
    stats = {"events": 0, "tombstones": 0, "collapsed": 0, "applied": 0, "stale": 0}
    batch, started = [], time.time()
    print("Customer 360 consumer running. Ctrl+C to stop.")

    def flush():
        nonlocal batch
        if not batch:
            return
        process_batch(batch, c360, account_index, stats)
        consumer.commit(asynchronous=False)          # bookmark AFTER the write
        print(f"batch of {len(batch):>4} | total events {stats['events']:>6} | "
              f"applied {stats['applied']:>6} | stale {stats['stale']:>4} | "
              f"collapsed {stats['collapsed']:>4} | {time.time() - started:5.1f}s")
        batch = []

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                flush()
                continue
            if msg.error():
                print("Kafka error:", msg.error())
                continue
            stats["events"] += 1
            if msg.value() is None:                   # tombstone: for Kafka compaction only
                stats["tombstones"] += 1
                continue
            key = msg.key().decode() if msg.key() else None
            batch.append((msg.topic(), key, json.loads(msg.value())))
            if len(batch) >= BATCH_SIZE:
                flush()
    except KeyboardInterrupt:
        print("Stopping...")
    finally:
        flush()
        consumer.close()
        print(stats)


if __name__ == "__main__":
    main()
