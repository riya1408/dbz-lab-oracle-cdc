"""Lumen Bank Customer 360 consumer.

Reads three CDC topics (CUSTOMER, ACCOUNTS, TRANSACTIONS) and maintains, in MongoDB:

  customer_360   one document per customer:
                   profile fields                       (guarded by _last_scn/_last_txseq)
                   accounts.<account_id>.{...}          (each guarded by its own _scn/_txseq)
                   net_position, recent_transactions,   (DERIVED - recomputed, never incremented)
                   spend_30d, txn_count_30d
  account_index  account_id -> customer_id  (transactions only know their account)
  transactions   one document per transaction, 90-day TTL (the "hot window")
  pending_txns   transactions whose account owner isn't known yet (the waiting room)

Design rules
- One version guard per SOURCE ROW, so rows from different tables never block each other.
- Topics arrive in any order: whichever event comes first creates the document (a stub).
- Derived fields are RECOMPUTED from stored data for the customers a batch touched,
  never built with $inc. A replayed event therefore can't double-count anything.
- Parked transactions live in MongoDB, not in memory: Kafka offsets are committed after
  each batch, so anything held only in memory would be lost on a crash.
- Batched, unordered bulk writes; Kafka offsets committed only after the batch is written.
"""
import datetime as dt
import json
import os
import time

from confluent_kafka import Consumer
from dotenv import load_dotenv
from pymongo import ASCENDING, DESCENDING, MongoClient, UpdateOne
from pymongo.errors import BulkWriteError

TOPIC_CUSTOMER = "bankdb3.BANK.CUSTOMER"
TOPIC_ACCOUNTS = "bankdb3.BANK.ACCOUNTS"
TOPIC_TXNS = "bankdb3.BANK.TRANSACTIONS"
BATCH_SIZE = 1000
HOT_WINDOW_DAYS = 90

CUSTOMER_FIELDS = {"NAME": "name", "EMAIL": "email", "CITY": "city", "SEGMENT": "segment",
                   "ANNUAL_INCOME": "annual_income", "KYC_STATUS": "kyc_status",
                   "RISK_TIER": "risk_tier", "JOINED_ON": "joined_on"}
ACCOUNT_FIELDS = {"ACCOUNT_TYPE": "type", "BALANCE": "balance", "STATUS": "status",
                  "OPENED_ON": "opened_on"}
TXN_FIELDS = {"TXN_TS": "ts", "AMOUNT": "amount", "DIRECTION": "direction", "CHANNEL": "channel",
              "CATEGORY": "category", "MERCHANT": "merchant", "CITY": "city"}


# ---------- small helpers ----------

def version_of(event):
    """(scn, txSeq) - the position of this change in Oracle's history."""
    src = event["source"]
    return int(src["scn"]), int(src.get("txSeq") or 0)


def clean(value, field):
    """Convert Debezium's encodings into proper types."""
    if value is None:
        return None
    if field in ("annual_income", "balance", "amount"):
        return float(value)                                             # decimals arrive as strings
    if field in ("joined_on", "opened_on"):
        return dt.datetime.fromtimestamp(value / 1000, dt.timezone.utc)      # DATE: epoch millis
    if field == "ts":
        return dt.datetime.fromtimestamp(value / 1_000_000, dt.timezone.utc)  # TIMESTAMP: epoch micros
    return value


def newer_than(scn_field, txseq_field, scn, txseq):
    """Filter: the stored version is missing, or older than (scn, txseq)."""
    return {"$or": [
        {scn_field: {"$exists": False}},
        {scn_field: {"$lt": scn}},
        {scn_field: scn, txseq_field: {"$lt": txseq}},
    ]}


def row_of(event):
    return event["before"] if event["op"] == "d" else event["after"]


# ---------- events -> MongoDB operations ----------

def customer_op(event):
    row = row_of(event)
    cid = int(row["CUSTOMER_ID"])
    scn, txseq = version_of(event)
    if event["op"] == "d":
        fields = {"_deleted": True}
    else:
        fields = {new: clean(row.get(old), new) for old, new in CUSTOMER_FIELDS.items()}
        fields["_deleted"] = False
    fields.update({"_last_scn": scn, "_last_txseq": txseq})
    return cid, UpdateOne({"_id": cid, **newer_than("_last_scn", "_last_txseq", scn, txseq)},
                          {"$set": fields}, upsert=True)


def account_ops(event):
    row = row_of(event)
    aid, cid = int(row["ACCOUNT_ID"]), int(row["CUSTOMER_ID"])
    scn, txseq = version_of(event)
    path = f"accounts.{aid}."
    if event["op"] == "d":
        fields = {path + "_deleted": True}
    else:
        fields = {path + new: clean(row.get(old), new) for old, new in ACCOUNT_FIELDS.items()}
        fields[path + "_deleted"] = False
    fields.update({path + "_scn": scn, path + "_txseq": txseq})
    doc_op = UpdateOne({"_id": cid, **newer_than(path + "_scn", path + "_txseq", scn, txseq)},
                       {"$set": fields}, upsert=True)
    index_op = UpdateOne({"_id": aid}, {"$set": {"customer_id": cid}}, upsert=True)
    return aid, cid, doc_op, index_op


def txn_op(event, cid):
    row = row_of(event)
    tid = int(row["TXN_ID"])
    scn, txseq = version_of(event)
    if event["op"] == "d":
        fields = {"_deleted": True}
    else:
        fields = {new: clean(row.get(old), new) for old, new in TXN_FIELDS.items()}
        fields["_deleted"] = False
    fields.update({"customer_id": cid, "account_id": int(row["ACCOUNT_ID"]),
                   "_scn": scn, "_txseq": txseq})
    return UpdateOne({"_id": tid, **newer_than("_scn", "_txseq", scn, txseq)},
                     {"$set": fields}, upsert=True)


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


# ---------- derived fields (recomputed, so replays can't double-count) ----------

def refresh_net_position(db, cids):
    """net_position = sum of balances of non-deleted accounts (loans and card dues are negative)."""
    if not cids:
        return
    db.customer_360.update_many({"_id": {"$in": list(cids)}}, [{"$set": {"net_position": {"$sum": {
        "$map": {
            "input": {"$filter": {"input": {"$objectToArray": {"$ifNull": ["$accounts", {}]}},
                                  "cond": {"$ne": ["$$this.v._deleted", True]}}},
            "in": "$$this.v.balance"}}}}}])


def refresh_activity(db, cids):
    """recent_transactions (last 10), spend_30d per category, txn_count_30d."""
    if not cids:
        return
    cids = list(cids)
    since = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=30)
    live = {"customer_id": {"$in": cids}, "_deleted": False}

    recent = {r["_id"]: r["recent"] for r in db.transactions.aggregate([
        {"$match": live},
        {"$group": {"_id": "$customer_id", "recent": {"$topN": {
            "n": 10, "sortBy": {"ts": -1},
            "output": {"txn_id": "$_id", "ts": "$ts", "amount": "$amount", "direction": "$direction",
                       "category": "$category", "merchant": "$merchant", "city": "$city",
                       "channel": "$channel"}}}}}])}

    spend, counts = {}, {}
    for r in db.transactions.aggregate([
            {"$match": {**live, "ts": {"$gte": since}}},
            {"$group": {"_id": {"c": "$customer_id", "cat": "$category", "dir": "$direction"},
                        "total": {"$sum": "$amount"}, "n": {"$sum": 1}}}]):
        c = r["_id"]["c"]
        counts[c] = counts.get(c, 0) + r["n"]
        if r["_id"]["dir"] == "DR":
            spend.setdefault(c, {})[r["_id"]["cat"]] = round(r["total"], 2)

    ops = [UpdateOne({"_id": c}, {"$set": {"recent_transactions": recent.get(c, []),
                                           "spend_30d": spend.get(c, {}),
                                           "txn_count_30d": counts.get(c, 0)}}, upsert=True)
           for c in cids]
    write(db.customer_360, ops)


# ---------- one batch ----------

def process_batch(batch, db, stats):
    events = dedupe(batch)
    stats["collapsed"] += len(batch) - len(events)

    doc_ops, idx_ops, owners = [], [], {}
    balance_customers, txn_events = set(), []
    for topic, event in events:
        if topic == TOPIC_CUSTOMER:
            _, op = customer_op(event)
            doc_ops.append(op)
        elif topic == TOPIC_ACCOUNTS:
            aid, cid, doc_op, idx_op = account_ops(event)
            doc_ops.append(doc_op)
            idx_ops.append(idx_op)
            owners[aid] = cid
            balance_customers.add(cid)
        else:
            txn_events.append(event)

    # 1. customers + accounts first, so transactions in this batch can find their owners
    applied, stale = write(db.customer_360, doc_ops)
    write(db.account_index, idx_ops)
    refresh_net_position(db, balance_customers)

    # 2. transactions: new ones + anything waiting in the waiting room
    parked = list(db.pending_txns.find({}, {"event": 1}).limit(5000))
    candidates = txn_events + [p["event"] for p in parked]
    unknown = {int(row_of(e)["ACCOUNT_ID"]) for e in candidates} - owners.keys()
    if unknown:
        for doc in db.account_index.find({"_id": {"$in": list(unknown)}}):
            owners[doc["_id"]] = doc["customer_id"]

    txn_ops, park_ops, resolved, activity_customers = [], [], [], set()
    for e in candidates:
        row = row_of(e)
        aid, tid = int(row["ACCOUNT_ID"]), int(row["TXN_ID"])
        if aid in owners:
            txn_ops.append(txn_op(e, owners[aid]))
            activity_customers.add(owners[aid])
            resolved.append(tid)
        else:
            park_ops.append(UpdateOne({"_id": tid}, {"$set": {"event": e, "account_id": aid,
                                                             "parked_at": dt.datetime.now(dt.timezone.utc)}},
                                      upsert=True))
    t_applied, t_stale = write(db.transactions, txn_ops)
    write(db.pending_txns, park_ops)
    if resolved:
        db.pending_txns.delete_many({"_id": {"$in": resolved}})
    refresh_activity(db, activity_customers)

    stats["applied"] += applied + t_applied
    stats["stale"] += stale + t_stale
    stats["parked_now"] = db.pending_txns.estimated_document_count()


# ---------- main loop ----------

def main():
    load_dotenv()
    db = MongoClient(os.environ["MONGO_URI"])[os.getenv("MONGO_DB", "lumen_bank")]
    db.transactions.create_index([("customer_id", ASCENDING), ("ts", DESCENDING)])
    db.transactions.create_index("ts", expireAfterSeconds=HOT_WINDOW_DAYS * 86400)   # TTL: hot window

    consumer = Consumer({
        "bootstrap.servers": "127.0.0.1:29092",
        "group.id": "lumen-360-v1",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([TOPIC_CUSTOMER, TOPIC_ACCOUNTS, TOPIC_TXNS])
    stats = {"events": 0, "tombstones": 0, "collapsed": 0, "applied": 0, "stale": 0, "parked_now": 0}
    batch, started = [], time.time()
    print("Customer 360 consumer running. Ctrl+C to stop.")

    def flush():
        nonlocal batch
        if not batch:
            return
        process_batch(batch, db, stats)
        consumer.commit(asynchronous=False)          # bookmark AFTER the write
        print(f"batch {len(batch):>4} | events {stats['events']:>6} | applied {stats['applied']:>6} | "
              f"stale {stats['stale']:>4} | parked {stats['parked_now']:>4} | {time.time() - started:6.1f}s")
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
