from dotenv import load_dotenv
import os
import json
from confluent_kafka import Consumer
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

load_dotenv()
client = MongoClient(os.getenv("MONGO_URI"))
collection = client[os.getenv("MONGO_DB", "cdc_lab")]["customers"]

consumer = Consumer({
    'bootstrap.servers': '127.0.0.1:29092',
    'group.id': 'cdc-lab-consumer-v4',
    # Only used the very first time (no saved bookmark yet). 'earliest' = read the snapshot too.
    'auto.offset.reset': os.getenv("OFFSET_RESET", "earliest"),
    'enable.auto.commit': False,       # we save the bookmark ourselves, AFTER writing
})

def on_assign(c, partitions):
    print("Ready - make your changes in Oracle now. Press Ctrl+C to stop.")

consumer.subscribe(['bankdb3.BANK.CUSTOMER'], on_assign=on_assign)

accepted = deleted = rejected = tombstones = 0

def guarded_write(customer_id, fields, scn, txseq):
    """Write only if this event is newer than what MongoDB already has.
    Returns True if written, False if the event was stale."""
    try:
        result = collection.update_one(
            {
                "_id": customer_id,
                "$or": [
                    {"_last_scn": {"$exists": False}},
                    {"_last_scn": {"$lt": scn}},
                    {"_last_scn": scn, "_last_txseq": {"$lt": txseq}},
                ],
            },
            {"$set": {**fields, "_last_scn": scn, "_last_txseq": txseq}},
            upsert=True,
        )
        return result.matched_count > 0 or result.upserted_id is not None
    except DuplicateKeyError:
        return False   # document exists with a newer version -> stale event

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            print("Error:", msg.error())
            continue

        # 1. Tombstone: empty message Debezium sends after a delete (for Kafka cleanup)
        if msg.value() is None:
            tombstones += 1
            print("TOMBSTONE skipped (nothing to do in MongoDB)")
            consumer.commit(message=msg, asynchronous=False)
            continue

        event = json.loads(msg.value().decode('utf-8'))
        op = event.get("op")
        source = event["source"]
        scn = int(source["scn"])
        raw_txseq = source.get("txSeq")
        txseq = int(raw_txseq) if raw_txseq is not None else 0

        if op == "d":
            # 2. Delete: the row is gone, so 'after' is empty -> the ID is in 'before'
            before = event["before"]
            customer_id = before["CUSTOMER_ID"]
            # Soft delete: keep the document as a marker WITH its version number,
            # so an old replayed insert/update can't bring the customer back.
            if guarded_write(customer_id, {"_deleted": True}, scn, txseq):
                deleted += 1
                print(f"DELETED  customer {customer_id} (scn={scn}, txseq={txseq})")
            else:
                rejected += 1
                print(f"REJECTED delete of {customer_id} (scn={scn}, txseq={txseq}) - stale")
        else:
            # 3. Snapshot (r), insert (c) or update (u)
            after = event.get("after")
            if after is None:
                consumer.commit(message=msg, asynchronous=False)
                continue
            customer_id = after["CUSTOMER_ID"]
            if guarded_write(customer_id, {**after, "_deleted": False}, scn, txseq):
                accepted += 1
                print(f"ACCEPTED customer {customer_id} op={op} (scn={scn}, txseq={txseq}): "
                      f"{after['NAME']} - {after['CITY']} - {after['SEGMENT']}")
            else:
                rejected += 1
                print(f"REJECTED customer {customer_id} op={op} (scn={scn}, txseq={txseq}) - stale")

        # 4. Save the bookmark ONLY after MongoDB finished. Crash before this = replay, not loss.
        consumer.commit(message=msg, asynchronous=False)

except KeyboardInterrupt:
    print("\nStopping...")
finally:
    consumer.close()

print(f"Accepted: {accepted}, Deleted: {deleted}, Rejected: {rejected}, Tombstones: {tombstones}")
