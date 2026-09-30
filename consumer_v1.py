from dotenv import load_dotenv
import os
import json
from confluent_kafka import Consumer
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

load_dotenv()
mongo_uri = os.getenv("MONGO_URI")

client = MongoClient(mongo_uri)
db = client["cdc_lab"]
collection = db["customers"]

consumer = Consumer({
    'bootstrap.servers': '127.0.0.1:29092',
    'group.id': 'cdc-lab-consumer-v3',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,
})

consumer.subscribe(['bankdb3.BANK.CUSTOMER'])

accepted = 0
rejected = 0

try:
    while True:
        msg = consumer.poll(timeout=5.0)
        if msg is None:
            break
        if msg.error():
            print("Error:", msg.error())
            continue

        event = json.loads(msg.value().decode('utf-8'))
        after = event.get("after")

        if after is None:
            continue

        customer_id = after["CUSTOMER_ID"]
        scn = int(event["source"]["scn"])
        raw_txseq = event["source"].get("txSeq")
        txseq = int(raw_txseq) if raw_txseq is not None else 0

        try:
            result = collection.update_one(
                {
                    "_id": customer_id,
                    "$or": [
                        {"_last_scn": {"$exists": False}},
                        {"_last_scn": {"$lt": scn}},
                        {"_last_scn": scn, "_last_txseq": {"$lt": txseq}}
                    ]
                },
                {"$set": {**after, "_last_scn": scn, "_last_txseq": txseq}},
                upsert=True
            )
            if result.matched_count > 0 or result.upserted_id is not None:
                accepted += 1
                print(f"ACCEPTED customer {customer_id} (scn={scn}, txseq={txseq}): {after['NAME']} - {after['CITY']} - {after['SEGMENT']}")
            else:
                rejected += 1
                print(f"REJECTED customer {customer_id} (scn={scn}, txseq={txseq}) - stale")
        except DuplicateKeyError:
            rejected += 1
            print(f"REJECTED customer {customer_id} (scn={scn}, txseq={txseq}) - stale (duplicate key)")

finally:
    consumer.close()

print(f"\nAccepted: {accepted}, Rejected: {rejected}")