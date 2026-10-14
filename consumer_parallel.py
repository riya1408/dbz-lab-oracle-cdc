from dotenv import load_dotenv
import os, json, time, statistics
from concurrent.futures import ThreadPoolExecutor
from confluent_kafka import Consumer
from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError

load_dotenv()
coll = MongoClient(os.getenv("MONGO_URI"))["cdc_lab"]["customers_load"]

consumer = Consumer({
    'bootstrap.servers': '127.0.0.1:29092',
    'group.id': 'cdc-load-test-4',
    'auto.offset.reset': 'latest',        # only events produced from now on
    'enable.auto.commit': False,
})
consumer.subscribe(['bankdb3.BANK.CUSTOMER'])

BATCH_SIZE = 2000
WRITERS = 4                               # number of parallel writers
pool = ThreadPoolExecutor(max_workers=WRITERS)
start_ms = time.time() * 1000

e2e, dbz_lag, cons_lag, write_secs = [], [], [], []
written = rejected = collapsed = 0
first_new_event_at = last_write_at = None

def pct(values, p):
    if len(values) < 2:
        return values[0] if values else 0
    return statistics.quantiles(values, n=100)[p - 1]

def write_group(ops):
    # One writer: sends its own group of rows. Returns (written, rejected).
    if not ops:
        return 0, 0
    try:
        r = coll.bulk_write(ops, ordered=False)
        return r.matched_count + r.upserted_count, 0
    except BulkWriteError as e:
        d = e.details
        if any(w["code"] != 11000 for w in d["writeErrors"]):
            raise
        return d["nMatched"] + d["nUpserted"], len(d["writeErrors"])

def flush(batch):
    global written, rejected, collapsed, last_write_at
    newest = {}
    for cid, scn, txseq, after, commit_ts, kafka_ts in batch:
        if cid not in newest or (scn, txseq) > newest[cid][:2]:
            newest[cid] = (scn, txseq, after)
    collapsed += len(batch) - len(newest)

    # Split by customer: the same customer always lands in the same group
    groups = [[] for _ in range(WRITERS)]
    for cid, (scn, txseq, after) in newest.items():
        g = hash(str(cid)) % WRITERS
        groups[g].append(UpdateOne(
            {"_id": cid, "$or": [
                {"_last_scn": {"$exists": False}},
                {"_last_scn": {"$lt": scn}},
                {"_last_scn": scn, "_last_txseq": {"$lt": txseq}},
            ]},
            {"$set": {**after, "_last_scn": scn, "_last_txseq": txseq}},
            upsert=True))

    t0 = time.time()
    # Send all groups at the same time, then wait for every one to finish
    for w, rj in pool.map(write_group, groups):
        written += w
        rejected += rj
    write_secs.append(time.time() - t0)

    now = time.time() * 1000
    last_write_at = now
    for *_, commit_ts, kafka_ts in batch:
        if commit_ts is not None:
            e2e.append(now - commit_ts)
            dbz_lag.append(kafka_ts - commit_ts)
            cons_lag.append(now - kafka_ts)

batch, last_msg_at = [], time.time()
print(f"Waiting for events... ({WRITERS} parallel writers) run the Oracle insert now.")
try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            if batch:
                flush(batch); batch = []
            idle = time.time() - last_msg_at
            if (e2e and idle > 30) or idle > 900:
                break
            continue
        if msg.error():
            print("Error:", msg.error()); continue
        last_msg_at = time.time()
        if msg.value() is None:
            continue
        event = json.loads(msg.value())
        after = event.get("after")
        if after is None:
            continue
        src = event["source"]
        commit_ts = src.get("commit_ts_ms") or src.get("ts_ms")
        kafka_ts = msg.timestamp()[1]
        is_new = src.get("snapshot") == "false" and commit_ts >= start_ms
        if is_new and first_new_event_at is None:
            first_new_event_at = time.time() * 1000
        batch.append((after["CUSTOMER_ID"], int(src["scn"]),
                      int(src.get("txSeq") or 0), after,
                      commit_ts if is_new else None, kafka_ts))
        if len(batch) >= BATCH_SIZE:
            flush(batch); batch = []
finally:
    consumer.close()
    pool.shutdown()

n = len(e2e)
print(f"\nNew events measured : {n}")
print(f"Written / rejected / collapsed: {written} / {rejected} / {collapsed}")
if n:
    secs = (last_write_at - first_new_event_at) / 1000
    print(f"Throughput          : {n / secs:.0f} events/sec over {secs:.1f}s")
    for name, vals in [("End-to-end", e2e), ("Oracle->Kafka (Debezium)", dbz_lag),
                       ("Kafka->MongoDB (consumer)", cons_lag)]:
        print(f"{name:26}: p50 {pct(vals,50)/1000:6.2f}s   p95 {pct(vals,95)/1000:6.2f}s")
    print(f"bulk_write per batch: avg {statistics.mean(write_secs):.2f}s over {len(write_secs)} batches")
