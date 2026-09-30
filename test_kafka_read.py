from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'bankdb3.BANK.CUSTOMER',
    bootstrap_servers='localhost:29092',
    auto_offset_reset='earliest',
    enable_auto_commit=False,
    value_deserializer=lambda v: json.loads(v.decode('utf-8')) if v else None,
    consumer_timeout_ms=10000
)

count = 0
for message in consumer:
    count += 1
    event = message.value
    print(f"--- Event {count} ---")
    print("op:", event.get("op"))
    print("after:", event.get("after"))

print(f"\nTotal events read: {count}")