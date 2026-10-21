"""Register (or update) the Debezium Oracle connector.

Reads connector.json, fills in the password from .env, and sends it to Kafka Connect.
Uses PUT /connectors/<name>/config, which creates the connector if it doesn't exist
and updates it if it does - so running this script twice is safe (idempotent).
"""
import json
import os
import sys
import time
import urllib.error
import urllib.request

from dotenv import load_dotenv

load_dotenv()
CONNECT_URL = "http://127.0.0.1:8083"

with open("connector.json") as f:
    connector = json.load(f)
name = connector["name"]
config = connector["config"]
config["database.password"] = os.environ["DBZ_PASSWORD"]

# 1. Wait for Kafka Connect's REST API to come up
for attempt in range(60):
    try:
        urllib.request.urlopen(CONNECT_URL + "/", timeout=3)
        break
    except Exception:
        print("Waiting for Kafka Connect...")
        time.sleep(5)
else:
    sys.exit("Kafka Connect did not come up. Check: docker compose logs connect")

# 2. Create or update the connector
request = urllib.request.Request(
    f"{CONNECT_URL}/connectors/{name}/config",
    data=json.dumps(config).encode(),
    headers={"Content-Type": "application/json"},
    method="PUT",
)
try:
    with urllib.request.urlopen(request) as response:
        print(f"Connector '{name}' registered (HTTP {response.status})")
except urllib.error.HTTPError as e:
    sys.exit(f"Connect rejected the config (HTTP {e.code}): {e.read().decode()}")

# 3. Show its status
time.sleep(10)
with urllib.request.urlopen(f"{CONNECT_URL}/connectors/{name}/status") as response:
    status = json.load(response)
print("Connector:", status["connector"]["state"])
for task in status.get("tasks", []):
    print(f"Task {task['id']}:", task["state"])
    if task["state"] == "FAILED":
        print(task.get("trace", "")[:1500])
