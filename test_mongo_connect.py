from dotenv import load_dotenv
import os
from pymongo import MongoClient

load_dotenv()
mongo_uri = os.getenv("MONGO_URI")

client = MongoClient(mongo_uri)

try:
    client.admin.command('ping')
    print("Connected to MongoDB successfully!")
except Exception as e:
    print("Connection failed:", e)