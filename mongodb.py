import time
import json
from pymongo import MongoClient

# Setup connection (Part of M1 Milestone)
client = MongoClient("mongodb://localhost:27017/")
db = client["performance_test"]
collection = db["measurements"]

data_file = "data.json"
groups = ["A", "B", "C"]


# Load data from JSON file
with open(data_file, "r") as f:
    data = json.load(f)

insert_durations = []

for group in groups:

    data_group = [item for item in data if item["group"] == group]
    
    start_time = time.time()
    collection.insert_many(data_group)
    duration = time.time() - start_time
    insert_durations.append(duration)

    print(f"MongoDB Insertion Time for group {group}: {duration:.4f} seconds")


# Aggregation Queries: SUM value per group (profiled like insertions)
agg_durations = []

for group in groups:
    pipeline = [
        {"$match": {"group": group}},
        {"$group": {"_id": "$group", "total_value": {"$sum": "$value"}}}
    ]

    start_time = time.time()
    results = list(collection.aggregate(pipeline))
    duration = time.time() - start_time
    agg_durations.append(duration)

    print(f"MongoDB Aggregation Time for group {group}: {duration:.4f} seconds — Result: {results}")

print(f"Aggregation durations: {agg_durations}")