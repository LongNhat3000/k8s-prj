import os
import time
from pymongo import MongoClient

mongo_uri = os.getenv("MONGO_URI", "mongodb://mongodb.default.svc.cluster.local:27017/?directConnection=true")
mongo_client = MongoClient(mongo_uri)
db = mongo_client["traffic_system"]
routes_col = db["assigned_routes"]

vehicle_id = "Truck_001"

# Assign 3 points in Dong Da / Ba Dinh area
destinations = [
    {"cust_id": f"Cust_{vehicle_id}_1", "latitude": 21.0263027, "longitude": 105.8374665, "order": 1, "status": "pending"},
    {"cust_id": f"Cust_{vehicle_id}_2", "latitude": 21.0265438, "longitude": 105.8370144, "order": 2, "status": "pending"},
    {"cust_id": f"Cust_{vehicle_id}_3", "latitude": 21.0268901, "longitude": 105.8363651, "order": 3, "status": "pending"}
]

print(f"Assigning 3 orders to {vehicle_id} in MongoDB...")
routes_col.update_one(
    {"vehicle_id": vehicle_id},
    {
        "$set": {
            "customers": destinations,
            "remaining_customers": destinations,
            "needs_optimization": True
        }
    },
    upsert=True
)
print("Done. Check logs of route-optimization to see the GA algorithm execute.")
