import os
import json
import time
from pymongo import MongoClient
import redis

# Connection URIs
mongo_uri = os.getenv("MONGO_URI", "mongodb://mongodb.default.svc.cluster.local:27017/?directConnection=true")
redis_host = os.getenv("REDIS_HOST", "redis.default.svc.cluster.local")
redis_port = int(os.getenv("REDIS_PORT_NUM", 6379))

print("🔗 Connecting to MongoDB...")
mongo_client = MongoClient(mongo_uri)
db = mongo_client["traffic_system"] 
routes_col = db["assigned_routes"]

print("🔗 Connecting to Redis...")
r = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)

# Find Truck_001
truck = routes_col.find_one({"vehicle_id": "Truck_001"})
if not truck:
    print("❌ Truck_001 not found in MongoDB. Please assign orders to Truck_001 first.")
    exit(1)

route = truck.get("new_assigned_route") or truck.get("assigned_route") or []
if len(route) < 3:
    print(f"❌ Route too short ({len(route)} edges). Please click on map to assign more destinations first.")
    exit(1)

# Pick an edge in the middle of the route
target_edge = route[len(route) // 2]
print(f"🎯 Selected target edge for congestion: {target_edge}")

print("Ghi đè trạng thái lên Redis liên tục trong 60 giây (để tránh Spark/Bot simulator ghi đè)...")
payload = {
    "edge_id": target_edge,
    "avg_speed": 2.5,
    "vehicle_count": 85,
    "distance": 200.0,
    "estimated_travel_time": 288.0,
    "is_congested": True,
    "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
}

try:
    for i in range(60):
        # 1. Write JSON string to edge:<edge_id>
        r.set(f"edge:{target_edge}", json.dumps(payload), ex=120)
        # 2. Set simulation lock key for Spark to ignore
        r.set(f"simulate_congestion:{target_edge}", "1", ex=120)
        # 3. Add to blocked_edges set
        r.sadd("blocked_edges", target_edge)
        
        print(f"[{i+1}/60] Congested {target_edge} (speed=2.5 km/h, blocked_edges=True)")
        time.sleep(1)
except KeyboardInterrupt:
    print("\nStopped simulating congestion.")

# Cleanup
r.srem("blocked_edges", target_edge)
r.delete(f"simulate_congestion:{target_edge}")
print("🧹 Cleaned up blocked_edges and simulation locks.")
