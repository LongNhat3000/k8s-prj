import json
import time
import random
import os
import heapq
from kafka_producer import GPSProducer

# --- BIẾN TOÀN CỤC MÔ PHỎNG VẬT LÝ ---
RHO_MAX = 250       # Mật độ tối đa (xe/km) — đường đô thị HN
V_MIN = 8.0         # Tốc độ tối thiểu khi tắc nặng (km/h) — thực tế HN: 5-8
edge_vehicle_count = {} 
STUCK_TIMEOUT = 3

ATTRACTOR_EDGES = [] 

# --- GRAPH LOOKUP ---
edge_by_id = {}       # edge_id → edge object
graph_adj = {}        # node_id → [edge, ...]

# --- MONGODB ROUTES (cho Truck) ---
mongo_routes = {}     # vehicle_id → list of edge_ids từ MongoDB


def load_map_graph(edges_filepath):
    print("Đang nạp bản đồ vào bộ nhớ...")
    with open(edges_filepath, 'r', encoding='utf-8') as f:
        edges = json.load(f)
    
    for edge in edges:
        edge_id = edge['edge_id']
        edge_vehicle_count[edge_id] = 0
        edge_by_id[edge_id] = edge
        
        start = edge['start_node']['node_id']
        if start not in graph_adj:
            graph_adj[start] = []
        graph_adj[start].append(edge)
    
    global ATTRACTOR_EDGES
    ATTRACTOR_EDGES = random.sample(edges, min(40, len(edges)))
    print(f"Đã nạp {len(edges)} edges, {len(graph_adj)} nodes.")
    print(f"Đã thiết lập {len(ATTRACTOR_EDGES)} trung tâm thu hút giao thông.")
        
    return edges


def load_routes_from_mongo():
    """
    Đọc routes từ MongoDB (new_assigned_route hoặc assigned_route)
    để Truck đi đúng route hiển thị trên dashboard.
    """
    global mongo_routes
    mongo_uri = os.getenv("MONGO_URI", "mongodb://mongodb:27017/")
    
    try:
        from pymongo import MongoClient
        client = MongoClient(mongo_uri, serverSelectionTimeoutMS=10000)
        db = client["traffic_system"]
        coll = db["assigned_routes"]
        
        count = 0
        for doc in coll.find({"vehicle_id": {"$regex": "^Truck_"}}):
            vid = doc["vehicle_id"]
            # Ưu tiên new_assigned_route (GA đã tối ưu), fallback assigned_route
            route = doc.get("new_assigned_route") or doc.get("assigned_route") or []
            if route and len(route) >= 2:
                # Validate: chỉ giữ edge_ids tồn tại trong graph
                valid_route = [eid for eid in route if eid in edge_by_id]
                if len(valid_route) >= 2:
                    mongo_routes[vid] = valid_route
                    count += 1
        
        client.close()
        print(f"✅ Đã tải {count} routes từ MongoDB cho Trucks.")
        
    except Exception as e:
        print(f"⚠️ Không kết nối được MongoDB: {e}")
        print("   → Trucks sẽ dùng Dijkstra fallback (route có thể khác dashboard).")


# ============================================================
# DIJKSTRA SHORTEST PATH (fallback khi không có MongoDB route)
# ============================================================

def dijkstra(start_node_id, end_node_id):
    """Tìm đường ngắn nhất → trả về list edge objects liên tục."""
    if start_node_id == end_node_id:
        return []
    if start_node_id not in graph_adj:
        return []
    
    dist = {start_node_id: 0}
    prev = {}  # node → (parent_node, edge)
    visited = set()
    heap = [(0, start_node_id)]
    
    while heap:
        cost, node = heapq.heappop(heap)
        if node in visited:
            continue
        visited.add(node)
        if node == end_node_id:
            break
        
        for edge in graph_adj.get(node, []):
            next_node = edge['end_node']['node_id']
            if next_node in visited:
                continue
            length_m = edge['length_meters']
            speed_kmh = max(edge['max_speed_kmh'], 1.0)
            edge_cost = length_m / (speed_kmh * 1000 / 3600)  # seconds
            new_cost = cost + edge_cost
            if new_cost < dist.get(next_node, float('inf')):
                dist[next_node] = new_cost
                prev[next_node] = (node, edge)
                heapq.heappush(heap, (new_cost, next_node))
    
    if end_node_id not in prev:
        return []
    
    path = []
    cur = end_node_id
    while cur != start_node_id:
        parent, edge = prev[cur]
        path.append(edge)
        cur = parent
    path.reverse()
    return path


def nearest_node(lat, lon):
    """Tìm node gần nhất (Euclidean)."""
    best = None
    best_d = float('inf')
    for node_id, edges_from in graph_adj.items():
        if edges_from:
            n = edges_from[0]['start_node']
            d = (n['lat'] - lat) ** 2 + (n['lon'] - lon) ** 2
            if d < best_d:
                best_d = d
                best = node_id
    return best


# ============================================================
# VEHICLE CLASS
# ============================================================

class Vehicle:
    cached_edge_lengths = []

    def __init__(self, v_id, v_type, all_edges):
        self.entity_id = v_id
        self.entity_type = v_type
        self.all_edges = all_edges
        
        if not Vehicle.cached_edge_lengths:
            Vehicle.cached_edge_lengths = [max(e['length_meters'], 1.0) for e in all_edges]
            
        self._spawn()

    def _spawn(self):
        """Khởi tạo xe lần đầu hoặc khi hoàn thành nhiệm vụ."""
        if self.entity_type == "Truck":
            self._init_truck_route()
        else:
            # Bot: spawn ngẫu nhiên
            self.current_edge = random.choices(self.all_edges, weights=Vehicle.cached_edge_lengths, k=1)[0]
            self.latitude = self.current_edge['start_node']['lat']
            self.longitude = self.current_edge['start_node']['lon']
            self.progress_meters = 0.0
            self.speed = self.current_edge['max_speed_kmh']
            self.stuck_count = 0
            edge_vehicle_count[self.current_edge['edge_id']] += 1
            self._init_bot_target()

    def _init_bot_target(self):
        """Bot: chọn 1 đích ngẫu nhiên, 20% vào điểm nóng."""
        if random.random() < 0.20:
            self.target_edge = random.choice(ATTRACTOR_EDGES)
        else:
            self.target_edge = random.choice(self.all_edges)
        # Bot dùng greedy routing (đơn giản, đủ simulate traffic)
        self._route_edges = None  

    def _init_truck_route(self):
        """
        Truck: đọc route từ MongoDB (đã load vào mongo_routes).
        Nếu không có → fallback Dijkstra tự tính.
        Xe SPAWN TẠI edge đầu tiên của route → đi đúng đường trên dashboard.
        """
        vid = self.entity_id
        
        if vid in mongo_routes:
            # ĐỌC ROUTE TỪ MONGODB — đi đúng đường hiển thị trên frontend
            route_edge_ids = mongo_routes[vid]
            self._route_edges = [edge_by_id[eid] for eid in route_edge_ids]
            self._route_index = 0
            
            # Spawn tại edge đầu tiên
            first_edge = self._route_edges[0]
            self.current_edge = first_edge
            self.latitude = first_edge['start_node']['lat']
            self.longitude = first_edge['start_node']['lon']
            self.progress_meters = 0.0
            self.speed = first_edge['max_speed_kmh']
            self.stuck_count = 0
            edge_vehicle_count[first_edge['edge_id']] += 1
            
        else:
            # FALLBACK: không có route trong MongoDB → tự tính Dijkstra
            self.current_edge = random.choices(self.all_edges, weights=Vehicle.cached_edge_lengths, k=1)[0]
            self.latitude = self.current_edge['start_node']['lat']
            self.longitude = self.current_edge['start_node']['lon']
            self.progress_meters = 0.0
            self.speed = self.current_edge['max_speed_kmh']
            self.stuck_count = 0
            edge_vehicle_count[self.current_edge['edge_id']] += 1
            self._fallback_dijkstra_route()

    def _fallback_dijkstra_route(self):
        """Fallback: tính Dijkstra route khi không có MongoDB data."""
        customer_edges = random.sample(self.all_edges, min(10, len(self.all_edges)))
        
        current_node = self.current_edge['end_node']['node_id']
        full_route = []
        unvisited = list(range(len(customer_edges)))
        
        while unvisited:
            best_idx = unvisited[0]
            best_dist = float('inf')
            cur_coords = graph_adj.get(current_node, [{}])
            if cur_coords and cur_coords[0]:
                cur_lat = cur_coords[0]['start_node']['lat']
                cur_lon = cur_coords[0]['start_node']['lon']
            else:
                cur_lat, cur_lon = self.latitude, self.longitude
            
            for i in unvisited:
                c = customer_edges[i]
                d = (c['start_node']['lat'] - cur_lat) ** 2 + (c['start_node']['lon'] - cur_lon) ** 2
                if d < best_dist:
                    best_dist = d
                    best_idx = i
            
            unvisited.remove(best_idx)
            target_node = customer_edges[best_idx]['start_node']['node_id']
            
            segment = dijkstra(current_node, target_node)
            if segment:
                full_route.extend(segment)
                current_node = target_node
        
        self._route_edges = full_route if full_route else None
        self._route_index = 0
        
        if not self._route_edges:
            self._route_edges = None
            self.target_edge = random.choice(self.all_edges)

    def move(self):
        edge_id = self.current_edge['edge_id']
        length_m = self.current_edge['length_meters']
        max_speed = self.current_edge['max_speed_kmh']
        
        # 1. Greenshields tính vận tốc
        n_vehicles = edge_vehicle_count[edge_id]
        # Minimum capacity: edge ngắn (<60m) vẫn chứa ít nhất 15 xe trước khi tắc
        effective_length_km = max(length_m / 1000, 15.0 / RHO_MAX)
        density = n_vehicles / effective_length_km if length_m > 0 else 0
        if density >= RHO_MAX:
            self.speed = V_MIN
        else:
            self.speed = max(V_MIN, max_speed * (1 - (density / RHO_MAX)))
            
        speed_ms = self.speed * (1000 / 3600)
        self.progress_meters += speed_ms
        
        # 2. Đi hết edge hiện tại → chuyển edge tiếp theo
        if self.progress_meters >= length_m:
            edge_vehicle_count[edge_id] -= 1
            
            if self.entity_type == "Truck" and self._route_edges:
                self._move_truck_on_route()
            else:
                self._move_greedy()
        else:
            # Interpolate vị trí trên edge
            ratio = self.progress_meters / length_m
            s = self.current_edge['start_node']
            e = self.current_edge['end_node']
            self.latitude = s['lat'] + (e['lat'] - s['lat']) * ratio
            self.longitude = s['lon'] + (e['lon'] - s['lon']) * ratio

    def _move_truck_on_route(self):
        """Truck đi theo route từ MongoDB — từng edge một."""
        self._route_index += 1
        
        if self._route_index >= len(self._route_edges):
            # Hoàn thành toàn bộ route → quay lại từ đầu (loop)
            self._route_index = 0
            first_edge = self._route_edges[0]
            self._enter_edge(first_edge)
            return
        
        next_edge = self._route_edges[self._route_index]
        
        # Gatekeeping: kiểm tra edge tiếp theo có đầy không
        next_length = next_edge['length_meters']
        next_capacity = max(15, RHO_MAX * (next_length / 1000))
        
        if edge_vehicle_count[next_edge['edge_id']] >= next_capacity:
            # Chờ tại ngã tư — giữ nguyên vị trí
            self._route_index -= 1  # Rollback index
            self.progress_meters = self.current_edge['length_meters']
            self.speed = V_MIN
            edge_vehicle_count[self.current_edge['edge_id']] += 1
            self.stuck_count += 1
            
            if self.stuck_count >= STUCK_TIMEOUT * 3:
                # Anti-deadlock: skip edge bị tắc
                self.stuck_count = 0
                self._route_index += 1
                if self._route_index >= len(self._route_edges):
                    self._route_index = 0
                next_edge = self._route_edges[self._route_index]
                self._enter_edge(next_edge)
        else:
            self._enter_edge(next_edge)
            self.stuck_count = 0

    def _move_greedy(self):
        """Bot / Truck fallback: greedy routing tới target_edge."""
        # Kiểm tra đã tới đích chưa
        if self.current_edge['edge_id'] == getattr(self, 'target_edge', {}).get('edge_id', ''):
            self._spawn()
            return
        
        end_node = self.current_edge['end_node']['node_id']
        next_edges = graph_adj.get(end_node, [])
        
        if not next_edges:
            self._spawn()
            return
        
        # Chọn edge gần target nhất
        target_lat = self.target_edge['start_node']['lat']
        target_lon = self.target_edge['start_node']['lon']
        
        best_edge = next_edges[0]
        min_dist = float('inf')
        for edge in next_edges:
            d = (edge['end_node']['lat'] - target_lat)**2 + (edge['end_node']['lon'] - target_lon)**2
            if d < min_dist:
                min_dist = d
                best_edge = edge
        
        # Gatekeeping
        next_length = best_edge['length_meters']
        next_capacity = max(15, RHO_MAX * (next_length / 1000))
        
        if edge_vehicle_count[best_edge['edge_id']] >= next_capacity:
            self.progress_meters = self.current_edge['length_meters']
            self.speed = V_MIN
            edge_vehicle_count[self.current_edge['edge_id']] += 1
            self.stuck_count += 1
            if self.stuck_count >= STUCK_TIMEOUT:
                self.stuck_count = 0
                self.target_edge = random.choice(self.all_edges)
        else:
            self._enter_edge(best_edge)
            self.stuck_count = 0

    def _enter_edge(self, edge):
        """Chuyển xe sang edge mới."""
        self.current_edge = edge
        self.progress_meters = 0.0
        self.latitude = edge['start_node']['lat']
        self.longitude = edge['start_node']['lon']
        edge_vehicle_count[edge['edge_id']] += 1

    def to_json_message(self):
        msg = {
            "entity_id": self.entity_id,
            "entity_type": self.entity_type,
            "latitude": round(self.latitude, 6),
            "longitude": round(self.longitude, 6),
            "speed": round(self.speed, 2),
            "timestamp": int(time.time() * 1000),
            "edge_id": self.current_edge['edge_id'],
        }
        # Truck gửi thêm route progress
        if self.entity_type == "Truck" and self._route_edges:
            msg["route_index"] = self._route_index
            msg["route_total"] = len(self._route_edges)
        return msg


if __name__ == "__main__":
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    edges_path = os.path.join(BASE_DIR, "data", "edges_schema.json")
    all_edges = load_map_graph(edges_path)
    
    # Tải routes từ MongoDB TRƯỚC khi khởi tạo Trucks
    print("Đang tải routes từ MongoDB...")
    load_routes_from_mongo()
    
    producer = GPSProducer()
    
    NUM_BOTS = int(os.getenv("NUM_BOTS", "3000"))
    TICK_INTERVAL = float(os.getenv("TICK_INTERVAL", "2.0"))
    
    print(f"Đang khởi tạo {NUM_BOTS} Bots và 100 Trucks (interval={TICK_INTERVAL}s)...")
    vehicles = [Vehicle(f"Bot_{i:04d}", "Bot", all_edges) for i in range(1, NUM_BOTS + 1)]
    
    print("Khởi tạo 100 Trucks (đi theo route MongoDB)...")
    trucks = [Vehicle(f"Truck_{i:03d}", "Truck", all_edges) for i in range(1, 101)]
    vehicles.extend(trucks)
    
    mongo_count = sum(1 for t in trucks if t._route_edges and t.entity_id in mongo_routes)
    fallback_count = len(trucks) - mongo_count
    print(f"✅ {mongo_count} Trucks dùng route MongoDB, {fallback_count} Trucks dùng Dijkstra fallback.")
    print(f"   Trung bình {sum(len(t._route_edges or []) for t in trucks) // max(1, len(trucks))} edges/truck.")
    
    print("Bắt đầu mô phỏng giao thông thời gian thực! (Ctrl+C để dừng)")
    try:
        while True:
            start_time = time.time()
            for v in vehicles:
                v.move()
                producer.produce_message(v.to_json_message())
            
            producer.flush()
            
            elapsed = time.time() - start_time
            sleep_time = max(0, TICK_INTERVAL - elapsed)
            time.sleep(sleep_time)
            
    except KeyboardInterrupt:
        print("\nĐã dừng mô phỏng an toàn.")
