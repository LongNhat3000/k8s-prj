# 🚚 Logistics Real-Time: Tối ưu tuyến đường giao hàng thời gian thực

## 1. Mô tả bài toán

Một công ty vận chuyển quản lý **100 xe tải** hoạt động trên bản đồ Hà Nội. Mỗi xe phục vụ **10 khách hàng** tại các vị trí khác nhau. Hệ thống cần:

1. **Mô phỏng giao thông** — 10.000 phương tiện (100 xe tải + 9.900 xe máy/ô tô) di chuyển trên 9.026 cạnh đường, tạo dữ liệu GPS real-time.
2. **Xử lý luồng dữ liệu lớn** — Nhận ~10.000 GPS messages/giây từ Kafka, map-matching vào đoạn đường, tính tốc độ trung bình mỗi cạnh.
3. **Phát hiện tắc nghẽn** — Cạnh có avg_speed ≤ 5 km/h → đánh dấu tắc đường.
4. **Tối ưu tuyến đường** — Dùng thuật toán di truyền (GA) tìm thứ tự giao hàng tối ưu cho mỗi xe, tránh đoạn tắc, và tính shortest-path (Dijkstra) qua 10 điểm giao hàng.
5. **Dashboard trực quan** — Bản đồ realtime hiển thị xe, traffic heatmap, và lộ trình tối ưu khi chọn xe.

**Input**: Bản đồ Hà Nội (OSM), 100 xe × 10 khách hàng = 1.000 điểm giao hàng.  
**Output**: Tuyến đường tối ưu liên tục (danh sách edge_id) cho từng xe, cập nhật realtime khi giao thông thay đổi.

---

## 2. Kiến trúc hệ thống

```
┌────────────────┐         ┌─────────┐         ┌──────────────────┐         ┌───────┐
│  Bot Simulation│────────▶│  KAFKA  │────────▶│ Stream Processing│────────▶│ REDIS │
│  (10k xe/GPS)  │  topic: │         │         │ (PySpark)        │  edge:* │       │
└────────────────┘ gps_stream└────┬────┘         └──────────────────┘         └───┬───┘
                                  │                                                │
                                  ▼                                                ▼
                    ┌──────────────────────┐                          ┌────────────────────┐
                    │  Dashboard Backend   │◀─── poll traffic ────────│                    │
                    │  (Node.js+Socket.IO) │                          │   Frontend         │
                    │  + PathFinder        │───── websocket ─────────▶│   (React+Leaflet)  │
                    └──────────┬───────────┘                          └────────────────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │      MongoDB         │◀──── Change Stream
                    │  (assigned_routes)   │
                    └──────────┬───────────┘
                               │
                    ┌──────────▼───────────┐
                    │  Route Optimization  │
                    │  (Genetic Algorithm) │
                    │  100 xe × 10 khách   │
                    └──────────────────────┘
```

| Module             | Công nghệ                        | Vai trò                                         |
| ------------------ | -------------------------------- | ----------------------------------------------- |
| Data Ingestion     | Python, confluent-kafka          | Mô phỏng 10k xe, gửi GPS → Kafka                |
| Stream Processing  | PySpark Structured Streaming     | Map-matching GPS → edge, tính avg_speed → Redis |
| Route Optimization | Python, GA + Dijkstra            | Tối ưu thứ tự 10 khách/xe, tính shortest-path   |
| Backend            | Node.js, Socket.IO, KafkaJS      | Trung gian: Kafka + Redis + MongoDB → Frontend  |
| Frontend           | React, Leaflet Canvas            | Dashboard realtime                              |
| Infrastructure     | Kafka, Redis, MongoDB, Zookeeper | Trên Kubernetes                                 |

---

## 3. Thuật toán tối ưu tuyến đường

**Bài toán**: Cho xe tải tại vị trí hiện tại, cần giao hàng cho 10 khách → tìm thứ tự đi + đường đi ngắn nhất.

### Bước 1: Genetic Algorithm (GA) — tối ưu thứ tự khách hàng

- **Cá thể**: Hoán vị [K1, K2, ..., K10] (thứ tự giao hàng)
- **Fitness**: Tổng thời gian di chuyển (dựa trên avg_speed realtime từ Redis)
- **Operators**: Crossover (OX), Mutation (swap), Selection (tournament)
- **Population**: 30 cá thể × 60 thế hệ
- **Kết quả**: Thứ tự giao hàng tối ưu tránh đoạn tắc

### Bước 2: Dijkstra — tìm đường ngắn nhất giữa mỗi cặp

Sau khi GA cho thứ tự [K3, K7, K1, ...], tính shortest-path liên tục:

```
Vị trí xe → K3 → K7 → K1 → ... → K10
     Dijkstra  Dijkstra  Dijkstra
```

**Kết quả cuối**: Mảng edge_id liên tục — xe đi chính xác trên đường, không "chim bay".

### Bước 3: Re-optimize khi giao thông thay đổi

Service route-optimization chạy vòng lặp mỗi 30s:

- Đọc traffic mới từ Redis (blocked_edges)
- Nếu route hiện tại đi qua đoạn tắc → chạy lại GA + Dijkstra
- Ghi `new_assigned_route` vào MongoDB → Backend nhận qua Change Stream → Push tới Frontend

---

## 4. Chạy với Kubernetes

```bash
# 1. Build images
docker build -t logictics-bot:latest       -f Dockerfile.data_ingestion .
docker build -t logictics-stream:v4        -f Dockerfile.stream_processing .
docker build -t logictics-opt:v5           -f Dockerfile.route_optimization .
docker build -t logictics-backend:v7       -f Dockerfile.backend .
docker build -t logictics-frontend:v4      -f Dockerfile.frontend .

# 2. Deploy
kubectl apply -f k8s/01-infrastructure.yaml
kubectl get pods -w                          # Đợi Running

# 3. Init MongoDB ReplicaSet
kubectl exec -it $(kubectl get pod -l app=mongodb -o name) -- mongosh --eval \
  "rs.initiate({_id:'rs0', members:[{_id:0, host:'mongodb.default.svc.cluster.local:27017'}]})"

# 4. Deploy microservices
kubectl apply -f k8s/02-microservices.yaml

# 5. Seed 100 xe × 10 khách hàng
kubectl exec -it $(kubectl get pod -l app=route-optimization -o jsonpath="{.items[0].metadata.name}") -- python route_optimization/seed_assigned_routes.py --count 100

# 6. Mở dashboard
kubectl port-forward svc/dashboard-frontend 3000:5173
# → http://localhost:3000
```

---

## 5. Chạy với Docker Compose

```bash
cd infrastructure
docker-compose up -d --build

# Seed data
docker compose exec route_optimization python route_optimization/seed_assigned_routes.py --count 100

# Dashboard: http://localhost:5173
```

---

## 6. Tạo tuyến đường tối ưu cho 100 xe

### Seed dữ liệu

```bash
python route_optimization/seed_assigned_routes.py --count 100
```

Tạo 100 document trong MongoDB `traffic_system.assigned_routes`:

```json
{
  "vehicle_id": "Truck_001",
  "assigned_route": ["E_101_TrucBach", "E_102_HangBong", ...],
  "remaining_customers": [
    {"cust_id": "Cust_T001_1", "latitude": 21.033, "longitude": 105.849},
    {"cust_id": "Cust_T001_2", "latitude": 21.028, "longitude": 105.832},
    ... // 10 khách hàng
  ]
}
```

### Chạy tối ưu

Service `route-optimization` tự động:

1. Đọc 100 xe từ MongoDB
2. Với mỗi xe: GA tối ưu thứ tự 10 khách → Dijkstra tính đường
3. Ghi `new_assigned_route` (danh sách edge_id liên tục) + `estimated_total_travel_time`
4. Dashboard nhận realtime qua Change Stream

### Xem kết quả trên Dashboard

- Click xe trên map → hiển thị tuyến đường tối ưu (nét đậm, cùng màu xe)
- Panel trái: danh sách 100 xe, ETA, số cạnh đường
- Traffic layer: xanh/cam/đỏ theo mức độ tắc nghẽn

### Reset & chạy lại GA

```bash
python route_optimization/seed_assigned_routes.py --count 100 --reset
```

Xóa `new_assigned_route` → GA tính lại từ đầu với traffic mới nhất.
