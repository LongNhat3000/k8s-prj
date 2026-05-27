# 🚚 Logistics Real-Time — Tối ưu tuyến đường giao hàng thời gian thực

## Mô tả

Hệ thống mô phỏng và tối ưu tuyến đường giao hàng real-time cho **100 xe tải × 10 khách hàng** trên bản đồ Hà Nội. Xử lý ~5.000 GPS messages/giây, phát hiện tắc nghẽn, và tự động tái tối ưu tuyến đường bằng Genetic Algorithm + Dijkstra.

---

## Kiến trúc hệ thống

```
┌────────────────┐         ┌─────────┐         ┌──────────────────┐         ┌───────┐
│  Bot Simulation│────────▶│  KAFKA  │────────▶│ Stream Processing│────────▶│ REDIS │
│  (5k xe/GPS)  │  topic: │         │         │ (PySpark)        │  edge:* │       │
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
| Data Ingestion     | Python, confluent-kafka          | Mô phỏng 5k xe, gửi GPS → Kafka                 |
| Stream Processing  | PySpark Structured Streaming     | Map-matching GPS → edge, tính avg_speed → Redis |
| Route Optimization | Python, GA + Dijkstra            | Tối ưu thứ tự 10 khách/xe, tính shortest-path   |
| Backend            | Node.js, Socket.IO, KafkaJS      | Trung gian: Kafka + Redis + MongoDB → Frontend  |
| Frontend           | React, Leaflet Canvas            | Dashboard realtime                              |
| Infrastructure     | Kafka, Redis, MongoDB, Zookeeper | Message broker, cache, storage                  |

---

## Tech Stack

- **Message Broker**: Apache Kafka + Zookeeper
- **Stream Processing**: PySpark Structured Streaming
- **Cache**: Redis 7
- **Database**: MongoDB 6 (ReplicaSet cho Change Stream)
- **Backend**: Node.js + Socket.IO + KafkaJS
- **Frontend**: React + Leaflet Canvas
- **Optimization**: Genetic Algorithm + Dijkstra (Python)
- **Orchestration**: Kubernetes (Docker Desktop) / Docker Compose

---

## Cấu trúc thư mục

```
Logictics-real-time/
├── data_ingestion/          # Bot simulation — GPS producer
├── stream_processing/       # PySpark consumer — map-matching + Redis
├── route_optimization/      # GA + Dijkstra optimizer
├── dashboard/
│   ├── bridge-server/       # Node.js backend (Socket.IO + REST)
│   └── frontend/            # React + Leaflet
├── data/                    # edges_schema.json, nodes, ...
├── k8s/
│   ├── 01-infrastructure.yaml
│   └── 02-microservices.yaml
├── infrastructure/          # Docker Compose config
├── Dockerfile.data_ingestion
├── Dockerfile.stream_processing
├── Dockerfile.route_optimization
├── Dockerfile.backend
└── Dockerfile.frontend
```

---

## Thuật toán tối ưu tuyến đường

**Bài toán**: Cho xe tải tại vị trí hiện tại, cần giao hàng cho 10 khách → tìm thứ tự đi + đường đi ngắn nhất.

### Bước 1: Genetic Algorithm — tối ưu thứ tự khách hàng

- **Cá thể**: Hoán vị [K1, K2, ..., K10]
- **Fitness**: Tổng thời gian di chuyển (dựa trên avg_speed realtime từ Redis)
- **Operators**: Crossover (OX), Mutation (swap), Selection (tournament)
- **Population**: 30 cá thể × 60 thế hệ

### Bước 2: Dijkstra — shortest-path giữa mỗi cặp

```
Vị trí xe → K3 → K7 → K1 → ... → K10
     Dijkstra  Dijkstra  Dijkstra
```

**Kết quả**: Mảng edge_id liên tục — xe đi chính xác trên đường, không "chim bay".

### Bước 3: Re-optimize mỗi 30s

- Đọc traffic mới từ Redis (blocked_edges)
- Nếu route hiện tại đi qua đoạn tắc → chạy lại GA + Dijkstra
- Ghi `new_assigned_route` vào MongoDB → Backend nhận qua Change Stream → Push tới Frontend

---

## Triển khai

### Yêu cầu

- Docker Desktop (bật Kubernetes trong Settings → Kubernetes → Enable)
- kubectl (đi kèm Docker Desktop)

---

### Option 1: Kubernetes (Docker Desktop)

#### 1. Build images

```powershell
docker build -t logictics-bot:latest -f Dockerfile.data_ingestion .
docker build -t logictics-stream:v5 -f Dockerfile.stream_processing .
docker build -t logictics-opt:v6 -f Dockerfile.route_optimization .
docker build -t logictics-backend:v7 -f Dockerfile.backend .
docker build -t logictics-frontend:v4 -f Dockerfile.frontend .
```

#### 2. Deploy infrastructure

```powershell
kubectl apply -f k8s/01-infrastructure.yaml
kubectl get pods -w                          # Đợi tất cả Running
```

#### 3. Init MongoDB ReplicaSet

```powershell
kubectl exec -it deployment/mongodb -- mongosh --eval "rs.initiate({_id:'rs0', members:[{_id:0, host:'mongodb.default.svc.cluster.local:27017'}]})"
```

> Kết quả mong đợi: `{ ok: 1 }`

#### 4. Deploy microservices

```powershell
kubectl apply -f k8s/02-microservices.yaml
kubectl get pods -w                          # Đợi tất cả Running
```

#### 5. Seed dữ liệu

```powershell
kubectl exec -it deployment/route-optimization -- python seed_assigned_routes.py --count 100
```

#### 6. Truy cập Dashboard

```powershell
# Terminal 1
kubectl port-forward svc/dashboard-backend 4000:4000

# Terminal 2
kubectl port-forward svc/dashboard-frontend 5173:5173
```

### 7. Dừng k8s

kubectl delete -f k8s/02-microservices.yaml
kubectl delete -f k8s/01-infrastructure.yaml

### 8. Kiểm tra log

# Log bot simulation (xem có gửi edge_id không)

kubectl logs deployment/bot-simulator --tail=20

# Log stream-processing (xem Spark còn stuck không)

kubectl logs deployment/stream-processing --tail=50 | findstr /V "WARN"

# Log backend (xem kết nối Redis/Mongo/Kafka)

kubectl logs deployment/dashboard-backend --tail=30

# Log route-optimization (xem GA có chạy không)

kubectl logs deployment/route-optimization --tail=20

# Kiểm tra Redis có data traffic chưa

kubectl exec -it deployment/redis -- redis-cli KEYS "edge:\*" | Select-Object -First 10

→ Mở **http://localhost:5173**

#### Rebuild khi sửa code

```powershell
docker build -t logictics-opt:v6 -f Dockerfile.route_optimization .
kubectl rollout restart deployment/route-optimization
```

---

### Option 2: Docker Compose

```powershell
cd infrastructure
docker-compose up -d --build

# Seed data
docker compose exec route_optimization python route_optimization/seed_assigned_routes.py --count 100
```

→ Mở **http://localhost:5173**

---

## Xử lý sự cố

| Lỗi                          | Nguyên nhân                  | Fix                                           |
| ---------------------------- | ---------------------------- | --------------------------------------------- |
| `ImagePullBackOff`           | Image không tồn tại local    | Kiểm tra `docker images`, build lại nếu thiếu |
| `CrashLoopBackOff`           | Dependency chưa sẵn sàng     | Đảm bảo infrastructure pods Running trước     |
| `container not found`        | Pod chưa start               | `kubectl logs <pod>` → check lỗi              |
| MongoDB `rs.initiate` fail   | Pod chưa Ready               | Đợi Running rồi thử lại                       |
| `REDIS_PORT=tcp://...` crash | K8s inject biến service link | Đã fix bằng `enableServiceLinks: false`       |

---

## Kết quả

- Click xe trên map → hiển thị tuyến đường tối ưu (nét đậm, cùng màu xe)
- Panel trái: danh sách 100 xe, ETA, số cạnh đường
- Traffic layer: xanh/cam/đỏ theo mức độ tắc nghẽn
- Route tự động cập nhật khi phát hiện tắc đường mới
