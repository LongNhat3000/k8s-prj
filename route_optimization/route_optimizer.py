"""
route_optimizer.py

Integration layer cho module Route Optimization.

Backend dashboard hien dang nghe MongoDB collection `assigned_routes`
va emit event `route_optimized` voi format:
    {
        path: updatedData.new_assigned_route,
        time: updatedData.estimated_total_travel_time
    }

Vi vay file nay ghi cac field:
- vehicle_id
- new_assigned_route
- estimated_total_travel_time

Ghi chu:
- GA hien tai moi toi uu thu tu khach hang (`optimized_customer_order`).
- GA chua sinh edge route moi that su tu graph.
- Tam thoi `new_assigned_route` dung `old_assigned_route` de dashboard co path edge_id hop le.
"""

from typing import Any, Dict, Optional
import time
import traceback

from optimizer_input_adapter import (
    build_optimization_input,
    validate_optimization_input,
    should_trigger_reroute,
)

from genetic_algorithm import run_genetic_algorithm


DEFAULT_MIN_AVG_SPEED = 10.0   # Đồng bộ với CONGESTION_THRESHOLD_KMH = 10
DEFAULT_POPULATION_SIZE = 30
DEFAULT_GENERATIONS = 60
DEFAULT_MUTATION_RATE = 0.15
REROUTE_COOLDOWN_SEC = 60      # Tối thiểu 60s giữa 2 lần re-route cùng xe

_last_reroute_time = {}        # vehicle_id → timestamp (epoch seconds)


def now_ms() -> int:
    return int(time.time() * 1000)


def build_no_reroute_response(vehicle_id: str, reason: str) -> Dict[str, Any]:
    return {
        "status": "skipped",
        "vehicle_id": vehicle_id,
        "reason": reason,
        "optimized": False,
        "updated_mongo": False,
        "timestamp": now_ms(),
    }


def build_error_response(vehicle_id: Optional[str], error: Exception) -> Dict[str, Any]:
    return {
        "status": "error",
        "vehicle_id": vehicle_id,
        "reason": str(error),
        "traceback": traceback.format_exc(),
        "optimized": False,
        "updated_mongo": False,
        "timestamp": now_ms(),
    }


def build_success_response(
    vehicle_id: str,
    old_assigned_route: list,
    new_assigned_route: list,
    ga_result: Dict[str, Any],
    mongo_updated: bool,
) -> Dict[str, Any]:
    return {
        "status": "optimized",
        "vehicle_id": vehicle_id,
        "old_assigned_route": old_assigned_route,
        "new_assigned_route": new_assigned_route,
        "estimated_total_travel_time": ga_result.get("estimated_total_cost", 0),
        "optimized_customer_order": ga_result.get("optimized_customer_order", []),
        "optimized_customers": ga_result.get("optimized_customers", []),
        "generation_count": ga_result.get("generation_count"),
        "optimized": True,
        "updated_mongo": mongo_updated,
        "timestamp": now_ms(),
    }


def build_new_assigned_route(
    old_assigned_route: list,
    ga_result: Dict[str, Any],
    opt_input: Dict[str, Any],
    graph=None,
) -> list:
    """
    Tạo route edge_id mới LIỀN MẠCH cho dashboard.

    Sử dụng GraphNetwork.shortest_path() để nối liên tục
    từ edge hiện tại qua tất cả customers theo thứ tự GA tối ưu.
    """
    # Nếu có graph → tính route liền mạch thực sự
    if graph is not None:
        from route_builder import build_route

        start_edge = opt_input.get("current_edge_id", "")
        optimized_customers = ga_result.get("optimized_customers", [])

        # Nếu GA không trả customers dạng dict, dùng remaining_customers gốc
        if not optimized_customers:
            optimized_customers = opt_input.get("remaining_customers", [])

        # Di chuyển các sink nodes (dead-ends) về cuối danh sách
        from route_builder import reorder_sinks_last
        optimized_customers = reorder_sinks_last(graph, optimized_customers)
        ga_result["optimized_customers"] = optimized_customers
        ga_result["optimized_customer_order"] = [c.get("cust_id") for c in optimized_customers]

        blocked_edges = opt_input.get("blocked_edges", [])

        new_route = build_route(
            graph=graph,
            start_edge=start_edge,
            customer_order=optimized_customers,
            blocked_edges=blocked_edges,
        )

        if new_route:
            return new_route

    # Fallback: trả về old_assigned_route nếu graph không có hoặc route rỗng
    if old_assigned_route:
        return old_assigned_route

    return []


def save_optimization_result_to_mongo(
    mongo_collection: Any,
    vehicle_id: str,
    ga_result: Dict[str, Any],
    old_assigned_route: list,
    new_assigned_route: list,
    customers: list = None,
    reroute_reason: str = "",
    old_customers: list = None,
) -> bool:
    """
    Cap nhat ket qua optimization vao MongoDB theo schema dashboard.

    Collection ky vong:
        assigned_routes

    Fields quan trong cho backend/frontend:
        vehicle_id
        new_assigned_route
        estimated_total_travel_time
    """
    estimated_total_travel_time = ga_result.get("estimated_total_cost", 0)

    # Xây dựng customers với status cho Frontend, giữ nguyên những khách đã giao
    customers_with_status = []
    delivered_customers = []
    if old_customers:
        delivered_customers = [c for c in old_customers if c.get("status") == "delivered"]
    
    customers_with_status.extend(delivered_customers)

    optimized_customers = ga_result.get("optimized_customers", [])
    if optimized_customers:
        for idx, cust in enumerate(optimized_customers):
            cust_id = cust.get("cust_id")
            if any(dc.get("cust_id") == cust_id for dc in delivered_customers):
                continue
            customers_with_status.append({
                "cust_id": cust_id,
                "latitude": cust.get("latitude", 0),
                "longitude": cust.get("longitude", 0),
                "order": len(customers_with_status) + 1,
                "status": "pending",  # pending | next | delivered
            })
        # Đánh dấu customer đầu tiên chưa giao là "next"
        for cust in customers_with_status:
            if cust.get("status") == "pending":
                cust["status"] = "next"
                break
    elif customers:
        for idx, cust in enumerate(customers):
            cust_id = cust.get("cust_id")
            if any(dc.get("cust_id") == cust_id for dc in delivered_customers):
                continue
            customers_with_status.append({
                "cust_id": cust_id,
                "latitude": cust.get("latitude", 0),
                "longitude": cust.get("longitude", 0),
                "order": len(customers_with_status) + 1,
                "status": "pending",
            })
        for cust in customers_with_status:
            if cust.get("status") == "pending":
                cust["status"] = "next"
                break

    update_doc = {
        "$set": {
            "vehicle_id": vehicle_id,
            "new_assigned_route": new_assigned_route,
            "estimated_total_travel_time": estimated_total_travel_time,
            "optimized_customer_order": ga_result.get("optimized_customer_order", []),
            "customers": customers_with_status,
            "current_edge_index": 0,
            "total_edges": len(new_assigned_route),
            "rerouted": True,
            "reroute_reason": reroute_reason,
            "optimization_result": {
                "old_assigned_route": old_assigned_route,
                "new_assigned_route": new_assigned_route,
                "optimized_customer_order": ga_result.get("optimized_customer_order", []),
                "estimated_total_cost": ga_result.get("estimated_total_cost"),
                "generation_count": ga_result.get("generation_count"),
                "optimized_at": now_ms(),
                "algorithm": "Genetic Algorithm",
            },
            "last_optimized_at": now_ms(),
            "route_status": "optimized",
            "needs_optimization": False,
        }
    }

    result = mongo_collection.update_one(
        {"vehicle_id": vehicle_id},
        update_doc,
        upsert=True,
    )

    return (
        getattr(result, "modified_count", 0) > 0
        or getattr(result, "matched_count", 0) > 0
        or getattr(result, "upserted_id", None) is not None
    )


def optimize_vehicle(
    vehicle_doc: Dict[str, Any],
    redis_client: Any,
    graph=None,
    mongo_collection: Optional[Any] = None,
    min_avg_speed: float = DEFAULT_MIN_AVG_SPEED,
    population_size: int = DEFAULT_POPULATION_SIZE,
    generations: int = DEFAULT_GENERATIONS,
    mutation_rate: float = DEFAULT_MUTATION_RATE,
    force: bool = False,
    random_seed: Optional[int] = None,
) -> Dict[str, Any]:
    vehicle_id = str(vehicle_doc.get("vehicle_id", ""))

    try:
        opt_input = build_optimization_input(
            vehicle_doc=vehicle_doc,
            redis_client=redis_client,
        )

        validate_optimization_input(opt_input)

        old_assigned_route = opt_input.get("assigned_route", [])

        # Cooldown: không re-route liên tục cho cùng 1 xe
        if not force:
            last_time = _last_reroute_time.get(vehicle_id, 0)
            if time.time() - last_time < REROUTE_COOLDOWN_SEC:
                return build_no_reroute_response(
                    vehicle_id=vehicle_id,
                    reason=f"Cooldown: chờ {REROUTE_COOLDOWN_SEC}s giữa 2 lần re-route",
                )

        reroute_needed = should_trigger_reroute(
            normalized_input=opt_input,
            min_avg_speed=min_avg_speed,
        )

        if not force and not reroute_needed:
            return build_no_reroute_response(
                vehicle_id=vehicle_id,
                reason="No blocked or congested edge detected",
            )

        ga_result = run_genetic_algorithm(
            opt_input=opt_input,
            population_size=population_size,
            generations=generations,
            mutation_rate=mutation_rate,
            random_seed=random_seed,
        )

        new_assigned_route = build_new_assigned_route(
            old_assigned_route=old_assigned_route,
            ga_result=ga_result,
            opt_input=opt_input,
            graph=graph,
        )

        # Xác định lý do re-route
        blocked_edges = opt_input.get("blocked_edges", [])
        reroute_reason = ""
        if blocked_edges:
            reroute_reason = f"Tắc nghẽn tại: {', '.join(blocked_edges[:3])}"
        elif force:
            reroute_reason = "Khởi tạo lần đầu"

        mongo_updated = False
        if mongo_collection is not None:
            mongo_updated = save_optimization_result_to_mongo(
                mongo_collection=mongo_collection,
                vehicle_id=vehicle_id,
                ga_result=ga_result,
                old_assigned_route=old_assigned_route,
                new_assigned_route=new_assigned_route,
                customers=opt_input.get("remaining_customers", []),
                reroute_reason=reroute_reason,
                old_customers=vehicle_doc.get("customers", []),
            )

        # Ghi lại thời điểm re-route
        _last_reroute_time[vehicle_id] = time.time()

        return build_success_response(
            vehicle_id=vehicle_id,
            old_assigned_route=old_assigned_route,
            new_assigned_route=new_assigned_route,
            ga_result=ga_result,
            mongo_updated=mongo_updated,
        )

    except Exception as error:
        return build_error_response(vehicle_id=vehicle_id, error=error)


def optimize_many_vehicles(
    vehicle_docs: list,
    redis_client: Any,
    graph=None,
    mongo_collection: Optional[Any] = None,
    min_avg_speed: float = DEFAULT_MIN_AVG_SPEED,
    population_size: int = DEFAULT_POPULATION_SIZE,
    generations: int = DEFAULT_GENERATIONS,
    mutation_rate: float = DEFAULT_MUTATION_RATE,
    force: bool = False,
) -> Dict[str, Any]:
    results = []

    for vehicle_doc in vehicle_docs:
        new_route = vehicle_doc.get("new_assigned_route")
        needs_bootstrap = not (isinstance(new_route, list) and len(new_route) > 0)
        needs_opt_flag = vehicle_doc.get("needs_optimization", False)
        effective_force = bool(force or needs_bootstrap or needs_opt_flag)

        result = optimize_vehicle(
            vehicle_doc=vehicle_doc,
            redis_client=redis_client,
            graph=graph,
            mongo_collection=mongo_collection,
            min_avg_speed=min_avg_speed,
            population_size=population_size,
            generations=generations,
            mutation_rate=mutation_rate,
            force=effective_force,
        )
        results.append(result)

    optimized_count = sum(1 for item in results if item.get("status") == "optimized")
    skipped_count = sum(1 for item in results if item.get("status") == "skipped")
    error_count = sum(1 for item in results if item.get("status") == "error")

    return {
        "total": len(results),
        "optimized_count": optimized_count,
        "skipped_count": skipped_count,
        "error_count": error_count,
        "results": results,
        "timestamp": now_ms(),
    }


def find_vehicle_by_id(mongo_collection: Any, vehicle_id: str) -> Optional[Dict[str, Any]]:
    return mongo_collection.find_one({"vehicle_id": vehicle_id})


def optimize_vehicle_by_id(
    vehicle_id: str,
    redis_client: Any,
    mongo_collection: Any,
    **kwargs: Any,
) -> Dict[str, Any]:
    vehicle_doc = find_vehicle_by_id(mongo_collection, vehicle_id)

    if not vehicle_doc:
        return {
            "status": "error",
            "vehicle_id": vehicle_id,
            "reason": "Vehicle not found in MongoDB",
            "optimized": False,
            "updated_mongo": False,
            "timestamp": now_ms(),
        }

    return optimize_vehicle(
        vehicle_doc=vehicle_doc,
        redis_client=redis_client,
        mongo_collection=mongo_collection,
        **kwargs,
    )


class FakeMongoUpdateResult:
    def __init__(
        self,
        matched_count: int = 1,
        modified_count: int = 1,
        upserted_id: Optional[str] = None,
    ):
        self.matched_count = matched_count
        self.modified_count = modified_count
        self.upserted_id = upserted_id


class FakeMongoCollection:
    def __init__(self):
        self.docs = {}

    def insert_one_doc(self, doc: Dict[str, Any]) -> None:
        self.docs[doc["vehicle_id"]] = doc

    def find_one(self, query: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        vehicle_id = query.get("vehicle_id")
        return self.docs.get(vehicle_id)

    def update_one(
        self,
        query: Dict[str, Any],
        update_doc: Dict[str, Any],
        upsert: bool = False,
    ) -> FakeMongoUpdateResult:
        vehicle_id = query.get("vehicle_id")

        if vehicle_id not in self.docs:
            if not upsert:
                return FakeMongoUpdateResult(matched_count=0, modified_count=0)

            self.docs[vehicle_id] = {"vehicle_id": vehicle_id}
            set_doc = update_doc.get("$set", {})
            self.docs[vehicle_id].update(set_doc)
            return FakeMongoUpdateResult(
                matched_count=0,
                modified_count=0,
                upserted_id=vehicle_id,
            )

        set_doc = update_doc.get("$set", {})
        self.docs[vehicle_id].update(set_doc)

        return FakeMongoUpdateResult(matched_count=1, modified_count=1)


if __name__ == "__main__":
    import os
    import time
    from pymongo import MongoClient
    import redis
    from graph_network import GraphNetwork
    from traffic_adapter import TrafficAdapter

    print("🚀 Khởi động Route Optimization Worker...")

    # 1. Kết nối thật vào các hệ thống
    mongo_uri = os.getenv("MONGO_URI", "mongodb://mongodb.default.svc.cluster.local:27017/?directConnection=true")
    redis_host = os.getenv("REDIS_HOST", "redis.default.svc.cluster.local")
    redis_port = int(os.getenv("REDIS_PORT_NUM", 6379))

    print(f"🔗 Đang kết nối MongoDB: {mongo_uri}")
    mongo_client = MongoClient(mongo_uri)
    # LƯU Ý: Đổi 'traffic_system' thành tên Database thật của bạn nếu khác
    db = mongo_client["traffic_system"] 
    real_mongo_collection = db["assigned_routes"]

    print(f"🔗 Đang kết nối Redis: {redis_host}:{redis_port}")
    real_redis_client = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)

    # 3. Load graph network (edges_schema.json) cho pathfinding
    edges_json = os.getenv("EDGES_JSON", "/app/data/edges_schema.json")
    print(f"🗺️  Đang load bản đồ: {edges_json}")
    graph = GraphNetwork()
    graph.load_from_schema(edges_json)
    print(f"✅ Graph loaded: {len(graph.nodes)} nodes, {len(graph.edges)} edges")

    # 4. Gắn traffic adapter (đọc real-time traffic từ Redis) vào graph
    traffic_adapter = TrafficAdapter(host=redis_host, port=redis_port)
    graph.set_traffic_adapter(traffic_adapter)

    print("✅ Đã kết nối thành công! Bắt đầu giám sát giao thông & tối ưu lộ trình 24/7...")

    # 2. Vòng lặp vĩnh cửu của Kubernetes
    while True:
        try:
            # Lấy danh sách toàn bộ xe tải đang hoạt động từ MongoDB
            # (Giả sử bạn chỉ muốn tối ưu cho Truck, hoặc lấy toàn bộ nếu collection chỉ chứa xe tải)
            trucks_cursor = real_mongo_collection.find({}) 
            vehicle_docs = list(trucks_cursor)

            if vehicle_docs:
                # Gọi hàm tối ưu xịn sò của bạn
                result = optimize_many_vehicles(
                    vehicle_docs=vehicle_docs,
                    redis_client=real_redis_client,
                    graph=graph,
                    mongo_collection=real_mongo_collection,
                    force=False # Chỉ chạy lại thuật toán GA khi đường bị kẹt (theo logic của bạn)
                )
                
                for res in result.get("results", []):
                    status = res.get("status")
                    vid = res.get("vehicle_id")
                    if status == "error":
                        print(f"❌ Lỗi tối ưu xe {vid}: {res.get('reason')}")
                        if "traceback" in res:
                            print(res["traceback"])
                    elif status == "optimized":
                        print(f"✅ Đã tối ưu thành công cho xe {vid}!")
            
        except Exception as e:
            print(f"❌ Lỗi vòng lặp: {e}")

        # Nghỉ ngơi 5 giây trước khi quét lại để không làm cháy CPU
        time.sleep(5)
