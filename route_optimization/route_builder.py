"""
route_builder.py

Xây dựng lộ trình LIỀN MẠCH (danh sách edge_id liên tục) từ:
- start_edge: edge hiện tại của xe
- customer_order: thứ tự khách hàng (sau khi GA tối ưu)
- graph: GraphNetwork (có shortest_path Dijkstra)
- blocked_edges: danh sách edge bị chặn (tắc đường)

Đảm bảo:
1. edge[i].end_node == edge[i+1].start_node (liền mạch)
2. Nếu không tìm được đường → bỏ qua customer đó, tiếp tục
3. Tránh lặp edge (dedup liên tiếp)
"""

from typing import List, Dict, Optional


def build_route(
    graph,
    start_edge: str,
    customer_order: List[Dict],
    blocked_edges: Optional[List[str]] = None,
) -> List[str]:
    """
    Xây dựng route liền mạch từ start_edge qua tất cả customers theo thứ tự.
    
    Args:
        graph: GraphNetwork object (có shortest_path, edge_end_node, nearest_node_to_point)
        start_edge: edge_id hiện tại của xe
        customer_order: list các dict {"cust_id", "latitude", "longitude"} theo thứ tự tối ưu
        blocked_edges: list edge_id bị chặn
        
    Returns:
        List[str]: danh sách edge_id liên tục tạo thành lộ trình
    """
    blocked_edges = blocked_edges or []

    if start_edge not in graph.edges:
        return []

    route_edges: List[str] = []
    current_node = graph.edge_end_node(start_edge)

    if current_node is None:
        return []

    for customer in customer_order:
        lat = customer.get("latitude")
        lon = customer.get("longitude")
        
        if lat is None or lon is None:
            continue

        target_node = graph.nearest_node_to_point(lat, lon)

        if target_node is None:
            continue

        # Nếu đã ở đúng node đích → bỏ qua (không cần di chuyển)
        if current_node == target_node:
            continue

        # Tìm shortest path từ current_node → target_node
        partial_route, cost = graph.shortest_path(
            current_node,
            target_node,
            blocked_edges=blocked_edges
        )

        if not partial_route or cost == float("inf"):
            # Không tìm được đường → thử không blocked
            partial_route, cost = graph.shortest_path(
                current_node,
                target_node,
                blocked_edges=[]
            )
            if not partial_route:
                continue  # Thực sự không reachable → skip customer

        # Kiểm tra liên tục: edge cuối route hiện tại → edge đầu partial
        if route_edges:
            last_edge_end = graph.edge_end_node(route_edges[-1])
            first_edge_start = graph.edge_start_node(partial_route[0])
            
            if last_edge_end != first_edge_start:
                # Gap! Cần bridge
                bridge, _ = graph.shortest_path(
                    last_edge_end,
                    first_edge_start,
                    blocked_edges=blocked_edges
                )
                if bridge:
                    route_edges.extend(bridge)

        # Dedup: không thêm edge trùng liên tiếp
        for edge_id in partial_route:
            if not route_edges or route_edges[-1] != edge_id:
                route_edges.append(edge_id)

        # Cập nhật current_node = end_node của edge cuối
        current_node = target_node

    return route_edges


def validate_route_continuity(graph, route_edges: List[str]) -> Dict:
    """
    Kiểm tra route có liền mạch không.
    
    Returns:
        {
            "is_continuous": bool,
            "total_edges": int,
            "gaps": [(index, edge_i_end_node, edge_i+1_start_node)]
        }
    """
    if not route_edges:
        return {"is_continuous": True, "total_edges": 0, "gaps": []}

    gaps = []
    for i in range(len(route_edges) - 1):
        end_node = graph.edge_end_node(route_edges[i])
        start_node = graph.edge_start_node(route_edges[i + 1])
        
        if end_node is None or start_node is None:
            gaps.append((i, end_node, start_node))
        elif end_node != start_node:
            gaps.append((i, end_node, start_node))

    return {
        "is_continuous": len(gaps) == 0,
        "total_edges": len(route_edges),
        "gaps": gaps
    }


def estimate_route_travel_time(graph, route_edges: List[str], blocked_edges=None) -> float:
    """Ước tính thời gian đi hết route (giây)."""
    total = 0.0
    for edge_id in route_edges:
        cost = graph.edge_cost(edge_id, blocked_edges=blocked_edges)
        if cost != float("inf"):
            total += cost
    return total
