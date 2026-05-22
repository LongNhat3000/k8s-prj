/**
 * PathFinder — Dijkstra shortest-path trên graph edges_schema.json
 * 
 * Dùng bởi server.js khi frontend emit "request_route":
 *   1. Snap GPS xe → nearest node
 *   2. Greedy nearest-customer ordering
 *   3. Dijkstra giữa mỗi pair → nối thành route liên tục
 *   4. Trả về mảng edge_id liên tiếp (KHÔNG gián đoạn)
 */

"use strict";

class MinHeap {
  constructor() { this._data = []; }
  get size() { return this._data.length; }
  
  push(item) {
    this._data.push(item);
    this._bubbleUp(this._data.length - 1);
  }
  
  pop() {
    if (this._data.length === 0) return null;
    const top = this._data[0];
    const last = this._data.pop();
    if (this._data.length > 0) {
      this._data[0] = last;
      this._sinkDown(0);
    }
    return top;
  }
  
  _bubbleUp(i) {
    while (i > 0) {
      const parent = (i - 1) >> 1;
      if (this._data[i].cost >= this._data[parent].cost) break;
      [this._data[i], this._data[parent]] = [this._data[parent], this._data[i]];
      i = parent;
    }
  }
  
  _sinkDown(i) {
    const n = this._data.length;
    while (true) {
      let smallest = i;
      const l = 2 * i + 1, r = 2 * i + 2;
      if (l < n && this._data[l].cost < this._data[smallest].cost) smallest = l;
      if (r < n && this._data[r].cost < this._data[smallest].cost) smallest = r;
      if (smallest === i) break;
      [this._data[i], this._data[smallest]] = [this._data[smallest], this._data[i]];
      i = smallest;
    }
  }
}

class PathFinder {
  constructor() {
    this.loaded = false;
    this._edges = [];          // raw edge objects
    this._adj = {};            // node_id → [{edge, toNode, cost}]
    this._nodeCoords = {};     // node_id → {lat, lon}
    this._edgeById = {};       // edge_id → edge object
  }

  /**
   * Load edges_schema.json và build adjacency list
   */
  load(filePath) {
    const fs = require("fs");
    const raw = fs.readFileSync(filePath, "utf-8");
    this._edges = JSON.parse(raw);
    
    this._adj = {};
    this._nodeCoords = {};
    this._edgeById = {};

    for (const edge of this._edges) {
      const from = edge.start_node.node_id;
      const to = edge.end_node.node_id;
      const lengthM = edge.length_meters || 500;
      const speedKmh = Math.max(edge.max_speed_kmh || 40, 1);
      // Cost = travel time (seconds)
      const cost = (lengthM / 1000) / speedKmh * 3600;

      if (!this._adj[from]) this._adj[from] = [];
      this._adj[from].push({ edge, toNode: to, cost });

      this._nodeCoords[from] = { lat: edge.start_node.lat, lon: edge.start_node.lon };
      this._nodeCoords[to] = { lat: edge.end_node.lat, lon: edge.end_node.lon };
      this._edgeById[edge.edge_id] = edge;
    }

    this.loaded = true;
    console.log(`✅ PathFinder loaded: ${this._edges.length} edges, ${Object.keys(this._adj).length} nodes`);
  }

  /**
   * Tìm node_id gần nhất với tọa độ GPS (lat, lon)
   */
  nearestNode(lat, lon) {
    let bestNode = null;
    let bestDist = Infinity;
    for (const [nodeId, coords] of Object.entries(this._nodeCoords)) {
      const d = (coords.lat - lat) ** 2 + (coords.lon - lon) ** 2;
      if (d < bestDist) {
        bestDist = d;
        bestNode = nodeId;
      }
    }
    return bestNode;
  }

  /**
   * Dijkstra: tìm đường ngắn nhất từ startNode → endNode
   * Trả về: { path: [edge_id, ...], cost: seconds }
   */
  dijkstra(startNode, endNode) {
    if (startNode === endNode) return { path: [], cost: 0 };
    if (!this._adj[startNode]) return { path: [], cost: Infinity };

    const dist = { [startNode]: 0 };
    const prev = {};  // node → { parentNode, edge }
    const visited = new Set();
    const heap = new MinHeap();
    heap.push({ node: startNode, cost: 0 });

    while (heap.size > 0) {
      const { node, cost } = heap.pop();
      if (visited.has(node)) continue;
      visited.add(node);

      if (node === endNode) break;

      for (const neighbor of (this._adj[node] || [])) {
        if (visited.has(neighbor.toNode)) continue;
        const newCost = cost + neighbor.cost;
        if (newCost < (dist[neighbor.toNode] ?? Infinity)) {
          dist[neighbor.toNode] = newCost;
          prev[neighbor.toNode] = { parentNode: node, edge: neighbor.edge };
          heap.push({ node: neighbor.toNode, cost: newCost });
        }
      }
    }

    // Traceback
    if (!prev[endNode] && startNode !== endNode) {
      return { path: [], cost: Infinity };
    }

    const pathEdges = [];
    let cur = endNode;
    while (cur !== startNode) {
      if (!prev[cur]) return { path: [], cost: Infinity };
      pathEdges.push(prev[cur].edge.edge_id);
      cur = prev[cur].parentNode;
    }
    pathEdges.reverse();
    return { path: pathEdges, cost: dist[endNode] || 0 };
  }

  /**
   * Tính route liên tục từ vị trí xe → qua tất cả customers
   * Thuật toán: Greedy Nearest-Customer + Dijkstra mỗi đoạn
   * 
   * @param {number} vehicleLat - GPS lat xe hiện tại
   * @param {number} vehicleLon - GPS lon xe hiện tại
   * @param {Array} customers - [{cust_id, latitude, longitude}, ...]
   * @returns {{ path: [edge_id,...], totalCost: number }}
   */
  buildRoute(vehicleLat, vehicleLon, customers) {
    if (!this.loaded || !customers || customers.length === 0) {
      return { path: [], totalCost: 0 };
    }

    // 1. Snap xe → nearest node
    let currentNode = this.nearestNode(vehicleLat, vehicleLon);
    if (!currentNode) return { path: [], totalCost: 0 };

    // 2. Greedy nearest-customer ordering
    const unvisited = customers.map((c, i) => ({
      ...c,
      _idx: i,
      _node: this.nearestNode(c.latitude, c.longitude),
    })).filter(c => c._node); // bỏ customer không snap được

    const fullPath = [];
    let totalCost = 0;

    while (unvisited.length > 0) {
      // Tìm customer gần nhất (Euclidean, nhanh hơn Dijkstra toàn bộ)
      let bestIdx = 0;
      let bestDist = Infinity;
      const curCoords = this._nodeCoords[currentNode];

      for (let i = 0; i < unvisited.length; i++) {
        const cNode = unvisited[i]._node;
        const cCoords = this._nodeCoords[cNode];
        if (!cCoords) continue;
        const d = (curCoords.lat - cCoords.lat) ** 2 + (curCoords.lon - cCoords.lon) ** 2;
        if (d < bestDist) {
          bestDist = d;
          bestIdx = i;
        }
      }

      const target = unvisited.splice(bestIdx, 1)[0];
      
      // 3. Dijkstra currentNode → target._node
      const segment = this.dijkstra(currentNode, target._node);
      
      if (segment.path.length > 0) {
        fullPath.push(...segment.path);
        totalCost += segment.cost;
        currentNode = target._node;
      } else {
        // Không tìm được đường → skip customer này
        console.warn(`⚠️ PathFinder: Không tìm được đường tới customer ${target.cust_id}`);
      }
    }

    return { path: fullPath, totalCost };
  }
}

module.exports = PathFinder;
