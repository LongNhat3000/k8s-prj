import React, { useEffect, useMemo, useState, useCallback, useRef } from "react";
import { MapContainer, TileLayer, Polyline, Popup, Marker, useMap } from "react-leaflet";
import L from "leaflet";
import "leaflet/dist/leaflet.css";
import edgesData from "./data/edges_schema.json";
import { io } from "socket.io-client";

// Trong K8s: Vite dev server proxy /socket.io → backend service
const SOCKET_URL = import.meta.env.VITE_API_URL || "";

const ROUTE_PALETTE = [
  "#e11d48", "#2563eb", "#16a34a", "#ca8a04", "#9333ea", "#0891b2",
  "#ea580c", "#4f46e5", "#db2777", "#0d9488", "#65a30d", "#7c3aed",
];

function hashHue(vehicleId) {
  let h = 0;
  for (let i = 0; i < vehicleId.length; i += 1) {
    h = (h * 31 + vehicleId.charCodeAt(i)) | 0;
  }
  return ROUTE_PALETTE[Math.abs(h) % ROUTE_PALETTE.length];
}

function compareTruckIds(a, b) {
  const na = parseInt(String(a).replace(/\D/g, ""), 10) || 0;
  const nb = parseInt(String(b).replace(/\D/g, ""), 10) || 0;
  if (na !== nb) return na - nb;
  return String(a).localeCompare(String(b));
}

function formatEtaMinutes(time) {
  if (time == null || Number.isNaN(Number(time))) return "—";
  const min = Number(time) / 60;
  if (min >= 10000) return `${(min / 60).toFixed(1)} giờ`;
  return `${min.toFixed(1)} phút`;
}

/**
 * Chuyển mảng edge_id → mảng các segments (mỗi segment là mảng [lat,lng]).
 * Phát hiện gián đoạn → tách segment mới → tránh "chim bay".
 */
function pathToSegments(path, edgeLookup) {
  if (!path || path.length === 0) return [];
  const segments = [];
  let currentSeg = [];

  path.forEach((id) => {
    const edge = edgeLookup[id];
    if (!edge) return;
    const startPt = [edge.start_node.lat, edge.start_node.lon];
    const endPt = [edge.end_node.lat, edge.end_node.lon];
    const lastPt = currentSeg.length > 0 ? currentSeg[currentSeg.length - 1] : null;
    if (!lastPt || Math.abs(lastPt[0] - startPt[0]) > 0.0001 || Math.abs(lastPt[1] - startPt[1]) > 0.0001) {
      if (currentSeg.length >= 2) segments.push(currentSeg);
      currentSeg = [startPt, endPt];
    } else {
      currentSeg.push(endPt);
    }
  });
  if (currentSeg.length >= 2) segments.push(currentSeg);
  return segments;
}

// =========================================================================
// CANVAS TRAFFIC LAYER — vẽ traffic trực tiếp lên Leaflet Canvas
// Không tạo DOM elements, không trigger React re-render
// =========================================================================

// Pre-build edge coords lookup (chỉ chạy 1 lần)
const EDGE_COORDS = {};
edgesData.forEach((edge) => {
  EDGE_COORDS[edge.edge_id] = {
    startLat: edge.start_node.lat,
    startLon: edge.start_node.lon,
    endLat: edge.end_node.lat,
    endLon: edge.end_node.lon,
  };
});

/**
 * CanvasTrafficLayer — Leaflet custom layer vẽ traffic bằng Canvas 2D.
 * Nhận trafficRef (useRef) chứa object { edge_id: avg_speed }.
 * Tự redraw mỗi 2s bằng setInterval (không phụ thuộc React render cycle).
 */
function CanvasTrafficLayer({ trafficRef }) {
  const map = useMap();
  const canvasLayerRef = useRef(null);
  const intervalRef = useRef(null);

  useEffect(() => {
    if (!map) return;

    // Tạo custom Canvas overlay
    const CanvasOverlay = L.Layer.extend({
      onAdd(leafletMap) {
        this._map = leafletMap;
        this._canvas = L.DomUtil.create("canvas", "traffic-canvas-layer");
        this._canvas.style.position = "absolute";
        this._canvas.style.pointerEvents = "none";
        this._canvas.style.zIndex = "200";
        const pane = leafletMap.getPane("overlayPane");
        pane.appendChild(this._canvas);
        leafletMap.on("moveend zoomend resize", this._reset, this);
        this._reset();
      },
      onRemove(leafletMap) {
        leafletMap.off("moveend zoomend resize", this._reset, this);
        L.DomUtil.remove(this._canvas);
      },
      _reset() {
        const size = this._map.getSize();
        const topLeft = this._map.containerPointToLayerPoint([0, 0]);
        L.DomUtil.setPosition(this._canvas, topLeft);
        this._canvas.width = size.x;
        this._canvas.height = size.y;
        this.draw();
      },
      draw() {
        const ctx = this._canvas.getContext("2d");
        const size = this._map.getSize();
        ctx.clearRect(0, 0, size.x, size.y);

        const traffic = trafficRef.current;
        if (!traffic) return;

        const bounds = this._map.getBounds();
        ctx.lineWidth = 3;
        ctx.lineCap = "round";
        ctx.globalAlpha = 0.85;

        for (const edgeId in traffic) {
          const coords = EDGE_COORDS[edgeId];
          if (!coords) continue;

          // Cull: bỏ edges ngoài viewport
          if (
            coords.startLat < bounds._southWest.lat - 0.01 &&
            coords.endLat < bounds._southWest.lat - 0.01
          ) continue;
          if (
            coords.startLat > bounds._northEast.lat + 0.01 &&
            coords.endLat > bounds._northEast.lat + 0.01
          ) continue;
          if (
            coords.startLon < bounds._southWest.lng - 0.01 &&
            coords.endLon < bounds._southWest.lng - 0.01
          ) continue;
          if (
            coords.startLon > bounds._northEast.lng + 0.01 &&
            coords.endLon > bounds._northEast.lng + 0.01
          ) continue;

          const speed = traffic[edgeId];
          if (speed <= 5) ctx.strokeStyle = "#ef4444";
          else if (speed <= 15) ctx.strokeStyle = "#f97316";
          else ctx.strokeStyle = "#22c55e";

          const p1 = this._map.latLngToContainerPoint([coords.startLat, coords.startLon]);
          const p2 = this._map.latLngToContainerPoint([coords.endLat, coords.endLon]);

          ctx.beginPath();
          ctx.moveTo(p1.x, p1.y);
          ctx.lineTo(p2.x, p2.y);
          ctx.stroke();
        }
        ctx.globalAlpha = 1;
      },
    });

    const layer = new CanvasOverlay();
    layer.addTo(map);
    canvasLayerRef.current = layer;

    // Redraw mỗi 2s (khi traffic data cập nhật qua useRef)
    intervalRef.current = setInterval(() => {
      if (canvasLayerRef.current) {
        canvasLayerRef.current.draw();
      }
    }, 2000);

    return () => {
      clearInterval(intervalRef.current);
      if (canvasLayerRef.current && map.hasLayer(canvasLayerRef.current)) {
        map.removeLayer(canvasLayerRef.current);
      }
    };
  }, [map, trafficRef]);

  return null; // Không render React DOM nào
}

// =========================================================================

function MapFlyTo({ lat, lon, zoom = 16 }) {
  const map = useMap();
  useEffect(() => {
    if (lat == null || lon == null) return;
    map.flyTo([lat, lon], zoom, { duration: 0.6 });
  }, [map, lat, lon, zoom]);
  return null;
}

function TruckMarker({ vehicle, routeColor, hasRoute, isSelected, onSelect }) {
  const rawSpd = Number(vehicle.speed) || 0;
  const spd = rawSpd > 0 ? Math.max(3, Math.round(rawSpd)) : 0;
  const shortId = String(vehicle.id).replace(/^Truck_/i, "");
  const borderColor = routeColor || "#94a3b8";

  const icon = useMemo(
    () =>
      L.divIcon({
        className: `truck-marker-root${isSelected ? " truck-marker-selected" : ""}`,
        html:
          `<div class="truck-marker-label${hasRoute ? " has-route" : ""}" style="border-left-color:${borderColor}">${shortId}</div>` +
          `<div class="truck-marker-icon" aria-hidden="true">🚚</div>` +
          `<div class="truck-marker-speed">${spd} km/h</div>`,
        iconSize: [80, 58],
        iconAnchor: [40, 52],
      }),
    [shortId, spd, borderColor, hasRoute, isSelected]
  );

  return (
    <Marker
      position={[vehicle.lat, vehicle.lon]}
      icon={icon}
      eventHandlers={{ click: () => onSelect(vehicle.id) }}
    >
      <Popup>
        <strong>{vehicle.id}</strong>
        <br />
        Tốc độ: {spd} km/h
        {hasRoute && (
          <>
            <br />
            <span style={{ color: routeColor }}>●</span> Có lộ trình (viền nhãn cùng màu đường)
          </>
        )}
      </Popup>
    </Marker>
  );
}

function App() {
  // === Traffic data dùng useRef — KHÔNG trigger re-render ===
  const trafficRef = useRef({});
  // Chỉ dùng useState cho data cần re-render UI (vehicles, routes, selection)
  const [vehicles, setVehicles] = useState({});
  const [routesByVehicle, setRoutesByVehicle] = useState({});
  const [selectedVehicleId, setSelectedVehicleId] = useState(null);
  const [routeFilter, setRouteFilter] = useState("");
  const socketRef = useRef(null);

  const edgeLookup = useMemo(() => {
    const map = {};
    edgesData.forEach((edge) => { map[edge.edge_id] = edge; });
    return map;
  }, []);

  const mergeRoutes = useCallback((list) => {
    setRoutesByVehicle((prev) => {
      const next = { ...prev };
      for (const item of list || []) {
        const vid = item.vehicle_id;
        if (!vid) continue;
        const path = item.path || [];
        if (path.length === 0) { delete next[vid]; continue; }
        next[vid] = {
          path,
          time: item.time,
          customers: item.customers || [],
          current_edge_index: item.current_edge_index || 0,
          total_edges: item.total_edges || path.length,
          rerouted: item.rerouted || false,
          reroute_reason: item.reroute_reason || "",
        };
      }
      return next;
    });
  }, []);

  useEffect(() => {
    const socket = io(SOCKET_URL, { transports: ["polling", "websocket"] });
    socketRef.current = socket;

    // Traffic → useRef (KHÔNG setState → KHÔNG re-render)
    socket.on("traffic_update", (data) => {
      trafficRef.current[data.edge_id] = data.avg_speed;
    });
    socket.on("traffic_batch", (batch) => {
      for (const item of batch) {
        trafficRef.current[item.edge_id] = item.avg_speed;
      }
    });

    // Vehicles → useState (cần re-render markers)
    socket.on("vehicle_update", (data) => {
      setVehicles((prev) => ({ ...prev, [data.id]: data }));
    });
    socket.on("vehicle_batch", (batch) => {
      setVehicles((prev) => {
        const next = { ...prev };
        for (const v of batch) { next[v.id] = v; }
        return next;
      });
    });

    // Routes
    socket.on("routes_snapshot", (payload) => { mergeRoutes(payload); });
    socket.on("route_optimized", (data) => { mergeRoutes([data]); });
    socket.on("route_result", (data) => {
      if (!data || !data.vehicle_id || !data.path || data.path.length === 0) return;
      // route_result (on-demand PathFinder) — CHỈ dùng nếu xe hoàn toàn chưa có route
      setRoutesByVehicle((prev) => {
        // Nếu đã có route (từ snapshot/GA) → KHÔNG ghi đè, giữ nguyên 100%
        if (prev[data.vehicle_id]?.path?.length > 0) return prev;
        const next = { ...prev };
        next[data.vehicle_id] = { path: data.path, time: data.time, customers: data.customers || [], current_edge_index: data.current_edge_index || 0, total_edges: data.total_edges || data.path.length, rerouted: false, reroute_reason: "" };
        return next;
      });
    });

    return () => {
      socket.disconnect();
      socketRef.current = null;
    };
  }, [mergeRoutes]);

  const routeEntries = useMemo(() => {
    const entries = Object.entries(routesByVehicle).filter(([, r]) => r.path?.length);
    entries.sort(([a], [b]) => compareTruckIds(a, b));
    return entries;
  }, [routesByVehicle]);

  const filteredRouteEntries = useMemo(() => {
    const q = routeFilter.trim().toLowerCase();
    if (!q) return routeEntries;
    return routeEntries.filter(([vid]) => vid.toLowerCase().includes(q));
  }, [routeEntries, routeFilter]);

  const truckList = useMemo(
    () => Object.values(vehicles).sort((a, b) => compareTruckIds(a.id, b.id)),
    [vehicles]
  );

  const selectedRoute = selectedVehicleId ? routesByVehicle[selectedVehicleId] : null;
  const selectedVehicle = selectedVehicleId ? vehicles[selectedVehicleId] : null;
  const selectedColor = selectedVehicleId ? hashHue(selectedVehicleId) : null;

  const selectVehicle = useCallback(
    (vid) => {
      setSelectedVehicleId((prev) => {
        const newSelected = prev === vid ? null : vid;
        // Chỉ request route on-demand nếu xe CHƯA CÓ route từ MongoDB
        if (newSelected && socketRef.current && !routesByVehicle[newSelected]?.path?.length) {
          const v = vehicles[newSelected];
          if (v && v.lat && v.lon) {
            socketRef.current.emit("request_route", { vehicle_id: newSelected, lat: v.lat, lon: v.lon });
          }
        }
        return newSelected;
      });
    },
    [vehicles, routesByVehicle]
  );

  return (
    <div style={{ height: "100vh", width: "100vw", position: "relative" }}>
      {/* === SIDEBAR PANEL === */}
      <aside
        className="dash-panel"
        style={{
          position: "absolute", top: 12, left: 12, zIndex: 1000,
          width: 280, maxHeight: "55vh", overflow: "auto",
          padding: "12px 14px", borderRadius: 10,
          background: "rgba(255,255,255,0.94)",
          boxShadow: "0 4px 20px rgba(0,0,0,0.12)", fontSize: 13,
        }}
      >
        <div style={{ fontWeight: 700, marginBottom: 8, color: "#111" }}>Xe tải & lộ trình</div>
        <div style={{ color: "#444", marginBottom: 8 }}>
          Đang hiển thị: <strong>{truckList.length}</strong> / 100 xe (Kafka)
        </div>
        <p style={{ fontSize: 11, color: "#666", margin: "0 0 10px", lineHeight: 1.4 }}>
          Nhãn trên map: <strong>001</strong> = Truck_001. Bấm xe hoặc mục trong danh sách để xem đường (nét đậm).
        </p>

        <div style={{ fontWeight: 600, marginBottom: 6, color: "#333" }}>
          Tối ưu theo xe ({routeEntries.length})
        </div>
        <input
          type="search"
          placeholder="Tìm Truck_001..."
          value={routeFilter}
          onChange={(e) => setRouteFilter(e.target.value)}
          style={{
            width: "100%", boxSizing: "border-box", padding: "6px 8px",
            marginBottom: 8, borderRadius: 6, border: "1px solid #ccc", fontSize: 12,
          }}
        />

        {filteredRouteEntries.length === 0 ? (
          <p style={{ margin: 0, color: "#666" }}>
            {routeEntries.length === 0
              ? "Chưa có lộ trình. Chạy seed + đợi GA ghi MongoDB."
              : "Không khớp tìm kiếm."}
          </p>
        ) : (
          <ul style={{ listStyle: "none", margin: 0, padding: 0 }}>
            {filteredRouteEntries.map(([vid, r]) => {
              const color = hashHue(vid);
              const active = vid === selectedVehicleId;
              return (
                <li key={vid} style={{ marginBottom: 6 }}>
                  <button
                    type="button"
                    onClick={() => selectVehicle(vid)}
                    style={{
                      width: "100%", textAlign: "left", padding: "8px 10px",
                      borderRadius: 8,
                      border: active ? `2px solid ${color}` : "1px solid #ddd",
                      background: active ? "rgba(124, 58, 237, 0.08)" : "#fff",
                      cursor: "pointer", fontSize: 12,
                    }}
                  >
                    <span style={{
                      display: "inline-block", width: 10, height: 10,
                      borderRadius: 2, background: color, marginRight: 8, verticalAlign: "middle",
                    }} />
                    <strong>{vid}</strong>
                    <div style={{ color: "#555", marginTop: 2, display: "block" }}>
                      ETA: {formatEtaMinutes(r.time)} · {r.path.length} cạnh
                    </div>
                  </button>
                </li>
              );
            })}
          </ul>
        )}
      </aside>

      {/* === LEGEND PANEL === */}
      <div style={{
        position: "absolute", top: 12, right: 12, zIndex: 1000,
        background: "rgba(255,255,255,0.94)", padding: "10px 12px",
        borderRadius: 10, boxShadow: "0 4px 20px rgba(0,0,0,0.12)",
        fontSize: 12, maxWidth: 240,
      }}>
        <div style={{ fontWeight: 700, marginBottom: 6 }}>Màu tuyến (Redis)</div>
        <div><span style={{ color: "#22c55e", fontWeight: 700 }}>■</span> &gt; 15 km/h (thông)</div>
        <div><span style={{ color: "#f97316", fontWeight: 700 }}>■</span> 5–15 km/h (chậm)</div>
        <div><span style={{ color: "#ef4444", fontWeight: 700 }}>■</span> ≤ 5 km/h (tắc)</div>
        <div style={{ marginTop: 8, fontWeight: 700 }}>Lộ trình (đang chọn)</div>
        {selectedVehicleId && selectedColor ? (
          <div style={{ marginTop: 4 }}>
            <span style={{ color: selectedColor, fontWeight: 700 }}>━━</span> {selectedVehicleId}
            <br />
            ETA: {selectedRoute ? formatEtaMinutes(selectedRoute.time) : "—"}
          </div>
        ) : (
          <div style={{ color: "#666" }}>Chọn xe ở panel trái hoặc bấm xe trên map.</div>
        )}
      </div>

      {/* === MAP === */}
      <MapContainer center={[21.0262, 105.8375]} zoom={15} style={{ height: "100%", width: "100%" }} preferCanvas>
        <TileLayer url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png" />

        {selectedVehicle && <MapFlyTo lat={selectedVehicle.lat} lon={selectedVehicle.lon} />}

        {/* Traffic layer: Canvas trực tiếp — KHÔNG qua React DOM */}
        <CanvasTrafficLayer trafficRef={trafficRef} />

        {/* Route layer: 2 màu — đã đi (mờ) + chưa đi (đậm) */}
        {selectedVehicleId && routesByVehicle[selectedVehicleId] && (() => {
          const r = routesByVehicle[selectedVehicleId];
          const color = hashHue(selectedVehicleId);
          const edgeIndex = r.current_edge_index || 0;
          const passedPath = r.path.slice(0, edgeIndex);
          const remainingPath = r.path.slice(edgeIndex);
          const passedSegments = pathToSegments(passedPath, edgeLookup);
          const remainingSegments = pathToSegments(remainingPath, edgeLookup);

          return (
            <>
              {/* Đoạn đã đi — mờ */}
              {passedSegments.map((seg, idx) => (
                <Polyline
                  key={`passed-${idx}`}
                  positions={seg}
                  pathOptions={{ color: "#9ca3af", weight: 4, opacity: 0.4, dashArray: "8 6" }}
                />
              ))}
              {/* Đoạn chưa đi — đậm */}
              {remainingSegments.map((seg, idx) => (
                <Polyline
                  key={`remain-${idx}`}
                  positions={seg}
                  pathOptions={{ color, weight: 7, opacity: 1 }}
                >
                  {idx === 0 && (
                    <Popup>
                      {selectedVehicleId}<br />
                      ETA: {formatEtaMinutes(r.time)}
                      {r.rerouted && r.reroute_reason && (
                        <><br /><span style={{ color: "#ef4444" }}>⚠️ {r.reroute_reason}</span></>
                      )}
                    </Popup>
                  )}
                </Polyline>
              ))}
            </>
          );
        })()}

        {/* Customer markers: chỉ hiện khi chọn xe */}
        {selectedVehicleId && routesByVehicle[selectedVehicleId]?.customers?.length > 0 && (() => {
          const customers = routesByVehicle[selectedVehicleId].customers;
          return customers.map((cust) => {
            const status = cust.status || "pending";
            let emoji = "🔴";
            let size = 28;
            if (status === "next") { emoji = "📍"; size = 36; }
            else if (status === "delivered") { emoji = "✅"; size = 24; }

            const icon = L.divIcon({
              className: `customer-marker customer-${status}`,
              html: `<div style="font-size:${size}px;text-align:center;line-height:1">${emoji}</div>`
                + `<div style="font-size:10px;text-align:center;color:#333;font-weight:600;margin-top:2px">${cust.order || ""}</div>`,
              iconSize: [40, 48],
              iconAnchor: [20, 44],
            });

            return (
              <Marker key={cust.cust_id} position={[cust.latitude, cust.longitude]} icon={icon}>
                <Popup>
                  <strong>{cust.cust_id}</strong><br />
                  Thứ tự: {cust.order}<br />
                  Trạng thái: {status === "delivered" ? "Đã giao" : status === "next" ? "Đang tới" : "Chờ giao"}
                </Popup>
              </Marker>
            );
          });
        })()}

        {/* Truck markers */}
        {truckList.map((v) => (
          <TruckMarker
            key={v.id}
            vehicle={v}
            routeColor={routesByVehicle[v.id] ? hashHue(v.id) : null}
            hasRoute={Boolean(routesByVehicle[v.id]?.path?.length)}
            isSelected={v.id === selectedVehicleId}
            onSelect={selectVehicle}
          />
        ))}
      </MapContainer>
    </div>
  );
}

export default App;
