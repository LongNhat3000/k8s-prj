import React, { useEffect, useMemo, useState, useCallback, useRef } from "react";
import { MapContainer, TileLayer, Polyline, Popup, Marker, useMap } from "react-leaflet";
import L from "leaflet";
import "leaflet/dist/leaflet.css";
import edgesData from "./data/edges_schema.json";
import { io } from "socket.io-client";

// Trong K8s: Vite dev server proxy /socket.io → backend service
// Fallback dùng VITE_API_URL hoặc rỗng (dùng proxy của Vite)
const SOCKET_URL = import.meta.env.VITE_API_URL || "";

const ROUTE_PALETTE = [
  "#e11d48",
  "#2563eb",
  "#16a34a",
  "#ca8a04",
  "#9333ea",
  "#0891b2",
  "#ea580c",
  "#4f46e5",
  "#db2777",
  "#0d9488",
  "#65a30d",
  "#7c3aed",
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
 * Khi 2 edge liên tiếp không nối nhau (end_node != start_node kế tiếp),
 * tạo segment mới → tránh vẽ đường "chim bay" xuyên qua thành phố.
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

    // Nếu segment hiện tại rỗng hoặc edge này không nối liền edge trước → segment mới
    const lastPt = currentSeg.length > 0 ? currentSeg[currentSeg.length - 1] : null;
    if (!lastPt || Math.abs(lastPt[0] - startPt[0]) > 0.0001 || Math.abs(lastPt[1] - startPt[1]) > 0.0001) {
      // Gián đoạn → lưu segment cũ và bắt đầu segment mới
      if (currentSeg.length >= 2) segments.push(currentSeg);
      currentSeg = [startPt, endPt];
    } else {
      // Liên tục → chỉ thêm end_node
      currentSeg.push(endPt);
    }
  });

  if (currentSeg.length >= 2) segments.push(currentSeg);
  return segments;
}

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
            <span style={{ color: routeColor }}>●</span> Có lộ trình GA (viền nhãn cùng màu đường)
          </>
        )}
      </Popup>
    </Marker>
  );
}

function App() {
  const [trafficData, setTrafficData] = useState({});
  const [vehicles, setVehicles] = useState({});
  const [routesByVehicle, setRoutesByVehicle] = useState({});
  const [selectedVehicleId, setSelectedVehicleId] = useState(null);
  const [routeFilter, setRouteFilter] = useState("");
  const socketRef = useRef(null);

  const edgeLookup = useMemo(() => {
    const map = {};
    edgesData.forEach((edge) => {
      map[edge.edge_id] = edge;
    });
    return map;
  }, []);

  const mergeRoutes = useCallback((list) => {
    setRoutesByVehicle((prev) => {
      const next = { ...prev };
      for (const item of list || []) {
        const vid = item.vehicle_id;
        if (!vid) continue;
        const path = item.path || [];
        if (path.length === 0) {
          delete next[vid];
          continue;
        }
        next[vid] = { path, time: item.time };
      }
      return next;
    });
  }, []);

  useEffect(() => {
    const socket = io(SOCKET_URL, { transports: ["polling", "websocket"] });
    socketRef.current = socket;

    // Legacy: từng event đơn lẻ (tương thích ngược)
    socket.on("traffic_update", (data) => {
      setTrafficData((prev) => ({ ...prev, [data.edge_id]: data.avg_speed }));
    });

    socket.on("vehicle_update", (data) => {
      setVehicles((prev) => ({ ...prev, [data.id]: data }));
    });

    // Batch events (tối ưu mới)
    socket.on("traffic_batch", (batch) => {
      setTrafficData((prev) => {
        const next = { ...prev };
        for (const item of batch) { next[item.edge_id] = item.avg_speed; }
        return next;
      });
    });

    socket.on("vehicle_batch", (batch) => {
      setVehicles((prev) => {
        const next = { ...prev };
        for (const v of batch) { next[v.id] = v; }
        return next;
      });
    });

    socket.on("routes_snapshot", (payload) => {
      mergeRoutes(payload);
    });

    socket.on("route_optimized", (data) => {
      mergeRoutes([data]);
    });

    // Nhận route tính on-demand từ backend PathFinder
    socket.on("route_result", (data) => {
      if (data && data.vehicle_id && data.path && data.path.length > 0) {
        mergeRoutes([{ vehicle_id: data.vehicle_id, path: data.path, time: data.time }]);
      }
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
        // Khi chọn xe mới → emit request_route để backend tính shortest path
        if (newSelected && socketRef.current) {
          const v = vehicles[newSelected];
          if (v && v.lat && v.lon) {
            socketRef.current.emit("request_route", { vehicle_id: newSelected, lat: v.lat, lon: v.lon });
          }
        }
        return newSelected;
      });
    },
    [vehicles]
  );

  return (
    <div style={{ height: "100vh", width: "100vw", position: "relative" }}>
      <aside
        className="dash-panel"
        style={{
          position: "absolute",
          top: 12,
          left: 12,
          zIndex: 1000,
          width: 280,
          maxHeight: "55vh",
          overflow: "auto",
          padding: "12px 14px",
          borderRadius: 10,
          background: "rgba(255,255,255,0.94)",
          boxShadow: "0 4px 20px rgba(0,0,0,0.12)",
          fontSize: 13,
        }}
      >
        <div style={{ fontWeight: 700, marginBottom: 8, color: "#111" }}>Xe tải & lộ trình</div>
        <div style={{ color: "#444", marginBottom: 8 }}>
          Đang hiển thị: <strong>{truckList.length}</strong> / 100 xe (Kafka)
        </div>
        <p style={{ fontSize: 11, color: "#666", margin: "0 0 10px", lineHeight: 1.4 }}>
          Nhãn trên map: <strong>001</strong> = Truck_001. Bấm xe hoặc mục trong danh sách để xem đường GA (nét đậm).
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
            width: "100%",
            boxSizing: "border-box",
            padding: "6px 8px",
            marginBottom: 8,
            borderRadius: 6,
            border: "1px solid #ccc",
            fontSize: 12,
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
                      width: "100%",
                      textAlign: "left",
                      padding: "8px 10px",
                      borderRadius: 8,
                      border: active ? `2px solid ${color}` : "1px solid #ddd",
                      background: active ? "rgba(124, 58, 237, 0.08)" : "#fff",
                      cursor: "pointer",
                      fontSize: 12,
                    }}
                  >
                    <span
                      style={{
                        display: "inline-block",
                        width: 10,
                        height: 10,
                        borderRadius: 2,
                        background: color,
                        marginRight: 8,
                        verticalAlign: "middle",
                      }}
                    />
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

      <div
        style={{
          position: "absolute",
          top: 12,
          right: 12,
          zIndex: 1000,
          background: "rgba(255,255,255,0.94)",
          padding: "10px 12px",
          borderRadius: 10,
          boxShadow: "0 4px 20px rgba(0,0,0,0.12)",
          fontSize: 12,
          maxWidth: 240,
        }}
      >
        <div style={{ fontWeight: 700, marginBottom: 6 }}>Màu tuyến (Redis)</div>
        <div><span style={{ color: "#3b82f6", fontWeight: 700 }}>■</span> Chưa có dữ liệu</div>
        <div><span style={{ color: "#22c55e", fontWeight: 700 }}>■</span> &gt; 15 km/h</div>
        <div><span style={{ color: "#f97316", fontWeight: 700 }}>■</span> 5–15 km/h</div>
        <div><span style={{ color: "#ef4444", fontWeight: 700 }}>■</span> ≤ 5 km/h</div>
        <div style={{ marginTop: 8, fontWeight: 700 }}>Lộ trình GA (đang chọn)</div>
        {selectedVehicleId && selectedColor ? (
          <div style={{ marginTop: 4 }}>
            <span style={{ color: selectedColor, fontWeight: 700 }}>━━</span> {selectedVehicleId}
            <br />
            ETA: {selectedRoute ? formatEtaMinutes(selectedRoute.time) : "—"}
            <br />
            <span style={{ color: "#666" }}>Các tuyến khác mờ khi đã chọn xe.</span>
          </div>
        ) : (
          <div style={{ color: "#666" }}>Chọn Truck_001… ở panel trái hoặc bấm xe trên map.</div>
        )}
      </div>

      <MapContainer center={[21.0262, 105.8375]} zoom={15} style={{ height: "100%", width: "100%" }} preferCanvas>
        <TileLayer url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png" />

        {selectedVehicle && <MapFlyTo lat={selectedVehicle.lat} lon={selectedVehicle.lon} />}

        {/* Traffic layer: chỉ hiển thị edges ĐÃ CÓ dữ liệu từ Redis */}
        {edgesData.map((edge) => {
          const speed = trafficData[edge.edge_id];
          if (speed === undefined) return null; // Ẩn edge chưa có dữ liệu traffic
          const roadColor = speed <= 5 ? "#ef4444" : speed <= 15 ? "#f97316" : "#22c55e";
          return (
            <Polyline
              key={edge.edge_id}
              positions={[
                [edge.start_node.lat, edge.start_node.lon],
                [edge.end_node.lat, edge.end_node.lon],
              ]}
              pathOptions={{ color: roadColor, weight: 4, opacity: 0.85 }}
            />
          );
        })}

        {/* Route layer: chỉ hiển thị route của xe ĐANG CHỌN */}
        {routeEntries.map(([vid, r]) => {
          // Ẩn route của các xe KHÔNG được chọn
          if (selectedVehicleId && vid !== selectedVehicleId) return null;
          // Nếu chưa chọn xe nào → không hiển thị route nào cả (tránh chằng chịt)
          if (!selectedVehicleId) return null;

          const segments = pathToSegments(r.path, edgeLookup);
          if (segments.length === 0) return null;
          const color = hashHue(vid);
          return segments.map((seg, idx) => (
            <React.Fragment key={`${vid}-seg-${idx}`}>
              <Polyline
                positions={seg}
                pathOptions={{
                  color,
                  weight: 7,
                  opacity: 1,
                }}
              >
                {idx === 0 && (
                  <Popup>
                    {vid}
                    <br />
                    ETA: {formatEtaMinutes(r.time)}
                  </Popup>
                )}
              </Polyline>
            </React.Fragment>
          ));
        })}

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
