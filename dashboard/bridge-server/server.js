const express = require("express");
const http = require("http");
const { Server } = require("socket.io");
const cors = require("cors");
const mongoose = require("mongoose");
const redis = require("redis");
const { Kafka } = require("kafkajs");
const PathFinder = require("./pathfinder");

const app = express();
app.use(cors());

const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: "*", methods: ["GET", "POST"] },
});

// --- CẤU HÌNH KẾT NỐI (K8s Service names) ---
// --- PATHFINDER (tính route shortest-path on-demand) ---
const pathfinder = new PathFinder();
const EDGES_PATH = process.env.EDGES_JSON || "/app/data/edges_schema.json";
try {
  pathfinder.load(EDGES_PATH);
} catch (err) {
  console.warn("⚠️ PathFinder: Không load được edges:", err.message);
}

const MONGO_URI = process.env.MONGO_URI || "mongodb://mongodb:27017/traffic_system?replicaSet=rs0";
const REDIS_URL = process.env.REDIS_HOST ? `redis://${process.env.REDIS_HOST}:6379` : "redis://redis.default.svc.cluster.local:6379";
const KAFKA_BROKER = process.env.KAFKA_BROKER || "kafka:9092";

// --- Trạng thái kết nối ---
let redisReady = false;
let mongoReady = false;

// 1. Kết nối Redis - RETRY khi service chưa sẵn sàng
const redisClient = redis.createClient({
  url: REDIS_URL,
  socket: {
    reconnectStrategy: (retries) => {
      console.log(`⏳ Redis reconnect attempt ${retries}...`);
      return Math.min(retries * 1000, 10000); // max 10s giữa các lần retry
    }
  }
});
redisClient.on("error", (err) => {
  if (redisReady) console.error("⚠️ Redis error:", err.message);
  redisReady = false;
});
redisClient.on("ready", () => {
  redisReady = true;
  console.log("✅ Đã kết nối Redis");
});
redisClient.connect().catch((err) => {
  console.warn("⚠️ Redis chưa sẵn sàng:", err.message, "- Sẽ tự retry...");
});

// 2. Kết nối MongoDB - RETRY khi service chưa sẵn sàng
function connectMongo() {
  mongoose.connect(MONGO_URI, {
    serverSelectionTimeoutMS: 10000,
  })
    .then(() => {
      mongoReady = true;
      console.log("✅ Đã kết nối MongoDB");
      setupChangeStreams();
    })
    .catch((err) => {
      console.warn("⚠️ MongoDB chưa sẵn sàng:", err.message, "- Retry sau 5s...");
      setTimeout(connectMongo, 5000);
    });
}
connectMongo();

// 3. Kết nối Kafka - Retry vô hạn
const kafka = new Kafka({
  clientId: "dashboard-backend",
  brokers: [KAFKA_BROKER],
  retry: { initialRetryTime: 3000, retries: 10 }
});

async function runKafkaForever() {
  while (true) {
    const consumer = kafka.consumer({ groupId: "dashboard-group" });
    try {
      console.log(`Đang kết nối Kafka (${KAFKA_BROKER})...`);
      await consumer.connect();
      await consumer.subscribe({ topic: "gps_stream", fromBeginning: false });
      console.log("✅ Kafka: đã subscribe topic gps_stream");

      // Buffer để batch emit vehicle_update mỗi 1s
      let vehicleBuffer = {};
      let edgeUpdateBuffer = {};  // vehicle_id → edge_id (batch update MongoDB mỗi 5s)
      setInterval(() => {
        const batch = Object.values(vehicleBuffer);
        if (batch.length > 0) {
          io.emit("vehicle_batch", batch);
          vehicleBuffer = {};
        }
      }, 1000);

      // Batch update current_edge_id vào MongoDB mỗi 5s
      setInterval(async () => {
        if (!mongoReady) return;
        const updates = Object.entries(edgeUpdateBuffer);
        if (updates.length === 0) return;
        edgeUpdateBuffer = {};
        
        const bulkOps = updates.map(([vehicle_id, edge_id]) => ({
          updateOne: {
            filter: { vehicle_id },
            update: {
              $set: { current_edge_id: edge_id, edge_id: edge_id },
              $setOnInsert: {
                new_assigned_route: [],
                assigned_route: [],
                customers: [],
                remaining_customers: [],
                current_edge_index: 0,
                total_edges: 0,
                estimated_total_travel_time: 0,
                needs_optimization: false
              }
            },
            upsert: true
          }
        }));
        
        try {
          await RouteModel.bulkWrite(bulkOps, { ordered: false });
        } catch (e) { /* ignore - best effort */ }
      }, 5000);

      await consumer.run({
        eachMessage: async ({ message }) => {
          try {
            const data = JSON.parse(message.value.toString());
            if (data.entity_type === "Truck") {
              // Buffer thay vì emit từng cái
              vehicleBuffer[data.entity_id] = {
                id: data.entity_id,
                lat: data.latitude,
                lon: data.longitude,
                speed: data.speed
              };
              // Track edge_id cho batch update MongoDB
              if (data.edge_id) {
                edgeUpdateBuffer[data.entity_id] = data.edge_id;
              }
            }
          } catch (e) { console.error("Lỗi parse Kafka:", e.message); }
        },
      });
      // Nếu consumer chạy thành công, dừng vòng lặp retry
      break;
    } catch (e) {
      console.warn("⚠️ Kafka consumer:", e.message || e, "- Retry sau 10s...");
      try { await consumer.disconnect(); } catch (_) { /* ignore */ }
      await new Promise((r) => setTimeout(r, 10000));
    }
  }
}
runKafkaForever();

// --- MONGODB CHANGE STREAMS (Lộ trình GA) ---
const RouteModel = mongoose.model("Route", new mongoose.Schema({
  vehicle_id: String,
  new_assigned_route: [String],
  assigned_route: [String],
  remaining_customers: [{
    cust_id: String, latitude: Number, longitude: Number
  }],
  customers: [{
    cust_id: String, latitude: Number, longitude: Number,
    order: Number, status: String
  }],
  current_edge_index: Number,
  total_edges: Number,
  rerouted: Boolean,
  reroute_reason: String,
  estimated_total_travel_time: Number,
  needs_optimization: Boolean,
  current_edge_id: String,
  edge_id: String,
  optimized_customer_order: [String],
  last_optimized_at: Number,
  route_status: String,
}), "assigned_routes");

function setupChangeStreams() {
  try {
    RouteModel.watch([], { fullDocument: "updateLookup" }).on("change", (change) => {
      const updatedData = change.fullDocument;
      if (updatedData) {
        io.emit("route_optimized", {
          vehicle_id: updatedData.vehicle_id,
          path: updatedData.new_assigned_route,
          time: updatedData.estimated_total_travel_time,
          customers: updatedData.customers || [],
          current_edge_index: updatedData.current_edge_index || 0,
          total_edges: updatedData.total_edges || 0,
          rerouted: updatedData.rerouted || false,
          reroute_reason: updatedData.reroute_reason || "",
        });
      }
    });
    console.log("✅ MongoDB Change Streams đã sẵn sàng");
  } catch (e) {
    console.error("⚠️ Change Streams error:", e.message);
  }
}

async function emitRoutesSnapshot(socket) {
  if (!mongoReady) return;
  try {
    const docs = await RouteModel.find({
      vehicle_id: { $exists: true, $nin: [null, ""] },
    })
      .select("vehicle_id new_assigned_route assigned_route estimated_total_travel_time customers current_edge_index total_edges")
      .lean();
    const payload = docs
      .filter((d) => {
        if (!d.vehicle_id) return false;
        const route = d.new_assigned_route || d.assigned_route || [];
        return Array.isArray(route) && route.length > 0;
      })
      .map((d) => ({
        vehicle_id: d.vehicle_id,
        path: d.new_assigned_route && d.new_assigned_route.length > 0 ? d.new_assigned_route : d.assigned_route || [],
        time: d.estimated_total_travel_time,
        customers: d.customers || [],
        current_edge_index: d.current_edge_index || 0,
        total_edges: d.total_edges || 0,
      }));
    socket.emit("routes_snapshot", payload);
    console.log(`📦 routes_snapshot: ${payload.length} routes, ${payload.filter(p => p.customers.length > 0).length} with customers`);
  } catch (e) {
    console.error("routes_snapshot:", e.message);
  }
}

// --- REAL-TIME TRAFFIC (Redis Polling) ---
io.on("connection", (socket) => {
  console.log("📡 Dashboard connected: " + socket.id);

  emitRoutesSnapshot(socket);

  // --- CREATE ORDER DIRECTLY FROM DASHBOARD ---
  socket.on("create_order", async (data) => {
    try {
      const { vehicle_id, lat, lon } = data || {};
      if (!vehicle_id || lat == null || lon == null) {
        console.warn("⚠️ create_order: Thiếu thông tin", data);
        return;
      }
      if (!mongoReady) {
        console.warn("⚠️ create_order: MongoDB chưa sẵn sàng");
        return;
      }

      let doc = await RouteModel.findOne({ vehicle_id });
      let customers = [];
      let remaining_customers = [];
      if (doc) {
        customers = doc.customers || [];
        remaining_customers = doc.remaining_customers || [];
      }

      const orderNum = customers.length + 1;
      const newCustId = `Cust_${vehicle_id}_${Date.now()}`;
      const newCust = {
        cust_id: newCustId,
        latitude: parseFloat(lat),
        longitude: parseFloat(lon),
        order: orderNum,
        status: customers.length === 0 ? "next" : "pending"
      };

      customers.push(newCust);
      remaining_customers.push(newCust);

      await RouteModel.updateOne(
        { vehicle_id },
        {
          $set: {
            customers,
            remaining_customers,
            needs_optimization: true
          }
        },
        { upsert: true }
      );
      console.log(`➕ Đã tạo đơn hàng mới cho ${vehicle_id}: ${newCustId} (${lat}, ${lon})`);
    } catch (err) {
      console.error("❌ Lỗi create_order:", err.message);
    }
  });

  // --- CLEAR ORDERS ---
  socket.on("clear_orders", async (data) => {
    try {
      const { vehicle_id } = data || {};
      if (!vehicle_id) return;
      if (!mongoReady) return;

      await RouteModel.updateOne(
        { vehicle_id },
        {
          $set: {
            customers: [],
            remaining_customers: [],
            new_assigned_route: [],
            assigned_route: [],
            estimated_total_travel_time: 0,
            current_edge_index: 0,
            total_edges: 0,
            rerouted: false,
            reroute_reason: "Reset đơn hàng",
            needs_optimization: false
          }
        }
      );
      console.log(`🧹 Đã xóa tất cả đơn hàng cho ${vehicle_id}`);
    } catch (err) {
      console.error("❌ Lỗi clear_orders:", err.message);
    }
  });

  // --- REQUEST ROUTE ON-DEMAND ---
  // Frontend gửi event khi user chọn xe → tính shortest path realtime
  socket.on("request_route", async (data) => {
    try {
      const { vehicle_id, lat, lon } = data || {};
      if (!vehicle_id || lat == null || lon == null) {
        socket.emit("route_result", { vehicle_id, error: "Missing lat/lon" });
        return;
      }

      if (!pathfinder.loaded) {
        socket.emit("route_result", { vehicle_id, path: [], time: 0, error: "PathFinder not loaded" });
        return;
      }

      // Lấy customers từ MongoDB (remaining_customers trong assigned_routes)
      let customers = [];
      let customersForFrontend = [];
      if (mongoReady) {
        const doc = await RouteModel.findOne({ vehicle_id }).lean();
        if (doc) {
          customers = doc.remaining_customers || doc.customers || [];
          customersForFrontend = doc.customers || doc.remaining_customers || [];
        }
      }

      if (customers.length === 0) {
        // Nếu MongoDB chưa có customers → tạo random 5 điểm giao hàng gần xe
        // (demo mode - khi chưa seed MongoDB)
        const nearNode = pathfinder.nearestNode(lat, lon);
        if (nearNode) {
          // Lấy 5 node ngẫu nhiên trong graph làm điểm giao hàng demo
          const allNodes = Object.entries(pathfinder._nodeCoords);
          for (let i = 0; i < Math.min(5, allNodes.length); i++) {
            const randIdx = Math.floor(Math.random() * allNodes.length);
            const [, coords] = allNodes[randIdx];
            customers.push({ cust_id: `Demo_${i+1}`, latitude: coords.lat, longitude: coords.lon });
          }
        }
      }

      if (customers.length === 0) {
        socket.emit("route_result", { vehicle_id, path: [], time: 0, error: "No customers" });
        return;
      }

      // Tính shortest path liên tục từ vị trí xe → qua tất cả customer
      const result = pathfinder.buildRoute(lat, lon, customers);

      socket.emit("route_result", {
        vehicle_id,
        path: result.path,
        time: Math.round(result.totalCost),
        customers: customersForFrontend,
        current_edge_index: 0,
        total_edges: result.path.length,
      });
      console.log(`🗺️ Route calculated for ${vehicle_id}: ${result.path.length} edges, ${Math.round(result.totalCost)}s`);
    } catch (err) {
      console.error("request_route error:", err.message);
      socket.emit("route_result", { vehicle_id: data?.vehicle_id, path: [], error: err.message });
    }
  });

  const trafficInterval = setInterval(async () => {
    if (!redisReady) return; // Bỏ qua nếu Redis chưa sẵn sàng
    try {
      // Dùng KEYS + GET từng key (ổn định, tránh lỗi Buffer với scanIterator/mGet)
      const keys = await redisClient.keys("edge:*");
      if (keys.length > 0) {
        const trafficBatch = [];
        for (const key of keys) {
          try {
            const rawData = await redisClient.get(String(key));
            if (rawData) {
              const payload = JSON.parse(rawData);
              trafficBatch.push({ edge_id: String(key).replace("edge:", ""), avg_speed: payload.avg_speed || 0 });
            }
          } catch (_) { /* skip */ }
        }
        if (trafficBatch.length > 0) {
          socket.emit("traffic_batch", trafficBatch);
        }
      }
    } catch (err) { console.error("Lỗi lấy Redis:", err.message); }
  }, 2000);

  socket.on("disconnect", () => {
    console.log("❌ Dashboard disconnected: " + socket.id);
    clearInterval(trafficInterval);
  });
});

// --- Xử lý lỗi không crash process ---
process.on("unhandledRejection", (err) => {
  console.error("⚠️ Unhandled rejection:", err.message);
});
process.on("uncaughtException", (err) => {
  console.error("⚠️ Uncaught exception:", err.message);
});

// --- START SERVER (luôn khởi động bất kể service khác) ---
const PORT = process.env.PORT || 30000;
server.listen(PORT, () => console.log(`🚀 SERVER RUNNING ON PORT ${PORT}`));
