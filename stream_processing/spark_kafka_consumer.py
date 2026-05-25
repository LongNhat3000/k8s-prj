"""
Stream Processing Module - Logistics Real-Time
PySpark Structured Streaming: Kafka → Aggregate → Redis

OPTIMIZED: Bot gửi sẵn edge_id → KHÔNG cần UDF map-matching O(9026) nữa.
Chỉ cần aggregate (speed, count) theo edge + window → ghi Redis.
"""

import os
import json
import glob
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, udf, window, avg, count,
    current_timestamp, lit
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, LongType, TimestampType
)

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)
logger = logging.getLogger("StreamProcessor")

# ─── Cấu hình ─────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BROKER", os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))
KAFKA_TOPIC             = os.getenv("KAFKA_TOPIC", "gps_stream")
REDIS_HOST              = os.getenv("REDIS_HOST", "redis")
REDIS_PORT              = int(os.getenv("REDIS_PORT_NUM", os.getenv("REDIS_PORT", "6379")))
EDGES_JSON              = os.getenv("EDGES_JSON", "/app/data/edges_schema.json")

# Tần suất cập nhật Redis (giây)
WATERMARK_DELAY   = "10 seconds"   # chấp nhận late data tới 10s
WINDOW_DURATION   = "10 seconds"   # cửa sổ tổng hợp 10s
SLIDE_DURATION    = "5 seconds"    # trượt mỗi 5s

# Ngưỡng phát hiện tắc đường (km/h)
CONGESTION_THRESHOLD_KMH = 10.0   # Thực tế HN: ≤10 km/h = tắc nghẽn

# ─── Schema GPS message từ Kafka ──────────────────────────────────────────────
# Bot gửi sẵn edge_id → không cần map-matching UDF nữa!
GPS_SCHEMA = StructType([
    StructField("entity_id",   StringType(),  True),
    StructField("entity_type", StringType(),  True),   # "Truck" | "Bot"
    StructField("latitude",    DoubleType(),  True),
    StructField("longitude",   DoubleType(),  True),
    StructField("speed",       DoubleType(),  True),   # km/h
    StructField("timestamp",   LongType(),    True),   # epoch ms
    StructField("edge_id",     StringType(),  True),   # ĐÃ CÓ SẴN từ bot
])

# ─── Pre-load edge lengths ────────────────────────────────────────────────────
# Load 1 lần vào dict (driver-side) → broadcast sang executors
_edge_lengths = {}

def _load_edge_lengths():
    global _edge_lengths
    try:
        with open(EDGES_JSON, "r", encoding="utf-8") as f:
            edges = json.load(f)
        _edge_lengths = {e["edge_id"]: float(e.get("length_meters", 500.0)) for e in edges}
        logger.info(f"✅ Loaded {len(_edge_lengths)} edge lengths.")
    except Exception as e:
        logger.warning(f"⚠️ Không load được edges: {e}. Dùng fallback 500m.")

_load_edge_lengths()


def _get_edge_length(edge_id: str) -> float:
    """Lấy chiều dài edge (metres). Fallback 500m."""
    return _edge_lengths.get(edge_id, 500.0)

get_edge_length_udf = udf(_get_edge_length, DoubleType())


# ─── Hàm ghi batch vào Redis ──────────────────────────────────────────────────
def write_edge_stats_to_redis(batch_df, batch_id: int):
    from redis_manager import RedisWriter

    if batch_df.isEmpty():
        return

    row_count = batch_df.count()
    logger.info(f"[Batch {batch_id}] Xử lý {row_count} records...")

    def write_partition(partition):
        writer = None
        try:
            writer = RedisWriter(host=REDIS_HOST, port=REDIS_PORT)
            records_to_write = []

            for row in partition:
                edge_id   = row["edge_id"]
                avg_speed = row["avg_speed"]
                distance  = row["edge_length_m"]

                if not edge_id or edge_id == "UNKNOWN" or avg_speed is None or avg_speed <= 0:
                    continue

                estimated_travel_time = (distance / 1000.0) / avg_speed * 3600.0

                payload = {
                    "edge_id": edge_id,
                    "avg_speed": round(avg_speed, 2),
                    "vehicle_count": int(row["vehicle_count"]),
                    "distance": round(distance, 1),
                    "estimated_travel_time": round(estimated_travel_time, 2),
                    "is_congested": avg_speed < CONGESTION_THRESHOLD_KMH,
                    "updated_at": row["window_end"].isoformat() if row["window_end"] else ""
                }

                records_to_write.append(payload)

            if records_to_write:
                writer.pipeline_set_many(records_to_write, ttl=120)
                logger.info(f"[Batch {batch_id}] Ghi {len(records_to_write)} edges → Redis.")

        except Exception as e:
            logger.error(f"[Batch {batch_id}] Lỗi partition: {e}", exc_info=True)
        finally:
            if writer:
                writer.close()

    batch_df.foreachPartition(write_partition)
    logger.info(f"[Batch {batch_id}] ✅ Done.")


# ─── Main ─────────────────────────────────────────────────────────────────────
def main():
    logger.info("Khởi động PySpark Structured Streaming (OPTIMIZED - no map-matching UDF)...")

    spark = (SparkSession.builder
        .appName("LogisticsProcessing")
        .config("spark.jars", ",".join(glob.glob("/opt/spark-jars/*.jar")))
        .config("spark.executor.memory", "1g")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    
    # Force override — đảm bảo không bị default 200 partitions
    spark.conf.set("spark.sql.shuffle.partitions", "4")
    
    logger.info(f"✅ shuffle.partitions = {spark.conf.get('spark.sql.shuffle.partitions')}")
    
    spark.sparkContext.addPyFile("/app/stream_processing/redis_manager.py")
    spark.sparkContext.setLogLevel("WARN")

    # ── 1. Đọc từ Kafka ───────────────────────────────────────────────────────
    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")      # CHỈ đọc data MỚI (tránh replay cũ)
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", 20000)    # Tăng throughput (bot gửi ~10K msg/s)
        .load()
    )

    # ── 2. Parse JSON ─────────────────────────────────────────────────────────
    parsed_df = (
        raw_df
        .selectExpr("CAST(value AS STRING) AS json_str")
        .select(from_json(col("json_str"), GPS_SCHEMA).alias("data"))
        .select("data.*")
        .filter(col("entity_type").isin("Truck", "Bot"))
        .filter(col("speed").isNotNull() & (col("speed") > 0))
        .filter(col("edge_id").isNotNull() & (col("edge_id") != ""))
    )

    # Chuyển epoch ms → Timestamp để dùng watermark
    with_ts_df = parsed_df.withColumn(
        "event_time",
        (col("timestamp") / 1000).cast(TimestampType())
    )

    # ── 3. KHÔNG CẦN MAP-MATCHING! edge_id đã có sẵn từ bot ──────────────────
    # (Trước đây chỗ này gọi UDF O(9026) per record → 45M ops/batch → STUCK)

    # ── 4. Watermark + Window + Aggregate ─────────────────────────────────────
    aggregated_df = (
        with_ts_df
        .withWatermark("event_time", WATERMARK_DELAY)
        .groupBy(
            window(col("event_time"), WINDOW_DURATION, SLIDE_DURATION),
            col("edge_id")
        )
        .agg(
            avg("speed").alias("avg_speed"),
            count("entity_id").alias("vehicle_count"),
        )
        .select(
            col("edge_id"),
            col("avg_speed"),
            col("vehicle_count"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
        )
    )

    # ── 5. Bổ sung edge_length từ pre-loaded dict ────────────────────────────
    enriched_df = aggregated_df.withColumn(
        "edge_length_m", get_edge_length_udf(col("edge_id"))
    )

    # ── 6. Ghi vào Redis qua foreachBatch ─────────────────────────────────────
    query = (
        enriched_df.writeStream
        .outputMode("update")
        .foreachBatch(write_edge_stats_to_redis)
        .option("checkpointLocation", "/tmp/spark_checkpoint/gps_stream_v2")
        .trigger(processingTime="5 seconds")
        .start()
    )

    logger.info("✅ Stream đang chạy. Nhấn Ctrl+C để dừng.")
    query.awaitTermination()

if __name__ == "__main__":
    main()
