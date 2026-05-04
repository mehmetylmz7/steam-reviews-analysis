"""
Adım 3: Spark Structured Streaming ile Veri Okuma
==================================================
Kafka'dan gelen steam-reviews mesajlarını Spark Structured Streaming ile okur,
3 katmanlı Delta Lake mimarisine (Bronze → Silver → Gold) yazar.

Katmanlar:
  Bronze : Ham JSON mesajları (sadece tip dönüşümü)
  Silver : Temizlenmiş & normalize edilmiş kayıtlar (nulllar, duplikeler giderildi)
  Gold   : Analiz için hazır, zenginleştirilmiş kayıtlar (is_positive, review_length vb.)
"""

import os
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, TimestampType
)

# ── Ortam değişkenleri ──────────────────────────────────────────────────────
KAFKA_BROKER  = os.getenv("KAFKA_BROKER",  "localhost:9092")
TOPIC_NAME    = os.getenv("TOPIC_NAME",    "steam-reviews")
DELTA_PATH    = os.getenv("DELTA_PATH",    "/delta")
CHECKPOINT    = f"{DELTA_PATH}/checkpoints"

BRONZE_PATH   = f"{DELTA_PATH}/bronze/steam_reviews"
SILVER_PATH   = f"{DELTA_PATH}/silver/steam_reviews"
GOLD_PATH     = f"{DELTA_PATH}/gold/steam_reviews"

print(f"[Streaming] Kafka  : {KAFKA_BROKER}")
print(f"[Streaming] Topic  : {TOPIC_NAME}")
print(f"[Streaming] Delta  : {DELTA_PATH}")

# ── Spark Session ────────────────────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("SteamReviews-StructuredStreaming")
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    # Küçük batch boyutu → düşük gecikme
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
print("[Streaming] SparkSession oluşturuldu.")

# ── Kafka mesaj şeması (JSON içerik) ────────────────────────────────────────
review_schema = StructType([
    StructField("timestamp",        StringType(), True),
    StructField("app_id",           StringType(), True),
    StructField("app_name",         StringType(), True),
    StructField("review_id",        StringType(), True),
    StructField("user_id",          StringType(), True),
    StructField("review_text",      StringType(), True),
    StructField("voted_up",         StringType(), True),
    StructField("votes_helpful",    StringType(), True),
    StructField("playtime_forever", StringType(), True),
])

# ── Kafka'dan oku (raw stream) ───────────────────────────────────────────────
kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", TOPIC_NAME)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)

# Kafka'dan gelen value binary → string → JSON parse
raw_df = (
    kafka_df
    .select(
        F.col("offset").alias("kafka_offset"),
        F.col("partition").alias("kafka_partition"),
        F.from_json(F.col("value").cast("string"), review_schema).alias("data"),
        F.current_timestamp().alias("ingestion_time")
    )
    .select(
        "kafka_offset",
        "kafka_partition",
        "ingestion_time",
        "data.*"
    )
)

# ════════════════════════════════════════════════════════════════════════════
# BRONZE KATMAN — Ham veriler, minimal dönüşüm
# ════════════════════════════════════════════════════════════════════════════
bronze_query = (
    raw_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINT}/bronze")
    .option("path", BRONZE_PATH)
    # 5 dakikalık mikro-batch penceresi
    .trigger(processingTime="30 seconds")
    .start()
)
print(f"[Streaming] Bronze yazımı başlatıldı → {BRONZE_PATH}")


# ════════════════════════════════════════════════════════════════════════════
# SILVER KATMAN — Temizleme & Normalizasyon
#   • voted_up → boolean'a çevir
#   • votes_helpful / playtime_forever → integer'a çevir
#   • Boş review_text satırları at
#   • Duplike review_id'leri at (watermark + dropDuplicates)
#   • Timestamp düzelt
# ════════════════════════════════════════════════════════════════════════════
silver_df = (
    raw_df
    # ── Tip dönüşümleri
    .withColumn("voted_up",
                F.when(F.lower(F.col("voted_up")).isin("true", "1", "yes"), True)
                 .otherwise(False).cast("boolean"))
    .withColumn("votes_helpful",
                F.col("votes_helpful").cast("integer"))
    .withColumn("playtime_forever",
                F.col("playtime_forever").cast("long"))
    .withColumn("event_time",
                F.to_timestamp("timestamp"))
    # ── Null / boş filtreler
    .filter(F.col("review_id").isNotNull())
    .filter(F.col("app_id").isNotNull())
    .filter(F.col("review_text").isNotNull() & (F.trim("review_text") != ""))
    # ── Watermark (geç gelen mesajlar için 10 dk tolerans)
    .withWatermark("ingestion_time", "10 minutes")
    # ── Duplike review_id temizle
    .dropDuplicates(["review_id"])
    # ── Gereksiz sütunları düşür
    .drop("timestamp")
    # ── Sütun sırası netleştir
    .select(
        "review_id",
        "app_id",
        "app_name",
        "user_id",
        "review_text",
        "voted_up",
        "votes_helpful",
        "playtime_forever",
        "event_time",
        "ingestion_time",
        "kafka_offset",
        "kafka_partition",
    )
)

silver_query = (
    silver_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINT}/silver")
    .option("path", SILVER_PATH)
    .trigger(processingTime="30 seconds")
    .start()
)
print(f"[Streaming] Silver yazımı başlatıldı → {SILVER_PATH}")


# ════════════════════════════════════════════════════════════════════════════
# GOLD KATMAN — Analiz için zenginleştirilmiş veriler
#   • is_positive  : voted_up değerinden türetilmiş etiket sütunu
#   • review_length: yorum karakter sayısı
#   • word_count   : yorum kelime sayısı
#   • playtime_hrs : playtime_forever dakika → saat
#   • review_date  : event_time'dan sadece tarih
# ════════════════════════════════════════════════════════════════════════════
gold_df = (
    silver_df
    .withColumn("is_positive",
                F.when(F.col("voted_up") == True, 1).otherwise(0).cast("integer"))
    .withColumn("review_length",
                F.length(F.col("review_text")))
    .withColumn("word_count",
                F.size(F.split(F.trim("review_text"), r"\s+")))
    .withColumn("playtime_hrs",
                F.round(F.col("playtime_forever") / 60.0, 2))
    .withColumn("review_date",
                F.to_date("event_time"))
    # Sınıflandırma label: voted_up booleanını integer label olarak sakla
    .select(
        "review_id",
        "app_id",
        "app_name",
        "user_id",
        "review_text",
        "voted_up",
        "is_positive",
        "review_length",
        "word_count",
        "votes_helpful",
        "playtime_forever",
        "playtime_hrs",
        "review_date",
        "event_time",
        "ingestion_time",
    )
)

gold_query = (
    gold_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINT}/gold")
    .option("path", GOLD_PATH)
    .trigger(processingTime="30 seconds")
    .start()
)
print(f"[Streaming] Gold yazımı başlatıldı → {GOLD_PATH}")

# ── Tüm query'lerin bitmesini bekle ─────────────────────────────────────────
print("[Streaming] Tüm katmanlar aktif. Kafka mesajları işleniyor...")
print(f"  Bronze : {BRONZE_PATH}")
print(f"  Silver : {SILVER_PATH}")
print(f"  Gold   : {GOLD_PATH}")

try:
    # Her 30 saniyede ilerleme raporu
    while True:
        time.sleep(30)
        for q, name in [(bronze_query, "Bronze"),
                        (silver_query, "Silver"),
                        (gold_query,   "Gold")]:
            p = q.lastProgress
            if p:
                rows = p.get("numInputRows", 0)
                rate = p.get("processedRowsPerSecond", 0)
                print(f"  [{name}] numInputRows={rows}  rate={rate:.1f} rows/s")
except KeyboardInterrupt:
    print("\n[Streaming] Durduruldu.")
finally:
    spark.stop()
    print("[Streaming] SparkSession kapatıldı.")
