import os
import time
import json
import pandas as pd
from kafka import KafkaProducer
from datetime import datetime

# Config
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC_NAME = os.getenv("TOPIC_NAME", "steam-reviews")
CSV_FILE = os.getenv("CSV_FILE", "/app/data/steam_reviews.csv")
MESSAGES_PER_SECOND = int(os.getenv("MESSAGES_PER_SECOND", "50"))

print(f"[Producer] Kafka Broker: {KAFKA_BROKER}")
print(f"[Producer] Topic: {TOPIC_NAME}")
print(f"[Producer] CSV: {CSV_FILE}")
print(f"[Producer] Hız: {MESSAGES_PER_SECOND} mesaj/sn")

# Kafka bağlantısı - Kafka hazır olana kadar bekle
producer = None
while producer is None:
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        print("[Producer] Kafka bağlantısı kuruldu!")
    except Exception as e:
        print(f"[Producer] Kafka bekleniyor... {e}")
        time.sleep(5)

# CSV oku
print(f"[Producer] CSV okunuyor: {CSV_FILE}")

delay = 1.0 / MESSAGES_PER_SECOND
sent = 0

try:
    for chunk in pd.read_csv(CSV_FILE, chunksize=1000, low_memory=False):
        # Sütun adlarını normalize et
        chunk.columns = [c.strip().lower().replace(" ", "_") for c in chunk.columns]
        
        for _, row in chunk.iterrows():
            message = {
                "timestamp": datetime.utcnow().isoformat(),
                "app_id": str(row.get("app_id", "")),
                "app_name": str(row.get("app_name", "")),
                "review_id": str(row.get("review_id", row.get("recommendationid", sent))),
                "user_id": str(row.get("author.steamid", row.get("steamid", sent))),
                "review_text": str(row.get("review", ""))[:500],
                "voted_up": str(row.get("voted_up", "")),
                "votes_helpful": str(row.get("votes_helpful", "0")),
                "playtime_forever": str(row.get("author.playtime_forever", "0"))
            }
            producer.send(TOPIC_NAME, value=message)
            sent += 1

            if sent % 1000 == 0:
                print(f"[Producer] {sent} mesaj gönderildi")

            time.sleep(delay)
            
except Exception as e:
    print(f"[Producer] CSV okunurken hata oluştu: {e}")

producer.flush()
print(f"[Producer] Tamamlandı! Toplam {sent} mesaj gönderildi.")
