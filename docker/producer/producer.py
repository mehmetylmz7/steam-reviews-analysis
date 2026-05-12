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
MESSAGES_PER_SECOND = int(os.getenv("MESSAGES_PER_SECOND", "3000"))

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
                
                # Kaggle veri setinde review_id ve author.steamid bulunmadığı için sayacı (sent) ID olarak kullanıyoruz
                "review_id": f"rev_{sent}",
                "user_id": f"user_{sent}",
                
                # Yorum metni Kaggle'da 'review_text' olarak yer alıyor
                "review_text": str(row.get("review_text", ""))[:500],
                
                # Olumlu/Olumsuz değerlendirmesi Kaggle'da 'review_score' olarak geçiyor (1 veya -1)
                "voted_up": str(row.get("review_score", "")),
                
                # Faydalı bulma sayısı Kaggle'da 'review_votes' olarak geçiyor
                "votes_helpful": str(row.get("review_votes", "0")),
                
                # Oynama süresi verisi bu veri setinde yer almıyor, pipeline'ı korumak için 0 atıyoruz
                "playtime_forever": "0"
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