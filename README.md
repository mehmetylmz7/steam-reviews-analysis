# Steam Reviews Analysis: Big Data & Machine Learning Pipeline

Bu proje, Steam platformundaki milyonlarca kullanıcı incelemesini (review) gerçek zamanlı olarak işleyen, temizleyen ve makine öğrenmesi algoritmalarıyla analiz eden uçtan uca bir veri hattı (data pipeline) sistemidir.

##  Proje Genel Bakışı
Proje, ham verinin kaynağından alınmasından başlayarak interaktif bir dashboard sunumuna kadar uzanan modern büyük veri mühendisliği ve veri bilimi pratiklerini içermektedir. Toplamda 6.4 milyon satırlık devasa bir veri seti üzerinde çalışılmıştır.

##  Teknoloji Yığını
*  Diller:  Python, SQL
*  Büyük Veri:  Apache Spark (PySpark), Spark Structured Streaming
*  Veri Akışı:  Apache Kafka
*  Depolama & Mimari:  Delta Lake (Bronze, Silver, Features katmanları)
*  Makine Öğrenmesi:  PySpark MLlib, MLflow
*  Konteynerleştirme:  Docker, Docker Compose
*  Platform:  Databricks
*  Görselleştirme:  Plotly, Matplotlib, Seaborn

##  Veri Hattı Mimarisi
Proje, "Medallion Architecture" (Madalyon Mimarisi) prensiplerine göre tasarlanmıştır:
1.  Ingestion (Kafka): Veriler Kafka Producer üzerinden mikro-partiler halinde sistem içine akıtılır.
2.  Bronze (Raw): Kafka'dan gelen ham JSON verileri olduğu gibi kaydedilir.
3.  Silver (Clean): Veri tipleri dönüştürülür, "NaN" ve gizli boşluklar temizlenir.
4.  Features (Gold): Metin verilerinden NLP tabanlı özellikler (uzunluk, kelime sayısı, anahtar kelime varlığı) türetilir.

##  Makine Öğrenmesi Süreci
Veri setinde tespit edilen  %82 olumlu - %18 olumsuz  şeklindeki sınıf dengesizliği (class imbalance), modelin ezberlemesini önlemek adına  Undersampling (Alt Örnekleme)  tekniği ile çözülmüştür.

Eğitilen modeller:
* Logistic Regression
* Random Forest
* Decision Tree
* Gradient Boosted Trees (GBT)
*  Linear SVC (En İyi Model) 

 Best Model Sonuçları (Linear SVC): 
* Accuracy: %74.09
* F1-Score: %75.31
* AUC-ROC: 0.7083

##  Dashboard ve Analiz
Modelin tüm oyunlar üzerindeki tahmin başarısı interaktif grafiklerle doğrulanmıştır. Proje kapsamında üretilen görselleştirmeler:
* Gerçek vs. Tahmini Beğeni Oranı Karşılaştırması (Plotly Bubble Chart)
* Model Karmaşıklık Matrisi (Confusion Matrix)
* ROC Eğrisi (Model Başarımı)
* Özellik Önemi (Feature Importance)

##  Nasıl Çalıştırılır?
1.  Altyapıyı ayağa kaldırın:
    ```bash
    docker-compose up -d
    ```
2.  Veri akışını başlatın:
    ```bash
    python docker/producer/producer.py
    ```
3.  Databricks üzerinde notebook'ları sırasıyla (4'ten 7'ye kadar) çalıştırın.
