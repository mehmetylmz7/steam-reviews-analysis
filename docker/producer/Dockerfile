FROM python:3.10-slim

WORKDIR /app

RUN pip install --default-timeout=300 --retries=5 kafka-python pandas

COPY producer.py .

CMD ["python", "producer.py"]
