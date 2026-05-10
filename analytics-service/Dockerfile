FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Environment variable for Kafka broker
ENV KAFKA_BROKER=kafka:29092

CMD ["faust", "-A", "processor", "worker", "-l", "info"]
