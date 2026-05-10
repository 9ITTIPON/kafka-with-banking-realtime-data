FROM python:3.10-slim

WORKDIR /app

RUN pip install --no-cache-dir requests

COPY traffic_generator.py .

ENV BANK_SERVICE_URL=http://bank-service:8080/transaction

CMD ["python", "traffic_generator.py"]
