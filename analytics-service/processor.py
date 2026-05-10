import faust
import base64
import json
from decimal import Decimal
import os

# Helper to decode Debezium's base64 decimal format
def decode_debezium_decimal(data, scale=2):
    if data is None:
        return 0.0
    decoded_bytes = base64.b64decode(data)
    # Debezium encodes decimals as variable-length bytes
    # This is a simple conversion for this example
    val = int.from_bytes(decoded_bytes, byteorder='big', signed=True)
    return float(Decimal(val) / Decimal(10**scale))

broker = os.getenv('KAFKA_BROKER', 'kafka://localhost:9092')
if not broker.startswith('kafka://'):
    broker = f'kafka://{broker}'

app = faust.App(
    'bank-analytics',
    broker=broker,
    value_serializer='json',
)

# Define the topic we consume from (Debezium output)
transactions_topic = app.topic('dbserver1.public.transactions')

# Define a topic for fraud alerts
alerts_topic = app.topic('fraud-alerts')

@app.agent(transactions_topic)
async def process_transactions(transactions):
    async for tx_event in transactions:
        # Debezium events have 'payload' -> 'after' for new inserts
        payload = tx_event.get('payload', {})
        after = payload.get('after')
        
        if after:
            account_id = after.get('account_id')
            raw_amount = after.get('amount')
            tx_type = after.get('type')
            
            # Decode the amount
            amount = decode_debezium_decimal(raw_amount)
            
            print(f"Processing transaction: {account_id} - ${amount} ({tx_type})")
            
            # Fraud detection logic: Amount > 10,000
            if amount > 10000:
                alert = {
                    "account_id": account_id,
                    "amount": amount,
                    "type": tx_type,
                    "reason": "High value transaction",
                    "timestamp": after.get('created_at')
                }
                print(f"!!! FRAUD ALERT !!! {alert}")
                await alerts_topic.send(value=alert)

if __name__ == '__main__':
    app.main()
