import requests
import random
import time
import json
import os

ACCOUNTS = [f"ACC-{i:03d}" for i in range(1, 11)]
TYPES = ["deposit", "withdrawal", "transfer"]

BANK_SERVICE_URL = os.getenv("BANK_SERVICE_URL", "http://localhost:8080/transaction")

def generate_transaction():
    account_id = random.choice(ACCOUNTS)
    # Most transactions are small, occasionally a large one (fraud)
    if random.random() < 0.1:
        amount = round(random.uniform(10001, 50000), 2)
    else:
        amount = round(random.uniform(10, 5000), 2)
    
    tx_type = random.choice(TYPES)
    
    payload = {
        "account_id": account_id,
        "amount": amount,
        "type": tx_type
    }
    
    try:
        response = requests.post(BANK_SERVICE_URL, json=payload)
        if response.status_code == 201:
            print(f"Sent: {payload}")
        else:
            print(f"Failed: {response.text}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    print("Starting traffic generator... (Ctrl+C to stop)")
    while True:
        generate_transaction()
        time.sleep(random.uniform(1, 5))
