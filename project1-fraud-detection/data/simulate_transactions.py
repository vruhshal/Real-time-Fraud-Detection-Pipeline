"""
simulate_transactions.py
Generates fake card transaction data using Faker.
Run: python simulate_transactions.py --num-events 10000 --output transactions.json
"""

import json
import random
import argparse
from faker import Faker
from datetime import datetime, timedelta

fake = Faker("en_IN")  # Indian locale for realistic local data
random.seed(42)


def make_transaction():
    """Generate one fake card transaction."""
    is_fraud = random.choices([0, 1], weights=[97, 3])[0]

    # Fraudulent transactions tend to be large, at odd hours, foreign merchants
    if is_fraud:
        amount = round(random.uniform(20000, 99999), 2)
        hour = random.choice([0, 1, 2, 3, 4, 23])
        merchant_city = fake.city()
        merchant_country = random.choice(["US", "UK", "SG", "AE"])
    else:
        amount = round(random.uniform(50, 15000), 2)
        hour = random.randint(8, 22)
        merchant_city = fake.city()
        merchant_country = "IN"

    txn_time = fake.date_time_this_year().replace(hour=hour)

    return {
        "transaction_id": fake.uuid4(),
        "card_id": fake.credit_card_number(card_type="visa16"),
        "customer_id": f"CUST_{random.randint(1000, 9999)}",
        "amount": amount,
        "currency": "INR",
        "merchant_name": fake.company(),
        "merchant_city": merchant_city,
        "merchant_country": merchant_country,
        "transaction_timestamp": txn_time.isoformat(),
        "transaction_type": random.choice(["POS", "ONLINE", "ATM", "CONTACTLESS"]),
        "is_fraud": is_fraud,
        "card_present": random.choice([True, False]),
        "device_id": f"DEV_{fake.uuid4()[:8]}",
    }


def main(num_events: int, output_path: str):
    print(f"Generating {num_events} transactions...")
    transactions = [make_transaction() for _ in range(num_events)]

    fraud_count = sum(1 for t in transactions if t["is_fraud"] == 1)
    print(f"  Total: {len(transactions)}")
    print(f"  Fraud: {fraud_count} ({round(fraud_count/len(transactions)*100, 1)}%)")
    print(f"  Legit: {len(transactions) - fraud_count}")

    with open(output_path, "w") as f:
        json.dump(transactions, f, indent=2, default=str)

    print(f"Saved to {output_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--num-events", type=int, default=10000)
    parser.add_argument("--output", type=str, default="transactions.json")
    args = parser.parse_args()
    main(args.num_events, args.output)
