import json
import time

from faker import Faker
import random
from confluent_kafka import SerializingProducer
from datetime import datetime, UTC

fake = Faker()

def generate_sales_transactions():
    user = fake.simple_profile()

    return {
        "transactionId": fake.uuid4(),
        "productId": random.choice([
            'TECH-LAP-001', 'TECH-MOB-002', 'TECH-TAB-003', 'TECH-WTC-004', 'TECH-AUD-005',
            'TECH-SPK-006', 'TECH-MON-007', 'TECH-KEY-008', 'TECH-MOU-009', 'TECH-PRN-010',
            'TECH-CAM-011', 'TECH-MIC-012', 'TECH-GAM-013', 'TECH-PWR-014', 'TECH-CHG-015'
        ]),
        "productName": random.choice([
            'MacBook Pro 16"', 'iPhone 15 Pro', 'iPad Air 5th Gen', 'Galaxy Watch 6',
            'Sony WH-1000XM5', 'JBL Flip 6', 'Dell 27" 4K Monitor', 'Logitech MX Keys',
            'Logitech MX Master 3', 'HP LaserJet Pro', 'Logitech C920', 'Blue Yeti X',
            'PlayStation 5', 'Anker 26800mAh', 'Samsung 15W Wireless Charger'
        ]),
        "productCategory": random.choice([
            'Laptops', 'Smartphones', 'Tablets', 'Wearables', 'Audio',
            'Computer Accessories', 'Gaming', 'Photography', 'Smart Home'
        ]),
        "productPrice": round(random.uniform(10, 1000), 2),
        "productQuantity": random.randint(1, 10),
        "productBrand": random.choice([
            'Apple', 'Samsung', 'Sony', 'Dell', 'HP', 'Lenovo', 'LG',
            'Logitech', 'JBL', 'Anker', 'Microsoft', 'Asus', 'Acer'
        ]),
        "currency": random.choice(['USD', 'EUR', 'GBP', 'JPY', 'AUD', 'CAD']),
        "customerId": user['username'],
        "transactionDate": datetime.utcnow().strftime('%Y-%m-%dT%H:%S.%f%z'),
        "paymentMethod": random.choice([
            "credit_card", "debit_card", "paypal", "apple_pay",
            "google_pay", "bank_transfer", "crypto"
        ]),
    }

def delivery_report(err, msg):
    if err is not None:
        print(f"Message of delivery failure: {err}")
    else:
        print(f"Message delivered to {msg.topic} [{msg.partition()}]")

def main():
    topic = 'sales_transactions'
    producer = SerializingProducer({
        'bootstrap.servers': 'localhost:9092'
    })
    current_time = datetime.now()
    while (datetime.now() - current_time).seconds < 120:
        try:
            transaction = generate_sales_transactions()
            transaction['totalAmount'] = transaction['productPrice'] * transaction['productQuantity']
            print(transaction)
            producer.produce(
                topic,
                key=transaction['transactionId'],
                value=json.dumps(transaction),
                on_delivery=delivery_report
            )
            producer.poll(0)
            time.sleep(5)
        except BufferError:
            print("Buffer full ! Waiting...")
            time.sleep(1)
        except Exception as e:
            print(e)

if __name__ == "__main__":
    main()