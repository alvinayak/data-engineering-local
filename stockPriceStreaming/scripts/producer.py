import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer
from snowflake import SnowflakeGenerator

# machine_id=1 for this producer instance
# if you run multiple producers, give each a different machine_id
snowflake = SnowflakeGenerator(machine_id=1, datacenter_id=1)

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    key_serializer=lambda k: k.encode('utf-8'),
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    enable_idempotence=True,
    acks='all',
    retries=5,
    retry_backoff_ms=500,
    max_in_flight_requests_per_connection=1
)

STOCKS = {
    'AAPL':  150.0,
    'GOOGL': 2800.0,
    'MSFT':  380.0,
    'AMZN':  185.0,
    'JPM':   195.0
}

def simulate_price(last_price):
    change_pct = random.uniform(-0.015, 0.015)
    return round(last_price * (1 + change_pct), 2)

print("Starting producer... Press Ctrl+C to stop")

while True:
    for symbol, price in STOCKS.items():
        new_price = simulate_price(price)
        STOCKS[symbol] = new_price

        message_id = snowflake.generate()

        event = {
            "message_id": message_id,      # Snowflake ID
            "symbol": symbol,
            "price": new_price,
            "event_time": datetime.utcnow().isoformat(),
            "volume": random.randint(100, 10000)
        }

        producer.send(
            topic='stock-prices',
            key=symbol,
            value=event
        )
        print(f"Sent: {symbol} @ {new_price} | ID: {message_id}")

    producer.flush()
    time.sleep(1)