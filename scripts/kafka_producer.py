import json
import random
import time
from datetime import datetime, timezone
from uuid import uuid4

from confluent_kafka import Producer


producer = Producer({
    "bootstrap.servers": "localhost:9092"
})

TOPIC = "order_events"


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(
            f"Sent -> partition={msg.partition()} "
            f"offset={msg.offset()}"
        )


for i in range(100):

    quantity = random.randint(1, 5)
    unit_price = round(random.uniform(10, 1500), 2)

    event = {
        "event_id": str(uuid4()),
        "order_id": 200000 + i,
        "customer_id": random.randint(1, 20000),
        "product_id": random.randint(1, 194),
        "quantity": quantity,
        "unit_price": unit_price,
        "total_amount": round(quantity * unit_price, 2),
        "event_type": "ORDER_CREATED",
        "event_timestamp": datetime.now(
            timezone.utc
        ).isoformat()
    }

    producer.produce(
        TOPIC,
        key=str(event["customer_id"]),
        value=json.dumps(event),
        callback=delivery_report
    )

    producer.poll(0)

    time.sleep(0.1)


producer.flush()

print()
print("100 order events sent successfully.")