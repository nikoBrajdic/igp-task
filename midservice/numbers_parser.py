import json
import pprint
from datetime import datetime, timezone

from kafka3 import KafkaProducer, KafkaConsumer

consumer = KafkaConsumer(
    'numbers',
    bootstrap_servers='kafka:9092',
    group_id='initials',
    auto_offset_reset='earliest',
    enable_auto_commit=False,
    heartbeat_interval_ms=3000,
    session_timeout_ms=10000,
    value_deserializer=lambda message: json.loads(message.decode("utf-8")),
)
producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda payload: json.dumps(payload).encode("utf-8"),
)

for msg in consumer:
    print("parsing message...")

    data = msg.value

    pprint.pprint(data, indent=4)

    data.update(dict(
        msg_offset=msg.offset,
        sum=sum(data["numbers"]),
        timestamp=datetime.now().astimezone(timezone.utc).isoformat(),
    ))

    print("data updated: ")

    pprint.pprint(data, indent=4)

    producer.send('final-numbers', value=data)

    print(f"message #{msg.offset} sent.\n")

    consumer.commit()
