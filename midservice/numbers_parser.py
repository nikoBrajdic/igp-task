import json
import os
import pprint
from datetime import datetime, timezone

from kafka3 import KafkaProducer, KafkaConsumer

consumer = KafkaConsumer(
    "numbers",
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
    group_id=os.getenv("KAFKA_GROUP_ID"),
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    heartbeat_interval_ms=3000,
    session_timeout_ms=10000,
    value_deserializer=lambda message: json.loads(message.decode("utf-8")),
)
producer = KafkaProducer(
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
    value_serializer=lambda payload: json.dumps(payload).encode("utf-8"),
)

for msg in consumer:
    print("parsing message...")

    data = msg.value

    pprint.pprint(data, indent=4)

    data.update(
        dict(
            msg_offset=msg.offset,
            sum=sum(data["numbers"]),
            timestamp=datetime.now().astimezone(timezone.utc).isoformat(),
        )
    )

    print("data updated: ")

    pprint.pprint(data, indent=4)

    producer.send("final-numbers", value=data)
    producer.flush()

    print(f"message #{msg.offset} sent.\n")

    consumer.commit()
