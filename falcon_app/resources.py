import json
import os
from datetime import datetime, timezone
from json import JSONDecodeError

from kafka3 import KafkaProducer, KafkaConsumer

from custom_exceptions import (
    EmptyRequestBody,
    EmptyPayload,
    NoNumbersProvided,
    InvalidJSON,
    MissingKey,
    NonNumberProvided,
)


class TakeNumbers:

    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
            value_serializer=lambda payload: json.dumps(payload).encode("utf-8"),
        )

    def on_post(self, req, resp):
        payload = self.clean(req)

        timestamp = datetime.now().astimezone(timezone.utc).isoformat()
        payload.update(dict(eventTimestamp=timestamp))

        self.producer.send("numbers", payload)
        self.producer.flush()

        resp.media = dict(status="ok", message="Submitted successfully.")

    def clean(self, req):
        try:
            payload = req.stream.read()

            if not payload:
                raise EmptyRequestBody

            data = json.loads(payload.decode("utf-8"))

            if not data:
                raise EmptyPayload

            if not isinstance(data, dict):
                raise InvalidJSON("Data must be a dict.")

            if "numbers" not in data:
                raise MissingKey("numbers")

            numbers = data["numbers"]

            if not isinstance(numbers, list):
                raise InvalidJSON('"numbers" must be a list.')

            if not numbers:
                raise NoNumbersProvided

            if not all(isinstance(number, int) for number in numbers):
                raise NonNumberProvided

            return data

        except JSONDecodeError:
            raise InvalidJSON


class TakeFinalNumbers:
    def __init__(self):
        self.consumer = KafkaConsumer(
            "final-numbers",
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
            group_id=os.getenv("KAFKA_GROUP_ID"),
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            heartbeat_interval_ms=2000,
            session_timeout_ms=10000,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        )

    def on_get(self, req, resp):
        print("Polling final-numbers for max 5 messages...")

        msg_pack = self.consumer.poll(timeout_ms=2000, max_records=5)

        data = [msg.value for msgs in msg_pack.values() for msg in msgs]

        self.consumer.commit()

        resp.media = data
