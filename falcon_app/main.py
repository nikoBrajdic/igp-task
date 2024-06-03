import json
import os
from json import JSONDecodeError
from datetime import datetime, timezone

import falcon
from kafka3 import KafkaProducer, KafkaConsumer





class APIKeyMiddleware:
    def __init__(self):
        self.valid_api_keys = {os.getenv("API_KEY", "")}

    def process_request(self, req, resp):
        api_key = req.get_header('X-API-KEY')
        if api_key and api_key not in self.valid_api_keys:
            raise falcon.HTTPUnauthorized(
                title='Unauthorized',
                description='Invalid or missing API key.'
            )


class ResponseTimeMiddleware:
    def process_request(self, req, resp):
        req.context.start_time = datetime.now()

    def process_response(self, req, resp, resource, req_succeeded):
        if not hasattr(req.context, 'start_time'):
            return
        elapsed_time = str(datetime.now() - req.context.start_time)
        resp.set_header('X-Response-Time', elapsed_time)


class TakeNumbers:

    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers="kafka:9092",
            value_serializer=lambda payload: json.dumps(payload).encode("utf-8"),
        )

    def on_post(self, req, resp):
        try:
            payload = req.stream.read()

            if not payload:
                raise falcon.HTTPBadRequest(title="Empty request body.")

            data = json.loads(payload.decode("utf-8"))

            if not data:
                raise falcon.HTTPBadRequest(title="Submitted payload empty.")

            numbers = data["numbers"]

            if not numbers:
                raise falcon.HTTPBadRequest(title="No numbers provided.")

            payload_out = dict(
                numbers=numbers,
                eventTimestamp=datetime.now().astimezone(timezone.utc).isoformat(),
            )

            self.producer.send("numbers", payload_out)
            self.producer.flush()

            resp.media = dict(status="ok", message="Submitted successfully.")

        except JSONDecodeError:
            raise falcon.HTTPBadRequest(title="Invalid JSON string payload.")

        except KeyError as e:
            raise falcon.HTTPBadRequest(
                title="Missing key", description="JSON must contain key {}".format(e)
            )


class TakeFinalNumbers:
    def __init__(self):
        self.consumer = KafkaConsumer(
            "final-numbers",
            bootstrap_servers="kafka:9092",
            group_id="finals",
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


app = falcon.App(middleware=[
    APIKeyMiddleware(),
    ResponseTimeMiddleware(),
])
app.add_route("/numbers", TakeNumbers())
app.add_route("/final_numbers", TakeFinalNumbers())
