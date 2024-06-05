import hashlib
import json
import os
import time
import traceback
from datetime import datetime, timezone
from json import JSONDecodeError

import redis
from kafka3 import KafkaProducer, KafkaConsumer, OffsetAndMetadata
from kafka3.errors import KafkaTimeoutError
from kafka3.coordinator.assignors.roundrobin import RoundRobinPartitionAssignor

from custom_exceptions import *


class Utils:
    @classmethod
    def hash_payload(cls, payload):
        return hashlib.md5(json.dumps(payload).encode("utf-8")).hexdigest()


class ProduceNumbers(Utils):

    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
            value_serializer=lambda payload: json.dumps(payload).encode("utf-8"),
        )

        self.redis = redis.Redis(
            host=os.getenv("REDIS_HOST"),
            port=os.getenv("REDIS_PORT"),
        )

    def on_post(self, req, resp):
        payload = self.clean(req)

        timestamp = datetime.now().astimezone(timezone.utc).isoformat()
        payload.update(dict(eventTimestamp=timestamp))

        if not self.try_produce_message(payload):
            raise falcon.HTTPInternalServerError(
                title="Maximum number of retries reached, message processing failed."
            )

        resp.media = dict(status="ok", message="Submitted successfully.")

    def try_produce_message(self, payload, retries=30):
        cache_key_in = "numbers_processing"
        cache_key_done = "numbers_produced"

        left = 1

        payload_hash = self.hash_payload(payload)

        while left < retries + 1:
            try:
                lock_in = self.redis.lock(
                    f"lock:{cache_key_in}", blocking=True, blocking_timeout=40
                )
                lock_done = self.redis.lock(f"lock:{cache_key_done}", timeout=40)

                if self.redis.sismember(cache_key_done, payload_hash):
                    print(f"message #{payload_hash} already produced, skipping.")
                    return True

                if self.redis.sismember(cache_key_in, payload_hash):
                    raise MessageInProcess(payload_hash)

                with lock_in:
                    self.redis.sadd(cache_key_in, payload_hash)

                print("validating outgoing message against cache...")

                if self.redis.sismember(cache_key_done, payload_hash):
                    print(f"message #{payload_hash} already produced, skipping.")
                    return True

                self.produce_message(payload)

                with lock_done:
                    with self.redis.pipeline() as pipe:
                        pipe.watch(cache_key_done, cache_key_in)

                        pipe.multi()

                        pipe.sadd(cache_key_done, payload_hash)
                        pipe.srem(cache_key_in, payload_hash)

                        pipe.execute()

                return True

            except KafkaTimeoutError:
                e = traceback.format_exc()
                print(f"Message not flushed, reason:\n{e}")

            except (MessageNotSent, MessageInProcess) as e:
                print(e)

            except Exception:
                e = traceback.format_exc()
                print(f"something went wrong while handling message:\n{e}")

            print(f"Retry {left}/{retries}...")
            time.sleep(1)
            left += 1

        return False

    def produce_message(self, payload):
        msg_future = self.producer.send(os.getenv("KAFKA_TOPIC_1"), payload)
        self.producer.flush()
        msg_future.get(timeout=10)

        if not msg_future.is_done:
            raise MessageNotSent

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


class ConsumeFinalNumbers(Utils):
    def __init__(self):
        self.consumer = KafkaConsumer(
            os.getenv("KAFKA_TOPIC_2"),
            bootstrap_servers=[os.getenv("KAFKA_BOOTSTRAP_SERVERS")],
            group_id=os.getenv("KAFKA_GROUP_2"),
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            session_timeout_ms=int(os.getenv('KAFKA_CONSUMER_SESSION_TIMEOUT', 30000)),
            heartbeat_interval_ms=int(os.getenv('KAFKA_CONSUMER_HEARTBEAT_INTERVAL', 10000)),
            max_poll_interval_ms=300000,
            partition_assignment_strategy=[RoundRobinPartitionAssignor],
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        )

        self.redis = redis.Redis(
            host=os.getenv("REDIS_HOST"), port=os.getenv("REDIS_PORT")
        )

    def on_get(self, req, resp):
        print(f"Polling {self.consumer.config['group_id']} for max 5 messages...")

        msg_pack = self.consumer.poll(timeout_ms=2000, max_records=5)

        data = list()
        errors = [
            self.process_batch(data, messages, tp) for tp, messages in msg_pack.items()
        ]

        resp.media = data
        if not data and errors:
            raise falcon.HTTPInternalServerError(title="Could not process messages.")

    def process_batch(self, data, messages, tp):
        errors = [self.try_process_message(data, msg, tp) for msg in messages]
        errors = any(e for e in errors if e is False)
        return errors

    def try_process_message(self, data, msg, tp, retries=30):
        cache_key_done = "final_numbers_consumed"
        cache_key_in = "final_numbers_processing"

        message_hash = self.hash_payload(msg.value)

        left = 1
        while left < retries + 1:
            try:
                lock_in = self.redis.lock(
                    f"lock:{cache_key_in}", blocking=True, blocking_timeout=40
                )
                lock_done = self.redis.lock(f"lock:{cache_key_done}", timeout=40)

                if self.redis.sismember(cache_key_done, message_hash):
                    print(f"Message #{msg.offset} already consumed, skipping.")
                    return True

                if self.redis.sismember(cache_key_in, message_hash):
                    raise MessageInProcess(msg.offset)

                with lock_in:
                    self.redis.sadd(cache_key_in, message_hash)

                if self.redis.sismember(cache_key_done, message_hash):
                    print(f"Message #{msg.offset} already consumed, skipping.")
                    return True

                data.append(msg.value)
                self.consumer.commit({tp: OffsetAndMetadata(msg.offset + 1, None)})

                with lock_done:
                    with self.redis.pipeline() as pipe:
                        pipe.watch(cache_key_done, cache_key_in)

                        pipe.multi()

                        pipe.sadd(cache_key_done, message_hash)
                        pipe.srem(cache_key_in, message_hash)

                        pipe.execute()

                return True

            except KafkaTimeoutError:
                e = traceback.format_exc()
                print(f"Message not flushed, reason:\n{e}")

            except MessageInProcess as e:
                print(e)

            except Exception:
                e = traceback.format_exc()
                print(f"something went wrong while handling message:\n{e}")

            print(f"Retry {left}/{retries}...")
            left += 1
            time.sleep(1)

        return False
