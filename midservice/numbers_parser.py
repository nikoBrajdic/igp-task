import hashlib
import json
import os
import time
import traceback
from datetime import datetime, timezone

import redis as r
from kafka3 import KafkaProducer, KafkaConsumer, ConsumerRebalanceListener
from kafka3.coordinator.assignors.roundrobin import RoundRobinPartitionAssignor
from kafka3.errors import KafkaTimeoutError


class MessageNotSent(Exception):
    def __init__(self, offset):
        super().__init__(f"Message #{offset} failed to send.")


class Config:
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    KAFKA_TOPIC_1 = os.getenv("KAFKA_TOPIC_1")
    KAFKA_TOPIC_2 = os.getenv("KAFKA_TOPIC_2")
    KAFKA_GROUP_1 = os.getenv("KAFKA_GROUP_1")
    KAFKA_GROUP_2 = os.getenv("KAFKA_GROUP_2")
    REDIS_HOST = os.getenv("REDIS_HOST")
    REDIS_PORT = os.getenv("REDIS_PORT")
    KAFKA_CONSUMER_SESSION_TIMEOUT = int(
        os.getenv("KAFKA_CONSUMER_SESSION_TIMEOUT", 30000)
    )
    KAFKA_CONSUMER_HEARTBEAT_INTERVAL = int(
        os.getenv("KAFKA_CONSUMER_HEARTBEAT_INTERVAL", 10000)
    )


class RebalanceListener(ConsumerRebalanceListener):
    def __init__(self, consumer_):
        self.consumer = consumer_

    def on_partitions_revoked(self, revoked_partitions):
        try:
            print(f"Partitions revoked: {revoked_partitions}")
            self.consumer.commit()
        except Exception as e:
            print(f"Error during partition revocation: {e}")

    def on_partitions_assigned(self, assigned_partitions):
        print(f"Partitions assigned: {assigned_partitions}")


class Utils:
    @classmethod
    def hash_payload(cls, payload):
        return hashlib.md5(json.dumps(payload).encode("utf-8")).hexdigest()

    @classmethod
    def get_redis_connection(cls):
        return r.Redis(host=Config.REDIS_HOST, port=Config.REDIS_PORT)

    @classmethod
    def change_message_state(cls, redis_conn, operations):
        with redis_conn.pipeline() as pipe:
            pipe.watch(*operations.keys())
            pipe.multi()
            for key, action in operations.items():
                action(pipe, key)
            pipe.execute()


consumer = KafkaConsumer(
    Config.KAFKA_TOPIC_1,
    bootstrap_servers=[Config.KAFKA_BOOTSTRAP_SERVERS],
    group_id=Config.KAFKA_GROUP_1,
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    session_timeout_ms=Config.KAFKA_CONSUMER_SESSION_TIMEOUT,
    heartbeat_interval_ms=Config.KAFKA_CONSUMER_HEARTBEAT_INTERVAL,
    partition_assignment_strategy=[RoundRobinPartitionAssignor],
    value_deserializer=lambda message: json.loads(message.decode("utf-8")),
)
consumer.subscribe([Config.KAFKA_TOPIC_1], listener=RebalanceListener(consumer))
producer = KafkaProducer(
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
    value_serializer=lambda payload: json.dumps(payload).encode("utf-8"),
)

redis = Utils.get_redis_connection()


def update_payload(msg):
    print("updating message...")

    data = msg.value
    data.update(
        dict(
            msg_offset=msg.offset,
            sum=sum(data["numbers"]),
            timestamp=datetime.now().astimezone(timezone.utc).isoformat(),
        )
    )

    print("message updated.")

    return data


def produce_message(data, msg):
    print("sending message...")

    msg_future = producer.send(os.getenv("KAFKA_TOPIC_2"), value=data)
    producer.flush()
    msg_sent = msg_future.get(timeout=10)

    if not msg_future.is_done:
        raise MessageNotSent(msg.offset)

    print(f"message #{msg.offset} sent.")


class MessageInProcess(Exception):
    def __init__(self, msg):
        super().__init__(f"message #{msg.offset} being processed, retrying.")


def try_process_message(msg, retries=30):
    cache_key_in = "numbers_processing"
    cache_key_done = "numbers_processed"
    cache_key_out = "final_numbers_produced"

    payload_hash_in = Utils.hash_payload(msg.value)

    for attempt in range(1, retries + 1):
        try:
            lock_in = redis.lock(
                f"lock:{cache_key_in}", blocking=True, blocking_timeout=40
            )
            lock_out = redis.lock(
                f"lock:{cache_key_out}", blocking=True, blocking_timeout=40
            )
            lock_done = redis.lock(
                f"lock:{cache_key_done}", blocking=True, blocking_timeout=40
            )

            with lock_in:
                print("validating incoming message against cache...")

                if redis.sismember(cache_key_done, payload_hash_in):
                    print(f"message #{msg.offset} already consumed, skipping.")
                    return True

                if redis.sismember(cache_key_in, payload_hash_in):
                    raise MessageInProcess(msg)

                redis.sadd(cache_key_in, payload_hash_in)

            data = update_payload(msg)
            payload_hash_out = Utils.hash_payload(data)

            with lock_out:
                print("validating outgoing message against cache...")

                if redis.sismember(cache_key_out, payload_hash_out):
                    print(f"message #{msg.offset} already produced, skipping.")
                    return True

            produce_message(data, msg)

            consumer.commit()

            with lock_done:
                Utils.change_message_state(
                    redis,
                    {
                        cache_key_out: lambda pipe, key: pipe.sadd(
                            key, payload_hash_out
                        ),
                        cache_key_done: lambda pipe, key: pipe.sadd(
                            key, payload_hash_in
                        ),
                        cache_key_in: lambda pipe, key: pipe.srem(key, payload_hash_in),
                    },
                )

            print(f"message #{msg.offset} consumed and set as latest.")
            return True

        except KafkaTimeoutError:
            e = traceback.format_exc()
            print(f"Message #{msg.offset} failed to flush, reason:\n{e}")

        except (MessageNotSent, MessageInProcess) as e:
            print(e)

        except Exception:
            e = traceback.format_exc()
            print(f"something went wrong while handling message:\n{e}")

        finally:
            redis.srem(cache_key_in, payload_hash_in)

        print(f"Retry {attempt}/{retries}...")
        time.sleep(1)

    print(f"Failed to process message #{msg.offset}")


def main():
    while msg := next(consumer):
        try_process_message(msg)


if __name__ == "__main__":
    main()
