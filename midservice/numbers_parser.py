import hashlib
import json
import os
import time
import traceback
from datetime import datetime, timezone

import redis as r
from kafka3 import KafkaProducer, KafkaConsumer
from kafka3.errors import KafkaTimeoutError


class MessageNotSent(Exception):
    def __init__(self, offset, left, retries):
        super().__init__(f"Message #{offset} failed to send.")


consumer = KafkaConsumer(
    os.getenv("KAFKA_TOPIC_1"),
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
    group_id=os.getenv("KAFKA_GROUP_1"),
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

redis = r.Redis(
    host=os.getenv("REDIS_HOST"),
    port=os.getenv("REDIS_PORT"),
)


def hash_payload(payload):
    return hashlib.md5(json.dumps(payload).encode("utf-8")).hexdigest()


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


def produce_message(data, left, msg, retries):
    print("sending message...")

    msg_future = producer.send(os.getenv("KAFKA_TOPIC_2"), value=data)
    producer.flush()
    msg_sent = msg_future.get(timeout=10)

    if not msg_future.is_done:
        raise MessageNotSent(msg.offset, left, retries)

    print(f"message #{msg.offset} sent.")


class MessageInProcess(Exception):
    def __init__(self, msg):
        super().__init__(f"message #{msg.offset} being processed, retrying.")


def try_process_message(msg, retries=30):
    cache_key_in = "numbers_processing"
    cache_key_done = "numbers_processed"
    cache_key_out = "final_numbers_produced"

    payload_hash_in = hash_payload(msg.value)

    left = 1
    while left < retries + 1:
        try:
            lock_in = redis.lock(
                f"lock:{cache_key_in}", blocking=True, blocking_timeout=40
            )
            lock_out = redis.lock(f"lock:{cache_key_out}", timeout=40)

            with lock_in:
                print("validating incoming message against cache...")

                if redis.sismember(cache_key_done, payload_hash_in):
                    print(f"message #{msg.offset} already consumed, skipping.")
                    return True

                if redis.sismember(cache_key_in, payload_hash_in):
                    raise MessageInProcess(msg)

                redis.sadd(cache_key_in, payload_hash_in)

            data = update_payload(msg)
            payload_hash_out = hash_payload(data)

            with lock_out:
                print("validating outgoing message against cache...")

                if redis.sismember(cache_key_out, payload_hash_out):
                    print(f"message #{msg.offset} already produced, skipping.")
                    return True

                produce_message(data, left, msg, retries)

                consumer.commit()

            with redis.pipeline() as pipe:
                pipe.watch(cache_key_in, cache_key_out, cache_key_done)
                pipe.multi()

                pipe.sadd(cache_key_out, payload_hash_out)
                pipe.sadd(cache_key_done, payload_hash_in)
                pipe.srem(cache_key_in, payload_hash_in)

                pipe.execute()

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

        print(f"Retry {left}/{retries}...")
        left += 1
        time.sleep(1)

    print(f"Failed to process message #{msg.offset}")


def main():
    while msg := next(consumer):
        try_process_message(msg)


if __name__ == "__main__":
    main()
