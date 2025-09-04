import json
import uuid

from confluent_kafka import Consumer, KafkaError

from kbeastpy.msg import ConfigMsg


class KBeastClient:
    def __init__(self, topics: str = "Accelerator", server: str = "127.0.0.1:29092"):
        self.topics = topics
        self.server = server

    def fetch_alarm_list(self) -> dict:
        consumer = self._create_consumer()
        topic = self.topics

        metadata = consumer.list_topics(topic, timeout=5)
        if topic not in metadata.topics:
            print(f"Topic '{topic}' not found.")
            consumer.close()
            return {}

        partitions = metadata.topics[topic].partitions
        consumer.subscribe([topic])

        alarm_list = {}
        eof_count = 0

        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    eof_count += 1
                    if eof_count >= len(partitions):
                        break
                    continue
                else:
                    print("Error:", msg.error())
                    continue

            if not self._is_valid_message(msg):
                continue

            key, value = self._parse_key_value(msg)
            if key and value:
                alarm_list[key] = value
                print(f"{key}: {value}")

        consumer.close()
        return self._build_nested_dict(alarm_list)

    def _create_consumer(self):
        return Consumer(
            {
                "bootstrap.servers": self.server,
                "group.id": f"Alarm-{uuid.uuid4()}",
                "auto.offset.reset": "beginning",
                "enable.partition.eof": True,
            }
        )

    def _is_valid_message(self, msg) -> bool:
        if msg is None or msg.error():
            return False
        return msg.key() is not None and msg.value() is not None

    def _parse_key_value(self, msg) -> tuple[str | None, ConfigMsg]:
        key = msg.key().decode("utf-8")
        if not key.startswith("config"):
            return None, None
        value = json.loads(msg.value())
        return key, value

    def _build_nested_dict(self, alarm_list: dict[str, ConfigMsg]) -> dict:
        result = {}
        for key, value in alarm_list.items():
            if value is None:
                continue

            if "delete" in value:
                continue

            keys = key[8:].split("/")[1:]
            current = result
            for i, k in enumerate(keys):
                if i == len(keys) - 1:
                    if "description" in value:
                        current[k] = value
                        continue
                    if k not in current:
                        current[k] = {}
                else:
                    current = current.setdefault(k, {})
        return result
