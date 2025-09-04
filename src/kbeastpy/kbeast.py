import json
import threading
import uuid
from typing import Callable

from confluent_kafka import Consumer, KafkaError

from kbeastpy.msg import ConfigMsg, ConfigStateMsg, MsgFormat


class KBeastClient:
    def __init__(self, config: str = "Accelerator", server: str = "127.0.0.1:29092"):
        self.config = config
        self.server = server

    def start_listner(self, cb: Callable[[MsgFormat, str, ConfigStateMsg], None]):
        thread = threading.Thread(target=self._listen, daemon=True, args=(cb,))
        thread.start()

    def _listen(self, cb: Callable[[MsgFormat, str, ConfigStateMsg], None]):
        consumer = self._create_consumer(enable_eof=False)
        topic = self.config

        metadata = consumer.list_topics(topic, timeout=5)
        if topic not in metadata.topics:
            print(f"Topic '{topic}' not found.")
            consumer.close()
            return {}

        consumer.subscribe([topic])

        try:
            while True:
                msg = consumer.poll(timeout=1.0)

                if not self._is_valid_message(msg):
                    continue

                key, value = self._parse_key_value(msg)

                if key is None:
                    continue

                msg_fmt = self._analyze_msg_format(key, value)

                if msg_fmt is None:
                    continue

                if msg_fmt == MsgFormat.STATE_LEAF or msg_fmt == MsgFormat.STATE_NODE:
                    key = key[7:]
                else:
                    key = key[8:]

                cb(msg_fmt, key, value)
        except StopIteration:
            # For mock test
            pass

    def fetch_alarm_list(self) -> dict:
        consumer = self._create_consumer(enable_eof=True)
        topic = self.config

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

            if key is None:
                continue

            if not key.startswith("config"):
                continue

            if key and value:
                alarm_list[key] = value

        consumer.close()
        return self._build_nested_dict(alarm_list)

    def _create_consumer(self, enable_eof: bool):
        return Consumer(
            {
                "bootstrap.servers": self.server,
                "group.id": f"Alarm-{uuid.uuid4()}",
                "auto.offset.reset": "beginning",
                "enable.partition.eof": enable_eof,
            }
        )

    def _is_valid_message(self, msg) -> bool:
        if msg is None or msg.error():
            return False
        return msg.key() is not None and msg.value() is not None

    def _parse_key_value(self, msg) -> tuple[str | None, ConfigMsg]:
        key = msg.key().decode("utf-8")
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

    def _analyze_msg_format(self, key: str, value: ConfigStateMsg) -> MsgFormat | None:
        if key.startswith("state"):
            if value is None:
                return None
            if "message" in value:
                return MsgFormat.STATE_LEAF
            return MsgFormat.STATE_NODE

        if key.startswith("config"):
            if value is None:
                return MsgFormat.CONFIG_NONE
            if "delete" in value:
                return MsgFormat.DELETE
            if "description" in value:
                return MsgFormat.CONFIG_LEAF
            return MsgFormat.CONFIG_NODE
