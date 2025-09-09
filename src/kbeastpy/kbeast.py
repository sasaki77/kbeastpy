import json
import threading
import uuid
from functools import lru_cache
from typing import Callable, Literal, TypedDict, Union

from confluent_kafka import Consumer, KafkaError, Producer

from kbeastpy.msg import (
    ConfigLeafMsg,
    ConfigMsg,
    ConfigNodeMsg,
    DeleteMsg,
    Msg,
    MsgFormat,
)

OffsetType = Literal["earliest", "latest"]


class AlarmConfigArg(TypedDict):
    path: str
    data: Union[ConfigLeafMsg, ConfigNodeMsg]


class KBeastClient:
    def __init__(self, config: str = "Accelerator", server: str = "127.0.0.1:29092"):
        self.config = config
        self.server = server

    def start_listner(
        self,
        cb: Callable[[MsgFormat, str, Msg], None],
        offset: OffsetType = "earliest",
        primary: bool = True,
        command: bool = False,
        talk: bool = False,
    ):
        thread = threading.Thread(
            target=self._listen, daemon=True, args=(cb, offset, primary, command, talk)
        )
        thread.start()

    def _listen(
        self,
        cb: Callable[[MsgFormat, str, Msg], None],
        offset: OffsetType = "earliest",
        primary: bool = True,
        command: bool = False,
        talk: bool = False,
    ):
        consumer = self._create_consumer(enable_eof=False, offset=offset)
        topics = self._get_topic_names(primary, command, talk)

        if len(topics) == 0:
            return

        metadata = consumer.list_topics(timeout=5)
        for topic in topics:
            if topic not in metadata.topics:
                print(f"Topic '{topic}' not found.")
                consumer.close()
                return {}

        consumer.subscribe(topics)

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

                stripped_key = self._strip_type_prefix(key, msg_fmt)

                cb(msg_fmt, stripped_key, value)
        except StopIteration:
            # For mock test
            pass

    def fetch_alarm_list(
        self, systems: list[str] | None = None, enabled: bool | None = None
    ) -> dict:
        consumer = self._create_consumer(enable_eof=True)
        topic = self.config

        metadata = consumer.list_topics(topic, timeout=5)
        if topic not in metadata.topics:
            print(f"Topic '{topic}' not found.")
            consumer.close()
            return {}

        partitions = metadata.topics[topic].partitions
        consumer.subscribe([topic])

        # alama_list keeps the last message for each key
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

        alarm_list_filtered = {}
        for key, value in alarm_list.items():
            if value is None or "delete" in value:
                continue
            if enabled is not None and value.get("enabled", True) != enabled:
                continue

            system_match = False
            if systems is not None:
                for system in systems:
                    if key.startswith(f"config:/{self.config}/{system}"):
                        system_match = True
                        break
                if not system_match:
                    continue

            alarm_list_filtered[key] = value

        return self._build_config_dict(alarm_list_filtered)

    def update_alarm_config(self, configs: list[AlarmConfigArg]):
        producer = self._create_producer()
        topic = self.config

        for config in configs:
            key = f"config:/{self.config}/{config['path']}"
            value = json.dumps(config["data"])

            try:
                producer.produce(
                    topic, key=key, value=value, callback=self._produce_delivery_report
                )
            except BufferError:
                producer.poll(1)  # wait buffer becomes available
                producer.produce(
                    topic, key=key, value=value, callback=self._produce_delivery_report
                )
            producer.poll(0)

        producer.flush()

    def delete(self, paths: list[str], user: str, host: str):
        producer = self._create_producer()
        topic = self.config

        msg: DeleteMsg = {"user": user, "host": host, "delete": "Deleting"}
        value = json.dumps(msg)

        for path in paths:
            key = f"config:/{self.config}/{path}"

            try:
                producer.produce(
                    topic, key=key, value=value, callback=self._produce_delivery_report
                )
            except BufferError:
                producer.poll(1)  # wait buffer becomes available
                producer.produce(
                    topic, key=key, value=value, callback=self._produce_delivery_report
                )
            producer.poll(0)

            try:
                producer.produce(
                    topic, key=key, value=None, callback=self._produce_delivery_report
                )
            except BufferError:
                producer.poll(1)  # wait buffer becomes available
                producer.produce(
                    topic, key=key, value=None, callback=self._produce_delivery_report
                )
            producer.poll(0)

        producer.flush()

    def _produce_delivery_report(self, err, msg):
        if err is not None:
            print(f"Delivery failed: {err}")

    def _create_consumer(self, enable_eof: bool, offset: OffsetType = "earliest"):
        return Consumer(
            {
                "bootstrap.servers": self.server,
                "group.id": f"Alarm-{uuid.uuid4()}",
                "auto.offset.reset": str(offset),
                "enable.partition.eof": enable_eof,
            }
        )

    def _create_producer(self):
        return Producer(
            {
                "bootstrap.servers": self.server,
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

    def _build_config_dict(
        self, alarm_list: dict[str, ConfigLeafMsg | ConfigNodeMsg]
    ) -> dict:
        result = {"childs": {}}
        for key, value in alarm_list.items():
            keys = key[8:].split("/")[1:]
            last = len(keys) - 1
            current = result

            for i, key in enumerate(keys):
                if i != last:
                    current = current["childs"].setdefault(key, {"childs": {}})
                    continue

                # leaf
                if "description" in value:
                    current["childs"][key] = value
                    continue

                # node
                if key not in current["childs"]:
                    current["childs"][key] = {"childs": {}}
                current["childs"][key].update(value)

        return result["childs"]

    def _analyze_msg_format(self, key: str, value: Msg) -> MsgFormat | None:
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

        if key.startswith("command"):
            return MsgFormat.COMMAND

        if key.startswith("talk"):
            return MsgFormat.TALK

        return None

    def _get_topic_names(self, primary: bool, command: bool, talk: bool) -> list[str]:
        names = []

        if primary:
            names.append(self.config)
        if command:
            names.append(f"{self.config}Command")
        if talk:
            names.append(f"{self.config}Talk")

        return names

    def _strip_type_prefix(self, key, msg_fmt):
        length_dit = self._get_prefix_length_dict()
        length = length_dit[msg_fmt]
        return key[length:]

    @lru_cache
    def _get_prefix_length_dict(self):
        config_len = len("config:/")
        delete_len = len("delete:/")
        state_len = len("state:/")
        command_len = len("command:/")
        talk_len = len("talk:/")

        return {
            MsgFormat.CONFIG_LEAF: config_len,
            MsgFormat.CONFIG_NODE: config_len,
            MsgFormat.CONFIG_NONE: config_len,
            MsgFormat.DELETE: delete_len,
            MsgFormat.STATE_LEAF: state_len,
            MsgFormat.STATE_NODE: state_len,
            MsgFormat.COMMAND: command_len,
            MsgFormat.TALK: talk_len,
        }
