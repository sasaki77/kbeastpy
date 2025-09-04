import json
import uuid

from confluent_kafka import Consumer, KafkaError


class KBeastClient:
    def __init__(self, topics: str = "Accelerator", server: str = "127.0.0.1:29092"):
        self.topics = topics
        self.server = server

    def fetch_alarm_list(self):
        conf = {
            "bootstrap.servers": self.server,
            "group.id": "Alarm-" + str(uuid.uuid4),
            "auto.offset.reset": "beginning",
            "enable.partition.eof": True,
        }

        consumer = Consumer(conf)
        topic = self.topics

        metadata = consumer.list_topics(topic, timeout=5)

        if topic not in metadata.topics:
            print(f"Topic '{topic}' not found.")
            consumer.close()
            return

        partitions = metadata.topics[topic].partitions

        consumer.subscribe([topic])

        eof_count = 0
        alarm_list = {}

        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    eof_count += 1
                    if len(partitions) <= eof_count:
                        break
                    else:
                        continue
                else:
                    print("Error:", msg.error())
                    continue

            record_key = msg.key()
            record_value = msg.value()

            if record_key is None or record_value is None:
                continue

            key_str = str(record_key.decode("utf-8"))
            if not key_str.startswith("config"):
                continue

            value = json.loads(record_value)
            alarm_list[key_str] = value

            print(f"{key_str}: {value}")

        consumer.close()

        ret_alarm_list = {}
        for key, value in alarm_list.items():
            if "delete" in value:
                continue

            if "description" not in value:
                continue

            key_list = key[8:].split("/")[1:]

            lk = len(key_list)
            current = ret_alarm_list
            for i, k in enumerate(key_list):
                if i == lk - 1:
                    current[k] = value
                if k not in current:
                    current[k] = {}
                    current = current[k]

        return ret_alarm_list
