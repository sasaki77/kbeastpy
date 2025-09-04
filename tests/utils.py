import json
from unittest.mock import MagicMock

from confluent_kafka import KafkaError

from kbeastpy.msg import ConfigLeafMsg


def config_leaf_elem(key: bytes, value: ConfigLeafMsg) -> MagicMock:
    return MagicMock(
        key=lambda: key,
        value=lambda: json.dumps(value).encode("utf-8"),
        error=lambda: None,
    )


def config_node_elem(key: bytes) -> MagicMock:
    return MagicMock(
        key=lambda: key,
        value=lambda: json.dumps({"user": "root", "host": "test"}).encode("utf-8"),
        error=lambda: None,
    )


def delete_elem(key: bytes) -> MagicMock:
    return MagicMock(
        key=lambda: key,
        value=lambda: json.dumps(
            {"user": "root", "host": "test", "delete": "Deleting"}
        ).encode("utf-8"),
        error=lambda: None,
    )


def eof_message() -> MagicMock:
    return MagicMock(error=lambda: MagicMock(code=lambda: KafkaError._PARTITION_EOF))
