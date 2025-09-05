import json
from unittest.mock import MagicMock

from confluent_kafka import KafkaError

from kbeastpy.msg import ConfigLeafMsg, StateLeafMsg

CONFIG = "Accelerator"
COMMAND = "AcceleratorCommand"
TALK = "AcceleratorTalk"


def config_leaf_elem(key: str, value: ConfigLeafMsg) -> MagicMock:
    return MagicMock(
        key=lambda: f"config:/{key}".encode("utf-8"),
        value=lambda: json.dumps(value).encode("utf-8"),
        topic=lambda: CONFIG,
        error=lambda: None,
    )


def config_node_elem(key: str) -> MagicMock:
    return MagicMock(
        key=lambda: f"config:/{key}".encode("utf-8"),
        value=lambda: json.dumps({"user": "root", "host": "test"}).encode("utf-8"),
        topic=lambda: CONFIG,
        error=lambda: None,
    )


def state_leaf_elem(key: str, value: StateLeafMsg) -> MagicMock:
    return MagicMock(
        key=lambda: f"state:/{key}".encode("utf-8"),
        value=lambda: json.dumps(value).encode("utf-8"),
        topic=lambda: CONFIG,
        error=lambda: None,
    )


def state_node_elem(key: str, severity: str = "MAJOR") -> MagicMock:
    return MagicMock(
        key=lambda: f"state:/{key}".encode("utf-8"),
        value=lambda: json.dumps({"severity": severity}).encode("utf-8"),
        topic=lambda: CONFIG,
        error=lambda: None,
    )


def delete_elem(key: str) -> MagicMock:
    return MagicMock(
        key=lambda: f"config:/{key}".encode("utf-8"),
        value=lambda: json.dumps(
            {"user": "root", "host": "test", "delete": "Deleting"}
        ).encode("utf-8"),
        topic=lambda: CONFIG,
        error=lambda: None,
    )


def talk_elem(key: str, severity: str = "MAJOR", talk: str = "talk") -> MagicMock:
    return MagicMock(
        key=lambda: f"talk:/{key}".encode("utf-8"),
        value=lambda: json.dumps(
            {"standout": False, "severity": severity, "talk": talk}
        ).encode("utf-8"),
        topic=lambda: TALK,
        error=lambda: None,
    )


def command_elem(key: str) -> MagicMock:
    return MagicMock(
        key=lambda: f"command:/{key}".encode("utf-8"),
        value=lambda: json.dumps(
            {"user": "user name", "host": "host name", "command": "acknowledge"}
        ).encode("utf-8"),
        topic=lambda: COMMAND,
        error=lambda: None,
    )


def eof_message() -> MagicMock:
    return MagicMock(error=lambda: MagicMock(code=lambda: KafkaError._PARTITION_EOF))
