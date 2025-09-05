import time

import pytest
from utils import (
    command_elem,
    config_leaf_elem,
    config_node_elem,
    delete_elem,
    eof_message,
    state_leaf_elem,
    state_node_elem,
    talk_elem,
)

from kbeastpy import KBeastClient
from kbeastpy.msg import Msg, MsgFormat


@pytest.fixture
def mock_consumer(mocker, request):
    # Mock for Consumer class
    mock_consumer_instance = mocker.MagicMock()

    # Mock for list_topics with 2 partitions
    mock_metadata = mocker.MagicMock()
    mock_metadata.topics = {
        "Accelerator": mocker.MagicMock(partitions={0: None, 1: None})
    }
    mock_consumer_instance.list_topics.return_value = mock_metadata

    # Mock for messages within poll
    messages = request.param
    mock_consumer_instance.poll.side_effect = messages + [None] * 5

    return mocker.patch("kbeastpy.kbeast.Consumer", return_value=mock_consumer_instance)


@pytest.mark.parametrize(
    ("mock_consumer", "expected"),
    [
        (
            [
                config_leaf_elem(
                    "Accelerator/alarm1",
                    {"user": "root", "host": "test", "description": "Alarm 1"},
                ),
                config_leaf_elem(
                    "Accelerator/alarm2",
                    {"user": "root", "host": "test", "description": "Alarm 2"},
                ),
                delete_elem("Accelerator/alarm2"),
                # EOF messages for 2 partitions
                eof_message(),
                eof_message(),
            ],
            {"alarm1": {"user": "root", "host": "test", "description": "Alarm 1"}},
        ),
        (
            [
                config_node_elem("Accelerator/Group2"),
                config_leaf_elem(
                    "Accelerator/Group1/alarm1",
                    {"user": "root", "host": "test", "description": "Alarm 1"},
                ),
                config_node_elem("Accelerator/Group1"),
                # EOF messages for 2 partitions
                eof_message(),
                eof_message(),
            ],
            {
                "Group1": {
                    "alarm1": {
                        "user": "root",
                        "host": "test",
                        "description": "Alarm 1",
                    },
                },
                "Group2": {},
            },
        ),
    ],
    indirect=["mock_consumer"],
)
def test_fetch_alarm_list(mock_consumer, expected):
    client = KBeastClient()
    result = client.fetch_alarm_list()

    assert result == expected


@pytest.mark.parametrize(
    ("mock_consumer", "expected"),
    [
        (
            [
                config_leaf_elem(
                    "Accelerator/alarm1",
                    {"user": "root", "host": "test", "description": "Alarm 1"},
                ),
                config_node_elem("Accelerator/Group1"),
                config_leaf_elem(
                    "Accelerator/Group1/alarm2",
                    {"user": "root", "host": "test", "description": "Alarm 2"},
                ),
                state_leaf_elem(
                    "Accelerator/alarm1",
                    {
                        "severity": "MAJOR",
                        "message": "LOLO_ALARM",
                        "value": "0.0",
                        "time": {"seconds": 1757033473, "nano": 838751329},
                        "current_severity": "MAJOR",
                        "current_message": "LOLO_ALARM",
                    },
                ),
                state_node_elem("Accelerator/Group1", "MAJOR"),
                talk_elem("Accelerator/alarm1", "MAJOR", "Alarm 1"),
                command_elem("Accelerator/Group1/alarm2"),
                delete_elem("Accelerator/Group1/alarm2"),
            ],
            [
                {
                    "msg_fmt": MsgFormat.CONFIG_LEAF,
                    "key": "Accelerator/alarm1",
                    "value": {"user": "root", "host": "test", "description": "Alarm 1"},
                },
                {
                    "msg_fmt": MsgFormat.CONFIG_NODE,
                    "key": "Accelerator/Group1",
                    "value": {"user": "root", "host": "test"},
                },
                {
                    "msg_fmt": MsgFormat.CONFIG_LEAF,
                    "key": "Accelerator/Group1/alarm2",
                    "value": {"user": "root", "host": "test", "description": "Alarm 2"},
                },
                {
                    "msg_fmt": MsgFormat.STATE_LEAF,
                    "key": "Accelerator/alarm1",
                    "value": {
                        "severity": "MAJOR",
                        "message": "LOLO_ALARM",
                        "value": "0.0",
                        "time": {"seconds": 1757033473, "nano": 838751329},
                        "current_severity": "MAJOR",
                        "current_message": "LOLO_ALARM",
                    },
                },
                {
                    "msg_fmt": MsgFormat.STATE_NODE,
                    "key": "Accelerator/Group1",
                    "value": {"severity": "MAJOR"},
                },
                {
                    "msg_fmt": MsgFormat.TALK,
                    "key": "Accelerator/alarm1",
                    "value": {
                        "standout": False,
                        "severity": "MAJOR",
                        "talk": "Alarm 1",
                    },
                },
                {
                    "msg_fmt": MsgFormat.COMMAND,
                    "key": "Accelerator/Group1/alarm2",
                    "value": {
                        "user": "user name",
                        "host": "host name",
                        "command": "acknowledge",
                    },
                },
                {
                    "msg_fmt": MsgFormat.DELETE,
                    "key": "Accelerator/Group1/alarm2",
                    "value": {"user": "root", "host": "test", "delete": "Deleting"},
                },
            ],
        ),
    ],
    indirect=["mock_consumer"],
)
def test_listener(mock_consumer, expected):
    data = []

    def cb(msg_fmt: MsgFormat, key: str, value: Msg):
        data.append((msg_fmt, key, value))

    client = KBeastClient()
    client.start_listner(cb=cb)
    time.sleep(1)

    assert len(expected) == len(data)
    for d, e in zip(data, expected):
        assert d[0] == e["msg_fmt"]
        assert d[1] == e["key"]
        assert d[2] == e["value"]
