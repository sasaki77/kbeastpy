import time

import pytest
from utils import config_leaf_elem, config_node_elem, delete_elem, eof_message

from kbeastpy import KBeastClient
from kbeastpy.msg import ConfigStateMsg, MsgFormat


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
                    b"config:/Accelerator/alarm1",
                    {"user": "root", "host": "test", "description": "Alarm 1"},
                ),
                config_leaf_elem(
                    b"config:/Accelerator/alarm2",
                    {"user": "root", "host": "test", "description": "Alarm 2"},
                ),
                delete_elem(b"config:/Accelerator/alarm2"),
                # EOF messages for 2 partitions
                eof_message(),
                eof_message(),
            ],
            {"alarm1": {"user": "root", "host": "test", "description": "Alarm 1"}},
        ),
        (
            [
                config_node_elem(b"config:/Accelerator/Group2"),
                config_leaf_elem(
                    b"config:/Accelerator/Group1/alarm1",
                    {"user": "root", "host": "test", "description": "Alarm 1"},
                ),
                config_node_elem(b"config:/Accelerator/Group1"),
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
                    b"config:/Accelerator/alarm1",
                    {"user": "root", "host": "test", "description": "Alarm 1"},
                ),
                config_node_elem(b"config:/Accelerator/Group1"),
                config_leaf_elem(
                    b"config:/Accelerator/Group1/alarm2",
                    {"user": "root", "host": "test", "description": "Alarm 2"},
                ),
                delete_elem(b"config:/Accelerator/alarm2"),
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
                    "msg_fmt": MsgFormat.DELETE,
                    "key": "Accelerator/alarm2",
                    "value": {"user": "root", "host": "test", "delete": "Deleting"},
                },
            ],
        ),
    ],
    indirect=["mock_consumer"],
)
def test_listener(mock_consumer, expected):
    data = []

    def cb(msg_fmt: MsgFormat, key: str, value: ConfigStateMsg):
        data.append((msg_fmt, key, value))

    client = KBeastClient()
    client.start_listner(cb=cb)
    time.sleep(1)

    assert len(expected) == len(data)
    for d, e in zip(data, expected):
        assert d[0] == e["msg_fmt"]
        assert d[1] == e["key"]
        assert d[2] == e["value"]
