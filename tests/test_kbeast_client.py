import json
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
from kbeastpy.kbeast import AlarmConfigArg
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


@pytest.fixture
def mock_producer(mocker):
    # Mock for Consumer class
    mock = mocker.MagicMock()

    mock.produce.return_value = None
    mock.flush.return_value = None

    # Mock for poll
    mock.poll.side_effect = None

    return (mocker.patch("kbeastpy.kbeast.Producer", return_value=mock), mock)


def test_update(mock_producer):
    _, mock = mock_producer
    configs: list[AlarmConfigArg] = [
        {
            "path": "PV1",
            "data": {
                "user": "user",
                "host": "host",
                "description": "desc",
                "enabled": True,
            },
        },
        {
            "path": "Group1",
            "data": {
                "user": "user",
                "host": "host",
            },
        },
    ]

    client = KBeastClient()
    client.update_alarm_config(configs)

    assert mock.produce.call_count == 2

    calls = mock.produce.call_args_list

    call1_args = calls[0][0]
    call1_kargs = calls[0][1]
    assert call1_args == ("Accelerator",)
    assert call1_kargs["key"] == "config:/Accelerator/PV1"
    assert call1_kargs["value"] == json.dumps(configs[0]["data"])

    call2_args = calls[1][0]
    call2_kargs = calls[1][1]
    assert call2_args == ("Accelerator",)
    assert call2_kargs["key"] == "config:/Accelerator/Group1"
    assert call2_kargs["value"] == json.dumps(configs[1]["data"])


def test_delete(mock_producer):
    _, mock = mock_producer
    user = "user"
    host = "host"
    paths = ["PV1", "PV2"]

    client = KBeastClient()
    client.delete(paths, user, host)

    assert mock.produce.call_count == 4

    calls = mock.produce.call_args_list

    # delete PV1
    ## 1st delete message for PV1
    call1_args = calls[0][0]
    call1_kargs = calls[0][1]
    assert call1_args == ("Accelerator",)
    assert call1_kargs["key"] == "config:/Accelerator/PV1"
    assert call1_kargs["value"] == json.dumps(
        {"user": "user", "host": "host", "delete": "Deleting"}
    )

    ## 2nd delete message for PV1
    call2_args = calls[1][0]
    call2_kargs = calls[1][1]
    assert call2_args == ("Accelerator",)
    assert call2_kargs["key"] == "config:/Accelerator/PV1"
    assert call2_kargs["value"] is None

    # delete PV2
    ## 1st delete message for PV2
    call3_args = calls[2][0]
    call3_kargs = calls[2][1]
    assert call3_args == ("Accelerator",)
    assert call3_kargs["key"] == "config:/Accelerator/PV2"
    assert call3_kargs["value"] == json.dumps(
        {"user": "user", "host": "host", "delete": "Deleting"}
    )

    ## 2nd delete message for PV2
    call2_args = calls[1][0]
    call4_args = calls[3][0]
    call4_kargs = calls[3][1]
    assert call4_args == ("Accelerator",)
    assert call4_kargs["key"] == "config:/Accelerator/PV2"
    assert call4_kargs["value"] is None


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
                config_node_elem("Accelerator/Group1/Group1-1"),
                config_leaf_elem(
                    "Accelerator/Group1/Group1-1/alarm2",
                    {"user": "root", "host": "test", "description": "Alarm 2"},
                ),
                # EOF messages for 2 partitions
                eof_message(),
                eof_message(),
            ],
            {
                "Group1": {
                    "user": "root",
                    "host": "test",
                    "childs": {
                        "alarm1": {
                            "user": "root",
                            "host": "test",
                            "description": "Alarm 1",
                        },
                        "Group1-1": {
                            "user": "root",
                            "host": "test",
                            "childs": {
                                "alarm2": {
                                    "user": "root",
                                    "host": "test",
                                    "description": "Alarm 2",
                                },
                            },
                        },
                    },
                },
                "Group2": {"user": "root", "host": "test", "childs": {}},
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
