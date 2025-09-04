import json
from unittest.mock import MagicMock

import pytest
from confluent_kafka import KafkaError

from kbeastpy import KBeastClient


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
                MagicMock(
                    key=lambda: b"config:/Accelerator/alarm1",
                    value=lambda: json.dumps({"description": "Alarm 1"}).encode(
                        "utf-8"
                    ),
                    error=lambda: None,
                ),
                MagicMock(
                    key=lambda: b"config:/Accelerator/alarm2",
                    value=lambda: json.dumps({"description": "Alarm 2"}).encode(
                        "utf-8"
                    ),
                    error=lambda: None,
                ),
                MagicMock(
                    key=lambda: b"config:/Accelerator/alarm2",
                    value=lambda: json.dumps({"delete": "Deleting"}).encode("utf-8"),
                    error=lambda: None,
                ),
                # EOF messages for 2 partitions
                MagicMock(
                    error=lambda: MagicMock(code=lambda: KafkaError._PARTITION_EOF)
                ),
                MagicMock(
                    error=lambda: MagicMock(code=lambda: KafkaError._PARTITION_EOF)
                ),
            ],
            {"alarm1": {"description": "Alarm 1"}},
        ),
        (
            [
                MagicMock(
                    key=lambda: b"config:/Accelerator/Group1",
                    value=lambda: json.dumps({"user": "root", "host": "test"}).encode(
                        "utf-8"
                    ),
                    error=lambda: None,
                ),
                MagicMock(
                    key=lambda: b"config:/Accelerator/Group1/alarm1",
                    value=lambda: json.dumps({"description": "Alarm 1"}).encode(
                        "utf-8"
                    ),
                    error=lambda: None,
                ),
                # EOF messages for 2 partitions
                MagicMock(
                    error=lambda: MagicMock(code=lambda: KafkaError._PARTITION_EOF)
                ),
                MagicMock(
                    error=lambda: MagicMock(code=lambda: KafkaError._PARTITION_EOF)
                ),
            ],
            {"Group1": {"alarm1": {"description": "Alarm 1"}}},
        ),
    ],
    indirect=["mock_consumer"],
)
def test_fetch_alarm_list(mock_consumer, expected):
    client = KBeastClient()
    result = client.fetch_alarm_list()

    assert result == expected
