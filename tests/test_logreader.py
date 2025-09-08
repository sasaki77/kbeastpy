import pytest

from kbeastpy import LogReader


@pytest.fixture
def es_mock(mocker):
    mock = mocker.MagicMock()

    # open_point_in_time
    mock.open_point_in_time.return_value = {"id": "mock_pit_id"}

    # close_point_in_time
    mock.close_point_in_time.return_value = {"succeeded": True}

    # search
    mock.search.side_effect = [
        {
            "hits": {
                "hits": [
                    {"_source": {"message": "foo"}, "sort": [1]},
                    {"_source": {"message": "bar"}, "sort": [2]},
                ]
            }
        },
        {"hits": {"hits": [{"_source": {"message": "foobar"}, "sort": [3]}]}},
        {"hits": {"hits": []}},
    ]

    return (mocker.patch("kbeastpy.logreader.Elasticsearch", return_value=mock), mock)


def test_logreader(es_mock):
    _, mock = es_mock
    r = LogReader()
    data = r.fetch_log(
        start="2025-09-01 00:00:00.000000", end="2025-09-01 23:59:59.999"
    )
    assert data == [{"message": "foo"}, {"message": "bar"}, {"message": "foobar"}]

    assert mock.search.call_count == 3

    mock.open_point_in_time.assert_called_once_with(
        index="accelerator_alarms_state_*", keep_alive="1m"
    )
    mock.close_point_in_time.assert_called_once_with(id="mock_pit_id")


@pytest.mark.parametrize(
    "start, end, systems, pv_pattern, severity_list, expected_must_len, expected_filter_len",
    [
        # Full Condition
        (
            "2025-09-01 00:00:00.000",
            "2025-09-05 23:59:59.999",
            ["sysA", "sysB"],
            "PV.*123",
            ["MAJOR", "MINOR"],
            2,  # must: systems + pv_pattern
            2,  # filter: severity + time range
        ),
        # Only start and end
        ("2025-09-01 00:00:00.000", "2025-09-02 00:00:00.000", None, None, None, 0, 1),
        # Only severity
        ("", "", None, None, ["MAJOR"], 0, 1),
        # Only systems
        ("", "", ["sysA"], None, None, 1, 0),
    ],
)
def test_create_query(
    start,
    end,
    systems,
    pv_pattern,
    severity_list,
    expected_must_len,
    expected_filter_len,
):
    es = LogReader(config="Test_config")
    query = es._create_query(
        start=start,
        end=end,
        systems=systems,
        pv_pattern=pv_pattern,
        severity_list=severity_list,
    )

    assert len(query["bool"]["must"]) == expected_must_len
    assert len(query["bool"]["filter"]) == expected_filter_len

    if systems:
        should = query["bool"]["must"][0]["bool"]["should"]
        for sys in systems:
            assert {"prefix": {"config": f"state:/Test_config/{sys}"}} in should

    if pv_pattern:
        assert {"regexp": {"pv": pv_pattern}} in query["bool"]["must"]

    if severity_list:
        assert {"terms": {"severity": severity_list}} in query["bool"]["filter"]

    if start or end:
        time_range = {}
        if start:
            time_range["gte"] = start
        if end:
            time_range["lte"] = end
        assert {"range": {"time": time_range}} in query["bool"]["filter"]
