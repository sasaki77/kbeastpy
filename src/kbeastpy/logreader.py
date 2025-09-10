from typing import Literal, TypedDict

from elasticsearch import Elasticsearch

Severity = Literal["OK", "MAJOR", "MINOR", "UNDEFINED", "INVALID"]


class LogData(TypedDict):
    config: str
    current_message: str
    current_severity: str
    latch: str
    message: str
    message_time: str
    notify: str
    pv: str
    severity: Severity
    time: str
    value: str


class LogReader(object):
    def __init__(
        self, config: str = "Accelerator", server: str = "http://localhost:9200"
    ):
        self.server = server
        self.config = config
        self.config_lower = config.lower()

    def fetch_log(
        self,
        start: str,
        end: str,
        systems: list[str] | None = None,
        pv_pattern: str | None = None,
        severity_list: list[Severity] | None = None,
    ) -> list[LogData]:
        es = Elasticsearch(self.server)

        index_name = f"{self.config_lower}_alarms_state_*"

        # Create PIT
        pit_response = es.open_point_in_time(index=index_name, keep_alive="1m")
        pit_id = pit_response["id"]

        query = self._create_query(start, end, systems, pv_pattern, severity_list)
        sort = [{"time": "asc"}]

        all_hits = []
        search_after = None

        while True:
            response = es.search(
                index=index_name,
                query=query,
                sort=sort,
                search_after=search_after,
                size=1000,
            )
            hits = response["hits"]["hits"]

            if not hits:
                break

            all_hits.extend(hits)
            search_after = hits[-1]["sort"]

        # close PIT
        es.close_point_in_time(id=pit_id)

        data = [doc["_source"] for doc in all_hits]
        return data

    def _create_query(
        self,
        start: str,
        end: str,
        systems: list[str] | None = None,
        pv_pattern: str | None = None,
        severity_list: list[Severity] | None = None,
    ) -> dict:
        must = []

        if systems:
            config_should = [
                {"prefix": {"config": f"state:/{self.config}/{prefix}"}}
                for prefix in systems
            ]
            must.append({"bool": {"should": config_should, "minimum_should_match": 1}})

        if pv_pattern:
            must.append({"regexp": {"pv": pv_pattern}})

        filter = []

        if severity_list:
            filter.append({"terms": {"severity": severity_list}})

        time_range = {}
        if start:
            time_range["gte"] = start
        if end:
            time_range["lte"] = end
        if time_range:
            filter.append({"range": {"time": time_range}})

        query = {
            "bool": {
                "must": must,
                "filter": filter,
            },
        }

        return query
