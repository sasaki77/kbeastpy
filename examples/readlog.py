from datetime import datetime

from kbeastpy.logreader import LogReader, Severity

# Create instance of LogReader
config = "Accelerator"
server = "http://elasticsearch:9200"

reader = LogReader(config=config, server=server)

# Fetch log data from 2025-09-01 to now
now = datetime.now()
start = "2025-09-01 00:00:00.000"
end = now.strftime("%Y-%m-%d %H:%M:%S.000")


data = reader.fetch_log(start=start, end=end)
print(data)

# Fetch log data with filters
now = datetime.now()
start = "2025-09-01 00:00:00.000"
end = now.strftime("%Y-%m-%d %H:%M:%S.000")
systems = ["Group1", "Group2"]
pv = "TEST:.*"
severity: list[Severity] = ["MAJOR", "MINOR"]

data = reader.fetch_log(
    start=start, end=end, systems=systems, pv_pattern=pv, severity_list=severity
)
print(data)
