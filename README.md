# kbeastpy

kbeastpy is a Python interface for the CS-Studio (Phoebus) alarm system based on Kafka. It enables developers and operators to interact with alarm messages via Kafka, providing tools for monitoring and managing alarms. It also supports retrieving historical alarm logs stored in Elasticsearch.

## Features

- Connects to Kafka topics used by the alarm system
- Consumes and parses alarm messages
- Produces alarm configuration messages
- Retrieves historical alarm logs from Elasticsearch
- Provides Python API and Command-line interface (CLI)

## Usage

### Python API

Here is an example of how to use the Python API.

#### KBeastClient

```python
import time

from kbeastpy.kbeast import AlarmConfig, KBeastClient
from kbeastpy.msg import Msg, MsgFormat

# Create instance of KBeastClient
config = "Accelerator"
server = "kafka:29092"

client = KBeastClient(config, server)

# Update configuration of alarm tree
configs: list[AlarmConfig] = [
    AlarmConfig(
        path="TEST:PV1",
        config={
            "user": "sasaki",
            "host": "host",
            "description": "Alarm 1",
        },
    ),
    AlarmConfig(
        path="Group1",
        config={
            "user": "sasaki",
            "host": "host",
        },
    ),
    AlarmConfig(
        path="Group1/Group1-1",
        config={
            "user": "sasaki",
            "host": "host",
        },
    ),
    AlarmConfig(
        path="Group1/Group1-1/TEST:PV2",
        config={
            "user": "sasaki",
            "host": "host",
            "description": "Alarm 2",
        },
    ),
]
client.update_alarm_config(configs)

# Show current configuration
config_tree = client.fetch_alarm_list()
print(config_tree)


# Start message listener from latest offset
# Set earliest to offset to listen from the begining
def cb(msg_fmt: MsgFormat, key: str, value: Msg):
    print(f"{msg_fmt} {key}: {value}")


client.start_listener(cb=cb, offset="latest", primary=True, command=False, talk=False)
time.sleep(5)

# Stop listener
client.stop_listener()
time.sleep(2)

# Delete PV
delete_list = [
    "Group1",
    "Group1/Group1-1",
    "Group1/Group1-1/TEST:PV2",
]
client.delete(paths=delete_list, user="sasaki", host="host")

# Show current configuration
config_tree = client.fetch_alarm_list()
print(config_tree)
```

#### LogReader

```python
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
```

### CLI

`kbeastpy` provides a CLI for interacting with the alarm system and querying alarm logs from Elasticsearch.

#### Show alarm tree configuration

List alarms from the specified Kafka topic.

```bash
kbeastpy list --config Accelerator --server 127.0.0.1:29092 --pretty
```

You can filter by system names and enabled status.

```bash
kbeastpy list --config Accelerator --server 127.0.0.1:29092 --pretty \
--systems "Group1,Group2/Group2-1" --enabled True
```

#### Update alarm configuration

Update the configuration of a specific alarm.

```bash
kbeastpy update --config Accelerator --server 127.0.0.1:29092 \
  --path "DEV:ALARM1" --user "operator" --host "host1" \
  --desc "ALARM1 description" --enabled True
```

#### Delete an alarm

Delete an alarm configuration.

```bash
kbeastpy delete --config Accelerator --server 127.0.0.1:29092 \
  --path "DEV:ALARM1" --user "operator" --host "host1"
```

#### Listen to alarm messages

Start a Kafka listener for alarm messages. You can choose which topics to subscribe to (`primary`, `command`, `talk`) and select whether to start from the latest offset or the beginning.

```bash
kbeastpy listen --config Accelerator --server 127.0.0.1:29092 \
  --primary yes --command no --talk yes --latest
```

#### Query alarm logs from Elasticsearch

Query historical alarm logs stored in Elasticsearch. You can filter by system names, PV name pattern (regex), severity, and time range.

```bash
kbeastpy log --config Accelerator --server http://127.0.0.1:9200 \
  --start "2025-09-01 00:00:00" --end "2025-09-09 23:59:59" \
  --systems "Group1,Group2/Group2-1" --pv "DEV:.*" --severity "MAJOR,MINOR" --pretty
```

## Development

This project supports development using Development Containers (devcontainers). The development environment is defined using `docker-compose`, which includes:

- A Python environment with uv
- A Phoebus alarm server
- A test IOC

To set up the development environment:

1. Open the project in a devcontainer-compatible editor (e.g., VS Code with Remote Containers).
2. Run the following command inside the container to install required packages:

   ```bash
   uv sync
   ```

This will install all dependencies defined in `pyproject.toml`.

## Testing

This project supports testing with both pytest and tox.

### Run tests directly with `pytest`:

```bash
pytest tests/
```

### Run tests across multiple environments using `tox`:

```bash
tox
```

## License

This project is licensed under the MIT License.
