import time

from kbeastpy.kbeast import AlarmConfigArg, KBeastClient
from kbeastpy.msg import Msg, MsgFormat

# Create instance of KBeastClient
config = "Accelerator"
server = "kafka:29092"

client = KBeastClient(config, server)

# Update configuration of alarm tree
configs: list[AlarmConfigArg] = [
    {
        "path": "TEST:PV1",
        "data": {
            "user": "sasaki",
            "host": "host",
            "description": "Alarm 1",
        },
    },
    {
        "path": "Group1",
        "data": {
            "user": "sasaki",
            "host": "host",
        },
    },
    {
        "path": "Group1/Group1-1",
        "data": {
            "user": "sasaki",
            "host": "host",
        },
    },
    {
        "path": "Group1/Group1-1/TEST:PV2",
        "data": {
            "user": "sasaki",
            "host": "host",
            "description": "Alarm 2",
        },
    },
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
