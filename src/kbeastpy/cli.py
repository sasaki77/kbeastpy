import time
from datetime import datetime
from pprint import pprint

import click

from kbeastpy import KBeastClient, LogReader, OffsetType
from kbeastpy.msg import Msg, MsgFormat


@click.group()
@click.version_option(package_name="kbeastpy", message="%(prog)s %(version)s")
def cli():
    pass


@cli.command()
@click.option("--config", "-c", type=str, default="Accelerator", help="Alarm topic")
@click.option(
    "--server", "-s", type=str, default="127.0.0.1:29092", help="IP for Kafka server"
)
def list(config, server):
    c = KBeastClient(config=config, server=server)
    alarms = c.fetch_alarm_list()
    click.echo(alarms)


@cli.command()
@click.option("--config", "-c", type=str, default="Accelerator", help="Alarm config")
@click.option(
    "--server", "-s", type=str, default="127.0.0.1:29092", help="IP for Kafka server"
)
@click.option("--primary", "-p", type=bool, default=True, help="Enale primary topic")
@click.option("--command", "-m", type=bool, default=False, help="Enale command topic")
@click.option("--talk", "-t", type=bool, default=False, help="Enale talk topic")
@click.option("--latest", "-l", type=bool, default=False, help="Set offset latest")
def listen(config, server, primary, command, talk, latest):
    c = KBeastClient(config=config, server=server)
    offset = OffsetType.LATEST if latest else OffsetType.EARLIEST
    c.start_listner(cb=cb, offset=offset, primary=primary, command=command, talk=talk)
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        exit()


cb_prefix = {
    MsgFormat.CONFIG_LEAF: "config",
    MsgFormat.CONFIG_NODE: "config",
    MsgFormat.CONFIG_NONE: "config",
    MsgFormat.DELETE: "delete",
    MsgFormat.STATE_LEAF: "state",
    MsgFormat.STATE_NODE: "state",
    MsgFormat.COMMAND: "command",
    MsgFormat.TALK: "talk",
}


def cb(msg_fmt: MsgFormat, key: str, value: Msg):
    t = cb_prefix.get(msg_fmt, None)

    click.echo(f"{t} -> {key}: {value}")


@cli.command()
@click.option("--config", "-c", type=str, default="Accelerator", help="Alarm topic")
@click.option(
    "--server", "-s", type=str, default="127.0.0.1:29092", help="IP for Kafka server"
)
@click.option("--start", "-t", type=str, help="Start of search range", required=True)
@click.option("--end", "-e", type=str, help="End of search range")
@click.option("--systems", "-g", type=str, default=None, help="Systems")
@click.option("--pv", "-p", type=str, default=None, help="PV name pattern")
@click.option("--severity", "-r", type=str, default=None, help="Severity list")
def log(config, server, start, end, systems, pv, severity):
    _end = end
    if _end is None:
        now = datetime.now()
        _end = now.strftime("%Y-%m-%d %H:%M:%S.000")

    system_list = systems.split(",") if systems else None
    severity_list = severity.split(",") if severity else None
    r = LogReader(config=config, server=server)
    data = r.fetch_log(start, _end, system_list, pv, severity_list)

    pprint(data)


if __name__ == "__main__":
    cli()
