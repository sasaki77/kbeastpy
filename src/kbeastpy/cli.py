import time

import click

from kbeastpy import KBeastClient
from kbeastpy.msg import ConfigStateMsg, MsgFormat


@click.group()
@click.version_option(package_name="kbeastpy", message="%(prog)s %(version)s")
def cli():
    pass


@cli.command()
@click.option("--topics", "-t", type=str, default="Accelerator", help="Alarm topic")
@click.option(
    "--server", "-s", type=str, default="127.0.0.1:29092", help="IP for Kafka server"
)
def list(topics, server):
    c = KBeastClient(topics=topics, server=server)
    alarms = c.fetch_alarm_list()
    click.echo(alarms)


@cli.command()
@click.option("--topics", "-t", type=str, default="Accelerator", help="Alarm topic")
@click.option(
    "--server", "-s", type=str, default="127.0.0.1:29092", help="IP for Kafka server"
)
def listen(topics, server):
    c = KBeastClient(topics=topics, server=server)
    c.start_listner(cb=cb)
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
}


def cb(msg_fmt: MsgFormat, key: str, value: ConfigStateMsg):
    t = cb_prefix.get(msg_fmt, None)

    click.echo(f"{t} -> {key}: {value}")


if __name__ == "__main__":
    cli()
