import time

import click

from kbeastpy import KBeastClient
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
@click.option("--command", "-c", type=bool, default=False, help="Enale command topic")
@click.option("--talk", "-t", type=bool, default=False, help="Enale talk topic")
def listen(config, server, primary, command, talk):
    c = KBeastClient(config=config, server=server)
    c.start_listner(cb=cb, primary=primary, command=command, talk=talk)
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


if __name__ == "__main__":
    cli()
