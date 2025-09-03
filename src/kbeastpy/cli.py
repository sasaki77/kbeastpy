import click

from kbeastpy import KBeastClient


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


if __name__ == "__main__":
    cli()
