import click

from .run import main

app_name = "terminal"


@click.option('-m', '--monitor', is_flag=True)
def app_cli(monitor=False):
    main(monitor=monitor)
