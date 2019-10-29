import click

from .run import main

app_name = "terminal"


@click.option('-m', '--monitor', is_flag=True)
@click.option('-k', '--keep', is_flag=True)
def app_cli(monitor=False, keep=False):
    main(monitor=monitor, keep=keep)
