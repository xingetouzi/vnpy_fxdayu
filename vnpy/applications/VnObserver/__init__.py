import click
from vnpy.trader.app.ctaStrategy.plugins.ctaMetric.observer import run_observer

app_name="observer"

@click.option('-d', '--dir-path', default=".", type=click.Path(file_okay=False))
@click.help_option('-h', '--help')
def app_cli(dir_path):
    run_observer(dir_path)