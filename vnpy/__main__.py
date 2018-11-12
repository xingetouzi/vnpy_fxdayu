import sys
import os
import click

CONTEXT_SETTINGS = {
    'default_map': {
        'run': {
        }
    }
}

def entry_point():
    cli(obj={})

@click.group(context_settings=CONTEXT_SETTINGS)
@click.option('-v', '--verbose', count=True)
@click.help_option('-h', '--help')
@click.pass_context
def cli(ctx, verbose):
    ctx.obj["VERBOSE"] = verbose

@cli.command()
@click.option("--crypto", is_flag=True)
def run(crypto=False):
    from vnpy.trader.run import main
    from vnpy.trader.run_crypto import main as main_crypto
    sys.path.append(os.path.abspath(os.getcwd()))
    if crypto:
        main_crypto()
    else:
        main()


