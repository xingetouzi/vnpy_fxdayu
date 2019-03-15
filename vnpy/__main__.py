import sys
import os
import click
import pathlib
import importlib

os.environ['FOR_DISABLE_CONSOLE_CTRL_HANDLER'] = 'T'

CONTEXT_SETTINGS = {'default_map': {'run': {}}}


class VnCliRun(click.MultiCommand):
    def __init__(self, *args, **kwargs):
        super(VnCliRun, self).__init__(*args, **kwargs)
        self._applications = {}
        self.load_application()

    def load_application(self):
        prefix = "vnpy.applications"
        dirpath = pathlib.Path(__file__).absolute().parent / "applications"
        for name in os.listdir(dirpath):
            if name != "__init__.py" and (dirpath / name).is_dir():
                pkg_name = name[:-3] if name.endswith(".py") else name
                mod = importlib.import_module(".".join([prefix, name]))
                app_name = getattr(mod, "app_name", None)
                app_cli = getattr(mod, "app_cli", None)
                if app_name and app_cli and callable(app_cli):
                    if not isinstance(app_cli, click.Command):
                        app_cli = click.command(name=app_name)(app_cli)
                    self._applications[app_name] = app_cli

    def list_commands(self, ctx):
        lst = list(self._applications.keys())
        lst.sort()
        return lst

    def get_command(self, ctx, name):
        return self._applications[name]


def entry_point():
    cli(obj={})


@click.group(context_settings=CONTEXT_SETTINGS)
@click.option('-v', '--verbose', count=True)
@click.help_option('-h', '--help')
@click.pass_context
def cli(ctx, verbose):
    ctx.obj["VERBOSE"] = verbose


@cli.command(cls=VnCliRun)
def run():
    sys.path.append(os.path.abspath(os.getcwd()))


@cli.command()
def version():
    from vnpy import __version__
    print(__version__)