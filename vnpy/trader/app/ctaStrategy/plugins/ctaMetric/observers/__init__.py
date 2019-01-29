from .log import LogFileMetricObserver
from .sqlite import SqliteMetricObserver
from .utils import run_observer as run_observer_cls

def run_observer(path, url=None, cls=None):
    cls = cls or SqliteMetricObserver
    run_observer_cls(cls, path, url)
