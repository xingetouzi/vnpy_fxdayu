import json
import os
import time
import re
import logging

import requests
from watchdog.observers import Observer
from watchdog.events import RegexMatchingEventHandler
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from vnpy.trader.app.ctaStrategy.plugins.ctaMetric.base import NumpyEncoder
from vnpy.trader.app.ctaStrategy.plugins.ctaMetric.senders.sqlite import OpenFalconMetric as OpenFalconMetricModel
from vnpy.trader.app.ctaStrategy.plugins.ctaMetric.base import OpenFalconMetric
from vnpy.trader.app.ctaStrategy.plugins.ctaMetric.observers.utils import get_open_falcon_url


class HandleMetric(object):
    def __init__(self, filepath):
        self.filepath = filepath
        self._init()

    def _init(self):
        engine = create_engine('sqlite:///%s' % self.filepath)
        self.engine = engine
        Session = sessionmaker()
        Session.configure(bind=engine)
        self.session = Session()

    def has_table(self):
        return self.engine.dialect.has_table(
            self.engine, OpenFalconMetricModel.__tablename__)

    def get_metrics(self):
        if self.has_table():
            metrics = self.session.query(OpenFalconMetricModel).all()
            metrics = [
                OpenFalconMetric.from_dict(metric.__dict__)
                for metric in metrics
            ]
            return metrics
        else:
            return []


class FileEventHandler(RegexMatchingEventHandler):
    def __init__(self, root, reg):
        RegexMatchingEventHandler.__init__(self, regexes=[reg])
        self.handlers = {}
        self.df_total = None  # 所有文件的最后记录的总数据
        self._root = root
        self._reg = reg
        self._init()

    def _init(self):
        for dirpath, _, filenames in os.walk(self._root):
            for file in filenames:
                if re.match(self._reg, file):
                    path = os.path.join(dirpath, file)
                    self.handle_file(path)

    def get_metrics(self):
        metrics = {}
        for handler in self.handlers.values():
            try:
                for metric in handler.get_metrics():
                    key = (metric.endpoint, metric.metric)
                    if key in metrics:
                        old = metrics[key]
                        if old.timestamp > metric.timestamp:
                            continue
                    metrics[key] = metric
            except Exception as e:
                logging.exception(e)
                continue
        return list(metrics.values())

    def handle_file(self, path):
        if path not in self.handlers:
            logging.info("开始处理Metric文件: %s" % path)
            self.handlers[path] = HandleMetric(path)

    def on_created(self, event):
        self.handle_file(event.src_path)

    def on_modified(self, event):
        pass


class SqliteMetricObserver(object):
    interval = 10
    reg = r"ctaMetric.sqlite"  # 监控的文件

    def __init__(self, root=".", url=None):
        self._root = os.path.abspath(root)
        self._observer = Observer()
        self._handler = FileEventHandler(self._root, self.reg)
        self._observer.schedule(self._handler, self._root, True)
        self._url = get_open_falcon_url(url)

    def run(self):
        self._observer.start()
        while True:
            try:
                time.sleep(self.interval)
                self.push_metrics(self._handler.get_metrics())  # 每5秒push一次
            except KeyboardInterrupt:
                self._observer.stop()
                break
            except Exception as e:
                logging.exception(e)
        self._observer.join()

    def dump_metrics(self, metrics):
        payload = [metric.__dict__ for metric in metrics]
        return json.dumps(payload, cls=NumpyEncoder)

    def push_metrics(self, metrics):
        r = requests.post(self._url, data=self.dump_metrics(metrics))
        logging.info("推送%s个指标，response:%s" % (len(metrics), r.content))


if __name__ == "__main__":
    from vnpy.trader.app.ctaStrategy.plugins.ctaMetric.observers.utils import run_observer

    run_observer(SqliteMetricObserver, ".")
