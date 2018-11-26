import json
import os
import time
import re
import logging

import requests
from watchdog.observers import Observer
from watchdog.events import RegexMatchingEventHandler
import pandas as pd


class HandleMetric(object):
    def __init__(self, metric_file):
        self.metric_file = metric_file
        self.data = []
        self.groupByTag = ["endpoint", "metric", "tags", "step", "counterType"]
        self.df_last_data = None
        self.file_position = 0  # 上次读取位置
        self.load_data()

    def load_data(self):
        if os.path.exists(self.metric_file):
            with open(self.metric_file, 'r') as f:
                if self.file_position != 0:
                    f.seek(self.file_position, 0)
                while True:
                    line = f.readline()
                    self.file_position = self.file_position + len(line)
                    if not line.strip():
                        break
                    elif '|' in line:
                        self.data.append(json.loads(line.strip().split('|')[-1]))
                    else:
                        pass
            self.get_last()

    def clear_data(self):
        self.data = []

    def data_order(self, data):
        return data.loc[data.groupby(self.groupByTag).timestamp.idxmax().values]

    def get_last(self):
        if self.data:
            if self.df_last_data is None:
                self.df_last_data = self.data_order(pd.DataFrame(self.data))
            elif not self.df_last_data.empty:
                temp = self.data_order(pd.DataFrame(self.data))
                self.df_last_data = self.data_order(self.df_last_data.append(temp, ignore_index=True))
        self.clear_data()


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

    def merge_data(self, data):
        if data.df_last_data is None:
            pass
        elif self.df_total is None:
            self.df_total = data.df_last_data
        else:
            self.df_total = data.data_order(self.df_total.append(data.df_last_data, ignore_index=True))

    def handle_file(self, path):
        if path not in self.handlers:
            logging.info("开始监听log文件:%s" % path)
            self.handlers[path] = HandleMetric(path)
            self.merge_data(self.handlers[path])  # 把单个文件的最后记录加进来

    def on_created(self, event):
        self.handle_file(event.src_path)
    
    def on_modified(self, event):
        path =event.src_path
        if path not in self.handlers:
            self.handle_file(path)
        handler = self.handlers[path]
        handler.load_data()
        self.merge_data(handler)


class MetricFileObserver(object):
    interval = 10
    reg = r"ctaMetric.log.*"  # 监控的文件

    def __init__(self, root=".", url=None):
        self._root = os.path.abspath(root)
        self._observer = Observer()
        self._handler = FileEventHandler(self._root, self.reg)
        self._observer.schedule(self._handler, self._root, True)
        self._url = url or os.environ.get("OPEN_FALCON_URL", "http://localhost:1988/v1/push")

    def run(self):
        self._observer.start()
        try:
            while True:
                time.sleep(self.interval)
                self.push_metric(self._handler.df_total)  # 每5秒push一次
        except KeyboardInterrupt:
            self._observer.stop()
        self._observer.join()

    def push_metric(self, df):
        if df is None:
            return
        if df.empty:
            return
        payload = []
        for _, row in df.iterrows():
            push_data = {
                "endpoint": row['endpoint'],
                "metric": row['metric'],
                "timestamp": int(time.time()), # update the local time
                "step": row['step'],
                "value": row['value'],
                "counterType": row['counterType'],
                "tags": row['tags'],
            }
            payload.append(push_data)
        # print(payload)
        r = requests.post(self._url, data=json.dumps(payload))
        logging.info("推送%s个指标，response:%s" % (len(payload), r.content))

def run_observer(path, url=None):
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(message)s")
    MetricFileObserver(path, url).run()

if __name__ == "__main__":
    run_observer(".")
    
