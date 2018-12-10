import logging
import json
from logging.handlers import TimedRotatingFileHandler

from six import with_metaclass
from vnpy.trader.utils import Singleton
from vnpy.trader.vtFunction import getTempPath

from .base import set_sender, MetricSender

@set_sender
class LogfileMetricSender(with_metaclass(Singleton, MetricSender)):
    def __init__(self):
        super(LogfileMetricSender, self).__init__()
        logger = logging.getLogger(__name__)
        filename = "ctaMetric.log"
        filepath = getTempPath(filename)
        self.hander = TimedRotatingFileHandler(filepath, when="d", backupCount=7)
        formater = logging.Formatter(fmt="%(asctime)s|%(message)s")
        self.hander.setFormatter(formater)
        logger.addHandler(self.hander)
        logger.setLevel(logging.INFO)
        logger.propagate = False
        self.logger = logger
        
    def pushMetrics(self, metrics):
        for metric in metrics:
            self.logger.info(metric.to_json())
        self.hander.close()
